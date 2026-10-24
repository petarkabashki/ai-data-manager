#!/usr/bin/env python3
"""Import a bulk per-contract futures archive from any vendor.

IBKR is a forward-building archive, not a backfill source: enumeration with
`includeExpired=True` reaches back about a year and a contract that expired
before that returns "No security definition has been found" even when named
explicitly. Deep history therefore has to come from somewhere else, and this
tool is the somewhere-else adapter.

It deliberately knows nothing about any one vendor's API. It takes a directory
of per-contract OHLCV files, maps each filename onto the repo's canonical
contract symbol, and merges the bars into the provider-first tree:

    futures/<provider>/<SYMBOL>/<SYMBOL>-<timeframe>.parquet

so an imported archive lands *beside* the IBKR feed rather than replacing it.
Both register under the same contract in `futures.yaml`, nested by provider
within each timeframe.

The filename is parsed by a regex supplying either a `symbol` group (already
canonical, e.g. `ESU26`) or `root` plus a delivery month — as `code` (the
letter) or `month` (a number) — plus `year` or `yy`. Examples:

    # FirstRate-style: ES_2026U_1hour.csv
    --pattern '(?P<root>[A-Z]{2,3})_(?P<year>\\d{4})(?P<code>[FGHJKMNQUVXZ])'

    # Databento-style: ESU6.ohlcv-1h.csv  (single-digit year needs --decade)
    --pattern '(?P<symbol>[A-Z]{2,3}[FGHJKMNQUVXZ]\\d{2})'

    # CSI-style: CL_201512.parquet
    --pattern '(?P<root>[A-Z]{2,3})_(?P<year>\\d{4})(?P<month>\\d{2})'

Nothing is written without `--apply`; the default is a dry run that prints the
mapping so a wrong pattern is caught before it fills the tree with mislabelled
contracts.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

from utils.catalog import merge_ohlcv_parquet, normalize_ohlcv, read_parquet_range
from utils.futures import (
    CODE_TO_MONTH,
    UnknownRootError,
    contract_for,
    futures_file_path,
    load_futures_catalog,
    parse_contract_symbol,
    save_futures_catalog,
    update_futures_entry,
)

READERS = {
    ".csv": pd.read_csv,
    ".txt": pd.read_csv,
    ".parquet": pd.read_parquet,
}


class PatternError(ValueError):
    """The filename pattern did not yield a contract."""


def contract_from_match(match: re.Match, *, decade: int | None) -> tuple[str, int, int]:
    """(root, year, delivery_month) from a filename match."""
    groups = match.groupdict()

    if groups.get("symbol"):
        return parse_contract_symbol(groups["symbol"])

    root = groups.get("root")
    if not root:
        raise PatternError("pattern must capture either 'symbol' or 'root'")

    if groups.get("month"):
        month = int(groups["month"])
    elif groups.get("code"):
        try:
            month = CODE_TO_MONTH[groups["code"].upper()]
        except KeyError as exc:
            raise PatternError(f"{groups['code']!r} is not a month code") from exc
    else:
        raise PatternError("pattern must capture 'month' or 'code'")

    if groups.get("year"):
        year = int(groups["year"])
        if year < 100:
            # A single- or double-digit year is ambiguous across decades and a
            # wrong guess silently files a 2016 contract as 2026. Make the
            # caller say which decade rather than picking one.
            if decade is None:
                raise PatternError(
                    f"year {groups['year']!r} is ambiguous; pass --decade (e.g. 2020)"
                )
            year = decade + (year % 10)
    elif groups.get("yy"):
        _, year, _ = parse_contract_symbol(f"ZZ{groups.get('code') or 'F'}{groups['yy']}")
    else:
        raise PatternError("pattern must capture 'year' or 'yy'")

    return root.upper(), year, month


def read_bars(path: Path, tz: str | None) -> pd.DataFrame:
    reader = READERS.get(path.suffix.lower())
    if reader is None:
        raise ValueError(f"No reader for {path.suffix} files")
    df = reader(path)
    # Vendors spell headers every way there is: "Date Time", "date_time",
    # " Close ". Fold whitespace and case before looking for anything.
    df.columns = [re.sub(r"[\s\-]+", "_", str(c).strip().lower()) for c in df.columns]

    # A vendor's time column is whatever they felt like calling it.
    for candidate in ("timestamp", "date_time", "datetime", "date", "time"):
        if candidate in df.columns:
            if candidate != "timestamp":
                df = df.rename(columns={candidate: "timestamp"})
            break
    else:
        raise ValueError(f"{path.name}: no recognisable time column in {list(df.columns)}")

    if not pd.api.types.is_numeric_dtype(df["timestamp"]):
        parsed = pd.to_datetime(df["timestamp"], errors="coerce", utc=tz is None)
        if tz is not None:
            # Vendors routinely publish exchange-local wall clock with no
            # offset. Localising it is the difference between correct bars and
            # a silent whole-session shift.
            parsed = parsed.dt.tz_localize(tz, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
        df["timestamp"] = parsed
        df = df[df["timestamp"].notna()]

    return normalize_ohlcv(df)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", required=True, type=Path, help="Directory of per-contract files")
    p.add_argument("--provider", required=True, help="Provider name, e.g. databento, csidata, firstrate")
    p.add_argument("--timeframe", required=True, help="Timeframe these files hold, e.g. 1h")
    p.add_argument("--pattern", required=True, help="Regex over the filename (see module docstring)")
    p.add_argument("--glob", default="*", help="Which files in --source to consider (default: all)")
    p.add_argument("--tz", help="Timezone of naive vendor timestamps, e.g. America/Chicago")
    p.add_argument("--decade", type=int, help="Decade for ambiguous 1-2 digit years, e.g. 2020")
    p.add_argument("--roots", nargs="*", help="Only import these roots")
    p.add_argument("--apply", action="store_true", help="Actually write (default: dry run)")
    args = p.parse_args()

    if not args.source.is_dir():
        print(f"error: {args.source} is not a directory", file=sys.stderr)
        return 2

    try:
        pattern = re.compile(args.pattern)
    except re.error as exc:
        print(f"error: bad --pattern: {exc}", file=sys.stderr)
        return 2

    files = sorted(f for f in args.source.rglob(args.glob) if f.is_file())
    if not files:
        print(f"No files matched {args.glob!r} under {args.source}")
        return 1

    catalog = load_futures_catalog() if args.apply else {}
    planned: list[tuple[Path, str]] = []
    skipped: list[tuple[str, str]] = []
    written = 0

    for path in files:
        match = pattern.search(path.name)
        if not match:
            skipped.append((path.name, "filename did not match --pattern"))
            continue
        try:
            root, year, month = contract_from_match(match, decade=args.decade)
        except (PatternError, ValueError) as exc:
            skipped.append((path.name, str(exc)))
            continue

        if args.roots and root not in {r.upper() for r in args.roots}:
            continue

        try:
            contract = contract_for(root, year, month)
        except UnknownRootError as exc:
            skipped.append((path.name, str(exc)))
            continue

        dst = futures_file_path(args.provider, contract.symbol, args.timeframe)
        planned.append((path, contract.symbol))

        if not args.apply:
            continue

        try:
            bars = read_bars(path, args.tz)
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the import
            skipped.append((path.name, f"read failed: {exc}"))
            continue

        if bars.empty:
            skipped.append((path.name, "no usable rows"))
            continue

        before, after = merge_ohlcv_parquet(bars, dst)
        span = read_parquet_range(dst)
        if span:
            start_date, end_date, _ = span
            # The last bar a contract ever printed *is* its last trade date, so
            # an archive that only labels files by delivery month still yields
            # a real expiry for every contract that has finished trading.
            expiry = pd.to_datetime(end_date).date()
            observed = contract_for(root, year, month, expiry=expiry)
            update_futures_entry(
                catalog, observed, args.timeframe, args.provider, dst, start_date, end_date
            )
        written += 1
        print(f"  {contract.symbol}: {path.name} ({before}->{after} rows)")

    print(f"\n{len(planned)} file(s) mapped to contracts")
    for path, symbol in planned[:10]:
        print(f"  {path.name} -> {symbol}")
    if len(planned) > 10:
        print(f"  … and {len(planned) - 10} more")

    if skipped:
        print(f"\n{len(skipped)} skipped:")
        for name, why in skipped[:10]:
            print(f"  {name}: {why}")
        if len(skipped) > 10:
            print(f"  … and {len(skipped) - 10} more")

    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply once the mapping looks right.")
        return 0

    if not written:
        print("\nNothing imported — no catalog written.")
        return 1

    save_futures_catalog(catalog)
    print(f"\nWrote {written} series and updated {len(catalog)} contract(s) in futures.yaml")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
