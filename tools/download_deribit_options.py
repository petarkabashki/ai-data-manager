#!/usr/bin/env -S uv run python
"""Download Deribit crypto option history into the options catalog.

Deribit is the only venue that serves bars for *expired* options, which is to
say the only one with any history at all: OKX, Gate and Bybit list thousands
of option markets but expose OHLCV for the live board only. Measured depth on
Deribit reaches at least March 2019, on both daily and intraday bars.

There is no bulk listing of expired instruments, so discovery constructs
candidate names and validates them one at a time (~39ms each). Give it the
expiries and a strike grid:

  # One known expiry, strikes it actually listed, daily bars
  uv run python tools/download_deribit_options.py \\
      --underlying BTC --expiries 2024-12-27 --strike-grid 20000:140000:5000

  # Every quarterly since 2019, dry run first
  uv run python tools/download_deribit_options.py \\
      --quarterlies --since 2019-01-01 --strike-grid 5000:150000:5000 --dry-run

  # Whatever is on the board right now, hourly
  uv run python tools/download_deribit_options.py --active --timeframes 1h

Prices are the option premium in the settlement coin — a BTC option quoting
0.1575 means 0.1575 BTC, not $0.1575 — and `volume` counts contracts. Both are
recorded in `options.yaml`; see `utils/options.py`.
"""

from __future__ import annotations

import argparse
import calendar
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import merge_ohlcv_parquet, read_parquet_range
from utils.deribit import DeribitClient, DeribitError, InstrumentInfo
from utils.options import (
    RIGHTS,
    UNDERLYINGS,
    UnknownUnderlyingError,
    contract_for,
    load_options_catalog,
    option_file_path,
    save_options_catalog,
    update_option_entry,
)

PROVIDER = "Deribit"

#: Deribit's quarterly expiries are the last Friday of these months.
QUARTERLY_MONTHS = (3, 6, 9, 12)


def last_friday(year: int, month: int) -> date:
    """The last Friday of a month — how every Deribit monthly expiry is set."""
    last_day = calendar.monthrange(year, month)[1]
    day = date(year, month, last_day)
    return day.fromordinal(day.toordinal() - ((day.weekday() - 4) % 7))


def generate_expiries(since: date, until: date, *, quarterly: bool) -> list[date]:
    """Candidate expiry dates in a range.

    Candidates, not facts: a generated date is only real if `get_instrument`
    confirms an instrument on it. Deribit's calendar has changed over the
    years and this deliberately doesn't try to model that history.
    """
    months = QUARTERLY_MONTHS if quarterly else range(1, 13)
    out: list[date] = []
    for year in range(since.year, until.year + 1):
        for month in months:
            expiry = last_friday(year, month)
            if since <= expiry <= until:
                out.append(expiry)
    return sorted(out)


def parse_strike_grid(spec: str) -> list[float]:
    """`min:max:step` → a list of strikes."""
    try:
        low, high, step = (float(part) for part in spec.split(":"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--strike-grid must be MIN:MAX:STEP, got {spec!r}"
        ) from exc
    if step <= 0 or high < low:
        raise argparse.ArgumentTypeError(f"--strike-grid {spec!r} describes no strikes")

    strikes: list[float] = []
    value = low
    while value <= high:
        strikes.append(value)
        value += step
    return strikes


def discover(
    client: DeribitClient,
    underlying: str,
    expiries: list[date],
    strikes: list[float],
    rights: list[str],
) -> tuple[list[InstrumentInfo], int]:
    """Probe the candidate grid, returning the instruments that really exist.

    Also returns the number of probes spent, because that is the cost the
    caller is trading against grid width.
    """
    found: list[InstrumentInfo] = []
    probes = 0
    for expiry in expiries:
        for strike in strikes:
            for right in rights:
                contract = contract_for(underlying, expiry, strike, right)
                probes += 1
                info = client.get_instrument(contract.deribit_name)
                if info:
                    found.append(info)
    return found, probes


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--underlying", default="BTC", choices=sorted(UNDERLYINGS),
                   help="Option underlying (default: BTC)")
    p.add_argument("--timeframes", nargs="+", default=["1d"],
                   help="Timeframes to store (default: 1d; 1h and finer are paged)")

    selection = p.add_argument_group("which contracts")
    selection.add_argument("--expiries", nargs="+",
                           help="Explicit expiry dates (YYYY-MM-DD)")
    selection.add_argument("--quarterlies", action="store_true",
                           help="Generate last-Friday expiries for Mar/Jun/Sep/Dec")
    selection.add_argument("--monthlies", action="store_true",
                           help="Generate last-Friday expiries for every month")
    selection.add_argument("--active", action="store_true",
                           help="Use the instruments currently on the board instead of probing")
    selection.add_argument("--since", help="Earliest generated expiry (YYYY-MM-DD)")
    selection.add_argument("--until", help="Latest generated expiry (default: today)")
    selection.add_argument("--strike-grid", type=parse_strike_grid,
                           help="Strikes to probe, as MIN:MAX:STEP (e.g. 20000:140000:5000)")
    selection.add_argument("--strikes", nargs="+", type=float,
                           help="Explicit strikes instead of a grid")
    selection.add_argument("--rights", nargs="+", default=["C", "P"], choices=sorted(RIGHTS),
                           help="Calls, puts, or both (default: both)")
    selection.add_argument("--limit", type=int,
                           help="Stop after this many contracts (useful for smoke tests)")

    p.add_argument("--keep-untraded", action="store_true",
                   help="Keep zero-volume bars; by default they are dropped as padding")
    p.add_argument("--sleep-seconds", type=float, default=0.05,
                   help="Delay between requests (default: 0.05)")
    p.add_argument("--request-timeout", type=float, default=30.0,
                   help="Per-request timeout in seconds")
    p.add_argument("--dry-run", action="store_true",
                   help="Discover and report without downloading or writing")
    args = p.parse_args()

    if not (args.expiries or args.quarterlies or args.monthlies or args.active):
        p.error("choose contracts with --expiries, --quarterlies, --monthlies or --active")
    if not args.active and not (args.strike_grid or args.strikes):
        p.error("--strike-grid or --strikes is required unless --active is used")

    for timeframe in args.timeframes:
        from utils.deribit import TIMEFRAME_TO_RESOLUTION

        if timeframe not in TIMEFRAME_TO_RESOLUTION:
            p.error(f"unsupported timeframe {timeframe!r}; "
                    f"choose from {sorted(TIMEFRAME_TO_RESOLUTION)}")

    catalog = load_options_catalog()
    failures: list[tuple[str, str, str]] = []
    written = 0

    with DeribitClient(timeout=args.request_timeout, sleep_seconds=args.sleep_seconds) as client:
        instruments, probes = _select_instruments(client, args, p)

        if args.limit:
            instruments = instruments[: args.limit]

        print(f"{len(instruments)} contract(s)"
              + (f" from {probes} probes" if probes else "")
              + f" for {args.underlying}")
        for info in instruments[:10]:
            print(f"  {info.name}  listed {info.listed} expiry {info.expiry}")
        if len(instruments) > 10:
            print(f"  … and {len(instruments) - 10} more")

        if args.dry_run:
            print("\nDry run — nothing downloaded. Re-run without --dry-run.")
            return 0
        if not instruments:
            print("Nothing to download.")
            return 1

        for info in instruments:
            try:
                contract = _contract_from_info(info)
            except (ValueError, UnknownUnderlyingError) as exc:
                failures.append((info.name, "-", str(exc)))
                continue

            for timeframe in args.timeframes:
                try:
                    bars = client.fetch_bars(
                        info, timeframe, keep_untraded=args.keep_untraded
                    )
                except (DeribitError, ValueError) as exc:
                    failures.append((info.name, timeframe, str(exc)))
                    continue

                if bars.empty:
                    # Normal, not an error: most strikes in a wide grid are far
                    # enough from the money that they never print a trade.
                    continue

                dst = option_file_path(PROVIDER, contract.underlying,
                                       contract.symbol, timeframe)
                before, after = merge_ohlcv_parquet(bars, dst)
                span = read_parquet_range(dst)
                if not span:
                    failures.append((info.name, timeframe, "file unreadable after merge"))
                    continue
                start_date, end_date, _ = span
                update_option_entry(catalog, contract, timeframe, PROVIDER,
                                    dst, start_date, end_date)
                written += 1
                print(f"  {contract.symbol} {timeframe}: {before}->{after} rows "
                      f"({start_date} -> {end_date})")

    if not written:
        print("\nNo series had any traded bars — nothing written.")
        return 1

    save_options_catalog(catalog)
    print(f"\nWrote {written} series; options.yaml holds {len(catalog)} contract(s)")

    if failures:
        print(f"\n{len(failures)} failure(s):")
        for name, timeframe, why in failures[:20]:
            print(f"  {name} {timeframe}: {why}")
        if len(failures) > 20:
            print(f"  … and {len(failures) - 20} more")
        return 1
    return 0


def _contract_from_info(info: InstrumentInfo):
    """Canonical contract for a discovered instrument, carrying its real metadata."""
    from utils.options import contract_from_deribit_name

    return contract_from_deribit_name(
        info.name, listed=info.listed, contract_size=info.contract_size
    )


def _select_instruments(
    client: DeribitClient,
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> tuple[list[InstrumentInfo], int]:
    """Resolve the CLI's contract selection into real instruments."""
    if args.active:
        names = client.active_option_names(args.underlying)
        resolved = [client.get_instrument(name) for name in names]
        return [i for i in resolved if i], 0

    if args.expiries:
        try:
            expiries = [date.fromisoformat(value) for value in args.expiries]
        except ValueError as exc:
            parser.error(f"bad --expiries value: {exc}")
    else:
        if not args.since:
            parser.error("--quarterlies/--monthlies need --since")
        expiries = generate_expiries(
            date.fromisoformat(args.since),
            date.fromisoformat(args.until) if args.until else date.today(),
            quarterly=args.quarterlies and not args.monthlies,
        )

    strikes = args.strikes if args.strikes else args.strike_grid
    print(f"Probing {len(expiries)} expiries x {len(strikes)} strikes x "
          f"{len(args.rights)} rights…")
    return discover(client, args.underlying, expiries, strikes, args.rights)


if __name__ == "__main__":
    raise SystemExit(main())
