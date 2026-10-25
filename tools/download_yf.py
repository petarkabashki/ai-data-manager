#!/usr/bin/env -S uv run python
"""Download Yahoo Finance bars for stocks, indexes, commodities and macro rates.

YF supplies the largest share of this repo — every universe's daily stock bars,
all of the indexes, the commodities — and until this existed there was no way
to refresh any of it. The data was produced out of band.

Usage examples:
  # Refresh daily bars for a stock universe
  uv run python tools/download_yf.py --universe sp500 --timeframes 1d

  # A few symbols, from scratch
  uv run python tools/download_yf.py --universe sp500 --symbols AAPL MSFT \\
      --timeframes 1d --full --since 2000-01-01

  # Indexes and commodities
  uv run python tools/download_yf.py --asset-class Index --timeframes 1d
  uv run python tools/download_yf.py --asset-class Commodity --timeframes 1d

  # Macro rate series (WTB3MS <- Yahoo ^IRX), daily only
  uv run python tools/download_yf.py --asset-class Macro --timeframes 1d

  # Adjusted closes instead of as-traded (recorded on the catalog node)
  uv run python tools/download_yf.py --universe sp500 --timeframes 1d --adjusted

  # Machine-readable: one JSON object on stdout, progress text on stderr
  uv run python tools/download_yf.py --universe sp500 --symbols AAPL --dry-run --json

`--dry-run` is safe in a read-only sandbox: it plans from the files on disk,
never constructs a yfinance session (no network, no ~/.cache), and never
saves a catalog. `tests/test_download_yf.py` holds it to that.

Macro (`WTB3MS`) is a rate series, not OHLCV: its file keeps its own layout
(`Value` on a `Date` index, `schema: rate_series`). The Yahoo close becomes
`Value` and is merged with `merge_rate_series()`, never `merge_ohlcv_parquet()`.
Which Yahoo series feeds each macro symbol is `utils/yf.MACRO_TICKERS`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.calendars import calendar_for
from utils.config import relative_to_repo
from utils.catalog import (
    REPO_ROOT,
    STOCK_UNIVERSES,
    commodity_file_path,
    index_file_path,
    load_index_catalog,
    load_simple_catalog,
    load_stock_catalog,
    macro_file_path,
    merge_ohlcv_parquet,
    merge_rate_series,
    read_rate_series,
    read_parquet_range,
    save_index_catalog,
    save_simple_catalog,
    save_stock_catalog,
    stock_file_path,
    update_index_timeframe_entry,
    update_simple_timeframe_entry,
    update_stock_timeframe_entry,
)
from utils.planning import format_ranges, plan_download_ranges, throttle_sleep, utc_now
from utils.report import print_table
from utils.semantics import Semantics, semantics_for
from utils.yf import UnsupportedTimeframe, YahooFetcher, fetch_ohlcv

PROVIDER = "YF"


def _stock_target(universe: str):
    """(loader, saver, path builder, writer) for one stock universe."""
    return (
        lambda: load_stock_catalog(universe),
        lambda catalog: save_stock_catalog(universe, catalog),
        lambda symbol, tf: stock_file_path(PROVIDER, universe, symbol, tf),
        lambda catalog, symbol, tf, path, start, end, semantics: update_stock_timeframe_entry(
            catalog, symbol, tf, PROVIDER, path, start, end, semantics=semantics
        ),
    )


def _index_target():
    return (
        load_index_catalog,
        save_index_catalog,
        lambda symbol, tf: index_file_path(PROVIDER, symbol, tf),
        lambda catalog, symbol, tf, path, start, end, semantics: update_index_timeframe_entry(
            catalog, symbol, tf, PROVIDER, path, start, end, semantics=semantics
        ),
    )


def _commodity_target():
    return (
        lambda: load_simple_catalog("commodities"),
        lambda catalog: save_simple_catalog("commodities", catalog),
        lambda symbol, tf: commodity_file_path(PROVIDER, symbol, tf),
        lambda catalog, symbol, tf, path, start, end, semantics: update_simple_timeframe_entry(
            catalog, symbol, tf, PROVIDER, path, start, end,
            asset_class="Commodity", semantics=semantics,
        ),
    )


def _macro_target():
    return (
        lambda: load_simple_catalog("macro"),
        lambda catalog: save_simple_catalog("macro", catalog),
        lambda symbol, tf: macro_file_path(PROVIDER, symbol, tf),
        lambda catalog, symbol, tf, path, start, end, semantics: update_simple_timeframe_entry(
            catalog, symbol, tf, PROVIDER, path, start, end,
            asset_class="Macro", semantics=semantics,
        ),
    )


def merge_macro(frame, dst: Path) -> tuple[int, int, str, str]:
    """Store a fetched Yahoo frame's closes as a rate series.
    Returns (rows_before, rows_after, start_date, end_date)."""
    import pandas as pd

    values = pd.Series(
        frame["close"].to_numpy(dtype="float64"),
        index=pd.to_datetime(frame["timestamp"], unit="ms").dt.normalize(),
    )
    old_rows, new_rows = merge_rate_series(values, dst)
    stored = read_rate_series(dst)
    return old_rows, new_rows, stored.index[0].strftime("%Y-%m-%d"), stored.index[-1].strftime("%Y-%m-%d")


def resolve_target(universe: str | None, asset_class: str | None):
    """(asset_class, label, loader, saver, path_for, write_entry) for the run."""
    if universe:
        return ("Stock", universe, *_stock_target(universe))
    if asset_class == "Index":
        return ("Index", "indexes", *_index_target())
    if asset_class == "Commodity":
        return ("Commodity", "commodities", *_commodity_target())
    if asset_class == "Macro":
        return ("Macro", "macro", *_macro_target())
    raise ValueError(f"Nothing to download for universe={universe!r} asset_class={asset_class!r}")


def download_symbol_timeframe(
    fetcher,
    symbol: str,
    asset_class: str,
    timeframe: str,
    ranges,
    auto_adjust: bool,
    sleep_seconds: float,
    log=print,
):
    """Fetch every planned window for one series and return one canonical frame."""
    import pandas as pd

    frames = []
    for planned in ranges:
        frame = fetch_ohlcv(
            fetcher, symbol, asset_class, timeframe,
            planned.start, planned.end, auto_adjust=auto_adjust,
        )
        if not frame.empty:
            frames.append(frame)
        throttle_sleep(sleep_seconds)

    if not frames:
        # Yahoo returns an empty frame for an unknown ticker rather than
        # erroring, so this is as likely to be a bad symbol as a quiet period.
        log(f"  {symbol}/{timeframe}: no rows returned")
        from utils.catalog import empty_ohlcv_frame
        return empty_ohlcv_frame()

    return (
        pd.concat(frames)
        .drop_duplicates(subset=["timestamp"], keep="last")
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    source = p.add_mutually_exclusive_group(required=True)
    source.add_argument("--universe", choices=STOCK_UNIVERSES, help="Stock universe to refresh")
    source.add_argument("--asset-class", choices=["Index", "Commodity", "Macro"],
                        help="Non-stock class to refresh")

    p.add_argument("--symbols", nargs="+", help="Explicit symbol subset (must exist in the catalog)")
    p.add_argument("--timeframes", nargs="+", default=["1d"],
                   help="Timeframes to download (default: 1d). Yahoo has no 4h/8h bar")
    p.add_argument("--since", help="Start date for new symbols or full refreshes (YYYY-MM-DD)")
    p.add_argument("--full", action="store_true", help="Ignore existing files and rebuild from --since")
    p.add_argument("--overlap-days", type=int, default=5,
                   help="Overlap window when appending to existing files")
    p.add_argument("--adjusted", action="store_true",
                   help="Store split/dividend-adjusted closes. Default is as-traded, matching "
                        "what's already on disk; either way the choice is recorded on the node")
    p.add_argument("--sleep-seconds", type=float, default=0.2, help="Delay between requests")
    p.add_argument("--limit", type=int, help="Cap the number of symbols (smoke tests)")
    p.add_argument("--dry-run", action="store_true", help="Plan only; fetch nothing, write nothing")
    p.add_argument("--json", action="store_true",
                   help="Emit one JSON object on stdout (plan, results, failures); progress goes to stderr")
    args = p.parse_args()

    # With --json, stdout carries exactly one JSON document; every human line
    # moves to stderr so the text stays visible without corrupting the JSON.
    def say(*parts):
        print(*parts, file=sys.stderr if args.json else sys.stdout)

    asset_class, label, load, save, path_for, write_entry = resolve_target(
        args.universe, args.asset_class
    )
    catalog = load()
    if not catalog:
        say(f"{label}: catalog is empty or missing")
        if args.json:
            print(json.dumps({"error": f"{label}: catalog is empty or missing"}))
        sys.exit(1)

    symbols = args.symbols or list(catalog)
    if args.limit:
        symbols = symbols[: args.limit]

    unknown = [s for s in symbols if s not in catalog]
    if unknown:
        p.error(f"not in the {label} catalog: {', '.join(unknown[:10])}")

    if asset_class == "Macro":
        # A rate series: no adjustment to choose, and only daily values exist.
        if args.adjusted:
            p.error("--adjusted does not apply to macro rate series")
        if set(args.timeframes) != {"1d"}:
            p.error("macro rate series are daily only: use --timeframes 1d")
        semantics = semantics_for(asset_class, PROVIDER)
    else:
        semantics = Semantics(
            schema="ohlcv",
            adjustment="adjusted" if args.adjusted else semantics_for(asset_class, PROVIDER).adjustment,
            volume_basis=semantics_for(asset_class, PROVIDER).volume_basis,
            bar_label="open",
        )

    fetcher = None if args.dry_run else YahooFetcher()
    download_end = utc_now()
    updated = 0
    failures: list[tuple[str, str, str]] = []
    rows: list[list] = []
    plan: list[dict] = []
    calendar_name = calendar_for(asset_class).name

    say(f"\n{label}: {len(symbols)} symbol(s) x {len(args.timeframes)} timeframe(s) from Yahoo")
    if asset_class == "Macro":
        say("Values  : Yahoo close, stored as Value (rate series)")
    else:
        say(f"Closes  : {'adjusted' if args.adjusted else 'as-traded (raw)'}")
    say(f"Calendar: {calendar_name}\n")

    for symbol in symbols:
        for timeframe in args.timeframes:
            dst = path_for(symbol, timeframe)
            try:
                ranges = plan_download_ranges(
                    dst,
                    since=args.since,
                    full=args.full,
                    download_end=download_end,
                    overlap_days=args.overlap_days,
                    timeframe=timeframe,
                )
            except ValueError as exc:
                p.error(str(exc))

            plan.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "file": relative_to_repo(dst),
                "current": not ranges,
                "ranges": [
                    {"start": r.start.strftime("%Y-%m-%d"), "end": r.end.strftime("%Y-%m-%d"),
                     "reason": r.reason}
                    for r in ranges
                ],
            })
            if not ranges:
                say(f"  {symbol}/{timeframe}: already current")
                continue

            say(f"  {symbol}/{timeframe}: {format_ranges(ranges)}")
            if args.dry_run:
                continue

            try:
                frame = download_symbol_timeframe(
                    fetcher, symbol, asset_class, timeframe, ranges,
                    auto_adjust=args.adjusted, sleep_seconds=args.sleep_seconds,
                )
            except UnsupportedTimeframe as exc:
                p.error(str(exc))
            except Exception as exc:
                failures.append((symbol, timeframe, str(exc)))
                say(f"  {symbol}/{timeframe}: FAILED ({exc})")
                continue

            if frame.empty:
                continue

            if asset_class == "Macro":
                old_rows, new_rows, start_date, end_date = merge_macro(frame, dst)
            else:
                old_rows, new_rows = merge_ohlcv_parquet(frame, dst)
                result = read_parquet_range(dst)
                if result is None:
                    failures.append((symbol, timeframe, "merged file unreadable"))
                    continue
                start_date, end_date, _ = result

            write_entry(catalog, symbol, timeframe, dst, start_date, end_date, semantics)
            save(catalog)  # checkpoint per series: a long run stays resumable
            updated += 1
            rows.append([symbol, timeframe, old_rows, new_rows, new_rows - old_rows, end_date])
            say(f"    wrote {relative_to_repo(dst)} ({old_rows}->{new_rows} rows)")

    if rows and not args.json:
        print()
        print_table(["Symbol", "TF", "Before", "After", "Added", "Through"], rows[:40])
        if len(rows) > 40:
            print(f"… and {len(rows) - 40} more")

    say(f"\nSummary\n  updated: {updated}\n  failures: {len(failures)}")
    for symbol, timeframe, message in failures[:20]:
        say(f"  - {symbol}/{timeframe}: {message}")
    if len(failures) > 20:
        say(f"  … and {len(failures) - 20} more")

    if args.json:
        print(json.dumps({
            "target": {"universe": args.universe, "asset_class": asset_class, "label": label},
            "provider": PROVIDER,
            "dry_run": args.dry_run,
            "adjusted": args.adjusted,
            "calendar": calendar_name,
            "timeframes": args.timeframes,
            "symbols": len(symbols),
            "plan": plan,
            "planned": sum(1 for item in plan if not item["current"]),
            "current": sum(1 for item in plan if item["current"]),
            "results": [
                {"symbol": r[0], "timeframe": r[1], "before": r[2], "after": r[3],
                 "added": r[4], "through": r[5]}
                for r in rows
            ],
            "updated": updated,
            "failures": [
                {"symbol": sym, "timeframe": tf, "error": msg} for sym, tf, msg in failures
            ],
        }, default=str))

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
