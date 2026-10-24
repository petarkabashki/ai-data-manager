#!/usr/bin/env -S uv run python
"""Download US-stock bars from IBKR for repo stock universes.

Usage examples:
  uv run python tools/download_ibkr_stock_overnight.py --universe sp500 --timeframes 1h 4h --since 2024-01-01

  uv run python tools/download_ibkr_stock_overnight.py --universe nasdaq100 --symbols AAPL MSFT NVDA --full --timeframes 1h

  uv run python tools/download_ibkr_stock_overnight.py --universe sp500 nasdaq100 --limit 25 --sleep-seconds 0.5
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import IBKR_CLIENT_ID, IBKR_HOST, IBKR_PORT, TMP_ROOT, relative_to_repo
from utils.catalog import (
    REPO_ROOT,
    TF_TO_MS,
    load_stock_catalog,
    merge_ohlcv_parquet,
    read_parquet_range,
    save_stock_catalog,
    stock_file_path,
    update_stock_timeframe_entry,
)
from utils.calendars import UsEquityCalendar
from utils.gaps import detect_gaps
from utils.ibkr import (
    DownloadRange,
    IbkrHistoricalClient,
    chunk_days_for_timeframe,
    dt_from_ms,
    download_ranges,
    filter_full_bars_outside_rth,
    format_ranges,
    plan_download_ranges,
    read_timestamp_bounds,
    utc_now,
)

UNIVERSE_CHOICES = ["sp500", "nasdaq100"]
STOCK_PROVIDER = "IBKR"
SESSION_CHOICES = ["all", "overnight"]

#: Progress artifacts are per (session, timeframe-set) so concurrent or
#: sequential runs don't overwrite each other's checkpoints — and so
#: retry_ibkr_failed_sequentially.py can find them deterministically.
DEFAULT_RESUME_LOG = TMP_ROOT / "ibkr_stock_download_resume.jsonl"
DEFAULT_PROGRESS_FILE = TMP_ROOT / "ibkr_stock_download_progress.json"


def progress_file_for(timeframes: list[str], session: str) -> Path:
    """Default progress path for one run, e.g. tmp/ibkr_stock_progress_all_1h-4h.json."""
    return TMP_ROOT / f"ibkr_stock_progress_{session}_{'-'.join(timeframes)}.json"


def resume_log_for(timeframes: list[str], session: str) -> Path:
    return TMP_ROOT / f"ibkr_stock_resume_{session}_{'-'.join(timeframes)}.jsonl"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _select_symbols(catalog: dict, requested: list[str] | None, limit: int | None) -> list[str]:
    symbols = requested or list(catalog.keys())
    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def _gap_ranges(path: Path, timeframe: str, download_end, session: str) -> list[DownloadRange]:
    """Internal-gap and trailing windows for an existing session-filtered file.

    Gaps are judged against the matching trading calendar, so weekends, market
    holidays and early closes don't produce windows IBKR can never fill. Before
    that, this fed every weekend straight back into a historical request, on
    every run.
    """
    calendar = UsEquityCalendar("overnight" if session == "overnight" else "rth")
    interval_ms = TF_TO_MS[timeframe]
    bounds = read_timestamp_bounds(path)
    if bounds is None:
        return []
    _, last_ms = bounds

    ranges: list[DownloadRange] = []
    gaps, trailing_candles = detect_gaps(path, timeframe, calendar)
    for gap in gaps:
        ranges.append(
            DownloadRange(
                start=dt_from_ms(gap.start_ms),
                end=dt_from_ms(gap.end_ms + interval_ms),
                reason=f"gap:{gap.missing_candles}",
            )
        )
    if trailing_candles > 0:
        trailing_end_ms = last_ms + trailing_candles * interval_ms
        ranges.append(
            DownloadRange(
                start=dt_from_ms(last_ms + interval_ms),
                end=min(download_end, dt_from_ms(trailing_end_ms + interval_ms)),
                reason="tail",
            )
        )
    return ranges


def _plan_ranges(
    path: Path,
    timeframe: str,
    since: str | None,
    full: bool,
    session: str,
    overlap_days: int,
    download_end,
) -> list[DownloadRange]:
    """Thin wrapper over the shared planner, adding the overnight gap pass."""
    extra = (
        _gap_ranges(path, timeframe, download_end, session)
        if session == "overnight" and not full and path.exists()
        else None
    )
    return plan_download_ranges(
        path,
        since=since,
        full=full,
        download_end=download_end,
        # In overnight mode the gap pass above already covers the tail, so the
        # generic overlap window would only duplicate requests.
        overlap_days=0 if session == "overnight" else overlap_days,
        timeframe=timeframe,
        extra_gap_ranges=extra,
    )


def _serialize_range(download_range: DownloadRange) -> dict:
    return {
        "start": download_range.start.isoformat(),
        "end": download_range.end.isoformat(),
        "reason": download_range.reason,
    }


def _write_resume_event(path: Path, payload: dict) -> None:
    _ensure_parent(path)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(payload, sort_keys=True) + "\n")


def _write_progress_file(path: Path, payload: dict) -> None:
    _ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def _resolve_use_rth(mode: str, timeframe: str) -> bool:
    """Whether to request regular-trading-hours-only bars for this timeframe.

    'auto' preserves the behaviour this tool has always had (RTH for daily and
    weekly bars, extended hours for intraday) — which contradicted the docs
    claiming useRTH=0 unconditionally. The flag makes the choice explicit and
    overridable instead of hardcoded.
    """
    if mode == "always":
        return True
    if mode == "never":
        return False
    return timeframe in ("1d", "1w")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--universe", nargs="+", choices=UNIVERSE_CHOICES, required=True,
                   help="Stock universes to download from existing catalogs")
    p.add_argument("--symbols", nargs="+",
                   help="Optional explicit symbol subset (must exist in the chosen universe catalog)")
    p.add_argument("--timeframes", nargs="+", choices=["1h", "4h", "8h", "1d", "1w"], default=["1h", "4h"],
                   help="IBKR bar sizes to request and store")
    p.add_argument("--since", help="Start date for new symbols or full refreshes (YYYY-MM-DD)")
    p.add_argument("--full", action="store_true",
                   help="Ignore existing intraday files and rebuild from --since")
    p.add_argument("--host", default=IBKR_HOST, help="IBKR TWS/Gateway host (env: IBKR_HOST)")
    p.add_argument("--port", type=int, default=IBKR_PORT, help="IBKR TWS/Gateway port (env: IBKR_PORT)")
    p.add_argument("--client-id", type=int, default=IBKR_CLIENT_ID, help="IBKR API client id (env: IBKR_CLIENT_ID)")
    p.add_argument("--chunk-days", type=int,
                   help="Historical request chunk size in calendar days (default adapts by timeframe)")
    p.add_argument("--overlap-days", type=int, default=5,
                   help="Overlap window when appending to existing files")
    p.add_argument("--what-to-show", default="TRADES", choices=["TRADES", "ADJUSTED_LAST"],
                   help="IBKR historical data type (ADJUSTED_LAST applies split/dividend adjustment)")
    p.add_argument("--use-rth", default="auto", choices=["auto", "always", "never"],
                   help="Restrict requests to regular trading hours. 'auto' (default) uses RTH for "
                        "daily/weekly bars and extended hours for intraday, matching prior behaviour")
    p.add_argument("--session", default="all", choices=SESSION_CHOICES,
                   help="Store all returned bars or only bars fully outside regular trading hours")
    p.add_argument("--sleep-seconds", type=float, default=0.35,
                   help="Delay between historical requests")
    p.add_argument("--request-timeout", type=float, default=60.0,
                   help="Per-IBKR request timeout in seconds before treating the chunk as failed")
    p.add_argument("--max-chunk-retries", type=int, default=2,
                   help="Number of retries for timeout/HMDS-disconnect chunk errors before skipping to the next symbol")
    p.add_argument("--limit", type=int,
                   help="Limit symbols per universe (useful for smoke tests)")
    p.add_argument("--resume-log", type=Path,
                   help="JSONL event log for resumable long downloads "
                        "(default: tmp/ibkr_stock_resume_<session>_<timeframes>.jsonl)")
    p.add_argument("--progress-file", type=Path,
                   help="JSON progress snapshot rewritten after each timeframe result "
                        "(default: tmp/ibkr_stock_progress_<session>_<timeframes>.json)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be downloaded without writing files or catalogs")
    args = p.parse_args()

    if args.progress_file is None:
        args.progress_file = progress_file_for(args.timeframes, args.session)
    if args.resume_log is None:
        args.resume_log = resume_log_for(args.timeframes, args.session)

    overall_failures: list[tuple[str, str, str]] = []
    overall_updates = 0
    download_end = utc_now()
    planned_items = 0
    completed_items = 0
    progress_state = {
        "status": "running",
        "universes": args.universe,
        "timeframes": args.timeframes,
        "session": args.session,
        "since": args.since,
        "full": args.full,
        "download_end": download_end.isoformat(),
        "planned_items": 0,
        "completed_items": 0,
        "updated_items": 0,
        "failures": [],
        "current": None,
        "updated_at": download_end.isoformat(),
    }
    _write_progress_file(args.progress_file, progress_state)

    with IbkrHistoricalClient(args.host, args.port, args.client_id, timeout=args.request_timeout) as client:
        contract_cache: dict[str, object] = {}

        for universe in args.universe:
            catalog = load_stock_catalog(universe)
            if not catalog:
                print(f"{universe}: catalog is empty or missing")
                continue

            symbols = _select_symbols(catalog, args.symbols, args.limit)
            print(f"\n[{universe}] {len(symbols)} symbols")

            for symbol in symbols:
                if symbol not in catalog:
                    overall_failures.append((universe, symbol, "symbol not found in catalog"))
                    print(f"  {symbol}: skipped (not in {universe} catalog)")
                    continue

                try:
                    contract = contract_cache.get(symbol)
                    if contract is None:
                        contract = client.resolve_us_stock_contract(symbol)
                        contract_cache[symbol] = contract

                    for timeframe in args.timeframes:
                        dst = stock_file_path(STOCK_PROVIDER, universe, symbol, timeframe, args.session)
                        ranges = _plan_ranges(
                            dst,
                            timeframe,
                            args.since,
                            args.full,
                            args.session,
                            args.overlap_days,
                            download_end,
                        )
                        chunk_days = chunk_days_for_timeframe(timeframe, args.chunk_days)
                        if not ranges:
                            print(f"  {symbol}/{timeframe}: already current, no gaps detected")
                            completed_items += 1
                            planned_items += 1
                            progress_state.update({
                                "planned_items": planned_items,
                                "completed_items": completed_items,
                                "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "current"},
                                "updated_at": utc_now().isoformat(),
                            })
                            _write_resume_event(args.resume_log, {
                                "event": "skip_current",
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "session": args.session,
                                "path": relative_to_repo(dst),
                                "updated_at": utc_now().isoformat(),
                            })
                            _write_progress_file(args.progress_file, progress_state)
                            continue

                        planned_items += 1
                        print(f"  {symbol}/{timeframe}: {format_ranges(ranges)}")
                        progress_state.update({
                            "planned_items": planned_items,
                            "current": {
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "status": "downloading",
                                "ranges": [_serialize_range(r) for r in ranges],
                            },
                            "updated_at": utc_now().isoformat(),
                        })
                        _write_resume_event(args.resume_log, {
                            "event": "download_start",
                            "universe": universe,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "session": args.session,
                            "ranges": [_serialize_range(r) for r in ranges],
                            "updated_at": utc_now().isoformat(),
                        })
                        _write_progress_file(args.progress_file, progress_state)
                        df = download_ranges(
                            client,
                            contract,
                            label=symbol,
                            timeframe=timeframe,
                            ranges=ranges,
                            chunk_days=chunk_days,
                            what_to_show=args.what_to_show,
                            use_rth=_resolve_use_rth(args.use_rth, timeframe),
                            sleep_seconds=args.sleep_seconds,
                            max_chunk_retries=args.max_chunk_retries,
                            post_filter=(
                                (lambda d, tf=timeframe: filter_full_bars_outside_rth(d, tf))
                                if args.session == "overnight"
                                else None
                            ),
                        )
                        if df.empty:
                            completed_items += 1
                            progress_state.update({
                                "completed_items": completed_items,
                                "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "empty"},
                                "updated_at": utc_now().isoformat(),
                            })
                            _write_resume_event(args.resume_log, {
                                "event": "download_empty",
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "session": args.session,
                                "ranges": [_serialize_range(r) for r in ranges],
                                "updated_at": utc_now().isoformat(),
                            })
                            _write_progress_file(args.progress_file, progress_state)
                            continue

                        if args.dry_run:
                            print(f"    [dry-run] would write {len(df)} rows to {dst}")
                            completed_items += 1
                            progress_state.update({
                                "completed_items": completed_items,
                                "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "dry_run"},
                                "updated_at": utc_now().isoformat(),
                            })
                            _write_resume_event(args.resume_log, {
                                "event": "dry_run",
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "session": args.session,
                                "rows": len(df),
                                "path": str(dst),
                                "updated_at": utc_now().isoformat(),
                            })
                            _write_progress_file(args.progress_file, progress_state)
                            continue

                        old_rows, new_rows = merge_ohlcv_parquet(df, dst)
                        result = read_parquet_range(dst)
                        if result is None:
                            raise RuntimeError(f"Could not read back merged parquet {dst}")
                        start_date, end_date, _ = result

                        # timeframe_dates is keyed by (timeframe, provider), so this can
                        # never clobber another provider's registration for this timeframe.
                        update_stock_timeframe_entry(
                            catalog,
                            symbol=symbol,
                            timeframe=timeframe,
                            provider=STOCK_PROVIDER,
                            file_path=dst,
                            start_date=start_date,
                            end_date=end_date,
                            session=args.session,
                            what_to_show=args.what_to_show,
                        )
                        overall_updates += 1
                        completed_items += 1
                        progress_state.update({
                            "completed_items": completed_items,
                            "updated_items": overall_updates,
                            "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "updated"},
                            "updated_at": utc_now().isoformat(),
                        })
                        _write_resume_event(args.resume_log, {
                            "event": "download_complete",
                            "universe": universe,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "session": args.session,
                            "rows_before": old_rows,
                            "rows_after": new_rows,
                            "ranges": [_serialize_range(r) for r in ranges],
                            "path": relative_to_repo(dst),
                            "updated_at": utc_now().isoformat(),
                        })
                        _write_progress_file(args.progress_file, progress_state)
                        print(f"    wrote {relative_to_repo(dst)} ({old_rows}->{new_rows} rows)")

                except Exception as exc:
                    overall_failures.append((universe, symbol, str(exc)))
                    progress_state["failures"].append({
                        "universe": universe,
                        "symbol": symbol,
                        "message": str(exc),
                        "updated_at": utc_now().isoformat(),
                    })
                    progress_state.update({
                        "current": {"universe": universe, "symbol": symbol, "status": "failed"},
                        "updated_at": utc_now().isoformat(),
                    })
                    _write_resume_event(args.resume_log, {
                        "event": "symbol_failed",
                        "universe": universe,
                        "symbol": symbol,
                        "message": str(exc),
                        "updated_at": utc_now().isoformat(),
                    })
                    _write_progress_file(args.progress_file, progress_state)
                    print(f"  {symbol}: FAILED ({exc})")

                if not args.dry_run:
                    save_stock_catalog(universe, catalog)

    progress_state.update({
        "status": "complete",
        "planned_items": planned_items,
        "completed_items": completed_items,
        "updated_items": overall_updates,
        "current": None,
        "updated_at": utc_now().isoformat(),
    })
    _write_progress_file(args.progress_file, progress_state)
    print("\nSummary")
    print(f"  updated timeframes: {overall_updates}")
    print(f"  failures: {len(overall_failures)}")
    if overall_failures:
        for universe, symbol, message in overall_failures[:20]:
            print(f"  - {universe}/{symbol}: {message}")
        if len(overall_failures) > 20:
            print(f"  ... and {len(overall_failures) - 20} more")


if __name__ == "__main__":
    main()
