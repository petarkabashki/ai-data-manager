#!/usr/bin/env -S uv run python
"""Download US-stock bars from IBKR for repo stock universes.

Usage examples:
  uv run python tools/download_ibkr_stock_overnight.py --universe sp500 --timeframes 1h 4h --since 2024-01-01

  uv run python tools/download_ibkr_stock_overnight.py --universe nasdaq100 --symbols AAPL MSFT NVDA --full --timeframes 1h

  uv run python tools/download_ibkr_stock_overnight.py --universe sp500 nasdaq100 --limit 25 --sleep-seconds 0.5

  # replace, not merge, from --since on (a split seam on the old scale); dry run first
  uv run python tools/download_ibkr_stock_overnight.py --universe sp500 --symbols MNST --timeframes 1d \\
      --full --since 2000-01-01 --replace --dry-run
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import DATA_ROOT, IBKR_CLIENT_ID, IBKR_HOST, IBKR_PORT, TMP_ROOT, relative_to_repo
from utils.catalog import (
    IBKR_SESSIONS,
    REPO_ROOT,
    TF_TO_MS,
    PendingStockWrites,
    load_stock_catalog,
    merge_ohlcv_parquet,
    replace_report,
    read_parquet_range,
    stock_file_path,
    stock_home_universe,
    update_stock_timeframe_entry,
)
from utils.calendars import UsEquityCalendar
from utils.code_version import CodeVersion
from utils.gaps import detect_gaps
from utils.ibkr import (
    clip_ranges_to_earliest,
    GatewayUnresponsive,
    SymbolHints,
    is_no_security_definition,
    is_pacing_violation,
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

UNIVERSE_CHOICES = ["sp500", "nasdaq100", "russell2000"]
STOCK_PROVIDER = "IBKR"
SESSION_CHOICES = list(IBKR_SESSIONS)

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


def trim_redundant_ranges(ranges, path: Path, refresh_hours: float, now_ts: float | None = None):
    """Drop windows that cannot yield new bars.

    - A `prepend` window under 3 days is the gap between the requested start
      and a first bar that already sits on it (weekend, holiday, first print).
    - A `tail_overlap` window is a refresh of the last few days; a file written
      within `refresh_hours` already had one, so a restart must not repeat it.
    """
    out = []
    recent = False
    if path.exists():
        age_h = ((now_ts if now_ts is not None else time.time()) - path.stat().st_mtime) / 3600
        recent = age_h < refresh_hours
    for r in ranges:
        if r.reason == "prepend" and (r.end - r.start) < timedelta(days=3):
            continue
        if r.reason == "tail_overlap" and recent:
            continue
        out.append(r)
    return out


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


#: --replace refuses when the download starts this much later than the stored span ...
REPLACE_MAX_LATE_START = timedelta(days=7)
#: ... or when more than this share of the stored span's rows would go unreplaced.
REPLACE_MAX_LOST = 0.02


def replace_refusal(report: dict) -> str | None:
    """Why a replace must not be written, or None. A short or late download must
    never shrink a file: it would drop history IBKR still holds but did not send."""
    if report["span_new"] == 0:
        return "download is empty"
    if report["first_old"] is not None and report["first_new"] is not None and \
            report["first_new"] - report["first_old"] > REPLACE_MAX_LATE_START.total_seconds() * 1000:
        return (f"download starts {dt_from_ms(report['first_new']).date()}, stored span "
                f"{dt_from_ms(report['first_old']).date()} (> {REPLACE_MAX_LATE_START.days} days later)")
    if report["span_old"] and report["lost"] > REPLACE_MAX_LOST * report["span_old"]:
        return (f"{report['lost']} of {report['span_old']} stored rows are not in the download "
                f"(> {REPLACE_MAX_LOST:.0%})")
    return None


def format_replace(report: dict) -> str:
    return (f"rows {report['rows_before']} -> {report['rows_after']}; replaced span "
            f"{report['span_old']} stored / {report['span_new']} downloaded, "
            f"{report['changed']} changed close, {report['lost']} lost, {report['added']} added")


def backup_before_replace(dst: Path, backup_dir: Path, data_root: Path | None = None) -> Path:
    """Copy `dst` under `backup_dir`, keeping its path below the data root."""
    import shutil
    try:
        rel = dst.resolve().relative_to((data_root or DATA_ROOT).resolve())
    except ValueError:
        rel = Path(dst.name)
    target = backup_dir / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(dst, target)
    return target


def _stop_for_code_change(args: argparse.Namespace, progress_state: dict) -> None:
    """Record why the run stops; the catalog flush happens as the `with` unwinds."""
    progress_state.update({"status": "code_changed", "updated_at": utc_now().isoformat()})
    _write_progress_file(args.progress_file, progress_state)
    _write_resume_event(args.resume_log, {"event": "code_changed", "updated_at": utc_now().isoformat()})


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
    p.add_argument("--replace", action="store_true",
                   help="With --full and --since: stored rows from --since on are replaced by the "
                        "download, not merged with it (a split seam on the old price scale). "
                        "Backs the file up first, refuses a short or late download, prints "
                        "rows before/after; with --dry-run prints that report only")
    p.add_argument("--no-share", action="store_true",
                   help="Keep files under the requested universe even when an earlier universe "
                        "(sp500 before nasdaq100) already lists the symbol")
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
    p.add_argument("--max-consecutive-timeouts", type=int, default=3,
                   help="Consecutive request timeouts that mean the Gateway session is dead; "
                        "the client then reconnects instead of timing out chunk after chunk")
    p.add_argument("--max-reconnects", type=int, default=3,
                   help="Reconnect attempts in a row before exiting with code 75 (EX_TEMPFAIL) "
                        "so a supervisor can wait for the Gateway and relaunch")
    p.add_argument("--hints-file", type=Path, default=DATA_ROOT / "stocks" / "ibkr" / ".symbol_hints.json",
                   help="Shared cache of IBKR earliest-bar dates and unresolvable tickers")
    p.add_argument("--refresh-hours", type=float, default=12.0,
                   help="Skip the last-days refresh of a file written within this many hours")
    p.add_argument("--hang-skip-hours", type=float, default=24.0,
                   help="Skip a symbol whose requests hung twice on fresh sessions for this long "
                        "(0 retries every run)")
    p.add_argument("--no-head-timestamp", action="store_true",
                   help="Don't ask IBKR for each symbol's earliest bar; request every window from --since")
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
    if args.replace and not (args.full and args.since):
        p.error("--replace needs --full and --since (the first bar to replace)")
    replace_from = (int(pd.Timestamp(args.since, tz="UTC").value // 1_000_000)
                    if args.replace else None)
    replace_backup = TMP_ROOT / f"ibkr_replace_backup_{utc_now():%Y%m%dT%H%M%SZ}"
    replace_rows: list[dict] = []

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

    hints = SymbolHints(args.hints_file)
    code = CodeVersion()
    reconnects = 0
    dead_hits: dict[str, int] = {}
    with IbkrHistoricalClient(
        args.host, args.port, args.client_id,
        timeout=args.request_timeout,
        max_consecutive_timeouts=args.max_consecutive_timeouts,
    ) as client, PendingStockWrites() as pending:
        contract_cache: dict[str, object] = {}
        head_cache: dict[str, datetime | None] = dict(hints.head)
        catalogs: dict[str, dict] = {}

        for universe in args.universe:
            catalog = catalogs[universe] = load_stock_catalog(universe)
            if not catalog:
                print(f"{universe}: catalog is empty or missing")
                continue

            symbols = _select_symbols(catalog, args.symbols, args.limit)
            print(f"\n[{universe}] {len(symbols)} symbols")

            queue = deque(symbols)
            while queue:
                # Between symbols: a process on old code must not write another item.
                code.exit_if_changed(before_exit=lambda: _stop_for_code_change(args, progress_state))
                symbol = queue.popleft()
                if symbol in hints.unresolvable:
                    print(f"  {symbol}: skipped (IBKR has no contract: {hints.unresolvable[symbol][:60]})")
                    continue
                if symbol not in catalog:
                    overall_failures.append((universe, symbol, "symbol not found in catalog"))
                    print(f"  {symbol}: skipped (not in {universe} catalog)")
                    continue

                # A symbol in several universes has one set of files, under the
                # first universe that lists it; both catalogs point at it.
                home = universe if args.no_share else stock_home_universe(symbol, universe, catalogs)
                if home != universe:
                    print(f"  {symbol}: files shared with {home}")

                cur_tf = None
                try:
                    contract = contract_cache.get(symbol)
                    if contract is None:
                        contract = client.resolve_us_stock_contract(symbol)
                        contract_cache[symbol] = contract

                    earliest = head_cache.get(symbol)
                    if earliest is None and not args.no_head_timestamp and symbol not in head_cache:
                        for head_try in range(3):
                            try:
                                earliest = client.head_timestamp(contract, args.what_to_show)
                                break
                            except GatewayUnresponsive:
                                raise
                            except Exception as exc:  # head lookup is an optimisation only
                                if is_pacing_violation(exc) and head_try < 2:
                                    print(f"  {symbol}: head lookup paced, waiting 20s")
                                    time.sleep(20)
                                    continue
                                print(f"  {symbol}: head timestamp unavailable ({exc})")
                                earliest = None
                                break
                        if earliest is not None:
                            head_cache[symbol] = earliest
                            if not args.dry_run:
                                hints.set_head(symbol, earliest)
                            print(f"  {symbol}: earliest IBKR bar {earliest.date()}")

                    for timeframe in args.timeframes:
                        cur_tf = timeframe
                        if args.hang_skip_hours and hints.is_hanging(f"{symbol}/{timeframe}", args.hang_skip_hours):
                            print(f"  {symbol}/{timeframe}: skipped (requests hung recently; retry after {args.hang_skip_hours:g}h)")
                            continue
                        dst = stock_file_path(STOCK_PROVIDER, home, symbol, timeframe, args.session)
                        ranges = _plan_ranges(
                            dst,
                            timeframe,
                            args.since,
                            args.full,
                            args.session,
                            args.overlap_days,
                            download_end,
                        )
                        # The per-timeframe floor (first bar a finished pass found) is
                        # tighter than IBKR's head timestamp, which is not per bar size.
                        tf_earliest = hints.floor.get(f"{symbol}/{timeframe}") or earliest
                        ranges = trim_redundant_ranges(
                            clip_ranges_to_earliest(ranges, tf_earliest), dst,
                            0 if args.replace else args.refresh_hours,  # a replace needs every bar
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
                            earliest=tf_earliest,
                            post_filter=(
                                (lambda d, tf=timeframe: filter_full_bars_outside_rth(d, tf))
                                if args.session == "overnight"
                                else None
                            ),
                        )
                        report = None
                        if args.replace:
                            report = replace_report(df, dst, replace_from) if dst.exists() else None
                            refusal = replace_refusal(report) if report else (
                                "download is empty" if df.empty else None)
                            line = format_replace(report) if report else f"new file, {len(df)} rows"
                            print(f"    replace {'[dry-run] ' if args.dry_run else ''}{line}"
                                  + (f"; REFUSED: {refusal}" if refusal else ""))
                            replace_rows.append({"symbol": symbol, "timeframe": timeframe,
                                                 "refused": refusal, **(report or {"path": str(dst)})})
                            if refusal:
                                overall_failures.append((universe, symbol, f"{timeframe}: replace refused: {refusal}"))
                                continue
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

                        if args.replace and dst.exists():
                            saved = backup_before_replace(dst, replace_backup)
                            print(f"    backup {relative_to_repo(saved)}")
                        old_rows, new_rows = merge_ohlcv_parquet(df, dst, replace_from=replace_from)
                        result = read_parquet_range(dst)
                        if result is None:
                            raise RuntimeError(f"Could not read back merged parquet {dst}")
                        start_date, end_date, _ = result
                        if any(r.reason == "prepend" for r in ranges) and not args.dry_run:
                            hints.set_floor(
                                f"{symbol}/{timeframe}",
                                datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc),
                            )

                        # timeframe_dates is keyed by (timeframe, provider), so this can
                        # never clobber another provider's registration for this timeframe.
                        register = partial(
                            update_stock_timeframe_entry,
                            symbol=symbol,
                            timeframe=timeframe,
                            provider=STOCK_PROVIDER,
                            file_path=dst,
                            start_date=start_date,
                            end_date=end_date,
                            session=args.session,
                            what_to_show=args.what_to_show,
                        )
                        register(catalog)
                        pending.add(universe, register)
                        if home != universe:
                            register(catalogs[home])
                            pending.add(home, register)
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

                except GatewayUnresponsive as exc:
                    # Not the symbol's fault: reopen the session and retry it.
                    # A session that dies on the same symbol again after a fresh
                    # reconnect is that symbol's problem (its request never
                    # answers), not the Gateway's: record it and move on.
                    dead_hits[symbol] = dead_hits.get(symbol, 0) + 1
                    if dead_hits[symbol] >= 2:
                        overall_failures.append((universe, symbol, f"requests hang: {exc}"))
                        progress_state["failures"].append({
                            "universe": universe, "symbol": symbol,
                            "message": f"requests hang: {exc}", "updated_at": utc_now().isoformat(),
                        })
                        _write_resume_event(args.resume_log, {
                            "event": "symbol_failed", "universe": universe, "symbol": symbol,
                            "message": f"requests hang after reconnect: {exc}",
                            "updated_at": utc_now().isoformat(),
                        })
                        _write_progress_file(args.progress_file, progress_state)
                        if not args.dry_run and cur_tf:
                            hints.mark_hang(f"{symbol}/{cur_tf}")
                        print(f"  {symbol}: FAILED (requests hang even on a fresh session; skipping)")
                        try:
                            client.reconnect()
                        except Exception as conn_exc:
                            print(f"  reconnect failed: {conn_exc}")
                        continue
                    reconnects += 1
                    print(f"  {symbol}: Gateway session dead ({exc}); reconnect {reconnects}/{args.max_reconnects}")
                    _write_resume_event(args.resume_log, {
                        "event": "gateway_reconnect", "symbol": symbol, "message": str(exc),
                        "attempt": reconnects, "updated_at": utc_now().isoformat(),
                    })
                    if reconnects > args.max_reconnects:
                        progress_state.update({"status": "gateway_down", "updated_at": utc_now().isoformat()})
                        _write_progress_file(args.progress_file, progress_state)
                        print("Gateway did not recover; exiting 75 so the supervisor can relaunch")
                        sys.exit(75)
                    time.sleep(10 * reconnects)
                    try:
                        client.reconnect()
                    except Exception as conn_exc:
                        print(f"  reconnect failed: {conn_exc}")
                        # The Gateway itself is down, so this says nothing about the symbol.
                        dead_hits[symbol] -= 1
                    queue.appendleft(symbol)
                    continue
                except Exception as exc:
                    if is_no_security_definition(exc) and not args.dry_run:
                        hints.mark_unresolvable(symbol, str(exc))
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

                reconnects = 0

                # Per-symbol, each catalog in its own txn. The mutators are re-applied
                # to the file, never the in-memory node, so a YF node written since
                # this run loaded survives. `continue` above leaves them queued, and
                # leaving the `with` flushes whatever is still queued.
                pending.flush()

    progress_state.update({
        "status": "complete",
        "planned_items": planned_items,
        "completed_items": completed_items,
        "updated_items": overall_updates,
        "current": None,
        "updated_at": utc_now().isoformat(),
    })
    _write_progress_file(args.progress_file, progress_state)
    if replace_rows:
        out = TMP_ROOT / f"ibkr_replace_report_{utc_now():%Y%m%dT%H%M%SZ}.json"
        _ensure_parent(out)
        out.write_text(json.dumps({"dry_run": args.dry_run, "since": args.since,
                                   "backup_dir": None if args.dry_run else str(replace_backup),
                                   "files": replace_rows}, indent=2))
        print(f"\nreplace report: {relative_to_repo(out)}")

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
