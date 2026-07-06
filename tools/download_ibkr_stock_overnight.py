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

from utils.catalog import (
    REPO_ROOT,
    TF_TO_MS,
    load_stock_catalog,
    normalize_ohlcv,
    read_parquet_range,
    save_stock_catalog,
    stock_file_path,
    update_stock_timeframe_entry,
)
from utils.gaps import detect_gaps
from utils.ibkr import (
    HistoricalRequest,
    IbkrHistoricalClient,
    bars_to_dataframe,
    daterange_chunks,
    filter_full_bars_outside_rth,
    ibkr_bar_size,
    merge_canonical_parquet,
    read_last_timestamp,
    throttle_sleep,
)

UNIVERSE_CHOICES = ["sp500", "nasdaq100"]
STOCK_PROVIDER = "IBKR"
SESSION_CHOICES = ["all", "overnight"]
DEFAULT_CHUNK_DAYS = {
    "1h": 365,
    "4h": 730,
    "8h": 730,
    "1d": 365,
    "1w": 3650,
}
DEFAULT_RESUME_LOG = REPO_ROOT / "tmp" / "ibkr_stock_download_resume.jsonl"
DEFAULT_PROGRESS_FILE = REPO_ROOT / "tmp" / "ibkr_stock_download_progress.json"


@dataclass(slots=True)
class DownloadRange:
    start: datetime
    end: datetime
    reason: str


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _coerce_start(
    since: str | None,
    full: bool,
    existing_path: Path,
    overlap_days: int,
) -> datetime:
    if full:
        if since is None:
            raise ValueError("--full requires --since so the initial historical range is explicit")
        return datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    last_timestamp = read_last_timestamp(existing_path)
    if last_timestamp is not None:
        return datetime.fromtimestamp(last_timestamp / 1000, tz=timezone.utc) - timedelta(days=overlap_days)

    if since:
        return datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    return _utc_now() - timedelta(days=365)


def _select_symbols(catalog: dict, requested: list[str] | None, limit: int | None) -> list[str]:
    symbols = requested or list(catalog.keys())
    if limit is not None:
        symbols = symbols[:limit]
    return symbols


def _chunk_days_for_timeframe(timeframe: str, configured: int | None) -> int:
    return configured if configured is not None else DEFAULT_CHUNK_DAYS[timeframe]


def _ibkr_duration_str(chunk_start: datetime, chunk_end: datetime) -> str:
    days = max(1, (chunk_end - chunk_start).days)
    if days > 365:
        return f"{math.ceil(days / 365)} Y"
    return f"{days} D"


def _read_timestamp_bounds(path: Path) -> tuple[int, int] | None:
    if not path.exists():
        return None
    df = normalize_ohlcv(pd.read_parquet(path))
    if df.empty:
        return None
    ts = df["timestamp"].astype("int64")
    return int(ts.iloc[0]), int(ts.iloc[-1])


def _dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def _merge_ranges(ranges: list[DownloadRange]) -> list[DownloadRange]:
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda r: (r.start, r.end))
    merged: list[DownloadRange] = [ordered[0]]
    for item in ordered[1:]:
        prev = merged[-1]
        if item.start <= prev.end:
            merged[-1] = DownloadRange(
                start=prev.start,
                end=max(prev.end, item.end),
                reason=f"{prev.reason},{item.reason}",
            )
        else:
            merged.append(item)
    return merged


def _plan_download_ranges(
    path: Path,
    timeframe: str,
    since: str | None,
    full: bool,
    session: str,
    overlap_days: int,
    download_end: datetime,
) -> list[DownloadRange]:
    if full:
        if since is None:
            raise ValueError("--full requires --since so the initial historical range is explicit")
        return [DownloadRange(
            start=datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc),
            end=download_end,
            reason="full",
        )]

    if not path.exists():
        start = (
            datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if since
            else _utc_now() - timedelta(days=365)
        )
        return [DownloadRange(start=start, end=download_end, reason="missing_file")]

    interval_ms = TF_TO_MS[timeframe]
    bounds = _read_timestamp_bounds(path)
    if bounds is None:
        start = (
            datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if since
            else _utc_now() - timedelta(days=365)
        )
        return [DownloadRange(start=start, end=download_end, reason="unreadable_file")]

    start_ms, last_ms = bounds
    ranges: list[DownloadRange] = []
    requested_start = (
        datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if since
        else None
    )
    if since:
        requested_start_ms = int(requested_start.timestamp() * 1000)
        if requested_start_ms < start_ms:
            prepend_end_ms = max(requested_start_ms, start_ms - interval_ms)
            ranges.append(
                DownloadRange(
                    start=requested_start,
                    end=_dt_from_ms(prepend_end_ms + interval_ms),
                    reason="prepend",
                )
            )

    if session == "overnight":
        gaps, trailing_candles = detect_gaps(path, timeframe)
        for gap in gaps:
            ranges.append(
                DownloadRange(
                    start=_dt_from_ms(gap.start_ms),
                    end=_dt_from_ms(gap.end_ms + interval_ms),
                    reason=f"gap:{gap.missing_candles}",
                )
            )

        if trailing_candles > 0:
            trailing_start_ms = last_ms + interval_ms
            trailing_end_ms = last_ms + trailing_candles * interval_ms
            ranges.append(
                DownloadRange(
                    start=_dt_from_ms(trailing_start_ms),
                    end=min(download_end, _dt_from_ms(trailing_end_ms + interval_ms)),
                    reason="tail",
                )
            )
    else:
        tail_start = _dt_from_ms(last_ms) - timedelta(days=overlap_days)
        if requested_start is not None:
            tail_start = max(tail_start, requested_start)
        if tail_start < download_end:
            ranges.append(
                DownloadRange(
                    start=tail_start,
                    end=download_end,
                    reason="tail_overlap",
                )
            )

    return _merge_ranges([r for r in ranges if r.start < r.end])


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


def _is_hmds_no_data_error(exc: Exception) -> bool:
    return "HMDS query returned no data" in str(exc)


def _is_timeout_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return isinstance(exc, TimeoutError) or "timed out" in text


def _is_retryable_chunk_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return _is_timeout_error(exc) or "hmds server disconnect occurred" in text


def _download_symbol_timeframe(
    client: IbkrHistoricalClient,
    contract,
    symbol: str,
    timeframe: str,
    ranges: list[DownloadRange],
    chunk_days: int,
    what_to_show: str,
    session: str,
    sleep_seconds: float,
    max_chunk_retries: int,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    total_chunks = 0
    skipped_no_data_chunks = 0
    use_rth = timeframe == "1d"
    for planned_range in ranges:
        for chunk_start, chunk_end in daterange_chunks(planned_range.start, planned_range.end, chunk_days):
            total_chunks += 1
            max_attempts = max(1, max_chunk_retries + 1)
            bars = None
            for attempt in range(1, max_attempts + 1):
                try:
                    bars = client.fetch_historical_bars(
                        HistoricalRequest(
                            contract=contract,
                            end=chunk_end,
                            duration=_ibkr_duration_str(chunk_start, chunk_end),
                            bar_size=ibkr_bar_size(timeframe),
                            what_to_show=what_to_show,
                            use_rth=use_rth,
                        )
                    )
                    break
                except Exception as exc:
                    if _is_hmds_no_data_error(exc):
                        skipped_no_data_chunks += 1
                        print(
                            f"  {symbol}/{timeframe}: no data for chunk "
                            f"{chunk_start.strftime('%Y-%m-%d')}->{chunk_end.strftime('%Y-%m-%d')}"
                        )
                        bars = []
                        break
                    if _is_retryable_chunk_error(exc) and attempt < max_attempts:
                        print(
                            f"  {symbol}/{timeframe}: retry {attempt}/{max_attempts - 1} after transient error "
                            f"for {chunk_start.strftime('%Y-%m-%d')}->{chunk_end.strftime('%Y-%m-%d')}"
                        )
                        throttle_sleep(max(sleep_seconds, 0.35) * attempt)
                        continue
                    raise

            if bars is None:
                raise RuntimeError(f"{symbol}/{timeframe}: no chunk result returned")

            df = bars_to_dataframe(bars)
            if not df.empty:
                start_ms = int(chunk_start.timestamp() * 1000)
                end_ms = int(chunk_end.timestamp() * 1000)
                df = df[(df["timestamp"] >= start_ms) & (df["timestamp"] <= end_ms)]
                if session == "overnight":
                    df = filter_full_bars_outside_rth(df, timeframe)
                if not df.empty:
                    frames.append(df)
            throttle_sleep(sleep_seconds)

    if not frames:
        detail = (
            f" after skipping {skipped_no_data_chunks} no-data chunks"
            if skipped_no_data_chunks
            else ""
        )
        print(f"  {symbol}/{timeframe}: no {session} bars returned across {total_chunks} chunks{detail}")
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    merged = (
        pd.concat(frames)
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    detail = (
        f" with {skipped_no_data_chunks} no-data chunks skipped"
        if skipped_no_data_chunks
        else ""
    )
    print(f"  {symbol}/{timeframe}: fetched {len(merged)} {session} bars across {total_chunks} chunks{detail}")
    return merged


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
    p.add_argument("--host", default="127.0.0.1", help="IBKR TWS/Gateway host")
    p.add_argument("--port", type=int, default=7497, help="IBKR TWS/Gateway port")
    p.add_argument("--client-id", type=int, default=73, help="IBKR API client id")
    p.add_argument("--chunk-days", type=int,
                   help="Historical request chunk size in calendar days (default adapts by timeframe)")
    p.add_argument("--overlap-days", type=int, default=5,
                   help="Overlap window when appending to existing files")
    p.add_argument("--what-to-show", default="TRADES", choices=["TRADES", "ADJUSTED_LAST"],
                   help="IBKR historical data type")
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
    p.add_argument("--resume-log", type=Path, default=DEFAULT_RESUME_LOG,
                   help="JSONL event log for resumable long downloads")
    p.add_argument("--progress-file", type=Path, default=DEFAULT_PROGRESS_FILE,
                   help="JSON progress snapshot rewritten after each timeframe result")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be downloaded without writing files or catalogs")
    args = p.parse_args()

    overall_failures: list[tuple[str, str, str]] = []
    overall_updates = 0
    download_end = _utc_now()
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
                        ranges = _plan_download_ranges(
                            dst,
                            timeframe,
                            args.since,
                            args.full,
                            args.session,
                            args.overlap_days,
                            download_end,
                        )
                        chunk_days = _chunk_days_for_timeframe(timeframe, args.chunk_days)
                        if not ranges:
                            print(f"  {symbol}/{timeframe}: already current, no gaps detected")
                            completed_items += 1
                            planned_items += 1
                            progress_state.update({
                                "planned_items": planned_items,
                                "completed_items": completed_items,
                                "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "current"},
                                "updated_at": _utc_now().isoformat(),
                            })
                            _write_resume_event(args.resume_log, {
                                "event": "skip_current",
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "session": args.session,
                                "path": str(dst.relative_to(REPO_ROOT)) if dst.exists() else str(dst),
                                "updated_at": _utc_now().isoformat(),
                            })
                            _write_progress_file(args.progress_file, progress_state)
                            continue

                        planned_items += 1
                        print(
                            f"  {symbol}/{timeframe}: "
                            f"{'; '.join(f'{r.start.strftime('%Y-%m-%d')}->{r.end.strftime('%Y-%m-%d')} ({r.reason})' for r in ranges)}"
                        )
                        progress_state.update({
                            "planned_items": planned_items,
                            "current": {
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "status": "downloading",
                                "ranges": [_serialize_range(r) for r in ranges],
                            },
                            "updated_at": _utc_now().isoformat(),
                        })
                        _write_resume_event(args.resume_log, {
                            "event": "download_start",
                            "universe": universe,
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "session": args.session,
                            "ranges": [_serialize_range(r) for r in ranges],
                            "updated_at": _utc_now().isoformat(),
                        })
                        _write_progress_file(args.progress_file, progress_state)
                        df = _download_symbol_timeframe(
                            client=client,
                            contract=contract,
                            symbol=symbol,
                            timeframe=timeframe,
                            ranges=ranges,
                            chunk_days=chunk_days,
                            what_to_show=args.what_to_show,
                            session=args.session,
                            sleep_seconds=args.sleep_seconds,
                            max_chunk_retries=args.max_chunk_retries,
                        )
                        if df.empty:
                            completed_items += 1
                            progress_state.update({
                                "completed_items": completed_items,
                                "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "empty"},
                                "updated_at": _utc_now().isoformat(),
                            })
                            _write_resume_event(args.resume_log, {
                                "event": "download_empty",
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "session": args.session,
                                "ranges": [_serialize_range(r) for r in ranges],
                                "updated_at": _utc_now().isoformat(),
                            })
                            _write_progress_file(args.progress_file, progress_state)
                            continue

                        if args.dry_run:
                            print(f"    [dry-run] would write {len(df)} rows to {dst}")
                            completed_items += 1
                            progress_state.update({
                                "completed_items": completed_items,
                                "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "dry_run"},
                                "updated_at": _utc_now().isoformat(),
                            })
                            _write_resume_event(args.resume_log, {
                                "event": "dry_run",
                                "universe": universe,
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "session": args.session,
                                "rows": len(df),
                                "path": str(dst),
                                "updated_at": _utc_now().isoformat(),
                            })
                            _write_progress_file(args.progress_file, progress_state)
                            continue

                        old_rows, new_rows = merge_canonical_parquet(df, dst)
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
                        )
                        overall_updates += 1
                        completed_items += 1
                        progress_state.update({
                            "completed_items": completed_items,
                            "updated_items": overall_updates,
                            "current": {"universe": universe, "symbol": symbol, "timeframe": timeframe, "status": "updated"},
                            "updated_at": _utc_now().isoformat(),
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
                            "path": str(dst.relative_to(REPO_ROOT)),
                            "updated_at": _utc_now().isoformat(),
                        })
                        _write_progress_file(args.progress_file, progress_state)
                        print(f"    wrote {dst.relative_to(REPO_ROOT)} ({old_rows}->{new_rows} rows)")

                except Exception as exc:
                    overall_failures.append((universe, symbol, str(exc)))
                    progress_state["failures"].append({
                        "universe": universe,
                        "symbol": symbol,
                        "message": str(exc),
                        "updated_at": _utc_now().isoformat(),
                    })
                    progress_state.update({
                        "current": {"universe": universe, "symbol": symbol, "status": "failed"},
                        "updated_at": _utc_now().isoformat(),
                    })
                    _write_resume_event(args.resume_log, {
                        "event": "symbol_failed",
                        "universe": universe,
                        "symbol": symbol,
                        "message": str(exc),
                        "updated_at": _utc_now().isoformat(),
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
        "updated_at": _utc_now().isoformat(),
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
