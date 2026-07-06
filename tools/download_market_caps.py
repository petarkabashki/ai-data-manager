#!/usr/bin/env -S uv run python
"""Fetch historical CoinGecko market-cap data for altcoin universe snapshots.

Usage examples:
  # Fetch a recent 365-day window for a hand-picked universe:
  uv run python tools/download_market_caps.py \
    --symbols sol ada xrp doge \
    --start 2025-07-01 --end 2026-06-01

  # Build monthly top-20 snapshots for all active coins, excluding BTC + stablecoins:
  uv run python tools/download_market_caps.py \
    --all-active \
    --start 2025-07-01 --end 2026-06-01 --frequency monthly \
    --top-n 20 --exclude-ids bitcoin --exclude-stablecoins

  # Use explicit CoinGecko ids when symbols are ambiguous:
  uv run python tools/download_market_caps.py \
    --coin-ids solana cardano chainlink \
    --dates 2025-09-01 2025-12-01 2026-03-01

Notes:
  - Without COINGECKO_PRO_API_KEY, CoinGecko's public API currently limits
    historical chart requests to roughly the most recent 365 days.
  - The tool caches the coin list and per-coin history under:
      price-data/market-cap/coingecko/
"""

from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.market_caps import (
    CoinGeckoClient,
    apply_exclusions,
    combine_histories,
    export_path,
    fetch_or_load_history,
    get_coin_list,
    parse_iso_date,
    rank_top_n,
    read_lines,
    resolve_coin_ids,
    snapshot_for_dates,
    validate_history_window,
    daterange,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    universe = parser.add_argument_group("universe")
    universe.add_argument("--coin-ids", nargs="+", help="Exact CoinGecko ids to fetch")
    universe.add_argument("--coin-ids-file", help="File with one CoinGecko id per line")
    universe.add_argument("--symbols", nargs="+", help="CoinGecko symbols to resolve (lower/upper case accepted)")
    universe.add_argument("--symbols-file", help="File with one CoinGecko symbol per line")
    universe.add_argument("--all-active", action="store_true", help="Use the full active CoinGecko universe")
    universe.add_argument("--all-coins", action="store_true", help="Use active + inactive CoinGecko universe")

    dates = parser.add_argument_group("dates")
    dates.add_argument("--date", dest="dates_inline", action="append", help="Explicit snapshot date (YYYY-MM-DD); repeatable")
    dates.add_argument("--dates", nargs="+", help="Explicit snapshot dates (YYYY-MM-DD)")
    dates.add_argument("--dates-file", help="File with one snapshot date per line")
    dates.add_argument("--start", help="Range start date (YYYY-MM-DD)")
    dates.add_argument("--end", help="Range end date (YYYY-MM-DD)")
    dates.add_argument(
        "--frequency",
        choices=["daily", "weekly", "monthly"],
        default="daily",
        help="Date frequency when using --start/--end (default: daily)",
    )

    ranking = parser.add_argument_group("ranking")
    ranking.add_argument("--top-n", type=int, help="Keep only top N per date")
    ranking.add_argument("--exclude-ids", nargs="+", help="CoinGecko ids to exclude from ranking output")
    ranking.add_argument("--exclude-symbols", nargs="+", help="Symbols to exclude from ranking output")
    ranking.add_argument(
        "--exclude-stablecoins",
        action="store_true",
        help="Exclude a built-in stablecoin list from ranking output",
    )

    io_group = parser.add_argument_group("io")
    io_group.add_argument("--vs-currency", default="usd", help="Market-cap quote currency (default: usd)")
    io_group.add_argument("--workers", type=int, default=4, help="Parallel coin fetch workers (default: 4)")
    io_group.add_argument("--refresh-coin-list", action="store_true", help="Ignore cached coin list and refetch")
    io_group.add_argument("--refresh-history", action="store_true", help="Ignore cached per-coin history and refetch")
    io_group.add_argument(
        "--output-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Export format for snapshot/history outputs (default: parquet)",
    )
    io_group.add_argument("--history-output", help="Optional explicit path for combined history export")
    io_group.add_argument("--snapshot-output", help="Optional explicit path for snapshot/ranking export")
    io_group.add_argument(
        "--history-only",
        action="store_true",
        help="Fetch and export combined history but skip snapshot/ranking export",
    )

    return parser.parse_args()


def _collect_strings(args: argparse.Namespace, inline_key: str, file_key: str) -> list[str]:
    values = list(getattr(args, inline_key) or [])
    file_path = getattr(args, file_key)
    if file_path:
        values.extend(read_lines(file_path))
    return values


def _collect_dates(args: argparse.Namespace) -> list[date]:
    raw_dates = _collect_strings(args, "dates_inline", "dates_file")
    raw_dates.extend(args.dates or [])

    if raw_dates:
        parsed = sorted({parse_iso_date(value) for value in raw_dates})
        return parsed

    if not args.start or not args.end:
        raise ValueError(
            "Specify explicit --date/--dates/--dates-file, or provide both --start and --end"
        )

    return daterange(parse_iso_date(args.start), parse_iso_date(args.end), args.frequency)


def _default_history_output(args: argparse.Namespace, start: date, end: date) -> Path:
    return export_path(
        f"market_caps_{args.vs_currency}_{start.isoformat()}_{end.isoformat()}",
        args.output_format,
    )


def _default_snapshot_output(args: argparse.Namespace, start: date, end: date) -> Path:
    suffix = f"top_{args.top_n}" if args.top_n else "snapshots"
    return export_path(
        f"{suffix}_{args.vs_currency}_{start.isoformat()}_{end.isoformat()}",
        args.output_format,
    )


def _save_frame(df: pd.DataFrame, path: Path, output_format: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "parquet":
        df.to_parquet(path, index=False)
        return
    df.to_csv(path, index=False)


def _format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def main() -> None:
    args = _parse_args()
    run_started = time.monotonic()

    client = CoinGeckoClient(log=print)
    effective_workers = args.workers
    if not client.config.has_full_history and args.workers > 1:
        effective_workers = 1
        print(
            "Notice: using 1 worker in public CoinGecko mode to reduce rate-limit stalls. "
            "Set COINGECKO_PRO_API_KEY for faster parallel backfills."
        )
    print("Stage 1/5: parsing requested dates")
    snapshot_dates = _collect_dates(args)
    start_date, end_date = min(snapshot_dates), max(snapshot_dates)
    validate_history_window(client.config, start_date, end_date)

    if args.top_n and args.all_active and not args.all_coins:
        print(
            "Warning: --all-active excludes inactive/delisted coins, so historical top-N results "
            "will not be fully correct for older dates. Use --all-coins with COINGECKO_PRO_API_KEY "
            "for a historically complete universe."
        )
    elif args.top_n and not any([args.all_active, args.all_coins]):
        print(
            "Warning: --top-n ranks only within the explicitly supplied coin ids/symbols. "
            "Use --all-coins for historically correct market-wide top-N lists."
        )

    coin_ids = _collect_strings(args, "coin_ids", "coin_ids_file")
    symbols = _collect_strings(args, "symbols", "symbols_file")
    print(
        f"Stage 2/5: loading CoinGecko id map "
        f"(refresh={'yes' if args.refresh_coin_list else 'no'})"
    )
    coin_list = get_coin_list(client, refresh=args.refresh_coin_list)
    print(f"  loaded {len(coin_list)} coin ids")
    print("Stage 3/5: resolving requested universe")
    resolved = resolve_coin_ids(
        coin_list,
        coin_ids=coin_ids,
        symbols=symbols,
        all_active=args.all_active,
        all_coins=args.all_coins,
    )

    print(
        f"Universe: {len(resolved)} coin(s) | source={client.config.source_label} | "
        f"window={start_date}..{end_date}"
    )

    histories: dict[str, pd.DataFrame] = {}
    source_counts = {"api": 0, "cache": 0}
    total_coins = len(resolved)
    completed = 0
    fetch_started = time.monotonic()
    progress_every = 25 if total_coins >= 100 else 10 if total_coins >= 25 else 1
    next_progress_mark = progress_every
    print(
        f"Stage 4/5: fetching histories with {effective_workers} worker(s) "
        f"for {total_coins} coin(s)"
    )

    def worker(coin_id: str) -> tuple[str, pd.DataFrame, str]:
        frame, source = fetch_or_load_history(
            client,
            coin_id=coin_id,
            start=start_date,
            end=end_date,
            vs_currency=args.vs_currency,
            refresh=args.refresh_history,
        )
        return coin_id, frame, source

    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures = {pool.submit(worker, coin_id): coin_id for coin_id in resolved["id"]}
        for future in as_completed(futures):
            try:
                coin_id, frame, source = future.result()
            except Exception as exc:
                elapsed = time.monotonic() - fetch_started
                print(
                    f"  error after {completed}/{total_coins} completed | "
                    f"api={source_counts['api']} cache={source_counts['cache']} | "
                    f"elapsed={_format_elapsed(elapsed)}"
                )
                raise RuntimeError(f"History fetch failed: {exc}") from exc
            histories[coin_id] = frame
            source_counts[source] += 1
            completed += 1
            elapsed = time.monotonic() - fetch_started
            rate = completed / elapsed if elapsed > 0 else 0.0
            remaining = total_coins - completed
            eta_seconds = remaining / rate if rate > 0 else 0.0
            print(f"  {coin_id}: {len(frame)} rows [{source}]")
            if completed >= next_progress_mark or completed == total_coins:
                pct = (completed / total_coins) * 100 if total_coins else 100.0
                print(
                    f"  progress: {completed}/{total_coins} ({pct:.1f}%) | "
                    f"api={source_counts['api']} cache={source_counts['cache']} | "
                    f"elapsed={_format_elapsed(elapsed)} | eta={_format_elapsed(eta_seconds)}"
                )
                next_progress_mark += progress_every

    print("Stage 5/5: combining and writing outputs")
    combined = combine_histories(histories.values(), resolved)
    if combined.empty:
        raise RuntimeError("No market-cap rows were returned for the requested universe/date window")

    history_output = Path(args.history_output) if args.history_output else _default_history_output(
        args, start_date, end_date
    )
    _save_frame(combined, history_output, args.output_format)
    print(f"Saved history: {history_output}")

    if args.history_only:
        print(
            f"Summary: {len(combined)} history rows | {source_counts['api']} API fetches | "
            f"{source_counts['cache']} cache hits | "
            f"elapsed={_format_elapsed(time.monotonic() - run_started)}"
        )
        return

    snapshots = snapshot_for_dates(combined, snapshot_dates)
    snapshots = apply_exclusions(
        snapshots,
        exclude_ids=args.exclude_ids,
        exclude_symbols=args.exclude_symbols,
        exclude_stablecoins=args.exclude_stablecoins,
    )
    if snapshots.empty:
        raise RuntimeError("No snapshot rows remained after date filtering/exclusions")

    if args.top_n:
        snapshots = rank_top_n(snapshots, args.top_n)

    snapshot_output = Path(args.snapshot_output) if args.snapshot_output else _default_snapshot_output(
        args, start_date, end_date
    )
    _save_frame(snapshots, snapshot_output, args.output_format)
    print(f"Saved snapshots: {snapshot_output}")
    print(
        f"Summary: {len(combined)} history rows | {len(snapshots)} snapshot rows | "
        f"{source_counts['api']} API fetches | {source_counts['cache']} cache hits | "
        f"elapsed={_format_elapsed(time.monotonic() - run_started)}"
    )


if __name__ == "__main__":
    main()
