#!/usr/bin/env -S uv run python
"""Download crypto OHLCV via freqtrade, merge into price-data/, update metadata.

Usage examples:
  # Append new data since last known end date:
  uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT ETH/USDT --timeframes 1d 4h

  # Full download (ignore existing data, download from --since):
  uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT --timeframes 1d --full --since 2020-01-01

  # Download all pairs already in the catalog (append mode):
  uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d

  # Download new pair not yet in catalog:
  uv run python tools/download_crypto.py --exchange binance --pairs SOL/USDT --timeframes 1d 4h 1h --full

  # Prepend older data:
  uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT --timeframes 1d --prepend --since 2017-01-01
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    CRYPTO_EXCHANGES,
    FREQTRADE_BIN,
    DATA_ROOT,
    REPO_ROOT,
    catalog_key,
    crypto_file_path,
    ft_timeframe,
    load_crypto_catalog,
    normalize_ohlcv,
    pair_to_symbol,
    read_parquet_range,
    save_crypto_catalog,
    update_catalog_entry,
)


def _date_to_yyyymmdd(date_str: str) -> str:
    """'2024-01-15' → '20240115'"""
    return date_str.replace("-", "")


def _ms_to_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def merge_parquet(src: Path, dst: Path) -> tuple[int, int]:
    """Merge src into dst. Normalizes both to canonical schema before merging.
    Returns (old_rows, new_rows). Canonical schema: [timestamp(int64ms), open, high, low, close, volume]."""
    new_df = normalize_ohlcv(pd.read_parquet(src))
    if dst.exists():
        old_df = normalize_ohlcv(pd.read_parquet(dst))
        old_rows = len(old_df)
        merged = (
            pd.concat([old_df, new_df])
            .drop_duplicates(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
    else:
        dst.parent.mkdir(parents=True, exist_ok=True)
        old_rows = 0
        merged = new_df

    merged.to_parquet(dst, index=False)
    return old_rows, len(merged)


def build_timerange(
    exchange: str,
    symbol: str,
    timeframe: str,
    catalog: dict,
    full: bool,
    since: str | None,
    prepend: bool,
) -> str | None:
    """Compute --timerange argument for freqtrade.
    Returns None to download everything (no restriction).
    """
    if prepend:
        # Download from 'since' up to the earliest known date
        if since is None:
            print(f"  [warn] --prepend requires --since; skipping {symbol}/{timeframe}")
            return "SKIP"
        key = catalog_key(symbol, exchange)
        entry = catalog.get(key, {})
        td = entry.get("timeframe_dates", {}).get(timeframe, {})
        fp_str = td.get("file_path", "")
        existing_start: str | None = None
        if fp_str and fp_str.endswith(".parquet"):
            result = read_parquet_range(REPO_ROOT / fp_str)
            if result:
                existing_start = result[0]
        if existing_start is None:
            existing_start = td.get("start_date")
        end_part = _date_to_yyyymmdd(existing_start) if existing_start else ""
        start_part = _date_to_yyyymmdd(since)
        return f"{start_part}-{end_part}" if end_part else f"{start_part}-"

    if full:
        start = since or "20200101"
        return f"{_date_to_yyyymmdd(start)}-"

    # Append mode: start from the last known end date
    key = catalog_key(symbol, exchange)
    entry = catalog.get(key, {})
    td = entry.get("timeframe_dates", {}).get(timeframe, {})
    fp_str = td.get("file_path", "")

    last_date: str | None = None
    # 1. Try the path recorded in YAML (if it's parquet)
    if fp_str and fp_str.endswith(".parquet"):
        result = read_parquet_range(REPO_ROOT / fp_str)
        if result:
            last_date = result[1]
    # 2. Fall back to the canonical parquet path (handles legacy YAML with .json paths)
    if last_date is None:
        canonical = crypto_file_path(exchange, symbol, timeframe)
        result = read_parquet_range(canonical)
        if result:
            last_date = result[1]
    # 3. Fall back to YAML end_date, then --since, then default
    if last_date is None:
        last_date = td.get("end_date") or since

    if last_date:
        # Start one day before to ensure overlap (freqtrade may skip partial candles)
        end_dt = datetime.strptime(last_date, "%Y-%m-%d") - timedelta(days=1)
        return f"{end_dt.strftime('%Y%m%d')}-"

    # No existing data → download everything from 2020
    fallback = since or "2020-01-01"
    return f"{_date_to_yyyymmdd(fallback)}-"


def run_freqtrade_download(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    datadir: Path,
    timerange: str | None,
    prepend: bool = False,
    trading_mode: str = "spot",
    log_file: Path | None = None,
) -> bool:
    """Run freqtrade download-data. Returns True on success.
    If log_file is given, freqtrade output is written there instead of stdout."""
    if not FREQTRADE_BIN.exists():
        print(f"ERROR: freqtrade not found at {FREQTRADE_BIN}")
        print("Set FREQTRADE_BIN env var to the correct path.")
        return False

    ft_tfs = [ft_timeframe(tf) for tf in timeframes]

    # freqtrade requires a --userdir; place it alongside the data dir
    userdir = datadir / "userdir"
    userdir.mkdir(parents=True, exist_ok=True)

    cmd = [
        str(FREQTRADE_BIN),
        "download-data",
        "--exchange", exchange,
        "-p", *pairs,
        "-t", *ft_tfs,
        "-d", str(datadir),
        "--userdir", str(userdir),
        "--data-format-ohlcv", "parquet",
        "--trading-mode", trading_mode,
    ]
    if timerange:
        cmd += ["--timerange", timerange]
    if prepend:
        cmd += ["--prepend"]

    pairs_preview = " ".join(pairs[:3]) + ("…" if len(pairs) > 3 else "")
    print(f"  [{timerange or 'all'}] {len(pairs)} pairs ({pairs_preview}), tfs={','.join(ft_tfs)}")

    if log_file:
        with open(log_file, "a") as lf:
            result = subprocess.run(cmd, stdout=lf, stderr=lf, text=True)
    else:
        result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode == 0


def process_downloads(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    datadir: Path,
    catalog: dict,
    dry_run: bool = False,
) -> list[tuple[str, str, int, int]]:
    """Move downloaded files into price-data/, merge with existing, return change log."""
    changes: list[tuple[str, str, int, int]] = []

    for pair in pairs:
        symbol = pair_to_symbol(pair)
        for tf in timeframes:
            ft_tf = ft_timeframe(tf)
            # freqtrade stores flat in the datadir root: {datadir}/{symbol}-{ft_tf}.parquet
            src = datadir / f"{symbol}-{ft_tf}.parquet"
            if not src.exists():
                print(f"  [warn] expected {src.name} not found in download output")
                continue

            dst = crypto_file_path(exchange, symbol, tf)

            if dry_run:
                print(f"  [dry-run] would merge {src.name} → {dst}")
                continue

            old_rows, new_rows = merge_parquet(src, dst)
            added = new_rows - old_rows
            changes.append((symbol, tf, old_rows, new_rows))
            print(f"  {symbol}/{tf}: {old_rows}→{new_rows} rows (+{added})")

            # Update catalog metadata from actual merged file
            result = read_parquet_range(dst)
            if result:
                start, end, _ = result
                update_catalog_entry(catalog, exchange, symbol, tf, dst, start, end)

    return changes


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", required=True, choices=CRYPTO_EXCHANGES)
    p.add_argument("--pairs", nargs="+", metavar="PAIR",
                   help="Pairs in BTC/USDT format (space-separated)")
    p.add_argument("--all-pairs", action="store_true",
                   help="Download all pairs already in the catalog")
    p.add_argument("--timeframes", "-t", nargs="+", default=["1d"],
                   help="Timeframes to download (default: 1d)")
    p.add_argument("--full", action="store_true",
                   help="Ignore existing data, download from --since")
    p.add_argument("--since", metavar="DATE",
                   help="Start date for full downloads (YYYY-MM-DD, default: 2020-01-01)")
    p.add_argument("--prepend", action="store_true",
                   help="Download older data and prepend (requires --since)")
    p.add_argument("--trading-mode", default="spot", choices=["spot", "futures"],
                   help="Trading mode (default: spot)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen without downloading or writing")
    p.add_argument("--log-file", metavar="PATH",
                   help="Write freqtrade output to this file (default: auto when >10 pairs)")
    args = p.parse_args()

    if not args.pairs and not args.all_pairs:
        p.error("Specify --pairs or --all-pairs")

    catalog = load_crypto_catalog(args.exchange)

    if args.all_pairs:
        pairs = [entry.get("name", k).replace("_", "/", 1)
                 for k, entry in catalog.items()]
    else:
        pairs = args.pairs

    timeframes = args.timeframes

    # Auto-redirect freqtrade output to log file for bulk runs
    import datetime as _dt
    log_file: Path | None = None
    if args.log_file:
        log_file = Path(args.log_file)
    elif len(pairs) > 10:
        log_file = Path(f"/tmp/ft_download_{_dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    print(f"\nDownloading {len(pairs)} pair(s) × {len(timeframes)} timeframe(s) from {args.exchange}")
    print(f"Pairs   : {', '.join(pairs[:5])}{'…' if len(pairs) > 5 else ''}")
    print(f"TFs     : {', '.join(timeframes)}")
    print(f"Mode    : {'full from ' + (args.since or '2020-01-01') if args.full else 'prepend since ' + str(args.since) if args.prepend else 'append'}")
    if log_file:
        print(f"FT log  : {log_file}  (tail -f to follow)")

    # Group pairs by computed timerange so we can batch freqtrade calls
    # For simplicity: compute the minimum start across all pairs per timeframe
    # and run one freqtrade call per timeframe (or one per pair if ranges differ significantly)
    # Pragmatic approach: run per-batch of pairs that share the same timerange

    if args.dry_run:
        print("\n[dry-run mode — no files will be changed]\n")

    with tempfile.TemporaryDirectory(prefix="ai_dm_ft_") as tmpdir:
        tmppath = Path(tmpdir)

        if args.full or args.prepend:
            # Single timerange for all pairs
            # Use the most common case: all pairs same range
            tr_example = build_timerange(
                args.exchange,
                pair_to_symbol(pairs[0]),
                timeframes[0],
                catalog,
                args.full,
                args.since,
                args.prepend,
            )
            if tr_example == "SKIP":
                print("Nothing to do.")
                return
            print(f"Timerange: {tr_example}\n")
            if not args.dry_run:
                ok = run_freqtrade_download(
                    args.exchange, pairs, timeframes, tmppath,
                    tr_example, prepend=args.prepend,
                    trading_mode=args.trading_mode, log_file=log_file,
                )
                if not ok:
                    print(f"\nfreqtrade exited with an error. Check {log_file or 'output above'}.")
                    sys.exit(1)
        else:
            # Append mode: compute per-pair start dates and group into batches
            # Group pairs by their timerange to minimise freqtrade invocations
            from collections import defaultdict
            tr_groups: dict[str, list[str]] = defaultdict(list)
            for pair in pairs:
                sym = pair_to_symbol(pair)
                # Use the earliest start across all requested timeframes
                earliest: str | None = None
                for tf in timeframes:
                    tr = build_timerange(args.exchange, sym, tf, catalog, False, args.since, False)
                    if tr and tr != "SKIP":
                        if earliest is None or tr < earliest:
                            earliest = tr
                tr_groups[earliest or "20200101-"].append(pair)

            print(f"Batches: {len(tr_groups)} distinct timerange(s)\n")
            for tr, batch_pairs in tr_groups.items():
                print(f"  [{tr}] {len(batch_pairs)} pairs")
                if not args.dry_run:
                    ok = run_freqtrade_download(
                        args.exchange, batch_pairs, timeframes, tmppath,
                        tr, trading_mode=args.trading_mode, log_file=log_file,
                    )
                    if not ok:
                        print(f"\nfreqtrade failed for batch [{tr}]. Continuing with others…")

        if args.dry_run:
            # Simulate: just list what would be merged
            for pair in pairs:
                sym = pair_to_symbol(pair)
                for tf in timeframes:
                    dst = crypto_file_path(args.exchange, sym, tf)
                    print(f"  [dry-run] {sym}/{tf} → {dst.relative_to(REPO_ROOT)}")
            return

        print("\nMerging downloaded files into price-data/…")
        changes = process_downloads(
            args.exchange, pairs, timeframes, tmppath, catalog, dry_run=False
        )

    if changes:
        print(f"\nSaving updated catalog to price-data/crypto_{args.exchange}.yaml…")
        save_crypto_catalog(args.exchange, catalog)
        print(f"Done. {len(changes)} file(s) updated.")
    else:
        print("\nNo changes (files may not have downloaded — check freqtrade output above).")


if __name__ == "__main__":
    main()
