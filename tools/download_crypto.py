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
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import TMP_ROOT, relative_to_repo
from utils.catalog import (
    CANONICAL_COLS,
    CRYPTO_EXCHANGES,
    FREQTRADE_BIN,
    TF_TO_MS,
    REPO_ROOT,
    catalog_key,
    crypto_file_path,
    crypto_futures_file_path,
    ft_timeframe,
    futures_pair_to_ft_symbol,
    futures_pair_to_symbol,
    empty_ohlcv_frame,
    load_crypto_catalog,
    load_crypto_futures_catalog,
    merge_ohlcv_parquet,
    normalize_ohlcv,
    pair_to_symbol,
    read_parquet_range,
    save_crypto_catalog,
    save_crypto_futures_catalog,
    update_catalog_entry,
)

# Exchanges where freqtrade doesn't support futures — use ccxt directly instead
CCXT_FUTURES_CLASS: dict[str, str] = {
    "kucoin": "kucoinfutures",
}


def _date_to_yyyymmdd(date_str: str) -> str:
    """'2024-01-15' → '20240115'"""
    return date_str.replace("-", "")


def _ms_to_date(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def build_timerange(
    exchange: str,
    symbol: str,
    timeframe: str,
    catalog: dict,
    full: bool,
    since: str | None,
    prepend: bool,
    trading_mode: str = "spot",
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
        canonical = (
            crypto_futures_file_path(exchange, symbol, timeframe)
            if trading_mode == "futures"
            else crypto_file_path(exchange, symbol, timeframe)
        )
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


def _is_empty_parquet(path: Path) -> bool:
    """True if the file carries no rows — or no columns at all, which is what a
    rejected download leaves behind."""
    try:
        import pyarrow.parquet as pq

        meta = pq.read_metadata(path)
        return meta.num_columns == 0 or meta.num_rows == 0
    except Exception:
        return True


def process_downloads(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    datadir: Path,
    catalog: dict,
    dry_run: bool = False,
    trading_mode: str = "spot",
) -> list[tuple[str, str, int, int]]:
    """Move downloaded files into price-data/, merge with existing, return change log."""
    changes: list[tuple[str, str, int, int]] = []

    is_futures = trading_mode == "futures"
    # freqtrade writes futures files into a futures/ subdir
    src_dir = datadir / "futures" if is_futures else datadir

    for pair in pairs:
        symbol = futures_pair_to_symbol(pair) if is_futures else pair_to_symbol(pair)
        ft_sym = futures_pair_to_ft_symbol(pair) if is_futures else pair_to_symbol(pair)
        for tf in timeframes:
            ft_tf = ft_timeframe(tf)
            src_suffix = "-futures" if is_futures else ""
            src = src_dir / f"{ft_sym}-{ft_tf}{src_suffix}.parquet"
            if not src.exists():
                print(f"  [warn] expected {src.name} not found in download output")
                continue

            dst = (
                crypto_futures_file_path(exchange, symbol, tf)
                if is_futures
                else crypto_file_path(exchange, symbol, tf)
            )

            if dry_run:
                print(f"  [dry-run] would merge {src.name} → {dst}")
                continue

            # An exchange that rejects a timeframe it advertises (bitget serves
            # no 2h, despite listing it) hands freqtrade back a zero-column
            # parquet. Merging that used to create an empty destination file and
            # a dateless catalog node — a series that looks real and holds
            # nothing. Leave any existing data alone and say so instead.
            if _is_empty_parquet(src):
                print(f"  [skip] {symbol}/{tf}: {exchange} returned no data")
                continue

            old_rows, new_rows = merge_ohlcv_parquet(src, dst)
            added = new_rows - old_rows
            if added == 0:
                zero_delta.append(f"{symbol}/{tf}")
            changes.append((symbol, tf, old_rows, new_rows))
            print(f"  {symbol}/{tf}: {old_rows}→{new_rows} rows (+{added})")

            result = read_parquet_range(dst)
            if result:
                start, end, _ = result
                update_catalog_entry(
                    catalog, exchange, symbol, tf, dst, start, end,
                    categories=["Crypto Futures"] if is_futures else None,
                    common_name=pair if is_futures else None,
                )

    return changes


def _last_stored_timestamp(path) -> int | None:
    """Last bar already on disk, or None when there is no usable file."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path, columns=["timestamp"])
    except Exception:
        return None
    if df.empty:
        return None
    return int(df["timestamp"].max())


def _fetch_ohlcv_paginated(
    ccxt_ex, pair: str, tf_ccxt: str, since_ms: int, tf_ms: int, limit: int = 200
) -> pd.DataFrame:
    """Paginate ccxt fetch_ohlcv from since_ms to now. Skips forward when API returns empty.

    A short page does *not* mean the end of the data. KuCoin returns fewer than
    `limit` candles mid-history for illiquid pairs, and treating that as the end
    truncated every series at exactly one page past `--since`: with
    `--since 2026-05-01` the 1h series stopped at 2026-05-09, 4h at 2026-06-03
    and 8h at 2026-07-06, each precisely `200 * tf` past the start. Advance on
    the last timestamp instead and stop only when the API stops making progress.
    """
    import time as _time

    all_rows: list = []
    current = since_ms
    now_ms = int(_time.time() * 1000)
    skip_ms = max(limit * tf_ms, 30 * 86_400_000)

    while current < now_ms:
        rows = ccxt_ex.fetch_ohlcv(pair, tf_ccxt, since=current, limit=limit)
        if not rows:
            current += skip_ms
            _time.sleep(0.1)
            continue
        all_rows.extend(rows)
        last_ts = rows[-1][0]
        if last_ts >= now_ms - tf_ms:
            break
        # Guard against an exchange that keeps returning the same page: without
        # this a non-advancing cursor would spin forever.
        if last_ts + tf_ms <= current:
            current += skip_ms
        else:
            current = last_ts + tf_ms
        _time.sleep(0.1)

    if not all_rows:
        return empty_ohlcv_frame()
    df = pd.DataFrame(all_rows, columns=CANONICAL_COLS)
    df["timestamp"] = df["timestamp"].astype("int64")
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype("float64")
    return df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def download_and_merge_ccxt_futures(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    catalog: dict,
    since: str = "2020-01-01",
    dry_run: bool = False,
    full: bool = False,
) -> list[tuple[str, str, int, int]]:
    """Download futures OHLCV via ccxt directly (for exchanges freqtrade doesn't support in futures mode)."""
    import ccxt as _ccxt

    ccxt_class_name = CCXT_FUTURES_CLASS[exchange]
    ccxt_ex: _ccxt.Exchange = getattr(_ccxt, ccxt_class_name)()
    supported_tfs = set(ccxt_ex.timeframes.keys())
    since_ms = int(_ccxt.Exchange.parse8601(f"{since}T00:00:00Z"))

    changes: list[tuple[str, str, int, int]] = []
    zero_delta: list[str] = []

    for pair in pairs:
        symbol = futures_pair_to_symbol(pair)
        for tf in timeframes:
            tf_ccxt = ft_timeframe(tf)
            if tf_ccxt not in supported_tfs:
                print(f"  [skip] {symbol}/{tf}: {tf_ccxt} not supported by {ccxt_class_name}")
                continue

            tf_ms = TF_TO_MS.get(tf)
            if tf_ms is None:
                print(f"  [skip] {symbol}/{tf}: variable-length timeframe not supported for ccxt pagination")
                continue

            dst = crypto_futures_file_path(exchange, symbol, tf)

            if dry_run:
                print(f"  [dry-run] {symbol}/{tf} → {relative_to_repo(dst)}")
                continue

            # Resume from what is already stored, like the freqtrade path does.
            # Using the global --since for every series meant a refresh always
            # refetched from 2020 and, combined with the short-page break above,
            # silently returned only data the file already had.
            start_ms = since_ms
            last_stored = _last_stored_timestamp(dst)
            if last_stored is not None and not full:
                start_ms = max(since_ms, last_stored - tf_ms)

            try:
                df = _fetch_ohlcv_paginated(ccxt_ex, pair, tf_ccxt, start_ms, tf_ms)
            except _ccxt.BadSymbol:
                print(f"  [skip] {symbol}: no longer listed on {ccxt_class_name}")
                break
            if df.empty:
                print(f"  [warn] {symbol}/{tf}: no data returned")
                continue

            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                df.to_parquet(tmp_path, index=False)
                old_rows, new_rows = merge_ohlcv_parquet(tmp_path, dst)
            finally:
                tmp_path.unlink(missing_ok=True)

            added = new_rows - old_rows
            changes.append((symbol, tf, old_rows, new_rows))
            print(f"  {symbol}/{tf}: {old_rows}→{new_rows} rows (+{added})")

            result = read_parquet_range(dst)
            if result:
                start, end, _ = result
                update_catalog_entry(
                    catalog, exchange, symbol, tf, dst, start, end,
                    categories=["Crypto Futures"],
                    common_name=pair,
                )

    if zero_delta:
        # A run that adds nothing looks identical to a successful one: the files
        # are rewritten, the catalog is saved and the exit code is 0. Say so,
        # because this is exactly how a truncating paginator hid for months.
        print(
            f"\n  [warn] {len(zero_delta)} series gained no rows "
            f"(already current, delisted, or the request window missed the data):"
        )
        print(f"    {', '.join(zero_delta[:12])}{' …' if len(zero_delta) > 12 else ''}")

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

    is_futures = args.trading_mode == "futures"
    catalog = (
        load_crypto_futures_catalog(args.exchange)
        if is_futures
        else load_crypto_catalog(args.exchange)
    )

    if args.all_pairs:
        if is_futures:
            # common_name stores the full futures pair (BTC/USDT:USDT)
            pairs = [
                entry.get("common_name") or futures_pair_to_symbol(entry.get("name", k)).replace("_", "/", 1) + ":USDT"
                for k, entry in catalog.items()
            ]
        else:
            pairs = [entry.get("name", k).replace("_", "/", 1)
                     for k, entry in catalog.items()]
    else:
        # Futures pairs carry a settlement suffix (BTC/USDT:USDT). `--all-pairs`
        # gets it from `common_name`; an explicit `--pairs BTC/USDT` used to be
        # passed through verbatim and every pair came back "no longer listed",
        # which reads like a delisting rather than a wrong symbol. Append it.
        pairs = [
            f"{pair}:{pair.split('/')[-1]}" if is_futures and ":" not in pair else pair
            for pair in args.pairs
        ]

    timeframes = args.timeframes

    use_ccxt = is_futures and args.exchange in CCXT_FUTURES_CLASS
    since_str = args.since or "2020-01-01"

    print(f"\nDownloading {len(pairs)} pair(s) × {len(timeframes)} timeframe(s) from {args.exchange}")
    print(f"Pairs   : {', '.join(pairs[:5])}{'…' if len(pairs) > 5 else ''}")
    print(f"TFs     : {', '.join(timeframes)}")
    print(f"Mode    : {'full from ' + since_str if args.full else 'prepend since ' + str(args.since) if args.prepend else 'append'}")
    if use_ccxt:
        print(f"Backend : ccxt/{CCXT_FUTURES_CLASS[args.exchange]} (freqtrade does not support {args.exchange} futures)")

    if args.dry_run:
        print("\n[dry-run mode — no files will be changed]\n")

    if use_ccxt:
        changes = download_and_merge_ccxt_futures(
            args.exchange, pairs, timeframes, catalog,
            since=since_str, dry_run=args.dry_run, full=args.full,
        )
        if args.dry_run:
            return
    else:
        # Auto-redirect freqtrade output to a log file for bulk runs. Logs go
        # under TMP_ROOT (repo tmp/) like the IBKR checkpoints, not /tmp.
        log_file: Path | None = None
        if args.log_file:
            log_file = Path(args.log_file)
        elif len(pairs) > 10:
            TMP_ROOT.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
            log_file = TMP_ROOT / f"ft_download_{args.exchange}_{stamp}.log"

        if log_file:
            print(f"FT log  : {log_file}  (tail -f to follow)")

        with tempfile.TemporaryDirectory(prefix="ai_dm_ft_") as tmpdir:
            tmppath = Path(tmpdir)

            if args.full or args.prepend:
                sym0 = futures_pair_to_symbol(pairs[0]) if is_futures else pair_to_symbol(pairs[0])
                tr_example = build_timerange(
                    args.exchange, sym0, timeframes[0], catalog,
                    args.full, args.since, args.prepend, trading_mode=args.trading_mode,
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
                from collections import defaultdict
                tr_groups: dict[str, list[str]] = defaultdict(list)
                for pair in pairs:
                    sym = futures_pair_to_symbol(pair) if is_futures else pair_to_symbol(pair)
                    earliest: str | None = None
                    for tf in timeframes:
                        tr = build_timerange(args.exchange, sym, tf, catalog, False, args.since, False,
                                             trading_mode=args.trading_mode)
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
                for pair in pairs:
                    sym = futures_pair_to_symbol(pair) if is_futures else pair_to_symbol(pair)
                    for tf in timeframes:
                        dst = (
                            crypto_futures_file_path(args.exchange, sym, tf)
                            if is_futures
                            else crypto_file_path(args.exchange, sym, tf)
                        )
                        print(f"  [dry-run] {sym}/{tf} → {relative_to_repo(dst)}")
                return

            print("\nMerging downloaded files into price-data/…")
            changes = process_downloads(
                args.exchange, pairs, timeframes, tmppath, catalog,
                dry_run=False, trading_mode=args.trading_mode,
            )

    catalog_name = f"crypto_futures_{args.exchange}.yaml" if is_futures else f"crypto_{args.exchange}.yaml"
    if changes:
        print(f"\nSaving updated catalog to price-data/{catalog_name}…")
        if is_futures:
            save_crypto_futures_catalog(args.exchange, catalog)
        else:
            save_crypto_catalog(args.exchange, catalog)
        print(f"Done. {len(changes)} file(s) updated.")
    else:
        print("\nNo changes.")


if __name__ == "__main__":
    main()
