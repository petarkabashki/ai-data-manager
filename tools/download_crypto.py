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
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    CANONICAL_COLS,
    CRYPTO_EXCHANGES,
    FREQTRADE_BIN,
    REPO_ROOT,
    TF_TO_MS,
    catalog_key,
    crypto_file_path,
    crypto_futures_file_path,
    empty_ohlcv_frame,
    ft_file_timeframe,
    ft_timeframe,
    futures_pair_to_ft_symbol,
    futures_pair_to_symbol,
    load_crypto_catalog,
    load_crypto_futures_catalog,
    merge_ohlcv_parquet,
    pair_to_symbol,
    read_parquet_range,
    save_crypto_catalog,
    save_crypto_futures_catalog,
    update_catalog_entry,
)
from utils.config import TMP_ROOT, relative_to_repo
from utils.crypto_universe import (
    EARLIEST,
    ccxt_exchange,
    listed_markets,
    listing_date,
    mark_retired,
)

DAY_MS = 86_400_000

# Series downloaded with ccxt instead of freqtrade, by (exchange, trading mode):
# "*" for every timeframe, else the set. Each needs paging freqtrade doesn't do:
#   kucoin futures — freqtrade doesn't support them at all;
#   bitget futures — requests at 1d and above are capped at a 90-day window;
#   bitget spot 3d/1w/1Mo — bitget serves 100 bars a page there, freqtrade
#     assumes more and stops after the first page (JST 1w ended 2023-11-20).
CCXT_ROUTES: dict[tuple[str, str], str | frozenset[str]] = {
    ("kucoin", "futures"): "*",
    ("bitget", "futures"): "*",
    ("bitget", "spot"): frozenset({"3d", "1w", "1Mo"}),
}

# (page limit, max request window in ms or None) for the ccxt path. bitget
# answers a since/until window with the bars *nearest until*, so its windows
# are sized to one page and the cursor walks forward window by window.
CCXT_PAGING: dict[tuple[str, str], tuple[int, int | None]] = {
    ("kucoin", "futures"): (200, None),
    ("bitget", "futures"): (200, 90 * DAY_MS),
    ("bitget", "spot"): (100, 365 * 10 * DAY_MS),
    ("binance", "futures"): (1000, None),
    ("bybit", "futures"): (1000, None),
}

# freqtrade runs this many pairs at a time and merges before the next batch,
# so a backfill interrupted by a restart keeps every finished batch.
FT_BATCH_PAIRS = 50


def freqtrade_can(trading_mode: str, pair: str) -> bool:
    """freqtrade's futures mode handles USDT-margined perpetuals only; USDC,
    USD1 and coin-margined (inverse) ones go through ccxt whole."""
    return trading_mode == "spot" or pair.endswith(":USDT")


def ccxt_timeframes(exchange: str, trading_mode: str, timeframes: list[str]) -> list[str]:
    """The requested timeframes that go through ccxt rather than freqtrade."""
    route = CCXT_ROUTES.get((exchange, trading_mode))
    if route is None:
        return []
    return list(timeframes) if route == "*" else [tf for tf in timeframes if tf in route]


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
    default_start: str | None = None,
) -> str | None:
    """Compute --timerange argument for freqtrade.

    A series with no data starts at `default_start` (the pair's listing) or,
    failing that, EARLIEST — not 2020: kucoin and bitget return nothing for
    a window that ends before the pair existed.
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
        start = since or default_start or EARLIEST
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

    # No existing data → from the pair's listing (default_start), else EARLIEST
    fallback = default_start or EARLIEST
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


def _warn_zero_delta(zero_delta: list[str]) -> None:
    """A run that adds nothing looks identical to a successful one: the files
    are rewritten, the catalog is saved and the exit code is 0. Say so,
    because this is exactly how a truncating paginator hid for months."""
    if zero_delta:
        print(
            f"\n  [warn] {len(zero_delta)} series gained no rows "
            f"(already current, delisted, or the request window missed the data):"
        )
        print(f"    {', '.join(zero_delta[:12])}{' …' if len(zero_delta) > 12 else ''}")


def _is_empty_parquet(path: Path) -> bool:
    """True if the file carries no rows — or no columns at all, which is what a
    rejected download leaves behind."""
    try:
        import pyarrow.parquet as pq

        meta = pq.read_metadata(path)
        return meta.num_columns == 0 or meta.num_rows == 0
    except Exception:
        return True


class CatalogCheckpoint:
    """Save the catalog every `every_s` seconds while merging, not only at the end.

    A bulk run merges hundreds of files. Saving the catalog once at the end
    meant a crash, or a backup taken mid-merge, left files on disk that no
    catalog node pointed at. Call it after each pair; main still saves once
    more when the run ends.
    """

    def __init__(self, save: Callable[[], None] | None, every_s: float = 60.0):
        self.save, self.every_s = save, every_s
        self.last = time.monotonic()
        self.saves = 0

    def __call__(self) -> None:
        if self.save and time.monotonic() - self.last >= self.every_s:
            self.save()
            self.saves += 1
            self.last = time.monotonic()


def process_downloads(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    datadir: Path,
    catalog: dict,
    dry_run: bool = False,
    trading_mode: str = "spot",
    checkpoint: Callable[[], None] | None = None,
) -> list[tuple[str, str, int, int]]:
    """Move downloaded files into price-data/, merge with existing, return change log."""
    changes: list[tuple[str, str, int, int]] = []
    zero_delta: list[str] = []

    is_futures = trading_mode == "futures"
    # freqtrade writes futures files into a futures/ subdir
    src_dir = datadir / "futures" if is_futures else datadir

    for pair in pairs:
        symbol = futures_pair_to_symbol(pair) if is_futures else pair_to_symbol(pair)
        ft_sym = futures_pair_to_ft_symbol(pair) if is_futures else pair_to_symbol(pair)
        for tf in timeframes:
            src_suffix = "-futures" if is_futures else ""
            src = src_dir / f"{ft_sym}-{ft_file_timeframe(tf)}{src_suffix}.parquet"
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
        if checkpoint and not dry_run:
            checkpoint()

    _warn_zero_delta(zero_delta)
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
    ccxt_ex, pair: str, tf_ccxt: str, since_ms: int, tf_ms: int, limit: int = 200,
    max_window_ms: int | None = None,
) -> pd.DataFrame:
    """Paginate ccxt fetch_ohlcv from since_ms to now. Skips forward when API returns empty.

    A short page does *not* mean the end of the data. KuCoin returns fewer than
    `limit` candles mid-history for illiquid pairs, and treating that as the end
    truncated every series at exactly one page past `--since`: with
    `--since 2026-05-01` the 1h series stopped at 2026-05-09, 4h at 2026-06-03
    and 8h at 2026-07-06, each precisely `200 * tf` past the start. Advance on
    the last timestamp instead and stop only when the API stops making progress.

    With `max_window_ms` every request carries an explicit `until`, one page
    wide and never wider than the cap. bitget needs this: it rejects futures
    windows over 90 days at 1d and above, and it fills a window from the *until*
    end, so a window wider than a page skips the bars at its start.

    `tf_ms` for a monthly series is an upper bound (31 days); the cursor then
    advances by one millisecond past the last bar instead of by a bar length.
    """
    import time as _time

    all_rows: list = []
    current = since_ms
    now_ms = int(_time.time() * 1000)
    window = min(limit * tf_ms, max_window_ms) if max_window_ms else None
    skip_ms = window or max(limit * tf_ms, 30 * DAY_MS)
    step = 1 if tf_ccxt == "1M" else tf_ms

    while current < now_ms:
        # bitget's until is inclusive: a window one page wide plus its end bar
        # holds limit + 1 bars, and the first is the one dropped.
        params = {"until": current + window - 1} if window else {}
        rows = ccxt_ex.fetch_ohlcv(pair, tf_ccxt, since=current, limit=limit, params=params)
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
        if last_ts + step <= current:
            current += skip_ms
        else:
            current = last_ts + step
        _time.sleep(0.1)

    # Keep closed bars only, as freqtrade does: a still-forming bar would set
    # the catalog's end_date to a period that isn't over.
    if tf_ccxt == "1M":
        today = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        closed_before = int(datetime(today.year, today.month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    else:
        closed_before = now_ms - tf_ms + 1
    all_rows = [r for r in all_rows if r[0] < closed_before]
    if not all_rows:
        return empty_ohlcv_frame()
    df = pd.DataFrame(all_rows, columns=CANONICAL_COLS)
    df["timestamp"] = df["timestamp"].astype("int64")
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = df[c].astype("float64")
    return df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def download_and_merge_ccxt(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    catalog: dict,
    trading_mode: str = "futures",
    starts: dict[str, str] | None = None,
    since: str | None = None,
    dry_run: bool = False,
    full: bool = False,
    checkpoint: Callable[[], None] | None = None,
    ccxt_ex=None,
) -> list[tuple[str, str, int, int]]:
    """Download OHLCV with ccxt directly, for the series in CCXT_ROUTES.

    A series resumes from its last stored bar; one with no data starts at
    `since`, else at its pair's listing (`starts`), else EARLIEST.
    """
    import ccxt as _ccxt

    is_futures = trading_mode == "futures"
    ccxt_ex = ccxt_ex or ccxt_exchange(exchange, trading_mode)
    supported_tfs = set(ccxt_ex.timeframes.keys())
    limit, max_window = CCXT_PAGING.get((exchange, trading_mode), (200, None))
    starts = starts or {}

    changes: list[tuple[str, str, int, int]] = []
    zero_delta: list[str] = []

    for pair in pairs:
        symbol = futures_pair_to_symbol(pair) if is_futures else pair_to_symbol(pair)
        first = since or starts.get(pair) or EARLIEST
        first_ms = int(_ccxt.Exchange.parse8601(f"{first}T00:00:00Z"))
        for tf in timeframes:
            tf_ccxt = ft_timeframe(tf)
            if tf_ccxt not in supported_tfs:
                print(f"  [skip] {symbol}/{tf}: {tf_ccxt} not supported by {ccxt_ex.id}")
                continue

            tf_ms = TF_TO_MS.get(tf) or (31 * DAY_MS if tf_ccxt == "1M" else None)
            if tf_ms is None:
                print(f"  [skip] {symbol}/{tf}: variable-length timeframe not supported for ccxt pagination")
                continue

            dst = (crypto_futures_file_path if is_futures else crypto_file_path)(exchange, symbol, tf)

            if dry_run:
                print(f"  [dry-run] {symbol}/{tf} from {first} → {relative_to_repo(dst)}")
                continue

            # Resume from what is already stored, like the freqtrade path does.
            # Using the global --since for every series meant a refresh always
            # refetched from 2020 and, combined with the short-page break above,
            # silently returned only data the file already had.
            start_ms = first_ms
            last_stored = _last_stored_timestamp(dst)
            if last_stored is not None and not full:
                start_ms = max(first_ms, last_stored - tf_ms)

            try:
                df = _fetch_ohlcv_paginated(ccxt_ex, pair, tf_ccxt, start_ms, tf_ms,
                                            limit=limit, max_window_ms=max_window)
            except _ccxt.BadSymbol:
                print(f"  [skip] {symbol}: no longer listed on {ccxt_ex.id}")
                break
            except _ccxt.BaseError as e:
                print(f"  [warn] {symbol}/{tf}: {type(e).__name__}: {str(e)[:200]}")
                continue
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
        if checkpoint and not dry_run:
            checkpoint()

    _warn_zero_delta(zero_delta)
    return changes


def _symbol(pair: str, is_futures: bool) -> str:
    return futures_pair_to_symbol(pair) if is_futures else pair_to_symbol(pair)


def _year_start(date: str) -> str:
    """Round a start down to 1 January, so freqtrade batches by year, not by day."""
    return f"{date[:4]}-01-01"


def listing_starts(exchange: str, trading_mode: str, pairs: list[str], timeframes: list[str],
                   catalog: dict, markets: dict[str, dict] | None, ccxt_ex=None) -> dict[str, str]:
    """Where each pair with a missing series should start: its listing date.

    Only pairs missing at least one requested timeframe need one. A pair
    already in the catalog starts where its earliest series does — no request;
    a new pair asks the venue (published listing time, else first monthly bar).
    """
    is_futures = trading_mode == "futures"
    starts: dict[str, str] = {}
    to_ask: list[str] = []
    for pair in pairs:
        entry = catalog.get(catalog_key(_symbol(pair, is_futures), exchange)) or {}
        have = set((entry.get("timeframe_dates") or {}))
        if all(tf in have for tf in timeframes):
            continue
        if entry.get("start_date"):
            starts[pair] = entry["start_date"]
        else:
            to_ask.append(pair)
    if to_ask:
        ccxt_ex = ccxt_ex or ccxt_exchange(exchange, trading_mode)
        if markets is None:
            markets = ccxt_ex.load_markets()
        print(f"Looking up listing dates for {len(to_ask)} new pair(s)…")
        for pair in to_ask:
            market = markets.get(pair)
            date = listing_date(ccxt_ex, market) if market else None
            if date:
                starts[pair] = max(date, EARLIEST)
    return starts


def select_pairs(args, catalog: dict, is_futures: bool, ccxt_ex=None) -> tuple[list[str], dict | None]:
    """The pairs to download, and the venue's markets when they were loaded.

    `--universe` also marks catalog pairs the venue no longer lists as retired
    (and clears the mark on a relisting); `--all-pairs` skips retired pairs.
    """
    if args.universe:
        ccxt_ex = ccxt_ex or ccxt_exchange(args.exchange, args.trading_mode)
        markets = listed_markets(ccxt_ex, args.exchange, args.trading_mode)
        # Two contracts must not share a storage name (the settlement suffix is
        # dropped: BTC/USD:BTC is BTC_USD). Keep the linear one if they would.
        by_name: dict[str, str] = {}
        for pair in sorted(markets, key=lambda p: not markets[p].get("linear")):
            name = _symbol(pair, is_futures)
            if name in by_name:
                print(f"  [skip] {pair}: stores as {name}, already taken by {by_name[name]}")
                continue
            by_name[name] = pair
        markets = {pair: markets[pair] for pair in by_name.values()}
        names = set(by_name)
        known = {e.get("name") for e in catalog.values() if isinstance(e, dict)}
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        retired, relisted = mark_retired(catalog, names, args.exchange, today)
        print(f"Universe: {len(markets)} listed · {len(names & known)} in the catalog · "
              f"{len(names - known)} new · {len(retired)} newly retired · {len(relisted)} relisted")
        if retired:
            print(f"Retired : {', '.join(retired[:20])}{'…' if len(retired) > 20 else ''}")
        return sorted(markets), markets

    if args.all_pairs:
        live = [(k, e) for k, e in catalog.items() if isinstance(e, dict) and not e.get("retired")]
        if is_futures:
            # common_name stores the full futures pair (BTC/USDT:USDT)
            return [
                e.get("common_name") or futures_pair_to_symbol(e.get("name", k)).replace("_", "/", 1) + ":USDT"
                for k, e in live
            ], None
        return [e.get("name", k).replace("_", "/", 1) for k, e in live], None

    # Futures pairs carry a settlement suffix (BTC/USDT:USDT). `--all-pairs`
    # gets it from `common_name`; an explicit `--pairs BTC/USDT` used to be
    # passed through verbatim and every pair came back "no longer listed",
    # which reads like a delisting rather than a wrong symbol. Append it.
    return [
        f"{pair}:{pair.split('/')[-1]}" if is_futures and ":" not in pair else pair
        for pair in args.pairs
    ], None


def download_with_freqtrade(args, pairs: list[str], timeframes: list[str], catalog: dict,
                            starts: dict[str, str], checkpoint: Callable[[], None]) -> tuple[list, int]:
    """The freqtrade path: batches by timerange, FT_BATCH_PAIRS pairs at a time,
    each merged before the next so an interrupted backfill keeps its progress.

    Returns (changes, number of batches freqtrade failed on)."""
    is_futures = args.trading_mode == "futures"

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

    from collections import defaultdict
    tr_groups: dict[str, list[str]] = defaultdict(list)
    for pair in pairs:
        sym = _symbol(pair, is_futures)
        default_start = _year_start(starts[pair]) if pair in starts else None
        earliest: str | None = None
        for tf in timeframes:
            tr = build_timerange(args.exchange, sym, tf, catalog, args.full, args.since, args.prepend,
                                 trading_mode=args.trading_mode, default_start=default_start)
            if tr and tr != "SKIP":
                if earliest is None or tr < earliest:
                    earliest = tr
        if earliest:
            tr_groups[earliest].append(pair)
    if not tr_groups:
        print("Nothing to do.")
        return [], 0

    print(f"Batches: {len(tr_groups)} distinct timerange(s)\n")
    changes: list = []
    failed = 0
    for tr, group in sorted(tr_groups.items()):
        print(f"  [{tr}] {len(group)} pairs")
        if args.dry_run:
            for pair in group:
                for tf in timeframes:
                    sym = _symbol(pair, is_futures)
                    dst = (crypto_futures_file_path if is_futures else crypto_file_path)(args.exchange, sym, tf)
                    print(f"  [dry-run] {sym}/{tf} → {relative_to_repo(dst)}")
            continue
        for i in range(0, len(group), FT_BATCH_PAIRS):
            batch = group[i:i + FT_BATCH_PAIRS]
            with tempfile.TemporaryDirectory(prefix="ai_dm_ft_") as tmpdir:
                tmppath = Path(tmpdir)
                ok = run_freqtrade_download(
                    args.exchange, batch, timeframes, tmppath, tr, prepend=args.prepend,
                    trading_mode=args.trading_mode, log_file=log_file,
                )
                if not ok:
                    failed += 1
                    print(f"\nfreqtrade failed for batch [{tr}] {batch[0]}…; merging what it wrote.")
                changes += process_downloads(
                    args.exchange, batch, timeframes, tmppath, catalog,
                    dry_run=False, trading_mode=args.trading_mode, checkpoint=checkpoint,
                )
    return changes, failed


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", required=True, choices=CRYPTO_EXCHANGES)
    p.add_argument("--pairs", nargs="+", metavar="PAIR",
                   help="Pairs in BTC/USDT format (space-separated)")
    p.add_argument("--all-pairs", action="store_true",
                   help="Download all pairs already in the catalog, except retired ones")
    p.add_argument("--universe", action="store_true",
                   help="Download every USDT pair the exchange lists (spot) or every USDT perpetual "
                        "(futures), new ones from their listing; mark catalog pairs it no longer "
                        "lists as retired")
    p.add_argument("--timeframes", "-t", nargs="+", default=["1d"],
                   help="Timeframes to download (default: 1d)")
    p.add_argument("--full", action="store_true",
                   help="Ignore existing data, download from --since (default: each pair's listing)")
    p.add_argument("--since", metavar="DATE",
                   help="Start date for new series and full downloads (YYYY-MM-DD, default: the pair's listing)")
    p.add_argument("--prepend", action="store_true",
                   help="Download older data and prepend (requires --since)")
    p.add_argument("--trading-mode", default="spot", choices=["spot", "futures"],
                   help="Trading mode (default: spot)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would happen without downloading or writing")
    p.add_argument("--log-file", metavar="PATH",
                   help="Write freqtrade output to this file (default: auto when >10 pairs)")
    args = p.parse_args()

    if sum(map(bool, (args.pairs, args.all_pairs, args.universe))) != 1:
        p.error("Specify exactly one of --pairs, --all-pairs or --universe")

    is_futures = args.trading_mode == "futures"
    catalog = (
        load_crypto_futures_catalog(args.exchange)
        if is_futures
        else load_crypto_catalog(args.exchange)
    )

    timeframes = args.timeframes
    ccxt_tfs = ccxt_timeframes(args.exchange, args.trading_mode, timeframes)
    ft_tfs = [tf for tf in timeframes if tf not in ccxt_tfs]
    ccxt_ex = ccxt_exchange(args.exchange, args.trading_mode)   # no network until used

    pairs, markets = select_pairs(args, catalog, is_futures, ccxt_ex)
    starts = {} if (args.since or args.prepend) else listing_starts(
        args.exchange, args.trading_mode, pairs, timeframes, catalog, markets, ccxt_ex)

    print(f"\nDownloading {len(pairs)} pair(s) × {len(timeframes)} timeframe(s) from {args.exchange}")
    print(f"Pairs   : {', '.join(pairs[:5])}{'…' if len(pairs) > 5 else ''}")
    print(f"TFs     : {', '.join(timeframes)}")
    print(f"Mode    : {'full' if args.full else 'prepend since ' + str(args.since) if args.prepend else 'append'}"
          f"{'; new series from ' + args.since if args.since and not args.prepend else '; new series from listing' if starts else ''}")
    if ccxt_tfs:
        print(f"Backend : ccxt/{ccxt_ex.id} for {', '.join(ccxt_tfs)}"
              f"{'; freqtrade for ' + ', '.join(ft_tfs) if ft_tfs else ''}")

    if args.dry_run:
        print("\n[dry-run mode — no files will be changed]\n")

    def save_catalog() -> None:
        (save_crypto_futures_catalog if is_futures else save_crypto_catalog)(args.exchange, catalog)

    checkpoint = CatalogCheckpoint(save_catalog)

    # (pairs, timeframes) per backend: freqtrade takes what it can page; ccxt
    # takes the routed timeframes for every pair and whole pairs freqtrade can't.
    ft_pairs = [p for p in pairs if freqtrade_can(args.trading_mode, p)]
    ccxt_jobs = [(pairs, ccxt_tfs), ([p for p in pairs if p not in set(ft_pairs)], ft_tfs)]

    changes: list = []
    failed = 0
    if ft_tfs and ft_pairs:
        if args.prepend and args.since is None:
            print("--prepend requires --since. Nothing to do.")
            return
        ft_changes, failed = download_with_freqtrade(args, ft_pairs, ft_tfs, catalog, starts, checkpoint)
        changes += ft_changes
    for job_pairs, job_tfs in ccxt_jobs:
        if not (job_pairs and job_tfs):
            continue
        if args.prepend:
            print(f"  [warn] --prepend is not supported on the ccxt path; skipping {len(job_pairs)} pair(s)")
            continue
        changes += download_and_merge_ccxt(
            args.exchange, job_pairs, job_tfs, catalog, trading_mode=args.trading_mode,
            starts=starts, since=args.since, dry_run=args.dry_run, full=args.full,
            checkpoint=checkpoint, ccxt_ex=ccxt_ex,
        )
    if args.dry_run:
        return

    catalog_name = f"crypto_futures_{args.exchange}.yaml" if is_futures else f"crypto_{args.exchange}.yaml"
    if changes or args.universe:
        print(f"\nSaving updated catalog to price-data/{catalog_name}…")
        save_catalog()
        print(f"Done. {len(changes)} file(s) updated.")
    else:
        print("\nNo changes.")
    if failed:
        print(f"freqtrade failed on {failed} batch(es); see {args.log_file or 'the FT log above'}.")
        sys.exit(1)


if __name__ == "__main__":
    main()
