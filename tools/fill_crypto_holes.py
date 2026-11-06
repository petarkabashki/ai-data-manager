#!/usr/bin/env -S uv run python
"""Fill the internal holes of stored crypto series, and nothing else.

Holes are read from each parquet file, never from catalog dates. By default this
prints the plan and touches nothing, on disk or on the network; `--apply` fetches
only the hole windows through the paged ccxt path, keeps closed bars and merges.

What the exchange answers with nothing goes into the exchange-empty record
(`exchange_empty.yaml` at the data root, see `utils/exchange_empty.py`) and is
not requested again. An error is not an answer: that hole stays as it was, is
not recorded, and the run exits non-zero. The parquet files and the record are
the only state, so a repeat run resumes where the last one stopped.

Usage examples:
  # What is there to fill?
  uv run python tools/fill_crypto_holes.py --exchange bitget --timeframes 1h 4h 6h 12h 1d 3d 1w

  # Holes of 50 candles or more on two series, a 10-hole trial
  uv run python tools/fill_crypto_holes.py --exchange bitget --timeframes 1h --min-candles 50 \\
      --symbols DOGE_USDT CARV_USDT --limit 10 --apply
"""

from __future__ import annotations

import argparse
import math
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import ccxt
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from download_crypto import (  # noqa: E402
    CatalogCheckpoint,
    _fetch_ohlcv_paginated,
    _symbol,
    ccxt_paging,
    select_pairs,
)
from utils.calendars import CONTINUOUS  # noqa: E402
from utils.catalog import (  # noqa: E402
    CRYPTO_EXCHANGES,
    DATE_LABELLED_TIMEFRAMES,
    TF_TO_MS,
    catalog_key,
    crypto_file_path,
    crypto_futures_file_path,
    date_label_ms,
    ft_timeframe,
    load_crypto_catalog,
    load_crypto_futures_catalog,
    merge_ohlcv_parquet,
    read_parquet_range,
    save_crypto_catalog,
    save_crypto_futures_catalog,
    update_catalog_entry,
)
from utils.config import relative_to_repo  # noqa: E402
from utils.crypto_universe import ccxt_exchange  # noqa: E402
from utils.exchange_empty import ExchangeEmptyRecord, bar_runs, split_bars  # noqa: E402
from utils.gaps import detect_gaps  # noqa: E402
from utils.report import allow_broken_pipe, print_table  # noqa: E402

# Stop after this many errors in a row: the exchange is down or refusing us, and
# working through the rest of the plan would only be more refused requests.
MAX_CONSECUTIVE_ERRORS = 20


@dataclass
class Hole:
    """Missing bars start_ms..end_ms (open times, inclusive) of one series."""

    symbol: str
    pair: str
    timeframe: str
    path: Path
    start_ms: int
    end_ms: int
    candles: int

    def requests(self, exchange: str, trading_mode: str) -> int:
        limit, _ = ccxt_paging(exchange, trading_mode, self.timeframe)
        return math.ceil(self.candles / max(limit - 1, 1))  # history pages come one bar short


@dataclass
class FillResult:
    filled: int = 0
    partly: int = 0
    empty: int = 0
    failed: int = 0
    candles_added: int = 0
    candles_empty: int = 0
    unlisted: list[str] = field(default_factory=list)
    aborted: bool = False


def find_holes(
    exchange: str,
    trading_mode: str,
    timeframes: list[str],
    catalog: dict,
    record: ExchangeEmptyRecord,
    min_candles: int = 1,
    symbols: list[str] | None = None,
) -> list[Hole]:
    """Every hole in the selection that isn't on record, read from the parquet.

    Retired pairs are left out, as `--all-pairs` leaves them out: the exchange no
    longer lists them. `min_candles` applies to what is left of a hole once the
    recorded part is taken away.
    """
    is_futures = trading_mode == "futures"
    file_path = crypto_futures_file_path if is_futures else crypto_file_path
    selection = argparse.Namespace(universe=False, all_pairs=True)
    pairs, _ = select_pairs(selection, catalog, is_futures)

    holes: list[Hole] = []
    for pair in sorted(pairs):
        symbol = _symbol(pair, is_futures)
        if symbols and symbol not in symbols:
            continue
        for tf in timeframes:
            tf_ms = TF_TO_MS.get(tf)
            if tf_ms is None:
                continue
            path = file_path(exchange, symbol, tf)
            if not path.exists():
                continue
            if read_parquet_range(path) is None:
                print(f"  [warn] {symbol}/{tf}: unreadable, skipped")
                continue
            gaps, _ = detect_gaps(path, tf, CONTINUOUS)
            windows = record.windows(path)
            for gap in gaps:
                outside, _ = split_bars(gap.start_ms, gap.end_ms, tf_ms, windows)
                holes += [Hole(symbol, pair, tf, path, first, last, count)
                          for first, last, count in outside if count >= min_candles]
    return holes


def print_plan(holes: list[Hole], exchange: str, trading_mode: str, timeframes: list[str]) -> None:
    rows = []
    for tf in timeframes:
        group = [h for h in holes if h.timeframe == tf]
        rows.append([tf, str(len({h.symbol for h in group})), str(len(group)),
                     str(sum(h.candles for h in group)),
                     str(sum(h.requests(exchange, trading_mode) for h in group))])
    rows.append(["all", str(len({(h.symbol, h.timeframe) for h in holes})), str(len(holes)),
                 str(sum(h.candles for h in holes)),
                 str(sum(h.requests(exchange, trading_mode) for h in holes))])
    print_table(["TF", "Series", "Holes", "Candles", "Requests"], rows)


def fetch_hole(ccxt_ex, hole: Hole, exchange: str, trading_mode: str) -> pd.DataFrame:
    """The bars the exchange holds inside one hole, closed bars only.

    The request runs one bar past the hole. bitget's history endpoint leaves out
    a bar that closes after `until` (its recent endpoint keeps it), so a request
    ending on the hole's own last bar gets nothing for that bar, and an answer of
    nothing goes on the exchange-empty record for good. Seen live 2026-11-06: 299
    of 300 bars at 1h to 1d, and 1-candle holes empty, while a wider window held
    the same bar.
    """
    tf_ms = TF_TO_MS[hole.timeframe]
    limit, max_window = ccxt_paging(exchange, trading_mode, hole.timeframe)
    df = _fetch_ohlcv_paginated(
        ccxt_ex, hole.pair, ft_timeframe(hole.timeframe), hole.start_ms, tf_ms,
        limit=limit, max_window_ms=max_window, until_ms=hole.end_ms + 2 * tf_ms - 1,
    )
    inside = df["timestamp"].between(hole.start_ms, hole.end_ms + tf_ms - 1)
    return df[inside].reset_index(drop=True)


def _refresh_bounds(catalog: dict, exchange: str, hole: Hole, is_futures: bool) -> bool:
    """Update the catalog dates if the file's bounds differ. True if they did."""
    symbol, tf, path = hole.symbol, hole.timeframe, hole.path
    result = read_parquet_range(path)
    if result is None:
        return False
    start, end, _ = result
    node = (catalog.get(catalog_key(symbol, exchange)) or {}).get("timeframe_dates", {}).get(tf) or {}
    if (str(node.get("start_date")), str(node.get("end_date"))) == (start, end):
        return False
    update_catalog_entry(
        catalog, exchange, symbol, tf, path, start, end,
        categories=["Crypto Futures"] if is_futures else None,
        common_name=hole.pair if is_futures else None,
    )
    return True


def fill_holes(
    holes: list[Hole],
    exchange: str,
    trading_mode: str,
    catalog: dict,
    record: ExchangeEmptyRecord,
    ccxt_ex,
    checkpoint: Callable[[], None] | None = None,
    on_catalog_change: Callable[[], None] | None = None,
) -> FillResult:
    """Fetch, merge and record, one series at a time.

    A series is merged before its windows are recorded, so a crash can repeat
    work but can't leave a window on record that the file should have held.
    """
    result = FillResult()
    is_futures = trading_mode == "futures"
    consecutive_errors = 0
    series: dict[tuple[str, str], list[Hole]] = {}
    for hole in holes:
        series.setdefault((hole.symbol, hole.timeframe), []).append(hole)

    for (symbol, tf), group in series.items():
        tf_ms = TF_TO_MS[tf]
        frames: list[pd.DataFrame] = []
        empties: list[tuple[int, int, int]] = []
        for hole in group:
            try:
                df = fetch_hole(ccxt_ex, hole, exchange, trading_mode)
            except ccxt.BadSymbol:
                print(f"  [skip] {symbol}: no longer listed on {ccxt_ex.id}")
                result.unlisted.append(symbol)
                break
            except ccxt.BaseError as e:
                print(f"  [fail] {symbol}/{tf} {hole.candles} candles: {type(e).__name__}: {str(e)[:200]}")
                result.failed += 1
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    result.aborted = True
                    break
                continue
            consecutive_errors = 0

            stamps = df["timestamp"]
            if tf in DATE_LABELLED_TIMEFRAMES:
                stamps = date_label_ms(stamps)
            got = set(stamps.tolist())
            off_grid = sorted(t for t in got if (t - hole.start_ms) % tf_ms)
            if off_grid:
                # The hole isn't empty, its bars just don't line up with the
                # stored ones. Neither merging them nor recording it is right.
                print(f"  [fail] {symbol}/{tf}: {len(off_grid)} bar(s) off the stored grid, "
                      f"first {off_grid[0]}; left as it was")
                result.failed += 1
                continue

            bars = range(hole.start_ms, hole.end_ms + 1, tf_ms)
            stayed_empty = bar_runs([b for b in bars if b not in got], tf_ms)
            empties += stayed_empty
            result.candles_empty += sum(run[2] for run in stayed_empty)
            if df.empty:
                result.empty += 1
            else:
                frames.append(df)
                if stayed_empty:
                    result.partly += 1
                else:
                    result.filled += 1

        if frames:
            old_rows, new_rows = merge_ohlcv_parquet(pd.concat(frames, ignore_index=True), group[0].path)
            result.candles_added += new_rows - old_rows
            print(f"  {symbol}/{tf}: {old_rows}→{new_rows} rows (+{new_rows - old_rows})")
            if _refresh_bounds(catalog, exchange, group[0], is_futures) and on_catalog_change:
                on_catalog_change()
        for first, last, count in empties:
            record.add(group[0].path, first, last, count)
        if checkpoint:
            checkpoint()
        if result.aborted:
            print(f"Stopping: {MAX_CONSECUTIVE_ERRORS} errors in a row.")
            break
    return result


def main(argv: list[str] | None = None) -> int:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", required=True, choices=CRYPTO_EXCHANGES)
    p.add_argument("--trading-mode", default="spot", choices=["spot", "futures"],
                   help="Trading mode (default: spot)")
    p.add_argument("--timeframes", "-t", nargs="+", required=True,
                   help="Timeframes to repair; 1Mo has no fixed interval and is not supported")
    p.add_argument("--symbols", nargs="+", metavar="SYMBOL", help="Only these series (DOGE_USDT)")
    p.add_argument("--min-candles", type=int, default=1, metavar="N",
                   help="Ignore holes of fewer than N candles (default: 1)")
    p.add_argument("--limit", type=int, metavar="N", help="Take only the first N holes, for a trial run")
    p.add_argument("--apply", action="store_true", help="Fetch and write. Without it, plan only")
    args = p.parse_args(argv)

    unsupported = [tf for tf in args.timeframes if TF_TO_MS.get(tf) is None]
    if unsupported:
        p.error(f"no fixed bar interval, so no hole rule: {', '.join(unsupported)}")

    is_futures = args.trading_mode == "futures"
    catalog = (load_crypto_futures_catalog if is_futures else load_crypto_catalog)(args.exchange)
    record = ExchangeEmptyRecord()
    holes = find_holes(args.exchange, args.trading_mode, args.timeframes, catalog, record,
                       min_candles=args.min_candles, symbols=args.symbols)

    print(f"{args.exchange} {args.trading_mode}: holes of {args.min_candles}+ candles, "
          f"from the parquet, less the exchange-empty record ({relative_to_repo(record.path)})\n")
    print_plan(holes, args.exchange, args.trading_mode, args.timeframes)
    selected = holes if args.limit is None else holes[:args.limit]
    if args.limit is not None:
        print(f"\n--limit {args.limit}: this run takes {len(selected)} hole(s), "
              f"{sum(h.requests(args.exchange, args.trading_mode) for h in selected)} request(s)")
    if not args.apply:
        print("\nPlan only: nothing was fetched or written. Add --apply to fill.")
        return 0
    if not selected:
        print("\nNothing to fill.")
        return 0

    catalog_changed = [False]

    def save_state() -> None:
        record.save()
        if catalog_changed[0]:
            (save_crypto_futures_catalog if is_futures else save_crypto_catalog)(args.exchange, catalog)
            catalog_changed[0] = False

    print()
    try:
        result = fill_holes(
            selected, args.exchange, args.trading_mode, catalog, record,
            ccxt_exchange(args.exchange, args.trading_mode),
            checkpoint=CatalogCheckpoint(save_state),
            on_catalog_change=lambda: catalog_changed.__setitem__(0, True),
        )
    finally:
        save_state()

    print(f"\nFilled {result.filled} · partly {result.partly} · exchange-empty {result.empty} · "
          f"failed {result.failed} · not listed {len(result.unlisted)}")
    print(f"Candles added {result.candles_added} · recorded empty {result.candles_empty}")
    return 1 if result.failed or result.aborted else 0


if __name__ == "__main__":
    sys.exit(main())
