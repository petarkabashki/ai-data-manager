#!/usr/bin/env -S uv run python
"""Query available price data — assets, timeframes, date ranges, and staleness.

Usage examples:
  uv run python tools/query.py
  uv run python tools/query.py --exchange binance
  uv run python tools/query.py --exchange binance --timeframe 1d
  uv run python tools/query.py --exchange binance --symbol BTC_USDT
  uv run python tools/query.py --stale-days 30          # only show stale entries
  uv run python tools/query.py --asset-class Crypto
  uv run python tools/query.py --list-exchanges
  uv run python tools/query.py --list-symbols --exchange binance --timeframe 1d
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    CRYPTO_EXCHANGES,
    DATA_ROOT,
    REPO_ROOT,
    load_crypto_catalog,
    read_parquet_range,
)
from utils.data import load_assets


def _today_str() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")


def _days_stale(end_date_str: str | None) -> int | None:
    if not end_date_str:
        return None
    try:
        end = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (datetime.now(tz=timezone.utc) - end).days
    except ValueError:
        return None


def _fmt_row(cols: list, widths: list[int]) -> str:
    return "  ".join(str(c).ljust(w) for c, w in zip(cols, widths))


def _print_table(headers: list[str], rows: list[list], sep: bool = True) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    print(_fmt_row(headers, widths))
    if sep:
        print("  ".join("-" * w for w in widths))
    for row in rows:
        print(_fmt_row(row, widths))


def cmd_list_exchanges(args: argparse.Namespace) -> None:
    rows = []
    for ex in CRYPTO_EXCHANGES:
        cat = load_crypto_catalog(ex)
        rows.append([ex, len(cat)])
    _print_table(["Exchange", "Symbols"], rows)


def cmd_list_symbols(args: argparse.Namespace) -> None:
    exchanges = [args.exchange] if args.exchange else CRYPTO_EXCHANGES
    rows = []
    for ex in exchanges:
        cat = load_crypto_catalog(ex)
        for key, entry in cat.items():
            sym = entry.get("name", key)
            tfs = entry.get("timeframes", [])
            if args.timeframe and args.timeframe not in tfs:
                continue
            print(sym)
    return


def cmd_query(args: argparse.Namespace) -> None:
    min_stale = args.stale_days

    # --- crypto ---
    exchanges = [args.exchange] if args.exchange else CRYPTO_EXCHANGES
    rows = []

    for ex in exchanges:
        cat = load_crypto_catalog(ex)
        for key, entry in cat.items():
            sym = entry.get("name", key)
            if args.symbol and sym != args.symbol:
                continue
            tfs = entry.get("timeframes", [])
            td = entry.get("timeframe_dates", {})

            # filter to requested timeframe
            if args.timeframe:
                if args.timeframe not in td:
                    continue
                tf_items = {args.timeframe: td[args.timeframe]}
            else:
                tf_items = td

            for tf, info in tf_items.items():
                start = info.get("start_date", "")
                end = info.get("end_date", "")
                fp_str = info.get("file_path", "")

                # Use actual file dates if catalog is stale/missing
                if fp_str and fp_str.endswith(".parquet"):
                    fp = REPO_ROOT / fp_str
                    result = read_parquet_range(fp)
                    if result:
                        start, end, _ = result

                stale = _days_stale(end)
                if min_stale is not None and (stale is None or stale < min_stale):
                    continue

                rows.append([
                    ex,
                    sym,
                    tf,
                    start or "—",
                    end or "—",
                    f"{stale}d" if stale is not None else "—",
                ])

    if not rows:
        print("No results.")
        return

    if args.list_symbols:
        seen = set()
        for r in rows:
            k = r[1]
            if k not in seen:
                print(k)
                seen.add(k)
        return

    rows.sort(key=lambda r: (r[0], r[2], -(int(r[5][:-1]) if r[5] != "—" else 0)))
    _print_table(["Exchange", "Symbol", "TF", "Start", "End", "Stale"], rows)
    print(f"\n{len(rows)} entries")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", choices=CRYPTO_EXCHANGES, help="Filter by exchange")
    p.add_argument("--symbol", "-s", help="Filter by symbol (e.g. BTC_USDT)")
    p.add_argument("--timeframe", "-t", help="Filter by timeframe (e.g. 1d, 4h)")
    p.add_argument("--stale-days", type=int, metavar="N", help="Only show entries stale by >= N days")
    p.add_argument("--list-exchanges", action="store_true", help="List exchanges and symbol counts")
    p.add_argument("--list-symbols", action="store_true", help="Print only symbol names (one per line)")
    args = p.parse_args()

    if args.list_exchanges:
        cmd_list_exchanges(args)
    elif args.list_symbols and not args.exchange and not args.timeframe:
        cmd_list_symbols(args)
    else:
        cmd_query(args)


if __name__ == "__main__":
    main()
