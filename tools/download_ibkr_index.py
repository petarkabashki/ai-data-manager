#!/usr/bin/env -S uv run python
"""Download index bars from IBKR into the indexes catalog.

Usage examples:
  uv run python tools/download_ibkr_index.py --timeframes 1h --since 2024-01-01

  uv run python tools/download_ibkr_index.py --symbols GSPC NDX --timeframes 1h --limit 2

  uv run python tools/download_ibkr_index.py --timeframes 1h 4h --full --since 2020-01-01 --port 4001

IBKR's registration is written to its own node under `timeframe_dates[tf]`, so
requesting a timeframe YF already covers (e.g. 1d) adds a sibling node rather
than replacing YF's.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import IBKR_HOST, IBKR_PORT, relative_to_repo
from utils.catalog import (
    REPO_ROOT,
    index_file_path,
    load_index_catalog,
    merge_ohlcv_parquet,
    read_parquet_range,
    save_index_catalog,
    update_index_timeframe_entry,
)
from utils.ibkr import (
    INDEX_CONTRACT_MAP,
    IbkrHistoricalClient,
    chunk_days_for_timeframe,
    download_ranges,
    format_ranges,
    plan_download_ranges,
    utc_now,
)

INDEX_PROVIDER = "IBKR"
#: Offset from the shared base client id so this tool and the stock downloader
#: can share one gateway without colliding on client id.
CLIENT_ID_OFFSET = 1


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--symbols", nargs="+", choices=sorted(INDEX_CONTRACT_MAP), default=sorted(INDEX_CONTRACT_MAP),
                   help="Index symbols to download (default: all mapped US indices)")
    p.add_argument("--timeframes", nargs="+", choices=["1h", "4h", "8h", "1d", "1w"], default=["1h"],
                   help="IBKR bar sizes to request and store")
    p.add_argument("--since", help="Start date for new symbols or full refreshes (YYYY-MM-DD)")
    p.add_argument("--full", action="store_true",
                   help="Ignore existing files and rebuild from --since")
    p.add_argument("--host", default=IBKR_HOST, help="IBKR TWS/Gateway host (env: IBKR_HOST)")
    p.add_argument("--port", type=int, default=IBKR_PORT, help="IBKR TWS/Gateway port (env: IBKR_PORT)")
    p.add_argument("--client-id", type=int, help="IBKR API client id (env: IBKR_CLIENT_ID + 1)")
    p.add_argument("--chunk-days", type=int,
                   help="Historical request chunk size in calendar days (default adapts by timeframe)")
    p.add_argument("--overlap-days", type=int, default=5,
                   help="Overlap window when appending to existing files")
    p.add_argument("--what-to-show", default="TRADES", choices=["TRADES", "MIDPOINT"],
                   help="IBKR historical data type (indices have no volume; MIDPOINT is a fallback if TRADES returns nothing)")
    p.add_argument("--sleep-seconds", type=float, default=0.35,
                   help="Delay between historical requests")
    p.add_argument("--request-timeout", type=float, default=60.0,
                   help="Per-IBKR request timeout in seconds before treating the chunk as failed")
    p.add_argument("--max-chunk-retries", type=int, default=2,
                   help="Retries for timeout/HMDS-disconnect chunk errors before failing the symbol")
    p.add_argument("--limit", type=int, help="Limit number of symbols (useful for smoke tests)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would be downloaded without writing files or catalogs")
    args = p.parse_args()

    from utils.config import IBKR_CLIENT_ID
    client_id = args.client_id if args.client_id is not None else IBKR_CLIENT_ID + CLIENT_ID_OFFSET

    symbols = args.symbols[: args.limit] if args.limit else args.symbols
    catalog = load_index_catalog()
    download_end = utc_now()
    failures: list[tuple[str, str, str]] = []
    updates = 0

    with IbkrHistoricalClient(args.host, args.port, client_id, timeout=args.request_timeout) as client:
        for symbol in symbols:
            if symbol not in catalog:
                failures.append((symbol, "-", "symbol not found in indexes.yaml"))
                print(f"{symbol}: skipped (not in indexes.yaml)")
                continue

            try:
                contract = client.resolve_index_contract(symbol)
            except Exception as exc:
                failures.append((symbol, "-", str(exc)))
                print(f"{symbol}: FAILED to resolve contract ({exc})")
                continue

            for timeframe in args.timeframes:
                dst = index_file_path(INDEX_PROVIDER, symbol, timeframe)
                ranges = plan_download_ranges(
                    dst,
                    since=args.since,
                    full=args.full,
                    download_end=download_end,
                    overlap_days=args.overlap_days,
                    timeframe=timeframe,
                )
                if not ranges:
                    print(f"  {symbol}/{timeframe}: already current, no gaps detected")
                    continue

                print(f"  {symbol}/{timeframe}: {format_ranges(ranges)}")

                try:
                    df = download_ranges(
                        client,
                        contract,
                        label=symbol,
                        timeframe=timeframe,
                        ranges=ranges,
                        chunk_days=chunk_days_for_timeframe(timeframe, args.chunk_days),
                        what_to_show=args.what_to_show,
                        use_rth=False,
                        sleep_seconds=args.sleep_seconds,
                        max_chunk_retries=args.max_chunk_retries,
                    )
                except Exception as exc:
                    failures.append((symbol, timeframe, str(exc)))
                    print(f"  {symbol}/{timeframe}: FAILED ({exc})")
                    continue

                if df.empty:
                    continue

                if args.dry_run:
                    print(f"    [dry-run] would write {len(df)} rows to {dst}")
                    continue

                old_rows, new_rows = merge_ohlcv_parquet(df, dst)
                result = read_parquet_range(dst)
                if result is None:
                    raise RuntimeError(f"Could not read back merged parquet {dst}")
                start_date, end_date, _ = result

                update_index_timeframe_entry(
                    catalog,
                    symbol=symbol,
                    timeframe=timeframe,
                    provider=INDEX_PROVIDER,
                    file_path=dst,
                    start_date=start_date,
                    end_date=end_date,
                )
                updates += 1
                print(f"    wrote {relative_to_repo(dst)} ({old_rows}->{new_rows} rows)")

            if not args.dry_run:
                save_index_catalog(catalog)

    print("\nSummary")
    print(f"  updated timeframes: {updates}")
    print(f"  failures: {len(failures)}")
    for symbol, timeframe, message in failures:
        print(f"  - {symbol}/{timeframe}: {message}")

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
