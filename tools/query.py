#!/usr/bin/env -S uv run python
"""Query what data exists — any asset class, any provider.

Answers "what do we have for X, how far back does it go, and how stale is it"
from the catalogs. This used to cover crypto only, despite the name: stocks,
indexes, commodities and macro were unqueryable.

Usage examples:
  uv run python tools/query.py --catalog crypto_binance --timeframe 1d
  uv run python tools/query.py --asset-class Stock --symbol AAPL
  uv run python tools/query.py --provider IBKR --timeframe 1h
  uv run python tools/query.py --stale-days 30            # only stale series
  uv run python tools/query.py --list-catalogs
  uv run python tools/query.py --list-symbols --asset-class Index
  uv run python tools/query.py --asset-class Crypto --json

By default dates come from the catalog, which is fast. `--rescan` reads each
parquet file instead — authoritative, but it opens every file, so it is opt-in
rather than the default it used to be (unusable across 5,000 symbols).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.calendars import calendar_for
from utils.catalog import (
    catalog_registry,
    iter_timeframe_nodes,
    read_parquet_range,
    resolve_from_repo,
)
from utils.planning import days_behind
from utils.report import allow_broken_pipe, print_table

ASSET_CLASSES = ["Crypto", "Crypto Futures", "Stock", "Index", "Commodity", "Macro"]


def select_catalogs(args, parser) -> list:
    specs = catalog_registry()
    if args.catalog:
        names = set(args.catalog)
        unknown = names - {s.name for s in specs}
        if unknown:
            parser.error(f"unknown catalog(s): {', '.join(sorted(unknown))}")
        specs = [s for s in specs if s.name in names]
    if args.asset_class:
        specs = [s for s in specs if s.asset_class in args.asset_class]
    return specs


def collect(args, parser) -> list[dict]:
    """One row per (catalog, symbol, timeframe, provider node)."""
    symbols = set(args.symbols or ([args.symbol] if args.symbol else []))
    timeframes = set(args.timeframes or ([args.timeframe] if args.timeframe else []))
    rows: list[dict] = []

    for spec in select_catalogs(args, parser):
        for key, entry in spec.load().items():
            if not isinstance(entry, dict):
                continue
            name = entry.get("name", key)
            if symbols and name not in symbols:
                continue

            for timeframe, node_key, node in iter_timeframe_nodes(entry):
                if timeframes and timeframe not in timeframes:
                    continue

                provider = (
                    node.get("provider")
                    or (node_key.split(":")[0] if node_key else None)
                    or entry.get("provider")
                )
                if args.provider and (provider or "").lower() != args.provider.lower():
                    continue

                start, end = node.get("start_date"), node.get("end_date")
                rows_count = None
                if args.rescan and node.get("file_path"):
                    actual = read_parquet_range(resolve_from_repo(node["file_path"]))
                    if actual:
                        start, end, rows_count = actual

                stale = days_behind(end, timeframe, calendar_for(spec.asset_class))
                if args.stale_days is not None and (stale is None or stale < args.stale_days):
                    continue

                rows.append(
                    {
                        "catalog": spec.name,
                        "asset_class": spec.asset_class,
                        "symbol": name,
                        "timeframe": timeframe,
                        "provider": provider,
                        "session": node.get("session"),
                        "start_date": start,
                        "end_date": end,
                        "rows": rows_count,
                        "stale_days": stale,
                        "adjustment": node.get("adjustment"),
                        "volume_basis": node.get("volume_basis"),
                        "file_path": node.get("file_path"),
                    }
                )

    rows.sort(key=lambda r: (r["catalog"], r["symbol"], r["timeframe"], r["provider"] or ""))
    return rows


def cmd_list_catalogs(args, parser) -> None:
    table = []
    for spec in select_catalogs(args, parser):
        catalog = spec.load()
        series = sum(len(iter_timeframe_nodes(e)) for e in catalog.values() if isinstance(e, dict))
        table.append([spec.name, spec.asset_class, len(catalog), series])

    if args.json:
        json.dump(
            [
                {"catalog": c, "asset_class": a, "symbols": s, "series": n}
                for c, a, s, n in table
            ],
            sys.stdout, indent=2,
        )
        sys.stdout.write("\n")
        return

    print_table(["Catalog", "Asset class", "Symbols", "Series"], table)
    print(f"\n{sum(r[2] for r in table)} symbols, {sum(r[3] for r in table)} series")


def main() -> None:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", nargs="+", help="Catalogs by name (sp500, indexes, crypto_binance…)")
    p.add_argument("--asset-class", nargs="+", choices=ASSET_CLASSES, help="Filter by asset class")
    p.add_argument("--symbol", "-s", help="Single symbol")
    p.add_argument("--symbols", nargs="+", help="Multiple symbols")
    p.add_argument("--timeframe", "-t", help="Single timeframe (e.g. 1d)")
    p.add_argument("--timeframes", nargs="+", help="Multiple timeframes")
    p.add_argument("--provider", help="Filter by provider (YF, IBKR, binance…), case-insensitive")
    p.add_argument("--stale-days", type=int, metavar="N", help="Only series N or more session days overdue")
    p.add_argument("--rescan", action="store_true",
                   help="Read dates from the parquet files instead of the catalog (slow, authoritative)")
    p.add_argument("--list-catalogs", action="store_true", help="List catalogs with symbol/series counts")
    p.add_argument("--list-symbols", action="store_true", help="Print matching symbol names, one per line")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    p.add_argument("--limit", type=int, default=200, help="Max rows to print (default 200; --json is unlimited)")
    args = p.parse_args()

    if args.list_catalogs:
        cmd_list_catalogs(args, p)
        return

    rows = collect(args, p)

    if args.list_symbols:
        seen: list[str] = []
        for row in rows:
            if row["symbol"] not in seen:
                seen.append(row["symbol"])
        if args.json:
            json.dump(seen, sys.stdout, indent=2)
            sys.stdout.write("\n")
        else:
            print("\n".join(seen))
        return

    if args.json:
        json.dump({"count": len(rows), "series": rows}, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return

    if not rows:
        print("No results.")
        return

    shown = rows[: args.limit]
    print_table(
        ["Catalog", "Symbol", "TF", "Provider", "Start", "End", "Stale"],
        [
            [
                r["catalog"], r["symbol"], r["timeframe"],
                (r["provider"] or "—") + (f":{r['session']}" if r.get("session") else ""),
                r["start_date"] or "—", r["end_date"] or "—",
                f"{r['stale_days']}d" if r["stale_days"] is not None else "—",
            ]
            for r in shown
        ],
    )
    print(f"\n{len(rows)} series" + (f" (showing {len(shown)})" if len(shown) < len(rows) else ""))
    if not args.rescan:
        print("Dates are from the catalog; pass --rescan to read the files themselves.")


if __name__ == "__main__":
    main()
