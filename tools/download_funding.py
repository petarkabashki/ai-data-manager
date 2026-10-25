#!/usr/bin/env -S uv run python
"""Fetch perpetual funding-rate history next to each contract's bars.

    uv run python tools/download_funding.py --exchange binance
    uv run python tools/download_funding.py --exchange bybit --symbols BTC_USDT ETH_USDT
    uv run python tools/download_funding.py --exchange binance --dry-run --json

Writes `price-data/crypto-futures/<exchange>/<SYMBOL>/<SYMBOL>-funding.parquet`
(`timestamp` ms UTC at the settlement, aligned to the hour; `funding_rate`, a
fraction per settlement, positive = longs pay shorts; `mark_price` on Binance,
null on Bybit) and records it as `timeframe_dates.funding` on the contract's
`crypto_futures_<exchange>.yaml` entry (`schema: funding_rate`).

Which contracts: every one in that catalog, or `--symbols` (which must be in
it — funding belongs to a perpetual whose bars we hold). Where each fetch
starts: after the last settlement on disk; with no file, at `--since` or the
contract's first bar. `--full` ignores the file and refetches from there,
the new rows winning. Public endpoints, no key.

Exit status: 0 when at least one contract succeeded (failures are listed),
1 when every contract failed, 2 on a bad argument. `--json` writes one JSON
document to stdout (keys: exchange, dry_run, plan[], results[], failures[],
updated) and the progress log to stderr.

Moved here from py-qwant's `scripts/sync_funding_rates.py`, which wrote the
same files; the layout and columns are unchanged, so its readers still work.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    FUNDING_COLS,
    funding_file_path,
    load_crypto_futures_catalog,
    read_funding,
    merge_funding,
    save_crypto_futures_catalog,
    update_funding_entry,
)
from utils.config import relative_to_repo

EXCHANGES = ("binance", "bybit")
#: Nothing on disk and no first bar to start from: 2017-07-14, before any
#: perpetual on either exchange.
FALLBACK_START_MS = 1_500_000_000_000

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "ai-data-manager funding sync"


def _get(url: str, params: dict) -> dict | list:
    for attempt in range(6):
        response = SESSION.get(url, params=params, timeout=20)
        if response.status_code in (418, 429):
            time.sleep(5.0 * (attempt + 1))
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"rate-limited too long: {url} {params}")


def _frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=FUNDING_COLS).astype(
        {"timestamp": "int64", "funding_rate": "float64", "mark_price": "float64"})


def fetch_binance(symbol: str, start_ms: int) -> pd.DataFrame:
    # With no startTime Binance returns the most RECENT page, so always name one.
    rows: list[dict] = []
    start = start_ms
    while True:
        page = _get("https://fapi.binance.com/fapi/v1/fundingRate",
                    {"symbol": symbol.replace("_", ""), "startTime": start, "limit": 1000})
        if not page:
            break
        rows.extend({"timestamp": int(r["fundingTime"]), "funding_rate": float(r["fundingRate"]),
                     "mark_price": float(r.get("markPrice") or 0.0) or None} for r in page)
        start = int(page[-1]["fundingTime"]) + 1
        if len(page) < 1000:
            break
        time.sleep(0.25)
    return _frame(rows)


def fetch_bybit(symbol: str, start_ms: int) -> pd.DataFrame:
    # Bybit pages backwards from endTime; walk down to start_ms.
    rows: list[dict] = []
    end = int(time.time() * 1000)
    while True:
        payload = _get("https://api.bybit.com/v5/market/funding/history",
                       {"category": "linear", "symbol": symbol.replace("_", ""), "endTime": end, "limit": 200})
        items = (payload.get("result") or {}).get("list") or []
        if not items:
            break
        rows.extend({"timestamp": int(r["fundingRateTimestamp"]), "funding_rate": float(r["fundingRate"]),
                     "mark_price": None} for r in items if int(r["fundingRateTimestamp"]) >= start_ms)
        oldest = min(int(r["fundingRateTimestamp"]) for r in items)
        if len(items) < 200 or oldest <= start_ms:
            break
        end = oldest - 1
        time.sleep(0.25)
    return _frame(rows)


FETCH = {"binance": fetch_binance, "bybit": fetch_bybit}


def _day_ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)


def _iso(ms: int | None) -> str | None:
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def plan_symbol(exchange: str, symbol: str, entry: dict, since: str | None, full: bool) -> dict:
    """Where one contract's fetch starts, and why."""
    path = funding_file_path(exchange, symbol)
    existing = read_funding(path)
    if not full and not existing.empty:
        start, reason = int(existing["timestamp"].max()) + 1, "after last settlement"
    elif since:
        start, reason = _day_ms(since), "--since"
    elif entry.get("start_date"):
        start, reason = _day_ms(entry["start_date"]), "first bar"
    else:
        start, reason = FALLBACK_START_MS, "no bars dated"
    return {"symbol": symbol, "start": _iso(start), "start_ms": start, "reason": reason,
            "rows_on_disk": len(existing), "file": relative_to_repo(path)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", required=True, choices=EXCHANGES)
    p.add_argument("--symbols", nargs="+", metavar="SYMBOL",
                   help="BTC_USDT or BTC/USDT; default: every contract in the catalog")
    p.add_argument("--since", help="YYYY-MM-DD start when there is no file (or with --full)")
    p.add_argument("--full", action="store_true", help="refetch from --since or the first bar")
    p.add_argument("--dry-run", action="store_true", help="plan only: no network, no writes")
    p.add_argument("--json", action="store_true", help="one JSON document on stdout")
    args = p.parse_args(argv)
    if args.since:
        try:
            date.fromisoformat(args.since)
        except ValueError:
            p.error(f"--since must be YYYY-MM-DD, not {args.since!r}")

    log = (lambda *a: print(*a, file=sys.stderr, flush=True)) if args.json else print
    catalog = load_crypto_futures_catalog(args.exchange)
    by_symbol = {entry.get("name", key): (key, entry) for key, entry in catalog.items()
                 if isinstance(entry, dict)}
    if args.symbols:
        wanted = [s.split(":")[0].replace("/", "_") for s in args.symbols]
        unknown = [s for s in wanted if s not in by_symbol]
        if unknown:
            p.error(f"not in crypto_futures_{args.exchange}: {unknown}")
        symbols = sorted(set(wanted))
    else:
        symbols = sorted(by_symbol)

    plan = [plan_symbol(args.exchange, s, by_symbol[s][1], args.since, args.full) for s in symbols]
    doc = {"exchange": args.exchange, "dry_run": args.dry_run, "plan": plan,
           "results": [], "failures": [], "updated": 0}
    if args.dry_run:
        for item in plan:
            log(f"{item['symbol']}: from {item['start']} ({item['reason']}), {item['rows_on_disk']} rows on disk")
        log(f"{len(plan)} contract(s) planned on {args.exchange}; dry run, nothing fetched")
    else:
        try:
            for item in plan:
                symbol = item["symbol"]
                try:
                    new = FETCH[args.exchange](symbol, item["start_ms"])
                    path = funding_file_path(args.exchange, symbol)
                    before, after = merge_funding(new, path)
                except Exception as exc:  # noqa: BLE001 — one contract's failure is reported, not fatal
                    doc["failures"].append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
                    log(f"{symbol}: FAILED {type(exc).__name__}: {exc}")
                    continue
                ts = read_funding(path)["timestamp"]
                start_date = None if ts.empty else _iso(int(ts.min()))[:10]
                end_date = None if ts.empty else _iso(int(ts.max()))[:10]
                if start_date:
                    update_funding_entry(catalog, symbol, args.exchange, path, start_date, end_date)
                doc["results"].append({"symbol": symbol, "fetched": len(new), "added": after - before,
                                       "rows": after, "start_date": start_date, "end_date": end_date})
                doc["updated"] += after > before
                log(f"{symbol}: +{after - before} rows, {after} total, {start_date}..{end_date}")
                time.sleep(0.2)
        finally:
            if doc["results"]:
                save_crypto_futures_catalog(args.exchange, catalog)
        log(f"{len(doc['results'])} ok, {len(doc['failures'])} failed, {doc['updated']} updated")

    for item in plan:
        item.pop("start_ms")
    if args.json:
        print(json.dumps(doc, default=str))
    if not args.dry_run and plan and not doc["results"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
