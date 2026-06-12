#!/usr/bin/env -S uv run python
"""Convert all crypto data to canonical parquet schema in-place.

Canonical schema: [timestamp(int64 ms UTC), open, high, low, close, volume] — all float64 except timestamp.

Operations performed:
  1. Parquet normalization   — rename/cast columns to canonical schema
  2. JSON → parquet          — convert legacy OHLCV JSON, merge with existing parquet, delete JSON
  3. Report JSON cleanup     — delete download_report.json files
  4. Metadata sync           — refresh YAML start/end dates from actual files

Run dry-run first:
  uv run python tools/convert_crypto.py --dry-run
  uv run python tools/convert_crypto.py --exchange binance --dry-run

Then run for real:
  uv run python tools/convert_crypto.py
  uv run python tools/convert_crypto.py --exchange binance
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    CANONICAL_COLS,
    CRYPTO_EXCHANGES,
    DATA_ROOT,
    REPO_ROOT,
    normalize_ohlcv,
    read_parquet_range,
    refresh_catalog_dates,
    save_crypto_catalog,
    load_crypto_catalog,
    update_catalog_entry,
    crypto_file_path,
)

CANONICAL_SET = frozenset(CANONICAL_COLS)


def is_canonical(path: Path) -> bool:
    """Return True if parquet file already has the canonical schema."""
    try:
        schema = pq.read_schema(path)
        return tuple(schema.names) == tuple(CANONICAL_COLS)
    except Exception:
        return False


def convert_parquet(path: Path, dry_run: bool = False) -> str:
    """Normalize a parquet file in-place. Returns status string."""
    try:
        schema_names = tuple(pq.read_schema(path).names)
    except Exception as e:
        return f"ERROR read_schema: {e}"

    if schema_names == tuple(CANONICAL_COLS):
        return "ok"

    if schema_names == ():
        return "empty"

    try:
        df = pd.read_parquet(path)
        normed = normalize_ohlcv(df)
        if normed.empty:
            return "empty"
        if not dry_run:
            tmp = path.with_suffix(".parquet.tmp")
            normed.to_parquet(tmp, index=False, compression="snappy")
            tmp.replace(path)
        return f"converted({list(schema_names)[:2]}...)"
    except Exception as e:
        return f"ERROR: {e}"


def json_to_parquet(json_path: Path, dry_run: bool = False) -> str:
    """Convert a legacy OHLCV JSON file to parquet, merging with existing if present.
    Deletes the JSON after successful conversion. Returns status string."""
    try:
        with open(json_path) as f:
            raw = json.load(f)
    except Exception as e:
        return f"ERROR read: {e}"

    if not isinstance(raw, list) or not raw:
        return "empty_json"

    try:
        df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
        new_df = normalize_ohlcv(df)
    except Exception as e:
        return f"ERROR parse: {e}"

    # Determine destination parquet path from JSON filename
    # JSON: BTC_USDT-1d.json  → parquet: {exchange}/{symbol}/{symbol}-1d.parquet
    stem = json_path.stem  # e.g. BTC_USDT-1d
    parts = stem.split("-", 1)
    if len(parts) != 2:
        return f"ERROR: can't parse filename {json_path.name}"

    symbol, tf = parts[0], parts[1]
    exchange = json_path.parent.name  # parent dir is exchange name
    dst = crypto_file_path(exchange, symbol, tf)

    if dry_run:
        action = "would_create" if not dst.exists() else "would_merge"
        return f"{action} → {dst.relative_to(REPO_ROOT)}"

    old_rows = 0
    if dst.exists():
        try:
            existing = normalize_ohlcv(pd.read_parquet(dst))
            old_rows = len(existing)
            merged = (
                pd.concat([existing, new_df])
                .drop_duplicates(subset=["timestamp"])
                .sort_values("timestamp")
                .reset_index(drop=True)
            )
        except Exception as e:
            return f"ERROR merge: {e}"
    else:
        dst.parent.mkdir(parents=True, exist_ok=True)
        merged = new_df

    try:
        tmp = dst.with_suffix(".parquet.tmp")
        merged.to_parquet(tmp, index=False, compression="snappy")
        tmp.replace(dst)
    except Exception as e:
        return f"ERROR write: {e}"

    json_path.unlink()
    added = len(merged) - old_rows
    return f"json→parquet ({old_rows}→{len(merged)}, +{added} rows)"


def find_parquet_files(exchange: str | None, symbol: str | None) -> list[Path]:
    base = DATA_ROOT / "cryptos"
    if exchange:
        base = base / exchange
    pattern = f"{symbol}-*.parquet" if symbol else "*.parquet"
    return list(base.rglob(pattern))


def find_json_files(exchange: str | None) -> list[Path]:
    base = DATA_ROOT / "cryptos"
    if exchange:
        base = base / exchange
    return [
        p for p in base.rglob("*.json")
        if p.name != "download_report.json"
    ]


def find_report_jsons(exchange: str | None) -> list[Path]:
    base = DATA_ROOT / "cryptos"
    if exchange:
        base = base / exchange
    return list(base.rglob("download_report.json"))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", choices=CRYPTO_EXCHANGES, help="Limit to one exchange")
    p.add_argument("--symbol", "-s", help="Limit to one symbol (e.g. BTC_USDT)")
    p.add_argument("--dry-run", action="store_true", help="Show what would happen, no writes")
    p.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    p.add_argument("--skip-metadata", action="store_true", help="Skip YAML metadata refresh at end")
    args = p.parse_args()

    t0 = time.monotonic()
    dry = args.dry_run
    if dry:
        print("[DRY RUN — no files will be written or deleted]\n")

    # ── 1. JSON OHLCV files ──────────────────────────────────────────────────
    json_files = find_json_files(args.exchange)
    print(f"JSON OHLCV files: {len(json_files)}")
    for jf in json_files:
        status = json_to_parquet(jf, dry_run=dry)
        print(f"  {jf.relative_to(DATA_ROOT)}  →  {status}")
    print()

    # ── 2. download_report.json cleanup ─────────────────────────────────────
    report_jsons = find_report_jsons(args.exchange)
    print(f"Report JSON files to delete: {len(report_jsons)}")
    for rj in report_jsons:
        if not dry:
            rj.unlink()
        print(f"  {'[would delete]' if dry else 'deleted'} {rj.relative_to(DATA_ROOT)}")
    print()

    # ── 3. Parquet normalization ─────────────────────────────────────────────
    all_parquets = find_parquet_files(args.exchange, args.symbol)
    to_convert = [f for f in all_parquets if not is_canonical(f)]
    already_ok = len(all_parquets) - len(to_convert)

    # Separate empty files (schema=()) from real work
    needs_work = []
    empties = []
    for f in to_convert:
        try:
            names = tuple(pq.read_schema(f).names)
            if names == ():
                empties.append(f)
            else:
                needs_work.append(f)
        except Exception:
            needs_work.append(f)

    print(f"Parquet files: {len(all_parquets)} total")
    print(f"  Already canonical : {already_ok}")
    print(f"  Need conversion   : {len(needs_work)}")
    print(f"  Empty (skipped)   : {len(empties)}")
    print()

    if not needs_work:
        print("Nothing to convert.")
    else:
        counts = {"converted": 0, "empty": 0, "error": 0}

        def worker(path: Path) -> tuple[Path, str]:
            return path, convert_parquet(path, dry_run=dry)

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futs = {pool.submit(worker, f): f for f in needs_work}
            done = 0
            errors: list[str] = []
            for fut in as_completed(futs):
                path, status = fut.result()
                done += 1
                if status.startswith("ERROR"):
                    counts["error"] += 1
                    errors.append(f"  {path.relative_to(DATA_ROOT)}: {status}")
                elif status == "empty":
                    counts["empty"] += 1
                elif status == "ok":
                    counts["converted"] += 1  # was already canonical (shouldn't happen)
                else:
                    counts["converted"] += 1
                if done % 1000 == 0 or done == len(needs_work):
                    elapsed = time.monotonic() - t0
                    print(f"  {done}/{len(needs_work)} ({elapsed:.0f}s)…", flush=True)

        print(f"\nConversion results:")
        print(f"  Converted : {counts['converted']}")
        print(f"  Empty     : {counts['empty']}")
        print(f"  Errors    : {counts['error']}")
        if errors:
            print("Errors:")
            for e in errors[:20]:
                print(e)
            if len(errors) > 20:
                print(f"  ... and {len(errors)-20} more")
        print()

    # ── 4. Metadata sync ─────────────────────────────────────────────────────
    if not args.skip_metadata and not dry:
        exchanges = [args.exchange] if args.exchange else CRYPTO_EXCHANGES
        print("Syncing YAML metadata from actual files…")
        total_updated = 0
        for ex in exchanges:
            updated = refresh_catalog_dates(ex)
            print(f"  {ex}: {updated} entries updated")
            total_updated += updated
        print(f"Total YAML entries updated: {total_updated}")

        # Also update catalog entries for any newly created parquet from JSON
        if json_files:
            for jf in json_files:
                stem = jf.stem
                parts = stem.split("-", 1)
                if len(parts) == 2:
                    symbol, tf = parts[0], parts[1]
                    exchange = jf.parent.name
                    dst = crypto_file_path(exchange, symbol, tf)
                    result = read_parquet_range(dst)
                    if result:
                        start, end, _ = result
                        catalog = load_crypto_catalog(exchange)
                        update_catalog_entry(catalog, exchange, symbol, tf, dst, start, end)
                        save_crypto_catalog(exchange, catalog)
                        print(f"  Metadata updated: {exchange}/{symbol}/{tf} → {start}..{end}")
    print()

    elapsed = time.monotonic() - t0
    print(f"Done in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
