#!/usr/bin/env -S uv run python
"""One-off migration: restructure price-data/ to a uniform provider-first layout.

See /home/grenada/.claude/plans/virtual-launching-sphinx.md for the full design.
Run phases in order: A B C D E F G. Each phase is idempotent (safe to re-run)
and supports --dry-run. Safety rule: copy -> verify -> delete original, never
the reverse.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    REPO_ROOT,
    DATA_ROOT,
    normalize_ohlcv,
    read_parquet_range,
    load_stock_catalog,
    save_stock_catalog,
    stock_file_path,
    update_stock_timeframe_entry,
)

import yaml

BASELINE_CSV = REPO_ROOT / "tmp" / "migration_baseline.csv"

STOCK_UNIVERSES = ["sp500", "nasdaq", "russell2000"]  # old file/folder names as they exist today


def log(msg: str) -> None:
    print(msg)


def _migrate_parquet_file(src: Path, dst: Path, dry_run: bool) -> tuple[str, str, int] | None:
    """Normalize src (any recognized schema) into canonical parquet at dst, verify, delete src.
    Returns (start_date, end_date, row_count) of dst, or None if src doesn't exist."""
    if not src.exists():
        return None
    if dst.exists() and not src.exists():
        return read_parquet_range(dst)

    raw = pd.read_parquet(src)
    normed = normalize_ohlcv(raw)
    if dry_run:
        if normed.empty:
            return None
        ts = normed["timestamp"]
        import datetime
        start = datetime.datetime.fromtimestamp(ts.iloc[0] / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
        end = datetime.datetime.fromtimestamp(ts.iloc[-1] / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
        return start, end, len(normed)

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".parquet.tmp")
    normed.to_parquet(tmp, index=False)
    tmp.replace(dst)

    result = read_parquet_range(dst)
    if result is None:
        raise RuntimeError(f"verification failed reading back {dst}")
    src.unlink()
    return result


def _migrate_csv_file(src: Path, dst: Path, dry_run: bool) -> tuple[str, str, int] | None:
    if not src.exists():
        return None
    raw = pd.read_csv(src, parse_dates=["Date"])
    normed = normalize_ohlcv(raw)
    if dry_run:
        if normed.empty:
            return None
        ts = normed["timestamp"]
        import datetime
        start = datetime.datetime.fromtimestamp(ts.iloc[0] / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
        end = datetime.datetime.fromtimestamp(ts.iloc[-1] / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
        return start, end, len(normed)

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".parquet.tmp")
    normed.to_parquet(tmp, index=False)
    tmp.replace(dst)

    result = read_parquet_range(dst)
    if result is None:
        raise RuntimeError(f"verification failed reading back {dst}")
    src.unlink()
    return result


# ---------------------------------------------------------------------------
# Phase A: dedupe BF.B/BRK.B, remove dead ETF entries, snapshot baseline
# ---------------------------------------------------------------------------

DUPLICATE_PAIRS = [("BF.B", "BF-B"), ("BRK.B", "BRK-B")]
DEAD_ETF_KEYS = ["SPY", "QQQ", "GLD", "TLT", "ARKK", "IWM"]


def phase_a(dry_run: bool) -> None:
    log("=== Phase A: dedupe + dead ETF cleanup + baseline snapshot ===")

    # --- dedupe BF.B/BRK.B ---
    catalog = load_stock_catalog("sp500")
    for dot_key, dash_key in DUPLICATE_PAIRS:
        dot_entry = catalog.get(dot_key)
        dash_entry = catalog.get(dash_key)
        if dot_entry is None:
            log(f"  {dot_key}: no duplicate entry found, skipping")
            continue
        if dash_entry is None:
            log(f"  WARNING: {dot_key} has no canonical {dash_key} entry — not deleting, needs manual review")
            continue

        # Verify every dot-key file is a true duplicate (same rows) of the dash-key file
        # before deleting anything.
        dot_files = []
        for tf, node in dot_entry.get("timeframe_dates", {}).items():
            fp = REPO_ROOT / node["file_path"]
            dash_fp_node = dash_entry.get("timeframe_dates", {}).get(tf)
            dot_files.append((tf, fp, dash_fp_node))

        for tf, fp, dash_node in dot_files:
            if not fp.exists():
                continue
            if dash_node is None:
                log(f"  WARNING: {dot_key}/{tf} has no counterpart in {dash_key} — not deleting {fp}, needs manual review")
                continue
            dash_fp = REPO_ROOT / dash_node["file_path"]
            dot_range = read_parquet_range(fp)
            dot_raw_rows = len(pd.read_parquet(fp))
            dash_range = read_parquet_range(dash_fp) if dash_fp.exists() else None
            is_dup = dot_raw_rows == 0 or dot_range == dash_range  # empty placeholder, or identical data
            if not is_dup:
                log(f"  WARNING: {dot_key}/{tf} ({dot_range}) differs from {dash_key}/{tf} ({dash_range}) — not deleting, needs manual review")
                continue
            log(f"  {dot_key}/{tf}: confirmed duplicate/empty of {dash_key}, {'would delete' if dry_run else 'deleting'} {fp}")
            if not dry_run:
                fp.unlink()

    if not dry_run:
        for dot_key, _ in DUPLICATE_PAIRS:
            catalog.pop(dot_key, None)
        save_stock_catalog("sp500", catalog)
        # clean up now-empty IBKR directories for the dot-form symbols
        for dot_key, _ in DUPLICATE_PAIRS:
            d = DATA_ROOT / "stocks" / "ibkr" / "sp500" / dot_key
            if d.exists() and not any(d.iterdir()):
                d.rmdir()
        log("  sp500.yaml saved with duplicates removed")
    else:
        log("  [dry-run] would remove BF.B/BRK.B catalog keys and their files")

    # --- remove dead ETF entries from assets.yaml ---
    assets_path = DATA_ROOT / "assets.yaml"
    with open(assets_path) as f:
        assets = yaml.safe_load(f)
    removed = [k for k in DEAD_ETF_KEYS if k in assets]
    log(f"  dead ETF entries found in assets.yaml: {removed}")
    if not dry_run and removed:
        for k in removed:
            del assets[k]
        tmp = assets_path.with_suffix(".yaml.tmp")
        with open(tmp, "w") as f:
            yaml.dump(assets, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        tmp.replace(assets_path)
        log(f"  removed {len(removed)} dead ETF entries from assets.yaml")

    # --- baseline snapshot ---
    if dry_run:
        log("  [dry-run] skipping baseline snapshot")
        return

    BASELINE_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows = []

    for universe in STOCK_UNIVERSES:
        cat = load_stock_catalog(universe)
        for symbol, entry in cat.items():
            for tf, node in entry.get("timeframe_dates", {}).items():
                fp = REPO_ROOT / node["file_path"]
                result = read_parquet_range(fp) if fp.exists() else None
                rows.append({
                    "asset_class": f"stock:{universe}",
                    "symbol": symbol,
                    "timeframe": tf,
                    "provider": node.get("file_path", "").split("/")[2] if "ibkr" in node.get("file_path", "") else "YF",
                    "row_count": result[2] if result else "",
                    "start_date": result[0] if result else "",
                    "end_date": result[1] if result else "",
                    "file_path": node.get("file_path", ""),
                })

    # indexes (inline in assets.yaml)
    for symbol, entry in assets.items():
        if not isinstance(entry, dict):
            continue
        if "Index" not in entry.get("categories", []):
            continue
        for tf, node in entry.get("timeframe_dates", {}).items():
            fp = REPO_ROOT / node["file_path"]
            result = read_parquet_range(fp) if fp.suffix == ".parquet" and fp.exists() else None
            rows.append({
                "asset_class": "index", "symbol": symbol, "timeframe": tf, "provider": "YF",
                "row_count": result[2] if result else "", "start_date": result[0] if result else "",
                "end_date": result[1] if result else "", "file_path": node.get("file_path", ""),
            })

    # commodities / macro
    for cat_name, yaml_name in [("commodity", "commodities.yaml"), ("macro", "macro.yaml")]:
        with open(DATA_ROOT / yaml_name) as f:
            cat = yaml.safe_load(f) or {}
        for symbol, entry in cat.items():
            for tf, node in entry.get("timeframe_dates", {}).items():
                fp = REPO_ROOT / node["file_path"]
                result = read_parquet_range(fp) if fp.exists() else None
                rows.append({
                    "asset_class": cat_name, "symbol": symbol, "timeframe": tf, "provider": "YF",
                    "row_count": result[2] if result else "", "start_date": result[0] if result else "",
                    "end_date": result[1] if result else "", "file_path": node.get("file_path", ""),
                })

    with open(BASELINE_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["asset_class", "symbol", "timeframe", "provider", "row_count", "start_date", "end_date", "file_path"])
        writer.writeheader()
        writer.writerows(rows)
    log(f"  baseline snapshot written: {BASELINE_CSV} ({len(rows)} rows)")


# ---------------------------------------------------------------------------
# Phase B: 27 stray YF stock files -> delisted_stocks.yaml / stocks/yf/delisted_stocks/
# ---------------------------------------------------------------------------

DELISTED_UNIVERSE = "delisted_stocks"


def phase_b(dry_run: bool) -> None:
    log("=== Phase B: stray YF files -> delisted_stocks bucket ===")
    stray_dir = DATA_ROOT / "stocks"
    stray_files = sorted(stray_dir.glob("*.parquet"))
    log(f"  found {len(stray_files)} stray files")

    catalog = load_stock_catalog(DELISTED_UNIVERSE) if not dry_run else {}
    migrated = 0

    for src in stray_files:
        symbol = src.stem
        dst = stock_file_path("YF", DELISTED_UNIVERSE, symbol, "1d")
        if dst.exists() and not src.exists():
            continue  # already migrated
        result = _migrate_parquet_file(src, dst, dry_run)
        if result is None:
            log(f"  {symbol}: SKIP (unreadable or empty)")
            continue
        start, end, rows = result
        log(f"  {symbol}: {'would migrate' if dry_run else 'migrated'} -> {dst.relative_to(REPO_ROOT)} ({rows} rows, {start}..{end})")
        migrated += 1
        if not dry_run:
            update_stock_timeframe_entry(
                catalog,
                symbol=symbol,
                timeframe="1d",
                provider="YF",
                file_path=dst,
                start_date=start,
                end_date=end,
                categories=["Stock", "Delisted"],
            )

    if not dry_run:
        save_stock_catalog(DELISTED_UNIVERSE, catalog)
        log(f"  {DELISTED_UNIVERSE}.yaml saved with {len(catalog)} entries")
    log(f"  {'would migrate' if dry_run else 'migrated'} {migrated}/{len(stray_files)} files")


# ---------------------------------------------------------------------------
# Phase C: bulk YF stock migration (sp500, nasdaq100, russell2000)
# ---------------------------------------------------------------------------

# (old folder/yaml name, new universe name) — nasdaq is renamed to nasdaq100
# to match the name used everywhere else (--universe nasdaq100).
UNIVERSE_RENAMES = [("sp500", "sp500"), ("nasdaq", "nasdaq100"), ("russell2000", "russell2000")]


def phase_c(dry_run: bool) -> None:
    log("=== Phase C: bulk YF stock migration ===")
    for old_name, new_name in UNIVERSE_RENAMES:
        old_catalog = load_stock_catalog(old_name)
        old_dir = DATA_ROOT / "stocks" / old_name
        files = sorted(old_dir.glob("*.parquet")) if old_dir.exists() else []
        log(f"  --- {old_name} -> {new_name}: {len(files)} files, {len(old_catalog)} catalog entries ---")

        new_catalog = load_stock_catalog(new_name) if new_name != old_name else {}
        if not dry_run and new_name == old_name:
            # rebuilding sp500.yaml/russell2000.yaml in place — start fresh, IBKR
            # entries get re-added in Phase D from the actual files on disk.
            new_catalog = {}

        migrated = 0
        for src in files:
            symbol = src.stem
            old_entry = old_catalog.get(symbol, {})
            dst = stock_file_path("YF", new_name, symbol, "1d")
            result = _migrate_parquet_file(src, dst, dry_run)
            if result is None:
                log(f"    {symbol}: SKIP (unreadable or empty)")
                continue
            start, end, rows = result
            migrated += 1
            if not dry_run:
                update_stock_timeframe_entry(
                    new_catalog,
                    symbol=symbol,
                    timeframe="1d",
                    provider="YF",
                    file_path=dst,
                    start_date=start,
                    end_date=end,
                    categories=old_entry.get("categories"),
                    common_name=old_entry.get("common_name"),
                    sector=old_entry.get("sector"),
                )

        log(f"    {'would migrate' if dry_run else 'migrated'} {migrated}/{len(files)} files")
        if not dry_run:
            save_stock_catalog(new_name, new_catalog)
            log(f"    {new_name}.yaml saved with {len(new_catalog)} entries")
            if old_name != new_name:
                old_path = old_dir
                if old_path.exists() and not any(old_path.iterdir()):
                    old_path.rmdir()
                old_yaml = DATA_ROOT / f"{old_name}.yaml"
                if old_yaml.exists():
                    old_yaml.unlink()


# ---------------------------------------------------------------------------
# Phase D: rewrite IBKR stock catalog entries into the nested schema
# ---------------------------------------------------------------------------

def phase_d(dry_run: bool) -> None:
    log("=== Phase D: IBKR stock catalog rewrite ===")
    for old_name, new_name in UNIVERSE_RENAMES:
        old_ibkr_dir = DATA_ROOT / "stocks" / "ibkr" / old_name
        new_ibkr_dir = DATA_ROOT / "stocks" / "ibkr" / new_name
        if old_name != new_name and old_ibkr_dir.exists() and not dry_run:
            new_ibkr_dir.parent.mkdir(parents=True, exist_ok=True)
            old_ibkr_dir.rename(new_ibkr_dir)
        scan_dir = new_ibkr_dir if new_ibkr_dir.exists() else old_ibkr_dir
        if not scan_dir.exists():
            log(f"  --- {new_name}: no IBKR data, skipping ---")
            continue

        symbol_dirs = sorted(d for d in scan_dir.iterdir() if d.is_dir())
        log(f"  --- {new_name}: {len(symbol_dirs)} symbols under {scan_dir.relative_to(REPO_ROOT)} ---")

        catalog = load_stock_catalog(new_name)
        registered = 0
        for sym_dir in symbol_dirs:
            symbol = sym_dir.name
            for fp in sorted(sym_dir.glob("*.parquet")):
                # filename is "<symbol>-<session>-<timeframe>.parquet"; parse using
                # the known symbol prefix since symbols themselves may contain dashes.
                remainder = fp.stem[len(symbol) + 1:]  # e.g. "all-1h"
                session, _, timeframe = remainder.partition("-")
                result = read_parquet_range(fp)
                if result is None:
                    log(f"    {symbol}/{timeframe}: SKIP (unreadable or empty) {fp.name}")
                    continue
                start, end, rows = result
                if not dry_run:
                    update_stock_timeframe_entry(
                        catalog,
                        symbol=symbol,
                        timeframe=timeframe,
                        provider="IBKR",
                        file_path=fp,
                        start_date=start,
                        end_date=end,
                        session=session,
                    )
                registered += 1

        log(f"    {'would register' if dry_run else 'registered'} {registered} (symbol, timeframe) nodes")
        if not dry_run:
            save_stock_catalog(new_name, catalog)
            log(f"    {new_name}.yaml saved with {len(catalog)} entries")


# ---------------------------------------------------------------------------
# Phase E: indexes -> provider-first parquet + new indexes.yaml
# ---------------------------------------------------------------------------

INDEX_SYMBOLS = ["STOXX50E", "DJI", "HSI", "FTSE", "N225", "GSPC", "GDAXI", "RUT", "IXIC", "NDX", "VIX"]


def _flat_index_entry(old_entry: dict, symbol: str, start: str, end: str, dst: Path) -> dict:
    return {
        "name": symbol,
        "common_name": old_entry.get("common_name", symbol),
        "description": old_entry.get("description", f"Historical data for {symbol}"),
        "hide_from_frontend": old_entry.get("hide_from_frontend", False),
        "provider": "YF",
        "categories": ["Index"],
        "timeframes": ["1d"],
        "timeframe_dates": {
            "1d": {"start_date": start, "end_date": end, "file_path": str(dst.relative_to(REPO_ROOT))}
        },
        "start_date": start,
        "end_date": end,
    }


def phase_e(dry_run: bool) -> None:
    log("=== Phase E: indexes -> provider-first ===")
    assets_path = DATA_ROOT / "assets.yaml"
    with open(assets_path) as f:
        assets = yaml.safe_load(f)

    indexes_yaml = {}
    old_dir = DATA_ROOT / "indexes"

    for symbol in INDEX_SYMBOLS:
        old_entry = assets.get(symbol, {})
        csv_src = old_dir / f"{symbol}.csv"
        parquet_src = old_dir / f"{symbol}.parquet"
        dst = DATA_ROOT / "indexes" / "yf" / symbol / f"{symbol}-1d.parquet"

        frames = []
        for src, is_csv in [(csv_src, True), (parquet_src, False)]:
            if not src.exists():
                continue
            raw = pd.read_csv(src, parse_dates=["Date"]) if is_csv else pd.read_parquet(src)
            frames.append(normalize_ohlcv(raw))

        if not frames:
            log(f"  {symbol}: SKIP (no source files found)")
            continue

        merged = pd.concat(frames).drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
        if merged.empty:
            log(f"  {symbol}: SKIP (empty after merge)")
            continue

        import datetime
        ts = merged["timestamp"]
        start = datetime.datetime.fromtimestamp(ts.iloc[0] / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")
        end = datetime.datetime.fromtimestamp(ts.iloc[-1] / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")

        log(f"  {symbol}: {len(frames)} source file(s) -> {'would write' if dry_run else 'writing'} {dst.relative_to(REPO_ROOT)} ({len(merged)} rows, {start}..{end})")
        if dry_run:
            continue

        dst.parent.mkdir(parents=True, exist_ok=True)
        tmp = dst.with_suffix(".parquet.tmp")
        merged.to_parquet(tmp, index=False)
        tmp.replace(dst)

        result = read_parquet_range(dst)
        if result is None:
            raise RuntimeError(f"verification failed reading back {dst}")
        if csv_src.exists():
            csv_src.unlink()
        if parquet_src.exists():
            parquet_src.unlink()

        indexes_yaml[symbol] = _flat_index_entry(old_entry, symbol, start, end, dst)

    if dry_run:
        log("  [dry-run] would create indexes.yaml and remove inline index entries from assets.yaml")
        return

    indexes_path = DATA_ROOT / "indexes.yaml"
    tmp = indexes_path.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(indexes_yaml, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    tmp.replace(indexes_path)
    log(f"  indexes.yaml saved with {len(indexes_yaml)} entries")

    for symbol in INDEX_SYMBOLS:
        assets.pop(symbol, None)
    tmp = assets_path.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(assets, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    tmp.replace(assets_path)
    log("  removed inline index entries from assets.yaml")


# ---------------------------------------------------------------------------
# Phase F: commodities / macro -> provider-first paths (schema unchanged)
# ---------------------------------------------------------------------------

COMMODITY_MACRO = [
    ("commodities.yaml", "commodities", "GC=F"),
    ("macro.yaml", "macro", "WTB3MS"),
]


def phase_f(dry_run: bool) -> None:
    log("=== Phase F: commodities / macro -> provider-first ===")
    for yaml_name, asset_class, symbol in COMMODITY_MACRO:
        yaml_path = DATA_ROOT / yaml_name
        with open(yaml_path) as f:
            cat = yaml.safe_load(f) or {}
        entry = cat.get(symbol)
        if entry is None:
            log(f"  {symbol}: SKIP (not found in {yaml_name})")
            continue

        old_fp = REPO_ROOT / entry["timeframe_dates"]["1d"]["file_path"]
        new_fp = DATA_ROOT / asset_class / "yf" / symbol / f"{symbol}-1d.parquet"

        if old_fp == new_fp:
            log(f"  {symbol}: already migrated, skipping")
            continue
        if not old_fp.exists():
            log(f"  {symbol}: SKIP (source file missing: {old_fp})")
            continue

        log(f"  {symbol}: {'would move' if dry_run else 'moving'} {old_fp.relative_to(REPO_ROOT)} -> {new_fp.relative_to(REPO_ROOT)}")
        if dry_run:
            continue

        new_fp.parent.mkdir(parents=True, exist_ok=True)
        # Not all series here are OHLCV-shaped (macro's WTB3MS is a single-column
        # rate series with the date in the index) — do a schema-agnostic copy and
        # verify by row count rather than assuming normalize_ohlcv applies.
        raw = pd.read_parquet(old_fp)
        raw.to_parquet(new_fp)
        readback = pd.read_parquet(new_fp)
        if len(readback) != len(raw):
            raise RuntimeError(f"verification failed: {new_fp} has {len(readback)} rows, expected {len(raw)}")
        old_fp.unlink()

        entry["timeframe_dates"]["1d"]["file_path"] = str(new_fp.relative_to(REPO_ROOT))
        tmp = yaml_path.with_suffix(".yaml.tmp")
        with open(tmp, "w") as f:
            yaml.dump(cat, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        tmp.replace(yaml_path)
        log(f"  {symbol}: {yaml_name} updated")


# ---------------------------------------------------------------------------
# Phase G: assets.yaml include-list fixes + stray-inline-entry cleanup
# ---------------------------------------------------------------------------

NEW_INCLUDES = [
    "sp500.yaml",
    "nasdaq100.yaml",
    "russell2000.yaml",
    "delisted_stocks.yaml",
    "indexes.yaml",
    "crypto_binance.yaml",
    "crypto_bitget.yaml",
    "crypto_kucoin.yaml",
    "crypto_bybit.yaml",
    "crypto_futures_binance.yaml",
    "crypto_futures_bybit.yaml",
    "crypto_futures_kucoin.yaml",
    "commodities.yaml",
    "macro.yaml",
]


def phase_g(dry_run: bool) -> None:
    log("=== Phase G: assets.yaml include-list + stray-entry cleanup ===")
    assets_path = DATA_ROOT / "assets.yaml"
    with open(assets_path) as f:
        assets = yaml.safe_load(f)

    old_include = assets.get("include", [])
    log(f"  old include list: {old_include}")
    log(f"  new include list: {NEW_INCLUDES}")

    delisted = load_stock_catalog(DELISTED_UNIVERSE)
    stray_inline_keys = [k for k in delisted if k in assets]
    log(f"  stray inline duplicate entries to remove: {stray_inline_keys}")

    if dry_run:
        log("  [dry-run] no changes written")
        return

    assets["include"] = NEW_INCLUDES
    for k in stray_inline_keys:
        del assets[k]

    tmp = assets_path.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(assets, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    tmp.replace(assets_path)
    log("  assets.yaml saved")

    from utils.data import load_assets
    merged = load_assets()
    log(f"  load_assets() resolved {len(merged)} total symbols")


PHASES = {
    "A": phase_a,
    "B": phase_b,
    "C": phase_c,
    "D": phase_d,
    "E": phase_e,
    "F": phase_f,
    "G": phase_g,
}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--phase", required=True, choices=sorted(PHASES))
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    PHASES[args.phase](args.dry_run)


if __name__ == "__main__":
    main()
