#!/usr/bin/env -S uv run python
"""Fold a later stock universe's IBKR files into the universe that already holds them.

A symbol listed by both sp500 and nasdaq100 used to be downloaded and stored
twice. `stock_home_universe()` now keeps one copy; this removes the old
duplicates under the later universe and repoints its catalog at the shared file.

    uv run python tools/dedupe_stock_universes.py                       # plan only
    uv run python tools/dedupe_stock_universes.py --apply --backup-dir DIR

Per file under `stocks/ibkr/<universe>/<SYMBOL>/`, against the same file in the
symbol's home universe:

  move   the home has no such file          -> the file is moved there
  merge  this copy has rows the home lacks  -> those rows are merged into the home
  drop   the home already has every row     -> the file is deleted

Where both copies carry a row for the same timestamp and the values differ, the
home's row is kept and the count is reported. Every file is copied under
`--backup-dir` first, and nothing is deleted until the home is verified to hold
every timestamp. Run it while no IBKR download is writing.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    DATA_ROOT,
    STOCK_UNIVERSE_PRIORITY,
    load_stock_catalog,
    merge_ohlcv_parquet,
    provider_node_key,
    read_parquet_range,
    save_stock_catalog,
    stock_home_universe,
)
from utils.config import TMP_ROOT, relative_to_repo

PROVIDER = "IBKR"
VALUE_COLUMNS = ["open", "high", "low", "close", "volume"]


@dataclass
class FilePlan:
    src: Path
    dst: Path
    action: str  # move | merge | drop
    only_rows: pd.DataFrame | None = None
    differing: int = 0
    bytes: int = 0


@dataclass
class Totals:
    files: int = 0
    bytes: int = 0
    rows_merged: int = 0
    differing: int = 0
    by_action: dict = field(default_factory=lambda: {"move": 0, "merge": 0, "drop": 0})


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path).sort_values("timestamp").drop_duplicates("timestamp")


def plan_file(src: Path, dst: Path) -> FilePlan:
    """Decide what to do with `src` given its home copy `dst`. Reads, never writes."""
    size = src.stat().st_size
    if not dst.exists():
        return FilePlan(src, dst, "move", bytes=size)
    mine, home = _read(src), _read(dst).set_index("timestamp")
    extra = mine[~mine["timestamp"].isin(home.index)]
    common = mine[mine["timestamp"].isin(home.index)].set_index("timestamp")
    same = np.ones(len(common), dtype=bool)
    for col in VALUE_COLUMNS:
        same &= np.isclose(common[col].to_numpy(float), home.loc[common.index, col].to_numpy(float),
                           rtol=1e-6, equal_nan=True)
    differing = int((~same).sum())
    if len(extra):
        return FilePlan(src, dst, "merge", only_rows=extra, differing=differing, bytes=size)
    return FilePlan(src, dst, "drop", differing=differing, bytes=size)


def fold_file(plan: FilePlan, backup_root: Path) -> None:
    """Back up the source and fold it into the home; the source stays until `remove_source`."""
    backup = backup_root / plan.src.relative_to(DATA_ROOT)
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(plan.src, backup)

    if plan.action == "move":
        plan.dst.parent.mkdir(parents=True, exist_ok=True)
        merge_ohlcv_parquet(plan.src, plan.dst)
    elif plan.action == "merge":
        merge_ohlcv_parquet(plan.only_rows, plan.dst)

    missing = set(_read(backup)["timestamp"]) - set(_read(plan.dst)["timestamp"])
    if missing:
        raise RuntimeError(f"{plan.dst} is missing {len(missing)} timestamps from {plan.src}; source kept")


def remove_source(plan: FilePlan) -> None:
    """Delete the duplicate. Call only once the catalog no longer points at it."""
    plan.src.unlink()
    try:
        plan.src.parent.rmdir()
    except OSError:
        pass  # other files remain in the symbol directory


def repoint_node(catalog: dict, symbol: str, timeframe: str, session: str | None, dst: Path) -> bool:
    """Point an existing catalog node at the shared file, refreshing its dates.

    Edits the node in place so its other fields survive. Returns False when the
    catalog has no such node (nothing to repoint).
    """
    nodes = catalog.get(symbol, {}).get("timeframe_dates", {}).get(timeframe)
    node = nodes.get(provider_node_key(PROVIDER, session)) if isinstance(nodes, dict) else None
    if not node:
        return False
    node["file_path"] = relative_to_repo(dst)
    span = read_parquet_range(dst)
    if span:
        node["start_date"], node["end_date"], _ = span
    return True


def _split_name(path: Path, symbol: str) -> tuple[str | None, str]:
    """`AAPL-all-1h.parquet` -> ('all', '1h'); `AAPL-1d.parquet` -> (None, '1d')."""
    parts = path.stem[len(symbol) + 1:].split("-")
    return (parts[0], parts[1]) if len(parts) == 2 else (None, parts[0])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--universe", default="nasdaq100", choices=STOCK_UNIVERSE_PRIORITY[1:],
                   help="Later universe to fold into the earlier one that lists the symbol")
    p.add_argument("--symbols", nargs="+", help="Limit to these symbols")
    p.add_argument("--apply", action="store_true", help="Make the changes (default: plan only)")
    p.add_argument("--backup-dir", type=Path,
                   help="Where originals are copied first (default: TMP_ROOT/dedupe_<universe>_<UTC stamp>)")
    args = p.parse_args()

    root = DATA_ROOT / "stocks" / PROVIDER.lower() / args.universe
    if not root.is_dir():
        print(f"no {PROVIDER} directory for {args.universe}: {root}", file=sys.stderr)
        return 2
    backup_root = args.backup_dir or TMP_ROOT / f"dedupe_{args.universe}_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"

    catalogs: dict[str, dict] = {}
    catalog = catalogs[args.universe] = load_stock_catalog(args.universe)
    symbols = sorted(d.name for d in root.iterdir() if d.is_dir())
    if args.symbols:
        symbols = [s for s in symbols if s in set(args.symbols)]

    totals, errors, shared = Totals(), [], 0
    for symbol in symbols:
        home = stock_home_universe(symbol, args.universe, catalogs)
        if home == args.universe:
            continue
        shared += 1
        home_dir = DATA_ROOT / "stocks" / PROVIDER.lower() / home / symbol
        folded: list[FilePlan] = []
        for src in sorted((root / symbol).glob("*.parquet")):
            session, timeframe = _split_name(src, symbol)
            try:
                plan = plan_file(src, home_dir / src.name)
                totals.files += 1
                totals.bytes += plan.bytes
                totals.by_action[plan.action] += 1
                totals.differing += plan.differing
                totals.rows_merged += len(plan.only_rows) if plan.only_rows is not None else 0
                if args.apply:
                    fold_file(plan, backup_root)
                    folded.append(plan)
                elif plan.action != "drop" or plan.differing:
                    print(f"  {symbol}/{src.name}: {plan.action}"
                          + (f", {len(plan.only_rows)} rows to merge" if plan.only_rows is not None else "")
                          + (f", {plan.differing} differing rows (home kept)" if plan.differing else ""))
            except Exception as exc:  # keep going; the source stays in place
                errors.append((symbol, src.name, str(exc)))
                print(f"  {symbol}/{src.name}: FAILED ({exc})")
        if folded:
            for plan in folded:
                session, timeframe = _split_name(plan.src, symbol)
                repoint_node(catalog, symbol, timeframe, session, plan.dst)
            save_stock_catalog(args.universe, catalog)
            for plan in folded:
                remove_source(plan)

    mode = "applied" if args.apply else "plan (nothing changed)"
    print(f"\n{mode}: {shared} shared symbols, {totals.files} files, {totals.bytes / 1e6:.1f} MB")
    print(f"  actions: {totals.by_action}; rows merged into home: {totals.rows_merged}; "
          f"rows differing from home: {totals.differing}")
    if args.apply:
        print(f"  originals backed up under {backup_root}")
    for symbol, name, message in errors[:20]:
        print(f"  - {symbol}/{name}: {message}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
