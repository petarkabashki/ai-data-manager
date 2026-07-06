#!/usr/bin/env -S uv run python
"""Verify all OHLCV-classed parquet files match the canonical schema on disk.

Canonical schema: timestamp (int64 ms UTC) + open/high/low/close/volume (float64).
Scans crypto, crypto-futures, stocks (yf+ibkr), indexes, and commodities — every
asset class documented as OHLCV-shaped in agents.md. Macro (`WTB3MS`, a plain
rate series) and market-cap (CoinGecko snapshots) are intentionally excluded —
they have different, documented schemas and were never meant to match this one.

Read-only: never modifies files. Run it after adding a new tool/provider to
catch anything that wrote raw provider output straight to disk without
routing through normalize_ohlcv().

    uv run python tools/check_schema.py
    uv run python tools/check_schema.py --path price-data/commodities
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import CANONICAL_COLS, DATA_ROOT, REPO_ROOT

CANONICAL_TYPES = {
    "timestamp": "int64",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "volume": "double",
}

OHLCV_ROOTS = [
    DATA_ROOT / "cryptos",
    DATA_ROOT / "crypto-futures",
    DATA_ROOT / "stocks",
    DATA_ROOT / "indexes",
    DATA_ROOT / "commodities",
]


def check_file(path: Path) -> str | None:
    """Return None if the file matches the canonical schema, else a description of the mismatch."""
    try:
        schema = pq.read_schema(path)
    except Exception as e:
        return f"unreadable: {e}"

    if len(schema.names) == 0:
        return None  # empty parquet — tolerated (marks a failed download)

    if list(schema.names) != CANONICAL_COLS:
        return f"columns {list(schema.names)} != {CANONICAL_COLS}"

    for name in CANONICAL_COLS:
        actual = str(schema.field(name).type)
        expected = CANONICAL_TYPES[name]
        if actual != expected:
            return f"{name}: dtype {actual!r} != {expected!r}"

    return None


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--path", help="Limit to one subdirectory (e.g. price-data/commodities) instead of all OHLCV classes")
    args = p.parse_args()

    roots = [Path(args.path)] if args.path else OHLCV_ROOTS
    all_files: list[Path] = []
    for root in roots:
        if root.exists():
            all_files.extend(root.rglob("*.parquet"))

    print(f"Checking {len(all_files)} parquet file(s) against canonical OHLCV schema…\n")

    mismatches: list[tuple[Path, str]] = []
    for f in sorted(all_files):
        issue = check_file(f)
        if issue:
            mismatches.append((f, issue))

    if not mismatches:
        print("All files match the canonical schema.")
        return

    print(f"{len(mismatches)} file(s) do not match:\n")
    for f, issue in mismatches:
        print(f"  {f.relative_to(REPO_ROOT)}: {issue}")
    sys.exit(1)


if __name__ == "__main__":
    main()
