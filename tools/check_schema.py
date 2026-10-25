#!/usr/bin/env -S uv run python
"""Verify all OHLCV-classed parquet files match the canonical schema on disk.

Canonical schema: timestamp (int64 ms UTC) + open/high/low/close/volume (float64).
Scans crypto, crypto-futures, stocks (yf+ibkr), indexes, and commodities — every
asset class documented as OHLCV-shaped in agents.md. Macro (`WTB3MS`, a plain
rate series) and market-cap (CoinGecko snapshots) are intentionally excluded —
they have different, documented schemas and were never meant to match this one.
Funding-rate files (`<SYMBOL>-funding.parquet` under crypto-futures) are checked
against their own schema, `utils.catalog.FUNDING_COLS`, instead.

Read-only: never modifies files. Run it after adding a new tool/provider to
catch anything that wrote raw provider output straight to disk without
routing through normalize_ohlcv().

    uv run python tools/check_schema.py
    uv run python tools/check_schema.py --path price-data/commodities
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    CANONICAL_ARROW_TYPES,
    CANONICAL_COLS,
    DATA_ROOT,
    FUNDING_ARROW_TYPES,
    FUNDING_COLS,
    FUNDING_TIMEFRAME,
    REPO_ROOT,
)
from utils.report import allow_broken_pipe

OHLCV_ROOTS = [
    DATA_ROOT / "cryptos",
    DATA_ROOT / "crypto-futures",
    DATA_ROOT / "stocks",
    DATA_ROOT / "indexes",
    DATA_ROOT / "commodities",
]


def _display_path(path: Path) -> str:
    """Repo-relative when possible, absolute otherwise."""
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def check_file(path: Path) -> str | None:
    """Return None if the file matches the canonical schema, else a description of the mismatch."""
    try:
        schema = pq.read_schema(path)
    except Exception as e:
        return f"unreadable: {e}"

    if len(schema.names) == 0:
        return None  # empty parquet — tolerated (marks a failed download)

    if path.name.endswith(f"-{FUNDING_TIMEFRAME}.parquet"):
        cols, types = FUNDING_COLS, FUNDING_ARROW_TYPES
    else:
        cols, types = CANONICAL_COLS, CANONICAL_ARROW_TYPES
    if list(schema.names) != cols:
        return f"columns {list(schema.names)} != {cols}"

    for name in cols:
        actual = str(schema.field(name).type)
        expected = types[name]
        if actual != expected:
            return f"{name}: dtype {actual!r} != {expected!r}"

    return None


def main() -> None:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--path", help="Limit to one subdirectory (e.g. price-data/commodities) instead of all OHLCV classes")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON instead of a table")
    args = p.parse_args()

    # Resolve --path so a relative argument still prints (relative_to(REPO_ROOT)
    # on a relative path raised ValueError — only on the mismatch branch, i.e.
    # exactly when the output was needed).
    roots = [Path(args.path).resolve()] if args.path else OHLCV_ROOTS
    all_files: list[Path] = []
    for root in roots:
        if root.exists():
            all_files.extend(root.rglob("*.parquet"))

    if not args.json:
        print(f"Checking {len(all_files)} parquet file(s) against canonical OHLCV schema…\n")

    mismatches: list[tuple[Path, str]] = []
    for f in sorted(all_files):
        issue = check_file(f)
        if issue:
            mismatches.append((f, issue))

    if args.json:
        json.dump(
            {
                "checked": len(all_files),
                "mismatches": [
                    {"path": _display_path(f), "issue": issue} for f, issue in mismatches
                ],
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    elif not mismatches:
        print("All files match the canonical schema.")
    else:
        print(f"{len(mismatches)} file(s) do not match:\n")
        for f, issue in mismatches:
            print(f"  {_display_path(f)}: {issue}")

    if mismatches:
        sys.exit(1)


if __name__ == "__main__":
    main()
