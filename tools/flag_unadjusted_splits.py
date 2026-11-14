#!/usr/bin/env -S uv run python
"""Record the split jumps IBKR left in its daily stock files, on their catalog nodes.

IBKR `TRADES` doesn't back-adjust splits from about 2000-2005, so those files
drop by the split ratio in one day (`ibkr/sp500/XOM` on 2001-07-19). The data
stays as IBKR serves it (owner decision 2026-11-13); the catalog says where:

    1d:
      IBKR:
        file_path: price-data/stocks/ibkr/sp500/XOM/XOM-all-1d.parquet
        adjustment: raw
        unadjusted_splits: ["2001-07-19"]

The dates come from a `check_adjustment.py --json` report: each IBKR row's
`split_jumps`. Every IBKR node in every stock catalog that points at a reported
file is updated, since one IBKR file serves several universes. A node whose file
the report audited without finding a jump loses the field; a node the report
didn't cover is left alone.

Dry run by default: prints each node it would change. `--apply` writes the
catalogs, each under its lock.

    uv run python tools/flag_unadjusted_splits.py --audit adj.json
    uv run python tools/flag_unadjusted_splits.py --audit adj.json --apply
    uv run python tools/flag_unadjusted_splits.py --audit adj.json --add-only --apply

`--add-only` never removes a date: use it when flagging new jumps, since the
jump detector can miss one already on record (CBAN 2005-05-17, 2026-11-14).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (  # noqa: E402
    STOCK_UNIVERSES, iter_timeframe_nodes, load_stock_catalog, run_with_catalog_backup,
    stock_catalog_path, stock_catalog_txn,
)
from utils.report import allow_broken_pipe  # noqa: E402

FIELD = "unadjusted_splits"


def splits_by_file(report: dict) -> dict[str, list[str]]:
    """IBKR file_path → its jump dates; [] for a file audited without a jump."""
    out: dict[str, list[str]] = {}
    for row in report["files"]:
        if row["provider"] != "IBKR" or row.get("split") == "unverifiable_no_overlap":
            continue
        out[row["file_path"]] = sorted({s["date"] for s in row.get("split_jumps", [])})
    return out


def apply_flags(
    catalog: dict, flags: dict[str, list[str]], add_only: bool = False,
) -> list[tuple[str, str, list | None, list]]:
    """Set or clear the field on matching IBKR nodes. Returns (symbol, file, old, new) per change.

    `add_only` keeps every date already on the node and only adds the report's:
    the jump detector can miss a recorded jump, which must not be dropped.
    """
    changes = []
    for key, entry in catalog.items():
        if not isinstance(entry, dict):
            continue
        for _tf, node_key, node in iter_timeframe_nodes(entry):
            provider = (node.get("provider") or node_key.split(":")[0]).upper()
            fp = node.get("file_path")
            if provider != "IBKR" or fp not in flags:
                continue
            old, new = node.get(FIELD), flags[fp]
            if add_only:
                new = sorted(set(old or []) | set(new))
            if (old or []) == new:
                continue
            if new:
                node[FIELD] = new
            else:
                node.pop(FIELD, None)
            changes.append((entry.get("name", key), fp, old, new))
    return changes


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--audit", type=Path, required=True, help="A check_adjustment.py --json report")
    p.add_argument("--catalog", nargs="+", choices=STOCK_UNIVERSES, default=STOCK_UNIVERSES)
    p.add_argument("--add-only", action="store_true",
                   help="Only add dates; keep every date already recorded (the detector can miss one)")
    p.add_argument("--apply", action="store_true", help="Back up, then write the catalogs")
    p.add_argument("--catalog-backup-dir", type=Path,
                   help="Default: TMP_ROOT/catalog-backup-flag_unadjusted_splits-<UTC stamp>")
    args = p.parse_args(argv)

    flags = splits_by_file(json.loads(args.audit.read_text()))
    if args.apply:
        return run_with_catalog_backup("flag_unadjusted_splits",
                                       [stock_catalog_path(u) for u in args.catalog],
                                       lambda: write(args, flags), args.catalog_backup_dir)
    return write(args, flags)


def write(args: argparse.Namespace, flags: dict[str, list[str]]) -> int:
    total = 0
    for universe in args.catalog:
        if args.apply:
            with stock_catalog_txn(universe) as fresh:
                changes = apply_flags(fresh, flags, args.add_only)
        else:
            changes = apply_flags(load_stock_catalog(universe), flags, args.add_only)
        for symbol, fp, old, new in changes:
            print(f"{universe:<16} {symbol:<8} {old or '-'} -> {new or '-'}  {fp}")
        total += len(changes)
    flagged = sum(1 for dates in flags.values() if dates)
    verb = "changed" if args.apply else "would change (dry run; add --apply)"
    print(f"\n{flagged} IBKR files with unadjusted splits; {total} nodes {verb}")
    return 0


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
