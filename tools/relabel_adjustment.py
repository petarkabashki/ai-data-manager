#!/usr/bin/env -S uv run python
"""Set the `adjustment` label on stock catalog nodes, for files whose label is wrong.

check_adjustment.py finds files whose bars disagree with their label: a YF file
labelled raw whose closes are split- and dividend-adjusted (sp500/WBD), say.
When the owner rules the bars stay as they are, the label changes instead:

    uv run python tools/relabel_adjustment.py --adjustment adjusted \\
        --files price-data/stocks/yf/sp500/WBD/WBD-1d.parquet

Every node, in every stock catalog, that points at a named file is relabelled.
Dry run by default. `--apply` backs up the catalogs first and writes nothing if
that backup fails.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (  # noqa: E402
    STOCK_UNIVERSES, iter_timeframe_nodes, load_stock_catalog, run_with_catalog_backup,
    stock_catalog_path, stock_catalog_txn,
)
from utils.report import allow_broken_pipe  # noqa: E402

LABELS = ("raw", "adjusted")


def relabel(catalog: dict, files: set[str], label: str) -> list[tuple[str, str, str | None]]:
    """Set `adjustment` on nodes whose file_path is in `files`. Returns (symbol, file, old label)."""
    changes = []
    for key, entry in catalog.items():
        if not isinstance(entry, dict):
            continue
        for _tf, _node_key, node in iter_timeframe_nodes(entry):
            fp = node.get("file_path")
            if fp in files and node.get("adjustment") != label:
                changes.append((entry.get("name", key), fp, node.get("adjustment")))
                node["adjustment"] = label
    return changes


def write(args: argparse.Namespace) -> int:
    files, total = set(args.files), 0
    for universe in args.catalog:
        if args.apply:
            with stock_catalog_txn(universe) as fresh:
                changes = relabel(fresh, files, args.adjustment)
        else:
            changes = relabel(load_stock_catalog(universe), files, args.adjustment)
        for symbol, fp, old in changes:
            print(f"{universe:<16} {symbol:<8} {old or '-'} -> {args.adjustment}  {fp}")
        total += len(changes)
    verb = "changed" if args.apply else "would change (dry run; add --apply)"
    print(f"\n{total} nodes {verb}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--files", nargs="+", required=True, help="file_paths, as stored in the catalog")
    p.add_argument("--adjustment", choices=LABELS, required=True)
    p.add_argument("--catalog", nargs="+", choices=STOCK_UNIVERSES, default=STOCK_UNIVERSES)
    p.add_argument("--apply", action="store_true", help="Back up, then write the catalogs")
    p.add_argument("--catalog-backup-dir", type=Path,
                   help="Default: TMP_ROOT/catalog-backup-relabel_adjustment-<UTC stamp>")
    args = p.parse_args(argv)
    if args.apply:
        return run_with_catalog_backup("relabel_adjustment", [stock_catalog_path(u) for u in args.catalog],
                                       lambda: write(args), args.catalog_backup_dir)
    return write(args)


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
