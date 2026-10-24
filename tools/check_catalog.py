#!/usr/bin/env -S uv run python
"""Audit every catalog against what's actually on disk, and against itself.

`check_schema.py` verifies that a parquet file has the right columns.
This verifies the layer above it: that the catalog says who produced each
series, what its numbers mean, and where it lives — and that the file is
really there.

Checks, per node:

  * semantics    — schema / adjustment / volume_basis / bar_label present and
                   drawn from their vocabularies (see utils/semantics.py)
  * file_missing — `file_path` points at nothing
  * date_drift   — recorded start/end disagree with the file's first/last bar
  * legacy_shape — a stocks/indexes timeframe still holds a flat node instead
                   of nesting by provider (pre-migration catalogs)
  * no_provider  — a nested node whose provider can't be determined

and repo-wide:

  * orphan_file  — a parquet under an OHLCV root that no catalog references

    uv run python tools/check_catalog.py
    uv run python tools/check_catalog.py --catalog indexes --json
    uv run python tools/check_catalog.py --fix          # backfill + migrate

`--fix` only ever adds information: it stamps default semantics on nodes that
lack them, migrates flat nodes to the nested shape, and refreshes drifted
dates from the files. It never deletes a node or touches a parquet file.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    CatalogSpec,
    catalog_registry,
    provider_node_key,
    read_parquet_range,
    recompute_entry_bounds,
    resolve_from_repo,
)
from utils.config import DATA_ROOT, relative_to_repo
from utils.report import allow_broken_pipe, print_table
from utils.semantics import apply_semantics, semantics_for, validate_node_semantics

#: Roots scanned for parquet files with no catalog entry.
OHLCV_ROOTS = [
    DATA_ROOT / "cryptos",
    DATA_ROOT / "crypto-futures",
    DATA_ROOT / "stocks",
    DATA_ROOT / "indexes",
    DATA_ROOT / "commodities",
    DATA_ROOT / "macro",
]


@dataclass
class Issue:
    catalog: str
    symbol: str
    timeframe: str
    node: str
    kind: str
    detail: str
    fixed: bool = False

    def as_dict(self) -> dict:
        return {
            "catalog": self.catalog,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "node": self.node,
            "kind": self.kind,
            "detail": self.detail,
            "fixed": self.fixed,
        }


@dataclass
class Audit:
    issues: list[Issue] = field(default_factory=list)
    nodes_checked: int = 0
    referenced_paths: set[str] = field(default_factory=set)

    def add(self, **kwargs) -> Issue:
        issue = Issue(**kwargs)
        self.issues.append(issue)
        return issue


def _node_provider(node: dict, node_key: str, entry: dict) -> str | None:
    """Provider for a node: explicit field, then the node key, then the entry."""
    return (
        node.get("provider")
        or (node_key.split(":")[0] if node_key else None)
        or entry.get("provider")
    )


def _migrate_flat_node(entry: dict, timeframe: str, node: dict) -> str:
    """Re-file a flat node under its provider key. Returns the key used."""
    provider = node.get("provider") or entry.get("provider") or "UNKNOWN"
    key = provider_node_key(provider, node.get("session"))
    node.setdefault("provider", provider)
    entry["timeframe_dates"][timeframe] = {key: node}
    return key


def audit_catalog(spec: CatalogSpec, fix: bool, check_dates: bool) -> tuple[Audit, bool]:
    """Audit one catalog. Returns (audit, catalog_was_modified)."""
    audit = Audit()
    catalog = spec.load()
    modified = False

    for symbol, entry in catalog.items():
        if not isinstance(entry, dict):
            continue

        for timeframe, tf_node in list(entry.get("timeframe_dates", {}).items()):
            if not isinstance(tf_node, dict):
                continue

            is_flat = "file_path" in tf_node or "start_date" in tf_node

            if is_flat and spec.nested:
                issue = audit.add(
                    catalog=spec.name, symbol=symbol, timeframe=timeframe, node="-",
                    kind="legacy_shape",
                    detail="flat node in a nested-by-provider catalog; "
                           "another provider's write would replace it",
                )
                if fix:
                    _migrate_flat_node(entry, timeframe, tf_node)
                    issue.fixed = True
                    modified = True
                    tf_node = entry["timeframe_dates"][timeframe]
                    is_flat = False

            nodes = {"-": tf_node} if is_flat else {
                k: v for k, v in tf_node.items() if isinstance(v, dict)
            }

            for node_key, node in nodes.items():
                audit.nodes_checked += 1

                provider = _node_provider(node, node_key if node_key != "-" else "", entry)
                if not provider:
                    audit.add(
                        catalog=spec.name, symbol=symbol, timeframe=timeframe, node=node_key,
                        kind="no_provider", detail="no provider on the node, key or entry",
                    )

                # --- semantics ---
                problems = validate_node_semantics(node)
                if problems:
                    issue = audit.add(
                        catalog=spec.name, symbol=symbol, timeframe=timeframe, node=node_key,
                        kind="semantics", detail="; ".join(problems),
                    )
                    if fix and all(p.startswith("missing ") for p in problems):
                        apply_semantics(
                            node,
                            semantics_for(spec.asset_class, provider, node.get("what_to_show")),
                        )
                        issue.fixed = True
                        modified = True

                # --- file presence and dates ---
                fp_str = node.get("file_path", "")
                if not fp_str:
                    audit.add(
                        catalog=spec.name, symbol=symbol, timeframe=timeframe, node=node_key,
                        kind="no_file_path", detail="node records no file_path",
                    )
                    continue

                path = resolve_from_repo(fp_str)
                audit.referenced_paths.add(str(path.resolve()))

                if not path.exists():
                    audit.add(
                        catalog=spec.name, symbol=symbol, timeframe=timeframe, node=node_key,
                        kind="file_missing", detail=fp_str,
                    )
                    continue

                if not check_dates or node.get("schema") == "rate_series":
                    continue

                actual = read_parquet_range(path)
                if actual is None:
                    continue
                start, end, _ = actual
                if node.get("start_date") != start or node.get("end_date") != end:
                    issue = audit.add(
                        catalog=spec.name, symbol=symbol, timeframe=timeframe, node=node_key,
                        kind="date_drift",
                        detail=f"catalog {node.get('start_date')}..{node.get('end_date')} "
                               f"vs file {start}..{end}",
                    )
                    if fix:
                        node["start_date"], node["end_date"] = start, end
                        issue.fixed = True
                        modified = True

        if modified:
            recompute_entry_bounds(entry)

    if fix and modified:
        spec.save(catalog)

    return audit, modified


def find_orphans(referenced: set[str]) -> list[Path]:
    """Parquet files under the OHLCV roots that no catalog node points at."""
    orphans: list[Path] = []
    for root in OHLCV_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.parquet"):
            if str(path.resolve()) not in referenced:
                orphans.append(path)
    return sorted(orphans)


def main() -> None:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", nargs="+", help="Limit to these catalogs by name (default: all)")
    p.add_argument("--fix", action="store_true",
                   help="Backfill missing semantics, migrate flat nodes, refresh drifted dates")
    p.add_argument("--skip-dates", action="store_true",
                   help="Don't open parquet files to verify dates (much faster)")
    p.add_argument("--skip-orphans", action="store_true",
                   help="Don't scan the data tree for unreferenced parquet files")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = p.parse_args()

    specs = catalog_registry()
    if args.catalog:
        wanted = set(args.catalog)
        unknown = wanted - {s.name for s in specs}
        if unknown:
            p.error(f"unknown catalog(s): {', '.join(sorted(unknown))}")
        specs = [s for s in specs if s.name in wanted]

    issues: list[Issue] = []
    referenced: set[str] = set()
    nodes_checked = 0
    catalogs_written: list[str] = []

    for spec in specs:
        audit, modified = audit_catalog(spec, fix=args.fix, check_dates=not args.skip_dates)
        issues.extend(audit.issues)
        referenced |= audit.referenced_paths
        nodes_checked += audit.nodes_checked
        if modified and args.fix:
            catalogs_written.append(spec.name)

    orphans = [] if args.skip_orphans or args.catalog else find_orphans(referenced)

    if args.json:
        json.dump(
            {
                "catalogs": [s.name for s in specs],
                "nodes_checked": nodes_checked,
                "issues": [i.as_dict() for i in issues],
                "orphan_files": [relative_to_repo(o) for o in orphans],
                "catalogs_written": catalogs_written,
            },
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
    else:
        print(f"Checked {nodes_checked} node(s) across {len(specs)} catalog(s).\n")
        if issues:
            by_kind = Counter(i.kind for i in issues)
            fixed = sum(1 for i in issues if i.fixed)
            print_table(
                ["Catalog", "Symbol", "TF", "Node", "Issue", "Detail", "Fixed"],
                [
                    [i.catalog, i.symbol, i.timeframe, i.node, i.kind, i.detail[:60],
                     "yes" if i.fixed else ""]
                    for i in issues[:60]
                ],
            )
            if len(issues) > 60:
                print(f"\n… and {len(issues) - 60} more")
            print("\nBy kind: " + ", ".join(f"{k}={v}" for k, v in sorted(by_kind.items())))
            if args.fix:
                print(f"Fixed {fixed} of {len(issues)}.")
            elif fixed == 0:
                print("Re-run with --fix to backfill semantics, migrate shapes and refresh dates.")
        else:
            print("Every node is complete and consistent with its file.")

        if orphans:
            print(f"\n{len(orphans)} parquet file(s) on disk that no catalog references:")
            for path in orphans[:20]:
                print(f"  {relative_to_repo(path)}")
            if len(orphans) > 20:
                print(f"  … and {len(orphans) - 20} more")

        if catalogs_written:
            print("\nRewrote: " + ", ".join(catalogs_written))

    unfixed = [i for i in issues if not i.fixed]
    if unfixed or orphans:
        sys.exit(1)


if __name__ == "__main__":
    main()
