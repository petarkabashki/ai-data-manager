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
On a stock catalog the fixes are collected during the scan and applied to a
fresh copy in `catalog_txn`, so running workers lose nothing and hold no lock
through the scan. A node a worker changed since the scan keeps its newer dates.

With `--fix`, an orphan crypto or crypto-futures file is also adopted: its
path names the catalog, symbol and timeframe, so it gets the node
`download_crypto` (or `download_funding`) would have written. Orphans arise
when a run is cut short, or a backup was taken mid-run, before the catalog
was saved; this is the repair step of a restore (docs/server.md#backups).

An orphan stock file is adopted the same way, from provider + universe dir +
symbol + session + timeframe in its path; see `adopt_stock_orphans` for when it
is left reported instead. Other orphans stay reported.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from functools import partial
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (
    FUNDING_TIMEFRAME,
    IBKR_SESSIONS,
    STOCK_UNIVERSE_PRIORITY,
    TF_TO_MS,
    CatalogSpec,
    DateFix,
    apply_date_fixes,
    catalog_registry,
    iter_timeframe_nodes,
    load_stock_catalog,
    node_at,
    provider_node_key,
    read_funding_range,
    read_parquet_range,
    recompute_entry_bounds,
    resolve_from_repo,
    all_catalog_paths,
    run_with_catalog_backup,
    stock_catalog_txn,
    stock_file_path,
    update_catalog_entry,
    update_funding_entry,
    update_stock_timeframe_entry,
)
from utils.config import DATA_ROOT, relative_to_repo
from utils.report import allow_broken_pipe, print_table
from utils.semantics import Semantics, apply_semantics, semantics_for, validate_node_semantics

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


def _replay_migration(catalog: dict, symbol: str, timeframe: str) -> None:
    tf_node = node_at(catalog, symbol, timeframe, "")
    if tf_node and ("file_path" in tf_node or "start_date" in tf_node):
        _migrate_flat_node(catalog[symbol], timeframe, tf_node)


def _replay_semantics(catalog: dict, symbol: str, timeframe: str, node_key: str, semantics) -> None:
    node = node_at(catalog, symbol, timeframe, node_key)
    if node is not None:
        apply_semantics(node, semantics)


def audit_catalog(spec: CatalogSpec, fix: bool, check_dates: bool) -> tuple[Audit, bool]:
    """Audit one catalog. Returns (audit, catalog_was_modified).

    The scan edits its own copy, so what it audits next sees the fix. A catalog
    with a `txn` isn't saved from that copy: each fix is also recorded and
    replayed on a fresh load, under the lock, once the scan is done.
    """
    audit = Audit()
    catalog = spec.load()
    modified = False
    replays: list[Callable[[dict], object]] = []

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
                    replays.append(partial(_replay_migration, symbol=symbol, timeframe=timeframe))
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
                        semantics = semantics_for(spec.asset_class, provider, node.get("what_to_show"))
                        apply_semantics(node, semantics)
                        replays.append(partial(_replay_semantics, symbol=symbol, timeframe=timeframe,
                                               node_key=node_key, semantics=semantics))
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

                actual = (read_funding_range(path) if node.get("schema") == "funding_rate"
                          else read_parquet_range(path))
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
                        seen = (node.get("start_date"), node.get("end_date"))
                        node["start_date"], node["end_date"] = start, end
                        date_fix = DateFix(symbol, timeframe, node_key, fp_str, seen, (start, end))
                        replays.append(partial(apply_date_fixes, fixes=[date_fix]))
                        issue.fixed = True
                        modified = True

        if modified:
            recompute_entry_bounds(entry)

    if fix and modified:
        if spec.txn:
            with spec.txn() as fresh:
                for replay in replays:
                    replay(fresh)
        else:
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


#: `stocks/<dir>/...` -> the provider its nodes carry.
STOCK_PROVIDER_DIRS = {"yf": "YF", "ibkr": "IBKR"}


@dataclass(frozen=True)
class StockOrphan:
    path: Path
    provider: str
    universe: str
    symbol: str
    session: str | None
    timeframe: str

    @property
    def node_key(self) -> str:
        return provider_node_key(self.provider, self.session)


def parse_stock_orphan(path: Path) -> StockOrphan | None:
    """Read a stock file's identity from its path, or None if it isn't canonical."""
    try:
        top, provider_dir, universe, symbol, name = path.relative_to(DATA_ROOT).parts
    except ValueError:
        return None
    provider = STOCK_PROVIDER_DIRS.get(provider_dir)
    if top != "stocks" or provider is None or universe not in STOCK_UNIVERSE_PRIORITY:
        return None
    if not name.startswith(f"{symbol}-"):
        return None
    # After the symbol, which may itself hold a dash (BRK-B).
    session, _, timeframe = name.removesuffix(".parquet")[len(symbol) + 1:].rpartition("-")
    session = session or None
    if timeframe not in TF_TO_MS or session not in (None, *IBKR_SESSIONS):
        return None
    if session and provider != "IBKR":
        return None
    if stock_file_path(provider, universe, symbol, timeframe, session) != path:
        return None
    return StockOrphan(path, provider, universe, symbol, session, timeframe)


def orphan_semantics(catalog: dict, orphan: StockOrphan) -> Semantics | None:
    """What an adopted node may claim, or None if nothing says, and it stays reported.

    The path can't tell adjusted bars from as-traded ones, so the adjustment comes
    from the provider's other nodes for this symbol. YF copies are mixed per symbol,
    so a YF file with nothing to go by isn't labelled by default. IBKR falls back to
    the default the workers request.
    """
    default = semantics_for("Stock", orphan.provider)
    seen = {
        node["adjustment"]
        for _, _, node in iter_timeframe_nodes(catalog.get(orphan.symbol) or {})
        if node.get("provider") == orphan.provider and node.get("adjustment")
    }
    if len(seen) > 1:
        return None
    if seen:
        return replace(default, adjustment=seen.pop())
    return default if orphan.provider == "IBKR" else None


def adopt_stock_orphans(orphans: list[Path], names: set[str]) -> tuple[list[Path], list[str]]:
    """Re-register stock files no node references, in catalogs named in `names`.

    Returns (adopted files, catalogs written). A file is adopted into its path's
    universe, and a catalog's existing node is never replaced: a node at that
    address pointing at another file is a decision somebody made (a dedupe
    repoint, a price-scale conflict), so the orphan stays reported. So does a
    file `orphan_semantics` can't label.

    Which catalogs get a node depends on the symbol's listings:

      * Nobody lists it: its path's universe, as a new entry.
      * Others list it but the path's universe doesn't: stays reported. Listing
        it there would claim membership and, through `stock_home_universe`,
        move the home of the symbol's IBKR files.
      * YF: the path's universe only. YF copies are per universe and can differ
        in adjustment, so one never stands in for another universe's.
      * IBKR, path under the symbol's home: the home, plus every later universe
        that lists it and has no such node, since those catalogs point at the
        home's file.
      * IBKR, path under a later universe: stays reported. It is a duplicate of
        the home's for `dedupe_stock_universes` (or a `--no-share` copy), not
        something to point the catalog at.

    Semantics follow `orphan_semantics`; a file it can't label stays reported.
    """
    parsed = [o for o in map(parse_stock_orphan, orphans) if o and o.universe in names]
    if not parsed:
        return [], []
    catalogs = {u: load_stock_catalog(u) for u in STOCK_UNIVERSE_PRIORITY}

    queue: dict[str, list[tuple[StockOrphan, str, str, Semantics]]] = {}
    for orphan in parsed:
        listing = [u for u in STOCK_UNIVERSE_PRIORITY if orphan.symbol in catalogs[u]]
        if listing and orphan.universe not in listing:
            continue
        shared = orphan.provider == "IBKR" and bool(listing)
        if shared and orphan.universe != listing[0]:
            continue
        if node_at(catalogs[orphan.universe], orphan.symbol, orphan.timeframe, orphan.node_key):
            continue
        semantics = orphan_semantics(catalogs[orphan.universe], orphan)
        if semantics is None:
            continue
        bounds = read_parquet_range(orphan.path)
        if not bounds:
            continue                     # empty: a failed download, not a series
        for universe in listing if shared else [orphan.universe]:
            if universe in names and not node_at(catalogs[universe], orphan.symbol,
                                                 orphan.timeframe, orphan.node_key):
                queue.setdefault(universe, []).append((orphan, bounds[0], bounds[1], semantics))

    adopted: set[Path] = set()
    written: list[str] = []
    for universe in STOCK_UNIVERSE_PRIORITY:     # homes first: later catalogs follow them
        if universe not in queue:
            continue
        with stock_catalog_txn(universe) as fresh:
            for orphan, start, end, semantics in queue[universe]:
                is_home = universe == orphan.universe
                if not is_home and orphan.path not in adopted:
                    continue
                current = node_at(fresh, orphan.symbol, orphan.timeframe, orphan.node_key)
                if current:              # written since the snapshot
                    if is_home and current.get("file_path") == relative_to_repo(orphan.path):
                        adopted.add(orphan.path)
                    continue
                update_stock_timeframe_entry(fresh, orphan.symbol, orphan.timeframe, orphan.provider,
                                             orphan.path, start, end, session=orphan.session,
                                             semantics=semantics)
                if is_home:
                    adopted.add(orphan.path)
        written.append(universe)
    return sorted(adopted), written


def adopt_orphans(orphans: list[Path], specs: list[CatalogSpec]) -> tuple[list[Path], list[str]]:
    """Give each orphan crypto / crypto-futures / stock file its catalog node.

    Returns (adopted files, catalogs written). Funding files go last: they
    attach to their contract's entry, which an orphan bar file may create.
    """
    by_name = {s.name: s for s in specs}
    loaded: dict[str, dict] = {}
    adopted: list[Path] = []

    def place(path: Path) -> tuple[str, str, str, str] | None:
        try:
            top, exchange, symbol, name = path.relative_to(DATA_ROOT).parts
        except ValueError:
            return None
        stem, _, timeframe = name.removesuffix(".parquet").rpartition("-")
        catalog = {"cryptos": f"crypto_{exchange}", "crypto-futures": f"crypto_futures_{exchange}"}.get(top)
        if stem != symbol or catalog not in by_name:
            return None
        return catalog, exchange, symbol, timeframe

    for path in sorted(orphans, key=lambda p: p.name.endswith(f"-{FUNDING_TIMEFRAME}.parquet")):
        where = place(path)
        if not where:
            continue
        name, exchange, symbol, timeframe = where
        futures = name.startswith("crypto_futures_")
        funding = futures and timeframe == FUNDING_TIMEFRAME
        bounds = read_funding_range(path) if funding else read_parquet_range(path)
        if not bounds:
            continue                     # empty: a failed download, not a series
        if name not in loaded:           # not setdefault: that would load on every call
            loaded[name] = by_name[name].load()
        catalog = loaded[name]
        if funding:
            try:
                update_funding_entry(catalog, symbol, exchange, path, bounds[0], bounds[1])
            except ValueError:
                continue                 # no bars for the contract: leave it reported
        else:
            base, _, quote = symbol.partition("_")
            update_catalog_entry(catalog, exchange, symbol, timeframe, path, bounds[0], bounds[1],
                                 categories=["Crypto Futures"] if futures else None,
                                 common_name=f"{base}/{quote}:{quote}" if futures else None)
        adopted.append(path)
    for name, catalog in loaded.items():
        by_name[name].save(catalog)
    stock_adopted, stock_written = adopt_stock_orphans(orphans, set(by_name))
    return adopted + stock_adopted, sorted({*loaded, *stock_written})


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
    p.add_argument("--catalog-backup-dir", type=Path,
                   help="With --fix. Default: TMP_ROOT/catalog-backup-check_catalog-<UTC stamp>")
    args = p.parse_args()

    specs = catalog_registry()
    if args.catalog:
        wanted = set(args.catalog)
        unknown = wanted - {s.name for s in specs}
        if unknown:
            p.error(f"unknown catalog(s): {', '.join(sorted(unknown))}")
        specs = [s for s in specs if s.name in wanted]
    if args.fix:
        sys.exit(run_with_catalog_backup("check_catalog", all_catalog_paths(),
                                         lambda: run(args, specs) or 0, args.catalog_backup_dir))
    run(args, specs)


def run(args: argparse.Namespace, specs: list) -> None:

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
    adopted: list[Path] = []
    if args.fix and orphans:
        adopted, written = adopt_orphans(orphans, specs)
        taken = set(adopted)
        orphans = [o for o in orphans if o not in taken]
        catalogs_written = sorted(set(catalogs_written) | set(written))

    if args.json:
        json.dump(
            {
                "catalogs": [s.name for s in specs],
                "nodes_checked": nodes_checked,
                "issues": [i.as_dict() for i in issues],
                "orphan_files": [relative_to_repo(o) for o in orphans],
                "orphans_adopted": [relative_to_repo(o) for o in adopted],
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

        if adopted:
            print(f"\nAdopted {len(adopted)} orphan file(s) into their catalogs.")
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
