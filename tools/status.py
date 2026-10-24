#!/usr/bin/env -S uv run python
"""One call that answers: what's here, what's stale, what's broken.

The repo has three audits — `check_gaps.py` for holes, `check_schema.py` for
file shape, `check_catalog.py` for metadata — and each needs its own
invocation, flags and output parsing. This is the roll-up: one command, one
table (or one JSON object), covering every asset class.

    uv run python tools/status.py                 # coverage + staleness
    uv run python tools/status.py --json          # for an agent
    uv run python tools/status.py --deep          # also files and metadata
    uv run python tools/status.py --stale-days 7

Fast by default: it reads the catalogs only, so it costs a few YAML loads and
no parquet reads. `--deep` adds per-file checks — existence, schema
conformance and catalog completeness — which opens every file, so it is
opt-in.

Exit code is non-zero when `--deep` finds something wrong, so it works as a
health check in a cron or CI step.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow.parquet as pq

from utils.calendars import calendar_for
from utils.catalog import (
    CANONICAL_COLS,
    CANONICAL_ARROW_TYPES,
    catalog_registry,
    iter_timeframe_nodes,
    resolve_from_repo,
)
from utils.planning import days_stale
from utils.report import allow_broken_pipe, print_table
from utils.semantics import validate_node_semantics

#: pyarrow spells float64 "double".


@dataclass
class CatalogStatus:
    name: str
    asset_class: str
    calendar: str
    symbols: int = 0
    series: int = 0
    timeframes: Counter = field(default_factory=Counter)
    providers: Counter = field(default_factory=Counter)
    oldest: str | None = None
    newest: str | None = None
    stale: int = 0
    undated: int = 0
    # --deep only
    missing_files: int = 0
    schema_mismatches: int = 0
    semantic_gaps: int = 0
    checked_files: int = 0

    @property
    def problems(self) -> int:
        return self.missing_files + self.schema_mismatches + self.semantic_gaps

    def as_dict(self) -> dict:
        return {
            "catalog": self.name,
            "asset_class": self.asset_class,
            "calendar": self.calendar,
            "symbols": self.symbols,
            "series": self.series,
            "timeframes": dict(sorted(self.timeframes.items())),
            "providers": dict(sorted(self.providers.items())),
            "oldest": self.oldest,
            "newest": self.newest,
            "stale": self.stale,
            "undated": self.undated,
            "missing_files": self.missing_files,
            "schema_mismatches": self.schema_mismatches,
            "semantic_gaps": self.semantic_gaps,
            "checked_files": self.checked_files,
        }


def _schema_ok(path: Path) -> bool:
    """Same rule as check_schema.py: exact columns, exact arrow types."""
    try:
        schema = pq.read_schema(path)
    except Exception:
        return False
    if len(schema.names) == 0:
        return True  # empty file — a failed download, not a schema violation
    if list(schema.names) != CANONICAL_COLS:
        return False
    return all(str(schema.field(n).type) == CANONICAL_ARROW_TYPES[n] for n in CANONICAL_COLS)


def summarize(spec, stale_days: int, deep: bool) -> CatalogStatus:
    status = CatalogStatus(
        name=spec.name,
        asset_class=spec.asset_class,
        calendar=calendar_for(spec.asset_class).name,
    )

    for key, entry in spec.load().items():
        if not isinstance(entry, dict):
            continue
        status.symbols += 1

        for timeframe, node_key, node in iter_timeframe_nodes(entry):
            status.series += 1
            status.timeframes[timeframe] += 1

            provider = (
                node.get("provider")
                or (node_key.split(":")[0] if node_key else None)
                or entry.get("provider")
                or "unknown"
            )
            status.providers[provider] += 1

            start, end = node.get("start_date"), node.get("end_date")
            if start and (status.oldest is None or start < status.oldest):
                status.oldest = start
            if end and (status.newest is None or end > status.newest):
                status.newest = end

            stale = days_stale(end)
            if stale is None:
                status.undated += 1
            elif stale >= stale_days:
                status.stale += 1

            if not deep:
                continue

            if validate_node_semantics(node):
                status.semantic_gaps += 1

            fp = node.get("file_path")
            if not fp:
                status.missing_files += 1
                continue
            path = resolve_from_repo(fp)
            if not path.exists():
                status.missing_files += 1
                continue
            status.checked_files += 1
            if node.get("schema") != "rate_series" and not _schema_ok(path):
                status.schema_mismatches += 1

    return status


def main() -> None:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", nargs="+", help="Limit to these catalogs by name")
    p.add_argument("--stale-days", type=int, default=3, metavar="N",
                   help="Series with an end date at least N days old count as stale (default 3)")
    p.add_argument("--deep", action="store_true",
                   help="Also verify files exist, match the canonical schema, and carry semantics")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = p.parse_args()

    specs = catalog_registry()
    if args.catalog:
        names = set(args.catalog)
        unknown = names - {s.name for s in specs}
        if unknown:
            p.error(f"unknown catalog(s): {', '.join(sorted(unknown))}")
        specs = [s for s in specs if s.name in names]

    statuses = [summarize(spec, args.stale_days, args.deep) for spec in specs]
    statuses = [s for s in statuses if s.symbols]  # skip catalogs that don't exist yet

    totals = {
        "symbols": sum(s.symbols for s in statuses),
        "series": sum(s.series for s in statuses),
        "stale": sum(s.stale for s in statuses),
        "undated": sum(s.undated for s in statuses),
        "problems": sum(s.problems for s in statuses),
    }

    if args.json:
        json.dump(
            {
                "generated_at": datetime.now(tz=timezone.utc).isoformat(),
                "stale_days": args.stale_days,
                "deep": args.deep,
                "totals": totals,
                "catalogs": [s.as_dict() for s in statuses],
            },
            sys.stdout, indent=2,
        )
        sys.stdout.write("\n")
    else:
        if not statuses:
            print("No catalogs found. Is price-data/ present?")
            sys.exit(1)

        headers = ["Catalog", "Class", "Symbols", "Series", "Oldest", "Newest",
                   f"Stale≥{args.stale_days}d", "Calendar"]
        rows = [
            [s.name, s.asset_class, s.symbols, s.series, s.oldest or "—",
             s.newest or "—", s.stale, s.calendar]
            for s in statuses
        ]
        if args.deep:
            headers += ["Missing", "Schema", "Semantics"]
            for row, s in zip(rows, statuses):
                row.extend([s.missing_files, s.schema_mismatches, s.semantic_gaps])

        print_table(headers, rows)
        print(
            f"\n{totals['symbols']} symbols · {totals['series']} series · "
            f"{totals['stale']} stale · {totals['undated']} undated"
        )

        if args.deep:
            if totals["problems"]:
                print(f"{totals['problems']} problem(s) — see check_catalog.py / check_schema.py for detail")
            else:
                print("No file or metadata problems found.")
        else:
            print("Pass --deep to also check files, schema and semantics.")

    if args.deep and totals["problems"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
