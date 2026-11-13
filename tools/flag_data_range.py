#!/usr/bin/env -S uv run python
"""Write `data_flags` on stock catalog nodes: known-bad ranges and breaks, with dates.

When the owner rules that a stored file's bars stay as they are for now, but
readers must know what is wrong with them, the ruling goes on every catalog
node that points at the file (format and kinds: utils/data_flags.py):

    # the 1.55 step Yahoo serves in VTOL on 2020-06-12
    uv run python tools/flag_data_range.py --kind break --date 2020-06-12 \\
        --file price-data/stocks/yf/russell2000/VTOL/VTOL-1d.parquet \\
        --reason "unadjusted 1.55 jump" --ref "Product msg_d3d90042"

    # IBKR's APO bars 2020-01-09..2021-12-31 wait on a re-fetch
    uv run python tools/flag_data_range.py --kind bad --from 2020-01-09 --to 2021-12-31 \\
        --file price-data/stocks/ibkr/sp500/APO/APO-all-1d.parquet --reason "..."

    # many at once: a YAML list of {file_path, kind, from, to | date, reason, ref}
    uv run python tools/flag_data_range.py --spec flags.yaml

`--clear` removes the flags named instead (same kind and dates). Kinds `bad`,
`uncertain` and `break`; `deleted` flags are written by delete_stock_bars.py.
Dry run by default. `--apply` backs up the catalogs first and writes nothing if
that backup fails.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import (  # noqa: E402
    STOCK_UNIVERSES, iter_timeframe_nodes, load_stock_catalog, run_with_catalog_backup,
    stock_catalog_path, stock_catalog_txn,
)
from utils.data_flags import clear_flag, make_flag, set_flag  # noqa: E402
from utils.report import allow_broken_pipe  # noqa: E402

KINDS = ("bad", "uncertain", "break")


def parse_specs(args: argparse.Namespace) -> list[tuple[str, dict]]:
    """(file_path, flag) for every flag asked for. Raises ValueError on a bad one."""
    today = dt.date.today().isoformat()
    if args.spec:
        items = yaml.safe_load(args.spec.read_text()) or []
    else:
        if not (args.file and args.kind and (args.reason or args.clear)):
            raise ValueError("give --spec, or --file, --kind and --reason")
        items = [{"file_path": args.file, "kind": args.kind, "from": args.start, "to": args.end,
                  "date": args.date, "reason": args.reason or "", "ref": args.ref}]
    out = []
    for item in items:
        if item.get("kind") not in KINDS:
            raise ValueError(f"{item.get('file_path')}: kind must be one of {', '.join(KINDS)}")
        flag = make_flag(item["kind"], start=_s(item.get("from")), end=_s(item.get("to")),
                         date=_s(item.get("date")), reason=item.get("reason") or "",
                         ref=item.get("ref"), recorded=today)
        out.append((item["file_path"], flag))
    return out


def _s(value) -> str | None:
    return None if value is None else str(value)


def flag_catalog(catalog: dict, flags: list[tuple[str, dict]], clear: bool) -> list[tuple[str, str, str]]:
    """Set (or clear) each flag on the nodes of its file. Returns (symbol, file, what) per change."""
    changes = []
    for key, entry in catalog.items():
        if not isinstance(entry, dict):
            continue
        for _tf, _node_key, node in iter_timeframe_nodes(entry):
            for fp, flag in flags:
                if node.get("file_path") != fp:
                    continue
                changed = clear_flag(node, flag) if clear else set_flag(node, dict(flag))
                if changed:
                    span = flag.get("date") or f"{flag['from']}..{flag['to']}"
                    changes.append((entry.get("name", key), fp,
                                    f"{'cleared' if clear else 'set'} {flag['kind']} {span}"))
    return changes


def write(args: argparse.Namespace, flags: list[tuple[str, dict]]) -> int:
    total, seen = 0, set()
    for universe in args.catalog:
        if args.apply:
            with stock_catalog_txn(universe) as fresh:
                changes = flag_catalog(fresh, flags, args.clear)
        else:
            changes = flag_catalog(load_stock_catalog(universe), flags, args.clear)
        for symbol, fp, what in changes:
            print(f"{universe:<16} {symbol:<8} {what}  {fp}")
            seen.add(fp)
        total += len(changes)
    missing = sorted({fp for fp, _ in flags} - seen)
    if missing and not args.clear:
        print(f"\nno node changed for {len(missing)} file(s) (already flagged, or not in a catalog):")
        for fp in missing:
            print(f"  {fp}")
    verb = "changed" if args.apply else "would change (dry run; add --apply)"
    print(f"\n{total} nodes {verb}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--spec", type=Path, help="YAML list of flags (file_path, kind, from, to | date, reason, ref)")
    p.add_argument("--file", help="file_path, as stored in the catalog")
    p.add_argument("--kind", choices=KINDS)
    p.add_argument("--from", dest="start", help="first date of a bad/uncertain range (inclusive)")
    p.add_argument("--to", dest="end", help="last date of the range (inclusive)")
    p.add_argument("--date", help="the date of a break")
    p.add_argument("--reason")
    p.add_argument("--ref", help="where the ruling was made, e.g. 'Product msg_d3d90042'")
    p.add_argument("--clear", action="store_true", help="Remove the named flags instead")
    p.add_argument("--catalog", nargs="+", choices=STOCK_UNIVERSES, default=STOCK_UNIVERSES)
    p.add_argument("--apply", action="store_true", help="Back up, then write the catalogs")
    p.add_argument("--catalog-backup-dir", type=Path,
                   help="Default: TMP_ROOT/catalog-backup-flag_data_range-<UTC stamp>")
    args = p.parse_args(argv)
    try:
        flags = parse_specs(args)
    except ValueError as exc:
        p.error(str(exc))
    if args.apply:
        return run_with_catalog_backup("flag_data_range", [stock_catalog_path(u) for u in args.catalog],
                                       lambda: write(args, flags), args.catalog_backup_dir)
    return write(args, flags)


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
