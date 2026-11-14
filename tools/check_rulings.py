#!/usr/bin/env -S uv run python
"""Fail when a recorded ruling has been undone: rows back where they were removed, or a flag gone.

Rulings live in three places, and a writer on old code can undo each of them
without any other check noticing (2026-11-13: a worker started on 11-04 code
refilled RCAT's deleted bars and dropped SSP/SUNS `data_flags`):

* `deleted_bars.yaml`  -- bars removed on purpose. Rows inside a window fail.
* `exchange_empty.yaml` -- windows a venue answered empty. Rows inside fail:
  either the record is wrong or the bars came from somewhere else.
* catalog annotations (`data_flags`, `unadjusted_splits`, `known_adjustment`).
  Each run compares every node's annotations with a baseline snapshot; an item
  present there and missing now fails. A `bad`/`uncertain` flag whose range a
  `deleted` flag now covers is the delete path working, not a loss.

The baseline is this tool's last clean run (`TMP_ROOT/check_rulings/annotations.json`).
A clean run extends it with any new annotation, so a flag is protected from the
first run after it is set. A run with losses leaves it alone; after a review
that finds the removal intended, `--accept` takes the current state as the
baseline. `--against DIR` compares with a catalog backup directory instead
(`<DIR>/<catalog>.yaml`, as `catalog_backup` writes them).

Twin blocks (the crypto deletions of 2026-11-13) are `check_twins.py`'s job.

    uv run python tools/check_rulings.py
    uv run python tools/check_rulings.py --json
    uv run python tools/check_rulings.py --against tmp/catalog-backup-delete_stock_bars-20261114T010516Z
    uv run python tools/check_rulings.py --accept      # after reviewing a reported loss

Exit 1 when anything is found, 0 when clean.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pyarrow.parquet as pq

from utils import deleted_bars, exchange_empty
from utils.catalog import ANNOTATION_KEYS, catalog_registry, iter_timeframe_nodes, load_yaml
from utils.config import DATA_ROOT, TMP_ROOT
from utils.report import allow_broken_pipe, print_table

BASELINE = Path("check_rulings") / "annotations.json"


# ── rows inside recorded windows ─────────────────────────────────────────────

def index_files(data_root: Path) -> dict[str, Path]:
    """Every parquet under the data root by its record key (the path's last four parts)."""
    out: dict[str, Path] = {}
    for dirpath, _, names in os.walk(data_root):
        for name in names:
            if name.endswith(".parquet"):
                path = Path(dirpath) / name
                out["/".join(path.parts[-4:])] = path
    return out


def rows_in_windows(series: dict[str, list[dict]], files: dict[str, Path], record: str) -> list[dict]:
    """One finding per window that holds rows, or per key whose file is missing."""
    found = []
    for key, spans in sorted(series.items()):
        path = files.get(key)
        if path is None:
            found.append({"check": record, "series": key, "problem": "file not found",
                          "window": "", "rows": None})
            continue
        try:
            ts = pq.read_table(path, columns=["timestamp"]).column("timestamp").to_numpy().astype("int64")
        except Exception as exc:  # unreadable file: report it, don't stop the check
            found.append({"check": record, "series": key, "problem": f"unreadable: {exc}",
                          "window": "", "rows": None})
            continue
        for w in spans:
            n = int(((ts >= int(w["start_ms"])) & (ts <= int(w["end_ms"]))).sum())
            if n:
                found.append({"check": record, "series": key, "problem": "rows inside window",
                              "window": f"{w.get('start')}..{w.get('end')}", "rows": n})
    return found


# ── annotations against a baseline ───────────────────────────────────────────

def _item(value) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def snapshot(catalogs: dict[str, dict]) -> dict[str, dict[str, list[str]]]:
    """node id ("catalog|symbol|timeframe|node key") → annotation key → items (canonical json).

    Every node is listed, annotated or not, so a node that lost its last flag
    is told apart from a node that is gone.
    """
    out: dict[str, dict[str, list[str]]] = {}
    for name, data in catalogs.items():
        for symbol, entry in (data or {}).items():
            if not isinstance(entry, dict):
                continue
            for timeframe, node_key, node in iter_timeframe_nodes(entry):
                notes = {}
                for key in ANNOTATION_KEYS:
                    value = node.get(key)
                    if value in (None, [], {}):
                        continue
                    items = value if isinstance(value, list) else [value]
                    notes[key] = sorted(_item(v) for v in items)
                out[f"{name}|{symbol}|{timeframe}|{node_key}"] = notes  # {} too: the node exists
    return out


def _deleted_cover(items: list[str]) -> list[tuple[str, str]]:
    spans = []
    for raw in items:
        flag = json.loads(raw)
        if isinstance(flag, dict) and flag.get("kind") == "deleted":
            spans.append((str(flag.get("start")), str(flag.get("end"))))
    return spans


def _superseded_by_delete(raw: str, current_flags: list[str]) -> bool:
    """A bad/uncertain range now inside a `deleted` flag: the bars went, the flag went with them."""
    flag = json.loads(raw)
    if not isinstance(flag, dict) or flag.get("kind") not in ("bad", "uncertain"):
        return False
    start, end = str(flag.get("start")), str(flag.get("end"))
    return any(s <= start and end <= e for s, e in _deleted_cover(current_flags))


def lost_annotations(baseline: dict, current: dict) -> list[dict]:
    found = []
    for node, notes in sorted(baseline.items()):
        now = current.get(node, {})
        for key, items in notes.items():
            have = set(now.get(key, []))
            for raw in items:
                if raw in have:
                    continue
                if key == "data_flags" and _superseded_by_delete(raw, now.get("data_flags", [])):
                    continue
                found.append({"check": "annotations", "series": node,
                              "problem": f"{key} lost" + ("" if node in current else " (node gone)"),
                              "window": raw, "rows": None})
    return found


def merged(baseline: dict, current: dict) -> dict:
    """The baseline after a clean run: what it had, plus everything current has."""
    out = {node: {k: list(v) for k, v in notes.items()} for node, notes in baseline.items()}
    for node, notes in current.items():
        mine = out.setdefault(node, {})
        for key, items in notes.items():
            mine[key] = sorted(set(mine.get(key, [])) | set(items))
    return out


def load_catalogs(against: Path | None = None) -> dict[str, dict]:
    if against is None:
        return {spec.name: spec.load() for spec in catalog_registry()}
    return {p.stem: load_yaml(p) for p in sorted(against.glob("*.yaml"))}


def _annotated(snap: dict) -> int:
    return sum(1 for notes in snap.values() if notes)


def write_baseline(path: Path, data: dict) -> None:
    data = {node: notes for node, notes in data.items() if notes}
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f".tmp{os.getpid()}")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
    tmp.replace(path)


# ── main ─────────────────────────────────────────────────────────────────────

def run(data_root: Path, baseline_path: Path, against: Path | None, accept: bool,
        catalogs: dict[str, dict] | None = None) -> tuple[list[dict], str]:
    files = index_files(data_root)
    found = rows_in_windows(deleted_bars.load(deleted_bars.record_path(data_root)), files,
                            "deleted_bars")
    empty = load_yaml(data_root / exchange_empty.RECORD_NAME).get("series") or {}
    found += rows_in_windows(empty, files, "exchange_empty")

    current = snapshot(catalogs if catalogs is not None else load_catalogs())
    if against is not None:
        base = snapshot(load_catalogs(against))
        current_cmp = {n: v for n, v in current.items() if n.split("|", 1)[0] in
                       {p.stem for p in against.glob("*.yaml")}}
        found += lost_annotations(base, current_cmp)
        return found, f"annotations compared with {against}"

    if accept:
        write_baseline(baseline_path, current)
        return found, f"baseline replaced with the current annotations ({_annotated(current)} nodes)"
    if not baseline_path.exists():
        write_baseline(baseline_path, current)
        return found, f"no baseline yet: recorded {_annotated(current)} annotated nodes"
    base = json.loads(baseline_path.read_text())
    losses = lost_annotations(base, current)
    found += losses
    if losses:
        return found, "baseline kept: review the losses, then --accept if intended"
    write_baseline(baseline_path, merged(base, current))
    return found, f"baseline extended ({_annotated(current)} annotated nodes now)"


def main(argv: list[str] | None = None) -> int:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--against", type=Path, metavar="DIR",
                   help="Compare annotations with the catalog yamls in this backup directory")
    p.add_argument("--accept", action="store_true",
                   help="Take the current annotations as the baseline (after reviewing a loss)")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = p.parse_args(argv)
    if args.against is not None and args.accept:
        p.error("--against compares with a backup; --accept sets the baseline")

    found, note = run(DATA_ROOT, TMP_ROOT / BASELINE, args.against, args.accept)
    if args.json:
        json.dump({"findings": found, "note": note}, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        if found:
            print_table(["Check", "Series", "Problem", "Window / item", "Rows"],
                        [[f["check"], f["series"], f["problem"], f["window"][:90],
                          "" if f["rows"] is None else f["rows"]] for f in found])
        print(f"{len(found)} finding(s); {note}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())
