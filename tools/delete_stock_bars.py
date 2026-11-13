#!/usr/bin/env -S uv run python
"""Delete a date range of bars from stored stock files, leaving a recorded gap.

When the owner rules that a range of a provider's bars is impossible and no fix
exists at that provider, the bars come out (YF TBBK's 17 flat zero-volume bars
over 2004-12-23..2005-01-18). They are never filled from another provider: the
gap stays, and the record keeps it a gap.

    uv run python tools/delete_stock_bars.py \\
        --range price-data/stocks/yf/russell2000/TBBK/TBBK-1d.parquet 2004-12-23 2005-01-18 \\
        --reason "flat zero-volume placeholder bars" --ref "Product msg_d3d90042"

    # several files, each with its own reason: a YAML list of
    # {file_path, from, to, reason, ref}
    uv run python tools/delete_stock_bars.py --spec deletes.yaml

Dry run by default: per range, the rows it would delete, their first and last
date, close range and zero-volume count. `--apply`:

- copies each original into `--backup-dir` (default
  `<data root>.stock-delete-backup-<UTC stamp>`, beside the data root),
- adds every range to the deleted-bars record (`<data root>/deleted_bars.yaml`,
  utils/deleted_bars.py), so no download, re-download or merge puts the bars
  back, even though the provider still serves them,
- rewrites each file without its ranges, atomically,
- writes the revision record `revisions/<run id>.parquet` (one row per deleted
  bar, old values and content hash, as delete_twin_bars.py) and
  `revisions/<run id>.yaml`,
- flags each range `deleted` (with the run id) on every catalog node of the
  file (`data_flags`, utils/data_flags.py) and refreshes the nodes' dates.

The catalogs are backed up before any of that; if the backup fails, nothing is
written. Dates are inclusive, in UTC.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

import utils.catalog as cat  # noqa: E402
from delete_twin_bars import revisions_dir, row_hash  # noqa: E402
from utils import deleted_bars  # noqa: E402
from utils.catalog import (  # noqa: E402
    STOCK_UNIVERSES, all_catalog_paths, iter_timeframe_nodes, refresh_dates_for_files,
    run_with_catalog_backup, stock_catalog_txn, write_parquet_atomic,
)
from utils.data_flags import make_flag, set_flag  # noqa: E402
from utils.report import allow_broken_pipe  # noqa: E402

OHLCV = ["timestamp", "open", "high", "low", "close", "volume"]


@dataclass
class Cut:
    file_path: str          # as stored in the catalog, "price-data/stocks/..."
    start: str              # first date, inclusive
    end: str                # last date, inclusive
    reason: str
    ref: str | None = None

    @property
    def start_ms(self) -> int:
        return int(pd.Timestamp(self.start, tz="UTC").value // 1_000_000)

    @property
    def end_ms(self) -> int:
        return int(pd.Timestamp(self.end, tz="UTC").value // 1_000_000)


def resolve(data_root: Path, file_path: str) -> Path:
    path = Path(file_path)
    return path if path.is_absolute() else data_root / file_path.split("/", 1)[1]


def parse_cuts(args: argparse.Namespace) -> list[Cut]:
    if args.spec:
        items = yaml.safe_load(args.spec.read_text()) or []
        cuts = [Cut(i["file_path"], str(i["from"]), str(i["to"]), i.get("reason") or "", i.get("ref"))
                for i in items]
    else:
        cuts = [Cut(fp, a, b, args.reason or "", args.ref) for fp, a, b in args.range or []]
    if not cuts:
        raise ValueError("give --range FILE FROM TO, or --spec")
    for c in cuts:
        if not c.reason:
            raise ValueError(f"{c.file_path}: a reason is required")
        if pd.Timestamp(c.start) > pd.Timestamp(c.end):
            raise ValueError(f"{c.file_path}: from {c.start} is after to {c.end}")
    return cuts


def rows_in(frame: pd.DataFrame, cut: Cut) -> pd.Series:
    ts = frame["timestamp"].astype("int64")
    return (ts >= cut.start_ms) & (ts <= cut.end_ms)


def describe(data_root: Path, cuts: list[Cut]) -> list[dict]:
    """What each cut would delete."""
    out = []
    for c in cuts:
        frame = pd.read_parquet(resolve(data_root, c.file_path))
        gone = frame[rows_in(frame, c)]
        day = lambda ms: str(pd.to_datetime(int(ms), unit="ms").date())  # noqa: E731
        out.append({
            "file_path": c.file_path, "from": c.start, "to": c.end, "rows": int(len(gone)),
            "rows_in_file": int(len(frame)),
            "first": day(gone["timestamp"].min()) if len(gone) else None,
            "last": day(gone["timestamp"].max()) if len(gone) else None,
            "close_min": float(gone["close"].min()) if len(gone) else None,
            "close_max": float(gone["close"].max()) if len(gone) else None,
            "zero_volume": int((gone["volume"] == 0).sum()) if len(gone) else 0,
        })
    return out


def flag_deleted(cuts: list[Cut], run_id: str, today: str, catalogs: list[str]) -> int:
    """A `deleted` flag on every node of each cut's file. Returns nodes changed."""
    changed = 0
    for universe in catalogs:
        with stock_catalog_txn(universe) as fresh:
            for entry in fresh.values():
                if not isinstance(entry, dict):
                    continue
                for _tf, _key, node in iter_timeframe_nodes(entry):
                    for c in cuts:
                        if node.get("file_path") == c.file_path:
                            flag = make_flag("deleted", start=c.start, end=c.end, reason=c.reason,
                                             ref=c.ref, recorded=today, run_id=run_id)
                            changed += set_flag(node, flag)
    return changed


def apply(data_root: Path, cuts: list[Cut], run_id: str, backup_dir: Path, catalogs: list[str]) -> dict:
    today = dt.date.today().isoformat()
    by_file: dict[str, list[Cut]] = {}
    for c in cuts:
        by_file.setdefault(c.file_path, []).append(c)
    record = deleted_bars.record_path(data_root)
    # Originals first, then the record: from here on no writer can refill the ranges.
    for fp in by_file:
        dst = backup_dir / fp.split("/", 1)[1]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(resolve(data_root, fp), dst)
    deleted_rows, rewritten, counts = [], [], {}
    for fp, file_cuts in by_file.items():
        path = resolve(data_root, fp)
        frame = pd.read_parquet(path)
        drop = pd.Series(False, index=frame.index)
        for c in file_cuts:
            hit = rows_in(frame, c)
            counts[(fp, c.start, c.end)] = int(hit.sum())
            drop |= hit
        for c in file_cuts:
            deleted_bars.add(record, fp, {
                "start_ms": c.start_ms, "end_ms": c.end_ms, "start": c.start, "end": c.end,
                "rows": counts[(fp, c.start, c.end)], "run_id": run_id, "reason": c.reason,
                **({"ref": c.ref} if c.ref else {}), "deleted": today,
            })
        for row in frame[drop].itertuples(index=False):
            deleted_rows.append({"file": fp, "timestamp": int(row.timestamp), "action": "deleted",
                                 **{k: float(getattr(row, k)) for k in OHLCV[1:]},
                                 "row_hash": row_hash(row)})
        write_parquet_atomic(frame[~drop].reset_index(drop=True), path)
        rewritten.append(path)
    out = revisions_dir(data_root)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(deleted_rows, columns=["file", "timestamp", "action", *OHLCV[1:], "row_hash"]) \
        .to_parquet(out / f"{run_id}.parquet", index=False)
    summary = {"run_id": run_id, "backup_dir": str(backup_dir), "rows_deleted": len(deleted_rows),
               "files": len(by_file),
               "ranges": [{"file_path": c.file_path, "from": c.start, "to": c.end,
                           "rows": counts[(c.file_path, c.start, c.end)], "reason": c.reason, "ref": c.ref}
                          for c in cuts]}
    (out / f"{run_id}.yaml").write_text(yaml.safe_dump(summary, sort_keys=False))
    summary["nodes_flagged"] = flag_deleted(cuts, run_id, today, catalogs)
    summary["catalogs_dates_refreshed"] = refresh_dates_for_files(rewritten)
    return summary


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--range", nargs=3, action="append", metavar=("FILE", "FROM", "TO"),
                   help="file_path as stored in the catalog, first and last date (inclusive)")
    p.add_argument("--spec", type=Path, help="YAML list of {file_path, from, to, reason, ref}")
    p.add_argument("--reason", help="Why the bars go (for --range)")
    p.add_argument("--ref", help="Where the ruling was made (for --range)")
    p.add_argument("--catalog", nargs="+", choices=STOCK_UNIVERSES, default=STOCK_UNIVERSES,
                   help="Catalogs whose nodes get the deleted flag")
    p.add_argument("--apply", action="store_true", help="Back up, delete, record, flag")
    p.add_argument("--run-id", help="Default: stock-delete-<UTC stamp>")
    p.add_argument("--backup-dir", type=Path,
                   help="Where originals are copied (default: <data root>.stock-delete-backup-<UTC stamp>)")
    p.add_argument("--catalog-backup-dir", type=Path,
                   help="Default: TMP_ROOT/catalog-backup-delete_stock_bars-<UTC stamp>")
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)
    try:
        cuts = parse_cuts(args)
    except ValueError as exc:
        p.error(str(exc))
    data_root = cat.DATA_ROOT
    plan = describe(data_root, cuts)
    if not args.apply:
        if args.json:
            print(json.dumps({"dry_run": True, "ranges": plan}, indent=1))
        else:
            for r in plan:
                print(f"{r['file_path']}  {r['from']}..{r['to']}: {r['rows']} of {r['rows_in_file']} rows"
                      f" ({r['first']}..{r['last']}, close {r['close_min']}..{r['close_max']},"
                      f" zero volume {r['zero_volume']})")
            print(f"\n{sum(r['rows'] for r in plan)} rows would be deleted (dry run; add --apply)")
        return 0
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = args.run_id or f"stock-delete-{stamp}"
    real = Path(data_root).resolve()
    backup_dir = args.backup_dir or real.with_name(f"{real.name}.stock-delete-backup-{stamp}")

    def body() -> int:
        summary = apply(data_root, cuts, run_id, backup_dir, args.catalog)
        print(json.dumps(summary, indent=1) if args.json else
              f"{run_id}: {summary['rows_deleted']} rows deleted from {summary['files']} file(s); "
              f"originals in {backup_dir}; record {revisions_dir(data_root) / run_id}.parquet")
        return 0

    return run_with_catalog_backup("delete_stock_bars", all_catalog_paths(), body, args.catalog_backup_dir)


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
