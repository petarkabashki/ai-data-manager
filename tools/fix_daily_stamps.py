#!/usr/bin/env -S uv run python
"""Restamp date-labelled bars to 00:00 UTC and drop the duplicate dates.

    uv run python tools/fix_daily_stamps.py --provider yf --dry-run
    uv run python tools/fix_daily_stamps.py --provider yf --json

A daily (weekly, monthly) bar is labelled by its calendar date: `timestamp` is
00:00 UTC of that date, whatever the venue's time zone. Yahoo files broke the
rule. History imported before 2026-05 is stamped 00:00 UTC, but download_yf
then wrote Yahoo's own stamp, the exchange's local midnight (04:00/05:00 UTC
for New York, the previous UTC day for Europe and Asia). The merge
deduplicated on the exact timestamp, so every date both conventions covered
was stored twice.

For each `*-1d`/`-1w`/`-1Mo` parquet under `price-data/*/yf/`, this:

- labels every row with `date_label_ms` (rounds to the nearest UTC midnight,
  which maps each venue's local midnight to its own date);
- keeps one row per label. A row stamped at venue midnight came from
  download_yf and wins over a 00:00 row from the old import; otherwise the
  later row wins;
- copies the original into `--backup-dir` (default
  `<data root>.stamp-backup-<today>`, beside the data root, never inside it),
  then writes the result atomically.

Dates and catalog bounds don't change, so no catalog is rewritten. A file
already in convention is left alone, so a second run changes nothing. Run it
through PDM (`submit fix_daily_stamps`): its lane is `equity`, so it can't
race a Yahoo download over the same files.

Exit status: 0 when every file was read (and, unless --dry-run, written),
1 when any failed, 2 on a bad argument. `--json` writes one JSON document to
stdout (keys: provider, dry_run, backup_dir, scanned, changed, rows_dropped,
files[], skipped[] (files without a `timestamp` column), failures[]) and the progress log to stderr.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import DATA_ROOT, DATE_LABELLED_TIMEFRAMES, date_label_ms, write_parquet_atomic
from utils.config import relative_to_repo

PROVIDERS = ("yf",)


def provider_files(root: Path, provider: str) -> list[Path]:
    return sorted(f for tf in DATE_LABELLED_TIMEFRAMES
                  for f in root.glob(f"*/{provider}/**/*-{tf}.parquet"))


def restamp(df: pd.DataFrame) -> pd.DataFrame:
    """One row per date label, stamped 00:00 UTC; venue-midnight rows win."""
    label = date_label_ms(df["timestamp"])
    out = df.assign(_venue=(df["timestamp"] != label), _pos=range(len(df)), timestamp=label)
    out = out.sort_values(["timestamp", "_venue", "_pos"])
    out = out.drop_duplicates(subset=["timestamp"], keep="last")
    return out.drop(columns=["_venue", "_pos"]).reset_index(drop=True)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--provider", required=True, choices=PROVIDERS)
    p.add_argument("--backup-dir", type=Path,
                   help="where originals are copied (default: <data root>.stamp-backup-<today>)")
    p.add_argument("--dry-run", action="store_true", help="report only: no backups, no writes")
    p.add_argument("--json", action="store_true", help="one JSON document on stdout")
    args = p.parse_args(argv)

    log = (lambda *a: print(*a, file=sys.stderr, flush=True)) if args.json else print
    root = DATA_ROOT
    real = root.resolve()       # price-data is often a symlink: back up beside its target
    backup_dir = args.backup_dir or real.with_name(f"{real.name}.stamp-backup-{date.today().isoformat()}")
    if backup_dir.resolve().is_relative_to(real):
        p.error(f"--backup-dir must be outside the data root: {backup_dir}")

    files = provider_files(root, args.provider)
    doc = {"provider": args.provider, "dry_run": args.dry_run, "backup_dir": str(backup_dir),
           "scanned": len(files), "changed": 0, "rows_dropped": 0, "files": [], "skipped": [],
           "failures": []}
    for path in files:
        rel = relative_to_repo(path)
        try:
            df = pd.read_parquet(path)
            if "timestamp" not in df.columns:   # not OHLCV (macro's Date/Value layout)
                doc["skipped"].append(rel)
                continue
            fixed = restamp(df)
            if len(fixed) == len(df) and fixed["timestamp"].equals(df["timestamp"].reset_index(drop=True)):
                continue
            if not args.dry_run:
                dst = backup_dir / path.relative_to(root)
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, dst)
                write_parquet_atomic(fixed, path)
        except Exception as exc:  # noqa: BLE001 — one file's failure is reported, not fatal
            doc["failures"].append({"file": rel, "error": f"{type(exc).__name__}: {exc}"})
            log(f"{rel}: FAILED {type(exc).__name__}: {exc}")
            continue
        dropped = len(df) - len(fixed)
        doc["changed"] += 1
        doc["rows_dropped"] += dropped
        doc["files"].append({"file": rel, "rows_before": len(df), "rows_after": len(fixed),
                             "dropped": dropped})
    verb = "would change" if args.dry_run else "changed"
    log(f"{doc['scanned']} {args.provider} file(s) scanned, {verb} {doc['changed']}, "
        f"{doc['rows_dropped']} duplicate row(s) dropped, {len(doc['failures'])} failed"
        + ("" if args.dry_run or not doc["changed"] else f"; originals in {backup_dir}"))
    if args.json:
        print(json.dumps(doc))
    return 1 if doc["failures"] else 0


if __name__ == "__main__":
    sys.exit(main())
