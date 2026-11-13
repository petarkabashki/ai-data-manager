#!/usr/bin/env -S uv run python
"""Replace YF daily stock files with a full raw re-download, audited row by row.

Two faults can't be fixed by appending:

- a file whose closes are dividend-adjusted while its node says `raw`
  (`check_adjustment.py` verdict `mislabelled_raw_is_adjusted`);
- a split seam: history at the old share count joined to bars at the new one
  (`check_adjustment.py` YF `split_jumps`, e.g. the 2026-05-04 batch).

Both need the whole history again, as Yahoo serves it now, raw
(`auto_adjust=False`: split-adjusted, not dividend-adjusted, like IBKR TRADES;
owner decision 2026-11-13).

Pick files with `--files` (catalog `file_path`s) or from a `check_adjustment.py
--json` report with `--audit` plus `--verdict` and/or `--jump-date`.

Dry run by default: prints each file with its stored first and last date and
row count, which is the window it will be re-fetched for. `--apply`:

- fetches from the stored first date to today, raw;
- refuses a file whose fetch is empty or doesn't cover the stored range (within
  `--slack-days`), so a ticker Yahoo has renamed or dropped is never truncated;
- copies the original into `--backup-dir` (default
  `<data root>.yf-backup-<today>`, beside the data root, never inside it);
- writes the revision record `revisions/<run id>/` at the data root: one
  parquet part per file, one row per bar that changed, `replaced`/`deleted`
  with the old values and `refetched`/`added` with the new, each with a content
  hash (sha256 of timestamp and OHLCV, as `delete_twin_bars.py`), plus
  `revisions/<run id>.yaml` with the per-file summary;
- replaces the file atomically and refreshes its catalog nodes' dates.

A file whose part already exists is skipped, so `--run-id <id>` resumes a run
that stopped. The node's `adjustment` stays `raw`, which the content now is.

    uv run python tools/redownload_yf.py --audit adj.json --verdict mislabelled_raw_is_adjusted
    uv run python tools/redownload_yf.py --audit adj.json --jump-date 2026-05-04 --apply
    uv run python tools/redownload_yf.py --files price-data/stocks/yf/sp500/APH/APH-1d.parquet --apply
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from delete_twin_bars import OHLCV, row_hash  # noqa: E402

from utils.catalog import (  # noqa: E402
    all_catalog_paths, date_label_ms, normalize_ohlcv, refresh_dates_for_files,
    run_with_catalog_backup, write_parquet_atomic,
)
from utils.config import DATA_ROOT  # noqa: E402
from utils.deleted_bars import drop_deleted  # noqa: E402
from utils.planning import throttle_sleep  # noqa: E402
from utils.report import allow_broken_pipe, print_table  # noqa: E402
from utils.yf import YahooFetcher, fetch_ohlcv  # noqa: E402

DAY_MS = 86_400_000


@dataclass
class Target:
    file_path: str          # as stored in the catalog, "price-data/stocks/yf/<universe>/<SYM>/<SYM>-1d.parquet"
    symbol: str
    first: str = ""
    last: str = ""
    rows: int = 0


def resolve(data_root: Path, file_path: str) -> Path:
    return data_root / file_path.split("/", 1)[1]


def parse_target(file_path: str) -> Target:
    parts = Path(file_path).parts
    if len(parts) != 6 or parts[:3] != ("price-data", "stocks", "yf") or not parts[5].endswith("-1d.parquet"):
        raise ValueError(f"not a YF daily stock file: {file_path}")
    return Target(file_path, parts[4])


def select_from_audit(report: dict, verdict: str | None, jump_date: str | None) -> list[str]:
    out = []
    for row in report["files"]:
        if row["provider"] != "YF":
            continue
        if verdict and row["verdict"] != verdict:
            continue
        if jump_date and not any(s["date"] == jump_date for s in row.get("split_jumps", [])):
            continue
        out.append(row["file_path"])
    return sorted(set(out))


def describe(data_root: Path, target: Target) -> Target:
    ts = pd.read_parquet(resolve(data_root, target.file_path), columns=["timestamp"])["timestamp"]
    target.rows = len(ts)
    if len(ts):
        target.first = str(pd.to_datetime(int(ts.min()), unit="ms").date())
        target.last = str(pd.to_datetime(int(ts.max()), unit="ms").date())
    return target


def _daily(frame: pd.DataFrame) -> pd.DataFrame:
    frame = normalize_ohlcv(frame)
    frame = frame.assign(timestamp=date_label_ms(frame["timestamp"]))
    return frame.drop_duplicates("timestamp", keep="last").sort_values("timestamp").reset_index(drop=True)


def diff_rows(file_path: str, old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """One row per bar that changed: the old side and the new side."""
    joined = old.merge(new, on="timestamp", how="outer", suffixes=("_old", "_new"), indicator="side")
    cols = OHLCV[1:]
    same = joined["side"].eq("both")
    for c in cols:
        same &= joined[f"{c}_old"].eq(joined[f"{c}_new"])
    rows = []
    for rec in joined[~same].itertuples(index=False):
        for side, action in (("old", "replaced" if rec.side == "both" else "deleted"),
                             ("new", "refetched" if rec.side == "both" else "added")):
            if (side == "old" and rec.side == "right_only") or (side == "new" and rec.side == "left_only"):
                continue
            bar = pd.Series({"timestamp": int(rec.timestamp),
                             **{c: float(getattr(rec, f"{c}_{side}")) for c in cols}})
            rows.append({"file": file_path, "timestamp": int(rec.timestamp), "action": action,
                         **{c: bar[c] for c in cols}, "row_hash": row_hash(bar)})
    return pd.DataFrame(rows, columns=["file", "timestamp", "action", *cols, "row_hash"])


def redownload(data_root: Path, target: Target, fetcher, run_dir: Path, backup_dir: Path,
               slack_days: int, today: datetime) -> dict:
    """Fetch, check coverage, back up, record, replace. Returns the file's summary."""
    path = resolve(data_root, target.file_path)
    part = run_dir / (target.file_path.replace("/", "__") + ".parquet")
    if part.exists():
        return {"file": target.file_path, "status": "done_earlier"}
    old = _daily(pd.read_parquet(path))
    start = datetime.fromisoformat(target.first).replace(tzinfo=timezone.utc)
    new = fetch_ohlcv(fetcher, target.symbol, "Stock", "1d", start, today, auto_adjust=False)
    if new.empty:
        return {"file": target.file_path, "status": "refused", "reason": "Yahoo returned nothing"}
    new = drop_deleted(_daily(new), path)      # bars deleted on purpose stay out
    slack = slack_days * DAY_MS
    if new["timestamp"].iloc[0] > old["timestamp"].iloc[0] + slack or \
            new["timestamp"].iloc[-1] < old["timestamp"].iloc[-1] - slack:
        got = (f"{pd.to_datetime(new['timestamp'].iloc[0], unit='ms').date()}.."
               f"{pd.to_datetime(new['timestamp'].iloc[-1], unit='ms').date()}")
        return {"file": target.file_path, "status": "refused",
                "reason": f"fetch {got} doesn't cover stored {target.first}..{target.last}"}
    changes = diff_rows(target.file_path, old, new)
    dst = backup_dir / target.file_path.split("/", 1)[1]
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, dst)
    run_dir.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(new, path)
    changes.to_parquet(part, index=False)       # last: its presence means "this file is done"
    actions = changes["action"].value_counts().to_dict()
    return {"file": target.file_path, "status": "replaced", "rows_before": len(old), "rows_after": len(new),
            **{a: int(actions.get(a, 0)) for a in ("replaced", "deleted", "added")}}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--files", nargs="+", metavar="FILE_PATH", help="Catalog file_paths to re-download")
    p.add_argument("--audit", type=Path, help="A check_adjustment.py --json report to select from")
    p.add_argument("--verdict", help="With --audit: YF rows with this verdict")
    p.add_argument("--jump-date", help="With --audit: YF rows with a split jump on this date")
    p.add_argument("--apply", action="store_true", help="Back up, record and replace")
    p.add_argument("--backup-dir", type=Path, help="Default: <data root>.yf-backup-<today>")
    p.add_argument("--catalog-backup-dir", type=Path,
                   help="Default: TMP_ROOT/catalog-backup-redownload_yf-<UTC stamp>")
    p.add_argument("--run-id", help="Resume this run (skips files already recorded)")
    p.add_argument("--slack-days", type=int, default=7,
                   help="How far inside the stored range the fetch may start or end (default 7)")
    p.add_argument("--sleep-seconds", type=float, default=0.5)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    if args.files:
        paths = sorted(set(args.files))
    elif args.audit:
        if not (args.verdict or args.jump_date):
            p.error("--audit needs --verdict and/or --jump-date")
        paths = select_from_audit(json.loads(args.audit.read_text()), args.verdict, args.jump_date)
    else:
        p.error("give --files or --audit")
    try:
        targets = [describe(DATA_ROOT, parse_target(fp)) for fp in paths]
    except (ValueError, FileNotFoundError) as exc:
        p.error(str(exc))

    if not args.apply:
        if args.json:
            print(json.dumps({"dry_run": True, "files": [asdict(t) for t in targets]}, indent=1))
        else:
            print_table(["file", "first", "last", "rows"], [[t.file_path, t.first, t.last, t.rows] for t in targets])
            print(f"\n{len(targets)} files. Dry run: nothing fetched or written. Add --apply.")
        return 0

    real = DATA_ROOT.resolve()
    backup_dir = args.backup_dir or real.with_name(f"{real.name}.yf-backup-{date.today().isoformat()}")
    if backup_dir.resolve().is_relative_to(real):
        p.error(f"--backup-dir must be outside the data root: {backup_dir}")
    return run_with_catalog_backup("redownload_yf", all_catalog_paths(),
                                   lambda: apply_redownload(args, targets, backup_dir), args.catalog_backup_dir)


def apply_redownload(args: argparse.Namespace, targets: list[Target], backup_dir: Path) -> int:
    """Back up, fetch and replace each target, then refresh the catalogs' dates."""
    now = datetime.now(tz=timezone.utc)
    run_id = args.run_id or f"yf-redownload-{now.strftime('%Y%m%dT%H%M%SZ')}"
    run_dir = DATA_ROOT / "revisions" / run_id
    today = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    fetcher = YahooFetcher()
    results, rewritten = [], []
    for i, target in enumerate(targets, 1):
        try:
            result = redownload(DATA_ROOT, target, fetcher, run_dir, backup_dir, args.slack_days, today)
        except Exception as exc:  # noqa: BLE001 — one bad ticker must not stop the batch
            result = {"file": target.file_path, "status": "failed", "reason": f"{type(exc).__name__}: {exc}"}
        results.append(result)
        if result["status"] == "replaced":
            rewritten.append(resolve(DATA_ROOT, target.file_path))
        if not args.json:
            print(f"[{i}/{len(targets)}] {result['status']:<12} {target.file_path} {result.get('reason', '')}",
                  flush=True)
        throttle_sleep(args.sleep_seconds)

    summary = {"run_id": run_id, "backup_dir": str(backup_dir), "adjustment": "raw",
               "selection": {"files": args.files, "audit": str(args.audit) if args.audit else None,
                             "verdict": args.verdict, "jump_date": args.jump_date},
               "counts": pd.Series([r["status"] for r in results]).value_counts().to_dict(),
               "files": results}
    try:
        summary["catalogs_written"] = refresh_dates_for_files(rewritten)
    except Exception as exc:  # noqa: BLE001 — the files are replaced; say what's left
        summary["catalogs_error"] = f"{type(exc).__name__}: {exc}; run check_catalog.py --fix"
    (DATA_ROOT / "revisions").mkdir(exist_ok=True)
    (DATA_ROOT / "revisions" / f"{run_id}.yaml").write_text(yaml.safe_dump(summary, sort_keys=False))
    if args.json:
        print(json.dumps({k: v for k, v in summary.items() if k != "files"}
                         | {"problems": [r for r in results if r["status"] in ("refused", "failed")]}, indent=1))
    else:
        print(f"\n{run_id}: {summary['counts']}; originals in {backup_dir}")
    bad = any(r["status"] in ("refused", "failed") for r in results) or "catalogs_error" in summary
    return 1 if bad else 0


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
