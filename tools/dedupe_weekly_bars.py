#!/usr/bin/env -S uv run python
"""Keep one row per week in IBKR weekly files: drop superseded partial-week bars.

IBKR stamps a weekly bar with the last trading day it covers. A fetch in
mid-week stored a partial bar under that day, and the next fetch added the
fuller bar under a later stamp beside it, so weeks held 2 to 5 rows. The 1w
merge now keeps the last row of each Monday-to-Sunday week
(`utils.catalog.one_row_per_week`, owner decision 2026-11-14); this repairs
the files written before that.

    uv run python tools/dedupe_weekly_bars.py                 # dry run, IBKR
    uv run python tools/dedupe_weekly_bars.py --apply
    uv run python tools/dedupe_weekly_bars.py --report-only --provider yf

Dry run by default: files and rows that would go, by universe and by week.
`--apply`:

- refuses a universe while an IBKR stock download for it is running (it
  merges into these files without a lock);
- copies each original into `--backup-dir` (default
  `<data root>.weekly-dedupe-backup-<UTC stamp>`, beside the data root);
- writes the revision record `revisions/<run id>.parquet` (one row per removed
  bar: file, old values, content hash, the stamp that superseded it) and
  `revisions/<run id>.yaml`;
- rewrites each file atomically.

Removed rows are not recorded as deleted bars: the week stays covered by its
final bar, and a later fetch of that week is welcome.

`--report-only --provider P` counts the same pattern in another provider's 1w
files (yf, or a crypto venue directory) without touching anything.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from delete_twin_bars import revisions_dir, row_hash  # noqa: E402
from utils.catalog import normalize_ohlcv, one_row_per_week, week_key, write_parquet_atomic  # noqa: E402
from utils.config import DATA_ROOT  # noqa: E402
from utils.report import allow_broken_pipe  # noqa: E402

IBKR_DOWNLOADER = "download_ibkr_stock_overnight"


def weekly_files(data_root: Path, provider: str) -> list[Path]:
    if provider == "ibkr":
        roots = [data_root / "stocks" / "ibkr", data_root / "indexes" / "ibkr"]
    elif provider == "yf":
        roots = [data_root / "stocks" / "yf", data_root / "indexes" / "yf"]
    else:
        roots = [data_root / "cryptos" / provider, data_root / "crypto-futures" / provider]
    return sorted(p for r in roots if r.exists() for p in r.rglob("*-1w.parquet")
                  if not any(part.startswith(".") for part in p.relative_to(data_root).parts))


def superseded(df: pd.DataFrame) -> pd.DataFrame:
    """Rows `one_row_per_week` drops, with the stamp of the row that wins the week."""
    df = normalize_ohlcv(df).sort_values("timestamp").reset_index(drop=True)
    kept = one_row_per_week(df)
    gone = df[~df["timestamp"].isin(kept["timestamp"])].copy()
    winner = dict(zip(week_key(kept["timestamp"]), kept["timestamp"]))
    gone["kept_timestamp"] = week_key(gone["timestamp"]).map(winner).astype("int64")
    return gone


def plan(data_root: Path, provider: str) -> list[dict]:
    out = []
    for path in weekly_files(data_root, provider):
        gone = superseded(pd.read_parquet(path))
        if len(gone):
            rel = path.relative_to(data_root)
            out.append({"path": path, "rel": str(rel), "universe": rel.parts[2] if len(rel.parts) > 3 else "",
                        "gone": gone})
    return out


def running_universes() -> set[str]:
    """Universes named on the command line of a running IBKR stock download."""
    found: set[str] = set()
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            argv = Path(f"/proc/{pid}/cmdline").read_bytes().decode(errors="replace").split("\0")
        except OSError:
            continue
        if not any(IBKR_DOWNLOADER in a for a in argv) or "--dry-run" in argv:
            continue
        if "--universe" in argv:
            i = argv.index("--universe") + 1
            while i < len(argv) and argv[i] and not argv[i].startswith("--"):
                found.add(argv[i])
                i += 1
        else:
            found.add("*")
    return found


def summarize(items: list[dict]) -> dict:
    by_universe: dict[str, list[int]] = {}
    by_week: dict[str, int] = {}
    for it in items:
        u = by_universe.setdefault(it["universe"], [0, 0])
        u[0] += 1
        u[1] += len(it["gone"])
        for k in it["gone"]["kept_timestamp"]:
            week = pd.Timestamp(int(k), unit="ms").to_period("W-SUN").start_time.date().isoformat()
            by_week[week] = by_week.get(week, 0) + 1
    top = dict(sorted(by_week.items(), key=lambda kv: -kv[1])[:8])
    return {"files": len(items), "rows": sum(len(it["gone"]) for it in items),
            "by_universe": {k: {"files": v[0], "rows": v[1]} for k, v in sorted(by_universe.items())},
            "top_weeks": top}


def apply(data_root: Path, items: list[dict], run_id: str, backup_dir: Path) -> dict:
    records = []
    for it in items:
        path: Path = it["path"]
        dst = backup_dir / it["rel"]
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dst)
    for it in items:
        path = it["path"]
        df = normalize_ohlcv(pd.read_parquet(path))
        gone = superseded(df)  # re-read: the plan may be minutes old
        kept = one_row_per_week(df)
        write_parquet_atomic(kept, path)
        for row in gone.itertuples(index=False):
            records.append({"run_id": run_id, "file_path": it["rel"], "timestamp": int(row.timestamp),
                            "open": row.open, "high": row.high, "low": row.low, "close": row.close,
                            "volume": row.volume, "row_hash": row_hash(row),
                            "superseded_by": int(row.kept_timestamp)})
    out = revisions_dir(data_root)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_parquet(out / f"{run_id}.parquet", index=False)
    summary = {"run_id": run_id, "tool": "dedupe_weekly_bars", "backup_dir": str(backup_dir),
               "files": len(items), "rows_removed": len(records),
               "reason": "IBKR 1w partial-week rows superseded by the week's last bar "
                         "(owner decision 2026-11-14)",
               "applied": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    (out / f"{run_id}.yaml").write_text(yaml.safe_dump(summary, sort_keys=False))
    return summary


def main(argv: list[str] | None = None) -> int:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--provider", default="ibkr", help="ibkr (default), yf, or a crypto venue directory")
    p.add_argument("--universe", nargs="+", help="Only these stock universes (IBKR)")
    p.add_argument("--apply", action="store_true", help="Back up, rewrite, write the revision record")
    p.add_argument("--report-only", action="store_true", help="Count only; never writes (for non-IBKR providers)")
    p.add_argument("--run-id")
    p.add_argument("--backup-dir", type=Path)
    p.add_argument("--data-root", type=Path, default=DATA_ROOT)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    if args.apply and (args.report_only or args.provider != "ibkr"):
        p.error("--apply only repairs IBKR files; other providers need their own decision")
    items = plan(args.data_root, args.provider)
    if args.universe:
        items = [it for it in items if it["universe"] in args.universe]
    summary = summarize(items)
    summary["provider"] = args.provider
    summary["weekly_files"] = len(weekly_files(args.data_root, args.provider))

    if args.apply and items:
        busy = running_universes()
        blocked = sorted({it["universe"] for it in items} & busy) if "*" not in busy else ["all"]
        if blocked:
            print(f"refused: an IBKR download is running for {', '.join(blocked)}; "
                  f"wait for it or pass --universe without it", file=sys.stderr)
            return 2
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = args.run_id or f"weekly-dedupe-{stamp}"
        real = args.data_root.resolve()
        backup_dir = args.backup_dir or real.with_name(f"{real.name}.weekly-dedupe-backup-{stamp}")
        summary.update(apply(args.data_root, items, run_id, backup_dir))

    if args.json:
        json.dump(summary, sys.stdout, indent=2)
        sys.stdout.write("\n")
    else:
        mode = "applied" if args.apply else "dry run"
        print(f"{args.provider} 1w ({mode}): {summary['files']} of {summary['weekly_files']} files, "
              f"{summary['rows']} superseded partial-week rows")
        for u, v in summary["by_universe"].items():
            print(f"  {u or '-'}: {v['files']} files, {v['rows']} rows")
        print("  top weeks:", ", ".join(f"{w} {n}" for w, n in summary["top_weeks"].items()))
        if args.apply and items:
            print(f"  run id {summary['run_id']}; originals in {summary['backup_dir']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
