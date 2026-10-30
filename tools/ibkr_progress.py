#!/usr/bin/env -S uv run python
"""One-line status for a running `download_ibkr_stock_overnight.py`.

The download tool rewrites a JSON snapshot after each timeframe result; its
stdout is block-buffered when redirected to a file, so the snapshot is the
reliable way to follow a long run.

    uv run python tools/ibkr_progress.py                  # newest snapshot in TMP_ROOT
    uv run python tools/ibkr_progress.py --watch 30       # refresh every 30 s until done
    uv run python tools/ibkr_progress.py --json           # for an agent
    uv run python tools/ibkr_progress.py --file tmp/ibkr_stock_progress_all_1h-4h.json

Exit code: 0 ok, 1 failures recorded or the run looks stalled, 2 no snapshot.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import TMP_ROOT


def newest_snapshot() -> Path | None:
    files = sorted(TMP_ROOT.glob("ibkr_stock_progress_*.json"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def summarize(path: Path, stall_seconds: float) -> dict:
    state = json.loads(path.read_text())
    done, planned = state.get("completed_items", 0), state.get("planned_items", 0)
    updated = datetime.fromisoformat(state["updated_at"])
    age = (datetime.now(timezone.utc) - updated).total_seconds()
    running = state.get("status") == "running"
    cur = state.get("current") or {}
    return {
        "file": str(path),
        "status": state.get("status"),
        "completed": done,
        "planned": planned,
        "pct": round(100 * done / planned, 1) if planned else 0.0,
        "updated_items": state.get("updated_items", 0),
        "failures": state.get("failures", []),
        "current": f"{cur.get('symbol')}/{cur.get('timeframe')}" if cur.get("symbol") else None,
        "age_seconds": round(age),
        "stalled": running and age > stall_seconds,
    }


def render(s: dict) -> str:
    cur = f" now {s['current']}" if s["current"] and s["status"] == "running" else ""
    flag = "  STALLED" if s["stalled"] else ""
    return (f"{s['status']}: {s['completed']}/{s['planned']} ({s['pct']}%), "
            f"{s['updated_items']} updated, {len(s['failures'])} failed{cur}, "
            f"last update {s['age_seconds']}s ago{flag}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--file", type=Path, help="Progress snapshot (default: newest in TMP_ROOT)")
    p.add_argument("--watch", type=float, metavar="SECONDS", help="Refresh until the run finishes")
    p.add_argument("--stall-seconds", type=float, default=600.0,
                   help="A running snapshot older than this is reported as stalled (default: 600)")
    p.add_argument("--json", action="store_true", help="Emit the summary as JSON")
    args = p.parse_args()

    while True:
        path = args.file or newest_snapshot()
        if path is None or not path.exists():
            print(f"no progress snapshot under {TMP_ROOT}", file=sys.stderr)
            return 2
        s = summarize(path, args.stall_seconds)
        print(json.dumps(s) if args.json else render(s), flush=True)
        if not args.watch or s["status"] != "running":
            break
        time.sleep(args.watch)
    return 1 if s["failures"] or s["stalled"] else 0


if __name__ == "__main__":
    sys.exit(main())
