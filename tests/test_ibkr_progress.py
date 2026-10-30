"""tools/ibkr_progress.py: summary of the downloader's progress snapshot."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

import ibkr_progress  # noqa: E402


def _snapshot(path: Path, *, status="running", age=5, failures=()):
    updated = datetime.now(timezone.utc) - timedelta(seconds=age)
    path.write_text(json.dumps({
        "status": status, "completed_items": 5, "planned_items": 20, "updated_items": 4,
        "failures": list(failures), "updated_at": updated.isoformat(),
        "current": {"symbol": "AAPL", "timeframe": "1h"},
    }))
    return path


def test_summarize_reports_progress_and_current(tmp_path):
    s = ibkr_progress.summarize(_snapshot(tmp_path / "p.json"), stall_seconds=600)
    assert (s["completed"], s["planned"], s["pct"]) == (5, 20, 25.0)
    assert s["current"] == "AAPL/1h" and not s["stalled"]
    assert "5/20 (25.0%)" in ibkr_progress.render(s) and "now AAPL/1h" in ibkr_progress.render(s)


def test_running_snapshot_older_than_threshold_is_stalled(tmp_path):
    s = ibkr_progress.summarize(_snapshot(tmp_path / "p.json", age=900), stall_seconds=600)
    assert s["stalled"] and "STALLED" in ibkr_progress.render(s)


def test_finished_snapshot_is_never_stalled(tmp_path):
    s = ibkr_progress.summarize(_snapshot(tmp_path / "p.json", status="complete", age=9999), stall_seconds=600)
    assert not s["stalled"]


def test_exit_codes(tmp_path, monkeypatch):
    ok = _snapshot(tmp_path / "ok.json")
    bad = _snapshot(tmp_path / "bad.json", failures=[{"symbol": "X"}])
    for path, code in ((ok, 0), (bad, 1), (tmp_path / "missing.json", 2)):
        monkeypatch.setattr(sys, "argv", ["ibkr_progress.py", "--file", str(path)])
        assert ibkr_progress.main() == code
