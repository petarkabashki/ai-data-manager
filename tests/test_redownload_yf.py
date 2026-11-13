"""redownload_yf replaces a YF file whole and records every bar it changed."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from redownload_yf import Target, describe, redownload, select_from_audit  # noqa: E402

DAY = 86_400_000
FP = "price-data/stocks/yf/sp500/APH/APH-1d.parquet"


def _bars(closes, start_day=20_000):
    return pd.DataFrame({
        "timestamp": pd.Series([(start_day + i) * DAY for i in range(len(closes))], dtype="int64"),
        "open": closes, "high": closes, "low": closes, "close": closes, "volume": [1.0] * len(closes),
    })


class Fake:
    def __init__(self, closes, start_day=20_000):
        self.closes, self.start_day = closes, start_day

    def fetch(self, ticker, interval, start, end, auto_adjust):
        assert auto_adjust is False
        days = pd.to_datetime([(self.start_day + i) * DAY for i in range(len(self.closes))], unit="ms")
        c = self.closes
        return pd.DataFrame({"Open": c, "High": c, "Low": c, "Close": c, "Volume": 1.0},
                            index=pd.DatetimeIndex(days, name="Date"))


@pytest.fixture
def stored(tmp_path):
    path = tmp_path / FP.split("/", 1)[1]
    path.parent.mkdir(parents=True)
    _bars([100.0, 100.0, 50.0, 50.0]).to_parquet(path, index=False)     # seam on day 3
    return tmp_path, path


def _run(root, fetcher):
    target = describe(root, Target(FP, "APH"))
    return redownload(root, target, fetcher, root / "revisions" / "run", root.parent / "backup",
                      slack_days=7, today=datetime(2030, 1, 1, tzinfo=timezone.utc))


def test_a_seam_is_replaced_backed_up_and_recorded(stored):
    root, path = stored
    result = _run(root, Fake([50.0, 50.0, 50.0, 50.0]))

    assert result["status"] == "replaced" and result["replaced"] == 2
    assert list(pd.read_parquet(path)["close"]) == [50.0] * 4
    assert list(pd.read_parquet(root.parent / "backup" / FP.split("/", 1)[1])["close"]) == [100, 100, 50, 50]
    record = pd.read_parquet(root / "revisions" / "run")
    assert sorted(record["action"]) == ["refetched", "refetched", "replaced", "replaced"]
    assert set(record.loc[record.action == "replaced", "close"]) == {100.0}
    assert record["row_hash"].str.len().eq(64).all()


def test_a_fetch_that_does_not_cover_the_file_is_refused(stored):
    root, path = stored
    result = _run(root, Fake([50.0, 50.0], start_day=20_100))

    assert result["status"] == "refused"
    assert list(pd.read_parquet(path)["close"]) == [100, 100, 50, 50]
    assert not (root / "revisions" / "run").exists()


def test_a_recorded_file_is_skipped_on_resume(stored):
    root, _ = stored
    _run(root, Fake([50.0] * 4))
    assert _run(root, Fake([1.0] * 4))["status"] == "done_earlier"


def test_selection_from_an_audit_report():
    report = {"files": [
        {"provider": "YF", "file_path": "a", "verdict": "mislabelled_raw_is_adjusted", "split_jumps": []},
        {"provider": "YF", "file_path": "b", "verdict": "ok",
         "split_jumps": [{"date": "2026-05-04"}]},
        {"provider": "IBKR", "file_path": "c", "verdict": "split_jump", "split_jumps": [{"date": "2026-05-04"}]},
    ]}
    assert select_from_audit(report, "mislabelled_raw_is_adjusted", None) == ["a"]
    assert select_from_audit(report, None, "2026-05-04") == ["b"]
