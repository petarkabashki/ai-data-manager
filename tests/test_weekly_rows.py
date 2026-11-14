"""IBKR weekly files hold one row per Monday-to-Sunday week (owner decision 2026-11-14)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tools"))

import check_schema  # noqa: E402
import dedupe_weekly_bars as dwb  # noqa: E402
from utils.catalog import merge_ohlcv_parquet, replace_report  # noqa: E402

DAY = 86_400_000
# 2026-11-02 is a Monday: days since epoch.
MON = int(pd.Timestamp("2026-11-02").timestamp() // 86_400)


def bar(day: int, close: float) -> dict:
    return {"timestamp": day * DAY, "open": close, "high": close, "low": close, "close": close, "volume": 1.0}


def frame(*rows) -> pd.DataFrame:
    return pd.DataFrame([bar(d, c) for d, c in rows]).astype({"timestamp": "int64"})


def ibkr_1w(tmp_path: Path) -> Path:
    return tmp_path / "stocks" / "ibkr" / "russell2000" / "NEXN" / "NEXN-all-1w.parquet"


def test_two_appends_in_one_week_leave_one_row(tmp_path):
    dst = ibkr_1w(tmp_path)
    merge_ohlcv_parquet(frame((MON - 3, 9.0), (MON + 1, 10.0)), dst)  # prior Friday, partial Tue
    merge_ohlcv_parquet(frame((MON + 2, 11.0)), dst)                  # Wed: fuller bar, same week
    df = pd.read_parquet(dst)
    assert list(df["timestamp"]) == [(MON - 3) * DAY, (MON + 2) * DAY]
    assert df["close"].iloc[-1] == 11.0


def test_other_providers_weekly_files_are_untouched_by_the_merge(tmp_path):
    dst = tmp_path / "stocks" / "yf" / "sp500" / "AAPL" / "AAPL-1w.parquet"
    merge_ohlcv_parquet(frame((MON + 1, 10.0)), dst)
    merge_ohlcv_parquet(frame((MON + 2, 11.0)), dst)
    assert len(pd.read_parquet(dst)) == 2  # report only until decided


def test_check_schema_fails_on_a_repeated_week_in_an_ibkr_weekly_file(tmp_path):
    dst = ibkr_1w(tmp_path)
    dst.parent.mkdir(parents=True)
    frame((MON + 1, 10.0), (MON + 4, 11.0)).to_parquet(dst, index=False)
    issue = check_schema.check_file(dst)
    assert issue and "1 extra row(s) in a week" in issue and "dedupe_weekly_bars" in issue
    frame((MON + 4, 11.0), (MON + 11, 12.0)).to_parquet(dst, index=False)
    assert check_schema.check_file(dst) is None


def test_replace_report_counts_partial_weeks_apart_from_lost_rows(tmp_path):
    dst = ibkr_1w(tmp_path)
    dst.parent.mkdir(parents=True)
    frame((MON - 3, 9.0), (MON + 1, 10.0), (MON + 2, 10.5)).to_parquet(dst, index=False)
    rep = replace_report(frame((MON - 3, 9.0), (MON + 4, 11.0)), dst, replace_from=0)
    assert rep["partial_weeks"] == 1 and rep["rows_before"] == 3
    assert rep["lost"] == 1  # the week's surviving stored stamp (Wed) differs from the download's (Fri)


def test_dedupe_tool_backs_up_records_and_rewrites(tmp_path, monkeypatch):
    dst = ibkr_1w(tmp_path)
    dst.parent.mkdir(parents=True)
    frame((MON - 3, 9.0), (MON + 1, 10.0), (MON + 2, 10.5), (MON + 4, 11.0)).to_parquet(dst, index=False)
    monkeypatch.setattr(dwb, "running_universes", lambda: set())
    backup = tmp_path / "backup"
    assert dwb.main(["--data-root", str(tmp_path), "--apply", "--run-id", "wk-test",
                     "--backup-dir", str(backup)]) == 0
    assert list(pd.read_parquet(dst)["timestamp"]) == [(MON - 3) * DAY, (MON + 4) * DAY]
    assert len(pd.read_parquet(backup / dst.relative_to(tmp_path))) == 4
    rec = pd.read_parquet(tmp_path / "revisions" / "wk-test.parquet")
    assert sorted(rec["timestamp"]) == [(MON + 1) * DAY, (MON + 2) * DAY]
    assert set(rec["superseded_by"]) == {(MON + 4) * DAY}
    summary = yaml.safe_load((tmp_path / "revisions" / "wk-test.yaml").read_text())
    assert summary["files"] == 1 and summary["rows_removed"] == 2


def test_dedupe_tool_refuses_a_universe_with_a_running_download(tmp_path, monkeypatch):
    dst = ibkr_1w(tmp_path)
    dst.parent.mkdir(parents=True)
    frame((MON + 1, 10.0), (MON + 2, 10.5)).to_parquet(dst, index=False)
    monkeypatch.setattr(dwb, "running_universes", lambda: {"russell2000"})
    assert dwb.main(["--data-root", str(tmp_path), "--apply", "--backup-dir", str(tmp_path / "b")]) == 2
    assert len(pd.read_parquet(dst)) == 2
