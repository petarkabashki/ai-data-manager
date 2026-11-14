"""A re-fetch across a split replaces stored rows: no bar survives on the old price scale."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tools"))

from download_ibkr_stock_overnight import (  # noqa: E402
    backup_before_replace, format_replace, replace_refusal,
)
from utils.catalog import merge_ohlcv_parquet, replace_report  # noqa: E402

DAY = 86_400_000


def bars(days, close) -> pd.DataFrame:
    days = list(days)
    c = [float(close)] * len(days) if not callable(close) else [close(d) for d in days]
    return pd.DataFrame({"timestamp": pd.Series([d * DAY for d in days], dtype="int64"),
                         "open": c, "high": c, "low": c, "close": c, "volume": [1.0] * len(days)})


def test_plain_merge_keeps_old_scale_rows_the_source_no_longer_serves(tmp_path):
    dst = tmp_path / "MNST-all-1d.parquet"
    merge_ohlcv_parquet(bars(range(0, 20), 100.0), dst)
    merge_ohlcv_parquet(bars(range(0, 19), 50.0), dst)  # split-adjusted, day 19 not served
    assert pd.read_parquet(dst)["close"].iloc[-1] == 100.0  # the seam this replace removes


def test_replace_from_the_start_rebuilds_from_the_download(tmp_path):
    dst = tmp_path / "MNST-all-1d.parquet"
    merge_ohlcv_parquet(bars(range(0, 20), 100.0), dst)
    before, after = merge_ohlcv_parquet(bars(range(0, 19), 50.0), dst, replace_from=0)
    df = pd.read_parquet(dst)
    assert (before, after) == (20, 19) and set(df["close"]) == {50.0}


def test_replace_from_a_date_keeps_earlier_rows(tmp_path):
    dst = tmp_path / "SUNS-all-1d.parquet"
    merge_ohlcv_parquet(bars(range(0, 30), 10.0), dst)
    merge_ohlcv_parquet(bars(range(10, 29), 12.5), dst, replace_from=10 * DAY)
    df = pd.read_parquet(dst)
    assert len(df) == 29
    assert set(df[df["timestamp"] < 10 * DAY]["close"]) == {10.0}
    assert set(df[df["timestamp"] >= 10 * DAY]["close"]) == {12.5}


def test_replace_respects_deleted_bars(tmp_path):
    root = tmp_path / "price-data"
    dst = root / "stocks/ibkr/russell2000/RCAT/RCAT-all-1d.parquet"
    dst.parent.mkdir(parents=True)
    (root / "deleted_bars.yaml").write_text(
        "version: 1\nseries:\n  ibkr/russell2000/RCAT/RCAT-all-1d.parquet:\n"
        f"  - {{start_ms: 0, end_ms: {4 * DAY}, start: a, end: b}}\n")
    merge_ohlcv_parquet(bars(range(5, 20), 1.0), dst)
    merge_ohlcv_parquet(bars(range(0, 20), 2.0), dst, replace_from=0)
    assert pd.read_parquet(dst)["timestamp"].min() == 5 * DAY


def test_report_counts_and_writes_nothing(tmp_path):
    dst = tmp_path / "APH-all-1d.parquet"
    merge_ohlcv_parquet(bars(range(0, 100), 100.0), dst)
    mtime = dst.stat().st_mtime_ns
    new = bars(range(0, 101), lambda d: 50.0 if d < 50 else 100.0)  # one more bar, half rescaled
    r = replace_report(new, dst, 0)
    assert dst.stat().st_mtime_ns == mtime
    assert (r["rows_before"], r["rows_after"], r["span_old"], r["span_new"]) == (100, 101, 100, 101)
    assert (r["changed"], r["lost"], r["added"]) == (50, 0, 1)
    assert replace_refusal(r) is None
    assert "rows 100 -> 101" in format_replace(r)


def test_refuses_a_short_or_late_download(tmp_path):
    dst = tmp_path / "DD-all-1d.parquet"
    merge_ohlcv_parquet(bars(range(0, 200), 100.0), dst)
    late = replace_report(bars(range(10, 200), 50.0), dst, 0)
    assert "later" in replace_refusal(late)
    holey = replace_report(bars([d for d in range(0, 200) if d % 10], 50.0), dst, 0)
    assert "not in the download" in replace_refusal(holey)
    assert "empty" in replace_refusal(replace_report(bars([], 1.0), dst, 0))
    few = replace_report(bars([d for d in range(0, 200) if d not in (50, 51)], 50.0), dst, 0)
    assert replace_refusal(few) is None  # 1% missing: a holiday-calendar difference, not a loss


def test_backup_keeps_the_path_below_the_data_root(tmp_path):
    dst = tmp_path / "price-data/stocks/ibkr/sp500/MNST/MNST-all-1d.parquet"
    dst.parent.mkdir(parents=True)
    dst.write_bytes(b"x")
    saved = backup_before_replace(dst, tmp_path / "bk", tmp_path / "price-data")
    assert saved == tmp_path / "bk/stocks/ibkr/sp500/MNST/MNST-all-1d.parquet"
    assert saved.read_bytes() == b"x"
