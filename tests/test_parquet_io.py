"""Atomic writes and the single merge implementation."""

from __future__ import annotations

import pandas as pd
import pytest

from utils.catalog import merge_ohlcv_parquet, normalize_ohlcv, write_parquet_atomic

DAY = 86_400_000


def test_write_atomic_creates_parents(tmp_path, ohlcv):
    dst = tmp_path / "a" / "b" / "x.parquet"
    write_parquet_atomic(ohlcv(), dst)
    assert dst.exists()
    assert not list(dst.parent.glob("*.tmp"))


def test_write_atomic_leaves_original_on_failure(tmp_path, ohlcv):
    dst = tmp_path / "x.parquet"
    write_parquet_atomic(ohlcv(n=3), dst)
    original = pd.read_parquet(dst)

    class Exploding(pd.DataFrame):
        def to_parquet(self, *a, **kw):
            raise RuntimeError("disk full")

    with pytest.raises(RuntimeError):
        write_parquet_atomic(Exploding(ohlcv(n=99)), dst)

    pd.testing.assert_frame_equal(pd.read_parquet(dst), original)
    assert not list(tmp_path.glob("*.tmp")), "temp file left behind"


def test_merge_into_missing_destination(tmp_path, ohlcv):
    dst = tmp_path / "new.parquet"
    old, new = merge_ohlcv_parquet(ohlcv(n=4), dst)
    assert (old, new) == (0, 4)


def test_merge_appends_and_dedups(tmp_path, ohlcv):
    dst = tmp_path / "x.parquet"
    merge_ohlcv_parquet(ohlcv(start_ms=0, n=5), dst)
    old, new = merge_ohlcv_parquet(ohlcv(start_ms=3 * DAY, n=5), dst)
    assert old == 5
    assert new == 8  # 2 overlapping bars deduped
    ts = pd.read_parquet(dst)["timestamp"].tolist()
    assert ts == sorted(ts) == [i * DAY for i in range(8)]


def test_merge_prepends_history(tmp_path, ohlcv):
    dst = tmp_path / "x.parquet"
    merge_ohlcv_parquet(ohlcv(start_ms=10 * DAY, n=3), dst)
    merge_ohlcv_parquet(ohlcv(start_ms=0, n=3), dst)
    assert pd.read_parquet(dst)["timestamp"].iloc[0] == 0


def test_merge_new_data_wins_on_conflict(tmp_path, ohlcv):
    """A re-downloaded bar replaces the stored one.

    Append mode re-requests the last day precisely because the stored bar may
    be a partial candle, so the newer value has to win.
    """
    dst = tmp_path / "x.parquet"
    merge_ohlcv_parquet(ohlcv(n=3, close=1.0), dst)
    merge_ohlcv_parquet(ohlcv(n=3, close=2.0), dst)
    assert pd.read_parquet(dst)["close"].tolist() == [2.0, 2.0, 2.0]


def test_merge_normalizes_non_canonical_input(tmp_path):
    dst = tmp_path / "x.parquet"
    legacy = pd.DataFrame(
        {"0": [0.0, float(DAY)], "1": [1.0, 1.0], "2": [1.0, 1.0],
         "3": [1.0, 1.0], "4": [1.0, 1.0], "5": [1.0, 1.0]}
    )
    merge_ohlcv_parquet(legacy, dst)
    stored = pd.read_parquet(dst)
    assert list(stored.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert stored["timestamp"].dtype == "int64"


def test_merge_accepts_a_source_path(tmp_path, ohlcv):
    src, dst = tmp_path / "src.parquet", tmp_path / "dst.parquet"
    write_parquet_atomic(ohlcv(n=2), src)
    assert merge_ohlcv_parquet(src, dst) == (0, 2)


def test_merge_result_reloads_as_canonical(tmp_path, ohlcv):
    dst = tmp_path / "x.parquet"
    merge_ohlcv_parquet(ohlcv(n=4), dst)
    reloaded = pd.read_parquet(dst)
    pd.testing.assert_frame_equal(reloaded, normalize_ohlcv(reloaded))
