"""tools/dedupe_stock_universes.py: fold a later universe's IBKR files into the home copy."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

import dedupe_stock_universes as dd  # noqa: E402

HOUR = 3_600_000


def _write(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path


def test_missing_home_file_is_moved(tmp_path, ohlcv):
    src = _write(ohlcv(0, 3, HOUR), tmp_path / "nq" / "A-all-1h.parquet")
    plan = dd.plan_file(src, tmp_path / "sp" / "A-all-1h.parquet")
    assert plan.action == "move" and plan.only_rows is None


def test_identical_copy_is_dropped(tmp_path, ohlcv):
    src = _write(ohlcv(0, 3, HOUR), tmp_path / "nq" / "A-all-1h.parquet")
    dst = _write(ohlcv(0, 3, HOUR), tmp_path / "sp" / "A-all-1h.parquet")
    plan = dd.plan_file(src, dst)
    assert plan.action == "drop" and plan.differing == 0


def test_home_with_more_rows_still_drops(tmp_path, ohlcv):
    src = _write(ohlcv(0, 3, HOUR), tmp_path / "nq" / "A-all-1h.parquet")
    dst = _write(ohlcv(0, 6, HOUR), tmp_path / "sp" / "A-all-1h.parquet")
    assert dd.plan_file(src, dst).action == "drop"


def test_rows_only_in_the_duplicate_are_merged_and_changed_values_counted(tmp_path, ohlcv):
    src = _write(ohlcv(0, 101, HOUR, close=1.0), tmp_path / "nq" / "A-all-1h.parquet")
    home = ohlcv(0, 100, HOUR, close=1.0)
    home.loc[1, "close"] = 9.0  # 1% of the shared bars: a revision, not a different price scale
    dst = _write(home, tmp_path / "sp" / "A-all-1h.parquet")
    plan = dd.plan_file(src, dst)
    assert plan.action == "merge"
    assert plan.only_rows["timestamp"].tolist() == [100 * HOUR]
    assert plan.differing == 1


def test_copies_on_different_price_scales_are_a_conflict_not_folded(tmp_path, ohlcv):
    src = _write(ohlcv(0, 6, HOUR, close=50.0), tmp_path / "nq" / "A-all-1h.parquet")
    dst = _write(ohlcv(0, 6, HOUR, close=12.5), tmp_path / "sp" / "A-all-1h.parquet")
    plan = dd.plan_file(src, dst)
    assert plan.action == "conflict" and abs(plan.median_ratio - 4.0) < 1e-9


def test_a_few_revised_volumes_do_not_make_a_conflict(tmp_path, ohlcv):
    src = _write(ohlcv(0, 100, HOUR), tmp_path / "nq" / "A-all-1h.parquet")
    home = ohlcv(0, 100, HOUR)
    home.loc[:2, "close"] = 1.5  # 3% of the shared bars, under the 5% share
    dst = _write(home, tmp_path / "sp" / "A-all-1h.parquet")
    assert dd.plan_file(src, dst).action == "drop"


def test_fold_backs_up_merges_verifies_and_removes(tmp_path, monkeypatch, ohlcv):
    monkeypatch.setattr(dd, "DATA_ROOT", tmp_path / "data")
    src = _write(ohlcv(0, 4, HOUR), tmp_path / "data" / "nq" / "A" / "A-all-1h.parquet")
    dst = _write(ohlcv(0, 3, HOUR), tmp_path / "data" / "sp" / "A" / "A-all-1h.parquet")
    plan = dd.plan_file(src, dst)
    backup_root = tmp_path / "backup"

    dd.fold_file(plan, backup_root)
    assert src.exists(), "the source stays until remove_source"
    assert (backup_root / "nq" / "A" / "A-all-1h.parquet").exists()
    assert len(pd.read_parquet(dst)) == 4

    dd.remove_source(plan)
    assert not src.exists() and not src.parent.exists()


def test_repoint_edits_the_node_in_place(tmp_path, monkeypatch, ohlcv):
    monkeypatch.setattr(dd, "relative_to_repo", lambda p: f"price-data/{p.parent.name}/{p.name}")
    dst = _write(ohlcv(0, 3, 86_400_000), tmp_path / "sp" / "A-all-1h.parquet")
    catalog = {"A": {"timeframe_dates": {"1h": {"IBKR": {
        "file_path": "price-data/nq/A-all-1h.parquet", "start_date": "x", "end_date": "y",
        "session": "all", "what_to_show": "TRADES"}}}}}

    assert dd.repoint_node(catalog, "A", "1h", "all", dst, dd.read_parquet_range(dst))
    node = catalog["A"]["timeframe_dates"]["1h"]["IBKR"]
    assert node["file_path"] == "price-data/sp/A-all-1h.parquet"
    assert node["what_to_show"] == "TRADES" and node["session"] == "all"
    assert node["start_date"] != "x"
    assert not dd.repoint_node(catalog, "A", "4h", "all", dst, None)
    assert not dd.repoint_node(catalog, "B", "1h", "all", dst, None)


def test_split_name_handles_sessions():
    assert dd._split_name(Path("AAPL-all-1h.parquet"), "AAPL") == ("all", "1h")
    assert dd._split_name(Path("AAPL-overnight-4h.parquet"), "AAPL") == ("overnight", "4h")
    assert dd._split_name(Path("AAPL-1d.parquet"), "AAPL") == (None, "1d")


def _drop_setup(tmp_path, monkeypatch, ohlcv):
    """A conflict copy under nq/ and its home under sp/, wired into dd's module globals."""
    import argparse
    import contextlib

    data = tmp_path / "data"
    monkeypatch.setattr(dd, "DATA_ROOT", data)
    monkeypatch.setattr(dd, "relative_to_repo", lambda p: str(Path(p).relative_to(tmp_path)))
    src = _write(ohlcv(0, 6, HOUR, close=50.0), data / "stocks" / "ibkr" / "nasdaq100" / "A" / "A-all-1h.parquet")
    dst = _write(ohlcv(0, 6, HOUR, close=12.5), data / "stocks" / "ibkr" / "sp500" / "A" / "A-all-1h.parquet")
    catalog = {"A": {"timeframe_dates": {"1h": {"IBKR": {
        "file_path": str(src.relative_to(tmp_path)), "start_date": "x", "end_date": "y", "session": "all",
        "data_flags": [{"kind": "bad", "from": "2020-01-01", "to": "2020-01-02"}]}}}}}
    monkeypatch.setattr(dd, "load_stock_catalog", lambda u: catalog)
    monkeypatch.setattr(dd, "stock_home_universe", lambda s, u, c: "sp500")
    monkeypatch.setattr(dd, "stock_catalog_txn", lambda u: contextlib.nullcontext(catalog))

    def args(**kw):
        base = dict(universe="nasdaq100", symbols=["A"], apply=False, backup_dir=tmp_path / "bk",
                    drop_conflicts=True, run_id="dedupe-drop-test")
        return argparse.Namespace(**{**base, **kw})
    return data, src, dst, catalog, args


def test_drop_conflicts_dry_run_changes_nothing(tmp_path, monkeypatch, ohlcv):
    data, src, dst, catalog, args = _drop_setup(tmp_path, monkeypatch, ohlcv)
    assert dd.run(args()) == 0
    assert src.exists() and "nasdaq100" in catalog["A"]["timeframe_dates"]["1h"]["IBKR"]["file_path"]
    assert not (data / "revisions").exists()


def test_without_drop_conflicts_a_conflict_is_left_alone(tmp_path, monkeypatch, ohlcv):
    data, src, dst, catalog, args = _drop_setup(tmp_path, monkeypatch, ohlcv)
    assert dd.run(args(apply=True, drop_conflicts=False)) == 0
    assert src.exists() and "data_flags" in catalog["A"]["timeframe_dates"]["1h"]["IBKR"]


def test_drop_conflicts_backs_up_records_repoints_and_removes(tmp_path, monkeypatch, ohlcv):
    data, src, dst, catalog, args = _drop_setup(tmp_path, monkeypatch, ohlcv)
    original = pd.read_parquet(src)
    assert dd.run(args(apply=True)) == 0

    assert not src.exists() and not src.parent.exists()
    assert len(pd.read_parquet(dst)) == 6 and pd.read_parquet(dst)["close"].eq(12.5).all(), "home untouched"
    backup = tmp_path / "bk" / "stocks" / "ibkr" / "nasdaq100" / "A" / "A-all-1h.parquet"
    pd.testing.assert_frame_equal(pd.read_parquet(backup), original)
    record = pd.read_parquet(data / "revisions" / "dedupe-drop-test.parquet")
    assert len(record) == 6 and set(record["action"]) == {"dropped_conflict_copy"}
    assert record["close"].eq(50.0).all() and record["row_hash"].str.len().eq(64).all()
    node = catalog["A"]["timeframe_dates"]["1h"]["IBKR"]
    assert node["file_path"] == str(dst.relative_to(tmp_path))
    assert "data_flags" not in node and node["start_date"] != "x"


def test_drop_conflicts_needs_symbols():
    import pytest
    with pytest.raises(SystemExit):
        dd.main(["--drop-conflicts"])
