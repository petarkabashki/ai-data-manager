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
    src = _write(ohlcv(0, 4, HOUR, close=1.0), tmp_path / "nq" / "A-all-1h.parquet")
    home = ohlcv(0, 3, HOUR, close=1.0)
    home.loc[1, "close"] = 9.0
    dst = _write(home, tmp_path / "sp" / "A-all-1h.parquet")
    plan = dd.plan_file(src, dst)
    assert plan.action == "merge"
    assert plan.only_rows["timestamp"].tolist() == [3 * HOUR]
    assert plan.differing == 1


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

    assert dd.repoint_node(catalog, "A", "1h", "all", dst)
    node = catalog["A"]["timeframe_dates"]["1h"]["IBKR"]
    assert node["file_path"] == "price-data/sp/A-all-1h.parquet"
    assert node["what_to_show"] == "TRADES" and node["session"] == "all"
    assert node["start_date"] != "x"
    assert not dd.repoint_node(catalog, "A", "4h", "all", dst)
    assert not dd.repoint_node(catalog, "B", "1h", "all", dst)


def test_split_name_handles_sessions():
    assert dd._split_name(Path("AAPL-all-1h.parquet"), "AAPL") == ("all", "1h")
    assert dd._split_name(Path("AAPL-overnight-4h.parquet"), "AAPL") == ("overnight", "4h")
    assert dd._split_name(Path("AAPL-1d.parquet"), "AAPL") == (None, "1d")
