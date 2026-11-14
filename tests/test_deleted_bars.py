"""Deleted stock bars stay deleted.

`delete_stock_bars.py` removes a range the owner ruled impossible (YF TBBK's
flat 16.72 bars). Yahoo still serves those bars, so the deleted-bars record must
keep every writer, the nightly merge and redownload_yf alike, from putting them
back, and the catalog must show the gap as a `deleted` flag.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tools"))

import delete_stock_bars  # noqa: E402
import flag_data_range  # noqa: E402
import utils.catalog as cat  # noqa: E402
from utils import deleted_bars  # noqa: E402

FP = "price-data/stocks/yf/russell2000/TBBK/TBBK-1d.parquet"
DAY = 86_400_000
START = pd.Timestamp("2004-12-01", tz="UTC").value // 1_000_000


def bars(n: int = 60, flat: tuple[int, int] = (15, 32)) -> pd.DataFrame:
    close = 20 + np.arange(n, dtype=float) * 0.1
    volume = np.full(n, 1000.0)
    close[flat[0]:flat[1]], volume[flat[0]:flat[1]] = 16.72, 0.0   # Yahoo's placeholder bars
    return pd.DataFrame({"timestamp": pd.Series([START + i * DAY for i in range(n)], dtype="int64"),
                         "open": close, "high": close, "low": close, "close": close, "volume": volume})


@pytest.fixture
def data(tmp_path, monkeypatch):
    root = tmp_path / "data"
    path = root / FP.split("/", 1)[1]
    path.parent.mkdir(parents=True)
    bars().to_parquet(path, index=False)
    (root / "russell2000.yaml").write_text(yaml.safe_dump({"TBBK": {"name": "TBBK", "timeframe_dates": {"1d": {
        "YF": {"file_path": FP, "adjustment": "raw", "start_date": "2004-12-01", "end_date": "2005-01-29"}}}}}))
    monkeypatch.setattr(cat, "DATA_ROOT", root)
    monkeypatch.setattr(cat, "TMP_ROOT", tmp_path / "tmp")
    return root


def _day(i: int) -> str:
    return str(pd.to_datetime(START + i * DAY, unit="ms").date())


def delete(root: Path, tmp_path: Path, *extra: str) -> int:
    return delete_stock_bars.main(["--range", FP, _day(15), _day(31), "--reason", "flat placeholders",
                                   "--ref", "test", "--catalog", "russell2000", "--run-id", "run1",
                                   "--backup-dir", str(tmp_path / "originals"), *extra])


def test_the_dry_run_changes_nothing(data, tmp_path, capsys):
    before = (data / FP.split("/", 1)[1]).read_bytes()
    assert delete(data, tmp_path) == 0
    assert "17 of 60 rows" in capsys.readouterr().out
    assert (data / FP.split("/", 1)[1]).read_bytes() == before
    assert not (data / deleted_bars.RECORD_NAME).exists()


def test_deleted_bars_stay_deleted(data, tmp_path):
    path = data / FP.split("/", 1)[1]
    assert delete(data, tmp_path, "--apply") == 0
    stored = pd.read_parquet(path)
    assert len(stored) == 43 and (stored["volume"] > 0).all()

    # the record, the revision record, the original and the catalog flag
    (window,) = yaml.safe_load((data / "deleted_bars.yaml").read_text())["series"][
        "yf/russell2000/TBBK/TBBK-1d.parquet"]
    assert (window["start"], window["end"], window["rows"], window["run_id"]) == (_day(15), _day(31), 17, "run1")
    assert len(pd.read_parquet(data / "revisions" / "run1.parquet")) == 17
    assert len(pd.read_parquet(tmp_path / "originals" / FP.split("/", 1)[1])) == 60
    node = yaml.safe_load((data / "russell2000.yaml").read_text())["TBBK"]["timeframe_dates"]["1d"]["YF"]
    (flag,) = node["data_flags"]
    assert (flag["kind"], flag["from"], flag["to"], flag["run_id"]) == ("deleted", _day(15), _day(31), "run1")

    # Yahoo serves the full history again: the nightly merge keeps the gap ...
    cat.merge_ohlcv_parquet(bars(), path)
    assert len(pd.read_parquet(path)) == 43
    # ... and so does a whole-file replacement (redownload_yf filters the same way)
    assert len(deleted_bars.drop_deleted(bars(), path)) == 43
    # bars outside the window still merge
    cat.merge_ohlcv_parquet(bars(70), path)
    assert len(pd.read_parquet(path)) == 53


def test_a_catalog_backup_failure_deletes_nothing(data, tmp_path):
    (tmp_path / "file").write_text("")
    before = (data / FP.split("/", 1)[1]).read_bytes()
    assert delete(data, tmp_path, "--apply", "--catalog-backup-dir", str(tmp_path / "file" / "b")) != 0
    assert (data / FP.split("/", 1)[1]).read_bytes() == before
    assert not (data / deleted_bars.RECORD_NAME).exists()


def test_flags_are_set_and_cleared(data, tmp_path):
    args = ["--file", FP, "--kind", "break", "--date", "2005-01-03", "--reason", "jump",
            "--catalog", "russell2000", "--catalog-backup-dir", str(tmp_path / "b")]
    node = lambda: yaml.safe_load((data / "russell2000.yaml").read_text())["TBBK"]["timeframe_dates"]["1d"]["YF"]  # noqa: E731
    assert flag_data_range.main([*args, "--apply"]) == 0
    assert [(f["kind"], f["date"]) for f in node()["data_flags"]] == [("break", "2005-01-03")]
    assert flag_data_range.main([*args, "--apply"]) == 0                        # idempotent
    assert len(node()["data_flags"]) == 1
    assert flag_data_range.main([*args, "--clear", "--apply"]) == 0
    assert "data_flags" not in node()


def test_a_range_flag_needs_both_dates(data):
    with pytest.raises(SystemExit):
        flag_data_range.main(["--file", FP, "--kind", "bad", "--from", "2005-01-03", "--reason", "x"])


def test_the_end_date_includes_its_intraday_bars():
    cut = delete_stock_bars.Cut("price-data/stocks/ibkr/x/X/X-all-4h.parquet", "2026-10-12", "2026-10-23", "r")
    last_4h = int(pd.Timestamp("2026-10-23 20:00", tz="UTC").value // 1_000_000)
    next_day = int(pd.Timestamp("2026-10-24", tz="UTC").value // 1_000_000)
    frame = pd.DataFrame({"timestamp": [last_4h, next_day]})
    assert list(delete_stock_bars.rows_in(frame, cut)) == [True, False]
