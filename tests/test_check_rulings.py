"""check_rulings: a deleted window with rows, an empty window with rows, or a lost flag fails."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

import check_rulings  # noqa: E402

DAY = 86_400_000


def write_bars(root: Path, rel: str, days: range) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    n = len(days)
    pd.DataFrame({"timestamp": pd.Series([d * DAY for d in days], dtype="int64"),
                  "open": [1.0] * n, "high": [1.0] * n, "low": [1.0] * n,
                  "close": [1.0] * n, "volume": [1.0] * n}).to_parquet(path, index=False)
    return path


def record(root: Path, name: str, key: str, start_day: int, end_day: int) -> None:
    (root / name).write_text(yaml.safe_dump({"version": 1, "series": {key: [
        {"start_ms": start_day * DAY, "end_ms": end_day * DAY, "start": f"d{start_day}",
         "end": f"d{end_day}"}]}}))


def flag(kind: str, start: str, end: str) -> dict:
    return {"kind": kind, "start": start, "end": end, "reason": "r"}


def catalog(flags: list[dict], splits: list | None = None) -> dict[str, dict]:
    node = {"file_path": "x", "data_flags": flags}
    if splits:
        node["unadjusted_splits"] = splits
    return {"russell2000": {"RCAT": {"timeframe_dates": {"1d": {"ibkr": node}}}}}


def test_rows_back_inside_a_deleted_window_fail(tmp_path):
    key = "ibkr/russell2000/RCAT/RCAT-1d.parquet"
    write_bars(tmp_path, f"stocks/{key}", range(0, 30))
    record(tmp_path, "deleted_bars.yaml", key, 5, 9)
    found, _ = check_rulings.run(tmp_path, tmp_path / "b.json", None, False, catalogs={})
    assert [(f["check"], f["rows"]) for f in found] == [("deleted_bars", 5)]


def test_a_deleted_window_without_rows_is_clean(tmp_path):
    key = "ibkr/russell2000/RCAT/RCAT-1d.parquet"
    write_bars(tmp_path, f"stocks/{key}", [d for d in range(30) if not 5 <= d <= 9])
    record(tmp_path, "deleted_bars.yaml", key, 5, 9)
    assert check_rulings.run(tmp_path, tmp_path / "b.json", None, False, catalogs={})[0] == []


def test_backup_copies_under_hidden_dirs_are_not_the_series(tmp_path):
    key = "ibkr/russell2000/RCAT/RCAT-1d.parquet"
    write_bars(tmp_path, f"stocks/{key}", range(10, 30))
    write_bars(tmp_path, f".deltas/20261103T071501Z/data/stocks/{key}", range(0, 30))
    record(tmp_path, "deleted_bars.yaml", key, 5, 9)
    assert check_rulings.run(tmp_path, tmp_path / "b.json", None, False, catalogs={})[0] == []


def test_rows_inside_an_exchange_empty_window_fail(tmp_path):
    key = "cryptos/kucoin/ABC_USDT/ABC_USDT-1d.parquet"
    write_bars(tmp_path, key, range(0, 30))
    record(tmp_path, "exchange_empty.yaml", key, 10, 10)
    found, _ = check_rulings.run(tmp_path, tmp_path / "b.json", None, False, catalogs={})
    assert [(f["check"], f["rows"]) for f in found] == [("exchange_empty", 1)]


def test_a_recorded_series_without_a_file_is_reported(tmp_path):
    record(tmp_path, "deleted_bars.yaml", "yf/russell2000/GONE/GONE-1d.parquet", 1, 2)
    found, _ = check_rulings.run(tmp_path, tmp_path / "b.json", None, False, catalogs={})
    assert found[0]["problem"] == "file not found"


def test_a_lost_flag_fails_and_keeps_the_baseline_until_accepted(tmp_path):
    base = tmp_path / "b.json"
    ssp = flag("uncertain", "2000-01-03", "2008-07-14")
    found, note = check_rulings.run(tmp_path, base, None, False, catalogs=catalog([ssp], ["2021-05-10"]))
    assert found == [] and "no baseline" in note

    found, _ = check_rulings.run(tmp_path, base, None, False, catalogs=catalog([]))
    assert {f["problem"] for f in found} == {"data_flags lost", "unadjusted_splits lost"}
    found, _ = check_rulings.run(tmp_path, base, None, False, catalogs=catalog([]))
    assert len(found) == 2  # the baseline did not move

    check_rulings.run(tmp_path, base, None, True, catalogs=catalog([]))
    assert check_rulings.run(tmp_path, base, None, False, catalogs=catalog([]))[0] == []


def test_a_clean_run_extends_the_baseline_with_new_flags(tmp_path):
    base = tmp_path / "b.json"
    check_rulings.run(tmp_path, base, None, False, catalogs=catalog([]))
    new = flag("bad", "2024-07-10", "2024-08-19")
    assert check_rulings.run(tmp_path, base, None, False, catalogs=catalog([new]))[0] == []
    found, _ = check_rulings.run(tmp_path, base, None, False, catalogs=catalog([]))
    assert [f["problem"] for f in found] == ["data_flags lost"]


def test_a_bad_flag_replaced_by_a_deleted_flag_is_not_a_loss(tmp_path):
    base = tmp_path / "b.json"
    check_rulings.run(tmp_path, base, None, False,
                      catalogs=catalog([flag("bad", "2000-01-03", "2002-07-02")]))
    after = catalog([flag("deleted", "2000-01-03", "2002-07-02")])
    assert check_rulings.run(tmp_path, base, None, False, catalogs=after)[0] == []


def test_against_a_backup_directory(tmp_path):
    backup = tmp_path / "backup"
    backup.mkdir()
    (backup / "russell2000.yaml").write_text(yaml.safe_dump(catalog([flag("bad", "a", "b")])["russell2000"]))
    found, note = check_rulings.run(tmp_path, tmp_path / "b.json", backup, False, catalogs=catalog([]))
    assert [f["problem"] for f in found] == ["data_flags lost"] and str(backup) in note
    assert not (tmp_path / "b.json").exists()  # a comparison never writes the baseline


def test_main_exit_codes(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(check_rulings, "DATA_ROOT", tmp_path)
    monkeypatch.setattr(check_rulings, "TMP_ROOT", tmp_path / "tmp")
    monkeypatch.setattr(check_rulings, "load_catalogs", lambda against=None: {})
    assert check_rulings.main(["--json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    key = "yf/russell2000/T/T-1d.parquet"
    write_bars(tmp_path, f"stocks/{key}", range(3))
    record(tmp_path, "deleted_bars.yaml", key, 0, 0)
    assert check_rulings.main([]) == 1
