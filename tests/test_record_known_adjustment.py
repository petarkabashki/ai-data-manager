"""record_known_adjustment writes what was measured, on every YF node of the file."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from record_known_adjustment import LABEL_KEYS, SPLIT_KEYS, apply_records, records, split_records  # noqa: E402

VZ = "price-data/stocks/yf/sp500/VZ/VZ-1d.parquet"
REPORT = {"files": [
    {"provider": "YF", "symbol": "VZ", "file_path": VZ, "verdict": "anomaly",
     "vs_ibkr": {"drift": -0.0033, "level": 1.0, "last": "2026-11-12"}, "vs_yf": []},
    {"provider": "YF", "symbol": "KO", "file_path": "price-data/stocks/yf/sp500/KO/KO-1d.parquet",
     "verdict": "ok", "vs_ibkr": {"drift": 0.0, "level": 1.0, "last": "2026-11-12"}, "vs_yf": []},
    {"provider": "IBKR", "symbol": "VZ", "file_path": "price-data/stocks/ibkr/sp500/VZ/VZ-all-1d.parquet",
     "verdict": "no_jump_found"},
]}


def test_only_findings_are_recorded_with_their_number():
    wanted, skipped = records(REPORT, {"VZ", "KO"}, set(), "Frontier spin-off", None, "2026-11-13")
    assert {k: v for k, v in wanted[VZ].items() if v is not None} == {
        "verdict": "anomaly", "drift": -0.0033, "reason": "Frontier spin-off", "recorded": "2026-11-13"}
    assert set(wanted) == {VZ}
    assert skipped == ["price-data/stocks/yf/sp500/KO/KO-1d.parquet (ok)"]


def test_records_go_on_yf_nodes_only_and_clear_cleanly():
    ibkr = {"file_path": VZ}                     # same path on an IBKR node is not touched
    catalog = {"VZ": {"timeframe_dates": {"1d": {"YF": {"file_path": VZ}, "IBKR": ibkr}}}}
    record = {"verdict": "anomaly", "drift": -0.0033, "reason": "x", "recorded": "2026-11-13"}
    assert len(apply_records(catalog, {VZ: record})) == 1
    assert catalog["VZ"]["timeframe_dates"]["1d"]["YF"]["known_adjustment"] == record
    assert "known_adjustment" not in ibkr
    assert apply_records(catalog, {VZ: record}) == []          # idempotent
    apply_records(catalog, {VZ: dict.fromkeys(LABEL_KEYS)})
    assert "known_adjustment" not in catalog["VZ"]["timeframe_dates"]["1d"]["YF"]


def test_label_and_split_parts_are_kept_and_cleared_apart():
    report = {"files": [{**REPORT["files"][0], "split": "split_jump",
                         "split_jumps": [{"date": "2010-07-01"}]}]}
    node = {"file_path": VZ, "known_adjustment": {"verdict": "level_mismatch", "level": 1.1, "reason": "old",
                                                  "recorded": "2026-11-01", "until_ibkr": "2026-10-01"}}
    catalog = {"VZ": {"timeframe_dates": {"1d": {"YF": node}}}}
    splits, skipped = split_records(report, {"VZ"}, set(), "Frontier spin-off ex-date", "2026-11-13")
    assert splits == {VZ: {"split_dates": ["2010-07-01"], "split_reason": "Frontier spin-off ex-date",
                           "split_recorded": "2026-11-13"}} and skipped == []
    apply_records(catalog, splits)
    assert node["known_adjustment"]["level"] == 1.1                       # label part kept
    label, _ = records(report, {"VZ"}, set(), "re-measured", None, "2026-11-13")
    apply_records(catalog, label)
    assert node["known_adjustment"] == {"verdict": "anomaly", "drift": -0.0033, "reason": "re-measured",
                                        "recorded": "2026-11-13", **splits[VZ]}  # stale level, until gone
    apply_records(catalog, {VZ: dict.fromkeys(SPLIT_KEYS)})
    assert "split_dates" not in node["known_adjustment"]
    apply_records(catalog, {VZ: dict.fromkeys(LABEL_KEYS)})
    assert "known_adjustment" not in node


def test_a_row_without_a_jump_has_no_split_to_record():
    _, skipped = split_records(REPORT, {"VZ"}, set(), "x", "2026-11-13")
    assert skipped == [f"{VZ} (None)"]
