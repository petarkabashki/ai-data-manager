"""record_known_adjustment writes what was measured, on every YF node of the file."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from record_known_adjustment import apply_records, records  # noqa: E402

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
    assert wanted == {VZ: {"verdict": "anomaly", "drift": -0.0033, "reason": "Frontier spin-off",
                           "recorded": "2026-11-13"}}
    assert skipped == ["price-data/stocks/yf/sp500/KO/KO-1d.parquet (ok)"]


def test_records_go_on_yf_nodes_only_and_clear_cleanly():
    ibkr = {"file_path": VZ}                     # same path on an IBKR node is not touched
    catalog = {"VZ": {"timeframe_dates": {"1d": {"YF": {"file_path": VZ}, "IBKR": ibkr}}}}
    record = {"verdict": "anomaly", "drift": -0.0033, "reason": "x", "recorded": "2026-11-13"}
    assert len(apply_records(catalog, {VZ: record})) == 1
    assert catalog["VZ"]["timeframe_dates"]["1d"]["YF"]["known_adjustment"] == record
    assert "known_adjustment" not in ibkr
    assert apply_records(catalog, {VZ: record}) == []          # idempotent
    apply_records(catalog, {VZ: None})
    assert "known_adjustment" not in catalog["VZ"]["timeframe_dates"]["1d"]["YF"]
