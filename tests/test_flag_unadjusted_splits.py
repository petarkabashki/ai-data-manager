"""IBKR split jumps from the adjustment audit land on every node of the file."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from flag_unadjusted_splits import apply_flags, splits_by_file  # noqa: E402

XOM = "price-data/stocks/ibkr/sp500/XOM/XOM-all-1d.parquet"
KO = "price-data/stocks/ibkr/sp500/KO/KO-all-1d.parquet"


def _catalog():
    return {
        "XOM": {"timeframe_dates": {"1d": {
            "YF": {"provider": "YF", "file_path": "price-data/stocks/yf/sp500/XOM/XOM-1d.parquet"},
            "IBKR": {"provider": "IBKR", "file_path": XOM, "adjustment": "raw"},
        }}},
        "KO": {"timeframe_dates": {"1d": {
            "IBKR": {"provider": "IBKR", "file_path": KO, "unadjusted_splits": ["2012-08-13"]},
        }}},
        "NEW": {"timeframe_dates": {"1d": {"IBKR": {"provider": "IBKR", "file_path": "elsewhere"}}}},
    }


def test_jumps_are_set_and_stale_flags_cleared():
    report = {"files": [
        {"provider": "IBKR", "file_path": XOM, "split": "split_jump",
         "split_jumps": [{"date": "2001-07-19"}, {"date": "2001-07-19"}]},
        {"provider": "IBKR", "file_path": KO, "split": "no_jump_found", "split_jumps": []},
        {"provider": "IBKR", "file_path": "elsewhere", "split": "unverifiable_no_overlap", "split_jumps": []},
        {"provider": "YF", "file_path": "y", "split": "split_jump", "split_jumps": [{"date": "2026-05-04"}]},
    ]}
    flags = splits_by_file(report)
    assert flags == {XOM: ["2001-07-19"], KO: []}

    catalog = _catalog()
    changes = apply_flags(catalog, flags)
    nodes = {k: v["timeframe_dates"]["1d"] for k, v in catalog.items()}
    assert nodes["XOM"]["IBKR"]["unadjusted_splits"] == ["2001-07-19"]
    assert "unadjusted_splits" not in nodes["XOM"]["YF"]
    assert "unadjusted_splits" not in nodes["KO"]["IBKR"]
    assert len(changes) == 2
    assert apply_flags(catalog, flags) == []          # idempotent
