"""The adjustment audit must catch the two faults measured on the stored tree.

- `yf/sp500/AAPL` is labelled `raw` but holds dividend-adjusted closes: against
  IBKR TRADES its ratio climbs toward 1 over the years.
- `ibkr/sp500/XOM` never back-adjusted the 2001 2:1 split, so it is 2x YF before
  2001-07-19 and falls 50% in one day.

A real crash, which both providers record, must not look like either.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from check_adjustment import apply_pending, audit, counts, failing  # noqa: E402

DAYS = 300
SPLIT_DAY = 150


def _prices(seed: int = 1) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return 50 * np.exp(np.cumsum(rng.normal(0, 0.01, DAYS)))


def _write(root: Path, file_path: str, closes: np.ndarray) -> None:
    path = root / file_path.split("/", 1)[1]
    path.parent.mkdir(parents=True, exist_ok=True)
    day = 86_400_000
    pd.DataFrame({
        "timestamp": pd.Series([(10_000 + i) * day for i in range(len(closes))], dtype="int64"),
        "open": closes, "high": closes, "low": closes, "close": closes,
        "volume": np.ones(len(closes)),
    }).to_parquet(path, index=False)


def _node(file_path: str, adjustment: str) -> dict:
    return {"file_path": file_path, "adjustment": adjustment, "schema": "ohlcv"}


def _entry(**providers: dict) -> dict:
    return {"timeframe_dates": {"1d": providers}}


def _by_file(rows: list[dict]) -> dict[str, dict]:
    return {r["file_path"]: r for r in rows}


def test_a_dividend_adjusted_file_labelled_raw_is_mislabelled(tmp_path):
    p = _prices()
    # Back-adjusted: 4% below the raw price at the start, converging to it.
    factor = np.linspace(0.96, 1.0, DAYS)
    files = {
        "price-data/stocks/yf/sp500/AAPL/AAPL-1d.parquet": p * factor,
        "price-data/stocks/yf/nasdaq100/AAPL/AAPL-1d.parquet": p * 1.0002,
        "price-data/stocks/ibkr/sp500/AAPL/AAPL-all-1d.parquet": p,
    }
    for fp, closes in files.items():
        _write(tmp_path, fp, closes)
    ibkr = _node("price-data/stocks/ibkr/sp500/AAPL/AAPL-all-1d.parquet", "raw")
    catalogs = {
        "sp500": {"AAPL": _entry(YF=_node("price-data/stocks/yf/sp500/AAPL/AAPL-1d.parquet", "raw"), IBKR=ibkr)},
        "nasdaq100": {"AAPL": _entry(YF=_node("price-data/stocks/yf/nasdaq100/AAPL/AAPL-1d.parquet", "raw"), IBKR=ibkr)},
    }
    rows = _by_file(audit(catalogs, tmp_path))

    assert rows["price-data/stocks/yf/sp500/AAPL/AAPL-1d.parquet"]["verdict"] == "mislabelled_raw_is_adjusted"
    assert rows["price-data/stocks/yf/nasdaq100/AAPL/AAPL-1d.parquet"]["verdict"] == "ok"
    ib = rows["price-data/stocks/ibkr/sp500/AAPL/AAPL-all-1d.parquet"]
    assert ib["verdict"] == "no_jump_found" and ib["catalog"] == "sp500"
    assert len(rows) == 3                      # the shared IBKR file is listed once


@pytest.mark.parametrize("dividends, verdict", [(False, "ok"), (True, "mislabelled_raw_is_adjusted")])
def test_a_spin_off_step_is_not_dividend_drift(tmp_path, dividends, verdict):
    """Yahoo scales history before a spin-off (A 2014-11-03: 0.72x); IBKR doesn't."""
    p = _prices(3)
    yf_closes = p.copy()
    yf_closes[:SPLIT_DAY] *= 0.85
    if dividends:
        yf_closes *= np.linspace(0.96, 1.0, DAYS)
    yf, ib = ("price-data/stocks/yf/sp500/A/A-1d.parquet", "price-data/stocks/ibkr/sp500/A/A-all-1d.parquet")
    _write(tmp_path, yf, yf_closes)
    _write(tmp_path, ib, p)
    catalogs = {"sp500": {"A": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    row = _by_file(audit(catalogs, tmp_path))[yf]

    assert row["verdict"] == verdict
    (step,) = row["vs_ibkr"]["one_day_steps"]
    assert step["date"] == str(pd.Timestamp((10_000 + SPLIT_DAY) * 86_400_000, unit="ms").date())
    assert row["vs_ibkr"]["step_drift"] == pytest.approx(1 / 0.85 - 1, abs=0.01)


def test_a_step_that_drifts_back_is_not_taken_out_of_drift(tmp_path):
    """ABBV 2024-08-07: the ratio fell 5.7% in a day and recovered; drift is 0."""
    p = _prices(6)
    yf_closes = p.copy()
    back = np.linspace(0.943, 1.0, 10)
    yf_closes[SPLIT_DAY:SPLIT_DAY + 10] *= back
    yf, ib = ("price-data/stocks/yf/sp500/ABBV/ABBV-1d.parquet", "price-data/stocks/ibkr/sp500/ABBV/ABBV-all-1d.parquet")
    _write(tmp_path, yf, yf_closes)
    _write(tmp_path, ib, p)
    catalogs = {"sp500": {"ABBV": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    row = _by_file(audit(catalogs, tmp_path))[yf]

    assert row["verdict"] == "ok"
    assert row["vs_ibkr"]["one_day_steps"] == [] and abs(row["vs_ibkr"]["drift"]) < 0.005


def test_a_noisy_ratio_keeps_its_steps_in_drift(tmp_path):
    """WBD vs IBKR 2021: many held 10-20% moves; removing them inverted the drift."""
    p = _prices(7)
    yf_closes = p * np.linspace(0.96, 1.0, DAYS)
    for i, day in enumerate(range(40, 220, 30)):
        yf_closes[day:] *= 1.1 if i % 2 == 0 else 1 / 1.1
    yf, ib = ("price-data/stocks/yf/sp500/WBD/WBD-1d.parquet", "price-data/stocks/ibkr/sp500/WBD/WBD-all-1d.parquet")
    _write(tmp_path, yf, yf_closes)
    _write(tmp_path, ib, p)
    catalogs = {"sp500": {"WBD": _entry(YF=_node(yf, "adjusted"), IBKR=_node(ib, "raw"))}}
    row = _by_file(audit(catalogs, tmp_path))[yf]

    assert row["vs_ibkr"]["noisy_ratio"] and row["vs_ibkr"]["step_drift"] == 0
    assert row["verdict"] == "ok"


def test_yearly_stock_dividends_are_share_steps_not_noise(tmp_path):
    """GABC: 5% in shares each year, applied by Yahoo, not by IBKR before 2005."""
    p = _prices(8)
    yf_closes = p.copy()
    for day in range(40, 260, 45):             # five 5% stock dividends
        yf_closes[:day] /= 1.05
    yf, ib = ("price-data/stocks/yf/russell2000/GABC/GABC-1d.parquet", "price-data/stocks/ibkr/russell2000/GABC/GABC-all-1d.parquet")
    _write(tmp_path, yf, yf_closes)
    _write(tmp_path, ib, p)
    catalogs = {"russell2000": {"GABC": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    row = _by_file(audit(catalogs, tmp_path))[yf]

    assert row["verdict"] == "ok"
    assert [s["stock_dividend"] for s in row["vs_ibkr"]["share_steps"]] == ["5%"] * 5
    assert not row["vs_ibkr"]["noisy_ratio"]


def test_an_unadjusted_split_is_owned_by_the_file_that_jumps(tmp_path):
    p = _prices(2)
    ibkr_closes = p.copy()
    ibkr_closes[:SPLIT_DAY] *= 2               # IBKR kept the pre-split share count
    yf, ib = ("price-data/stocks/yf/sp500/XOM/XOM-1d.parquet",
              "price-data/stocks/ibkr/sp500/XOM/XOM-all-1d.parquet")
    _write(tmp_path, yf, p)
    _write(tmp_path, ib, ibkr_closes)
    catalogs = {"sp500": {"XOM": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    rows = _by_file(audit(catalogs, tmp_path))

    assert rows[yf]["verdict"] == "ok" and rows[yf]["split"] == "no_jump_found"
    assert rows[ib]["verdict"] == "split_jump"
    (jump,) = rows[ib]["split_jumps"]
    assert jump["factor"] == "1:2"
    assert jump["date"] == str(pd.Timestamp((10_000 + SPLIT_DAY) * 86_400_000, unit="ms").date())
    assert counts(list(rows.values()))["sp500"]["IBKR"] == {"split_jump": 1}

    # Once the date is on the node's `unadjusted_splits`, it is no longer a finding.
    catalogs["sp500"]["XOM"]["timeframe_dates"]["1d"]["IBKR"]["unadjusted_splits"] = [jump["date"]]
    flagged = _by_file(audit(catalogs, tmp_path))[ib]
    assert flagged["verdict"] == flagged["split"] == "flagged_unadjusted"
    assert flagged["split_jumps"] == [jump]


def test_a_crash_both_providers_record_is_clean(tmp_path):
    p = _prices(3)
    p[SPLIT_DAY:] *= 0.5                       # a real one-day -50%, in both files
    yf, ib = ("price-data/stocks/yf/sp500/KO/KO-1d.parquet",
              "price-data/stocks/ibkr/sp500/KO/KO-all-1d.parquet")
    _write(tmp_path, yf, p * 1.0005)
    _write(tmp_path, ib, p)
    catalogs = {"sp500": {"KO": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    rows = _by_file(audit(catalogs, tmp_path))

    assert rows[yf]["verdict"] == "ok" and rows[yf]["split"] == "no_jump_found"
    assert rows[ib]["verdict"] == "no_jump_found"


def test_no_overlap_is_unverifiable_not_passed(tmp_path):
    yf = "price-data/stocks/yf/delisted_stocks/OLD/OLD-1d.parquet"
    _write(tmp_path, yf, _prices(4))
    catalogs = {"delisted_stocks": {"OLD": _entry(YF=_node(yf, "raw"))}}
    (row,) = audit(catalogs, tmp_path)

    assert row["verdict"] == "unverifiable_no_overlap"
    assert row["split"] == "unverifiable_no_overlap"


def test_a_one_day_spike_is_a_bad_bar_not_a_split(tmp_path):
    p = _prices(5)
    spiked = p.copy()
    spiked[SPLIT_DAY] *= 25                    # one bad print, back the next day
    yf, ib = ("price-data/stocks/yf/sp500/AEYE/AEYE-1d.parquet",
              "price-data/stocks/ibkr/sp500/AEYE/AEYE-all-1d.parquet")
    _write(tmp_path, yf, p)
    _write(tmp_path, ib, spiked)
    catalogs = {"sp500": {"AEYE": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    rows = _by_file(audit(catalogs, tmp_path))

    assert rows[ib]["verdict"] == "no_jump_found"
    assert rows[yf]["verdict"] == "ok" and len(rows[yf]["bad_bar_dates"]) == 2


def test_a_recorded_finding_is_known_until_it_moves_or_lapses(tmp_path):
    """CTVA: YF carries the 2026-10-01 split, IBKR's file stops before it."""
    p = _prices(9)
    yf, ib = ("price-data/stocks/yf/sp500/CTVA/CTVA-1d.parquet", "price-data/stocks/ibkr/sp500/CTVA/CTVA-all-1d.parquet")
    _write(tmp_path, yf, p * 0.15)
    _write(tmp_path, ib, p)
    last = str(pd.Timestamp((10_000 + DAYS - 1) * 86_400_000, unit="ms").date())
    yf_node = _node(yf, "raw")
    catalogs = {"sp500": {"CTVA": _entry(YF=yf_node, IBKR=_node(ib, "raw"))}}
    assert _by_file(audit(catalogs, tmp_path))[yf]["verdict"] == "level_mismatch"

    yf_node["known_adjustment"] = {"verdict": "level_mismatch", "level": 0.15, "until_ibkr": "2099-01-01"}
    row = _by_file(audit(catalogs, tmp_path))[yf]
    assert row["verdict"] == "known" and row["known"]["measured_verdict"] == "level_mismatch"

    yf_node["known_adjustment"]["level"] = 0.30              # the file moved: a new finding
    assert _by_file(audit(catalogs, tmp_path))[yf]["verdict"] == "level_mismatch"

    yf_node["known_adjustment"].update(level=0.15, until_ibkr=last)  # IBKR reached the date
    assert _by_file(audit(catalogs, tmp_path))[yf]["verdict"] == "level_mismatch"


def test_a_noisy_ratio_takes_no_share_steps_out_either(tmp_path):
    """OKE vs IBKR 2020: held moves of all sizes; some land near 4% or 5% by chance."""
    p = _prices(10)
    yf_closes = p.copy()
    for i, day in enumerate(range(30, 160, 30)):     # five 12% moves: a noisy ratio
        yf_closes[day:] *= 1.12 if i % 2 == 0 else 1 / 1.12
    for day in (200, 250):                             # two that happen to be 5%
        yf_closes[day:] *= 1.05
    yf, ib = ("price-data/stocks/yf/sp500/OKE/OKE-1d.parquet", "price-data/stocks/ibkr/sp500/OKE/OKE-all-1d.parquet")
    _write(tmp_path, yf, yf_closes)
    _write(tmp_path, ib, p)
    catalogs = {"sp500": {"OKE": _entry(YF=_node(yf, "raw"), IBKR=_node(ib, "raw"))}}
    row = _by_file(audit(catalogs, tmp_path))[yf]

    assert row["vs_ibkr"]["noisy_ratio"] and row["vs_ibkr"]["share_steps"]
    assert row["vs_ibkr"]["step_drift"] == 0


def _yf_jump(tmp_path: Path) -> tuple[dict, str, str, dict]:
    """MO 2007-04-02 shape: a one-day jump on the YF file (Yahoo's Kraft spin-off factor)."""
    p = _prices(11)
    yf_closes = p.copy()
    yf_closes[:SPLIT_DAY] *= 2                 # YF kept the pre-split level
    yf, ib = ("price-data/stocks/yf/sp500/MO/MO-1d.parquet", "price-data/stocks/ibkr/sp500/MO/MO-all-1d.parquet")
    _write(tmp_path, yf, yf_closes)
    _write(tmp_path, ib, p)
    yf_node = _node(yf, "raw")
    return {"sp500": {"MO": _entry(YF=yf_node, IBKR=_node(ib, "raw"))}}, yf, ib, yf_node


def test_a_yf_split_jump_on_record_reads_known(tmp_path):
    catalogs, yf, _ib, yf_node = _yf_jump(tmp_path)
    row = _by_file(audit(catalogs, tmp_path))[yf]
    assert row["split"] == "split_jump" and failing(row)
    (date,) = row["open_split_dates"]

    yf_node["known_adjustment"] = {"split_dates": [date], "split_reason": "Kraft spin-off"}
    row = _by_file(audit(catalogs, tmp_path))[yf]
    assert row["split"] == "known_split_jump" and row["open_split_dates"] == [] and not failing(row)

    yf_node["known_adjustment"]["split_dates"] = ["1999-01-01"]       # another date: still open
    assert _by_file(audit(catalogs, tmp_path))[yf]["split"] == "split_jump"


def test_pending_findings_pass_until_their_review_date(tmp_path):
    catalogs, yf, _ib, _ = _yf_jump(tmp_path)
    rows = audit(catalogs, tmp_path)
    date = _by_file(rows)[yf]["open_split_dates"][0]
    entry = {"file_path": yf, "split_dates": [date], "reason": "repair candidate", "review_by": "2026-12-01"}

    apply_pending(rows, {yf: entry}, "2026-11-13")
    row = _by_file(rows)[yf]
    assert row["split"] == "pending_split_jump" and row["pending"]["reason"] == "repair candidate"
    assert not any(failing(r) for r in rows)

    late = audit(catalogs, tmp_path)
    apply_pending(late, {yf: entry}, "2026-12-02")                   # undecided past review_by: fails
    assert failing(_by_file(late)[yf])


def test_a_pending_label_finding_must_measure_the_same():
    row = {"file_path": "f", "verdict": "anomaly", "split": "no_jump_found",
           "vs_ibkr": {"drift": -0.05, "level": 1.0, "last": "2026-11-12"}}
    apply_pending([row], {"f": {"file_path": "f", "verdict": "anomaly", "drift": -0.20}}, "2026-11-13")
    assert row["verdict"] == "anomaly"                                 # a different number: still open
    apply_pending([row], {"f": {"file_path": "f", "verdict": "anomaly", "drift": -0.05}}, "2026-11-13")
    assert row["verdict"] == "pending" and not failing(row)
