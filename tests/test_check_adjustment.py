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

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from check_adjustment import audit, counts  # noqa: E402

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
