"""A split seam must be distinguishable from a market crash.

The `stocks/yf` tree holds files that were appended to across a corporate
action: history written before the split keeps the old share count, bars added
after it use the new one, and the join is a step that never traded. Measured on
the real tree, `nasdaq100/ORLY` sits at exactly 15x the provider's price before
2024-08-07 and 1.0x after. These tests pin the part that makes the check usable
-- telling that seam apart from March 2020, when everything really did fall by
half.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from check_splits import check_file, nearest_simple_ratio  # noqa: E402

TOL = 0.003


def _write(path: Path, closes: list[float]) -> Path:
    day = 86_400_000
    pd.DataFrame(
        {
            "timestamp": pd.Series([i * day for i in range(len(closes))], dtype="int64"),
            "open": closes, "high": closes, "low": closes,
            "close": closes, "volume": [1.0] * len(closes),
        }
    ).to_parquet(path, index=False)
    return path


def test_a_clean_split_factor_is_recognised():
    assert str(nearest_simple_ratio(10.0, TOL)) == "10"
    assert str(nearest_simple_ratio(1 / 15, TOL)) == "1/15"
    assert str(nearest_simple_ratio(1.5, TOL)) == "3/2"


def test_a_crash_is_not_a_split_factor():
    """The bug this guards: with denominators to 30, 0.58 fits 5/13 to 0.3%.

    Requiring the smaller term to be tiny is what keeps March 2020 out.
    """
    assert nearest_simple_ratio(0.5308, TOL) is None
    assert nearest_simple_ratio(0.5921, TOL) is None
    assert nearest_simple_ratio(1.7618, TOL) is None


def test_a_split_seam_is_reported_with_its_factor(tmp_path):
    # Ten bars at the old share count, then ten at the new one.
    found = check_file(_write(tmp_path / "orly.parquet",
                              [150.0] * 10 + [10.0] * 10), TOL)
    assert len(found) == 1
    assert found[0]["factor"] == "1:15"
    assert found[0]["before"] == 150.0 and found[0]["after"] == 10.0


def test_an_ordinary_series_is_quiet(tmp_path):
    closes = [100.0 * (1.01 ** i) for i in range(60)]
    assert check_file(_write(tmp_path / "calm.parquet", closes), TOL) == []


def test_a_large_move_is_reported_even_without_a_clean_factor(tmp_path):
    """Filtering on the factor hid the real finding.

    SMCI's seam is 12.52x -- not a clean split, but not a day's trading either.
    The factor labels a move; it must not decide whether to report it.
    """
    found = check_file(_write(tmp_path / "smci.parquet",
                              [616.94] * 5 + [49.27] * 5), TOL)
    assert len(found) == 1
    assert found[0]["factor"] == ""
    assert found[0]["ratio"] < 0.1


def test_an_unreadable_file_is_reported_not_raised(tmp_path):
    broken = tmp_path / "broken.parquet"
    broken.write_bytes(b"not a parquet file")
    found = check_file(broken, TOL)
    assert len(found) == 1 and "unreadable" in found[0]["error"]


def test_non_positive_prices_do_not_crash_the_scan(tmp_path):
    """`russell2000/VHI` really does hold negative closes (min -595.88)."""
    found = check_file(_write(tmp_path / "vhi.parquet",
                              [10.0, -595.88, 0.0, 10.0, 10.0]), TOL)
    assert all("error" not in f for f in found)
