"""normalize_ohlcv() — the single point every byte of price data passes through."""

from __future__ import annotations

import pandas as pd
import pytest

from utils.catalog import CANONICAL_COLS, empty_ohlcv_frame, normalize_ohlcv

MS_2021_01_01 = 1_609_459_200_000


def assert_canonical(df: pd.DataFrame) -> None:
    assert list(df.columns) == CANONICAL_COLS
    assert df["timestamp"].dtype == "int64"
    for col in CANONICAL_COLS[1:]:
        assert df[col].dtype == "float64", col


def test_empty_frame_is_canonical():
    result = normalize_ohlcv(pd.DataFrame())
    assert_canonical(result)
    assert result.empty


def test_zero_column_frame_is_canonical():
    assert_canonical(normalize_ohlcv(pd.DataFrame(index=[0, 1])))


def test_empty_ohlcv_frame_has_dtypes():
    # The hand-written `pd.DataFrame(columns=CANONICAL_COLS)` this replaces
    # produced object-typed columns.
    assert_canonical(empty_ohlcv_frame())


def test_freqtrade_numeric_columns():
    raw = pd.DataFrame(
        {
            "0": [float(MS_2021_01_01), float(MS_2021_01_01 + 86_400_000)],
            "1": [1.0, 2.0],
            "2": [3.0, 4.0],
            "3": [0.5, 0.6],
            "4": [1.5, 2.5],
            "5": [10.0, 20.0],
        }
    )
    result = normalize_ohlcv(raw)
    assert_canonical(result)
    assert result["timestamp"].tolist() == [MS_2021_01_01, MS_2021_01_01 + 86_400_000]
    assert result["close"].tolist() == [1.5, 2.5]


def test_numeric_columns_drop_null_timestamps():
    raw = pd.DataFrame(
        {
            "0": [float(MS_2021_01_01), None],
            "1": [1.0, 1.0], "2": [1.0, 1.0], "3": [1.0, 1.0],
            "4": [1.0, 1.0], "5": [1.0, 1.0],
            "date": ["a", "b"],  # extra column from a bad merge
        }
    )
    assert len(normalize_ohlcv(raw)) == 1


def test_canonical_passthrough_drops_extra_columns():
    raw = pd.DataFrame(
        {
            "timestamp": [MS_2021_01_01],
            "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [9.0],
            "date": pd.to_datetime(["2021-01-01"]),
        }
    )
    assert_canonical(normalize_ohlcv(raw))
    assert "date" not in normalize_ohlcv(raw).columns


def test_capitalized_timestamp_variant():
    raw = pd.DataFrame(
        {
            "Timestamp": [MS_2021_01_01],
            "Open": [1.0], "High": [2.0], "Low": [0.5], "Close": [1.5], "Volume": [9.0],
        }
    )
    result = normalize_ohlcv(raw)
    assert_canonical(result)
    assert result["timestamp"].iloc[0] == MS_2021_01_01


def test_freqtrade_date_column():
    raw = pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-01-01"], utc=True),
            "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [9.0],
        }
    )
    assert normalize_ohlcv(raw)["timestamp"].iloc[0] == MS_2021_01_01


def test_yahoo_finance_schema_drops_adj_close():
    raw = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2021-01-01"]),
            "Open": [1.0], "High": [2.0], "Low": [0.5], "Close": [1.5],
            "Adj Close": [1.4], "Volume": [9.0],
        }
    )
    result = normalize_ohlcv(raw)
    assert_canonical(result)
    assert result["close"].iloc[0] == 1.5  # Close, not Adj Close — see ROADMAP E6


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_datetime_resolutions_are_not_corrupted(unit):
    """A naive //1_000_000 divide silently corrupts non-nanosecond inputs."""
    ts = pd.Series(pd.to_datetime(["2021-01-01"])).astype(f"datetime64[{unit}]")
    raw = pd.DataFrame(
        {"timestamp": ts, "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [9.0]}
    )
    assert normalize_ohlcv(raw)["timestamp"].iloc[0] == MS_2021_01_01


def test_tz_aware_timestamps_convert_to_utc():
    ts = pd.Series(pd.to_datetime(["2021-01-01 00:00:00"])).dt.tz_localize("America/New_York")
    raw = pd.DataFrame(
        {"timestamp": ts, "open": [1.0], "high": [2.0], "low": [0.5], "close": [1.5], "volume": [9.0]}
    )
    # 2021-01-01 00:00 New York is 05:00 UTC.
    assert normalize_ohlcv(raw)["timestamp"].iloc[0] == MS_2021_01_01 + 5 * 3_600_000


def test_output_is_deduped_and_sorted():
    raw = pd.DataFrame(
        {
            "timestamp": [300, 100, 200, 100],
            "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0,
        }
    )
    result = normalize_ohlcv(raw)
    assert result["timestamp"].tolist() == [100, 200, 300]


def test_unrecognized_schema_raises():
    with pytest.raises(ValueError, match="Unrecognized OHLCV schema"):
        normalize_ohlcv(pd.DataFrame({"foo": [1], "bar": [2]}))
