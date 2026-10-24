"""Yahoo ingestion: ticker spelling, interval mapping, lookback limits, shape.

The network call itself is not exercised — `Fetcher` exists precisely so
everything around it can be. What's tested here is what actually goes wrong in
practice: a mis-spelled ticker (which Yahoo answers with an empty frame rather
than an error), an interval Yahoo doesn't have, an intraday window older than
Yahoo serves, and the index/column shape yfinance returns.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from utils.yf import (
    OUR_TF_TO_YF_INTERVAL,
    UnsupportedTimeframe,
    clamp_start,
    frame_from_yahoo,
    yf_interval,
    yf_ticker,
)

NOW = datetime(2026, 9, 13, tzinfo=timezone.utc)


# --- ticker spelling ----------------------------------------------------------

@pytest.mark.parametrize(
    "symbol,asset_class,expected",
    [
        ("GSPC", "Index", "^GSPC"),
        ("VIX", "Index", "^VIX"),
        ("N225", "Index", "^N225"),
        ("^GSPC", "Index", "^GSPC"),        # already prefixed, not double-prefixed
        ("AAPL", "Stock", "AAPL"),
        ("BRK.B", "Stock", "BRK-B"),        # Yahoo uses a dash for share classes
        ("BF.B", "Stock", "BF-B"),
        ("GC=F", "Commodity", "GC=F"),      # already Yahoo's own spelling
    ],
)
def test_ticker_mapping(symbol, asset_class, expected):
    assert yf_ticker(symbol, asset_class) == expected


def test_unknown_index_still_gets_a_prefix():
    """Better a plausible guess than a silently unprefixed equity ticker."""
    assert yf_ticker("NEWIDX", "Index") == "^NEWIDX"


# --- intervals ----------------------------------------------------------------

def test_interval_mapping():
    assert yf_interval("1d") == "1d"
    assert yf_interval("1h") == "60m"     # Yahoo's name, not ours
    assert yf_interval("1w") == "1wk"
    assert yf_interval("1Mo") == "1mo"


@pytest.mark.parametrize("timeframe", ["4h", "8h", "2h", "3d"])
def test_timeframes_yahoo_does_not_have_are_rejected_loudly(timeframe):
    """IBKR has 4h and 8h bars; Yahoo doesn't. Fail rather than silently
    substituting a different bar size."""
    assert timeframe not in OUR_TF_TO_YF_INTERVAL
    with pytest.raises(UnsupportedTimeframe, match="Yahoo has no"):
        yf_interval(timeframe)


# --- lookback limits ----------------------------------------------------------

def test_daily_history_is_not_clamped():
    ancient = datetime(1980, 12, 12, tzinfo=timezone.utc)
    assert clamp_start(ancient, "1d", now=NOW) == ancient


def test_hourly_history_is_clamped_to_yahoos_limit():
    """Yahoo serves ~730 days of 60m bars and silently truncates beyond it."""
    ancient = datetime(2015, 1, 1, tzinfo=timezone.utc)
    clamped = clamp_start(ancient, "1h", now=NOW)
    assert clamped > ancient
    assert (NOW - clamped).days == 730


def test_minute_history_is_clamped_hard():
    assert (NOW - clamp_start(datetime(2020, 1, 1, tzinfo=timezone.utc), "1m", now=NOW)).days == 7


def test_a_recent_start_is_left_alone():
    recent = NOW - timedelta(days=3)
    assert clamp_start(recent, "1h", now=NOW) == recent


# --- frame shape --------------------------------------------------------------

def _yahoo_daily(rows: int = 3, adjusted: bool = False) -> pd.DataFrame:
    """What yfinance returns: timestamp in the *index*, capitalised columns."""
    index = pd.DatetimeIndex(
        [datetime(2025, 1, 2) + timedelta(days=i) for i in range(rows)], name="Date"
    )
    data = {
        "Open": [1.0] * rows, "High": [2.0] * rows, "Low": [0.5] * rows,
        "Close": [1.5] * rows, "Volume": [1000.0] * rows,
    }
    if not adjusted:
        data["Adj Close"] = [1.4] * rows
    return pd.DataFrame(data, index=index)


def test_daily_frame_is_normalized():
    result = frame_from_yahoo(_yahoo_daily())
    assert list(result.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert result["timestamp"].dtype == "int64"
    assert len(result) == 3


def test_adj_close_is_dropped_not_substituted():
    """With auto_adjust=False, `close` must be the as-traded close."""
    result = frame_from_yahoo(_yahoo_daily())
    assert result["close"].tolist() == [1.5, 1.5, 1.5]  # not 1.4


def test_intraday_index_is_named_datetime_not_date():
    """yfinance names the index `Datetime` for intraday — normalize_ohlcv only
    recognises `Date`, so it has to be renamed or ingestion raises."""
    raw = _yahoo_daily()
    raw.index.name = "Datetime"
    result = frame_from_yahoo(raw)
    assert len(result) == 3


def test_tz_aware_intraday_index_converts_to_utc():
    raw = _yahoo_daily()
    raw.index = raw.index.tz_localize("America/New_York")
    raw.index.name = "Datetime"
    result = frame_from_yahoo(raw)
    # 2025-01-02 00:00 New York is 05:00 UTC.
    assert result["timestamp"].iloc[0] == int(
        datetime(2025, 1, 2, 5, tzinfo=timezone.utc).timestamp() * 1000
    )


def test_multiindex_columns_are_flattened():
    """A single-ticker download can still come back column-multi-indexed."""
    raw = _yahoo_daily()
    raw.columns = pd.MultiIndex.from_product([raw.columns, ["AAPL"]])
    assert len(frame_from_yahoo(raw)) == 3


def test_empty_response_is_an_empty_canonical_frame():
    """An unknown ticker is not an error at Yahoo — it's an empty frame."""
    for empty in (pd.DataFrame(), None):
        result = frame_from_yahoo(empty)
        assert result.empty
        assert list(result.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
        assert result["timestamp"].dtype == "int64"
