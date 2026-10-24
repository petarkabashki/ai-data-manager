"""Gap detection, path builders and the freqtrade naming translation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from utils.catalog import (
    crypto_file_path,
    crypto_futures_file_path,
    ft_timeframe,
    futures_pair_to_ft_symbol,
    futures_pair_to_symbol,
    index_file_path,
    our_timeframe,
    pair_to_symbol,
    stock_file_path,
    symbol_to_pair,
    write_parquet_atomic,
)
from utils.gaps import detect_gaps
from utils.ibkr import (
    DownloadRange,
    daterange_chunks,
    ibkr_duration_str,
    merge_ranges,
    parse_date,
    plan_download_ranges,
    read_timestamp_bounds,
)

DAY = 86_400_000


# --- paths -------------------------------------------------------------------

def test_provider_is_always_the_first_level_under_the_class():
    """The provider-first path rule, asserted once per asset class."""
    assert crypto_file_path("binance", "BTC_USDT", "1d").parts[-4:] == (
        "cryptos", "binance", "BTC_USDT", "BTC_USDT-1d.parquet")
    assert crypto_futures_file_path("bybit", "BTC_USDT", "4h").parts[-4:] == (
        "crypto-futures", "bybit", "BTC_USDT", "BTC_USDT-4h.parquet")
    assert index_file_path("IBKR", "GSPC", "1h").parts[-3:] == (
        "ibkr", "GSPC", "GSPC-1h.parquet")
    assert stock_file_path("YF", "sp500", "AAPL", "1d").parts[-4:] == (
        "yf", "sp500", "AAPL", "AAPL-1d.parquet")


def test_session_appears_in_the_filename_only_when_given():
    assert stock_file_path("IBKR", "sp500", "AAPL", "1h", "overnight").name == "AAPL-overnight-1h.parquet"
    assert stock_file_path("YF", "sp500", "AAPL", "1d").name == "AAPL-1d.parquet"


def test_freqtrade_naming_round_trips():
    assert pair_to_symbol("BTC/USDT") == "BTC_USDT"
    assert symbol_to_pair("BTC_USDT") == "BTC/USDT"
    assert ft_timeframe("1Mo") == "1M" and our_timeframe("1M") == "1Mo"
    assert ft_timeframe("1d") == "1d"
    assert futures_pair_to_symbol("BTC/USDT:USDT") == "BTC_USDT"
    assert futures_pair_to_ft_symbol("BTC/USDT:USDT") == "BTC_USDT_USDT"


# --- gaps --------------------------------------------------------------------

def _write(tmp_path, ohlcv, name="x.parquet", **kw):
    path = tmp_path / name
    write_parquet_atomic(ohlcv(**kw), path)
    return path


def test_no_gaps_in_a_contiguous_series(tmp_path, ohlcv):
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    path = _write(tmp_path, ohlcv, start_ms=now - 4 * DAY, n=5)
    gaps, trailing = detect_gaps(path, "1d")
    assert gaps == []
    assert trailing == 0


def test_internal_gap_is_found(tmp_path, ohlcv):
    import pandas as pd
    first = ohlcv(start_ms=0, n=3)
    second = ohlcv(start_ms=10 * DAY, n=3)
    path = tmp_path / "x.parquet"
    write_parquet_atomic(pd.concat([first, second], ignore_index=True), path)

    gaps, _ = detect_gaps(path, "1d")
    assert len(gaps) == 1
    assert gaps[0].missing_candles == 7  # days 3..9 inclusive
    assert gaps[0].start_ms == 3 * DAY


def test_trailing_candles_measure_staleness(tmp_path, ohlcv):
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    path = _write(tmp_path, ohlcv, start_ms=now - 14 * DAY, n=5)  # ends 10 days ago
    _, trailing = detect_gaps(path, "1d")
    assert 9 <= trailing <= 11


def test_variable_timeframe_is_skipped(tmp_path, ohlcv):
    path = _write(tmp_path, ohlcv, n=3)
    assert detect_gaps(path, "1Mo") == ([], 0)


def test_missing_file_is_not_an_error(tmp_path):
    assert detect_gaps(tmp_path / "nope.parquet", "1d") == ([], 0)


# --- IBKR planner -------------------------------------------------------------

def test_duration_string_switches_to_years_past_365_days():
    assert ibkr_duration_str(parse_date("2025-01-01"), parse_date("2025-06-01")) == "151 D"
    # Rounds up, so the request always covers at least the window asked for.
    assert ibkr_duration_str(parse_date("2020-01-01"), parse_date("2026-01-01")) == "7 Y"
    # Exactly 365 days still fits a day-denominated request.
    assert ibkr_duration_str(parse_date("2025-01-01"), parse_date("2026-01-01")) == "365 D"
    assert ibkr_duration_str(parse_date("2025-01-01"), parse_date("2026-01-02")) == "2 Y"


def test_daterange_chunks_tile_the_window_in_order():
    start, end = parse_date("2024-01-01"), parse_date("2024-12-31")
    chunks = daterange_chunks(start, end, 100)
    assert chunks[0][0] == start and chunks[-1][1] == end
    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:]):
        assert prev_end == next_start


def test_merge_ranges_coalesces_overlaps():
    a = DownloadRange(parse_date("2024-01-01"), parse_date("2024-03-01"), "a")
    b = DownloadRange(parse_date("2024-02-01"), parse_date("2024-04-01"), "b")
    c = DownloadRange(parse_date("2024-09-01"), parse_date("2024-10-01"), "c")
    merged = merge_ranges([c, b, a])
    assert len(merged) == 2
    assert merged[0].end == parse_date("2024-04-01")
    assert merged[0].reason == "a,b"


def test_plan_full_requires_since(tmp_path):
    with pytest.raises(ValueError, match="--full requires --since"):
        plan_download_ranges(tmp_path / "x.parquet", since=None, full=True,
                             download_end=datetime.now(tz=timezone.utc))


def test_plan_missing_file_requests_everything(tmp_path):
    ranges = plan_download_ranges(
        tmp_path / "nope.parquet", since="2024-01-01", full=False,
        download_end=datetime.now(tz=timezone.utc),
    )
    assert [r.reason for r in ranges] == ["missing_file"]
    assert ranges[0].start == parse_date("2024-01-01")


def test_plan_appends_a_tail_overlap(tmp_path, ohlcv):
    now = datetime.now(tz=timezone.utc)
    start_ms = int((now - timedelta(days=30)).timestamp() * 1000)
    path = _write(tmp_path, ohlcv, start_ms=start_ms, n=25)

    ranges = plan_download_ranges(path, since=None, full=False, download_end=now, overlap_days=5)
    assert [r.reason for r in ranges] == ["tail_overlap"]
    assert ranges[0].end == now


def test_plan_prepends_when_since_predates_the_file(tmp_path, ohlcv):
    now = datetime.now(tz=timezone.utc)
    start_ms = int((now - timedelta(days=30)).timestamp() * 1000)
    path = _write(tmp_path, ohlcv, start_ms=start_ms, n=25)

    ranges = plan_download_ranges(path, since="2015-01-01", full=False, download_end=now)
    assert [r.reason for r in ranges] == ["prepend", "tail_overlap"]
    assert ranges[0].start == parse_date("2015-01-01")


def test_plan_full_ignores_what_is_on_disk(tmp_path, ohlcv):
    now = datetime.now(tz=timezone.utc)
    path = _write(tmp_path, ohlcv, n=10)
    ranges = plan_download_ranges(path, since="2020-01-01", full=True, download_end=now)
    assert [r.reason for r in ranges] == ["full"]


def test_read_timestamp_bounds(tmp_path, ohlcv):
    path = _write(tmp_path, ohlcv, start_ms=0, n=5)
    assert read_timestamp_bounds(path) == (0, 4 * DAY)
    assert read_timestamp_bounds(tmp_path / "nope.parquet") is None
