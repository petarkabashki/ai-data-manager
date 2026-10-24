"""Gap detection against a trading calendar.

The case that motivated this: a complete daily equity series used to report a
gap for every weekend and market holiday — hundreds of them — and the IBKR
overnight planner turned each one into a historical request that could never be
satisfied, on every run.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from utils.calendars import CONTINUOUS, UsEquityCalendar, is_us_trading_day
from utils.catalog import write_parquet_atomic
from utils.gaps import detect_gaps

DAY_MS = 86_400_000
HOUR_MS = 3_600_000


def _ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)


def _frame(timestamps: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.Series(timestamps, dtype="int64"),
            "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 100.0,
        }
    )


def _trading_days(start: date, end: date) -> list[date]:
    days, day = [], start
    while day <= end:
        if is_us_trading_day(day):
            days.append(day)
        day += timedelta(days=1)
    return days


@pytest.fixture
def equity_daily(tmp_path):
    """A complete daily series over a year of real trading days."""

    def _make(start=date(2025, 1, 1), end=date(2025, 12, 31), skip: set[date] | None = None):
        days = [d for d in _trading_days(start, end) if d not in (skip or set())]
        path = tmp_path / f"series-{len(days)}.parquet"
        write_parquet_atomic(_frame([_ms(d) for d in days]), path)
        return path, days

    return _make


# --- the bug ------------------------------------------------------------------

def test_complete_equity_year_has_no_gaps(equity_daily):
    path, days = equity_daily()
    assert len(days) > 240, "sanity: a year is ~250 trading days"

    gaps, _ = detect_gaps(path, "1d", UsEquityCalendar("rth"))
    assert gaps == []


def test_the_same_series_is_full_of_holes_under_the_24_7_rule(equity_daily):
    """Pin the old behaviour, so the fix can't silently regress."""
    path, _ = equity_daily()
    gaps, _ = detect_gaps(path, "1d", CONTINUOUS)
    assert len(gaps) > 50  # every weekend, plus every holiday


def test_crypto_is_unaffected_by_the_change(tmp_path):
    """A 24/7 series with a genuine hole still reports it, identically."""
    timestamps = [i * DAY_MS for i in range(10)] + [i * DAY_MS for i in range(15, 20)]
    path = tmp_path / "crypto.parquet"
    write_parquet_atomic(_frame(timestamps), path)

    gaps, _ = detect_gaps(path, "1d")  # default calendar
    assert len(gaps) == 1
    assert gaps[0].missing_candles == 5


# --- real gaps are still found ------------------------------------------------

def test_a_missing_trading_day_is_reported(equity_daily):
    missing = date(2025, 3, 12)  # an ordinary Wednesday
    assert is_us_trading_day(missing)
    path, _ = equity_daily(skip={missing})

    gaps, _ = detect_gaps(path, "1d", UsEquityCalendar("rth"))
    assert len(gaps) == 1
    assert gaps[0].missing_candles == 1
    assert gaps[0].start_ms == _ms(missing)


def test_a_missing_week_of_trading_days_is_counted_correctly(equity_daily):
    skip = {date(2025, 3, 10) + timedelta(days=i) for i in range(5)}  # Mon–Fri
    path, _ = equity_daily(skip=skip)

    gaps, _ = detect_gaps(path, "1d", UsEquityCalendar("rth"))
    assert len(gaps) == 1
    assert gaps[0].missing_candles == 5  # the weekend either side doesn't count


def test_a_gap_ending_at_a_holiday_counts_only_trading_days(equity_daily):
    # Skip 30 June–3 July; 4 July is a holiday, so only four bars are missing.
    skip = {date(2025, 6, 30), date(2025, 7, 1), date(2025, 7, 2), date(2025, 7, 3)}
    path, _ = equity_daily(skip=skip)

    gaps, _ = detect_gaps(path, "1d", UsEquityCalendar("rth"))
    assert len(gaps) == 1
    assert gaps[0].missing_candles == 4


# --- staleness ----------------------------------------------------------------

def test_trailing_staleness_ignores_a_closed_market(tmp_path):
    """A series current to Friday's close isn't stale on Saturday."""
    today = datetime.now(tz=timezone.utc).date()
    days = _trading_days(today - timedelta(days=30), today)
    path = tmp_path / "fresh.parquet"
    write_parquet_atomic(_frame([_ms(d) for d in days]), path)

    _, trailing = detect_gaps(path, "1d", UsEquityCalendar("rth"))
    assert trailing == 0


def test_trailing_staleness_counts_only_missed_trading_days(tmp_path):
    today = datetime.now(tz=timezone.utc).date()
    cutoff = today - timedelta(days=21)
    days = _trading_days(today - timedelta(days=60), cutoff)
    path = tmp_path / "stale.parquet"
    write_parquet_atomic(_frame([_ms(d) for d in days]), path)

    _, trailing = detect_gaps(path, "1d", UsEquityCalendar("rth"))
    expected = len([d for d in _trading_days(cutoff + timedelta(days=1), today)]) - 1
    assert abs(trailing - expected) <= 2  # boundary day, depending on run time
    assert trailing < 21, "weekends must not inflate staleness"


# --- intraday -----------------------------------------------------------------

def test_intraday_overnight_series_has_no_weekend_gaps(tmp_path):
    """`--session overnight` keeps only out-of-hours bars; the planner used to
    re-request every weekend between them."""
    cal = UsEquityCalendar("overnight")
    timestamps: list[int] = []
    for day in _trading_days(date(2025, 6, 2), date(2025, 6, 30)):
        base = _ms(day)
        timestamps.extend(cal.expected_starts(base - 1, base + DAY_MS, HOUR_MS))

    path = tmp_path / "overnight.parquet"
    write_parquet_atomic(_frame(sorted(set(timestamps))), path)

    gaps, _ = detect_gaps(path, "1h", cal)
    assert gaps == []
