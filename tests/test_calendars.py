"""Session calendars: holiday rules, session windows, and expected bar starts."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from utils.calendars import (
    CONTINUOUS,
    UsEquityCalendar,
    calendar_by_name,
    calendar_for,
    is_us_trading_day,
    us_early_closes,
    us_market_holidays,
)

DAY_MS = 86_400_000
HOUR_MS = 3_600_000


def ms(y: int, m: int, d: int, hh: int = 0, mm: int = 0) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=timezone.utc).timestamp() * 1000)


# --- holiday rules ------------------------------------------------------------

def test_2025_matches_the_published_nyse_calendar():
    assert us_market_holidays(2025) == frozenset({
        date(2025, 1, 1),    # New Year's Day
        date(2025, 1, 9),    # Carter national day of mourning (ad hoc)
        date(2025, 1, 20),   # MLK
        date(2025, 2, 17),   # Washington's Birthday
        date(2025, 4, 18),   # Good Friday
        date(2025, 5, 26),   # Memorial Day
        date(2025, 6, 19),   # Juneteenth
        date(2025, 7, 4),    # Independence Day
        date(2025, 9, 1),    # Labor Day
        date(2025, 11, 27),  # Thanksgiving
        date(2025, 12, 25),  # Christmas
    })


def test_2026_matches_the_published_nyse_calendar():
    assert us_market_holidays(2026) == frozenset({
        date(2026, 1, 1), date(2026, 1, 19), date(2026, 2, 16),
        date(2026, 4, 3),    # Good Friday
        date(2026, 5, 25), date(2026, 6, 19),
        date(2026, 7, 3),    # 4 July falls on Saturday → observed Friday
        date(2026, 9, 7), date(2026, 11, 26), date(2026, 12, 25),
    })


def test_saturday_new_year_is_not_observed():
    """The exchange doesn't close the preceding 31 December for it."""
    assert date(2022, 1, 1).weekday() == 5
    assert date(2021, 12, 31) not in us_market_holidays(2021)
    assert date(2022, 1, 1) not in us_market_holidays(2022)


def test_sunday_holiday_shifts_forward():
    assert date(2022, 12, 25).weekday() == 6
    assert date(2022, 12, 26) in us_market_holidays(2022)


def test_juneteenth_only_from_2022():
    assert date(2021, 6, 18) not in us_market_holidays(2021)
    assert date(2022, 6, 20) in us_market_holidays(2022)  # 19th was a Sunday


def test_ad_hoc_closures_are_included():
    assert date(2012, 10, 29) in us_market_holidays(2012)  # Hurricane Sandy
    assert date(2001, 9, 12) in us_market_holidays(2001)


def test_early_closes():
    closes = us_early_closes(2025)
    assert date(2025, 11, 28) in closes  # day after Thanksgiving
    assert date(2025, 12, 24) in closes  # Christmas Eve
    assert date(2025, 7, 3) in closes    # 4 July is a Friday
    assert date(2025, 12, 25) not in closes  # a full holiday, not an early close


def test_is_us_trading_day():
    assert is_us_trading_day(date(2025, 6, 17))       # ordinary Tuesday
    assert not is_us_trading_day(date(2025, 6, 21))   # Saturday
    assert not is_us_trading_day(date(2025, 6, 19))   # Juneteenth


# --- continuous calendar ------------------------------------------------------

def test_continuous_expects_every_interval():
    starts = CONTINUOUS.expected_starts(ms(2025, 6, 1), ms(2025, 6, 5), DAY_MS)
    assert starts == [ms(2025, 6, 2), ms(2025, 6, 3), ms(2025, 6, 4)]


def test_continuous_expects_bars_across_a_weekend():
    """The behaviour crypto relies on, and equities must not inherit."""
    starts = CONTINUOUS.expected_starts(ms(2025, 6, 20), ms(2025, 6, 24), DAY_MS)
    assert len(starts) == 3  # Sat, Sun, Mon


# --- US equity daily ----------------------------------------------------------

def test_daily_skips_weekends():
    cal = UsEquityCalendar("rth")
    # Friday 20 June → Tuesday 24 June. Only Monday 23rd is expected;
    # the 21st and 22nd are the weekend.
    starts = cal.expected_starts(ms(2025, 6, 20), ms(2025, 6, 24), DAY_MS)
    assert starts == [ms(2025, 6, 23)]


def test_daily_skips_holidays():
    cal = UsEquityCalendar("rth")
    # Wednesday 18 June → Friday 20 June, across Juneteenth on the 19th.
    assert cal.expected_starts(ms(2025, 6, 18), ms(2025, 6, 20), DAY_MS) == []


def test_a_long_weekend_produces_no_expectation_at_all():
    cal = UsEquityCalendar("rth")
    # Friday 4 July 2025 is a holiday: Thursday 3rd → Monday 7th is empty.
    assert cal.expected_starts(ms(2025, 7, 3), ms(2025, 7, 7), DAY_MS) == []


def test_daily_counts_a_real_missing_trading_day():
    cal = UsEquityCalendar("rth")
    # Monday 16 June → Thursday 19 June: Tue 17th and Wed 18th are missing.
    starts = cal.expected_starts(ms(2025, 6, 16), ms(2025, 6, 19), DAY_MS)
    assert starts == [ms(2025, 6, 17), ms(2025, 6, 18)]


# --- US equity intraday -------------------------------------------------------

def test_intraday_rth_expects_bars_only_inside_the_session():
    cal = UsEquityCalendar("rth")
    # Tuesday 17 June 2025, 00:00 UTC → next midnight. RTH is 13:30–20:00 UTC
    # (09:30–16:00 EDT), so roughly seven hourly bars overlap it.
    starts = cal.expected_starts(ms(2025, 6, 17), ms(2025, 6, 18), HOUR_MS)
    assert starts, "expected some intraday bars on a trading day"
    assert all(ms(2025, 6, 17, 13) <= t < ms(2025, 6, 17, 20) for t in starts)


def test_intraday_expects_nothing_on_a_closed_day():
    cal = UsEquityCalendar("rth")
    assert cal.expected_starts(ms(2025, 6, 21), ms(2025, 6, 22), HOUR_MS) == []  # Saturday
    assert cal.expected_starts(ms(2025, 6, 19), ms(2025, 6, 20), HOUR_MS) == []  # Juneteenth


def test_extended_session_is_wider_than_rth():
    rth = UsEquityCalendar("rth").expected_starts(ms(2025, 6, 17), ms(2025, 6, 18), HOUR_MS)
    ext = UsEquityCalendar("extended").expected_starts(ms(2025, 6, 17), ms(2025, 6, 18), HOUR_MS)
    assert len(ext) > len(rth)


def test_overnight_is_the_complement_of_rth():
    """`--session overnight` stores bars outside 09:30–16:00, so gap detection
    has to expect bars in exactly that complement."""
    overnight = UsEquityCalendar("overnight")
    rth = UsEquityCalendar("rth")
    window = (ms(2025, 6, 17), ms(2025, 6, 18))

    overnight_starts = set(overnight.expected_starts(*window, HOUR_MS))
    rth_starts = set(rth.expected_starts(*window, HOUR_MS))
    assert overnight_starts
    # The one bar straddling the 16:00 close can belong to both.
    assert len(overnight_starts & rth_starts) <= 1


def test_overnight_spans_the_weekend_as_one_closure():
    """Friday close → Monday open is a single non-trading stretch, not a gap."""
    cal = UsEquityCalendar("overnight")
    # Saturday is entirely inside the Friday→Monday overnight window, but the
    # window is one session, so a bar-less Saturday isn't a hole in the data.
    saturday = cal.expected_starts(ms(2025, 6, 21, 6), ms(2025, 6, 21, 12), HOUR_MS)
    assert saturday == []


def test_early_close_shortens_the_expected_session():
    cal = UsEquityCalendar("rth")
    normal = cal.expected_starts(ms(2025, 11, 26), ms(2025, 11, 27), HOUR_MS)
    early = cal.expected_starts(ms(2025, 11, 28), ms(2025, 11, 29), HOUR_MS)
    assert early, "the day after Thanksgiving is a half day, not a closure"
    assert len(early) < len(normal)


def test_dst_shift_is_handled_by_the_zone():
    """A session is 09:30 New York either side of a DST change, so its UTC
    time-of-day moves by an hour and the expectation moves with it."""
    cal = UsEquityCalendar("rth")

    def first_utc_hour(day_start_ms: int) -> int:
        starts = cal.expected_starts(day_start_ms, day_start_ms + DAY_MS, HOUR_MS)
        return (min(starts) - day_start_ms) // HOUR_MS

    winter = first_utc_hour(ms(2025, 1, 7))   # EST, UTC-5 → 09:30 ET = 14:30 UTC
    summer = first_utc_hour(ms(2025, 7, 8))   # EDT, UTC-4 → 09:30 ET = 13:30 UTC
    assert winter == summer + 1


# --- registry -----------------------------------------------------------------

def test_calendar_for_asset_class():
    assert calendar_for("Crypto") is CONTINUOUS
    assert calendar_for("Crypto Futures") is CONTINUOUS
    assert calendar_for("Stock").name == "us_equity_rth"
    assert calendar_for("Stock", "overnight").name == "us_equity_overnight"
    assert calendar_for("Index").name == "us_equity_rth"
    assert calendar_for(None) is CONTINUOUS  # unknown class: assume nothing


def test_calendar_by_name_round_trips():
    for name in ("continuous", "us_equity_rth", "us_equity_extended", "us_equity_overnight"):
        assert calendar_by_name(name).name == name


def test_calendar_by_name_rejects_nonsense():
    with pytest.raises(ValueError, match="Unknown calendar"):
        calendar_by_name("lse")
