"""Session calendars — which bars a series is *supposed* to have.

Gap detection used to assume every series trades 24/7. That is right for crypto
and wrong for everything else: on a daily S&P 500 series every weekend is a
two-day hole and every market holiday another, so `detect_gaps()` reported
hundreds of gaps that don't exist, and
`download_ibkr_stock_overnight.py` fed them straight back into IBKR requests
that could never be satisfied — on every run, forever.

A calendar answers one question:

    expected_starts(after_ms, before_ms, interval_ms) -> list[int]

"which bar-start instants strictly between these two should exist?" Gap
detection then reports a gap only when that list is non-empty. This is
deliberately a *suppression* rule rather than a prediction of the provider's
exact bar grid: intraday grids differ by provider and request type, and
inventing them would produce confident nonsense. The cost is that a genuinely
missing bar which happens to sit inside a closure window won't be reported.
That trade is the right way round — a false gap triggers an unsatisfiable
download, a missed one is visible as staleness.

No third-party calendar package: the holiday rules below are self-contained, so
this works in an environment with no network.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

import numpy as np

NY_TZ = ZoneInfo("America/New_York")

DAY_MS = 86_400_000


# --- US market holidays -------------------------------------------------------

def _easter(year: int) -> date:
    """Anonymous Gregorian algorithm — needed only for Good Friday."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    lam = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * lam) // 451
    month, day = divmod(h + lam - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """nth weekday of a month (weekday: Monday=0). n=-1 means the last one."""
    if n > 0:
        first = date(year, month, 1)
        offset = (weekday - first.weekday()) % 7
        return first + timedelta(days=offset + 7 * (n - 1))
    last_day = (date(year, month % 12 + 1, 1) - timedelta(days=1)) if month < 12 else date(year, 12, 31)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _observed(day: date) -> date | None:
    """NYSE observance for a fixed-date holiday.

    Saturday holidays shift to the preceding Friday, Sunday to the following
    Monday — except that a Saturday New Year's Day is not observed at all
    (the exchange does not close the preceding 31 December).
    """
    if day.weekday() == 5:  # Saturday
        if day.month == 1 and day.day == 1:
            return None
        return day - timedelta(days=1)
    if day.weekday() == 6:  # Sunday
        return day + timedelta(days=1)
    return day


#: Closures that no rule predicts. Not exhaustive before 2001 — a missing entry
#: shows up as a reported gap on one day, which is the safe direction to err.
AD_HOC_CLOSURES: frozenset[date] = frozenset(
    {
        date(2001, 9, 11), date(2001, 9, 12), date(2001, 9, 13), date(2001, 9, 14),
        date(2004, 6, 11),   # Reagan national day of mourning
        date(2007, 1, 2),    # Ford national day of mourning
        date(2012, 10, 29), date(2012, 10, 30),  # Hurricane Sandy
        date(2018, 12, 5),   # G.H.W. Bush national day of mourning
        date(2025, 1, 9),    # Carter national day of mourning
    }
)


@lru_cache(maxsize=64)
def us_market_holidays(year: int) -> frozenset[date]:
    """NYSE full-day closures for one calendar year."""
    days: set[date] = set()

    for observed in (
        _observed(date(year, 1, 1)),                    # New Year's Day
        _observed(date(year, 7, 4)),                    # Independence Day
        _observed(date(year, 12, 25)),                  # Christmas
    ):
        if observed is not None:
            days.add(observed)

    days.add(_nth_weekday(year, 1, 0, 3))               # MLK (3rd Monday Jan)
    days.add(_nth_weekday(year, 2, 0, 3))               # Washington's Birthday
    days.add(_easter(year) - timedelta(days=2))         # Good Friday
    days.add(_nth_weekday(year, 5, 0, -1))              # Memorial Day
    days.add(_nth_weekday(year, 9, 0, 1))               # Labor Day
    days.add(_nth_weekday(year, 11, 3, 4))              # Thanksgiving

    if year >= 2022:                                    # Juneteenth
        observed = _observed(date(year, 6, 19))
        if observed is not None:
            days.add(observed)
    if year < 1998:                                     # MLK observed from 1998
        days.discard(_nth_weekday(year, 1, 0, 3))

    days |= {d for d in AD_HOC_CLOSURES if d.year == year}
    return frozenset(days)


@lru_cache(maxsize=64)
def us_early_closes(year: int) -> frozenset[date]:
    """Days the NYSE closes at 13:00 ET instead of 16:00."""
    days: set[date] = set()
    holidays = us_market_holidays(year)

    # Day after Thanksgiving.
    days.add(_nth_weekday(year, 11, 3, 4) + timedelta(days=1))

    # 3 July, when Independence Day itself is a trading day.
    july_3 = date(year, 7, 3)
    if july_3.weekday() < 5 and date(year, 7, 4).weekday() < 5:
        days.add(july_3)

    # Christmas Eve, when it's a weekday and not itself a holiday.
    christmas_eve = date(year, 12, 24)
    if christmas_eve.weekday() < 5:
        days.add(christmas_eve)

    return frozenset(d for d in days if d.weekday() < 5 and d not in holidays)


def is_us_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in us_market_holidays(day.year)


def us_trading_days_between(first: date, last: date) -> int:
    """US trading days in [first, last], both ends included; 0 if last < first."""
    if last < first:
        return 0
    holidays = [d for year in range(first.year, last.year + 1) for d in us_market_holidays(year)]
    return int(np.busday_count(first, last + timedelta(days=1), holidays=holidays))


# --- calendars ----------------------------------------------------------------

def _to_ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def _ny(day: date, clock: time) -> datetime:
    """A New York wall-clock instant, DST handled by the zone itself."""
    return datetime.combine(day, clock, tzinfo=NY_TZ)


@dataclass(frozen=True, slots=True)
class Window:
    """A half-open [start, end) interval in epoch milliseconds."""

    start_ms: int
    end_ms: int

    def overlaps(self, start_ms: int, end_ms: int) -> bool:
        return start_ms < self.end_ms and end_ms > self.start_ms


def _aligned_starts(after_ms: int, before_ms: int, interval_ms: int) -> list[int]:
    """Epoch-aligned bar starts strictly between two instants."""
    if interval_ms <= 0 or before_ms <= after_ms:
        return []
    first = ((after_ms // interval_ms) + 1) * interval_ms
    return list(range(first, before_ms, interval_ms))


class ContinuousCalendar:
    """A market that never closes. The correct calendar for crypto."""

    name = "continuous"

    def is_session_day(self, day: date) -> bool:
        return True

    def session_days(self, first: date, last: date) -> int:
        """Days in [first, last] with a session; 0 if last < first."""
        return max(0, (last - first).days + 1)

    def windows(self, start_ms: int, end_ms: int) -> list[Window]:
        return [Window(start_ms, end_ms)]

    def expected_starts(self, after_ms: int, before_ms: int, interval_ms: int) -> list[int]:
        return _aligned_starts(after_ms, before_ms, interval_ms)


class UsEquityCalendar:
    """NYSE sessions, in one of three shapes.

    `rth`       09:30–16:00 ET (13:00 on early closes)
    `extended`  04:00–20:00 ET
    `overnight` the complement of RTH: 16:00 ET on a trading day through
                09:30 ET on the next one — which is what `--session overnight`
                stores, so gap detection has to use the same definition.

    Daily and weekly bars ignore the intraday window entirely: a daily bar is
    expected iff its UTC date is a trading day, which matches how both YF and
    IBKR stamp daily bars (midnight UTC of the session date).
    """

    def __init__(self, session: str = "rth"):
        if session not in ("rth", "extended", "overnight", "all"):
            raise ValueError(f"Unknown equity session {session!r}")
        self.session = "rth" if session == "all" else session
        self.name = f"us_equity_{self.session}"

    def is_session_day(self, day: date) -> bool:
        return is_us_trading_day(day)

    def session_days(self, first: date, last: date) -> int:
        return us_trading_days_between(first, last)

    # -- session windows --

    def _day_windows(self, day: date) -> list[Window]:
        """Session windows for one calendar day. Empty when the market is shut.

        Note that `overnight` is *two* windows per trading day — pre-market and
        post-market — not one stretch running from one close to the next open.
        US equities don't trade at all over a weekend, so a window spanning
        Friday's close to Monday's open would expect bars on Saturday and
        report their absence as a gap, which is the whole bug this fixes.
        """
        if not is_us_trading_day(day):
            return []

        close = time(13, 0) if day in us_early_closes(day.year) else time(16, 0)

        if self.session == "extended":
            return [Window(_to_ms(_ny(day, time(4, 0))), _to_ms(_ny(day, time(20, 0))))]

        if self.session == "rth":
            return [Window(_to_ms(_ny(day, time(9, 30))), _to_ms(_ny(day, close)))]

        # overnight: the extended-hours session minus RTH.
        return [
            Window(_to_ms(_ny(day, time(4, 0))), _to_ms(_ny(day, time(9, 30)))),
            Window(_to_ms(_ny(day, close)), _to_ms(_ny(day, time(20, 0)))),
        ]

    def windows(self, start_ms: int, end_ms: int) -> list[Window]:
        first = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).astimezone(NY_TZ).date()
        last = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).astimezone(NY_TZ).date()

        windows: list[Window] = []
        day = first - timedelta(days=1)  # a session can start on the previous UTC date
        while day <= last + timedelta(days=1):
            windows.extend(
                w for w in self._day_windows(day)
                if w.end_ms > start_ms and w.start_ms < end_ms
            )
            day += timedelta(days=1)
        return windows

    # -- expected bars --

    def expected_starts(self, after_ms: int, before_ms: int, interval_ms: int) -> list[int]:
        candidates = _aligned_starts(after_ms, before_ms, interval_ms)
        if not candidates:
            return []

        if interval_ms >= DAY_MS:
            return [
                ts for ts in candidates
                if is_us_trading_day(datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date())
            ]

        windows = self.windows(candidates[0], candidates[-1] + interval_ms)
        if not windows:
            return []
        return [
            ts for ts in candidates
            if any(w.overlaps(ts, ts + interval_ms) for w in windows)
        ]


class CmeFuturesCalendar:
    """CME Globex electronic sessions, as far as gap detection needs them.

    The trading week runs Sunday 18:00 ET through Friday 17:00 ET, broken each
    weekday by a 60-minute maintenance halt from 17:00 to 18:00 ET. So a
    "trading day" here is the Globex convention: the session that *opens* at
    18:00 ET on day D-1 and closes at 17:00 ET on day D, which is why a Sunday
    evening carries Monday's session and Friday evening is shut.

    Holiday handling is deliberately coarse. CME runs shortened sessions on
    most US holidays rather than closing outright, and the exact schedule
    varies by product group. Modelling that faithfully would need a per-product
    table, which is the kind of thing `utils/calendars.py` exists to avoid. We
    close the whole session on `us_market_holidays` and end it at 13:00 ET on
    `us_early_closes`.

    Measured against real IBKR bars, this runs slightly *over*: 482 expected
    hourly bars against 480 delivered for ES over 2026-08-18..2026-09-16, so
    roughly two phantom gaps a month at the session boundaries. That direction
    is the one to watch, because the IBKR planner turns every reported gap into
    a historical request that can never be satisfied. Two a month is tolerable;
    if it grows, tighten the boundary handling here rather than teaching the
    planner to ignore gaps.
    """

    name = "cme_futures"

    OPEN = time(18, 0)
    CLOSE = time(17, 0)
    EARLY_CLOSE = time(13, 0)

    def _session_for(self, close_day: date) -> Window | None:
        """The session closing on `close_day`, or None when there isn't one.

        Sessions are named for the day they close. Monday's session opens
        Sunday 18:00 ET; Saturday and Sunday never close a session.
        """
        if close_day.weekday() >= 5:  # Saturday, Sunday
            return None
        if not is_us_trading_day(close_day):
            return None

        open_day = close_day - timedelta(days=1)
        if open_day.weekday() == 5:  # Saturday never opens; Friday's close is final
            return None

        close = self.EARLY_CLOSE if close_day in us_early_closes(close_day.year) else self.CLOSE
        return Window(_to_ms(_ny(open_day, self.OPEN)), _to_ms(_ny(close_day, close)))

    # A session closes on exactly the US trading days (Monday's opens on Sunday).
    def is_session_day(self, day: date) -> bool:
        return is_us_trading_day(day)

    def session_days(self, first: date, last: date) -> int:
        return us_trading_days_between(first, last)

    def windows(self, start_ms: int, end_ms: int) -> list[Window]:
        first = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).astimezone(NY_TZ).date()
        last = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).astimezone(NY_TZ).date()

        windows: list[Window] = []
        day = first - timedelta(days=1)  # a session opens on the previous date
        while day <= last + timedelta(days=1):
            window = self._session_for(day)
            if window and window.end_ms > start_ms and window.start_ms < end_ms:
                windows.append(window)
            day += timedelta(days=1)
        return windows

    def expected_starts(self, after_ms: int, before_ms: int, interval_ms: int) -> list[int]:
        candidates = _aligned_starts(after_ms, before_ms, interval_ms)
        if not candidates:
            return []

        if interval_ms >= DAY_MS:
            return [
                ts for ts in candidates
                if self._session_for(datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date())
            ]

        windows = self.windows(candidates[0], candidates[-1] + interval_ms)
        if not windows:
            return []
        return [
            ts for ts in candidates
            if any(w.overlaps(ts, ts + interval_ms) for w in windows)
        ]


CONTINUOUS = ContinuousCalendar()

#: Asset class -> calendar factory. Indexes and commodities follow US equity
#: hours closely enough for gap purposes; crypto never closes.
_BY_ASSET_CLASS = {
    "Crypto": lambda session=None: CONTINUOUS,
    "Crypto Futures": lambda session=None: CONTINUOUS,
    "Futures": lambda session=None: CME_FUTURES,
    "Stock": lambda session=None: UsEquityCalendar(session or "rth"),
    "Index": lambda session=None: UsEquityCalendar(session or "rth"),
    "Commodity": lambda session=None: UsEquityCalendar(session or "extended"),
    "Macro": lambda session=None: UsEquityCalendar(session or "rth"),
}

CME_FUTURES = CmeFuturesCalendar()

CALENDAR_NAMES = [
    "continuous",
    "us_equity_rth",
    "us_equity_extended",
    "us_equity_overnight",
    "cme_futures",
]


def calendar_for(asset_class: str | None, session: str | None = None):
    """Calendar for an asset class, optionally narrowed by stored session."""
    factory = _BY_ASSET_CLASS.get(asset_class or "")
    if factory is None:
        return CONTINUOUS
    return factory(session)


def calendar_by_name(name: str):
    """Calendar from a CLI `--calendar` value."""
    if name == "continuous":
        return CONTINUOUS
    if name == "cme_futures":
        return CME_FUTURES
    if name.startswith("us_equity_"):
        return UsEquityCalendar(name[len("us_equity_"):])
    raise ValueError(f"Unknown calendar {name!r}; expected one of {CALENDAR_NAMES}")
