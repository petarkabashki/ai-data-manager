"""Staleness counts session days overdue, and a check's findings aren't a failed job."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from pdm.alerts import Alerter, found_problems
from utils import planning
from utils.calendars import CONTINUOUS, CmeFuturesCalendar, UsEquityCalendar
from utils.planning import days_behind

MONDAY = datetime(2026, 10, 26, 0, 52, tzinfo=timezone.utc)
EQUITY = UsEquityCalendar()


@pytest.mark.parametrize(("end", "timeframe", "calendar", "behind"), [
    ("2026-10-23", "1d", EQUITY, 0),        # Friday's bar is current on Monday morning
    ("2026-10-22", "1d", EQUITY, 1),        # missing Friday: the weekend doesn't count
    ("2026-10-21", "1d", EQUITY, 2),
    ("2026-10-23", "1h", EQUITY, 0),        # intraday counts by day, like daily
    ("2026-10-25", "1d", CONTINUOUS, 0),    # crypto: yesterday's bar is the newest closed
    ("2026-10-24", "1d", CONTINUOUS, 1),    # yesterday's bar closed 52 minutes ago
    ("2026-10-22", "1d", CONTINUOUS, 3),
    ("2026-10-22", "3d", CONTINUOUS, 0),    # its next bar (10-25..10-28) is still forming
    ("2026-10-20", "3d", CONTINUOUS, 1),    # 10-23..10-26 closed at midnight
    ("2026-10-12", "1w", CONTINUOUS, 1),    # Monday-aligned week closed at midnight
    ("2026-10-15", "1w", CONTINUOUS, 0),    # kucoin's Thursday weeks: 10-22..10-29 is forming
    ("2026-10-05", "1w", CONTINUOUS, 8),
    ("2026-09-01", "1Mo", CONTINUOUS, 0),   # October hasn't closed
    ("2026-08-01", "1Mo", CONTINUOUS, 26),  # September closed on 10-01
    ("2026-10-23", "1d", CmeFuturesCalendar(), 0),
])
def test_days_behind(end, timeframe, calendar, behind):
    assert days_behind(end, timeframe, calendar, now=MONDAY) == behind


def test_days_behind_skips_holidays_and_rejects_non_dates():
    # Thanksgiving 2026 is Thursday 11-26: Wednesday's bar is current on Friday morning.
    friday = datetime(2026, 11, 27, 1, 0, tzinfo=timezone.utc)
    assert days_behind("2026-11-25", "1d", EQUITY, now=friday) == 0
    assert days_behind("2026-11-24", "1d", EQUITY, now=friday) == 1
    assert days_behind(None, "1d") is None
    assert days_behind("soon", "1d") is None
    # An old series is counted in trading days, quickly.
    assert days_behind("2000-01-03", "1d", EQUITY, now=MONDAY) == pytest.approx(6742, abs=1)


def test_a_weekend_no_longer_trips_the_stale_alert(tmp_path, monkeypatch):
    def row(symbol, end, catalog="sp500", asset_class="Stock", timeframe="1d"):
        return {"catalog": catalog, "asset_class": asset_class, "symbol": symbol, "timeframe": timeframe,
                "provider": "yf", "session": None, "end_date": end}

    alerts = Alerter(lambda *a: None, tmp_path, stale_days=2)
    monkeypatch.setattr(planning, "utc_now", lambda: datetime(2026, 10, 23, 23, 50, tzinfo=timezone.utc))
    assert alerts.check_stale([row("AAPL", "2026-10-23"), row("BTC", "2026-10-15", "crypto_binance",
                                                              "Crypto", "3d")]) == []     # baseline
    monkeypatch.setattr(planning, "utc_now", lambda: MONDAY)
    # Friday's close on Monday, and a 3d bar still forming, are both current.
    assert alerts.check_stale([row("AAPL", "2026-10-23"), row("BTC", "2026-10-21", "crypto_binance",
                                                              "Crypto", "3d")]) == []
    monkeypatch.setattr(planning, "utc_now", lambda: datetime(2026, 10, 28, 1, 0, tzinfo=timezone.utc))
    out = alerts.check_stale([row("AAPL", "2026-10-23")])          # missed Monday's and Tuesday's runs
    assert [a["kind"] for a in out] == ["stale"] and out[0]["count"] == 1


def _check_job(tmp_path, log_text, state="failed", returncode=1, tool="check_catalog", source="client"):
    log = tmp_path / f"{tool}-{state}.log"
    log.write_text(log_text)
    return SimpleNamespace(id="c1", tool=tool, args=["--fix", "--json"], state=state, source=source,
                           returncode=returncode, error=None, log_path=str(log))


def test_a_check_that_finds_problems_is_a_report_not_a_failure(tmp_path):
    report = '{\n  "issues": [],\n  "orphan_files": ["a.parquet"]\n}\n'
    crash = "Traceback (most recent call last):\n  File \"x\", line 1\nKeyError: 'end_date'\n"
    assert found_problems(_check_job(tmp_path, report))
    assert not found_problems(_check_job(tmp_path, crash))
    assert not found_problems(_check_job(tmp_path, report, returncode=2))          # refused its arguments
    assert not found_problems(_check_job(tmp_path, report, tool="download_yf"))
    assert not found_problems(_check_job(tmp_path, report, tool="query"))

    alerts = Alerter(lambda *a: None, tmp_path)
    assert alerts.on_job(_check_job(tmp_path, report)) is None
    assert alerts.on_job(_check_job(tmp_path, crash))["kind"] == "job"

    # A scheduled check that crashed, then ran and found problems, has recovered.
    sched = "schedule:audit-nightly"
    assert alerts.on_job(_check_job(tmp_path, crash, source=sched))["kind"] == "job"
    assert alerts.on_job(_check_job(tmp_path, report, source=sched))["kind"] == "recovered"
