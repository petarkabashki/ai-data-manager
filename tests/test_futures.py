"""Futures contract identity, layout, calendar and semantics."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from utils.calendars import CME_FUTURES, calendar_by_name, calendar_for
from utils.futures import (
    ROOTS,
    UnknownRootError,
    contract_for,
    contract_from_symbol,
    contract_symbol,
    futures_catalog_path,
    futures_file_path,
    parse_contract_symbol,
)
from utils.semantics import semantics_for

HOUR_MS = 3_600_000
DAY_MS = 86_400_000


def ms(y: int, m: int, d: int, hh: int = 0, mm: int = 0) -> int:
    return int(datetime(y, m, d, hh, mm, tzinfo=timezone.utc).timestamp() * 1000)


# --- contract identity --------------------------------------------------------

def test_canonical_symbol_is_root_month_code_and_two_digit_year():
    assert contract_symbol("ES", 2026, 9) == "ESU26"
    assert contract_symbol("CL", 2025, 12) == "CLZ25"
    assert contract_symbol("gc", 2026, 2) == "GCG26"


def test_symbol_round_trips_through_the_parser():
    for root, year, month in [("ES", 2026, 9), ("NQ", 2031, 12), ("ZN", 2026, 3)]:
        parsed = parse_contract_symbol(contract_symbol(root, year, month))
        assert parsed == (root, year, month)


def test_two_digit_year_resolves_against_a_sliding_window():
    """`U26` is 2026 today and still 2026 when read from a 2090 pivot."""
    assert parse_contract_symbol("ESU26", pivot_year=2026)[1] == 2026
    assert parse_contract_symbol("ESU99", pivot_year=2026)[1] == 1999
    assert parse_contract_symbol("ESU05", pivot_year=2090)[1] == 2105


def test_parser_rejects_things_that_are_not_contracts():
    for bad in ["ES", "ESU2026", "ES-26", "ESA26", "", "AAPL"]:
        with pytest.raises(ValueError):
            parse_contract_symbol(bad)


def test_contract_for_fills_venue_metadata_from_the_root_table():
    es = contract_for("es", 2026, 9)
    assert (es.symbol, es.root, es.exchange, es.multiplier, es.currency) == (
        "ESU26", "ES", "CME", 50, "USD",
    )


def test_unknown_root_names_the_table_to_edit():
    with pytest.raises(UnknownRootError, match="ROOTS"):
        contract_for("XYZ", 2026, 9)


def test_symbol_names_the_delivery_month_not_the_expiry_month():
    """December crude is CLZ and stops trading in November; both must hold."""
    cl = contract_for("CL", 2025, 12, expiry=date(2025, 11, 20))
    assert cl.symbol == "CLZ25"
    assert cl.contract_month == "2025-12"
    assert cl.expiry == date(2025, 11, 20)
    entry = cl.catalog_entry()
    assert entry["contract_month"] == "2025-12"
    assert entry["expiry"] == "2025-11-20"
    assert entry["root"] == "CL" and entry["exchange"] == "NYMEX"


def test_a_contract_month_away_from_expiry_still_has_one_identity():
    """The same contract from two providers must land on one symbol.

    A provider that reports only the delivery month and one that reports only
    the last trade date have to agree, which is the whole point of keying on
    the delivery month.
    """
    from_month = contract_for("CL", 2025, 12)
    from_vendor = contract_from_symbol("CLZ25", expiry=date(2025, 11, 20))
    assert from_month.symbol == from_vendor.symbol == "CLZ25"
    assert futures_file_path("ibkr", from_month.symbol, "1h") == futures_file_path(
        "ibkr", from_vendor.symbol, "1h"
    )


def test_an_impossible_month_is_rejected_rather_than_stored():
    with pytest.raises(ValueError):
        contract_symbol("ES", 2026, 13)


def test_every_root_has_the_metadata_the_catalog_entry_needs():
    for root in ROOTS:
        entry = contract_for(root, 2026, 3).catalog_entry()
        assert entry["multiplier"] and entry["exchange"] and entry["currency"]
        assert entry["categories"] == ["Futures"]


# --- layout -------------------------------------------------------------------

def test_path_is_provider_first_so_a_second_provider_is_a_sibling():
    ibkr = futures_file_path("IBKR", "ESU26", "1h")
    other = futures_file_path("databento", "ESU26", "1h")
    assert ibkr.parts[-4:] == ("futures", "ibkr", "ESU26", "ESU26-1h.parquet")
    assert other.parts[-4:] == ("futures", "databento", "ESU26", "ESU26-1h.parquet")
    assert ibkr != other
    assert futures_catalog_path().name == "futures.yaml"


# --- semantics ----------------------------------------------------------------

def test_futures_default_is_raw_prices_in_contracts():
    sem = semantics_for("Futures", None, None)
    assert sem.adjustment == "raw"
    assert sem.volume_basis == "contracts"


# --- CME calendar -------------------------------------------------------------

def test_calendar_is_wired_to_the_asset_class_and_the_cli_name():
    assert calendar_for("Futures") is CME_FUTURES
    assert calendar_by_name("cme_futures") is CME_FUTURES


def test_session_opens_sunday_evening_and_closes_friday_afternoon():
    """Sunday 18:00 ET opens Monday's session; Friday 17:00 ET ends the week."""
    # 2026-09-13 is a Sunday, 2026-09-18 the Friday.
    sunday_2300_utc = ms(2026, 9, 13, 23)   # 19:00 ET Sunday -> open
    saturday_0000_utc = ms(2026, 9, 19, 12)  # 08:00 ET Saturday -> shut
    starts = CME_FUTURES.expected_starts(sunday_2300_utc - HOUR_MS, sunday_2300_utc + HOUR_MS, HOUR_MS)
    assert starts, "Sunday evening should be inside Monday's session"
    starts = CME_FUTURES.expected_starts(saturday_0000_utc - HOUR_MS, saturday_0000_utc + HOUR_MS, HOUR_MS)
    assert starts == [], "Saturday is never a session"


def test_the_daily_maintenance_halt_is_not_a_gap():
    """17:00-18:00 ET on a weekday is a scheduled halt, not missing data."""
    # 2026-09-16 is a Wednesday. 17:00 ET = 21:00 UTC (EDT).
    halt_start = ms(2026, 9, 16, 21)
    starts = CME_FUTURES.expected_starts(halt_start - 1, halt_start + HOUR_MS, HOUR_MS)
    assert starts == [], "no bar is expected during the maintenance halt"


def test_daily_bars_skip_weekends_and_holidays():
    # Christmas 2026 falls on a Friday; the 26th is a Saturday.
    for shut in [date(2026, 12, 25), date(2026, 12, 26), date(2026, 12, 27)]:
        ts = ms(shut.year, shut.month, shut.day)
        assert CME_FUTURES.expected_starts(ts - 1, ts + 1, DAY_MS) == [], shut
    # A plain Tuesday is expected.
    ts = ms(2026, 12, 29)
    assert CME_FUTURES.expected_starts(ts - 1, ts + 1, DAY_MS) == [ts]


def test_a_full_week_of_hourly_bars_is_roughly_the_globex_schedule():
    """23 hours a day, five days: enough to catch an off-by-a-session error."""
    start = ms(2026, 9, 13)  # Sunday
    end = ms(2026, 9, 20)    # the following Sunday
    starts = CME_FUTURES.expected_starts(start, end, HOUR_MS)
    # 5 sessions x 23 hours = 115, allowing a little slack at the boundaries.
    assert 110 <= len(starts) <= 120, len(starts)
