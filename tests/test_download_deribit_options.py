"""Expiry-calendar and strike-grid generation in the Deribit options tool."""

from __future__ import annotations

import argparse
import importlib.util
from datetime import date
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "download_deribit_options",
    Path(__file__).resolve().parent.parent / "tools" / "download_deribit_options.py",
)
assert _spec and _spec.loader
tool = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tool)


# --- the expiry calendar ------------------------------------------------------

def test_last_friday_is_the_last_one_not_the_fourth():
    """A month with five Fridays is where a "fourth Friday" rule goes wrong."""
    assert tool.last_friday(2024, 5) == date(2024, 5, 31)
    assert tool.last_friday(2024, 12) == date(2024, 12, 27)
    assert tool.last_friday(2019, 12) == date(2019, 12, 27)


def test_last_friday_when_the_month_ends_on_a_friday():
    assert tool.last_friday(2024, 5).weekday() == 4
    assert tool.last_friday(2026, 1) == date(2026, 1, 30)


def test_quarterlies_are_the_four_months_that_have_them():
    expiries = tool.generate_expiries(date(2024, 1, 1), date(2024, 12, 31), quarterly=True)
    assert [e.month for e in expiries] == [3, 6, 9, 12]
    assert all(e.weekday() == 4 for e in expiries)


def test_monthlies_cover_every_month():
    expiries = tool.generate_expiries(date(2024, 1, 1), date(2024, 12, 31), quarterly=False)
    assert len(expiries) == 12


def test_generated_expiries_stay_inside_the_requested_range():
    expiries = tool.generate_expiries(date(2024, 4, 1), date(2024, 10, 1), quarterly=True)
    assert expiries == [date(2024, 6, 28), date(2024, 9, 27)]


# --- the strike grid ----------------------------------------------------------

def test_a_strike_grid_includes_both_ends():
    assert tool.parse_strike_grid("20000:40000:5000") == [20000, 25000, 30000, 35000, 40000]


def test_a_grid_that_describes_no_strikes_is_refused():
    """A silent empty grid would report "0 contracts found" and read as
    "Deribit has no history", which is the wrong conclusion entirely."""
    with pytest.raises(argparse.ArgumentTypeError):
        tool.parse_strike_grid("40000:20000:5000")
    with pytest.raises(argparse.ArgumentTypeError):
        tool.parse_strike_grid("20000:40000:0")
    with pytest.raises(argparse.ArgumentTypeError):
        tool.parse_strike_grid("20000-40000")


# --- discovery ----------------------------------------------------------------

def test_discovery_probes_the_full_grid_and_keeps_only_what_exists():
    from utils.deribit import InstrumentInfo

    real = {"BTC-27DEC24-60000-C", "BTC-27DEC24-65000-P"}

    class StubClient:
        def __init__(self) -> None:
            self.asked: list[str] = []

        def get_instrument(self, name):
            self.asked.append(name)
            if name not in real:
                return None
            return InstrumentInfo(name, date(2023, 12, 28), date(2024, 12, 27), 0.0001, 1.0)

    client = StubClient()
    found, probes = tool.discover(
        client, "BTC", [date(2024, 12, 27)], [60000, 65000], ["C", "P"]
    )

    assert probes == 4
    assert len(client.asked) == 4
    assert {i.name for i in found} == real
    # Probes go out under Deribit's spelling, not the repo's canonical symbol.
    assert "BTC-27DEC24-60000-C" in client.asked
