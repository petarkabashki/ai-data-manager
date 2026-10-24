"""Option contract identity: the things that would silently mis-file a series."""

from __future__ import annotations

from datetime import date

import pytest

from utils.options import (
    OptionContract,
    contract_for,
    contract_from_deribit_name,
    format_strike,
    option_symbol,
    parse_deribit_name,
    parse_option_symbol,
    update_option_entry,
)


# --- the symbol ---------------------------------------------------------------

def test_a_directory_listing_sorts_chronologically():
    """The only reason we don't just store Deribit's own name.

    `27DEC24` sorts before `28JUN24` lexicographically, so a directory of
    Deribit names is in alphabetical month order — August, December, January.
    """
    deribit_order = sorted(["BTC-27DEC24-60000-C", "BTC-28JUN24-60000-C",
                            "BTC-30AUG24-60000-C"])
    assert deribit_order[0].startswith("BTC-27DEC24")  # December first: wrong

    ours = sorted(
        option_symbol("BTC", expiry, 60000, "C")
        for expiry in (date(2024, 12, 27), date(2024, 6, 28), date(2024, 8, 30))
    )
    assert [s.split("-")[1] for s in ours] == ["20240628", "20240830", "20241227"]


def test_an_integral_strike_never_renders_a_trailing_zero():
    """60000.0 and 60000 must be one directory, not two."""
    assert format_strike(60000.0) == "60000"
    assert format_strike(60000) == "60000"
    assert format_strike(0.5) == "0.5"


def test_symbol_round_trips():
    symbol = option_symbol("BTC", date(2024, 12, 27), 60000, "C")
    assert symbol == "BTC-20241227-60000-C"
    assert parse_option_symbol(symbol) == ("BTC", date(2024, 12, 27), 60000.0, "C")


def test_a_call_and_a_put_are_different_contracts():
    args = ("BTC", date(2024, 12, 27), 60000)
    assert option_symbol(*args, "C") != option_symbol(*args, "P")


def test_something_that_is_neither_a_call_nor_a_put_is_rejected():
    with pytest.raises(ValueError, match="right must be"):
        option_symbol("BTC", date(2024, 12, 27), 60000, "X")


def test_a_nonsense_strike_is_rejected_rather_than_stored():
    with pytest.raises(ValueError, match="strike must be positive"):
        OptionContract("BTC", date(2024, 12, 27), 0, "C")


# --- the Deribit spelling -----------------------------------------------------

def test_deribit_does_not_zero_pad_the_day():
    """Deribit's own board lists `2OCT26`; `02OCT26` is not an instrument."""
    contract = contract_for("BTC", date(2026, 10, 2), 60000, "C")
    assert contract.deribit_name == "BTC-2OCT26-60000-C"


def test_deribit_name_round_trips():
    assert parse_deribit_name("BTC-27DEC24-60000-C") == (
        "BTC", date(2024, 12, 27), 60000.0, "C"
    )
    contract = contract_from_deribit_name("BTC-27DEC24-60000-C")
    assert contract.symbol == "BTC-20241227-60000-C"
    assert contract.deribit_name == "BTC-27DEC24-60000-C"


def test_a_two_digit_year_resolves_to_the_nearby_century():
    """An archive that outlives this decade must still read `19` as 2019."""
    _, expiry, _, _ = parse_deribit_name("BTC-27DEC19-8000-C", pivot_year=2026)
    assert expiry.year == 2019


def test_a_name_that_is_not_an_option_is_refused():
    with pytest.raises(ValueError, match="Not a Deribit option name"):
        parse_deribit_name("BTC-PERPETUAL")


# --- what the catalog has to record -------------------------------------------

def test_the_entry_says_the_premium_is_not_in_dollars():
    """A BTC option quoting 0.1575 is 0.1575 BTC. Reading it as USD is
    wrong by five orders of magnitude, and nothing in an OHLCV file says so."""
    entry = contract_for("BTC", date(2024, 12, 27), 60000, "C").catalog_entry()
    assert entry["quote_currency"] == "BTC"
    assert entry["contract_size"] == 1.0
    assert entry["categories"] == ["Crypto Options"]


def test_the_entry_keeps_strike_and_right_as_data():
    entry = contract_for("BTC", date(2024, 12, 27), 60000, "P").catalog_entry()
    assert entry["strike"] == 60000
    assert entry["right"] == "put"
    assert entry["expiry"] == "2024-12-27"


def test_two_venues_for_one_option_coexist_in_the_catalog(tmp_path):
    """Nested-by-provider is the whole reason this catalog isn't flat."""
    contract = contract_for("BTC", date(2024, 12, 27), 60000, "C")
    catalog: dict = {}
    for provider in ("Deribit", "OKX"):
        update_option_entry(
            catalog, contract, "1d", provider,
            tmp_path / f"{provider}.parquet", "2024-01-02", "2024-12-27",
        )

    node = catalog[contract.symbol]["timeframe_dates"]["1d"]
    assert sorted(node) == ["Deribit", "OKX"]
    assert node["Deribit"]["provider"] == "Deribit"
    # Semantics are stamped so `check_catalog.py` sees a complete node.
    assert node["Deribit"]["volume_basis"] == "contracts"
    assert node["Deribit"]["adjustment"] == "not_applicable"


def test_a_listing_date_arrives_from_whichever_venue_knows_it(tmp_path):
    contract = contract_for("BTC", date(2024, 12, 27), 60000, "C")
    dated = contract_for("BTC", date(2024, 12, 27), 60000, "C", listed=date(2023, 12, 28))
    catalog: dict = {}
    update_option_entry(catalog, contract, "1d", "OKX",
                        tmp_path / "a.parquet", "2024-01-02", "2024-12-27")
    assert catalog[contract.symbol]["listed"] is None

    update_option_entry(catalog, dated, "1d", "Deribit",
                        tmp_path / "b.parquet", "2023-12-28", "2024-12-27")
    assert catalog[contract.symbol]["listed"] == "2023-12-28"
