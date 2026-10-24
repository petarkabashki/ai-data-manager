"""The meaning layer: vocabularies, provider defaults, and what writers stamp."""

from __future__ import annotations

import pytest

from utils.catalog import (
    commodity_file_path,
    index_file_path,
    macro_file_path,
    stock_file_path,
    update_catalog_entry,
    update_index_timeframe_entry,
    update_simple_timeframe_entry,
    update_stock_timeframe_entry,
)
from utils.semantics import (
    SEMANTIC_FIELDS,
    Semantics,
    apply_semantics,
    asset_class_from_categories,
    semantics_for,
    validate_node_semantics,
)


# --- vocabularies -------------------------------------------------------------

def test_semantics_rejects_values_outside_its_vocabulary():
    with pytest.raises(ValueError, match="adjustment='sort-of'"):
        Semantics(adjustment="sort-of")
    with pytest.raises(ValueError, match="volume_basis"):
        Semantics(volume_basis="lots")


def test_validate_reports_missing_and_invalid_separately():
    assert validate_node_semantics({}) == [f"missing {f}" for f in SEMANTIC_FIELDS]

    node = {"schema": "ohlcv", "adjustment": "nope", "volume_basis": "shares", "bar_label": "open"}
    problems = validate_node_semantics(node)
    assert len(problems) == 1
    assert problems[0].startswith("adjustment='nope'")


def test_apply_semantics_never_overwrites_an_explicit_value():
    node = {"adjustment": "adjusted"}
    apply_semantics(node, semantics_for("Stock", "YF"))
    assert node["adjustment"] == "adjusted"  # not clobbered by the raw default
    assert node["volume_basis"] == "shares"  # but the gaps are filled


# --- provider defaults --------------------------------------------------------

@pytest.mark.parametrize(
    "asset_class,provider,expected",
    [
        ("Crypto", "binance", ("ohlcv", "not_applicable", "base_asset")),
        ("Crypto Futures", "binance", ("ohlcv", "not_applicable", "base_asset")),
        # KuCoin futures klines are denominated in contracts, not base asset.
        ("Crypto Futures", "kucoin", ("ohlcv", "not_applicable", "contracts")),
        ("Stock", "YF", ("ohlcv", "raw", "shares")),
        ("Index", "IBKR", ("ohlcv", "not_applicable", "none")),
        ("Commodity", "YF", ("ohlcv", "raw", "contracts")),
        ("Macro", "YF", ("rate_series", "not_applicable", "none")),
    ],
)
def test_defaults_by_asset_class(asset_class, provider, expected):
    s = semantics_for(asset_class, provider)
    assert (s.schema, s.adjustment, s.volume_basis) == expected
    assert s.bar_label == "open"


def test_adjusted_last_flips_the_adjustment_flag():
    assert semantics_for("Stock", "IBKR").adjustment == "raw"
    assert semantics_for("Stock", "IBKR", "ADJUSTED_LAST").adjustment == "adjusted"
    assert semantics_for("Stock", "IBKR", "adjusted_last").adjustment == "adjusted"
    assert semantics_for("Stock", "IBKR", "TRADES").adjustment == "raw"


def test_unknown_asset_class_is_an_error_not_a_silent_default():
    with pytest.raises(ValueError, match="Unknown asset class"):
        semantics_for("Bonds", "YF")


def test_asset_class_from_categories():
    assert asset_class_from_categories(["Crypto Futures"]) == "Crypto Futures"
    assert asset_class_from_categories(["Tech", "Stock"]) == "Stock"
    assert asset_class_from_categories(["Tech"]) is None
    assert asset_class_from_categories(None) is None


# --- what the writers stamp ---------------------------------------------------

def _node_has_semantics(node):
    assert validate_node_semantics(node) == [], node


def test_crypto_writer_stamps_semantics():
    catalog: dict = {}
    update_catalog_entry(
        catalog, "binance", "BTC_USDT", "1d",
        (index_file_path("YF", "X", "1d").parent.parent.parent
         / "cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet"),
        "2017-08-17", "2026-09-01",
    )
    node = catalog["BTC_USDT"]["timeframe_dates"]["1d"]
    _node_has_semantics(node)
    assert node["adjustment"] == "not_applicable"
    assert node["volume_basis"] == "base_asset"


def test_crypto_futures_writer_uses_the_futures_defaults():
    catalog: dict = {}
    update_catalog_entry(
        catalog, "kucoin", "BTC_USDT", "1d",
        macro_file_path("YF", "X", "1d"),  # path irrelevant to this assertion
        "2020-01-01", "2026-09-01",
        categories=["Crypto Futures"],
    )
    node = catalog["BTC_USDT_kucoin"]["timeframe_dates"]["1d"]
    assert node["volume_basis"] == "contracts"


def test_stock_writer_stamps_and_honours_what_to_show():
    catalog: dict = {}
    update_stock_timeframe_entry(
        catalog, "AAPL", "1d", "IBKR", stock_file_path("IBKR", "sp500", "AAPL", "1d", "all"),
        "2024-01-02", "2026-09-01", session="all", what_to_show="ADJUSTED_LAST",
    )
    node = catalog["AAPL"]["timeframe_dates"]["1d"]["IBKR"]
    _node_has_semantics(node)
    assert node["adjustment"] == "adjusted"
    assert node["volume_basis"] == "shares"


def test_index_writer_marks_volume_unusable():
    catalog: dict = {}
    update_index_timeframe_entry(
        catalog, "GSPC", "1h", "IBKR", index_file_path("IBKR", "GSPC", "1h"),
        "2024-01-02", "2026-09-01",
    )
    node = catalog["GSPC"]["timeframe_dates"]["1h"]["IBKR"]
    _node_has_semantics(node)
    assert node["volume_basis"] == "none"


def test_macro_writer_marks_the_series_as_not_ohlcv():
    """The check_schema.py exclusion for WTB3MS becomes data, not convention."""
    catalog: dict = {}
    update_simple_timeframe_entry(
        catalog, "WTB3MS", "1d", "YF", macro_file_path("YF", "WTB3MS", "1d"),
        "1954-01-04", "2026-09-01", asset_class="Macro",
    )
    node = catalog["WTB3MS"]["timeframe_dates"]["1d"]
    _node_has_semantics(node)
    assert node["schema"] == "rate_series"


def test_commodity_writer_stamps_semantics():
    catalog: dict = {}
    update_simple_timeframe_entry(
        catalog, "GC=F", "1d", "YF", commodity_file_path("YF", "GC=F", "1d"),
        "2000-08-30", "2026-09-01", asset_class="Commodity",
    )
    node = catalog["GC=F"]["timeframe_dates"]["1d"]
    _node_has_semantics(node)
    assert node["schema"] == "ohlcv"
    assert catalog["GC=F"]["categories"] == ["Commodity"]


def test_explicit_semantics_beat_the_defaults():
    catalog: dict = {}
    update_stock_timeframe_entry(
        catalog, "AAPL", "1d", "YF", stock_file_path("YF", "sp500", "AAPL", "1d"),
        "1980-12-12", "2026-09-01",
        semantics=Semantics("ohlcv", "adjusted", "shares", "close"),
    )
    node = catalog["AAPL"]["timeframe_dates"]["1d"]["YF"]
    assert node["adjustment"] == "adjusted"
    assert node["bar_label"] == "close"
