"""Catalog writers — above all, that one write can't clobber another's node.

Each of these fails against the pre-E1 writers.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from utils.catalog import (
    index_file_path,
    provider_node_key,
    recompute_entry_bounds,
    stock_file_path,
    update_catalog_entry,
    update_index_timeframe_entry,
    update_stock_timeframe_entry,
)


# --- node keys ---------------------------------------------------------------

@pytest.mark.parametrize(
    "provider,session,expected",
    [
        ("IBKR", None, "IBKR"),
        ("IBKR", "all", "IBKR"),        # default session keeps the bare key,
        ("YF", "", "YF"),               # so existing catalogs keep resolving
        ("IBKR", "overnight", "IBKR:overnight"),
    ],
)
def test_provider_node_key(provider, session, expected):
    assert provider_node_key(provider, session) == expected


# --- indexes -----------------------------------------------------------------

def test_index_providers_coexist_on_one_timeframe():
    """An IBKR daily download must not evict YF's daily registration."""
    catalog: dict = {}
    yf_path = index_file_path("YF", "GSPC", "1d")
    ibkr_path = index_file_path("IBKR", "GSPC", "1d")

    update_index_timeframe_entry(catalog, "GSPC", "1d", "YF", yf_path, "1950-01-03", "2026-09-01")
    update_index_timeframe_entry(catalog, "GSPC", "1d", "IBKR", ibkr_path, "2024-01-02", "2026-09-01")

    node = catalog["GSPC"]["timeframe_dates"]["1d"]
    assert set(node) == {"YF", "IBKR"}
    assert node["YF"]["file_path"].endswith("indexes/yf/GSPC/GSPC-1d.parquet")
    assert node["IBKR"]["file_path"].endswith("indexes/ibkr/GSPC/GSPC-1d.parquet")


def test_index_legacy_flat_node_is_migrated_not_replaced():
    """Older indexes.yaml is flat; the first nested write must preserve it."""
    catalog = {
        "GSPC": {
            "name": "GSPC",
            "provider": "YF",
            "timeframes": ["1d"],
            "timeframe_dates": {
                "1d": {
                    "start_date": "1950-01-03",
                    "end_date": "2026-09-01",
                    "file_path": "price-data/indexes/yf/GSPC/GSPC-1d.parquet",
                }
            },
        }
    }
    update_index_timeframe_entry(
        catalog, "GSPC", "1d", "IBKR", index_file_path("IBKR", "GSPC", "1d"),
        "2024-01-02", "2026-09-01",
    )
    node = catalog["GSPC"]["timeframe_dates"]["1d"]
    assert set(node) == {"YF", "IBKR"}
    assert node["YF"]["start_date"] == "1950-01-03"


def test_index_new_timeframe_sits_alongside_existing():
    catalog: dict = {}
    update_index_timeframe_entry(catalog, "NDX", "1d", "YF", index_file_path("YF", "NDX", "1d"), "1985-10-01", "2026-09-01")
    update_index_timeframe_entry(catalog, "NDX", "1h", "IBKR", index_file_path("IBKR", "NDX", "1h"), "2024-01-02", "2026-09-01")
    assert set(catalog["NDX"]["timeframe_dates"]) == {"1d", "1h"}
    assert catalog["NDX"]["timeframes"] == ["1h", "1d"]  # sorted by duration


# --- stocks ------------------------------------------------------------------

def test_stock_providers_coexist_on_one_timeframe():
    catalog: dict = {}
    update_stock_timeframe_entry(
        catalog, "AAPL", "1d", "YF", stock_file_path("YF", "sp500", "AAPL", "1d"),
        "1980-12-12", "2026-09-01",
    )
    update_stock_timeframe_entry(
        catalog, "AAPL", "1d", "IBKR", stock_file_path("IBKR", "sp500", "AAPL", "1d", "all"),
        "2024-01-02", "2026-09-01", session="all",
    )
    assert set(catalog["AAPL"]["timeframe_dates"]["1d"]) == {"YF", "IBKR"}


def test_stock_sessions_do_not_clobber_each_other():
    """`session` is in the filename, so it must be in the catalog key too.

    Otherwise two parquet files exist and only one is registered.
    """
    catalog: dict = {}
    all_path = stock_file_path("IBKR", "sp500", "AAPL", "1h", "all")
    overnight_path = stock_file_path("IBKR", "sp500", "AAPL", "1h", "overnight")
    assert all_path != overnight_path

    update_stock_timeframe_entry(
        catalog, "AAPL", "1h", "IBKR", all_path, "2024-01-02", "2026-09-01", session="all"
    )
    update_stock_timeframe_entry(
        catalog, "AAPL", "1h", "IBKR", overnight_path, "2024-01-02", "2026-09-01", session="overnight"
    )

    node = catalog["AAPL"]["timeframe_dates"]["1h"]
    assert set(node) == {"IBKR", "IBKR:overnight"}
    assert node["IBKR"]["file_path"].endswith("AAPL-all-1h.parquet")
    assert node["IBKR:overnight"]["file_path"].endswith("AAPL-overnight-1h.parquet")


def test_stock_records_adjustment_mode():
    catalog: dict = {}
    update_stock_timeframe_entry(
        catalog, "AAPL", "1d", "IBKR", stock_file_path("IBKR", "sp500", "AAPL", "1d", "all"),
        "2024-01-02", "2026-09-01", session="all", what_to_show="ADJUSTED_LAST",
    )
    assert catalog["AAPL"]["timeframe_dates"]["1d"]["IBKR"]["adjustment"] == "adjusted"


def test_stock_repeat_write_updates_in_place():
    catalog: dict = {}
    path = stock_file_path("IBKR", "sp500", "AAPL", "1h", "all")
    for end in ("2026-08-01", "2026-09-01"):
        update_stock_timeframe_entry(
            catalog, "AAPL", "1h", "IBKR", path, "2024-01-02", end, session="all"
        )
    node = catalog["AAPL"]["timeframe_dates"]["1h"]
    assert list(node) == ["IBKR"]
    assert node["IBKR"]["end_date"] == "2026-09-01"


# --- crypto (flat shape) ------------------------------------------------------

def test_crypto_stays_flat():
    catalog: dict = {}
    update_catalog_entry(
        catalog, "binance", "BTC_USDT", "1d",
        Path("price-data/cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet").resolve(),
        "2017-08-17", "2026-09-01",
    )
    node = catalog["BTC_USDT"]["timeframe_dates"]["1d"]
    assert "file_path" in node, "crypto has one provider per timeframe; don't nest it"


def test_crypto_key_includes_exchange_for_non_binance():
    catalog: dict = {}
    update_catalog_entry(
        catalog, "bitget", "BTC_USDT", "1d",
        Path("price-data/cryptos/bitget/BTC_USDT/BTC_USDT-1d.parquet").resolve(),
        "2020-01-01", "2026-09-01",
    )
    assert list(catalog) == ["BTC_USDT_bitget"]


# --- bounds ------------------------------------------------------------------

def test_bounds_span_every_node_in_both_shapes():
    entry = {
        "timeframe_dates": {
            "1d": {
                "YF": {"start_date": "1980-12-12", "end_date": "2026-01-01"},
                "IBKR": {"start_date": "2024-01-02", "end_date": "2026-09-01"},
            },
            "4h": {"start_date": "2015-01-01", "end_date": "2025-01-01"},  # flat
        }
    }
    recompute_entry_bounds(entry)
    assert entry["start_date"] == "1980-12-12"
    assert entry["end_date"] == "2026-09-01"
