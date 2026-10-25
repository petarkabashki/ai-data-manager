"""tools/query.py and tools/status.py across every asset class.

Both used to be crypto-shaped or absent. These run the real CLIs against a
temp data root holding one of each catalog shape.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def populated_root(tmp_path, monkeypatch):
    """A data root with a stock (two providers), an index, and a crypto pair."""
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    names = ("utils.config", "utils.catalog", "utils.data", "utils.calendars",
             "utils.semantics", "utils.futures", "utils.options", "utils.planning",
             "query", "status")
    for name in names:
        sys.modules.pop(name, None)

    sys.path.insert(0, str(REPO_ROOT / "tools"))
    catalog = importlib.import_module("utils.catalog")

    def frame(n=5):
        return pd.DataFrame(
            {
                "timestamp": pd.Series([i * 86_400_000 for i in range(n)], dtype="int64"),
                "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 9.0,
            }
        )

    # Stocks: AAPL daily from YF, hourly from IBKR — the nested shape.
    (root / "sp500.yaml").write_text(
        yaml.dump({"AAPL": {"name": "AAPL", "categories": ["Stock"],
                            "timeframes": [], "timeframe_dates": {}}})
    )
    stock_catalog = catalog.load_stock_catalog("sp500")
    for provider, timeframe, session in (("YF", "1d", None), ("IBKR", "1h", "all")):
        path = catalog.stock_file_path(provider, "sp500", "AAPL", timeframe, session)
        catalog.write_parquet_atomic(frame(), path)
        start, end, _ = catalog.read_parquet_range(path)
        catalog.update_stock_timeframe_entry(
            stock_catalog, "AAPL", timeframe, provider, path, start, end, session=session
        )
    catalog.save_stock_catalog("sp500", stock_catalog)

    # Index.
    (root / "indexes.yaml").write_text(
        yaml.dump({"GSPC": {"name": "GSPC", "categories": ["Index"],
                            "timeframes": [], "timeframe_dates": {}}})
    )
    index_catalog = catalog.load_index_catalog()
    index_path = catalog.index_file_path("YF", "GSPC", "1d")
    catalog.write_parquet_atomic(frame(), index_path)
    start, end, _ = catalog.read_parquet_range(index_path)
    catalog.update_index_timeframe_entry(
        index_catalog, "GSPC", "1d", "YF", index_path, start, end
    )
    catalog.save_index_catalog(index_catalog)

    # Crypto: the flat shape.
    crypto_catalog: dict = {}
    crypto_path = catalog.crypto_file_path("binance", "BTC_USDT", "1d")
    catalog.write_parquet_atomic(frame(), crypto_path)
    start, end, _ = catalog.read_parquet_range(crypto_path)
    catalog.update_catalog_entry(
        crypto_catalog, "binance", "BTC_USDT", "1d", crypto_path, start, end
    )
    catalog.save_crypto_catalog("binance", crypto_catalog)

    yield root, catalog

    sys.path.remove(str(REPO_ROOT / "tools"))
    for name in names:
        sys.modules.pop(name, None)


def _run(module_name, argv, monkeypatch, capsys):
    module = importlib.import_module(module_name)
    monkeypatch.setattr(sys, "argv", [f"{module_name}.py", *argv])
    try:
        module.main()
    except SystemExit as exc:
        if exc.code:
            return capsys.readouterr().out, exc.code
    return capsys.readouterr().out, 0


# --- query --------------------------------------------------------------------

def test_query_covers_every_asset_class(populated_root, monkeypatch, capsys):
    """The old query.py saw crypto only, despite the name."""
    out, _ = _run("query", ["--json"], monkeypatch, capsys)
    payload = json.loads(out)

    classes = {row["asset_class"] for row in payload["series"]}
    assert {"Stock", "Index", "Crypto"} <= classes
    assert payload["count"] == 4  # AAPL 1d + AAPL 1h + GSPC 1d + BTC_USDT 1d


def test_query_filters_by_asset_class(populated_root, monkeypatch, capsys):
    out, _ = _run("query", ["--asset-class", "Index", "--json"], monkeypatch, capsys)
    payload = json.loads(out)
    assert [row["symbol"] for row in payload["series"]] == ["GSPC"]


def test_query_filters_by_provider_case_insensitively(populated_root, monkeypatch, capsys):
    """Directories are lowercase, catalog nodes upper — callers shouldn't care."""
    for spelling in ("IBKR", "ibkr"):
        out, _ = _run("query", ["--provider", spelling, "--json"], monkeypatch, capsys)
        payload = json.loads(out)
        assert payload["count"] == 1
        assert payload["series"][0]["timeframe"] == "1h"


def test_query_reports_session_and_semantics(populated_root, monkeypatch, capsys):
    out, _ = _run("query", ["--provider", "IBKR", "--json"], monkeypatch, capsys)
    row = json.loads(out)["series"][0]
    assert row["session"] == "all"
    assert row["adjustment"] == "raw"
    assert row["volume_basis"] == "shares"


def test_query_does_not_read_files_unless_asked(populated_root, monkeypatch, capsys):
    """Re-reading every parquet was the default; across 5,000 symbols it's unusable."""
    out, _ = _run("query", ["--json"], monkeypatch, capsys)
    assert all(row["rows"] is None for row in json.loads(out)["series"])

    out, _ = _run("query", ["--rescan", "--json"], monkeypatch, capsys)
    assert all(row["rows"] == 5 for row in json.loads(out)["series"])


def test_query_list_catalogs(populated_root, monkeypatch, capsys):
    out, _ = _run("query", ["--list-catalogs", "--json"], monkeypatch, capsys)
    by_name = {row["catalog"]: row for row in json.loads(out)}
    assert by_name["sp500"]["series"] == 2
    assert by_name["crypto_binance"]["symbols"] == 1


def test_query_list_symbols_is_deduped(populated_root, monkeypatch, capsys):
    out, _ = _run("query", ["--list-symbols", "--asset-class", "Stock", "--json"],
                  monkeypatch, capsys)
    assert json.loads(out) == ["AAPL"]  # one entry, though it has two series


def test_query_stale_filter(populated_root, monkeypatch, capsys):
    """Everything in the fixture is stamped 1970, so it's all extremely stale."""
    out, _ = _run("query", ["--stale-days", "100000", "--json"], monkeypatch, capsys)
    assert json.loads(out)["count"] == 0

    out, _ = _run("query", ["--stale-days", "1", "--json"], monkeypatch, capsys)
    assert json.loads(out)["count"] == 4


def test_query_rejects_an_unknown_catalog(populated_root, monkeypatch, capsys):
    _, code = _run("query", ["--catalog", "nope"], monkeypatch, capsys)
    assert code != 0


# --- status -------------------------------------------------------------------

def test_status_rolls_up_every_catalog(populated_root, monkeypatch, capsys):
    out, _ = _run("status", ["--json"], monkeypatch, capsys)
    payload = json.loads(out)

    assert payload["totals"]["symbols"] == 3
    assert payload["totals"]["series"] == 4
    by_name = {c["catalog"]: c for c in payload["catalogs"]}
    assert set(by_name) == {"sp500", "indexes", "crypto_binance"}


def test_status_reports_the_calendar_per_class(populated_root, monkeypatch, capsys):
    out, _ = _run("status", ["--json"], monkeypatch, capsys)
    by_name = {c["catalog"]: c for c in json.loads(out)["catalogs"]}
    assert by_name["crypto_binance"]["calendar"] == "continuous"
    assert by_name["sp500"]["calendar"] == "us_equity_rth"


def test_status_breaks_down_providers_and_timeframes(populated_root, monkeypatch, capsys):
    out, _ = _run("status", ["--json"], monkeypatch, capsys)
    sp500 = {c["catalog"]: c for c in json.loads(out)["catalogs"]}["sp500"]
    assert sp500["providers"] == {"IBKR": 1, "YF": 1}
    assert sp500["timeframes"] == {"1d": 1, "1h": 1}


def test_status_is_shallow_by_default(populated_root, monkeypatch, capsys):
    out, _ = _run("status", ["--json"], monkeypatch, capsys)
    payload = json.loads(out)
    assert payload["deep"] is False
    assert all(c["checked_files"] == 0 for c in payload["catalogs"])


def test_deep_status_is_clean_on_good_data(populated_root, monkeypatch, capsys):
    out, code = _run("status", ["--deep", "--json"], monkeypatch, capsys)
    payload = json.loads(out)
    assert payload["totals"]["problems"] == 0
    assert sum(c["checked_files"] for c in payload["catalogs"]) == 4
    assert code == 0


def test_deep_status_finds_a_missing_file_and_exits_nonzero(populated_root, monkeypatch, capsys):
    root, catalog = populated_root
    catalog.index_file_path("YF", "GSPC", "1d").unlink()

    out, code = _run("status", ["--deep", "--json"], monkeypatch, capsys)
    payload = json.loads(out)
    assert payload["totals"]["problems"] == 1
    by_name = {c["catalog"]: c for c in payload["catalogs"]}
    assert by_name["indexes"]["missing_files"] == 1
    assert code != 0


def test_deep_status_finds_a_schema_mismatch(populated_root, monkeypatch, capsys):
    root, catalog = populated_root
    path = catalog.index_file_path("YF", "GSPC", "1d")
    pd.DataFrame({"Date": ["2025-01-01"], "Close": [1.0]}).to_parquet(path, index=False)

    out, code = _run("status", ["--deep", "--json"], monkeypatch, capsys)
    by_name = {c["catalog"]: c for c in json.loads(out)["catalogs"]}
    assert by_name["indexes"]["schema_mismatches"] == 1
    assert code != 0


def test_deep_status_finds_missing_semantics(populated_root, monkeypatch, capsys):
    root, catalog = populated_root
    stored = yaml.safe_load((root / "indexes.yaml").read_text())
    del stored["GSPC"]["timeframe_dates"]["1d"]["YF"]["adjustment"]
    (root / "indexes.yaml").write_text(yaml.dump(stored))

    out, code = _run("status", ["--deep", "--json"], monkeypatch, capsys)
    by_name = {c["catalog"]: c for c in json.loads(out)["catalogs"]}
    assert by_name["indexes"]["semantic_gaps"] == 1
    assert code != 0


def test_status_table_output_is_readable(populated_root, monkeypatch, capsys):
    out, _ = _run("status", [], monkeypatch, capsys)
    assert "sp500" in out and "us_equity_rth" in out
    assert "3 symbols" in out and "4 series" in out
