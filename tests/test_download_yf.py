"""tools/download_yf.py end to end, with a fake fetcher in place of Yahoo.

Everything but the HTTP call is real: the planner, the merge, the catalog
writes, the semantics stamped on the node. The fetcher records what it was
asked for, so the tests can assert on the *requests* as well as the result.
"""

from __future__ import annotations

import importlib
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


class FakeYahoo:
    """Returns a daily frame covering whatever window is asked for."""

    def __init__(self, empty: bool = False):
        self.calls: list[dict] = []
        self.empty = empty

    def fetch(self, ticker, interval, start, end, auto_adjust):
        self.calls.append(
            {"ticker": ticker, "interval": interval, "start": start,
             "end": end, "auto_adjust": auto_adjust}
        )
        if self.empty:
            return pd.DataFrame()

        days = pd.date_range(start.date(), end.date(), freq="D", name="Date")
        close = 2.0 if auto_adjust else 1.5
        return pd.DataFrame(
            {
                "Open": 1.0, "High": 2.0, "Low": 0.5,
                "Close": close, "Adj Close": 1.4, "Volume": 1000.0,
            },
            index=days,
        )


@pytest.fixture
def yf_env(tmp_path, monkeypatch):
    """Temp data root, utils reloaded against it, the tool importable."""
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    names = ("utils.config", "utils.catalog", "utils.data", "utils.planning",
             "utils.yf", "download_yf")
    for name in names:
        sys.modules.pop(name, None)

    sys.path.insert(0, str(REPO_ROOT / "tools"))
    catalog = importlib.import_module("utils.catalog")
    tool = importlib.import_module("download_yf")
    yield root, catalog, tool

    sys.path.remove(str(REPO_ROOT / "tools"))
    for name in names:
        sys.modules.pop(name, None)


def _seed_stock_catalog(root, symbols=("AAPL",)):
    (root / "sp500.yaml").write_text(
        yaml.dump({s: {"name": s, "categories": ["Stock"], "timeframes": [],
                       "timeframe_dates": {}} for s in symbols})
    )


def _run(tool, argv, monkeypatch, fetcher):
    monkeypatch.setattr(tool, "YahooFetcher", lambda *a, **k: fetcher)
    monkeypatch.setattr(sys, "argv", ["download_yf.py", *argv])
    tool.main()


# --- happy path ---------------------------------------------------------------

def test_downloads_a_stock_universe_end_to_end(yf_env, monkeypatch, capsys):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root, ("AAPL", "MSFT"))
    fetcher = FakeYahoo()

    _run(tool, ["--universe", "sp500", "--timeframes", "1d",
                "--full", "--since", "2025-01-01"], monkeypatch, fetcher)

    # Files landed in the provider-first location.
    for symbol in ("AAPL", "MSFT"):
        path = catalog.stock_file_path("YF", "sp500", symbol, "1d")
        assert path.exists(), path
        stored = pd.read_parquet(path)
        assert list(stored.columns) == ["timestamp", "open", "high", "low", "close", "volume"]

    stored_catalog = yaml.safe_load((root / "sp500.yaml").read_text())
    node = stored_catalog["AAPL"]["timeframe_dates"]["1d"]["YF"]
    assert node["file_path"].endswith("stocks/yf/sp500/AAPL/AAPL-1d.parquet")
    assert node["provider"] == "YF"


def test_node_records_the_adjustment_choice(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, FakeYahoo())
    raw_node = yaml.safe_load((root / "sp500.yaml").read_text())["AAPL"]["timeframe_dates"]["1d"]["YF"]
    assert raw_node["adjustment"] == "raw"
    assert raw_node["volume_basis"] == "shares"
    assert raw_node["schema"] == "ohlcv"


def test_adjusted_flag_changes_both_the_request_and_the_record(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)
    fetcher = FakeYahoo()

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01", "--adjusted"], monkeypatch, fetcher)

    assert fetcher.calls[0]["auto_adjust"] is True
    node = yaml.safe_load((root / "sp500.yaml").read_text())["AAPL"]["timeframe_dates"]["1d"]["YF"]
    assert node["adjustment"] == "adjusted"
    # And the stored close is the adjusted one the fake returned.
    stored = pd.read_parquet(catalog.stock_file_path("YF", "sp500", "AAPL", "1d"))
    assert stored["close"].iloc[0] == 2.0


def test_ticker_is_translated_before_the_request(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root, ("BRK.B",))
    fetcher = FakeYahoo()

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, fetcher)

    assert fetcher.calls[0]["ticker"] == "BRK-B"
    # …but the file keeps our spelling.
    assert catalog.stock_file_path("YF", "sp500", "BRK.B", "1d").exists()


def test_indexes_use_the_caret_ticker_and_index_paths(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    (root / "indexes.yaml").write_text(
        yaml.dump({"GSPC": {"name": "GSPC", "categories": ["Index"],
                            "timeframes": [], "timeframe_dates": {}}})
    )
    fetcher = FakeYahoo()

    _run(tool, ["--asset-class", "Index", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, fetcher)

    assert fetcher.calls[0]["ticker"] == "^GSPC"
    assert catalog.index_file_path("YF", "GSPC", "1d").exists()
    node = yaml.safe_load((root / "indexes.yaml").read_text())["GSPC"]["timeframe_dates"]["1d"]["YF"]
    assert node["volume_basis"] == "none"  # index volume isn't meaningful


def test_commodities_write_to_their_own_catalog(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    (root / "commodities.yaml").write_text(
        yaml.dump({"GC=F": {"name": "GC=F", "categories": ["Commodity"],
                            "timeframes": [], "timeframe_dates": {}}})
    )

    _run(tool, ["--asset-class", "Commodity", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, FakeYahoo())

    assert catalog.commodity_file_path("YF", "GC=F", "1d").exists()
    node = yaml.safe_load((root / "commodities.yaml").read_text())["GC=F"]["timeframe_dates"]["1d"]
    assert node["schema"] == "ohlcv"


# --- incremental behaviour ----------------------------------------------------

def test_second_run_only_requests_the_tail(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, FakeYahoo())

    second = FakeYahoo()
    _run(tool, ["--universe", "sp500", "--timeframes", "1d",
                "--overlap-days", "5"], monkeypatch, second)

    assert len(second.calls) == 1
    requested_start = second.calls[0]["start"]
    # The tail window starts ~5 days before the last stored bar, not in 2025.
    assert (datetime.now(tz=timezone.utc) - requested_start) < timedelta(days=400)


def test_merging_is_idempotent(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)
    path = catalog.stock_file_path("YF", "sp500", "AAPL", "1d")

    for _ in range(2):
        _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                    "--since", "2025-01-01"], monkeypatch, FakeYahoo())
        rows = len(pd.read_parquet(path))

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, FakeYahoo())
    assert len(pd.read_parquet(path)) == rows


# --- failure modes ------------------------------------------------------------

def test_empty_response_writes_nothing(yf_env, monkeypatch):
    """An unknown ticker comes back empty rather than erroring — that must not
    create an empty parquet or a catalog node."""
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01"], monkeypatch, FakeYahoo(empty=True))

    assert not catalog.stock_file_path("YF", "sp500", "AAPL", "1d").exists()
    assert yaml.safe_load((root / "sp500.yaml").read_text())["AAPL"]["timeframe_dates"] == {}


def test_dry_run_touches_nothing(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)
    fetcher = FakeYahoo()

    _run(tool, ["--universe", "sp500", "--timeframes", "1d", "--full",
                "--since", "2025-01-01", "--dry-run"], monkeypatch, fetcher)

    assert fetcher.calls == []
    assert not catalog.stock_file_path("YF", "sp500", "AAPL", "1d").exists()


def test_unknown_symbol_is_rejected_before_any_request(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)
    fetcher = FakeYahoo()

    with pytest.raises(SystemExit):
        _run(tool, ["--universe", "sp500", "--symbols", "NOPE",
                    "--timeframes", "1d"], monkeypatch, fetcher)
    assert fetcher.calls == []


def test_timeframe_yahoo_lacks_is_rejected(yf_env, monkeypatch):
    root, catalog, tool = yf_env
    _seed_stock_catalog(root)

    with pytest.raises(SystemExit):
        _run(tool, ["--universe", "sp500", "--timeframes", "4h", "--full",
                    "--since", "2025-01-01"], monkeypatch, FakeYahoo())
