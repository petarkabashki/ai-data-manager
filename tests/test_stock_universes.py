"""A symbol listed by several stock universes is stored once, under the first one."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from utils import catalog as cat  # noqa: E402
from utils.catalog import STOCK_UNIVERSE_PRIORITY, stock_home_universe  # noqa: E402


def _catalogs(**universes):
    return {name: {s: {} for s in symbols} for name, symbols in universes.items()}


def test_overlapping_symbol_lives_under_the_earlier_universe():
    c = _catalogs(sp500=["AAPL", "MSFT"], nasdaq100=["AAPL", "ARM"])
    assert stock_home_universe("AAPL", "nasdaq100", c) == "sp500"


def test_symbol_only_in_the_later_universe_keeps_its_own():
    c = _catalogs(sp500=["AAPL"], nasdaq100=["AAPL", "ARM"])
    assert stock_home_universe("ARM", "nasdaq100", c) == "nasdaq100"


def test_the_earlier_universe_never_moves_to_a_later_one():
    c = _catalogs(sp500=["AAPL"], nasdaq100=["AAPL"], russell2000=["AAPL"])
    assert stock_home_universe("AAPL", "sp500", c) == "sp500"
    assert stock_home_universe("AAPL", "russell2000", c) == "sp500"


def test_priority_skips_a_universe_that_does_not_list_the_symbol():
    c = _catalogs(sp500=[], nasdaq100=["ARM"], russell2000=["ARM"])
    assert stock_home_universe("ARM", "russell2000", c) == "nasdaq100"


def test_unranked_universe_is_left_alone():
    assert stock_home_universe("AAPL", "my_custom_list", _catalogs(sp500=["AAPL"])) == "my_custom_list"


def test_catalogs_are_loaded_once_and_cached(monkeypatch):
    loaded = []
    monkeypatch.setattr(cat, "load_stock_catalog", lambda u: loaded.append(u) or {"AAPL": {}})
    cache: dict = {}
    assert stock_home_universe("AAPL", "nasdaq100", cache) == "sp500"
    assert stock_home_universe("AAPL", "nasdaq100", cache) == "sp500"
    assert loaded == ["sp500"]


def test_every_downloadable_universe_is_ranked():
    import download_ibkr_stock_overnight as tool

    assert set(tool.UNIVERSE_CHOICES) <= set(STOCK_UNIVERSE_PRIORITY)
