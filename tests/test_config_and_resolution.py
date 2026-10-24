"""Config resolution and catalog path resolution, with a throwaway data root."""

from __future__ import annotations

import importlib
import sys

import pytest
import yaml

from utils.catalog import write_parquet_atomic


@pytest.fixture
def data_root(tmp_path, monkeypatch):
    """Reload utils.* against a tmp PRICE_DATA_ROOT.

    Proves the repo runs somewhere other than one hardcoded machine, and lets
    the resolution tests build a real (tiny) catalog on disk.
    """
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in ("utils.config", "utils.catalog", "utils.data"):
        sys.modules.pop(name, None)
    importlib.import_module("utils.config")
    importlib.import_module("utils.catalog")
    importlib.import_module("utils.data")
    yield root
    for name in ("utils.config", "utils.catalog", "utils.data"):
        sys.modules.pop(name, None)


def test_data_root_follows_the_environment(data_root):
    from utils.config import ASSETS_YAML, DATA_ROOT

    assert DATA_ROOT == data_root
    assert ASSETS_YAML == data_root / "assets.yaml"


def test_int_env_vars_are_validated(monkeypatch):
    monkeypatch.setenv("IBKR_PORT", "not-a-port")
    sys.modules.pop("utils.config", None)
    with pytest.raises(ValueError, match="IBKR_PORT must be an integer"):
        importlib.import_module("utils.config")
    sys.modules.pop("utils.config", None)


def test_relative_to_repo_survives_an_external_data_root(data_root):
    from utils.config import relative_to_repo, resolve_from_repo

    external = data_root / "cryptos" / "binance" / "BTC_USDT" / "BTC_USDT-1d.parquet"
    stored = relative_to_repo(external)
    # A stored path must be portable: anchored to DATA_ROOT, never to this
    # machine's checkout, so a catalog stays valid on another machine.
    assert stored == "price-data/cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet"
    assert resolve_from_repo(stored) == external


def test_a_symlinked_data_root_still_stores_a_relative_path(tmp_path, monkeypatch):
    """The default layout has `price-data/` as a symlink out of the checkout.

    Resolving it used to push every path outside REPO_ROOT, so `relative_to_repo`
    fell back to absolute and wrote this machine's paths into the catalogs.
    """
    real = tmp_path / "elsewhere" / "price-data"
    real.mkdir(parents=True)
    link = tmp_path / "checkout" / "price-data"
    link.parent.mkdir()
    link.symlink_to(real)

    monkeypatch.setenv("PRICE_DATA_ROOT", str(link))
    sys.modules.pop("utils.config", None)
    config = importlib.import_module("utils.config")
    try:
        target = link / "indexes" / "yf" / "GSPC" / "GSPC-1d.parquet"
        stored = config.relative_to_repo(target)
        assert stored == "price-data/indexes/yf/GSPC/GSPC-1d.parquet"
        assert config.resolve_from_repo(stored) == target
    finally:
        sys.modules.pop("utils.config", None)


def test_absolute_paths_from_older_catalogs_still_resolve(data_root):
    """Catalogs written before the fix carry absolute paths; don't break them."""
    from utils.config import resolve_from_repo

    external = data_root / "cryptos" / "binance" / "BTC_USDT" / "BTC_USDT-1d.parquet"
    assert resolve_from_repo(str(external)) == external


def _build_catalog(root, ohlcv):
    """A minimal assets.yaml + one flat and one nested entry, with real files."""
    flat_file = root / "cryptos" / "binance" / "BTC_USDT" / "BTC_USDT-1d.parquet"
    nested_all = root / "stocks" / "ibkr" / "sp500" / "AAPL" / "AAPL-all-1h.parquet"
    nested_overnight = root / "stocks" / "ibkr" / "sp500" / "AAPL" / "AAPL-overnight-1h.parquet"
    yf_daily = root / "stocks" / "yf" / "sp500" / "AAPL" / "AAPL-1d.parquet"
    for path in (flat_file, nested_all, nested_overnight, yf_daily):
        write_parquet_atomic(ohlcv(n=2), path)

    (root / "crypto_binance.yaml").write_text(
        yaml.dump(
            {
                "BTC_USDT": {
                    "name": "BTC_USDT",
                    "timeframe_dates": {
                        "1d": {"start_date": "2017-08-17", "end_date": "2026-09-01",
                               "file_path": str(flat_file)}
                    },
                }
            }
        )
    )
    (root / "sp500.yaml").write_text(
        yaml.dump(
            {
                "AAPL": {
                    "name": "AAPL",
                    "timeframe_dates": {
                        "1d": {"YF": {"file_path": str(yf_daily)}},
                        "1h": {
                            "IBKR": {"file_path": str(nested_all), "session": "all"},
                            "IBKR:overnight": {"file_path": str(nested_overnight),
                                               "session": "overnight"},
                        },
                    },
                }
            }
        )
    )
    (root / "assets.yaml").write_text(
        yaml.dump({"include": ["crypto_binance.yaml", "sp500.yaml"]})
    )
    return {"flat": flat_file, "all": nested_all, "overnight": nested_overnight, "yf": yf_daily}


def test_asset_path_reads_both_catalog_shapes(data_root, ohlcv):
    from utils.data import asset_path

    files = _build_catalog(data_root, ohlcv)
    assert asset_path("BTC_USDT", "1d") == files["flat"]
    assert asset_path("AAPL", "1d", provider="YF") == files["yf"]


def test_asset_path_selects_by_session(data_root, ohlcv):
    from utils.data import asset_path

    files = _build_catalog(data_root, ohlcv)
    assert asset_path("AAPL", "1h", provider="IBKR", session="all") == files["all"]
    assert asset_path("AAPL", "1h", provider="IBKR", session="overnight") == files["overnight"]


def test_asset_path_provider_matching_is_case_insensitive(data_root, ohlcv):
    from utils.data import asset_path

    files = _build_catalog(data_root, ohlcv)
    assert asset_path("AAPL", "1d", provider="yf") == files["yf"]


def test_asset_path_returns_none_for_unknowns(data_root, ohlcv):
    from utils.data import asset_path

    _build_catalog(data_root, ohlcv)
    assert asset_path("NOPE", "1d") is None
    assert asset_path("AAPL", "7h") is None
    assert asset_path("AAPL", "1d", provider="IBKR") is None


def test_load_assets_merges_every_include(data_root, ohlcv):
    from utils.data import load_assets

    _build_catalog(data_root, ohlcv)
    assets = load_assets()
    assert set(assets) == {"BTC_USDT", "AAPL"}
    assert "include" not in assets
