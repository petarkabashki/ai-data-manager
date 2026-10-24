"""tools/check_catalog.py — the catalog/disk consistency audit."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture
def catalog_env(tmp_path, monkeypatch):
    """A throwaway data root with utils.* and the checker reloaded against it."""
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in ("utils.config", "utils.catalog", "utils.data", "check_catalog"):
        sys.modules.pop(name, None)

    sys.path.insert(0, str(REPO_ROOT / "tools"))
    catalog = importlib.import_module("utils.catalog")
    checker = importlib.import_module("check_catalog")
    yield root, catalog, checker

    sys.path.remove(str(REPO_ROOT / "tools"))
    for name in ("utils.config", "utils.catalog", "utils.data", "check_catalog"):
        sys.modules.pop(name, None)


def _spec(checker, catalog_mod, name):
    return next(s for s in catalog_mod.catalog_registry() if s.name == name)


def _write_index_yaml(root, entry):
    (root / "indexes.yaml").write_text(yaml.dump({"GSPC": entry}))


def test_registry_covers_every_catalog(catalog_env):
    _, catalog, _ = catalog_env
    names = {s.name for s in catalog.catalog_registry()}
    assert {"crypto_binance", "crypto_futures_bybit", "sp500", "russell2000",
            "indexes", "commodities", "macro"} <= names
    # The two that previously had no CRUD at all.
    assert {"commodities", "macro"} <= names


def test_missing_semantics_are_reported_then_backfilled(catalog_env, ohlcv):
    root, catalog, checker = catalog_env
    path = catalog.index_file_path("YF", "GSPC", "1d")
    catalog.write_parquet_atomic(ohlcv(n=3), path)
    start, end, _ = catalog.read_parquet_range(path)

    _write_index_yaml(root, {
        "name": "GSPC", "provider": "YF", "timeframes": ["1d"],
        "timeframe_dates": {
            "1d": {"YF": {"start_date": start, "end_date": end,
                          "file_path": catalog.relative_to_repo(path)}}
        },
    })
    spec = _spec(checker, catalog, "indexes")

    audit, modified = checker.audit_catalog(spec, fix=False, check_dates=True)
    assert [i.kind for i in audit.issues] == ["semantics"]
    assert not modified

    audit, modified = checker.audit_catalog(spec, fix=True, check_dates=True)
    assert audit.issues[0].fixed and modified

    stored = yaml.safe_load((root / "indexes.yaml").read_text())
    node = stored["GSPC"]["timeframe_dates"]["1d"]["YF"]
    assert node["schema"] == "ohlcv"
    assert node["adjustment"] == "not_applicable"
    assert node["volume_basis"] == "none"


def test_invalid_semantics_are_reported_but_not_silently_rewritten(catalog_env, ohlcv):
    root, catalog, checker = catalog_env
    path = catalog.index_file_path("YF", "GSPC", "1d")
    catalog.write_parquet_atomic(ohlcv(n=3), path)
    start, end, _ = catalog.read_parquet_range(path)

    _write_index_yaml(root, {
        "name": "GSPC", "provider": "YF", "timeframes": ["1d"],
        "timeframe_dates": {
            "1d": {"YF": {
                "start_date": start, "end_date": end,
                "file_path": catalog.relative_to_repo(path),
                "schema": "ohlcv", "adjustment": "who-knows",
                "volume_basis": "none", "bar_label": "open",
            }}
        },
    })
    audit, _ = checker.audit_catalog(_spec(checker, catalog, "indexes"), fix=True, check_dates=True)
    issue = audit.issues[0]
    assert issue.kind == "semantics" and not issue.fixed

    stored = yaml.safe_load((root / "indexes.yaml").read_text())
    assert stored["GSPC"]["timeframe_dates"]["1d"]["YF"]["adjustment"] == "who-knows"


def test_legacy_flat_node_is_detected_and_migrated(catalog_env, ohlcv):
    root, catalog, checker = catalog_env
    path = catalog.index_file_path("YF", "GSPC", "1d")
    catalog.write_parquet_atomic(ohlcv(n=3), path)
    start, end, _ = catalog.read_parquet_range(path)

    _write_index_yaml(root, {
        "name": "GSPC", "provider": "YF", "timeframes": ["1d"],
        "timeframe_dates": {
            "1d": {"start_date": start, "end_date": end,
                   "file_path": catalog.relative_to_repo(path)}
        },
    })
    spec = _spec(checker, catalog, "indexes")

    audit, _ = checker.audit_catalog(spec, fix=False, check_dates=True)
    assert "legacy_shape" in {i.kind for i in audit.issues}

    checker.audit_catalog(spec, fix=True, check_dates=True)
    node = yaml.safe_load((root / "indexes.yaml").read_text())["GSPC"]["timeframe_dates"]["1d"]
    assert set(node) == {"YF"}
    assert node["YF"]["file_path"].endswith("GSPC-1d.parquet")


def test_date_drift_is_detected_and_refreshed(catalog_env, ohlcv):
    root, catalog, checker = catalog_env
    path = catalog.index_file_path("YF", "GSPC", "1d")
    catalog.write_parquet_atomic(ohlcv(n=3), path)

    _write_index_yaml(root, {
        "name": "GSPC", "provider": "YF", "timeframes": ["1d"],
        "timeframe_dates": {
            "1d": {"YF": {
                "start_date": "1999-01-01", "end_date": "1999-12-31",
                "file_path": catalog.relative_to_repo(path),
                "schema": "ohlcv", "adjustment": "not_applicable",
                "volume_basis": "none", "bar_label": "open",
            }}
        },
    })
    spec = _spec(checker, catalog, "indexes")

    audit, _ = checker.audit_catalog(spec, fix=False, check_dates=True)
    assert [i.kind for i in audit.issues] == ["date_drift"]

    checker.audit_catalog(spec, fix=True, check_dates=True)
    node = yaml.safe_load((root / "indexes.yaml").read_text())["GSPC"]["timeframe_dates"]["1d"]["YF"]
    real_start, real_end, _ = catalog.read_parquet_range(path)
    assert (node["start_date"], node["end_date"]) == (real_start, real_end)


def test_missing_file_is_reported_and_never_auto_fixed(catalog_env):
    root, catalog, checker = catalog_env
    _write_index_yaml(root, {
        "name": "GSPC", "provider": "YF", "timeframes": ["1d"],
        "timeframe_dates": {
            "1d": {"YF": {
                "start_date": "2024-01-01", "end_date": "2024-01-03",
                "file_path": "price-data/indexes/yf/GSPC/GSPC-1d.parquet",
                "schema": "ohlcv", "adjustment": "not_applicable",
                "volume_basis": "none", "bar_label": "open",
            }}
        },
    })
    audit, _ = checker.audit_catalog(_spec(checker, catalog, "indexes"), fix=True, check_dates=True)
    issue = audit.issues[0]
    assert issue.kind == "file_missing" and not issue.fixed


def test_rate_series_nodes_skip_the_date_check(catalog_env, tmp_path):
    """Macro isn't OHLCV, so read_parquet_range must never be asked about it."""
    root, catalog, checker = catalog_env
    path = catalog.macro_file_path("YF", "WTB3MS", "1d")
    path.parent.mkdir(parents=True, exist_ok=True)
    import pandas as pd
    pd.DataFrame({"Value": [0.05, 0.06]},
                 index=pd.to_datetime(["2024-01-01", "2024-01-02"])).to_parquet(path)

    (root / "macro.yaml").write_text(yaml.dump({"WTB3MS": {
        "name": "WTB3MS", "provider": "YF", "timeframes": ["1d"],
        "timeframe_dates": {"1d": {
            "start_date": "1954-01-04", "end_date": "2026-09-01",
            "file_path": catalog.relative_to_repo(path), "provider": "YF",
            "schema": "rate_series", "adjustment": "not_applicable",
            "volume_basis": "none", "bar_label": "open",
        }},
    }}))
    audit, _ = checker.audit_catalog(_spec(checker, catalog, "macro"), fix=False, check_dates=True)
    assert audit.issues == []
    assert audit.nodes_checked == 1


def test_orphan_files_are_found(catalog_env, ohlcv):
    root, catalog, checker = catalog_env
    referenced = catalog.index_file_path("YF", "GSPC", "1d")
    orphan = catalog.index_file_path("IBKR", "NDX", "1h")
    for path in (referenced, orphan):
        catalog.write_parquet_atomic(ohlcv(n=2), path)

    orphans = checker.find_orphans({str(referenced.resolve())})
    assert orphans == [orphan]
