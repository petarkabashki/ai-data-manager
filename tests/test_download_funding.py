"""download_funding: perpetual funding settlements next to each contract's bars.

No network: the exchange fetchers are replaced. The layout and columns match
what py-qwant's `scripts/sync_funding_rates.py` wrote, so its readers keep
working against files this tool now owns.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DAY = 86_400_000
H8 = 8 * 3_600_000
T0 = 1_704_067_200_000          # 2024-01-01 00:00 UTC


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Temp data root with a Binance and a Bybit contract; the tool importable."""
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in [m for m in sys.modules if m.startswith("utils.") or m in ("download_funding", "check_schema", "status")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.syspath_prepend(str(REPO_ROOT / "tools"))
    for exchange, key in (("binance", "BTC_USDT"), ("bybit", "BTC_USDT_bybit")):
        bars = f"price-data/crypto-futures/{exchange}/BTC_USDT/BTC_USDT-1d.parquet"
        (root / f"crypto_futures_{exchange}.yaml").write_text(yaml.dump({key: {
            "name": "BTC_USDT", "common_name": "BTC/USDT:USDT", "provider": exchange,
            "categories": ["Crypto Futures"], "timeframes": ["1d"],
            "start_date": "2024-01-01", "end_date": "2024-01-10",
            "timeframe_dates": {"1d": {"start_date": "2024-01-01", "end_date": "2024-01-10",
                                       "file_path": bars, "schema": "ohlcv",
                                       "adjustment": "not_applicable", "volume_basis": "base_asset",
                                       "bar_label": "open"}}}}))
    return root, importlib.import_module("download_funding"), importlib.import_module("utils.catalog")


class FakeVenue:
    """Settlements every 8h from T0 for `days`, stamped with exchange jitter."""

    def __init__(self, days: int = 3, rate: float = 0.0001, fail: set[str] = frozenset()):
        self.days, self.rate, self.fail, self.calls = days, rate, fail, []

    def __call__(self, symbol, start_ms):
        self.calls.append((symbol, start_ms))
        if symbol in self.fail:
            raise RuntimeError("HTTP 400")
        ts = [T0 + i * H8 + 3 for i in range(self.days * 3) if T0 + i * H8 + 3 >= start_ms]
        return pd.DataFrame({"timestamp": ts, "funding_rate": [self.rate] * len(ts),
                             "mark_price": [42_000.0] * len(ts)})


def _run(tool, monkeypatch, argv, venue, exchange="binance"):
    monkeypatch.setitem(tool.FETCH, exchange, venue)
    monkeypatch.setattr(tool.time, "sleep", lambda s: None)
    return tool.main(["--exchange", exchange, *argv])


def _catalog(root, exchange="binance"):
    return yaml.safe_load((root / f"crypto_futures_{exchange}.yaml").read_text())


def test_first_fetch_starts_at_the_first_bar_and_registers_a_funding_node(env, monkeypatch):
    root, tool, catalog = env
    venue = FakeVenue()
    assert _run(tool, monkeypatch, [], venue) == 0

    assert venue.calls == [("BTC_USDT", T0)]
    path = catalog.funding_file_path("binance", "BTC_USDT")
    assert path.name == "BTC_USDT-funding.parquet"
    schema = pq.read_schema(path)
    assert [(f.name, str(f.type)) for f in schema] == [
        ("timestamp", "int64"), ("funding_rate", "double"), ("mark_price", "double")]
    df = pd.read_parquet(path)
    assert len(df) == 9 and (df["timestamp"] % 3_600_000 == 0).all()   # jitter rounded off

    entry = _catalog(root)["BTC_USDT"]
    node = entry["timeframe_dates"]["funding"]
    assert node == {"start_date": "2024-01-01", "end_date": "2024-01-03",
                    "file_path": "price-data/crypto-futures/binance/BTC_USDT/BTC_USDT-funding.parquet",
                    "schema": "funding_rate", "adjustment": "not_applicable",
                    "volume_basis": "none", "bar_label": "close"}
    assert entry["timeframes"] == ["1d"]                                 # bar timeframes only
    assert entry["timeframe_dates"]["1d"]["schema"] == "ohlcv"


def test_rerun_resumes_after_the_last_settlement_and_new_rows_win(env, monkeypatch):
    root, tool, catalog = env
    _run(tool, monkeypatch, [], FakeVenue(days=2))
    venue = FakeVenue(days=4, rate=0.0005)
    assert _run(tool, monkeypatch, [], venue) == 0

    last = T0 + 5 * H8                       # 6 settlements on disk, rounded
    assert venue.calls == [("BTC_USDT", last + 1)]
    df = pd.read_parquet(catalog.funding_file_path("binance", "BTC_USDT"))
    assert len(df) == 12 and df["timestamp"].is_unique and df["timestamp"].is_monotonic_increasing
    # The last stored settlement is re-served (its jittered stamp is past the
    # rounded one) and replaced by the fresh copy: harmless, and never a duplicate.
    assert (df["funding_rate"].iloc[:5] == 0.0001).all()
    assert (df["funding_rate"].iloc[5:] == 0.0005).all()
    assert _catalog(root)["BTC_USDT"]["timeframe_dates"]["funding"]["end_date"] == "2024-01-04"

    venue = FakeVenue(days=4, rate=0.0009)
    _run(tool, monkeypatch, ["--full"], venue)                    # refetch: from the first bar
    assert venue.calls == [("BTC_USDT", T0)]
    df = pd.read_parquet(catalog.funding_file_path("binance", "BTC_USDT"))
    assert len(df) == 12 and (df["funding_rate"] == 0.0009).all()


def test_bybit_uses_its_catalog_key(env, monkeypatch):
    root, tool, catalog = env
    assert _run(tool, monkeypatch, ["--symbols", "BTC/USDT"], FakeVenue(), exchange="bybit") == 0
    assert "funding" in _catalog(root, "bybit")["BTC_USDT_bybit"]["timeframe_dates"]


def test_unknown_symbols_are_refused(env, monkeypatch, capsys):
    _root, tool, _catalog_mod = env
    with pytest.raises(SystemExit) as exc:
        _run(tool, monkeypatch, ["--symbols", "DOGE_USDT"], FakeVenue())
    assert exc.value.code == 2
    assert "not in crypto_futures_binance" in capsys.readouterr().err


def test_dry_run_plans_without_fetching_or_writing(env, monkeypatch, capsys):
    root, tool, catalog = env
    venue = FakeVenue()
    assert _run(tool, monkeypatch, ["--dry-run", "--json", "--since", "2023-06-01"], venue) == 0
    doc = json.loads(capsys.readouterr().out)
    assert venue.calls == []
    assert not catalog.funding_file_path("binance", "BTC_USDT").exists()
    assert doc["dry_run"] is True and doc["results"] == []
    assert doc["plan"] == [{"symbol": "BTC_USDT", "start": "2023-06-01T00:00:00+00:00",
                            "reason": "--since", "rows_on_disk": 0,
                            "file": "price-data/crypto-futures/binance/BTC_USDT/BTC_USDT-funding.parquet"}]


def test_failures_are_reported_and_only_a_total_failure_exits_nonzero(env, monkeypatch, capsys):
    root, tool, _catalog_mod = env
    assert _run(tool, monkeypatch, ["--json"], FakeVenue(fail={"BTC_USDT"})) == 1
    doc = json.loads(capsys.readouterr().out)
    assert doc["failures"] == [{"symbol": "BTC_USDT", "error": "RuntimeError: HTTP 400"}]
    assert "funding" not in _catalog(root)["BTC_USDT"]["timeframe_dates"]


def test_bybit_pages_backwards_down_to_the_start(env, monkeypatch):
    _root, tool, _catalog_mod = env
    settlements = [T0 + i * H8 for i in range(450)]             # newest first, 200 a page
    calls = []

    def fake_get(url, params):
        calls.append(params["endTime"])
        page = [t for t in reversed(settlements) if t <= params["endTime"]][:200]
        return {"result": {"list": [{"fundingRateTimestamp": str(t), "fundingRate": "0.0001"}
                                    for t in page]}}

    monkeypatch.setattr(tool, "_get", fake_get)
    monkeypatch.setattr(tool.time, "sleep", lambda s: None)
    start = settlements[100]
    df = tool.fetch_bybit("BTC_USDT", start)
    assert sorted(df["timestamp"]) == settlements[100:]
    assert df["mark_price"].isna().all()
    assert len(calls) == 2                      # stopped once a page reached the start


def test_checkers_hold_funding_files_to_their_own_schema(env, monkeypatch):
    root, tool, catalog = env
    _run(tool, monkeypatch, [], FakeVenue())
    check_schema = importlib.import_module("check_schema")
    status = importlib.import_module("status")
    good = catalog.funding_file_path("binance", "BTC_USDT")
    assert check_schema.check_file(good) is None
    assert status._schema_ok(good, "funding_rate")

    bad = good.with_name("ETH_USDT-funding.parquet")
    pd.DataFrame({"timestamp": [T0], "funding_rate": [0.1]}).to_parquet(bad, index=False)
    assert "columns" in check_schema.check_file(bad)
    assert not status._schema_ok(bad, "funding_rate")
    assert catalog.read_funding_range(good) == ("2024-01-01", "2024-01-03", 9)
