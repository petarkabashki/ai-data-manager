"""The full crypto universe: which pairs count, delisted ones retired, history from listing.

bitget and kucoin answer a request that starts before a pair was listed with
an empty page, bitget fills a since/until window from the until end, and its
futures reject windows over 90 days — every one of these truncated series
silently, so each has a test here.
"""

from __future__ import annotations

import sys
import time
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

import download_crypto as dc  # noqa: E402

from pdm.alerts import Alerter  # noqa: E402
from utils.crypto_universe import (  # noqa: E402
    in_universe,
    listing_date,
    mark_retired,
    published_listing_ms,
)

H = 3_600_000
DAY = 86_400_000


def ms(day: str) -> int:
    return int(datetime.fromisoformat(day).replace(tzinfo=timezone.utc).timestamp() * 1000)


def spot(base, quote="USDT", **kw):
    return {"symbol": f"{base}/{quote}", "base": base, "quote": quote, "spot": True, "active": True, **kw}


def test_the_universe_is_every_quote_without_leveraged_innovation_zone_or_demo_markets():
    assert in_universe(spot("BTC"), "binance", "spot")
    assert in_universe(spot("BTC", "USDC"), "binance", "spot")
    assert in_universe(spot("ETH", "BTC"), "kucoin", "spot")
    assert in_universe(spot("BTC", "TRY"), "binance", "spot")
    assert not in_universe(spot("BTC3L"), "kucoin", "spot")
    assert not in_universe(spot("ETHUP"), "binance", "spot")
    assert not in_universe(spot("OLD", active=False), "binance", "spot")
    assert in_universe(spot("TRX", info={"areaSymbol": "no"}), "bitget", "spot")
    assert not in_universe(spot("RNNNN", info={"areaSymbol": "yes"}), "bitget", "spot")
    swap = {"symbol": "BTC/USDT:USDT", "swap": True, "linear": True, "settle": "USDT", "active": True}
    assert in_universe(swap, "bybit", "futures")
    assert in_universe({**swap, "symbol": "BTC/USDC:USDC", "settle": "USDC"}, "bybit", "futures")
    assert in_universe({**swap, "symbol": "BTC/USD:BTC", "linear": False, "inverse": True, "settle": "BTC"},
                       "bybit", "futures")
    assert not in_universe({**swap, "settle": "SUSDT"}, "bitget", "futures")     # demo trading
    assert not in_universe({**swap, "swap": False, "future": True}, "bybit", "futures")   # dated
    assert not in_universe(spot("BTC"), "bybit", "futures")


def test_published_listing_time():
    assert published_listing_ms({"created": 1_600_000_000_000}) == 1_600_000_000_000
    assert published_listing_ms({"created": None, "info": {"openTime": "1618466640000"}}) == 1618466640000
    assert published_listing_ms({"info": {"openTime": ""}}) is None
    assert published_listing_ms({"info": {"openTime": "0"}}) is None


class FakeVenue:
    """Bars by timeframe; serves a since/until window from the until end, like bitget,
    with until inclusive."""

    def __init__(self, bars: dict[str, list[int]], cap_ms: int | None = None, id="bitget"):
        self.id, self.bars, self.cap_ms, self.calls = id, bars, cap_ms, []
        self.timeframes = {tf: tf for tf in ("1h", "1d", "1w", "1M")}

    def parse8601(self, s):
        return ms(s.replace("T00:00:00Z", ""))

    def fetch_ohlcv(self, pair, tf, since=None, limit=100, params=None):
        until = (params or {}).get("until")
        self.calls.append((tf, since, until))
        if until is None:
            until = since + limit * {"1h": H, "1d": DAY, "1w": 7 * DAY, "1M": 31 * DAY}[tf]
        if self.cap_ms and until - since > self.cap_ms:
            raise RuntimeError("startTime and endTime interval cannot be greater than 90 days")
        picked = [t for t in self.bars[tf] if since <= t <= until][-limit:]
        return [[t, 1.0, 2.0, 0.5, 1.5, 10.0] for t in picked]


@pytest.fixture
def frozen_now(monkeypatch):
    def freeze(when_ms: int):
        monkeypatch.setattr(time, "time", lambda: when_ms / 1000)
        monkeypatch.setattr(time, "sleep", lambda s: None)
    return freeze


def test_windowed_paging_walks_forward_one_page_at_a_time(frozen_now):
    t0 = ms("2026-01-01")
    frozen_now(t0 + 1000 * H + H // 2)                       # bar 1000 is still forming
    venue = FakeVenue({"1h": [t0 + k * H for k in range(1001)]}, cap_ms=90 * DAY)
    df = dc._fetch_ohlcv_paginated(venue, "BTC/USDT:USDT", "1h", t0, H, limit=200, max_window_ms=90 * DAY)
    assert len(df) == 1000 and df["timestamp"].iloc[-1] == t0 + 999 * H
    assert df["timestamp"].diff().max() == H                  # no bar lost at a window edge
    assert all(until - since <= 200 * H for _, since, until in venue.calls)


def test_a_window_wider_than_a_page_is_what_lost_bars():
    """Why the window is sized to one page: bitget returns the bars nearest `until`."""
    t0 = ms("2026-01-01")
    venue = FakeVenue({"1h": [t0 + k * H for k in range(1001)]})
    rows = venue.fetch_ohlcv("X", "1h", since=t0, limit=200, params={"until": t0 + 90 * DAY})
    assert rows[0][0] > t0                                    # the start of the window is gone


def test_weekly_futures_stay_inside_bitgets_90_day_cap(frozen_now):
    t0 = ms("2024-01-01")
    weeks = [t0 + k * 7 * DAY for k in range(150)]
    frozen_now(weeks[-1] + 3 * DAY)
    venue = FakeVenue({"1w": weeks}, cap_ms=90 * DAY)
    df = dc._fetch_ohlcv_paginated(venue, "BTC/USDT:USDT", "1w", t0, 7 * DAY, limit=200, max_window_ms=90 * DAY)
    assert len(df) == 149                                    # every closed week, the forming one dropped
    days = FakeVenue({"1d": [t0 + k * DAY for k in range(400)]}, cap_ms=90 * DAY)
    frozen_now(t0 + 400 * DAY)
    df = dc._fetch_ohlcv_paginated(days, "BTC/USDT:USDT", "1d", t0, DAY, limit=90, max_window_ms=90 * DAY)
    assert len(df) == 400                                     # 90-day windows meet without a gap


def test_monthly_paging_steps_past_each_bar_and_drops_the_forming_month(frozen_now):
    months = [ms(f"{y}-{m:02d}-01") for y in (2024, 2025, 2026) for m in range(1, 13)][:34]   # to 2026-10
    frozen_now(ms("2026-10-15"))
    venue = FakeVenue({"1M": months})
    df = dc._fetch_ohlcv_paginated(venue, "JST/USDT", "1M", ms("2022-01-01"), 31 * DAY,
                                   limit=10, max_window_ms=3650 * DAY)
    assert df["timestamp"].iloc[0] == ms("2024-01-01")
    assert df["timestamp"].iloc[-1] == ms("2026-09-01") and len(df) == 33


def test_listing_date_published_else_first_monthly_bar():
    venue = FakeVenue({"1M": [ms("2023-07-01"), ms("2023-08-01")]})
    assert listing_date(venue, {"symbol": "WLD/USDT", "info": {"openTime": str(ms("2023-07-24"))}}) == "2023-07-24"
    assert listing_date(venue, {"symbol": "WLD/USDT"}) == "2023-07-01"
    assert listing_date(FakeVenue({"1M": []}), {"symbol": "NEW/USDT"}) is None


def test_delisted_pairs_are_retired_not_deleted_and_come_back_on_relisting():
    catalog = {
        "BTC_USDT_kucoin": {"name": "BTC_USDT", "timeframe_dates": {"1d": {"end_date": "2026-10-25"}}},
        "MASA_USDT_kucoin": {"name": "MASA_USDT", "timeframe_dates": {"1d": {"end_date": "2024-05-22"}}},
    }
    assert mark_retired(catalog, {"BTC_USDT"}, "kucoin", "2026-10-26") == (["MASA_USDT"], [])
    masa = catalog["MASA_USDT_kucoin"]
    assert masa["retired"] == "2026-10-26" and masa["retired_reason"] == "not listed on kucoin"
    assert masa["timeframe_dates"]                            # nothing removed
    assert mark_retired(catalog, {"BTC_USDT"}, "kucoin", "2026-11-30") == ([], [])
    assert masa["retired"] == "2026-10-26"                    # first retirement date kept
    assert mark_retired(catalog, {"BTC_USDT", "MASA_USDT"}, "kucoin", "2026-12-01") == ([], ["MASA_USDT"])
    assert "retired" not in masa and "retired_reason" not in masa


def test_routing_sends_only_the_broken_paths_to_ccxt():
    tfs = ["1h", "4h", "1d", "3d", "1w", "1Mo"]
    assert dc.ccxt_timeframes("bitget", "spot", tfs) == ["3d", "1w", "1Mo"]
    assert dc.ccxt_timeframes("bitget", "futures", tfs) == tfs
    assert dc.ccxt_timeframes("kucoin", "futures", tfs) == tfs
    assert dc.ccxt_timeframes("binance", "spot", tfs) == []
    # freqtrade's futures mode is USDT-margined only; the rest goes through ccxt whole.
    assert dc.freqtrade_can("futures", "BTC/USDT:USDT")
    assert not dc.freqtrade_can("futures", "BTC/USDC:USDC")
    assert not dc.freqtrade_can("futures", "BTC/USD:BTC")
    assert dc.freqtrade_can("spot", "ETH/BTC")


def test_all_pairs_skips_retired_entries():
    catalog = {"A": {"name": "BTC_USDT"}, "B": {"name": "MASA_USDT", "retired": "2026-10-26"}}
    args = Namespace(universe=False, all_pairs=True, pairs=None, exchange="kucoin", trading_mode="spot")
    assert dc.select_pairs(args, catalog, is_futures=False)[0] == ["BTC/USDT"]


def test_new_pairs_start_at_listing_and_known_pairs_need_no_request():
    catalog = {"ETH_USDT_kucoin": {"name": "ETH_USDT", "start_date": "2017-10-26",
                                   "timeframe_dates": {"1d": {}}}}
    venue = FakeVenue({"1M": [ms("2023-07-01")]})
    markets = {"WLD/USDT": {"symbol": "WLD/USDT"}, "ETH/USDT": {"symbol": "ETH/USDT"}}
    starts = dc.listing_starts("kucoin", "spot", ["ETH/USDT", "WLD/USDT"], ["1d", "1w"], catalog, markets, venue)
    assert starts == {"ETH/USDT": "2017-10-26", "WLD/USDT": "2023-07-01"}
    assert [c[0] for c in venue.calls] == ["1M"]              # one request, for the new pair only
    assert dc._year_start("2023-07-01") == "2023-01-01"


def test_a_series_with_no_data_starts_at_listing_not_2020(tmp_path, monkeypatch):
    monkeypatch.setattr(dc, "read_parquet_range", lambda path: None)
    assert dc.build_timerange("kucoin", "WLD_USDT", "1d", {}, False, None, False,
                              default_start="2023-07-01") == "20230701-"
    assert dc.build_timerange("kucoin", "WLD_USDT", "1d", {}, False, None, False) == "20170101-"


@pytest.fixture
def status_mod():
    import status
    return status


def test_status_counts_retired_series_apart_from_stale(status_mod):
    catalog = {
        "BTC_USDT_kucoin": {"name": "BTC_USDT", "timeframe_dates": {"1d": {"start_date": "2019-01-01", "end_date": "2020-01-01"}}},
        "MASA_USDT_kucoin": {"name": "MASA_USDT", "retired": "2026-10-26",
                             "timeframe_dates": {"1d": {"start_date": "2024-04-08", "end_date": "2024-05-22"},
                                                 "1h": {"start_date": "2024-04-08", "end_date": "2024-05-22"}}},
    }
    spec = SimpleNamespace(name="crypto_kucoin", asset_class="Crypto", load=lambda: catalog)
    s = status_mod.summarize(spec, stale_days=2, deep=False)
    assert (s.stale, s.retired, s.series) == (1, 2, 3)


def test_the_stale_alert_ignores_retired_series(tmp_path):
    def row(symbol, retired=None):
        return {"catalog": "crypto_kucoin", "asset_class": "Crypto", "symbol": symbol, "timeframe": "1d",
                "provider": "kucoin", "session": None, "end_date": "2024-05-22", "retired": retired}
    alerts = Alerter(lambda *a: None, tmp_path, stale_days=2)
    alerts.check_stale([row("MASA_USDT")])                    # baseline: stale
    assert alerts.check_stale([row("MASA_USDT", retired="2026-10-26")]) == []   # no "recovered" either


def test_series_rows_carry_retired_and_its_reason(monkeypatch):
    """forthkit's resolver prefers a listed series over a retired one, from /v1/series rows."""
    from pdm import index as pdm_index

    catalog = {
        "BTC_USDT_kucoin": {"name": "BTC_USDT", "timeframe_dates": {"1d": {"start_date": "2019-01-01",
                                                                         "end_date": "2020-01-01"}}},
        "MASA_USDT_kucoin": {"name": "MASA_USDT", "retired": "2026-10-26", "retired_reason": "not listed on kucoin",
                             "timeframe_dates": {"1d": {"start_date": "2024-04-08", "end_date": "2024-05-22"}}},
    }
    spec = SimpleNamespace(name="crypto_kucoin", asset_class="Crypto", load=lambda: catalog)
    monkeypatch.setattr(pdm_index, "catalog_registry", lambda: [spec])
    idx = pdm_index.SeriesIndex()
    idx.reload()
    rows = {r["symbol"]: r for r in idx.query()}
    assert (rows["MASA_USDT"]["retired"], rows["MASA_USDT"]["retired_reason"]) == ("2026-10-26", "not listed on kucoin")
    assert (rows["BTC_USDT"]["retired"], rows["BTC_USDT"]["retired_reason"]) == (None, None)
    assert [r["symbol"] for r in idx.query(stale_days=2)] == ["BTC_USDT"]
