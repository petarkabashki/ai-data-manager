"""bitget spot history repair: the paged route, the hole fill, the exchange-empty record.

The fake exchange behaves as bitget was seen to: a page is capped (200 bars, 100
at 3d/1w/1Mo), a larger limit is refused with code 40020, and a window is filled
from its `until` end, inclusive. So a window wider than a page, or a cursor that
steps wrongly, leaves real holes in the result and fails these tests.
"""

from __future__ import annotations

import importlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import ccxt
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

H = 3_600_000
DAY = 86_400_000
BAR = {"1h": H, "4h": 4 * H, "6h": 6 * H, "12h": 12 * H, "1d": DAY, "3d": 3 * DAY, "1w": 7 * DAY}
PAGE = {"3d": 100, "1w": 100, "1M": 100}


def ms(date: str) -> int:
    return int(datetime.fromisoformat(date).replace(tzinfo=timezone.utc).timestamp() * 1000)


class CappedBitget:
    id = "bitget"

    def __init__(self, bars: dict[str, list[int]], now_ms: int):
        self.bars, self.now_ms, self.calls = bars, now_ms, []
        self.timeframes = dict.fromkeys(["1h", "4h", "6h", "12h", "1d", "3d", "1w", "1M"], "x")
        self.fail_on_call: int | None = None
        self.error: Exception = ccxt.NetworkError("connection reset")

    def fetch_ohlcv(self, pair, tf, since=None, limit=100, params=None):
        self.calls.append((pair, tf, since, (params or {}).get("until")))
        if self.fail_on_call is not None and len(self.calls) == self.fail_on_call:
            raise self.error
        if limit > PAGE.get(tf, 200):
            raise ccxt.BadRequest('bitget {"code":"40020","msg":"Parameter limit error"}')
        until = min((params or {}).get("until", self.now_ms), self.now_ms)
        served = [t for t in self.bars.get(tf, []) if t <= until][-limit:]
        return [[t, 1.0, 2.0, 0.5, 1.5, 10.0] for t in served if t >= since]


@pytest.fixture
def env(tmp_path, monkeypatch):
    """Fresh modules against a tmp data root (the suite's re-import convention)."""
    monkeypatch.setenv("PRICE_DATA_ROOT", str(tmp_path))
    tools = ("download_crypto", "fill_crypto_holes", "check_gaps")
    for name in [m for m in sys.modules if m.startswith("utils.") or m in tools]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    return importlib.import_module("fill_crypto_holes")


def freeze(monkeypatch, now_ms: int) -> int:
    monkeypatch.setattr(time, "time", lambda: now_ms / 1000)
    return now_ms


def store(fill, symbol: str, tf: str, bars: list[int], exchange: str = "bitget") -> Path:
    """A stored series and its catalog node."""
    from utils.catalog import crypto_file_path, load_crypto_catalog, read_parquet_range, \
        save_crypto_catalog, update_catalog_entry

    path = crypto_file_path(exchange, symbol, tf)
    frame = pd.DataFrame({"timestamp": bars, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.0, "volume": 5.0})
    fill.merge_ohlcv_parquet(frame, path)
    catalog = load_crypto_catalog(exchange)
    start, end, _ = read_parquet_range(path)
    update_catalog_entry(catalog, exchange, symbol, tf, path, start, end)
    save_crypto_catalog(exchange, catalog)
    return path


def stored_bars(path: Path) -> list[int]:
    return pd.read_parquet(path, columns=["timestamp"])["timestamp"].tolist()


def run(fill, monkeypatch, venue, *argv) -> int:
    monkeypatch.setattr(fill, "ccxt_exchange", lambda *a: venue)
    return fill.main(["--exchange", "bitget", *argv])


# -- the route -----------------------------------------------------------------


def test_every_bitget_spot_timeframe_takes_ccxt_at_its_own_page_size(env):
    import download_crypto as dc

    tfs = ["1h", "4h", "6h", "12h", "1d", "3d", "1w", "1Mo"]
    assert dc.ccxt_timeframes("bitget", "spot", tfs) == tfs
    assert [dc.ccxt_paging("bitget", "spot", tf)[0] for tf in tfs] == [200] * 5 + [100] * 3
    assert dc.ccxt_paging("binance", "futures", "1h") == (1000, None)


@pytest.mark.parametrize("tf", ["1h", "4h", "6h", "12h", "1d", "3d", "1w"])
def test_the_paged_path_loses_nothing_at_any_page_edge(env, monkeypatch, tf):
    import download_crypto as dc

    t0, step = ms("2015-01-05"), BAR[tf]             # a Monday, as bitget's weeks are
    if tf == "1w":
        t0 = ms("2008-01-07")
    bars = [t0 + k * step for k in range(1001)]
    now = freeze(monkeypatch, bars[-1] + step // 2)   # the last bar is still forming
    venue = CappedBitget({tf: bars}, now)
    limit, window = dc.ccxt_paging("bitget", "spot", tf)
    df = dc._fetch_ohlcv_paginated(venue, "DOGE/USDT", tf, t0, step, limit=limit, max_window_ms=window)
    assert df["timestamp"].tolist() == bars[:-1]
    assert len(venue.calls) == 1001 // limit + 1      # the last holds the forming bar


def test_a_monthly_window_is_never_wider_than_a_page(env, monkeypatch):
    """100 × the 31-day upper bound spans 102 months, so the oldest were dropped."""
    import download_crypto as dc

    months = [ms(f"{y}-{m:02d}-01") for y in range(2010, 2027) for m in range(1, 13)]
    now = freeze(monkeypatch, ms("2026-11-06"))
    months = [m for m in months if m <= now]
    venue = CappedBitget({"1M": months}, now)
    limit, window = dc.ccxt_paging("bitget", "spot", "1Mo")
    df = dc._fetch_ohlcv_paginated(venue, "BTC/USDT", "1M", months[0], 31 * DAY, limit=limit, max_window_ms=window)
    assert df["timestamp"].tolist() == months[:-1]    # all but the forming month


def test_nightly_new_series_start_at_listing_and_old_ones_resume(env, monkeypatch):
    import download_crypto as dc
    from utils.catalog import crypto_file_path, load_crypto_catalog

    listed = ms("2026-01-05")
    now = freeze(monkeypatch, ms("2026-11-06") + H // 2)
    hours = list(range(listed, now, H))               # the last is still forming
    days = [t for t in hours if t % DAY == 0]
    venue = CappedBitget({"1h": hours, "1d": days}, now)
    old = store(env, "AAA_USDT", "1h", hours[:-4])
    catalog = load_crypto_catalog("bitget")

    dc.download_and_merge_ccxt("bitget", ["AAA/USDT"], ["1h"], catalog, trading_mode="spot",
                               starts={}, ccxt_ex=venue)
    assert len(venue.calls) == 1                      # the tail only
    assert venue.calls[0][2] == hours[-6]             # one bar of overlap
    assert stored_bars(old) == hours[:-1]

    venue.calls.clear()
    dc.download_and_merge_ccxt("bitget", ["NEW/USDT"], ["1d"], catalog, trading_mode="spot",
                               starts={"NEW/USDT": "2026-01-05"}, ccxt_ex=venue)
    assert venue.calls[0][2] == listed
    assert stored_bars(crypto_file_path("bitget", "NEW_USDT", "1d")) == days[:-1]


def test_until_also_bounds_an_exchange_paged_without_windows(env, monkeypatch):
    """binance spot takes no `until`: it serves from `since`, past the end of a hole."""
    import download_crypto as dc

    t0 = ms("2024-01-01")
    now = freeze(monkeypatch, t0 + 1000 * H)

    class SinceFirst(CappedBitget):
        def fetch_ohlcv(self, pair, tf, since=None, limit=100, params=None):
            self.calls.append((pair, tf, since, None))
            return [[t, 1.0, 2.0, 0.5, 1.5, 10.0] for t in self.bars[tf] if t >= since][:limit]

    venue = SinceFirst({"1h": [t0 + k * H for k in range(1000)]}, now)
    df = dc._fetch_ohlcv_paginated(venue, "A/USDT", "1h", t0 + 10 * H, H, limit=200, until_ms=t0 + 20 * H - 1)
    assert df["timestamp"].tolist() == [t0 + k * H for k in range(10, 20)]
    assert len(venue.calls) == 1


def test_a_nightly_error_mid_walk_merges_nothing_so_it_leaves_no_hole(env, monkeypatch):
    import download_crypto as dc
    from utils.catalog import load_crypto_catalog

    t0 = ms("2026-01-05")
    now = freeze(monkeypatch, t0 + 1000 * H)
    hours = [t0 + k * H for k in range(1000)]
    path = store(env, "AAA_USDT", "1h", hours[:100])
    venue = CappedBitget({"1h": hours}, now)
    venue.fail_on_call = 3
    dc.download_and_merge_ccxt("bitget", ["AAA/USDT"], ["1h"], load_crypto_catalog("bitget"),
                               trading_mode="spot", ccxt_ex=venue)
    assert stored_bars(path) == hours[:100]


# -- finding holes ------------------------------------------------------------


def holed(start: int, step: int, kept: int, missing: int, cycles: int) -> tuple[list[int], list[int]]:
    """DOGE's shape: `kept` bars, then `missing`, repeating. (stored, everything)."""
    everything = [start + k * step for k in range((kept + missing) * cycles + kept)]
    stored = [t for k, t in enumerate(everything) if k % (kept + missing) < kept]
    return stored, everything


def test_holes_come_from_the_parquet_not_the_catalog_dates(env, monkeypatch):
    stored, _ = holed(ms("2024-01-01"), H, 200, 300, 2)
    store(env, "DOGE_USDT", "1h", stored)             # the catalog says start..end, no holes
    from utils.catalog import load_crypto_catalog

    holes = env.find_holes("bitget", "spot", ["1h"], load_crypto_catalog("bitget"), env.ExchangeEmptyRecord())
    assert [(h.candles, h.pair) for h in holes] == [(300, "DOGE/USDT")] * 2
    assert holes[0].start_ms == stored[199] + H and holes[0].end_ms == stored[200] - H
    assert sum(h.requests("bitget", "spot") for h in holes) == 4


def test_weekly_holes_are_counted_from_the_stored_mondays(env):
    """bitget's weeks open on Monday; on the epoch's Thursday grid a 2-bar hole read as 3."""
    from utils.calendars import CONTINUOUS
    from utils.gaps import Gap, detect_gaps

    monday = ms("2024-01-01")
    weeks = [monday + k * 7 * DAY for k in (0, 1, 4, 5)]
    path = store(env, "BTC_USDT", "1w", weeks)
    assert detect_gaps(path, "1w", CONTINUOUS)[0] == [Gap(monday + 14 * DAY, monday + 21 * DAY, 2)]


def test_min_candles_and_retired_pairs_are_left_out(env):
    from utils.catalog import load_crypto_catalog, save_crypto_catalog

    t0 = ms("2024-01-01")
    store(env, "AAA_USDT", "1h", [t0, t0 + 3 * H, t0 + 4 * H, t0 + 400 * H])
    store(env, "OLD_USDT", "1h", [t0, t0 + 400 * H])
    catalog = load_crypto_catalog("bitget")
    catalog["OLD_USDT_bitget"]["retired"] = "2026-01-01"
    save_crypto_catalog("bitget", catalog)
    holes = env.find_holes("bitget", "spot", ["1h"], load_crypto_catalog("bitget"),
                           env.ExchangeEmptyRecord(), min_candles=10)
    assert [(h.symbol, h.candles) for h in holes] == [("AAA_USDT", 395)]


# -- the fill -----------------------------------------------------------------


def test_plan_only_writes_nothing_and_never_reaches_the_exchange(env, monkeypatch, capsys):
    from utils.catalog import catalog_path
    from utils.exchange_empty import record_path

    stored, _ = holed(ms("2024-01-01"), H, 200, 300, 2)
    path = store(env, "DOGE_USDT", "1h", stored)
    before = path.read_bytes(), catalog_path("bitget").read_bytes()

    def no_network(*a):
        raise AssertionError("plan-only must not build an exchange")

    monkeypatch.setattr(env, "ccxt_exchange", no_network)
    assert env.main(["--exchange", "bitget", "--timeframes", "1h"]) == 0
    assert (path.read_bytes(), catalog_path("bitget").read_bytes()) == before
    assert not record_path().exists()
    out = capsys.readouterr().out
    assert "Plan only" in out and "600" in out


@pytest.mark.parametrize("tf,kept,missing", [("1h", 200, 300), ("1d", 200, 300), ("1h", 201, 700),
                                             ("4h", 100, 527), ("3d", 50, 201), ("1w", 40, 250)])
def test_a_fill_fills_at_every_page_size(env, monkeypatch, tf, kept, missing):
    start = ms("2005-01-03") if tf == "1w" else ms("2015-01-05")     # Mondays
    stored, everything = holed(start, BAR[tf], kept, missing, 3)
    now = freeze(monkeypatch, everything[-1] + 2 * BAR[tf])
    path = store(env, "DOGE_USDT", tf, stored)
    venue = CappedBitget({tf: everything}, now)

    assert run(env, monkeypatch, venue, "--timeframes", tf, "--apply") == 0
    assert stored_bars(path) == everything
    # Only the holes were asked for, and each request fits one page.
    assert all(pair == "DOGE/USDT" and since >= everything[kept - 1] for pair, _, since, _ in venue.calls)
    page = PAGE.get(tf, 200)
    assert all((until - since + 1) // BAR[tf] <= page for *_, since, until in venue.calls)
    assert len(venue.calls) == 3 * -(-missing // page)


def test_exchange_empty_is_recorded_and_not_retried(env, monkeypatch, capsys):
    from utils.exchange_empty import ExchangeEmptyRecord, record_path

    t0 = ms("2024-01-01")
    stored = [t0 + k * H for k in range(10)] + [t0 + k * H for k in range(510, 520)]
    now = freeze(monkeypatch, ms("2026-11-06"))
    path = store(env, "ARX_USDT", "1h", stored)
    venue = CappedBitget({"1h": stored}, now)         # the exchange holds no more than we do

    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 0
    assert stored_bars(path) == stored
    assert len(venue.calls) == 3                      # 500 empty bars, three 200-bar windows
    assert record_path().parent == path.parents[3]    # beside the catalogs, not in cryptos/
    [window] = ExchangeEmptyRecord().windows(path)
    assert (window["start_ms"], window["end_ms"], window["candles"]) == (stored[9] + H, stored[10] - H, 500)

    venue.calls.clear()
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 0
    assert venue.calls == []
    assert "Nothing to fill" in capsys.readouterr().out


def test_a_partly_filled_window_records_only_what_stayed_empty(env, monkeypatch):
    from utils.exchange_empty import ExchangeEmptyRecord

    t0 = ms("2024-01-01")
    everything = [t0 + k * H for k in range(1000)]
    stored = everything[:100] + everything[900:]
    served = everything[:100] + everything[300:500] + everything[900:]
    now = freeze(monkeypatch, ms("2026-11-06"))
    path = store(env, "AAA_USDT", "1h", stored)

    assert run(env, monkeypatch, CappedBitget({"1h": served}, now), "--timeframes", "1h", "--apply") == 0
    assert stored_bars(path) == served
    windows = [(w["start_ms"], w["end_ms"], w["candles"]) for w in ExchangeEmptyRecord().windows(path)]
    assert windows == [(everything[100], everything[299], 200), (everything[500], everything[899], 400)]


@pytest.mark.parametrize("error", [ccxt.NetworkError("reset"), ccxt.RateLimitExceeded("429"),
                                   ccxt.BadRequest("40020")])
def test_an_error_is_not_exchange_empty_and_the_hole_is_retried(env, monkeypatch, error):
    from utils.exchange_empty import ExchangeEmptyRecord, record_path

    stored, everything = holed(ms("2024-01-01"), H, 200, 500, 1)
    now = freeze(monkeypatch, ms("2026-11-06"))
    path = store(env, "AAA_USDT", "1h", stored)
    venue = CappedBitget({"1h": everything}, now)
    venue.fail_on_call, venue.error = 2, error        # after one good page of three

    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 1
    assert not record_path().exists() or ExchangeEmptyRecord().windows(path) == []
    assert stored_bars(path) == stored

    venue.fail_on_call = None
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 0     # resumes
    assert stored_bars(path) == everything


def test_an_outage_stops_the_run_instead_of_working_through_the_plan(env, monkeypatch):
    from utils.exchange_empty import record_path

    now = freeze(monkeypatch, ms("2026-11-06"))
    t0 = ms("2024-01-01")
    for k in range(env.MAX_CONSECUTIVE_ERRORS + 5):
        store(env, f"A{k}_USDT", "1h", [t0, t0 + 3 * H])

    class Down(CappedBitget):
        def fetch_ohlcv(self, *a, **kw):
            self.calls.append(a)
            raise ccxt.ExchangeNotAvailable("503")

    venue = Down({}, now)
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 1
    assert len(venue.calls) == env.MAX_CONSECUTIVE_ERRORS
    assert not record_path().exists()


def test_bars_off_the_stored_grid_are_neither_merged_nor_recorded(env, monkeypatch):
    """The window isn't empty, so recording it would hide a real difference."""
    from utils.exchange_empty import record_path

    t0 = ms("2024-01-02")
    stored = [t0, t0 + 30 * DAY]
    now = freeze(monkeypatch, ms("2026-11-06"))
    path = store(env, "AAA_USDT", "3d", stored)
    served = [t0 + DAY + k * 3 * DAY for k in range(9)]
    assert run(env, monkeypatch, CappedBitget({"3d": served}, now), "--timeframes", "3d", "--apply") == 1
    assert stored_bars(path) == stored
    assert not record_path().exists()


def test_limit_takes_a_small_trial_and_the_next_run_resumes(env, monkeypatch):
    stored, everything = holed(ms("2024-01-01"), H, 200, 300, 3)
    now = freeze(monkeypatch, ms("2026-11-06"))
    path = store(env, "DOGE_USDT", "1h", stored)
    venue = CappedBitget({"1h": everything}, now)

    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--limit", "1", "--apply") == 0
    assert len(stored_bars(path)) == len(stored) + 300 and len(venue.calls) == 2
    venue.calls.clear()
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 0
    assert stored_bars(path) == everything and len(venue.calls) == 4


def test_catalog_dates_follow_the_file_only_when_they_moved(env, monkeypatch):
    from utils.catalog import catalog_path, load_crypto_catalog, read_parquet_range, save_crypto_catalog

    stored, everything = holed(ms("2024-01-01"), H, 200, 300, 1)
    now = freeze(monkeypatch, ms("2026-11-06"))
    aaa = store(env, "AAA_USDT", "1h", stored)
    store(env, "BBB_USDT", "1h", stored)
    catalog = load_crypto_catalog("bitget")
    catalog["AAA_USDT_bitget"]["timeframe_dates"]["1h"]["end_date"] = "2023-12-31"    # stale
    save_crypto_catalog("bitget", catalog)

    venue = CappedBitget({"1h": everything}, now)
    untouched = catalog_path("bitget").read_bytes()
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--symbols", "BBB_USDT", "--apply") == 0
    assert catalog_path("bitget").read_bytes() == untouched
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--symbols", "AAA_USDT", "--apply") == 0
    node = load_crypto_catalog("bitget")["AAA_USDT_bitget"]["timeframe_dates"]["1h"]
    assert node["end_date"] == read_parquet_range(aaa)[1] != "2023-12-31"


# -- check_gaps ---------------------------------------------------------------


def test_check_gaps_reports_the_record_apart_and_not_as_failures(env, monkeypatch, capsys):
    from utils.exchange_empty import ExchangeEmptyRecord
    from utils.gaps import audit_exchange, honour_exchange_empty

    t0 = ms("2024-01-01")
    bars = [t0 + k * H for k in (0, 1, 102, 103, 204, 205)]
    now = freeze(monkeypatch, bars[-1] + H)
    monkeypatch.setattr("utils.gaps._now_ms", lambda: now)
    path = store(env, "ARX_USDT", "1h", bars)
    record = ExchangeEmptyRecord()
    record.add(path, t0 + 2 * H, t0 + 101 * H, 100)         # the whole first gap
    record.add(path, t0 + 104 * H, t0 + 153 * H, 50)        # half of the second
    record.save()

    audit = audit_exchange("bitget", timeframes=["1h"])[0]
    assert [g.missing_candles for g in audit.internal_gaps] == [100, 100]
    honour_exchange_empty([audit])
    assert [(g.start_ms, g.missing_candles) for g in audit.internal_gaps] == [(t0 + 154 * H, 50)]
    assert [g.missing_candles for g in audit.exchange_empty] == [100, 50]

    audit.internal_gaps = []                                # only the record is left
    assert audit.ok

    import check_gaps

    monkeypatch.setattr(sys, "argv", ["check_gaps", "--exchange", "bitget", "--timeframe", "1h", "--json"])
    check_gaps.main()
    [series] = json.loads(capsys.readouterr().out)["series"]
    assert [g["missing_candles"] for g in series["internal_gaps"]] == [50]
    assert [g["missing_candles"] for g in series["exchange_empty"]] == [100, 50]

    monkeypatch.setattr(sys, "argv", ["check_gaps", "--exchange", "bitget", "--timeframe", "1h", "--internal"])
    check_gaps.main()
    out = capsys.readouterr().out
    assert "EXCHANGE-EMPTY" in out and "150 candles" in out and "Internal gaps : 1" in out


def test_check_gaps_passes_a_series_whose_holes_are_all_on_record(env, monkeypatch):
    """End to end: the fill records, the audit then has nothing to report."""
    from utils.gaps import audit_exchange, honour_exchange_empty

    t0 = ms("2024-01-01")
    bars = [t0, t0 + H, t0 + 300 * H, t0 + 301 * H]
    now = freeze(monkeypatch, bars[-1] + H)
    monkeypatch.setattr("utils.gaps._now_ms", lambda: now)
    store(env, "ARX_USDT", "1h", bars)
    venue = CappedBitget({"1h": bars}, bars[-1] + H)
    assert run(env, monkeypatch, venue, "--timeframes", "1h", "--apply") == 0
    audits = audit_exchange("bitget", timeframes=["1h"])
    honour_exchange_empty(audits)
    assert audits[0].ok and audits[0].exchange_empty[0].missing_candles == 298


def test_the_fill_runs_in_the_crypto_lane():
    from pdm.jobs import TOOL_LANES

    assert TOOL_LANES["fill_crypto_holes"] == TOOL_LANES["download_crypto"]
