"""PDM server/client: protocol, watcher, jobs, scheduler, live, and a full
round trip over localhost TCP. No network beyond 127.0.0.1, no price-data/."""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import textwrap
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
import yaml

from pdm.jobs import JobManager, validate
from pdm.config_store import ConfigStore
from pdm.live import LiveManager, Subscription, ccxt_symbol, our_symbol
from pdm.refresh import PlanError, build_argv
from pdm.protocol import arrow_to_frame, bar_topic, frame_to_arrow, parse_series_path
from pdm.scheduler import Schedule, Scheduler, parse_interval, schedule_targets
from pdm.watch import DataWatcher, last_timestamp
from utils.catalog import write_parquet_atomic

DAY = 86_400_000


# --- protocol -------------------------------------------------------------------

def test_arrow_round_trip_keeps_canonical_dtypes(ohlcv):
    df = ohlcv(0, 10)
    back = arrow_to_frame(frame_to_arrow(df))
    pd.testing.assert_frame_equal(back, df)
    assert back["timestamp"].dtype == "int64"


@pytest.mark.parametrize(
    "path, topic",
    [
        ("stocks/yf/sp500/AAPL/AAPL-1d.parquet", "bar/stocks/yf/sp500/AAPL/1d"),
        ("stocks/ibkr/sp500/AAPL/AAPL-all-1h.parquet", "bar/stocks/ibkr/sp500/AAPL/all-1h"),
        ("stocks/yf/sp500/BRK.B/BRK.B-1d.parquet", "bar/stocks/yf/sp500/BRK.B/1d"),
        ("cryptos/binance/BTC_USDT/BTC_USDT-5m.parquet", "bar/cryptos/binance/BTC_USDT/5m"),
        ("indexes/yf/GSPC/GSPC-1d.parquet", "bar/indexes/yf/GSPC/1d"),
    ],
)
def test_bar_topic_mirrors_the_storage_path(path, topic):
    assert bar_topic(path) == topic


def test_topic_prefix_with_slash_does_not_leak_across_symbols():
    """`BRK` must not select `BRK.B` — the reason topics use `/`, not `.`."""
    brk_b = bar_topic("stocks/yf/sp500/BRK.B/BRK.B-1d.parquet")
    assert not brk_b.startswith("bar/stocks/yf/sp500/BRK/")


def test_parse_series_path_reads_session_and_universe():
    meta = parse_series_path("stocks/ibkr/nasdaq100/AAPL/AAPL-overnight-1h.parquet")
    assert meta["provider"] == "ibkr" and meta["universe"] == "nasdaq100"
    assert meta["session"] == "overnight" and meta["timeframe"] == "1h"
    meta = parse_series_path("cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet")
    assert meta["universe"] is None and meta["session"] is None and meta["symbol"] == "BTC_USDT"


# --- watcher --------------------------------------------------------------------

def _series(root: Path, rel: str, df) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    write_parquet_atomic(df, path)
    return path


def test_watcher_reports_only_rows_newer_than_last_seen(tmp_path, ohlcv):
    rel = "cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet"
    path = _series(tmp_path, rel, ohlcv(0, 5))
    w = DataWatcher(tmp_path, tmp_path / "state.json")
    assert w.prime() == 1
    assert last_timestamp(path) == 4 * DAY
    assert w.scan_changes() == []

    time.sleep(0.01)
    write_parquet_atomic(ohlcv(0, 8), path)  # three new days appended
    (update,) = w.scan_changes()
    assert update.path == rel and not update.new_file
    assert update.frame["timestamp"].tolist() == [5 * DAY, 6 * DAY, 7 * DAY]
    assert w.scan_changes() == []


def test_watcher_truncates_large_updates_and_flags_new_files(tmp_path, ohlcv):
    w = DataWatcher(tmp_path, None, max_rows=10)
    w.prime()
    _series(tmp_path, "stocks/yf/sp500/AAPL/AAPL-1d.parquet", ohlcv(0, 50))
    (update,) = w.scan_changes()
    assert update.new_file and update.truncated
    assert update.total_new_rows == 50 and len(update.frame) == 10
    assert update.frame["timestamp"].iloc[-1] == 49 * DAY  # keeps the newest rows


def test_watcher_ignores_non_ohlcv_dirs(tmp_path, ohlcv):
    _series(tmp_path, "market-cap/coingecko/history/x.parquet", ohlcv(0, 3))
    w = DataWatcher(tmp_path, None)
    w.prime()
    assert w.series_count() == 0


def test_watcher_state_survives_restart(tmp_path, ohlcv):
    _series(tmp_path, "cryptos/binance/ETH_USDT/ETH_USDT-1d.parquet", ohlcv(0, 5))
    state = tmp_path / "state.json"
    DataWatcher(tmp_path, state).prime()
    assert DataWatcher(tmp_path, state).prime() == 0  # nothing re-read


# --- jobs -----------------------------------------------------------------------

def test_only_whitelisted_tools_run():
    assert validate("download_yf.py", ["--limit", 2]) == ("download_yf", ["--limit", "2"])
    with pytest.raises(ValueError, match="not runnable"):
        validate("rm", ["-rf", "/"])
    with pytest.raises(ValueError, match="list of strings"):
        validate("status", "--json; rm -rf /")


def test_yf_and_ibkr_share_a_lane():
    """Both write sp500.yaml; concurrent load-edit-save would lose updates."""
    from pdm.jobs import TOOL_LANES

    assert TOOL_LANES["download_yf"] == TOOL_LANES["download_ibkr_stock_overnight"]
    assert TOOL_LANES["download_yf"] == TOOL_LANES["download_ibkr_index"]


def test_a_provider_tool_runs_in_its_providers_lane():
    """fix_daily_stamps rewrites the files that provider's downloader writes."""
    from pdm.jobs import TOOL_LANES, lane_for

    assert lane_for("fix_daily_stamps", ["--provider", "bitget"]) == TOOL_LANES["download_crypto"]
    assert lane_for("fix_daily_stamps", ["--provider=deribit"]) == TOOL_LANES["download_deribit_options"]
    assert lane_for("fix_daily_stamps", ["--provider", "yf"]) == TOOL_LANES["download_yf"]
    assert lane_for("download_yf", ["--provider", "bitget"]) == TOOL_LANES["download_yf"]


def _fake_tools(tmp_path: Path) -> Path:
    tools = tmp_path / "tools"
    tools.mkdir(parents=True)
    (tools / "status.py").write_text("import sys; print('ok', sys.argv[1:])\n")
    (tools / "check_gaps.py").write_text("import sys; sys.exit(3)\n")
    (tools / "download_yf.py").write_text("import time; time.sleep(30)\n")
    return tools


def _wait_state(jm, job_id, states, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if jm.get(job_id).state in states:
            return jm.get(job_id)
        time.sleep(0.05)
    raise AssertionError(f"job stuck in {jm.get(job_id).state}")


def test_job_runs_logs_and_reports_exit_codes(tmp_path):
    events = []
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs", on_event=lambda j: events.append(j.state))
    ok = jm.submit("status", ["--json"])
    bad = jm.submit("check_gaps", [])
    assert _wait_state(jm, ok.id, {"succeeded"}).returncode == 0
    assert _wait_state(jm, bad.id, {"failed"}).returncode == 3
    assert "ok ['--json']" in jm.log_tail(ok.id)
    assert {"queued", "running", "succeeded", "failed"} <= set(events)
    jm.stop()


def test_cancel_queued_and_running_jobs(tmp_path):
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs")
    first = jm.submit("download_yf", [])
    second = jm.submit("download_yf", [])  # same lane: waits behind the first
    _wait_state(jm, first.id, {"running"})
    assert jm.cancel(second.id).state == "cancelled"
    jm.cancel(first.id)
    assert _wait_state(jm, first.id, {"cancelled"}).state == "cancelled"
    jm.stop()


def test_unfinished_jobs_survive_a_restart_in_order(tmp_path):
    tools, state = _fake_tools(tmp_path), tmp_path / "pdm_jobs.json"
    jm = JobManager(tools, tmp_path / "logs", state_path=state)
    done = jm.submit("status", [])
    _wait_state(jm, done.id, {"succeeded"})
    running = jm.submit("download_yf", ["--universe", "sp500"])
    waiting = jm.submit("download_yf", ["--universe", "nasdaq100"])
    _wait_state(jm, running.id, {"running"})
    jm.stop()
    assert jm.get(running.id).state == "interrupted"     # not "failed": the tool never finished

    again = JobManager(tools, tmp_path / "logs", state_path=state)
    assert {j["id"] for j in again.list()} == {done.id, waiting.id, running.id}
    assert again.get(done.id).state == "succeeded"       # a finished job's outcome survives too
    first, second = again.get(running.id), again.get(waiting.id)
    assert first.state == second.state == "queued"
    assert (first.requeued, second.requeued) == (1, 0)
    assert first.started_at is None and first.args == ["--universe", "sp500"]
    time.sleep(0.3)
    assert first.state == "queued"                       # nothing runs before resume()
    again.resume()
    _wait_state(again, running.id, {"running"})          # the interrupted run goes first
    assert again.get(waiting.id).state == "queued"
    again.cancel(waiting.id)
    again.cancel(running.id)
    _wait_state(again, running.id, {"cancelled"})
    assert {j["state"] for j in json.loads(state.read_text())["jobs"]} == {"succeeded", "cancelled"}
    again.stop()


def test_only_the_newest_finished_jobs_are_saved(tmp_path):
    state = tmp_path / "pdm_jobs.json"
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs", state_path=state, keep_finished=2)
    ids = [jm.submit("status", [str(i)]).id for i in range(3)]
    for job_id in ids:
        _wait_state(jm, job_id, {"succeeded"})
    jm.stop()
    assert [j["id"] for j in json.loads(state.read_text())["jobs"]] == ids[1:]
    again = JobManager(_fake_tools(tmp_path / "b"), tmp_path / "logs", state_path=state)
    assert again.get(ids[2]).returncode == 0 and again.get(ids[0]) is None


def test_restore_after_a_crash_settles_or_drops_what_cannot_run(tmp_path, capsys):
    state = tmp_path / "pdm_jobs.json"
    base = {"lane": "equity", "submitted_at": 1.0, "args": []}
    state.write_text(json.dumps({"version": 1, "jobs": [
        {**base, "id": "killed", "tool": "download_yf", "state": "running", "started_at": 2.0},
        {**base, "id": "cancelling", "tool": "download_yf", "state": "running", "error": "cancel requested"},
        {**base, "id": "gone", "tool": "removed_tool", "state": "queued"},
        {**base, "id": "later", "tool": "status", "state": "queued", "future_field": 1},
    ]}))
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs", state_path=state)
    assert [j["id"] for j in jm.list()] == ["later", "cancelling", "killed"]
    assert jm.get("killed").requeued == 1 and jm.get("later").lane == "audit"
    assert jm.get("killed").state == "queued" and jm.get("cancelling").state == "cancelled"
    assert "dropping saved job gone" in capsys.readouterr().err
    assert {j["id"] for j in json.loads(state.read_text())["jobs"]} == {"killed", "cancelling", "later"}

    state.write_text("{not json")
    assert JobManager(_fake_tools(tmp_path / "b"), tmp_path / "logs", state_path=state).list() == []


# --- scheduler ------------------------------------------------------------------

def test_interval_parsing():
    assert parse_interval("15m") == timedelta(minutes=15)
    with pytest.raises(ValueError):
        parse_interval("soon")


def test_daily_schedule_respects_weekdays():
    s = Schedule.from_config({"name": "x", "tool": "status", "at": "22:30", "days": ["mon", "fri"]})
    saturday = datetime(2026, 10, 24, 23, 0, tzinfo=timezone.utc)
    assert s.compute_next(saturday) == datetime(2026, 10, 26, 22, 30, tzinfo=timezone.utc)
    friday_early = datetime(2026, 10, 23, 10, 0, tzinfo=timezone.utc)
    assert s.compute_next(friday_early) == datetime(2026, 10, 23, 22, 30, tzinfo=timezone.utc)


def test_schedule_needs_exactly_one_trigger():
    with pytest.raises(ValueError, match="exactly one"):
        Schedule.from_config({"tool": "status"})
    with pytest.raises(ValueError, match="not runnable"):
        Schedule.from_config({"tool": "bash", "every": "1m"})


def test_busy_schedule_is_skipped_not_stacked(tmp_path):
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs")
    s = Schedule.from_config({"name": "slow", "tool": "download_yf", "every": "1m"})
    sched = Scheduler(jm, [s])
    now = datetime.now(timezone.utc)
    s.next_run = now
    sched.tick(now)
    s.next_run = now
    sched.tick(now + timedelta(minutes=1))
    assert len(jm.list()) == 1
    jm.stop()


def test_a_restart_keeps_each_schedules_clock(tmp_path):
    """`every: 8h` used to count from server start, so frequent deploys starved it."""
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs")
    state = tmp_path / "pdm_schedules.json"

    def configs():
        return [Schedule.from_config({"name": "funding", "tool": "status", "every": "8h"}),
                Schedule.from_config({"name": "close", "tool": "status", "at": "22:30"}),
                Schedule.from_config({"name": "new", "tool": "status", "every": "8h"})]

    first = Scheduler(jm, configs(), state_path=state)
    now = datetime.now(timezone.utc)
    first.get("close").last_run = now - timedelta(days=2)   # its slot passed while we were down
    first.run_now("funding")                                # saves both
    assert set(json.loads(state.read_text())["last_run"]) == {"funding", "close"}

    again = Scheduler(jm, configs(), state_path=state)
    after = datetime.now(timezone.utc)
    funding = again.get("funding")
    assert funding.last_run is not None and funding.next_run == funding.last_run + timedelta(hours=8)
    assert again.get("close").next_run <= after                         # missed slot: due now, once
    assert again.get("new").next_run >= after + timedelta(hours=8) - timedelta(seconds=5)   # never ran

    state.write_text("{not json")
    assert Scheduler(jm, configs(), state_path=state).get("funding").last_run is None
    jm.stop()


def test_scheduler_management_persists_every_change(tmp_path):
    jm = JobManager(_fake_tools(tmp_path), tmp_path / "logs")
    saved = []
    sched = Scheduler(jm, [], on_change=saved.append)
    s, created = sched.put({"name": "yf", "refresh": {"provider": "yf", "target": {"universe": "sp500"},
                                                      "timeframes": ["1d"], "symbols": ["AAPL"]},
                            "at": "22:30", "days": ["mon", "fri"]})
    assert created and s.tool == "download_yf" and "--json" in s.args and s.timeout_s == 1800
    assert saved[-1] == [s.raw] and "dry_run" not in saved[-1][0]["refresh"]
    assert sched.put({"name": "yf", "tool": "status", "every": "1h"})[1] is False   # replace
    assert sched.set_enabled("yf", False).public()["next_run"] is None
    assert saved[-1][0]["enabled"] is False
    job = sched.run_now("yf")                       # runs even while paused
    assert _wait_state(jm, job.id, {"succeeded"}).source == "schedule:yf"
    sched.remove("yf")
    assert saved[-1] == [] and sched.schedules == []
    with pytest.raises(ValueError, match="no schedule"):
        sched.remove("yf")
    with pytest.raises(ValueError, match="'refresh' or 'tool', not both"):
        sched.put({"name": "x", "tool": "status", "refresh": {}, "every": "1h"})
    jm.stop()


def test_schedule_targets_map_args_to_catalog_and_symbols():
    def targets(raw):
        return schedule_targets(Schedule.from_config({"name": "x", "every": "1h", **raw}))

    yf = {"provider": "yf", "target": {"universe": "nasdaq100"}, "timeframes": ["1d", "1h"]}
    assert targets({"refresh": yf}) == {"catalog": "nasdaq100", "symbols": None, "timeframes": ["1d", "1h"]}
    assert targets({"refresh": {**yf, "symbols": ["AAPL"]}})["symbols"] == ["AAPL"]
    idx = {"provider": "yf", "target": {"asset_class": "Index"}, "timeframes": ["1d"]}
    assert targets({"refresh": idx})["catalog"] == "indexes"
    cr = {"provider": "crypto", "target": {"exchange": "bybit", "trading_mode": "futures"},
          "timeframes": ["1h"], "symbols": ["BTC/USDT"]}
    assert targets({"refresh": cr}) == {"catalog": "crypto_futures_bybit", "symbols": ["BTC_USDT"],
                                        "timeframes": ["1h"]}
    assert targets({"tool": "status"})["catalog"] is None
    hand = {"tool": "download_crypto", "args": ["--exchange", "kucoin", "--all-pairs", "--timeframes", "1h"]}
    assert targets(hand) == {"catalog": "crypto_kucoin", "symbols": None, "timeframes": ["1h"]}
    assert targets({"tool": "download_yf", "args": ["--universe", "sp500"]})["catalog"] == "sp500"


# --- refresh plans ----------------------------------------------------------------

def test_refresh_plan_builds_argv_and_rejects_bad_input():
    tool, argv, plan = build_argv({"target": {"universe": "sp500"}, "timeframes": ["1d"],
                                   "symbols": ["AAPL", "BRK.B"], "since": "2026-01-01",
                                   "dry_run": True})
    assert tool == "download_yf"
    assert argv == ["--universe=sp500", "--timeframes", "1d", "--symbols", "AAPL", "BRK.B",
                    "--json", "--since=2026-01-01"]
    assert plan["provider"] == "yf" and plan["full"] is False
    tool, argv, _ = build_argv({"provider": "crypto", "target": {"exchange": "binance"},
                                "timeframes": ["1h"], "dry_run": False})
    assert (tool, argv[-1]) == ("download_crypto", "--all-pairs")
    for bad, msg in [
        ({"target": {"universe": "sp500"}, "timeframes": ["1d"]}, "dry_run is required"),
        ({"target": {"universe": "ftse"}, "timeframes": ["1d"], "dry_run": True}, "yf target"),
        ({"target": {"universe": "sp500"}, "timeframes": ["7m"], "dry_run": True}, "Yahoo has no"),
        ({"target": {"universe": "sp500"}, "timeframes": ["1d"], "symbols": ["A;rm -rf"],
          "dry_run": True}, "invalid entries"),
        ({"target": {"universe": "sp500"}, "timeframes": ["1d"], "argv": [], "dry_run": True},
         "unknown refresh field"),
    ]:
        with pytest.raises(PlanError, match=msg):
            build_argv(bad)


# --- config store -------------------------------------------------------------------

def test_config_store_rewrites_atomically_and_keeps_other_keys(tmp_path):
    path = tmp_path / "pdm.yaml"
    path.write_text("qwant_compat: true\nschedules: []\n")
    store = ConfigStore(path)
    store.set("live", [{"exchange": "binance", "symbols": ["BTC/USDT"]}])
    data = yaml.safe_load(path.read_text())
    assert data == {"qwant_compat": True, "schedules": [],
                    "live": [{"exchange": "binance", "symbols": ["BTC/USDT"]}]}
    assert path.read_text().startswith("# PDM server config") and not path.with_suffix(".yaml.tmp").exists()
    assert ConfigStore(None).get("live") is None


# --- live -----------------------------------------------------------------------

def _manager(sent, raw_sent=None, **kw):
    return LiveManager(lambda t, h, f=None: sent.append((t, h, f)),
                       publish_raw=(lambda t, b: raw_sent.append((t, b))) if raw_sent is not None else None,
                       **kw)


def test_symbol_spellings():
    assert ccxt_symbol("BTC_USDT", "spot") == "BTC/USDT"
    assert ccxt_symbol("BTC/USDT", "swap") == "BTC/USDT:USDT"
    assert our_symbol("BTC/USDT:USDT") == "BTC_USDT"


def test_live_publishes_each_closed_bar_once_and_forming_on_change():
    sent = []
    mgr = _manager(sent)
    sub = Subscription.from_config({"exchange": "binance", "symbols": ["BTC/USDT"], "timeframe": "1m"})
    m = 60_000
    bars = [[0, 1, 1, 1, 1, 1], [m, 1, 1, 1, 2, 1], [2 * m, 1, 1, 1, 3, 1]]
    mgr.handle_bars(sub, "BTC/USDT", bars, now_ms=2 * m + 10)      # 0, 1 closed; 2 forming
    assert [(h["closed"], f["timestamp"].tolist()) for _, h, f in sent] == [
        (True, [m]), (False, [2 * m])
    ]
    assert sent[0][0] == "live/binance/BTC_USDT/1m"
    sent.clear()
    mgr.handle_bars(sub, "BTC/USDT", bars, now_ms=2 * m + 20)      # nothing changed
    assert sent == []
    bars.append([3 * m, 1, 1, 1, 4, 1])
    mgr.handle_bars(sub, "BTC/USDT", bars, now_ms=3 * m + 5)       # 2 closes, 3 forms
    assert [(h["closed"], f["timestamp"].tolist()) for _, h, f in sent] == [
        (True, [2 * m]), (False, [3 * m])
    ]


def test_quotes_match_py_qwant_and_throttle():
    import msgpack

    sent, raw = [], []
    mgr = _manager(sent, raw, qwant_compat=True)
    sub = Subscription.from_config({"exchange": "binance", "market": "swap", "kind": "quotes",
                                    "symbols": ["BTC/USDT"], "throttle_ms": 1000})
    ticker = {"bid": 100.0, "ask": 102.0, "last": None, "bidVolume": 3.0, "askVolume": 4.0,
              "timestamp": 1_700_000_000_000}
    assert mgr.handle_quote(sub, "BTC/USDT", ticker, now=1000.0)
    assert not mgr.handle_quote(sub, "BTC/USDT", ticker, now=1000.5)   # inside the window
    assert mgr.handle_quote(sub, "BTC/USDT", ticker, now=1001.2)
    topic, header, frame = sent[0]
    assert topic == "quote/binance/swap/BTC_USDT" and frame is None
    assert header["last"] == 101.0 and header["bid_size"] == 3.0     # last = mid when missing
    assert raw[0][0] == b"prices"
    assert msgpack.unpackb(raw[0][1]) == {"symbol": "BTC_USDT", "timestamp": "2023-11-14T22:13:20+00:00",
                                          "last": 101.0, "bid": 100.0, "ask": 102.0}
    assert len(sent) == len(raw) == 2


class FakeAsyncExchange:
    """ccxt.pro stand-in: websocket methods that yield a canned update each call."""
    has = {"watchOHLCV": True, "watchBidsAsks": True}

    def __init__(self):
        self.closed = False

    async def watch_ohlcv(self, symbol, timeframe, limit=5):
        await asyncio.sleep(0.02)
        now = int(time.time() * 1000) // 60_000 * 60_000
        return [[now - 60_000, 1, 2, 0.5, 1.5, 10], [now, 1.5, 2, 1, 1.7, 3]]

    async def watch_bids_asks(self, symbols):
        await asyncio.sleep(0.02)
        return {symbols[0]: {"bid": 1.0, "ask": 1.2, "last": 1.1, "timestamp": int(time.time() * 1000)}}

    async def close(self):
        self.closed = True


def test_live_manager_streams_and_is_managed_at_runtime():
    sent, saved = [], []
    ex = FakeAsyncExchange()
    mgr = LiveManager(lambda t, h, f=None: sent.append(t),
                      [{"exchange": "binance", "symbols": ["BTC/USDT"], "timeframe": "1m"}],
                      on_change=saved.append, exchange_factory=lambda e, m: ex)
    mgr.start()
    try:
        added = mgr.add({"exchange": "binance", "market": "swap", "kind": "quotes",
                         "symbols": ["ETH_USDT", "SOL/USDT"]})
        assert added["added"] == ["ETH/USDT", "SOL/USDT"]
        assert mgr.add({"exchange": "binance", "market": "swap", "kind": "quotes",
                        "symbols": ["ETH/USDT"]})["added"] == []           # idempotent
        deadline = time.time() + 5
        while time.time() < deadline and not {"live/binance/BTC_USDT/1m",
                                              "quote/binance/swap/SOL_USDT"} <= set(sent):
            time.sleep(0.05)
        assert {"live/binance/BTC_USDT/1m", "quote/binance/swap/ETH_USDT",
                "quote/binance/swap/SOL_USDT"} <= set(sent)
        (row,) = mgr.list(symbol="SOL_USDT")
        assert row["kind"] == "quotes" and row["symbols"][1]["transport"] == "websocket"
        assert all(age is not None for age in mgr.freshness().values())

        mgr.remove({"exchange": "binance", "market": "swap", "kind": "quotes", "symbols": ["SOL/USDT"]})
        mgr.remove({"exchange": "binance", "timeframe": "1m"})              # whole subscription
        assert saved[-1] == [{"exchange": "binance", "market": "swap", "kind": "quotes",
                              "symbols": ["ETH/USDT"]}]
        with pytest.raises(ValueError, match="not subscribed"):
            mgr.remove({"exchange": "binance", "market": "swap", "kind": "quotes", "symbols": ["XRP/USDT"]})
        with pytest.raises(ValueError, match="exchange must be"):
            mgr.add({"exchange": "ftx", "symbols": ["BTC/USDT"]})
    finally:
        mgr.stop()
    assert ex.closed


# --- end to end -----------------------------------------------------------------

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def running_server(tmp_path, ohlcv, monkeypatch):
    """A real server over TCP on 127.0.0.1, against a tmp data root."""
    root = tmp_path / "price-data"
    root.mkdir()
    # The suite's convention: point PRICE_DATA_ROOT at tmp and re-import, so
    # every module (pdm.index's catalog_registry included) sees the same root.
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in [m for m in sys.modules if m.startswith(("utils.", "pdm."))]:
        monkeypatch.delitem(sys.modules, name)

    _series(root, "cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet", ohlcv(0, 30))
    (root / "crypto_binance.yaml").write_text(yaml.safe_dump({
        "BTC_USDT_binance": {
            "name": "BTC_USDT",
            "common_name": "Bitcoin/Tether",
            "timeframe_dates": {"1d": {
                "start_date": "1970-01-01", "end_date": "1970-01-30",
                "file_path": "price-data/cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet",
            }},
        }
    }))

    tools = tmp_path / "tools"
    tools.mkdir()
    # A stand-in download tool that appends two daily bars through the real
    # atomic writer, the way every real tool writes.
    (tools / "download_crypto.py").write_text(textwrap.dedent(f"""
        import sys
        sys.path.insert(0, {str(Path(__file__).resolve().parent.parent)!r})
        import pandas as pd
        from utils.catalog import merge_ohlcv_parquet
        ts = [30 * {DAY}, 31 * {DAY}]
        df = pd.DataFrame({{"timestamp": pd.Series(ts, dtype="int64"), "open": 1.0,
                           "high": 2.0, "low": 0.5, "close": 9.0, "volume": 1.0}})
        merge_ohlcv_parquet(df, __import__("pathlib").Path({str(root)!r})
                            / "cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet")
    """))

    # A stand-in download_yf: --dry-run --json prints a plan; refuses ZZZZ like
    # the real tool refuses a symbol outside the universe.
    (tools / "download_yf.py").write_text(textwrap.dedent("""
        import json, sys
        if "ZZZZ" in sys.argv:
            print("download_yf.py: error: symbol(s) not in sp500: ZZZZ", file=sys.stderr)
            sys.exit(2)
        print("planning...", file=sys.stderr)
        print(json.dumps({"dry_run": "--dry-run" in sys.argv, "planned": 1, "current": 0,
                          "plan": [], "results": [], "updated": 0, "failures": []}))
    """))

    # A stand-in read-only audit tool: echoes its argv as a JSON report.
    (tools / "check_gaps.py").write_text(
        "import json, sys\nprint(json.dumps({'argv': sys.argv[1:], 'gaps': []}))\n"
        "sys.exit(1 if '--internal' in sys.argv else 0)\n")

    import importlib

    PdmServer = importlib.import_module("pdm.server").PdmServer
    req, pub, http = _free_port(), _free_port(), _free_port()
    config_path = tmp_path / "pdm.yaml"
    config_path.write_text(yaml.safe_dump({"qwant_compat": True, "schedules": [
        {"name": "btc-daily", "every": "1d",
         "refresh": {"provider": "crypto", "target": {"exchange": "binance"},
                     "timeframes": ["1d"], "symbols": ["BTC/USDT"]}},
    ]}))
    server = PdmServer(data_root=root, tools_dir=tools, tmp_root=tmp_path / "tmp",
                       bind_host="127.0.0.1", req_port=req, pub_port=pub,
                       token="sekret", scan_seconds=3600, config_path=config_path,
                       heartbeat_seconds=0.3, http_port=http)
    server.live._factory = lambda exchange, market: FakeAsyncExchange()   # no network
    ready = threading.Event()
    thread = threading.Thread(target=server.serve, args=(ready,), daemon=True)
    thread.start()
    ready.wait(5)
    server.index_ready.wait(10)
    server.watcher.ready.wait(10)
    yield server, req, pub
    server.stop()
    thread.join(5)


def test_round_trip_query_bars_job_and_broadcast(running_server):
    from pdm.client import PdmError, PriceDataClient

    server, req, pub = running_server
    with PriceDataClient("127.0.0.1", req, pub, token="sekret", timeout=10) as c:
        assert c.ping()["series_indexed"] == 1
        assert c.catalogs()[0]["series"] >= 1 or any(x["series"] for x in c.catalogs())
        (row,) = c.series(symbol="BTC_USDT")
        assert row["catalog"] == "crypto_binance"

        df = c.bars(symbol="BTC_USDT", timeframe="1d", start="1970-01-10", end="1970-01-12")
        assert df["timestamp"].tolist() == [9 * DAY, 10 * DAY, 11 * DAY]
        assert len(c.bars(path="cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet", limit=4)) == 4

        with pytest.raises(PdmError, match="under the data root"):
            c.bars(path="../../etc/passwd")
        with pytest.raises(PdmError, match="no series"):
            c.bars(symbol="NOPE", timeframe="1d")

        received = []
        listener_ready = threading.Event()

        def listen():
            sub = PriceDataClient("127.0.0.1", req, pub, timeout=10)
            gen = sub.subscribe("bar/cryptos/binance/BTC_USDT/", "job/", timeout=15)
            listener_ready.set()
            for topic, header, frame in gen:
                received.append((topic, header, frame))
                if topic.startswith("bar/"):
                    break
            sub.close()

        t = threading.Thread(target=listen, daemon=True)
        t.start()
        listener_ready.wait(5)
        time.sleep(0.5)  # PUB/SUB slow joiner: let the subscription propagate

        job = c.submit("download_crypto", ["--exchange", "binance"])
        done = c.wait(job["id"], poll=0.1, timeout=30)
        assert done["state"] == "succeeded", c.job(job["id"], log_lines=20)
        t.join(15)

        bars = [(tp, h, f) for tp, h, f in received if tp.startswith("bar/")]
        assert bars, f"no bar broadcast; got {[tp for tp, _, _ in received]}"
        topic, header, frame = bars[0]
        assert topic == "bar/cryptos/binance/BTC_USDT/1d"
        assert header["symbol"] == "BTC_USDT" and header["trigger"] == f"job:{job['id']}"
        assert frame["timestamp"].tolist() == [30 * DAY, 31 * DAY]
        assert any(tp.startswith("job/succeeded/") for tp, _, _ in received)


def test_mutating_methods_need_the_token(running_server):
    from pdm.client import PdmError, PriceDataClient

    _, req, pub = running_server
    with PriceDataClient("127.0.0.1", req, pub, token="wrong", timeout=10) as c:
        c.ping()  # reads stay open
        with pytest.raises(PdmError, match="unauthorized"):
            c.submit("status", [])
        with pytest.raises(PdmError, match="unknown method"):
            c.call("drop_tables")
        with pytest.raises(PdmError, match="unauthorized"):
            c.call("alert_test")             # it runs the notify command
        assert c.call("alerts") == []


def test_alerts_over_the_wire(running_server):
    from pdm.client import PriceDataClient

    _, req, pub = running_server
    with PriceDataClient("127.0.0.1", req, pub, token="sekret", timeout=10) as c:
        sent = c.call("alert_test")
        assert sent["kind"] == "test" and "PDM_NOTIFY_CMD unset" in sent["text"]
        assert [a["id"] for a in c.call("alerts", limit=5)] == [sent["id"]]


def test_coverage_refresh_schedules_and_live_over_the_wire(running_server):
    import msgpack  # noqa: F401  (the client decodes `prices` with it)
    from pdm.client import PdmError, PriceDataClient

    server, req, pub = running_server
    config_path = server.store.path
    with PriceDataClient("127.0.0.1", req, pub, token="sekret", timeout=10) as c:
        (sched,) = c.schedules(symbol="BTC_USDT")
        assert sched["name"] == "btc-daily" and sched["targets"]["catalog"] == "crypto_binance"
        assert c.schedules(symbol="ETH_USDT") == []

        # schedules: add one covering the whole catalog, pause it, persist
        c.schedule_put({"name": "all-binance", "every": "6h", "tool": "download_crypto",
                        "args": ["--exchange=binance", "--timeframes", "1h", "--all-pairs"]})
        assert {s["name"] for s in c.schedules(catalog="crypto_binance")} == {"btc-daily", "all-binance"}
        assert c.schedule_pause("all-binance")["enabled"] is False
        on_disk = yaml.safe_load(config_path.read_text())
        assert [s["name"] for s in on_disk["schedules"]] == ["btc-daily", "all-binance"]
        assert on_disk["schedules"][1]["enabled"] is False and on_disk["qwant_compat"] is True

        # live: add quotes, see them broadcast (incl. the py-qwant feed), remove
        heard = []
        ready = threading.Event()

        def listen():
            sub = PriceDataClient("127.0.0.1", req, pub, timeout=10)
            gen = sub.subscribe("quote/", "prices", "server/heartbeat", timeout=10)
            ready.set()
            for topic, header, _ in gen:
                heard.append((topic, header))
                if {"quote/binance/spot/ETH_USDT", "prices", "server/heartbeat"} <= {t for t, _ in heard}:
                    break
            sub.close()

        t = threading.Thread(target=listen, daemon=True)
        t.start()
        ready.wait(5)
        time.sleep(0.5)
        c.live_add("binance", ["ETH/USDT"], kind="quotes")
        t.join(10)
        topics = {tp for tp, _ in heard}
        assert {"quote/binance/spot/ETH_USDT", "prices", "server/heartbeat"} <= topics, topics
        prices = next(h for tp, h in heard if tp == "prices")
        assert prices["symbol"] == "ETH_USDT" and set(prices) == {"symbol", "timestamp", "last", "bid", "ask"}

        cov = c.coverage("BTC_USDT")
        assert [r["timeframe"] for r in cov["stored"]] == ["1d"]
        assert {s["name"] for s in cov["schedules"]} == {"btc-daily", "all-binance"}
        assert cov["live"] == []
        assert c.live(symbol="ETH/USDT")["subscriptions"][0]["kind"] == "quotes"
        c.live_remove("binance", kind="quotes")
        assert c.live()["subscriptions"] == [] and yaml.safe_load(config_path.read_text())["live"] == []

        # refresh: dry run previews; a refused plan never queues; one at a time
        plan = {"provider": "yf", "target": {"universe": "sp500"}, "timeframes": ["1d"]}
        prev = c.refresh(dry_run=True, symbols=["AAPL"], **plan)
        assert prev["refused"] is False and prev["plan"]["dry_run"] is True
        assert prev["argv"][-1] == "--dry-run"
        with pytest.raises(PdmError, match="dry_run is required"):
            c.call("refresh", **plan)
        with pytest.raises(PdmError, match="refused by dry run: symbol"):
            c.refresh(dry_run=False, symbols=["ZZZZ"], **plan)
        assert not [j for j in c.jobs() if j["source"] == "refresh"]
        job = c.refresh(dry_run=False, symbols=["AAPL"], **plan)
        assert job["timeout_s"] == 1800 and job["meta"]["preview"]["plan"]["planned"] == 1
        done = c.wait(job["id"], poll=0.1, timeout=30)
        assert done["state"] == "succeeded" and done["result"]["dry_run"] is False

        c.schedule_remove("all-binance")
        assert [s["name"] for s in c.schedules()] == ["btc-daily"]

    with PriceDataClient("127.0.0.1", req, pub, token="wrong", timeout=10) as c:
        for call in (lambda: c.live_add("binance", ["BTC/USDT"]),
                     lambda: c.schedule_pause("btc-daily"),
                     lambda: c.refresh(dry_run=False, symbols=["AAPL"], **plan)):
            with pytest.raises(PdmError, match="unauthorized"):
                call()
        # a dry run reads only: open without the token, so a secret-less agent can preview
        assert c.refresh(dry_run=True, symbols=["AAPL"], **plan)["refused"] is False
        assert "refresh (dry_run=false)" in c.ping()["auth_required_for"]


def test_needs_token_exempts_only_dry_run_refresh():
    from pdm.server import needs_token

    assert not needs_token("refresh", {"dry_run": True})
    assert needs_token("refresh", {"dry_run": False})
    assert needs_token("refresh", {"dry_run": "true"})      # only a real bool exempts
    assert needs_token("refresh", {})
    assert needs_token("cancel", {}) and not needs_token("job", {"id": "x"})


def test_client_token_comes_from_env_or_a_file_outside_the_repo(tmp_path, monkeypatch):
    import importlib

    def token() -> str:   # a fresh import: other fixtures drop utils.* from sys.modules
        monkeypatch.delitem(sys.modules, "utils.config", raising=False)
        return importlib.import_module("utils.config").PDM_TOKEN

    tok = tmp_path / "token"
    tok.write_text("from-file\n")
    monkeypatch.setenv("PDM_TOKEN_FILE", str(tok))
    monkeypatch.delenv("PDM_TOKEN", raising=False)
    assert token() == "from-file"
    monkeypatch.setenv("PDM_TOKEN", "from-env")
    assert token() == "from-env"
    monkeypatch.delenv("PDM_TOKEN")
    monkeypatch.setenv("PDM_TOKEN_FILE", str(tmp_path / "missing"))
    assert token() == ""


def test_client_wait_stops_on_every_terminal_state(monkeypatch):
    from pdm.client import TERMINAL, PriceDataClient
    from pdm.jobs import TERMINAL as SERVER_TERMINAL

    assert set(TERMINAL) == SERVER_TERMINAL
    c = PriceDataClient("127.0.0.1", 1, 2, timeout=1)
    states = iter(["queued", "running", "timed_out"])
    monkeypatch.setattr(c, "job", lambda job_id, log_lines=0: {"id": job_id, "state": next(states)})
    assert c.wait("j", poll=0, timeout=5)["state"] == "timed_out"
    c.close()



def test_audit_options_are_typed_and_cannot_write(tmp_path):
    from pdm.audit import AuditError, build_argv

    root = tmp_path / "price-data"
    root.mkdir()
    assert build_argv("check_gaps", {"catalog": ["sp500"], "timeframe": "1d", "internal": True,
                                     "stale_days": 2}, root) == [
        "--catalog", "sp500", "--timeframe", "1d", "--internal", "--stale-days", "2", "--json"]
    assert build_argv("check_splits", {"path": ["a.parquet", "b.parquet"]}, root) == [
        "--path", str(root / "a.parquet"), "--path", str(root / "b.parquet"), "--json"]
    for tool, options, msg in [
        ("check_catalog", {"fix": True}, "unknown option"),
        ("download_yf", {}, "audit tool must be"),
        ("check_schema", {"path": "../../etc/passwd"}, "not under the data root"),
        ("query", {"symbol": "A; rm -rf /"}, "invalid value"),
        ("status", {"stale_days": "3"}, "integer"),
        ("status", {"deep": "yes"}, "true or false"),
    ]:
        with pytest.raises(AuditError, match=msg):
            build_argv(tool, options, root)


def test_bars_cap_refuses_in_words(running_server, monkeypatch):
    from pdm.client import PdmError, PriceDataClient

    server, req, pub = running_server
    monkeypatch.setattr(sys.modules[type(server).__module__], "MAX_BARS_ROWS", 10)
    with PriceDataClient("127.0.0.1", req, pub, timeout=10) as c:
        with pytest.raises(PdmError, match="30 rows match, over the 10-row cap"):
            c.bars(symbol="BTC_USDT", timeframe="1d")
        with pytest.raises(PdmError, match="over the 10-row cap"):
            c.bars(symbol="BTC_USDT", timeframe="1d", limit=11)
        assert len(c.bars(symbol="BTC_USDT", timeframe="1d", limit=10)) == 10
        assert len(c.bars(symbol="BTC_USDT", timeframe="1d", start="1970-01-21")) == 10


def test_http_gateway_is_read_only(running_server):
    import json
    import urllib.error
    import urllib.request

    from pdm.protocol import arrow_to_frame

    server, _, _ = running_server
    base = f"http://127.0.0.1:{server.http_port}/v1"

    def get(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base + path, data=data,
                                     headers={"Content-Type": "application/json"} if data else {})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status, r.headers, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    status, _, body = get("/series?symbol=BTC_USDT")
    assert status == 200 and json.loads(body)["result"]["series"][0]["catalog"] == "crypto_binance"

    status, _, body = get("/bars?symbol=BTC_USDT&timeframe=1d&limit=3")
    res = json.loads(body)["result"]
    assert res["columns"][0] == "timestamp" and res["data"][-1][0] == 29 * DAY   # an int, not 2.5e9
    assert isinstance(res["data"][-1][0], int)

    status, headers, body = get("/bars?symbol=BTC_USDT&timeframe=1d&limit=3&format=arrow")
    assert headers["Content-Type"] == "application/vnd.apache.arrow.stream"
    assert arrow_to_frame(body)["timestamp"].tolist() == [27 * DAY, 28 * DAY, 29 * DAY]

    status, _, body = get("/audit", {"tool": "check_gaps", "options": {"catalog": ["crypto_binance"],
                                                                       "internal": True}})
    res = json.loads(body)["result"]
    assert status == 200 and res["ok"] and res["exit"] == 1
    assert res["report"]["argv"] == ["--catalog", "crypto_binance", "--internal", "--json"]

    plan = {"target": {"universe": "sp500"}, "timeframes": ["1d"], "symbols": ["AAPL"]}
    status, _, body = get("/refresh", {**plan, "dry_run": True})
    assert status == 200 and json.loads(body)["result"]["refused"] is False
    assert get("/refresh", {**plan, "dry_run": False})[0] == 403
    assert get("/refresh?dry_run=true&timeframes=1d", {"target": {"universe": "sp500"}})[0] == 200
    for path, body in [("/submit", {"tool": "status"}), ("/cancel", {"id": "x"}),
                       ("/live_add", {"exchange": "binance", "symbols": ["BTC/USDT"]}),
                       ("/schedule_remove", {"name": "btc-daily"})]:
        assert get(path, body)[0] == 403, path
    assert get("/bars?symbol=NOPE&timeframe=1d")[0] == 400
    assert get("/bars?symbol=BTC_USDT&limit=x")[0] == 400
    assert get("/nope/deeper")[0] == 404
    assert [s.name for s in server.scheduler.schedules] == ["btc-daily"]   # untouched


def test_series_name_filter_and_fields():
    from pdm.index import SeriesIndex, name_matches, name_words

    assert name_words("Apple Inc.") == ("apple", "inc")
    assert name_matches(name_words("APPLE"), name_words("Apple Inc."))
    assert name_matches(name_words("apple inc"), name_words("Apple Inc."))
    assert not name_matches(name_words("app"), name_words("Apple Inc."))   # whole words only
    assert not name_matches(name_words("inc"), name_words("Apple Inc."))   # leading words only

    idx = SeriesIndex()
    row = dict(catalog="sp500", asset_class="Stock", provider="YF", session=None, start_date=None,
               end_date=None, adjustment=None, volume_basis=None, file_path=None)
    idx.rows = [
        {**row, "symbol": "AAPL", "common_name": "Apple Inc.", "timeframe": "1d"},
        {**row, "symbol": "AAPL", "common_name": "Apple Inc.", "timeframe": "1h"},
        {**row, "symbol": "APLE", "common_name": "Apple Hospitality REIT", "timeframe": "1d"},
        {**row, "symbol": "NVDA", "common_name": "Nvidia", "timeframe": "1d"},
        {**row, "symbol": "X", "common_name": None, "timeframe": "1d"},
    ]
    idx._words = {r["common_name"]: name_words(r["common_name"]) for r in idx.rows if r["common_name"]}
    assert {r["symbol"] for r in idx.query(name="apple")} == {"AAPL", "APLE"}
    assert {r["symbol"] for r in idx.query(name="Apple Inc")} == {"AAPL"}
    assert [r["symbol"] for r in idx.query(name="nvidia")] == ["NVDA"]
    with pytest.raises(ValueError):
        idx.query(name="...")


def test_series_name_and_fields_over_the_wire(running_server):
    from pdm.client import PdmError, PriceDataClient

    server, req, pub = running_server
    with PriceDataClient("127.0.0.1", req, pub, timeout=10) as client:
        _series_name_checks(server, client, PdmError)


def _series_name_checks(server, client, PdmError):
    import json

    rows = client.series(symbol="BTC_USDT")
    assert rows[0]["common_name"] == "Bitcoin/Tether"
    res = client.call("series", name="bitcoin", fields=["symbol", "catalog", "common_name"], limit=0)
    assert res["series"] == [{"symbol": "BTC_USDT", "catalog": "crypto_binance",
                              "common_name": "Bitcoin/Tether"}]
    assert client.call("series", name="tether")["count"] == 0          # not a leading word
    with pytest.raises(PdmError, match="fields must be"):
        client.call("series", fields=["nope"])
    with pytest.raises(PdmError, match="name needs"):
        client.call("series", name="  ")

    status, body = _http(server, "/series?name=Bitcoin&fields=symbol,common_name")
    assert status == 200
    assert json.loads(body)["result"]["series"] == [{"symbol": "BTC_USDT", "common_name": "Bitcoin/Tether"}]


def _http(server, path):
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{server.http_port}/v1{path}", timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def test_bars_derive_timestamp_for_dated_layouts(running_server):
    """Macro files keep `Date` as the pandas index and have no `timestamp`;
    bars must still carry the time, as int64 ms first, and honour start/end."""
    from pdm.client import PdmError, PriceDataClient

    server, req, pub = running_server
    root = server.data_root
    macro = root / "macro/yf/WTB3MS/WTB3MS-1d.parquet"
    macro.parent.mkdir(parents=True)
    dates = pd.to_datetime(["2026-01-05", "2026-01-12", "2026-01-19"])
    pd.DataFrame({"Value": [4.1, 4.0, 3.9]}, index=pd.Index(dates, name="Date")).to_parquet(macro)
    pd.DataFrame({"Value": [1.0]}).to_parquet(root / "macro/undated.parquet")

    with PriceDataClient("127.0.0.1", req, pub, timeout=10) as c:
        df, meta = c.bars(path="macro/yf/WTB3MS/WTB3MS-1d.parquet", limit=2, with_meta=True)
        assert list(df.columns) == ["timestamp", "Value"] and meta["columns"] == ["timestamp", "Value"]
        assert df["timestamp"].tolist() == [int(d.value // 10**6) for d in dates[1:]]
        df = c.bars(path="macro/yf/WTB3MS/WTB3MS-1d.parquet", start="2026-01-10", end="2026-01-15")
        assert df["Value"].tolist() == [4.0]
        with pytest.raises(PdmError, match="no time column"):
            c.bars(path="macro/undated.parquet")


def test_refresh_plan_accepts_funding():
    tool, argv, plan = build_argv({"dry_run": True, "provider": "funding",
                                   "target": {"exchange": "bybit"}, "symbols": ["BTC_USDT"]})
    assert tool == "download_funding"
    assert argv == ["--exchange=bybit", "--symbols", "BTC_USDT", "--json"]
    assert plan["timeframes"] == ["funding"]
    assert build_argv({"dry_run": True, "provider": "funding", "target": {"exchange": "binance"},
                       "timeframes": ["funding"], "full": True})[1][-1] == "--full"
    with pytest.raises(PlanError, match="no timeframes"):
        build_argv({"dry_run": True, "provider": "funding", "target": {"exchange": "binance"},
                    "timeframes": ["8h"]})
    with pytest.raises(PlanError, match="binance|bybit"):
        build_argv({"dry_run": True, "provider": "funding", "target": {"exchange": "bitget"}})
    s = Schedule.from_config({"name": "f", "every": "8h", "refresh": {
        "provider": "funding", "target": {"exchange": "binance"}}})
    assert schedule_targets(s) == {"catalog": "crypto_futures_binance", "symbols": None,
                                   "timeframes": ["funding"]}


def test_refresh_plan_accepts_macro():
    tool, argv, _ = build_argv({"dry_run": True, "target": {"asset_class": "Macro"}, "timeframes": ["1d"]})
    assert tool == "download_yf" and "--asset-class=Macro" in argv
    with pytest.raises(PlanError, match="Index|Commodity|Macro"):
        build_argv({"dry_run": True, "target": {"asset_class": "Bond"}, "timeframes": ["1d"]})
    assert schedule_targets(Schedule(name="m", tool="download_yf",
                                     args=["--asset-class", "Macro", "--timeframes", "1d"],
                                     every=timedelta(days=1)))["catalog"] == "macro"
