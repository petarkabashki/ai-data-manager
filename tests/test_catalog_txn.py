"""Stock catalog writers overlap: IBKR workers run outside the PDM lanes.

So a writer that saves a catalog from memory erases what landed since it loaded.
These tests run the writers against a file that changes under them.
"""

from __future__ import annotations

import fcntl
import importlib
import os
import signal
import stat
import subprocess
import sys
import threading
from pathlib import Path

import pandas as pd
from dataclasses import replace

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULES = ("utils.config", "utils.catalog", "utils.data", "utils.planning", "utils.yf", "utils.ibkr",
           "download_yf", "download_ibkr_stock_overnight", "check_catalog", "dedupe_stock_universes")


class Env:
    def __init__(self, root):
        self.root = root
        self.catalog = importlib.import_module("utils.catalog")

    def tool(self, name):
        return importlib.import_module(name)

    def seed(self, universe, *symbols, **nodes):
        """A catalog listing `symbols`."""
        self.catalog.save_stock_catalog(universe, {
            s: {"name": s, "categories": ["Stock"], "timeframes": [], "timeframe_dates": {}}
            for s in symbols
        })

    def worker_writes(self, universe, symbol, tf="1d", provider="IBKR", start="2001-01-01",
                      end="2001-06-30", path=None, session=None):
        """What another process does while the code under test is running."""
        cat = self.catalog
        path = path or cat.stock_file_path(provider, universe, symbol, tf, session)
        with cat.stock_catalog_txn(universe) as fresh:
            cat.update_stock_timeframe_entry(fresh, symbol, tf, provider, path, start, end, session=session)

    def node(self, universe, symbol, tf, key):
        return self.catalog.load_stock_catalog(universe)[symbol]["timeframe_dates"][tf][key]


@pytest.fixture
def env(tmp_path, monkeypatch):
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    monkeypatch.setenv("TMP_ROOT", str(tmp_path / "tmp"))
    for name in MODULES:
        sys.modules.pop(name, None)
    sys.path.insert(0, str(REPO_ROOT / "tools"))
    yield Env(root)
    sys.path.remove(str(REPO_ROOT / "tools"))
    for name in MODULES:
        sys.modules.pop(name, None)


# --- catalog_txn ---------------------------------------------------------------

WRITER = """
import sys
from utils.catalog import stock_catalog_txn, stock_file_path, update_stock_timeframe_entry
prefix, count = sys.argv[1], int(sys.argv[2])
print("ready", flush=True)
sys.stdin.readline()
for i in range(count):
    symbol = f"{prefix}{i}"
    with stock_catalog_txn("russell2000") as fresh:
        update_stock_timeframe_entry(fresh, symbol, "1d", "IBKR",
                                     stock_file_path("IBKR", "russell2000", symbol, "1d"),
                                     "2020-01-01", "2020-01-02")
"""


def test_two_processes_writing_different_symbols_lose_no_nodes(env, tmp_path):
    env.seed("russell2000", *(f"PAD{i}" for i in range(300)))
    process_env = {**os.environ, "PYTHONPATH": str(REPO_ROOT)}
    procs = [
        subprocess.Popen([sys.executable, "-c", WRITER, prefix, "25"], env=process_env, cwd=tmp_path,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        for prefix in ("A", "B")
    ]
    for proc in procs:
        assert proc.stdout.readline().strip() == "ready", proc.stderr.read()
    for proc in procs:                       # released together, so they overlap
        proc.stdin.write("go\n")
        proc.stdin.flush()
    for proc in procs:
        _, err = proc.communicate(timeout=120)
        assert proc.returncode == 0, err

    written = env.catalog.load_stock_catalog("russell2000")
    assert {f"{p}{i}" for p in "AB" for i in range(25)} <= set(written)
    assert not list(env.root.glob("*.tmp"))


def test_a_body_that_raises_writes_nothing(env):
    cat = env.catalog
    env.seed("sp500", "AAA")
    before = cat.stock_catalog_path("sp500").read_bytes()
    with pytest.raises(RuntimeError):
        with cat.stock_catalog_txn("sp500") as fresh:
            fresh["BBB"] = {}
            raise RuntimeError("mid-edit")
    assert cat.stock_catalog_path("sp500").read_bytes() == before
    with cat.stock_catalog_txn("sp500") as fresh:     # and the lock was released
        assert "BBB" not in fresh


def test_the_lock_is_the_one_old_code_takes(env):
    """A process still on the old save_stock_symbol must exclude this one."""
    cat = env.catalog
    env.seed("sp500", "AAA")
    with open(cat.stock_catalog_path("sp500").with_name("sp500.yaml.lock"), "a") as other:
        with cat.stock_catalog_txn("sp500"):
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_the_body_sees_what_landed_after_the_caller_loaded(env):
    cat = env.catalog
    env.seed("russell2000", "AAA", "BBB")
    stale = cat.load_stock_catalog("russell2000")
    env.worker_writes("russell2000", "BBB")
    assert stale["BBB"]["timeframe_dates"] == {}
    with cat.stock_catalog_txn("russell2000") as fresh:
        fresh["AAA"]["note"] = 1
    assert env.node("russell2000", "BBB", "1d", "IBKR")["end_date"] == "2001-06-30"


# --- _dump_yaml ----------------------------------------------------------------

def test_unlocked_writers_never_collide_on_the_temp_file(env, tmp_path):
    cat = env.catalog
    path = tmp_path / "cat.yaml"
    data = {f"S{i}": {"a": i} for i in range(200)}
    errors: list[BaseException] = []

    def hammer():
        try:
            for _ in range(40):
                cat._dump_yaml(path, data)
        except BaseException as exc:  # noqa: BLE001 - reported by the assert below
            errors.append(exc)

    threads = [threading.Thread(target=hammer) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert list(tmp_path.glob("*.tmp")) == []


def test_temp_names_are_unique_and_beside_the_catalog(env, tmp_path, monkeypatch):
    cat = env.catalog
    path = tmp_path / "cat.yaml"
    temps: list[Path] = []
    real = Path.replace

    def spy(self, target):
        if Path(target) == path:                       # not the pre-image backup's rename
            temps.append(self)
        return real(self, target)

    monkeypatch.setattr(Path, "replace", spy)
    cat._dump_yaml(path, {"a": 1})
    cat._dump_yaml(path, {"a": 2})
    assert len(set(temps)) == 2
    assert {t.parent for t in temps} == {tmp_path}


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o664])
def test_an_existing_catalog_keeps_its_mode(env, tmp_path, mode):
    cat = env.catalog
    path = tmp_path / "cat.yaml"
    cat._dump_yaml(path, {"a": 1})
    path.chmod(mode)
    cat._dump_yaml(path, {"a": 2})
    assert stat.S_IMODE(path.stat().st_mode) == mode
    assert yaml.safe_load(path.read_text()) == {"a": 2}


def test_a_new_catalog_gets_the_umask_default_not_0600(env, tmp_path):
    cat = env.catalog
    previous = os.umask(0o027)
    try:
        cat._dump_yaml(tmp_path / "cat.yaml", {"a": 1})
    finally:
        os.umask(previous)
    assert stat.S_IMODE((tmp_path / "cat.yaml").stat().st_mode) == 0o640


# --- download_yf ---------------------------------------------------------------

class FakeYahoo:
    """Daily bars for any window, with a hook that fires as a series is fetched."""

    def __init__(self, during=None):
        self.during = during
        self.calls = 0

    def fetch(self, ticker, interval, start, end, auto_adjust):
        self.calls += 1
        if self.during:
            self.during(self.calls, ticker)
        days = pd.date_range(start.date(), end.date(), freq="D", name="Date")
        return pd.DataFrame({"Open": 1.0, "High": 2.0, "Low": 0.5, "Close": 1.5,
                             "Adj Close": 1.4, "Volume": 1000.0}, index=days)


def _run_yf(tool, monkeypatch, fake, *symbols):
    monkeypatch.setattr(tool, "YahooFetcher", lambda: fake)
    monkeypatch.setattr(sys, "argv", ["download_yf.py", "--universe", "sp500", "--symbols", *symbols,
                                      "--timeframes", "1d", "--since", "2024-01-01",
                                      "--sleep-seconds", "0"])
    tool.main()


def test_yf_keeps_what_a_worker_wrote_during_the_run(env, monkeypatch):
    """Both providers' nodes on one symbol, and a symbol YF isn't working on."""
    tool = env.tool("download_yf")
    env.seed("sp500", "AAPL", "MSFT")

    def worker(call, ticker):
        env.worker_writes("sp500", "AAPL")
        env.worker_writes("sp500", "MSFT")

    _run_yf(tool, monkeypatch, FakeYahoo(worker), "AAPL")

    assert env.node("sp500", "AAPL", "1d", "IBKR")["end_date"] == "2001-06-30"
    assert env.node("sp500", "AAPL", "1d", "YF")["file_path"].endswith("yf/sp500/AAPL/AAPL-1d.parquet")
    assert env.node("sp500", "MSFT", "1d", "IBKR")["end_date"] == "2001-06-30"
    entry = env.catalog.load_stock_catalog("sp500")["AAPL"]
    assert entry["start_date"] == "2001-01-01"     # bounds span both providers
    assert entry["end_date"] == env.node("sp500", "AAPL", "1d", "YF")["end_date"]


def test_yf_writes_on_a_timer_not_per_series(env, monkeypatch):
    tool = env.tool("download_yf")
    env.seed("sp500", "AAA", "BBB", "CCC")
    dumps = []
    real = tool.PendingStockWrites.flush
    monkeypatch.setattr(tool.PendingStockWrites, "flush",
                        lambda self: (dumps.append(sum(map(len, self.pending.values()))), real(self)))
    monkeypatch.setattr(tool, "STOCK_FLUSH_SECONDS", 3600.0)
    _run_yf(tool, monkeypatch, FakeYahoo(), "AAA", "BBB", "CCC")
    assert dumps == [3]
    assert env.node("sp500", "CCC", "1d", "YF")


def test_yf_flushes_when_the_timer_is_due(env, monkeypatch):
    tool = env.tool("download_yf")
    env.seed("sp500", "AAA", "BBB")
    seen = []

    def look(call, ticker):
        if call == 2:
            seen.append(env.catalog.load_stock_catalog("sp500")["AAA"]["timeframe_dates"])

    monkeypatch.setattr(tool, "STOCK_FLUSH_SECONDS", 0.0)
    _run_yf(tool, monkeypatch, FakeYahoo(look), "AAA", "BBB")
    assert "YF" in seen[0]["1d"]


@pytest.mark.parametrize("stop", ["exception", "interrupt", "sigterm"])
def test_yf_flushes_what_it_finished_when_the_run_dies(env, monkeypatch, stop):
    """Files already merged must not be left without nodes."""
    tool = env.tool("download_yf")
    env.seed("sp500", "AAA", "BBB")
    before = signal.getsignal(signal.SIGTERM)
    merge = tool.merge_ohlcv_parquet

    def die_on_second(frame, dst, daily=None):
        if dst.name.startswith("BBB"):
            if stop == "exception":
                raise OSError("disk full")
            if stop == "interrupt":
                raise KeyboardInterrupt
            os.kill(os.getpid(), signal.SIGTERM)
        return merge(frame, dst, daily=daily)

    monkeypatch.setattr(tool, "merge_ohlcv_parquet", die_on_second)
    with pytest.raises((OSError, KeyboardInterrupt, SystemExit)):
        _run_yf(tool, monkeypatch, FakeYahoo(), "AAA", "BBB")

    assert "YF" in env.node("sp500", "AAA", "1d", "YF")["provider"]
    assert signal.getsignal(signal.SIGTERM) is before


def test_yf_index_target_still_saves_per_series(env, monkeypatch):
    tool = env.tool("download_yf")
    cat = env.catalog
    cat.save_index_catalog({"SPX": {"name": "SPX", "categories": ["Index"], "timeframes": [],
                                    "timeframe_dates": {}}})
    saves = []
    monkeypatch.setattr(tool, "_index_target", lambda original=tool._index_target: (
        lambda t: (t[0], lambda c: (saves.append(1), t[1](c)), t[2], t[3]))(original()))
    monkeypatch.setattr(tool, "YahooFetcher", lambda: FakeYahoo())
    monkeypatch.setattr(sys, "argv", ["download_yf.py", "--asset-class", "Index", "--timeframes", "1d",
                                      "--since", "2024-01-01", "--sleep-seconds", "0"])
    tool.main()
    assert saves == [1]
    assert not cat.stock_catalog_path("sp500").exists()


# --- IBKR stock tool -----------------------------------------------------------

class FakeIbkr:
    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def resolve_us_stock_contract(self, symbol):
        return symbol

    def head_timestamp(self, contract, what_to_show):
        return None


def _run_ibkr(env, tool, monkeypatch, tmp_path, ohlcv, during, universe, *symbols):
    def fake_download(*a, **k):
        during()
        return ohlcv(start_ms=1_577_836_800_000, n=3)      # 2020-01-01..03

    monkeypatch.setattr(tool, "IbkrHistoricalClient", FakeIbkr)
    monkeypatch.setattr(tool, "download_ranges", fake_download)
    monkeypatch.setattr(sys, "argv", [
        "download_ibkr_stock_overnight.py", "--universe", universe, "--symbols", *symbols,
        "--timeframes", "1d", "--since", "2020-01-01", "--sleep-seconds", "0",
        "--no-head-timestamp", "--hints-file", str(tmp_path / "hints.json"),
        "--progress-file", str(tmp_path / "progress.json"), "--resume-log", str(tmp_path / "resume.jsonl"),
    ])
    tool.main()


def test_ibkr_cannot_regress_yf_dates_on_the_symbol_or_its_home(env, monkeypatch, tmp_path, ohlcv):
    """YF moves on while IBKR is fetching; IBKR's write must not take that back.

    The old code stored IBKR's in-memory symbol node, loaded before YF's update.
    """
    tool = env.tool("download_ibkr_stock_overnight")
    env.seed("sp500", "AAPL", "OTHER")
    env.seed("nasdaq100", "AAPL")
    for universe in ("sp500", "nasdaq100"):
        env.worker_writes(universe, "AAPL", provider="YF", start="2000-01-01", end="2023-01-01")

    def yf_moves_on():
        for universe in ("sp500", "nasdaq100"):
            env.worker_writes(universe, "AAPL", provider="YF", start="2000-01-01", end="2024-06-30")
        env.worker_writes("sp500", "OTHER", provider="YF", start="2000-01-01", end="2024-06-30")

    _run_ibkr(env, tool, monkeypatch, tmp_path, ohlcv, yf_moves_on, "nasdaq100", "AAPL")

    for universe in ("sp500", "nasdaq100"):
        assert env.node(universe, "AAPL", "1d", "YF")["end_date"] == "2024-06-30"
        ibkr = env.node(universe, "AAPL", "1d", "IBKR")
        assert (ibkr["start_date"], ibkr["end_date"]) == ("2020-01-01", "2020-01-03")
        assert ibkr["file_path"].endswith("ibkr/sp500/AAPL/AAPL-all-1d.parquet")  # the shared file
        entry = env.catalog.load_stock_catalog(universe)["AAPL"]
        assert (entry["start_date"], entry["end_date"]) == ("2000-01-01", "2024-06-30")
    assert env.node("sp500", "OTHER", "1d", "YF")["end_date"] == "2024-06-30"


def test_ibkr_flushes_on_the_way_out_of_a_dead_session(env, monkeypatch, tmp_path, ohlcv):
    """`sys.exit(75)` and every `continue` skip the end of the symbol's loop body."""
    tool = env.tool("download_ibkr_stock_overnight")
    env.seed("sp500", "AAA")
    with pytest.raises(RuntimeError):
        with tool.PendingStockWrites() as pending:
            pending.add("sp500", lambda fresh: fresh["AAA"].update(note=1))
            raise RuntimeError("session died")
    assert env.catalog.load_stock_catalog("sp500")["AAA"]["note"] == 1


def test_a_flush_that_fails_keeps_its_mutators_for_the_retry(env):
    cat = env.catalog
    env.seed("sp500", "AAA")
    pending = cat.PendingStockWrites()
    calls = []

    def flaky(fresh):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("try again")
        fresh["AAA"]["note"] = 1

    pending.add("sp500", flaky)
    with pytest.raises(OSError):
        pending.flush()
    pending.flush()
    assert cat.load_stock_catalog("sp500")["AAA"]["note"] == 1


# --- scans: no lock held, and nothing a worker wrote is undone -------------------

def _bars(env, ohlcv, universe, symbol, provider="IBKR", tf="1d", session=None, n=3):
    path = env.catalog.stock_file_path(provider, universe, symbol, tf, session)
    env.catalog.write_parquet_atomic(ohlcv(n=n), path)
    return path


def _lock_is_free(env, universe):
    with open(env.catalog.stock_catalog_path(universe).with_name(f"{universe}.yaml.lock"), "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(f, fcntl.LOCK_UN)
    return True


def _stale_node(env, universe, symbol, path):
    env.worker_writes(universe, symbol, start="1960-01-01", end="1960-01-02", path=path)


def _during_scan(module, monkeypatch, once):
    """Run `once` the first time `module` reads a parquet file."""
    real = module.read_parquet_range
    fired = []

    def read(path):
        if not fired:
            fired.append(path)
            once()
        return real(path)

    monkeypatch.setattr(module, "read_parquet_range", read)


def test_refresh_stock_catalog_dates_merges_with_a_worker(env, monkeypatch, ohlcv):
    cat = env.catalog
    env.seed("russell2000", "AAA", "BBB")
    path = _bars(env, ohlcv, "russell2000", "AAA")
    _stale_node(env, "russell2000", "AAA", path)

    def worker():
        assert _lock_is_free(env, "russell2000")
        env.worker_writes("russell2000", "BBB")

    _during_scan(cat, monkeypatch, worker)
    assert cat.refresh_stock_catalog_dates("russell2000") == 1
    assert env.node("russell2000", "AAA", "1d", "IBKR")["end_date"] == "1970-01-03"
    assert env.node("russell2000", "BBB", "1d", "IBKR")


def test_a_refresh_does_not_undo_newer_dates(env, monkeypatch, ohlcv):
    """The scan read the file, then a worker extended it and wrote its node."""
    cat = env.catalog
    env.seed("sp500", "AAA")
    path = _bars(env, ohlcv, "sp500", "AAA")
    _stale_node(env, "sp500", "AAA", path)
    _during_scan(cat, monkeypatch, lambda: None)
    fixes = cat.scan_date_fixes(cat.load_stock_catalog("sp500"))
    assert len(fixes) == 1
    env.worker_writes("sp500", "AAA", start="1970-01-01", end="1980-01-01", path=path)
    with cat.stock_catalog_txn("sp500") as fresh:
        assert cat.apply_date_fixes(fresh, fixes) == 0
    assert env.node("sp500", "AAA", "1d", "IBKR")["end_date"] == "1980-01-01"


def test_refresh_dates_for_files_merges_with_a_worker(env, monkeypatch, ohlcv):
    cat = env.catalog
    env.seed("sp500", "AAA", "BBB")
    path = _bars(env, ohlcv, "sp500", "AAA")
    _stale_node(env, "sp500", "AAA", path)

    def worker():
        assert _lock_is_free(env, "sp500")
        env.worker_writes("sp500", "BBB")

    _during_scan(cat, monkeypatch, worker)
    assert cat.refresh_dates_for_files([path]) == ["sp500"]
    assert env.node("sp500", "AAA", "1d", "IBKR")["end_date"] == "1970-01-03"
    assert env.node("sp500", "BBB", "1d", "IBKR")


def _stock_spec(env, name):
    return next(s for s in env.catalog.catalog_registry() if s.name == name)


def test_check_catalog_fix_merges_with_a_worker(env, monkeypatch, ohlcv):
    cat = env.catalog
    checker = env.tool("check_catalog")
    env.seed("sp500", "AAA", "BBB", "CCC")
    drifted = _bars(env, ohlcv, "sp500", "AAA")
    newer = _bars(env, ohlcv, "sp500", "CCC")
    _stale_node(env, "sp500", "AAA", drifted)
    _stale_node(env, "sp500", "CCC", newer)
    # Lacks semantics, and is flat: both fixes must reach the file too.
    data = cat.load_stock_catalog("sp500")
    node = data["AAA"]["timeframe_dates"]["1d"]["IBKR"]
    del node["schema"]
    data["AAA"]["timeframe_dates"]["1d"] = node
    cat.save_stock_catalog("sp500", data)

    def worker():
        assert _lock_is_free(env, "sp500")
        env.worker_writes("sp500", "BBB")
        env.worker_writes("sp500", "CCC", start="1970-01-01", end="1980-01-01", path=newer)

    _during_scan(checker, monkeypatch, worker)
    audit, modified = checker.audit_catalog(_stock_spec(env, "sp500"), fix=True, check_dates=True)

    assert modified
    assert {i.kind for i in audit.issues} >= {"legacy_shape", "semantics", "date_drift"}
    fixed = env.node("sp500", "AAA", "1d", "IBKR")
    assert (fixed["schema"], fixed["end_date"]) == ("ohlcv", "1970-01-03")
    assert env.node("sp500", "BBB", "1d", "IBKR")
    assert env.node("sp500", "CCC", "1d", "IBKR")["end_date"] == "1980-01-01"


def test_dedupe_merges_with_a_worker_and_is_in_the_equity_lane(env, monkeypatch, ohlcv):
    cat = env.catalog
    dd = env.tool("dedupe_stock_universes")
    env.seed("sp500", "AAA")
    env.seed("nasdaq100", "AAA", "ZZZ")
    src = _bars(env, ohlcv, "nasdaq100", "AAA", session="all")
    env.worker_writes("nasdaq100", "AAA", session="all", path=src)

    def worker():
        assert _lock_is_free(env, "nasdaq100")
        env.worker_writes("nasdaq100", "ZZZ", session="all")

    _during_scan(dd, monkeypatch, worker)
    monkeypatch.setattr(sys, "argv", ["dedupe_stock_universes.py", "--apply", "--universe", "nasdaq100"])
    assert dd.main() == 0

    assert not src.exists()
    assert env.node("nasdaq100", "AAA", "1d", "IBKR")["file_path"].endswith("ibkr/sp500/AAA/AAA-all-1d.parquet")
    assert env.node("nasdaq100", "ZZZ", "1d", "IBKR")

    from pdm.jobs import TOOL_LANES
    assert TOOL_LANES["dedupe_stock_universes"] == "equity"


# --- orphan adoption -----------------------------------------------------------

def _yf_sibling(env, ohlcv, universe, symbol, adjustment="raw", tf="1h"):
    """A registered YF node for the symbol, what an orphan's adjustment is read from."""
    path = _bars(env, ohlcv, universe, symbol, "YF", tf)
    with env.catalog.stock_catalog_txn(universe) as fresh:
        env.catalog.update_stock_timeframe_entry(
            fresh, symbol, tf, "YF", path, "1970-01-01", "1970-01-03",
            semantics=replace(importlib.import_module("utils.semantics").semantics_for("Stock", "YF"),
                              adjustment=adjustment),
        )
    return path


def _adopt(env):
    checker = env.tool("check_catalog")
    specs = env.catalog.catalog_registry()
    adopted, written = checker.adopt_orphans(checker.find_orphans(set()), specs)
    left = checker.find_orphans({
        str(p.resolve()) for p in adopted
    })
    return adopted, written, left


def test_stock_orphans_are_adopted_from_their_paths(env, ohlcv):
    env.seed("sp500", "AAPL", "BRK-B")
    siblings = [_yf_sibling(env, ohlcv, "sp500", s) for s in ("AAPL", "BRK-B")]
    yf = _bars(env, ohlcv, "sp500", "AAPL", "YF")
    ibkr = _bars(env, ohlcv, "sp500", "AAPL", tf="1h", session="all")
    overnight = _bars(env, ohlcv, "sp500", "AAPL", tf="1h", session="overnight")
    dashed = _bars(env, ohlcv, "sp500", "BRK-B", "YF")

    adopted, written, left = _adopt(env)

    assert sorted(adopted) == sorted([yf, ibkr, overnight, dashed])
    assert written == ["sp500"] and sorted(left) == sorted(siblings)
    node = env.node("sp500", "AAPL", "1d", "YF")
    assert (node["provider"], node["start_date"], node["end_date"]) == ("YF", "1970-01-01", "1970-01-03")
    assert node["schema"] == "ohlcv" and "session" not in node
    assert env.node("sp500", "AAPL", "1h", "IBKR")["session"] == "all"
    assert env.node("sp500", "AAPL", "1h", "IBKR:overnight")["session"] == "overnight"
    assert env.node("sp500", "BRK-B", "1d", "YF")
    entry = env.catalog.load_stock_catalog("sp500")["AAPL"]
    assert entry["timeframes"] == sorted(entry["timeframes"], key=["1h", "1d"].index)

    checker = env.tool("check_catalog")
    audit, _ = checker.audit_catalog(_stock_spec(env, "sp500"), fix=False, check_dates=True)
    assert audit.issues == []
    assert checker.find_orphans(audit.referenced_paths) == []


def test_a_shared_ibkr_file_goes_to_its_home_and_the_catalogs_that_point_at_it(env, ohlcv):
    env.seed("sp500", "AAPL")
    env.seed("nasdaq100", "AAPL")
    env.seed("russell2000", "OTHER")
    path = _bars(env, ohlcv, "sp500", "AAPL", session="all")

    adopted, written, _ = _adopt(env)

    assert adopted == [path] and written == ["nasdaq100", "sp500"]
    for universe in ("sp500", "nasdaq100"):
        assert env.node(universe, "AAPL", "1d", "IBKR")["file_path"].endswith("ibkr/sp500/AAPL/AAPL-all-1d.parquet")
    assert "AAPL" not in env.catalog.load_stock_catalog("russell2000")


def test_an_ibkr_copy_under_a_later_universe_stays_reported(env, ohlcv):
    """A duplicate for dedupe_stock_universes: the catalog's node must not move to it."""
    env.seed("sp500", "AAPL")
    env.seed("nasdaq100", "AAPL")
    home = _bars(env, ohlcv, "sp500", "AAPL", session="all")
    duplicate = _bars(env, ohlcv, "nasdaq100", "AAPL", session="all")
    env.worker_writes("nasdaq100", "AAPL", session="all", path=home)
    env.worker_writes("sp500", "AAPL", session="all", path=home)

    checker = env.tool("check_catalog")
    specs = env.catalog.catalog_registry()
    assert checker.adopt_orphans([duplicate], specs) == ([], [])
    assert env.node("nasdaq100", "AAPL", "1d", "IBKR")["file_path"].endswith("ibkr/sp500/AAPL/AAPL-all-1d.parquet")


def test_yf_copies_stay_in_their_own_universe(env, ohlcv):
    """YF is not shared: its copies can differ in adjustment."""
    env.seed("sp500", "AAPL")
    env.seed("nasdaq100", "AAPL")
    _yf_sibling(env, ohlcv, "nasdaq100", "AAPL")
    path = _bars(env, ohlcv, "nasdaq100", "AAPL", "YF")

    adopted, written, _ = _adopt(env)

    assert adopted == [path] and written == ["nasdaq100"]
    assert env.catalog.load_stock_catalog("sp500")["AAPL"]["timeframe_dates"] == {}


def test_an_adopted_yf_file_takes_the_adjustment_of_its_symbols_other_nodes(env, ohlcv):
    env.seed("sp500", "AAPL")
    _yf_sibling(env, ohlcv, "sp500", "AAPL", adjustment="adjusted")
    path = _bars(env, ohlcv, "sp500", "AAPL", "YF")

    adopted, _, _ = _adopt(env)

    assert adopted == [path]
    assert env.node("sp500", "AAPL", "1d", "YF")["adjustment"] == "adjusted"


def test_a_yf_file_nothing_can_label_stays_reported(env, ohlcv):
    """Adopting it would state an adjustment nobody knows."""
    env.seed("sp500", "AAPL", "MSFT")
    _bars(env, ohlcv, "sp500", "AAPL", "YF")                 # no other node
    _yf_sibling(env, ohlcv, "sp500", "MSFT", "adjusted", "1h")
    _yf_sibling(env, ohlcv, "sp500", "MSFT", "raw", "1w")    # other nodes disagree
    _bars(env, ohlcv, "sp500", "MSFT", "YF")

    adopted, written, _ = _adopt(env)

    assert adopted == [] and written == []
    catalog = env.catalog.load_stock_catalog("sp500")
    assert catalog["AAPL"]["timeframe_dates"] == {} and "1d" not in catalog["MSFT"]["timeframe_dates"]


def test_an_ibkr_file_with_no_other_node_gets_the_workers_default(env, ohlcv):
    env.seed("sp500", "AAPL")
    path = _bars(env, ohlcv, "sp500", "AAPL")

    adopted, _, _ = _adopt(env)

    assert adopted == [path]
    assert env.node("sp500", "AAPL", "1d", "IBKR")["adjustment"] == "raw"


def test_a_universe_that_does_not_list_a_symbol_others_list_gets_no_entry(env, ohlcv):
    """Adding it would claim membership and move the symbol's IBKR home."""
    env.seed("sp500", "OTHER")
    env.seed("nasdaq100", "XYZ")
    _bars(env, ohlcv, "sp500", "XYZ", session="all")
    _bars(env, ohlcv, "sp500", "XYZ", "YF")

    adopted, written, _ = _adopt(env)

    assert adopted == [] and written == []
    assert "XYZ" not in env.catalog.load_stock_catalog("sp500")
    assert env.catalog.stock_home_universe("XYZ", "nasdaq100") == "nasdaq100"


def test_a_symbol_nobody_lists_is_registered_where_its_path_says(env, ohlcv):
    env.seed("russell2000", "OTHER")
    path = _bars(env, ohlcv, "russell2000", "LOST", session="all")
    adopted, written, _ = _adopt(env)
    assert adopted == [path] and written == ["russell2000"]
    assert env.node("russell2000", "LOST", "1d", "IBKR")


def test_a_node_pointing_elsewhere_is_never_replaced(env, ohlcv):
    """CRWD-style: the later universe's node holds its own price-scale conflict copy."""
    env.seed("sp500", "AAA")
    env.seed("nasdaq100", "AAA")
    home = _bars(env, ohlcv, "sp500", "AAA", session="all")
    own = _bars(env, ohlcv, "nasdaq100", "AAA", session="all")
    env.worker_writes("nasdaq100", "AAA", session="all", path=own)
    other = env.catalog.stock_file_path("IBKR", "sp500", "AAA", "1d", "overnight")
    env.catalog.write_parquet_atomic(ohlcv(n=2), other)
    env.worker_writes("sp500", "AAA", path=other)

    checker = env.tool("check_catalog")
    specs = env.catalog.catalog_registry()
    assert checker.adopt_orphans([home], specs) == ([], [])

    env.seed("sp500", "BBB")
    env.seed("nasdaq100", "BBB")
    home = _bars(env, ohlcv, "sp500", "BBB", session="all")
    own = _bars(env, ohlcv, "nasdaq100", "BBB", session="all")
    env.worker_writes("nasdaq100", "BBB", session="all", path=own)
    adopted, _ = checker.adopt_orphans([home], specs)
    assert adopted == [home]
    assert env.node("nasdaq100", "BBB", "1d", "IBKR")["file_path"].endswith("ibkr/nasdaq100/BBB/BBB-all-1d.parquet")


def test_a_home_that_gained_a_node_keeps_later_catalogs_out(env, monkeypatch, ohlcv):
    """The home turned the file down, so a later catalog must not point at it."""
    checker = env.tool("check_catalog")
    env.seed("sp500", "AAA")
    env.seed("nasdaq100", "AAA")
    path = _bars(env, ohlcv, "sp500", "AAA", session="all")
    elsewhere = env.catalog.stock_file_path("IBKR", "sp500", "AAA", "1h", "all")
    _during_scan(checker, monkeypatch, lambda: env.worker_writes(
        "sp500", "AAA", session="all", path=elsewhere))
    assert checker.adopt_orphans([path], env.catalog.catalog_registry())[0] == []
    assert env.catalog.load_stock_catalog("nasdaq100")["AAA"]["timeframe_dates"] == {}


def test_a_node_a_worker_wrote_since_the_snapshot_survives(env, monkeypatch, ohlcv):
    checker = env.tool("check_catalog")
    env.seed("sp500", "AAA")
    path = _bars(env, ohlcv, "sp500", "AAA", session="all")
    _during_scan(checker, monkeypatch, lambda: env.worker_writes(
        "sp500", "AAA", start="1970-01-01", end="1980-01-01", path=path, session="all"))
    adopted, _ = checker.adopt_orphans([path], env.catalog.catalog_registry())
    assert adopted == [path]
    assert env.node("sp500", "AAA", "1d", "IBKR")["end_date"] == "1980-01-01"


def test_empty_and_non_canonical_stock_files_are_left_alone(env, ohlcv):
    env.seed("sp500", "AAA")
    checker = env.tool("check_catalog")
    cat = env.catalog
    empty = cat.stock_file_path("YF", "sp500", "AAA", "1d")
    empty.parent.mkdir(parents=True)
    pd.DataFrame().to_parquet(empty)
    odd = [
        cat.DATA_ROOT / "stocks" / "ibkr" / "sp500" / "AAA" / "AAA-weird-1d.parquet",
        cat.DATA_ROOT / "stocks" / "yf" / "sp500" / "AAA" / "AAA-all-1d.parquet",
        cat.DATA_ROOT / "stocks" / "ibkr" / "sp500" / "AAA" / "AAA-1fortnight.parquet",
        cat.DATA_ROOT / "stocks" / "ibkr" / "unlisted" / "AAA" / "AAA-all-1d.parquet",
        cat.DATA_ROOT / "stocks" / "polygon" / "sp500" / "AAA" / "AAA-1d.parquet",
        cat.DATA_ROOT / "stocks" / "ibkr" / "sp500" / "AAA" / "BBB-all-1d.parquet",
    ]
    for path in odd:
        cat.write_parquet_atomic(ohlcv(n=2), path)

    adopted, written = checker.adopt_orphans([empty, *odd], cat.catalog_registry())
    assert (adopted, written) == ([], [])
