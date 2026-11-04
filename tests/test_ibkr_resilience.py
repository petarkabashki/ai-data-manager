"""Dead-session detection and the shared per-symbol hints."""

import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from utils.ibkr import (
    GatewayUnresponsive,
    IbkrHistoricalClient,
    SymbolHints,
    is_no_security_definition,
    is_pacing_violation,
    is_retryable_chunk_error,
)


def _client(connected=True, max_timeouts=3):
    c = IbkrHistoricalClient("127.0.0.1", 4001, 1, timeout=0.01, max_consecutive_timeouts=max_timeouts)
    c._app = SimpleNamespace(isConnected=lambda: connected)
    return c


def _req(c, answered=False):
    rid = c._new_req_id()
    c._events[rid] = threading.Event()
    if answered:
        c._events[rid].set()
    return rid


def test_timeouts_escalate_to_gateway_unresponsive():
    c = _client()
    for _ in range(2):
        with pytest.raises(TimeoutError):
            c._wait(_req(c))
    with pytest.raises(GatewayUnresponsive):
        c._wait(_req(c))


def test_any_answer_resets_the_timeout_count():
    c = _client()
    for _ in range(2):
        with pytest.raises(TimeoutError):
            c._wait(_req(c))
    c._wait(_req(c, answered=True))
    for _ in range(2):  # would have been the third timeout without the reset
        with pytest.raises(TimeoutError):
            c._wait(_req(c))


def test_closed_socket_is_immediately_dead():
    with pytest.raises(GatewayUnresponsive):
        _client(connected=False)._wait(_req(_client()))


def test_gateway_unresponsive_is_not_retried_as_a_chunk_error():
    assert not is_retryable_chunk_error(GatewayUnresponsive("3 consecutive IBKR request timeouts"))
    assert is_retryable_chunk_error(TimeoutError("IBKR request 5 timed out"))


def test_error_classifiers():
    assert is_pacing_violation(RuntimeError("Historical data request pacing violation"))
    assert is_no_security_definition(RuntimeError("No security definition has been found"))
    assert not is_pacing_violation(RuntimeError("timed out"))


def test_hints_persist_and_merge_between_workers(tmp_path):
    path = tmp_path / "hints.json"
    a, b = SymbolHints(path), SymbolHints(path)
    when = datetime(2004, 8, 19, tzinfo=timezone.utc)
    a.set_head("GOOG", when)
    b.mark_unresolvable("XYZ", "No security definition has been found")
    a.set_head("MSFT", when)  # a's stale view must not drop b's entry
    fresh = SymbolHints(path)
    assert set(fresh.head) == {"GOOG", "MSFT"}
    assert fresh.head["GOOG"] == when
    assert "XYZ" in fresh.unresolvable


def test_downloader_caps_hanging_symbol_retries():
    """The reconnect path must not requeue one hanging symbol forever."""
    src = open("tools/download_ibkr_stock_overnight.py").read()
    assert "dead_hits[symbol] >= 2" in src and "dead_hits[symbol] -= 1" in src


def test_trim_redundant_ranges(tmp_path):
    import os
    import time
    from datetime import timedelta

    from tools.download_ibkr_stock_overnight import trim_redundant_ranges
    from utils.planning import DownloadRange

    t0 = datetime(2000, 1, 1, tzinfo=timezone.utc)
    now = datetime(2026, 11, 3, tzinfo=timezone.utc)
    short_prepend = DownloadRange(t0, t0 + timedelta(days=2), "prepend")
    long_prepend = DownloadRange(t0, t0 + timedelta(days=400), "prepend")
    tail = DownloadRange(now - timedelta(days=5), now, "tail_overlap")
    f = tmp_path / "x.parquet"
    f.write_bytes(b"")
    fresh = trim_redundant_ranges([short_prepend, long_prepend, tail], f, 12)
    assert fresh == [long_prepend]  # just written: no tail refresh, no 2-day prepend
    os.utime(f, (time.time() - 48 * 3600,) * 2)
    stale = trim_redundant_ranges([short_prepend, long_prepend, tail], f, 12)
    assert stale == [long_prepend, tail]
    assert trim_redundant_ranges([tail], tmp_path / "missing.parquet", 12) == [tail]


def test_hints_floor_persists(tmp_path):
    path = tmp_path / "hints.json"
    when = datetime(2010, 11, 15, tzinfo=timezone.utc)
    SymbolHints(path).set_floor("ABUS/1d", when)
    SymbolHints(path).set_head("ABUS", when)  # a later head write must keep the floor
    assert SymbolHints(path).floor["ABUS/1d"] == when


def test_hang_marker_persists_and_expires(tmp_path):
    path = tmp_path / "hints.json"
    SymbolHints(path).mark_hang("ADMA")
    fresh = SymbolHints(path)
    assert fresh.is_hanging("ADMA", 24)
    assert not fresh.is_hanging("ADMA", 0.0)  # window of zero hours: already aged out
    assert not fresh.is_hanging("OTHER", 24)
