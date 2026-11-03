import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import dm_triage as dm  # noqa: E402


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(dm, "STATE", tmp_path)
    monkeypatch.setattr(dm, "SPOOL", tmp_path / "events.jsonl")
    monkeypatch.setattr(dm, "ACKS", tmp_path / "acks.jsonl")
    monkeypatch.setattr(dm, "BATCH_SECS", 90)
    monkeypatch.setattr(dm, "ACK_SECS", 240)
    out = {"desktop": [], "slack": [], "agent": []}
    monkeypatch.setattr(dm, "_desktop", lambda sev, text: out["desktop"].append((sev, text)))
    monkeypatch.setattr(dm, "_slack", lambda text: out["slack"].append(text) or True)
    monkeypatch.setattr(dm, "AGENT_ADDRESS", "proj/Triage")
    monkeypatch.setattr(dm, "send_to_agent", lambda text: out["agent"].append(text) or True)
    return out


def ev(sev="critical", state="raised", key="k", ts=None):
    e = dm.make_event("ibkr", key, sev, state, f"{key} problem")
    if ts:
        e["ts"] = ts
    return e


def test_new_problem_waits_for_the_batch_window_then_wakes_the_agent_once(env):
    t0 = 1_000_000.0
    first = ev(ts=dm.datetime.fromtimestamp(t0, dm.timezone.utc).isoformat())
    state = dm.dispatch_step({}, t0 + 10, [first])
    assert env["agent"] == []                       # too soon: related events may still arrive
    second = ev(key="k2", ts=dm.datetime.fromtimestamp(t0 + 30, dm.timezone.utc).isoformat())
    state = dm.dispatch_step(state, t0 + 40, [second])
    state = dm.dispatch_step(state, t0 + 95, [])
    assert len(env["agent"]) == 1                   # one wake covering both
    assert first["id"] in env["agent"][0] and second["id"] in env["agent"][0]
    assert state["pending"] == []


def test_info_events_never_wake_the_agent(env):
    t0 = 1_000_000.0
    info = ev(sev="info", ts=dm.datetime.fromtimestamp(t0, dm.timezone.utc).isoformat())
    dm.dispatch_step({}, t0 + 500, [info])
    assert env["agent"] == [] and env["desktop"] == []


def test_agent_ack_clears_the_batch_and_nothing_falls_back(env):
    t0 = 1_000_000.0
    e = ev(ts=dm.datetime.fromtimestamp(t0, dm.timezone.utc).isoformat())
    state = dm.dispatch_step({}, t0 + 100, [e])
    dm.relay("critical", "agent summary", [e["id"]])           # what `dm relay` does
    env["desktop"].clear()
    state = dm.dispatch_step(state, t0 + 100 + 1000, [])
    assert state["sent"] == [] and env["desktop"] == []         # acked: no raw fallback


def test_silent_agent_triggers_a_direct_relay_after_the_ack_deadline(env):
    t0 = 1_000_000.0
    e = ev(ts=dm.datetime.fromtimestamp(t0, dm.timezone.utc).isoformat())
    state = dm.dispatch_step({}, t0 + 100, [e])
    state = dm.dispatch_step(state, t0 + 100 + 100, [])        # still inside the deadline
    assert env["desktop"] == []
    state = dm.dispatch_step(state, t0 + 100 + 300, [])
    assert len(env["desktop"]) == 1 and "did not respond" in env["desktop"][0][1]
    assert env["slack"] and state["sent"] == []


def test_unreachable_agent_relays_directly_straight_away(env, monkeypatch):
    monkeypatch.setattr(dm, "send_to_agent", lambda text: False)
    t0 = 1_000_000.0
    e = ev(ts=dm.datetime.fromtimestamp(t0, dm.timezone.utc).isoformat())
    dm.dispatch_step({}, t0 + 100, [e])
    assert len(env["desktop"]) == 1 and "unavailable" in env["desktop"][0][1]


def test_pdm_recovered_alert_becomes_a_cleared_info_event():
    out = dm.pdm_alert_to_event({"kind": "recovered", "id": "abc", "time": "t", "text": "40 series fresh again"})
    assert (out["state"], out["severity"], out["source"]) == ("cleared", "info", "pdm")
    failed = dm.pdm_alert_to_event({"kind": "job", "id": "d", "text": "refresh failed"})
    assert (failed["state"], failed["severity"], failed["key"]) == ("raised", "warn", "pdm-job")


def test_remediate_refuses_anything_not_allow_listed(env, capsys):
    args = type("A", (), {"name": "rm-rf"})()
    assert dm.cmd_remediate(args) == 2


def test_remediate_honours_the_cooldown(env, monkeypatch):
    calls = []
    monkeypatch.setitem(dm.REMEDIATIONS, "ibkr-workers-restart", (["true"], 30))
    monkeypatch.setattr(dm.subprocess, "run", lambda *a, **k: calls.append(a) or type(
        "P", (), {"returncode": 0, "stdout": "ok", "stderr": ""})())
    args = type("A", (), {"name": "ibkr-workers-restart"})()
    assert dm.cmd_remediate(args) == 0
    assert dm.cmd_remediate(args) == 3                           # second call inside the cooldown
    assert len(calls) == 1
