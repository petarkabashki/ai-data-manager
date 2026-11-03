#!/usr/bin/env python3
"""Data-management alerting: one event format, a dispatcher, and the relay a triage agent uses.

    sources ──emit──▶ spool ──dispatch──▶ triage agent ──relay──▶ desktop + Slack
                                   └─ no ack in time ──────────▶ relay directly (no LLM needed)

Subcommands
  emit       append one event to the spool (any script or service can call this)
  pdm-poll   turn PDM server alerts into events (polls; needs no token)
  dispatch   batch events, wake the triage agent, fall back to a direct relay if it stays silent
  relay      send a message to the owner (desktop + Slack webhook) and acknowledge event ids
  remediate  run one allow-listed fix, with a shared cooldown

State lives under $DM_STATE_DIR (default $XDG_STATE_HOME/dm-alerts, else ~/.local/state/dm-alerts).
The Slack incoming-webhook URL is read from $DM_SLACK_WEBHOOK_FILE (default
~/.config/ibkr-monitor/slack_webhook), never from the command line or the environment.

Event fields: id, ts, source, key, severity (info|warn|critical), state (raised|cleared),
title, detail, remediation (optional name from the allow-list below).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

STATE = Path(os.environ.get("DM_STATE_DIR")
             or Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "dm-alerts")
WEBHOOK_FILE = Path(os.environ.get("DM_SLACK_WEBHOOK_FILE") or Path.home() / ".config/ibkr-monitor/slack_webhook")
AGENT_ADDRESS = os.environ.get("DM_TRIAGE_ADDRESS", "")     # e.g. ai-data-manager/Triage; empty = no agent, relay directly
BATCH_SECS = int(os.environ.get("DM_BATCH_SECS", "90"))     # how long a new problem waits so related ones group together
ACK_SECS = int(os.environ.get("DM_ACK_SECS", "240"))        # agent silence before the dispatcher relays directly
POLL_SECS = int(os.environ.get("DM_POLL_SECS", "15"))

SPOOL = STATE / "events.jsonl"
DISPATCH_STATE = STATE / "dispatch.json"
ACKS = STATE / "acks.jsonl"

# name -> (argv run locally, cooldown minutes). The only things the triage agent may do.
REMEDIATIONS: dict[str, tuple[list[str], int]] = {
    "ibkr-workers-restart": (
        ["ssh", "-o", "BatchMode=yes", "pdm-host", "bash ~/work/ai-data-manager/tmp/restart_workers.sh"], 30),
}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _append(path: Path, rec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as fh:
        fh.write(json.dumps(rec) + "\n")


def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text().splitlines():
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return out


# --- emit -------------------------------------------------------------------------------------

def make_event(source: str, key: str, severity: str, state: str, title: str,
               detail: str = "", remediation: str = "") -> dict:
    ts = now_iso()
    eid = hashlib.sha1(f"{ts}|{source}|{key}|{state}|{title}".encode()).hexdigest()[:10]
    ev = {"id": eid, "ts": ts, "source": source, "key": key, "severity": severity,
          "state": state, "title": title, "detail": detail}
    if remediation:
        ev["remediation"] = remediation
    return ev


def cmd_emit(a: argparse.Namespace) -> int:
    ev = make_event(a.source, a.key, a.severity, a.state, a.title, a.detail, a.remediation)
    _append(SPOOL, ev)
    print(ev["id"])
    return 0


# --- pdm-poll ---------------------------------------------------------------------------------

def pdm_alert_to_event(alert: dict) -> dict:
    kind = alert.get("kind", "alert")
    state = "cleared" if kind == "recovered" else "raised"
    severity = "info" if state == "cleared" else "warn"
    ev = make_event("pdm", f"pdm-{kind}", severity, state, alert.get("text", "")[:300],
                    detail=json.dumps({k: alert[k] for k in ("id", "time", "job", "series") if k in alert})[:1500])
    ev["pdm_alert_id"] = alert.get("id", "")
    return ev


def cmd_pdm_poll(a: argparse.Namespace) -> int:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from pdm.client import PriceDataClient
    from utils.config import PDM_PUB_PORT, PDM_REQ_PORT, PDM_SERVER, PDM_TOKEN

    seen_path = STATE / "pdm_seen.json"
    seen = set(json.loads(seen_path.read_text())) if seen_path.exists() else None
    client = PriceDataClient(PDM_SERVER, PDM_REQ_PORT, PDM_PUB_PORT, PDM_TOKEN, timeout=60)
    while True:
        try:
            alerts = client.call("alerts", limit=50)
        except Exception as exc:  # server briefly down: try again, the dispatcher notices a dead source separately
            print(f"{now_iso()} pdm alerts unavailable: {exc}", file=sys.stderr)
            alerts = None
        if alerts is not None:
            ids = {x.get("id") for x in alerts}
            if seen is None:                     # first run: record history, don't replay it
                seen = ids
            else:
                for alert in reversed(alerts):   # oldest first
                    if alert.get("id") not in seen:
                        _append(SPOOL, pdm_alert_to_event(alert))
                seen |= ids
            seen_path.parent.mkdir(parents=True, exist_ok=True)
            seen_path.write_text(json.dumps(sorted(x for x in seen if x)[-500:]))
        if a.once:
            return 0
        time.sleep(a.interval)


# --- relay (also used by the triage agent) ---------------------------------------------------

def _desktop(severity: str, text: str) -> None:
    urgency = {"critical": "critical", "warn": "normal"}.get(severity, "low")
    subprocess.run(["notify-send", "-u", urgency, "-a", "Data management", "Data management", text],
                   capture_output=True, check=False, timeout=10)


def _slack(text: str) -> bool:
    if not WEBHOOK_FILE.exists():
        return False
    import urllib.request
    req = urllib.request.Request(WEBHOOK_FILE.read_text().strip(), data=json.dumps({"text": text}).encode(),
                                 headers={"Content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return 200 <= resp.status < 300
    except OSError:
        return False


def relay(severity: str, text: str, ids: list[str], via: str = "agent") -> None:
    _desktop(severity, text)
    ok = _slack(text)
    _append(STATE / "relay.log.jsonl", {"ts": now_iso(), "severity": severity, "via": via, "slack": ok,
                                         "ids": ids, "text": text})
    for eid in ids:
        _append(ACKS, {"ts": now_iso(), "id": eid, "via": via})


def cmd_relay(a: argparse.Namespace) -> int:
    ids = [x for x in (a.ids or "").split(",") if x]
    relay(a.severity, a.text, ids, via="agent")
    print("relayed" + ("" if WEBHOOK_FILE.exists() else " (desktop only: no slack webhook configured)"))
    return 0


# --- remediate --------------------------------------------------------------------------------

def cmd_remediate(a: argparse.Namespace) -> int:
    if a.name not in REMEDIATIONS:
        print(f"refused: {a.name!r} is not allow-listed ({', '.join(REMEDIATIONS)})", file=sys.stderr)
        return 2
    argv, cooldown = REMEDIATIONS[a.name]
    stamp = STATE / f"remediate-{a.name}.ts"
    if stamp.exists() and time.time() - float(stamp.read_text() or 0) < cooldown * 60:
        print(f"skipped: {a.name} already ran within the last {cooldown} min")
        return 3
    STATE.mkdir(parents=True, exist_ok=True)
    stamp.write_text(str(time.time()))
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=300, check=False)
    _append(STATE / "relay.log.jsonl", {"ts": now_iso(), "remediation": a.name, "exit": proc.returncode,
                                         "tail": (proc.stdout + proc.stderr)[-400:]})
    print((proc.stdout + proc.stderr)[-400:].strip() or f"exit {proc.returncode}")
    return proc.returncode


# --- dispatch ---------------------------------------------------------------------------------

def _wake_worthy(ev: dict) -> bool:
    return ev["severity"] in ("warn", "critical")


def format_batch(events: list[dict]) -> str:
    lines = ["New data-management events to triage (oldest first). Follow your instructions: group related ones,",
             "relay a plain-language summary with `dm relay` passing every id below, and use `dm remediate` only for",
             "an event's own listed remediation.", ""]
    for ev in events:
        rem = f" remediation={ev['remediation']}" if ev.get("remediation") else ""
        lines.append(f"- id={ev['id']} {ev['ts']} [{ev['severity']}/{ev['state']}] {ev['source']}:{ev['key']}"
                     f" — {ev['title']}{rem}" + (f"\n    {ev['detail']}" if ev.get("detail") else ""))
    return "\n".join(lines)


def send_to_agent(text: str) -> bool:
    if not AGENT_ADDRESS:
        return False
    proc = subprocess.run(["omnigent", "session", "send", AGENT_ADDRESS, text],
                          capture_output=True, text=True, timeout=60, check=False)
    return proc.returncode == 0


def direct_relay(events: list[dict], why: str) -> None:
    worst = "critical" if any(e["severity"] == "critical" for e in events) else "warn"
    body = "\n".join(f"• [{e['severity']}] {e['source']}: {e['title']}" for e in events[:12])
    more = f"\n…and {len(events) - 12} more" if len(events) > 12 else ""
    relay(worst, f"{why}\n{body}{more}", [e["id"] for e in events], via="direct")


def dispatch_step(state: dict, now: float, new: list[dict]) -> dict:
    """One pass. `state` = {"pending": [events], "sent": [{"ids", "deadline", "events"}]}."""
    pending: list[dict] = state.get("pending", [])
    sent: list[dict] = state.get("sent", [])
    pending += [e for e in new if _wake_worthy(e) or e["state"] == "cleared"]

    acked = {a["id"] for a in _read(ACKS)}
    still_sent = []
    for batch in sent:
        left = [e for e in batch["events"] if e["id"] not in acked]
        if not left:
            continue
        if now >= batch["deadline"]:
            direct_relay(left, "Triage agent did not respond; raw alerts:")
        else:
            still_sent.append({**batch, "events": left})

    # wake the agent once the oldest actionable event has waited BATCH_SECS (so related events group)
    if any(_wake_worthy(e) for e in pending):
        oldest = min(datetime.fromisoformat(e["ts"]).timestamp() for e in pending)
        if now - oldest >= BATCH_SECS:
            if send_to_agent(format_batch(pending)):
                still_sent.append({"events": pending, "deadline": now + ACK_SECS})
            else:
                direct_relay(pending, "Triage agent unavailable; raw alerts:")
            pending = []
    return {"pending": pending, "sent": still_sent}


def cmd_dispatch(a: argparse.Namespace) -> int:
    offset = 0
    state = json.loads(DISPATCH_STATE.read_text()) if DISPATCH_STATE.exists() else {}
    offset = state.pop("offset", SPOOL.stat().st_size if SPOOL.exists() and not state else 0)
    print(f"{now_iso()} dispatching; agent={AGENT_ADDRESS or '(none: direct relay)'} batch={BATCH_SECS}s ack={ACK_SECS}s")
    while True:
        new: list[dict] = []
        if SPOOL.exists():
            with SPOOL.open("rb") as fh:
                fh.seek(offset)
                for line in fh.read().decode("utf-8", "ignore").splitlines():
                    try:
                        new.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
                offset = fh.tell()
        state = dispatch_step(state, time.time(), new)
        DISPATCH_STATE.parent.mkdir(parents=True, exist_ok=True)
        (STATE / "dispatch.heartbeat").touch()      # producers fall back to direct alerts if this goes stale
        DISPATCH_STATE.write_text(json.dumps({**state, "offset": offset}))
        if a.once:
            return 0
        time.sleep(POLL_SECS)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("emit")
    e.add_argument("--source", required=True)
    e.add_argument("--key", required=True)
    e.add_argument("--severity", choices=["info", "warn", "critical"], default="warn")
    e.add_argument("--state", choices=["raised", "cleared"], default="raised")
    e.add_argument("--title", required=True)
    e.add_argument("--detail", default="")
    e.add_argument("--remediation", default="", choices=["", *REMEDIATIONS])
    e.set_defaults(fn=cmd_emit)

    pp = sub.add_parser("pdm-poll")
    pp.add_argument("--interval", type=int, default=60)
    pp.add_argument("--once", action="store_true")
    pp.set_defaults(fn=cmd_pdm_poll)

    d = sub.add_parser("dispatch")
    d.add_argument("--once", action="store_true")
    d.set_defaults(fn=cmd_dispatch)

    r = sub.add_parser("relay")
    r.add_argument("--severity", choices=["info", "warn", "critical"], default="warn")
    r.add_argument("--text", required=True)
    r.add_argument("--ids", default="", help="comma-separated event ids this message covers")
    r.set_defaults(fn=cmd_relay)

    m = sub.add_parser("remediate")
    m.add_argument("name")
    m.set_defaults(fn=cmd_remediate)

    args = p.parse_args()
    return args.fn(args)


if __name__ == "__main__":
    raise SystemExit(main())
