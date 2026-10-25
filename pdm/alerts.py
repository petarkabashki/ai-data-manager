"""Tell the owner when something needs a look.

Two things raise an alert:

* **job**: a run ended `failed` or `timed_out`. A scheduled job alerts once.
  While its schedule keeps failing it stays quiet, and the next success sends
  one `recovered` alert. A one-off job (a client submit, a refresh) always
  alerts.
* **stale**: series that were fresh at the previous check and are now
  `stale_days` or more behind their catalog `end_date`. A series that stopped
  updating long ago never alerts, only one that stops now. Such a series
  sends `recovered` once it is fresh again. The first check after a fresh
  install only records which series are fresh.

Every alert is appended to `<state_dir>/pdm_alerts.jsonl` (the newest `keep`)
and published on `server/alert`. When `notify_cmd` is set, the alert is also
handed to that command. The command is split with shlex and run without a
shell; it gets the one-line text on stdin and the whole alert as JSON in
`$PDM_ALERT`. For example:

    PDM_NOTIFY_CMD='curl -fsS -d @- https://ntfy.sh/<topic>'

A failed notify is logged and not retried; the alert is already in the file.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import threading
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from pathlib import Path

from pdm.protocol import new_id
from utils.planning import days_stale

log = logging.getLogger("pdm.alerts")

FAILED = {"failed", "timed_out"}
MAX_TEXT = 400


def series_key(row: dict) -> str:
    return "/".join(str(row.get(k) or "-") for k in ("catalog", "symbol", "timeframe", "provider", "session"))


def log_tail(path: str | None, lines: int = 3) -> list[str]:
    """The last non-empty lines of a job log (the error, usually)."""
    if not path:
        return []
    try:
        with open(path, "rb") as fh:
            fh.seek(0, os.SEEK_END)
            fh.seek(max(0, fh.tell() - 8192))
            text = fh.read().decode("utf-8", "replace")
    except OSError:
        return []
    return [ln.strip()[:200] for ln in text.splitlines() if ln.strip()][-lines:]


class Alerter:
    def __init__(
        self,
        publish: Callable[[str, dict], None],
        state_dir: Path,
        notify_cmd: str = "",
        stale_days: int = 4,
        keep: int = 200,
    ):
        self.publish = publish
        self.log_path = Path(state_dir) / "pdm_alerts.jsonl"
        self.state_path = Path(state_dir) / "pdm_alerts_state.json"
        self.notify = shlex.split(notify_cmd) if notify_cmd else []
        self.stale_days = stale_days
        self.keep = keep
        self._lock = threading.Lock()
        self._failing: set[str] = set()          # schedules whose last run failed
        self._fresh: set[str] | None = None      # None until the first stale check
        self._stale: set[str] = set()            # stale series already alerted
        self._load()

    # --- triggers -------------------------------------------------------------

    def on_job(self, job) -> dict | None:
        """Call with every job event; alerts on failures and recoveries."""
        schedule = job.source if job.source.startswith("schedule:") else None
        with self._lock:
            failed = job.state in FAILED and schedule not in self._failing
            recovered = job.state == "succeeded" and schedule in self._failing
            if schedule and (failed or recovered):
                (self._failing.add if failed else self._failing.discard)(schedule)
                self._save()
        if recovered:
            return self.raise_("recovered", f"{schedule} succeeded again ({job.tool}, job {job.id})",
                               job=job.id, source=schedule)
        if not failed:
            return None
        exit_part = f" exit {job.returncode}" if job.returncode is not None else ""
        tail = log_tail(job.log_path)
        why = job.error or (tail[-1] if tail else "")
        text = f"{job.tool} {job.state}{exit_part} [{job.source}] job {job.id}" + (f": {why}" if why else "")
        return self.raise_("job", text, job=job.id, tool=job.tool, args=job.args, state=job.state,
                           source=job.source, returncode=job.returncode, log_tail=tail)

    def check_stale(self, rows: Iterable[dict]) -> list[dict]:
        """Diff series freshness against the previous check; returns the alerts raised."""
        rows = list(rows)
        stale_rows = {series_key(r): r for r in rows
                      if (d := days_stale(r.get("end_date"))) is not None and d >= self.stale_days}
        fresh = {series_key(r) for r in rows} - set(stale_rows)
        alerts = []
        with self._lock:
            first = self._fresh is None
            newly = [] if first else sorted(set(stale_rows) & self._fresh)
            back = sorted(self._stale & fresh)
            self._fresh = fresh
            self._stale = (self._stale | set(newly)) & set(stale_rows)
            self._save()
        if newly:
            alerts.append(self.raise_(
                "stale", f"{len(newly)} series stopped updating (>= {self.stale_days}d behind): "
                + groups(stale_rows[k] for k in newly), series=newly[:200], count=len(newly)))
        if back:
            alerts.append(self.raise_("recovered", f"{len(back)} stale series are fresh again",
                                      series=back[:200], count=len(back)))
        if first:
            log.info("stale check baseline: %d fresh, %d stale series", len(fresh), len(stale_rows))
        return alerts

    # --- output ---------------------------------------------------------------

    def raise_(self, kind: str, text: str, **extra) -> dict:
        alert = {"type": "alert", "kind": kind, "id": new_id(),
                 "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                 "text": text[:MAX_TEXT], **extra}
        log.warning("alert %s: %s", kind, alert["text"])
        self._append(alert)
        self.publish("server/alert", alert)
        if self.notify:
            threading.Thread(target=self._notify, args=(alert,), name="notify", daemon=True).start()
        return alert

    def recent(self, limit: int = 50) -> list[dict]:
        """The newest alerts, newest first."""
        try:
            lines = self.log_path.read_text().splitlines()
        except OSError:
            return []
        out = []
        for line in reversed(lines):
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
            if len(out) >= limit:
                break
        return out

    def _notify(self, alert: dict) -> None:
        try:
            proc = subprocess.run(self.notify, input=alert["text"], text=True, capture_output=True, check=False,
                                  timeout=30, env={**os.environ, "PDM_ALERT": json.dumps(alert)})
            if proc.returncode:
                log.error("notify exit %s: %s", proc.returncode, proc.stderr.strip()[-300:])
        except (OSError, subprocess.SubprocessError) as exc:
            log.error("notify failed: %s", exc)

    # --- persistence ----------------------------------------------------------

    def _append(self, alert: dict) -> None:
        with self._lock:
            try:
                self.log_path.parent.mkdir(parents=True, exist_ok=True)
                with self.log_path.open("a") as fh:
                    fh.write(json.dumps(alert) + "\n")
                lines = self.log_path.read_text().splitlines()
                if len(lines) > 2 * self.keep:     # trim now and then, not on every append
                    _write_atomic(self.log_path, "\n".join(lines[-self.keep:]) + "\n")
            except OSError as exc:
                log.error("cannot record alert: %s", exc)

    def _load(self) -> None:
        try:
            raw = json.loads(self.state_path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        self._failing = set(raw.get("failing") or [])
        self._fresh = set(raw["fresh"]) if raw.get("fresh") is not None else None
        self._stale = set(raw.get("stale") or [])

    def _save(self) -> None:
        state = {"failing": sorted(self._failing), "stale": sorted(self._stale),
                 "fresh": sorted(self._fresh) if self._fresh is not None else None}
        try:
            _write_atomic(self.state_path, json.dumps(state))
        except OSError as exc:
            log.error("cannot save alert state: %s", exc)


def groups(rows: Iterable[dict], top: int = 6) -> str:
    """'crypto_binance 1d x12, sp500 1d x3, +2 more'."""
    counts = Counter(f"{r['catalog']} {r['timeframe']}" for r in rows)
    parts = [f"{name} x{n}" for name, n in counts.most_common(top)]
    if len(counts) > top:
        parts.append(f"+{len(counts) - top} more")
    return ", ".join(parts)


def _write_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    tmp.replace(path)
