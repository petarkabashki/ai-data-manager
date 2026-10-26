"""Recurring jobs from `pdm.yaml`, manageable at runtime.

```yaml
schedules:
  - name: yf-sp500-daily
    refresh:                       # a typed refresh plan (see pdm/refresh.py) …
      provider: yf
      target: {universe: sp500}
      timeframes: [1d]
    at: "22:30"                    # UTC, daily …
    days: [mon, tue, wed, thu, fri]   # … optionally only on these days
  - name: nightly-health
    tool: status                   # … or any whitelisted tool + raw args
    args: [--deep, --json]
    every: 24h                     # fixed interval: 30s, 15m, 1h, 1d
    enabled: false                 # paused; kept in the file
```

Clients list, add, replace, pause, resume, remove and run schedules over the
server (`schedule_*` methods); every change is written back to `pdm.yaml`.

A schedule whose previous run is still queued or running is skipped rather
than queued again, so a slow provider can't build an ever-growing backlog.

Each schedule's last run is saved to `state_path`, so a restart doesn't
reset the clock. Next run = the first due time after the last run, or now if
that has already passed. Before, `every: 8h` counted from server start, and
funding stopped for 16h on a day with four deploys; an `at` slot that fell
while the server was down was skipped until the next day. A schedule that
has never run starts from now, so a fresh install doesn't fire everything.
Times are UTC on purpose: market closes move with daylight saving, and a
schedule that silently shifts by an hour twice a year is worse than one that
says plainly which clock it uses.
"""

from __future__ import annotations

import json
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from datetime import time as dtime
from pathlib import Path

from pdm.jobs import JobManager, validate
from pdm.refresh import DEFAULT_TIMEOUT_S, build_argv

log = logging.getLogger("pdm.scheduler")

DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


def parse_interval(text: str) -> timedelta:
    match = re.fullmatch(r"\s*(\d+)\s*([smhd])\s*", str(text))
    if not match:
        raise ValueError(f"bad interval {text!r}; use e.g. 30s, 15m, 1h, 1d")
    seconds = int(match.group(1)) * _UNITS[match.group(2)]
    if seconds < 10:
        raise ValueError("interval must be at least 10s")
    return timedelta(seconds=seconds)


@dataclass
class Schedule:
    name: str
    tool: str
    args: list[str]
    every: timedelta | None = None
    at: dtime | None = None
    days: frozenset[int] | None = None   # weekday numbers, Monday = 0
    enabled: bool = True
    plan: dict | None = None             # set for refresh-kind schedules
    timeout_s: float | None = None
    raw: dict = field(default_factory=dict)
    next_run: datetime | None = None
    last_run: datetime | None = None
    last_job_id: str | None = None

    @classmethod
    def from_config(cls, raw: dict) -> "Schedule":
        if not isinstance(raw, dict):
            raise ValueError("a schedule must be a mapping")
        name = raw.get("name") or raw.get("tool")
        if not name or not _NAME.match(str(name)):
            raise ValueError(f"schedule name {name!r}: use letters, digits, _ . - (max 64)")
        plan = None
        timeout_s = None
        if raw.get("refresh") is not None:
            if raw.get("tool"):
                raise ValueError(f"schedule {name!r}: set 'refresh' or 'tool', not both")
            plan = dict(raw["refresh"])
            plan.pop("dry_run", None)
            tool, args, _ = build_argv({**plan, "dry_run": False})
            timeout_s = min(float(raw.get("timeout_s", DEFAULT_TIMEOUT_S)), DEFAULT_TIMEOUT_S)
        elif raw.get("tool"):
            tool, args = validate(raw["tool"], list(raw.get("args", [])))
            if raw.get("timeout_s"):
                timeout_s = float(raw["timeout_s"])
        else:
            raise ValueError(f"schedule {name!r}: needs 'refresh' (a plan) or 'tool'")
        every = parse_interval(raw["every"]) if raw.get("every") else None
        at = None
        if raw.get("at"):
            match = re.fullmatch(r"(\d{1,2}):(\d{2})", str(raw["at"]))
            if not match:
                raise ValueError(f"schedule {name!r}: at must be HH:MM (UTC)")
            at = dtime(int(match.group(1)), int(match.group(2)))
        if (every is None) == (at is None):
            raise ValueError(f"schedule {name!r}: set exactly one of 'every' or 'at'")
        days = None
        if raw.get("days"):
            try:
                days = frozenset(DAYS.index(str(d).lower()[:3]) for d in raw["days"])
            except ValueError:
                raise ValueError(f"schedule {name!r}: days must be mon..sun") from None
        enabled = raw.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"schedule {name!r}: enabled must be true or false")
        clean = {k: v for k, v in raw.items() if k in
                 {"name", "refresh", "tool", "args", "every", "at", "days", "enabled", "timeout_s"}}
        clean["name"] = name
        if plan is not None:
            clean["refresh"] = plan
        return cls(name=name, tool=tool, args=args, every=every, at=at, days=days,
                   enabled=enabled, plan=plan, timeout_s=timeout_s, raw=clean)

    def compute_next(self, now: datetime) -> datetime:
        if self.every is not None:
            return now + self.every
        candidate = now.replace(hour=self.at.hour, minute=self.at.minute, second=0, microsecond=0)
        if candidate <= now:
            candidate += timedelta(days=1)
        while self.days is not None and candidate.weekday() not in self.days:
            candidate += timedelta(days=1)
        return candidate

    def public(self) -> dict:
        return {
            "name": self.name,
            "kind": "refresh" if self.plan is not None else "tool",
            "enabled": self.enabled,
            "refresh": self.plan,
            "tool": self.tool,
            "args": self.args,
            "every": self.raw.get("every"),
            "at": self.at.strftime("%H:%M") if self.at else None,
            "days": [DAYS[d] for d in sorted(self.days)] if self.days else None,
            "timeout_s": self.timeout_s,
            "next_run": self.next_run.isoformat() if (self.next_run and self.enabled) else None,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_job_id": self.last_job_id,
        }


def schedule_targets(s: Schedule) -> dict:
    """What a schedule covers, in catalog terms: {catalog, symbols|None}.

    `symbols: None` means "every symbol in the catalog". Used to answer
    "which schedules refresh AAPL?".
    """
    args = s.args
    flags = {}   # --flag=value and --flag value both occur (refresh plans vs hand-written args)
    for i, a in enumerate(args):
        if a.startswith("--") and "=" in a:
            k, v = a[2:].split("=", 1)
            flags[k] = v
        elif a.startswith("--") and i + 1 < len(args) and not args[i + 1].startswith("--"):
            flags[a[2:]] = args[i + 1]
    listed: list[str] | None = None
    if "--symbols" in args or "--pairs" in args:
        flag = "--symbols" if "--symbols" in args else "--pairs"
        i = args.index(flag) + 1
        listed = []
        while i < len(args) and not args[i].startswith("--"):
            listed.append(args[i].replace("/", "_"))
            i += 1
    timeframes = []
    if "--timeframes" in args:
        i = args.index("--timeframes") + 1
        while i < len(args) and not args[i].startswith("--"):
            timeframes.append(args[i])
            i += 1
    catalog = None
    if s.tool == "download_yf":
        if "universe" in flags:
            catalog = flags["universe"]
        elif flags.get("asset-class") == "Index":
            catalog = "indexes"
        elif flags.get("asset-class") == "Commodity":
            catalog = "commodities"
        elif flags.get("asset-class") == "Macro":
            catalog = "macro"
    elif s.tool == "download_funding" and "exchange" in flags:
        catalog = f"crypto_futures_{flags['exchange']}"
        timeframes = ["funding"]
    elif s.tool == "download_crypto" and "exchange" in flags:
        mode = flags.get("trading-mode", "spot")
        catalog = f"crypto_futures_{flags['exchange']}" if mode == "futures" else f"crypto_{flags['exchange']}"
    return {"catalog": catalog, "symbols": listed, "timeframes": timeframes or None}


class Scheduler:
    def __init__(
        self,
        jobs: JobManager,
        schedules: list[Schedule],
        run_on_start: bool = False,
        on_change: Callable[[list[dict]], None] | None = None,
        state_path: Path | None = None,
    ):
        self.jobs = jobs
        self._schedules: dict[str, Schedule] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self.on_change = on_change or (lambda raws: None)
        self.state_path = state_path
        last_runs = self._load_last_runs()
        now = datetime.now(timezone.utc)
        for s in schedules:
            if s.name in self._schedules:
                raise ValueError(f"duplicate schedule name {s.name!r}")
            s.last_run = last_runs.get(s.name)
            if run_on_start and s.every:
                s.next_run = now
            elif s.last_run:
                s.next_run = max(s.compute_next(s.last_run), now)
            else:
                s.next_run = s.compute_next(now)
            self._schedules[s.name] = s
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)

    @property
    def schedules(self) -> list[Schedule]:
        with self._lock:
            return list(self._schedules.values())

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    # --- management -----------------------------------------------------------

    def get(self, name: str) -> Schedule:
        with self._lock:
            if name not in self._schedules:
                raise ValueError(f"no schedule named {name!r}")
            return self._schedules[name]

    def put(self, raw: dict) -> tuple[Schedule, bool]:
        """Create or replace a schedule by name. Returns (schedule, created)."""
        s = Schedule.from_config(raw)
        s.next_run = s.compute_next(datetime.now(timezone.utc))
        with self._lock:
            old = self._schedules.get(s.name)
            if old:
                s.last_run, s.last_job_id = old.last_run, old.last_job_id
            self._schedules[s.name] = s
            self._changed()
        return s, old is None

    def remove(self, name: str) -> Schedule:
        with self._lock:
            s = self.get(name)
            del self._schedules[name]
            self._changed()
        return s

    def set_enabled(self, name: str, enabled: bool) -> Schedule:
        with self._lock:
            s = self.get(name)
            s.enabled = enabled
            s.raw["enabled"] = enabled
            if enabled:
                s.next_run = s.compute_next(datetime.now(timezone.utc))
            self._changed()
        return s

    def run_now(self, name: str):
        """Fire one schedule immediately (even if paused); next_run is unchanged."""
        return self._fire(self.get(name), datetime.now(timezone.utc))

    def _changed(self) -> None:
        self.on_change([s.raw for s in self._schedules.values()])

    # --- last runs, kept across restarts ----------------------------------------

    def _load_last_runs(self) -> dict[str, datetime]:
        if not self.state_path:
            return {}
        try:
            raw = json.loads(self.state_path.read_text())
            return {name: datetime.fromisoformat(ts) for name, ts in (raw.get("last_run") or {}).items()}
        except (OSError, ValueError, AttributeError) as exc:
            if not isinstance(exc, FileNotFoundError):
                log.warning("ignoring unreadable schedule state %s: %s", self.state_path, exc)
            return {}

    def _save_last_runs(self) -> None:
        if not self.state_path:
            return
        with self._lock:
            runs = {s.name: s.last_run.isoformat() for s in self._schedules.values() if s.last_run}
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_name(self.state_path.name + ".tmp")
            tmp.write_text(json.dumps({"version": 1, "last_run": runs}))
            tmp.replace(self.state_path)
        except OSError as exc:
            log.error("cannot save schedule state: %s", exc)

    # --- firing ---------------------------------------------------------------

    def _fire(self, s: Schedule, now: datetime):
        source = f"schedule:{s.name}"
        busy = self.jobs.active_from(source)
        if busy:
            log.info("schedule %s skipped: job %s still %s", s.name, busy.id, busy.state)
            return busy
        job = self.jobs.submit(
            s.tool, s.args, source=source, timeout_s=s.timeout_s,
            json_stdout="--json" in s.args and s.plan is not None,
            meta={"schedule": s.name, "plan": s.plan} if s.plan else {"schedule": s.name},
        )
        s.last_run, s.last_job_id = now, job.id
        self._save_last_runs()
        return job

    def tick(self, now: datetime) -> list[str]:
        """Fire every due, enabled schedule; returns the names fired (or skipped as busy)."""
        fired = []
        with self._lock:
            due = [s for s in self._schedules.values()
                   if s.enabled and s.next_run and s.next_run <= now]
        for s in due:
            self._fire(s, now)
            fired.append(s.name)
            s.next_run = s.compute_next(now)
        return fired

    def _loop(self) -> None:
        while not self._stop.wait(5.0):
            try:
                self.tick(datetime.now(timezone.utc))
            except Exception:
                log.exception("scheduler tick failed")
