"""Queue and run the existing tools as jobs.

A job is one invocation of a script in `tools/`, run as a subprocess with an
argv list (never a shell string), so the server reuses every tool exactly as a
person would run it — same planner, same atomic merge, same catalog writers —
and a crashing download cannot take the server down with it.

Jobs run in **lanes**, one worker thread per lane, serial within it:

  * `equity`  — YF and IBKR both write `sp500.yaml`, `indexes.yaml` …; each
    tool loads a catalog, edits it and saves it, so two concurrent writers to
    one catalog lose updates. Every tool that writes an equity catalog shares
    this lane.
  * `crypto`  — freqtrade spot/futures downloads and conversions.
  * `options`, `marketcap`, `futures` — disjoint catalogs, own lanes.
  * `audit`   — read-only tools; never blocked behind a multi-hour download.

Lanes also respect provider pacing: IBKR punishes parallel connections, and
one lane per provider is exactly one connection.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pdm.protocol import new_id

#: tool name → lane. Anything not listed cannot be run through the server.
TOOL_LANES: dict[str, str] = {
    "download_yf": "equity",
    "download_ibkr_stock_overnight": "equity",
    "download_ibkr_index": "equity",
    "retry_ibkr_failed_sequentially": "equity",
    "check_catalog": "equity",          # --fix writes catalogs
    "download_crypto": "crypto",
    "convert_crypto": "crypto",
    "download_deribit_options": "options",
    "download_market_caps": "marketcap",
    "import_futures": "futures",
    "status": "audit",
    "query": "audit",
    "check_gaps": "audit",
    "check_schema": "audit",
    "check_splits": "audit",
}

#: Tools whose completion cannot have written bars — skip the disk diff.
READ_ONLY_TOOLS = {"status", "query", "check_gaps", "check_schema", "check_splits"}

TERMINAL = {"succeeded", "failed", "cancelled", "timed_out"}


@dataclass
class Job:
    tool: str
    args: list[str]
    lane: str
    source: str = "client"          # "client" or "schedule:<name>"
    id: str = field(default_factory=new_id)
    state: str = "queued"           # queued → running → succeeded|failed|cancelled|timed_out
    submitted_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    returncode: int | None = None
    log_path: str | None = None
    error: str | None = None
    #: Kill the run after this many seconds (state → timed_out). None = no cap.
    timeout_s: float | None = None
    #: The tool writes one JSON document to stdout (`--json`): capture stdout
    #: apart from the log and parse it into `result` when the run ends.
    json_stdout: bool = False
    result: dict | None = None
    #: Caller-supplied context echoed back (e.g. the refresh plan request).
    meta: dict | None = None

    def public(self) -> dict:
        return asdict(self)


def validate(tool: str, args: list) -> tuple[str, list[str]]:
    tool = tool.removesuffix(".py")
    if tool not in TOOL_LANES:
        raise ValueError(f"tool {tool!r} is not runnable via PDM; allowed: {sorted(TOOL_LANES)}")
    if not isinstance(args, list) or not all(isinstance(a, (str, int, float)) for a in args):
        raise ValueError("args must be a list of strings")
    return tool, [str(a) for a in args]


class JobManager:
    def __init__(
        self,
        tools_dir: Path,
        log_dir: Path,
        on_event: Callable[[Job], None] | None = None,
        python: str = sys.executable,
        history: int = 500,
    ):
        self.tools_dir = Path(tools_dir)
        self.log_dir = Path(log_dir)
        self.on_event = on_event or (lambda job: None)
        self.python = python
        self._jobs: dict[str, Job] = {}
        self._order: deque[str] = deque(maxlen=history)
        self._queues: dict[str, deque[str]] = {}
        self._procs: dict[str, subprocess.Popen] = {}
        self._cond = threading.Condition()
        self._workers: dict[str, threading.Thread] = {}
        self._stopping = False

    # --- public API -----------------------------------------------------------

    def submit(
        self,
        tool: str,
        args: list,
        source: str = "client",
        *,
        timeout_s: float | None = None,
        json_stdout: bool = False,
        meta: dict | None = None,
    ) -> Job:
        tool, args = validate(tool, args)
        job = Job(tool=tool, args=args, lane=TOOL_LANES[tool], source=source,
                  timeout_s=timeout_s, json_stdout=json_stdout, meta=meta)
        with self._cond:
            if len(self._order) == self._order.maxlen:
                oldest = self._jobs.get(self._order[0])
                if oldest and oldest.state in TERMINAL:  # a queued job must stay reachable
                    del self._jobs[oldest.id]
            self._jobs[job.id] = job
            self._order.append(job.id)
            self._queues.setdefault(job.lane, deque()).append(job.id)
            self._ensure_worker(job.lane)
            self._cond.notify_all()
        self.on_event(job)
        return job

    def get(self, job_id: str) -> Job | None:
        return self._jobs.get(job_id)

    def list(self, state: str | None = None, limit: int = 50) -> list[dict]:
        jobs = [self._jobs[i] for i in reversed(self._order) if i in self._jobs]
        if state:
            jobs = [j for j in jobs if j.state == state]
        return [j.public() for j in jobs[:limit]]

    def active_from(self, source: str) -> Job | None:
        """A queued or running job from this source — schedules use it to avoid piling up."""
        for job in list(self._jobs.values()):
            if job.source == source and job.state not in TERMINAL:
                return job
        return None

    def cancel(self, job_id: str) -> Job:
        with self._cond:
            job = self._jobs.get(job_id)
            if job is None:
                raise ValueError(f"unknown job {job_id}")
            if job.state == "queued":
                self._queues[job.lane].remove(job_id)
                job.state, job.finished_at = "cancelled", time.time()
            elif job.state == "running":
                proc = self._procs.get(job_id)
                if proc:
                    proc.terminate()
                job.error = "cancel requested"
            else:
                return job
        if job.state == "cancelled":
            self.on_event(job)
        return job

    def log_tail(self, job_id: str, lines: int = 50) -> str:
        job = self._jobs.get(job_id)
        if job is None or not job.log_path or not Path(job.log_path).exists():
            return ""
        with open(job.log_path, "rb") as f:
            f.seek(0, 2)
            f.seek(max(0, f.tell() - 256 * 1024))
            text = f.read().decode(errors="replace")
        return "\n".join(text.splitlines()[-lines:])

    def lanes(self) -> dict[str, dict]:
        out = {}
        for lane in sorted(set(TOOL_LANES.values())):
            running = [j.id for j in self._jobs.values() if j.lane == lane and j.state == "running"]
            out[lane] = {"queued": len(self._queues.get(lane, ())), "running": running}
        return out

    def stop(self, timeout: float = 10.0) -> None:
        with self._cond:
            self._stopping = True
            for proc in self._procs.values():
                proc.terminate()
            self._cond.notify_all()
        for t in self._workers.values():
            t.join(timeout)

    # --- workers --------------------------------------------------------------

    def _ensure_worker(self, lane: str) -> None:
        if lane in self._workers and self._workers[lane].is_alive():
            return
        t = threading.Thread(target=self._work, args=(lane,), name=f"lane-{lane}", daemon=True)
        self._workers[lane] = t
        t.start()

    def _work(self, lane: str) -> None:
        while True:
            with self._cond:
                while not self._stopping and not self._queues.get(lane):
                    self._cond.wait()
                if self._stopping:
                    return
                job = self._jobs[self._queues[lane].popleft()]
                job.state, job.started_at = "running", time.time()
            self.on_event(job)
            self._run(job)
            self.on_event(job)

    def _run(self, job: Job) -> None:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        log_path = self.log_dir / f"{time.strftime('%Y%m%d-%H%M%S')}-{job.tool}-{job.id}.log"
        job.log_path = str(log_path)
        script = self.tools_dir / f"{job.tool}.py"
        json_path = log_path.with_suffix(".json") if job.json_stdout else None
        timed_out = False
        try:
            with open(log_path, "wb") as log, \
                    (open(json_path, "wb") if json_path else open(os.devnull, "wb")) as out:
                log.write(f"$ {job.tool} {' '.join(job.args)}\n".encode())
                log.flush()
                proc = subprocess.Popen(
                    [self.python, str(script), *job.args],
                    stdout=out if json_path else log,
                    stderr=log if json_path else subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    cwd=self.tools_dir.parent,
                )
                with self._cond:
                    self._procs[job.id] = proc
                    # cancel() between "running" and Popen found no process to stop
                    if job.error == "cancel requested":
                        proc.terminate()
                try:
                    job.returncode = proc.wait(timeout=job.timeout_s)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    proc.terminate()
                    try:
                        job.returncode = proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        job.returncode = proc.wait()
        except Exception as exc:  # e.g. the interpreter vanished
            job.error = f"{type(exc).__name__}: {exc}"
            job.returncode = -1
        finally:
            with self._cond:
                self._procs.pop(job.id, None)
        if json_path and json_path.exists():
            try:
                job.result = json.loads(json_path.read_text() or "null")
            except json.JSONDecodeError:
                job.result = None  # killed mid-write, or the tool failed before printing
        job.finished_at = time.time()
        if timed_out:
            job.state = "timed_out"
            job.error = f"exceeded the {job.timeout_s:g}s cap; terminated"
        elif job.error == "cancel requested":
            job.state = "cancelled"
        else:
            job.state = "succeeded" if job.returncode == 0 else "failed"
