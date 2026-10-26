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

With a `state_path`, unfinished jobs survive a restart. Every state change
rewrites the file (atomically) with the queued and running jobs, so even a
SIGKILL leaves an up-to-date copy. On start they are queued again in their
original order, a job that was running going first in its lane. Rerunning one
that was killed is safe because every tool resumes from what is on disk and
merges atomically. The newest `keep_finished` finished jobs are saved too,
results included, so a client polling an id across a restart still gets
its outcome.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from pathlib import Path

from pdm.protocol import new_id


def _signal_tree(proc: subprocess.Popen, sig: int = signal.SIGTERM) -> None:
    """Signal a job's whole process group, not only the tool.

    Tools start children (download_crypto runs freqtrade); terminating just
    the tool leaves them running as orphans, still downloading into a temp
    dir nobody will merge. Each job leads its own group (start_new_session).
    """
    try:
        os.killpg(proc.pid, sig)
    except (ProcessLookupError, PermissionError):
        pass

#: tool name → lane. Anything not listed cannot be run through the server.
TOOL_LANES: dict[str, str] = {
    "download_yf": "equity",
    "download_ibkr_stock_overnight": "equity",
    "download_ibkr_index": "equity",
    "retry_ibkr_failed_sequentially": "equity",
    "check_catalog": "equity",          # --fix writes catalogs
    "fix_daily_stamps": "equity",       # per provider, see PROVIDER_LANES
    "download_crypto": "crypto",
    "download_funding": "crypto",       # writes the crypto_futures catalogs too
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

#: Tools that rewrite whichever provider's files `--provider` names run in
#: that provider's lane instead, so they never race its downloader.
PROVIDER_TOOLS = {"fix_daily_stamps"}
PROVIDER_LANES = {"binance": "crypto", "bybit": "crypto", "kucoin": "crypto",
                  "bitget": "crypto", "deribit": "options"}


def lane_for(tool: str, args: list[str]) -> str:
    """The tool's lane, or for a PROVIDER_TOOLS job its provider's."""
    provider = None
    if tool in PROVIDER_TOOLS:
        for i, a in enumerate(args):      # last one wins, as with argparse
            if a == "--provider" and i + 1 < len(args):
                provider = args[i + 1]
            elif a.startswith("--provider="):
                provider = a.split("=", 1)[1]
    return PROVIDER_LANES.get(provider, TOOL_LANES[tool])


#: Tools whose completion cannot have written bars — skip the disk diff.
READ_ONLY_TOOLS = {"status", "query", "check_gaps", "check_schema", "check_splits"}

TERMINAL = {"succeeded", "failed", "cancelled", "timed_out"}

#: A run cut short by the server stopping; queued again on the next start.
INTERRUPTED = "interrupted"


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
    #: How many times a server restart put this job back in its queue.
    requeued: int = 0

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
        state_path: Path | None = None,
        keep_finished: int = 50,
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
        self.state_path = Path(state_path) if state_path else None
        self.keep_finished = keep_finished
        self._load()

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
        job = Job(tool=tool, args=args, lane=lane_for(tool, args), source=source,
                  timeout_s=timeout_s, json_stdout=json_stdout, meta=meta)
        with self._cond:
            if len(self._order) == self._order.maxlen:
                oldest = self._jobs.get(self._order[0])
                if oldest and oldest.state in TERMINAL:  # a queued job must stay reachable
                    del self._jobs[oldest.id]
            self._jobs[job.id] = job
            self._order.append(job.id)
            self._queues.setdefault(job.lane, deque()).append(job.id)
            self._save()
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
                    _signal_tree(proc)
                job.error = "cancel requested"
            else:
                return job
            self._save()
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

    def resume(self) -> None:
        """Start the lanes that jobs restored from `state_path` are waiting in."""
        with self._cond:
            for lane, queue in self._queues.items():
                if queue:
                    self._ensure_worker(lane)
            self._cond.notify_all()

    def stop(self, timeout: float = 10.0) -> None:
        with self._cond:
            self._stopping = True
            for proc in self._procs.values():
                _signal_tree(proc)
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
                self._save()
            self.on_event(job)
            self._run(job)
            with self._cond:
                self._save()
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
                    start_new_session=True,
                )
                with self._cond:
                    self._procs[job.id] = proc
                    # cancel() or stop() between "running" and Popen found no process to stop
                    if job.error == "cancel requested" or self._stopping:
                        _signal_tree(proc)
                try:
                    job.returncode = proc.wait(timeout=job.timeout_s)
                except subprocess.TimeoutExpired:
                    timed_out = True
                    _signal_tree(proc)
                    try:
                        job.returncode = proc.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        _signal_tree(proc, signal.SIGKILL)
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
        elif self._stopping:
            job.state = INTERRUPTED   # stop() terminated it: not the tool's verdict
        else:
            job.state = "succeeded" if job.returncode == 0 else "failed"

    # --- persistence ----------------------------------------------------------

    def _save(self) -> None:
        """Write the unfinished jobs, and the newest finished ones, to
        `state_path`. Call with `_cond` held."""
        if not self.state_path:
            return
        jobs = [self._jobs[i] for i in self._order if i in self._jobs]
        finished = [j for j in jobs if j.state in TERMINAL]
        recent = {j.id for j in finished[-self.keep_finished:]} if self.keep_finished else set()
        keep = [j.public() for j in jobs if j.state not in TERMINAL or j.id in recent]
        try:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_name(self.state_path.name + ".tmp")
            tmp.write_text(json.dumps({"version": 1, "jobs": keep}, default=str))
            os.replace(tmp, self.state_path)
        except OSError as exc:  # a full disk must not stop the queue
            print(f"pdm.jobs: cannot save {self.state_path}: {exc}", file=sys.stderr, flush=True)

    def _load(self) -> None:
        if not self.state_path or not self.state_path.exists():
            return
        try:
            saved = json.loads(self.state_path.read_text()).get("jobs") or []
        except (OSError, ValueError, AttributeError) as exc:
            print(f"pdm.jobs: ignoring unreadable {self.state_path}: {exc}", file=sys.stderr, flush=True)
            return
        names = set(Job.__dataclass_fields__)
        restored: list[tuple[bool, Job]] = []
        for raw in saved:
            try:
                job = Job(**{k: v for k, v in raw.items() if k in names})
                validate(job.tool, job.args)   # a tool since removed is dropped
            except (TypeError, ValueError) as exc:
                print(f"pdm.jobs: dropping saved job {raw.get('id')}: {exc}", file=sys.stderr, flush=True)
                continue
            job.lane = lane_for(job.tool, job.args)
            if job.state in ("running", INTERRUPTED) and job.error == "cancel requested":
                job.state, job.finished_at = "cancelled", time.time()
            was_running = job.state in ("running", INTERRUPTED)
            if was_running:
                job.requeued += 1
                job.state = "queued"
                job.started_at = job.finished_at = job.returncode = job.result = None
                job.log_path = None
                job.error = None
            if job.state == "queued" or job.state in TERMINAL:
                restored.append((was_running, job))
        # Interrupted runs were at the head of their lane: put them back there.
        restored.sort(key=lambda r: (not r[0], r[1].submitted_at))
        with self._cond:
            for _, job in restored:
                self._jobs[job.id] = job
                self._order.append(job.id)
                if job.state == "queued":
                    self._queues.setdefault(job.lane, deque()).append(job.id)
            self._save()
