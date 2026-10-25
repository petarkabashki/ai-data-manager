"""The PDM server: request/reply + broadcast over ZeroMQ.

Threading model — ZeroMQ sockets are not thread-safe, so exactly one thread
(the loop in `serve()`) owns the ROUTER and the PUB socket. Request handlers
run on a thread pool, and they, the job lanes, the watcher and the live
streams all hand outgoing messages back through one inproc PULL socket, each
from its own thread-local PUSH. Nothing but the loop ever touches a network
socket.

Methods (request `method` → `params`):

  ping                                   liveness + server facts
  catalogs                               every catalog with symbol/series counts
  series     catalog asset_class symbol timeframe provider session stale_days limit
  bars       (same filters, must match one series) | path; start end limit
  status     catalog stale_days          tools/status.py --json, catalog-level
  submit     tool args                   queue a job (auth)
  jobs       state limit                 recent jobs, newest first
  job        id log_lines                one job + the tail of its log
  cancel     id                          (auth)
  refresh    provider target timeframes symbols since full dry_run timeout_s
                                         approved one-shot refresh (auth; pdm/refresh.py)
  coverage   symbol                      stored series + schedules + live subs for a symbol
  schedules  symbol catalog              schedules (optionally: those covering a symbol)
  schedule_put     schedule={...}        create/replace by name (auth)
  schedule_remove  name                  (auth)
  schedule_pause   name / schedule_resume name / schedule_run name   (auth)
  live       symbol exchange             live subscriptions with per-symbol freshness
  live_add   exchange market kind timeframe symbols ...   (auth)
  live_remove exchange market kind timeframe [symbols]    (auth; no symbols = all)
  audit      tool options={...}          read-only check on the host (no token; pdm/audit.py)
  reload                                 rebuild the series index (auth)
  topics                                 the broadcast topic scheme

Every schedule_* and live_* change is written back to pdm.yaml.
"""

from __future__ import annotations

import hmac
import json
import logging
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import zmq

from pdm.index import SeriesIndex
from pdm.jobs import READ_ONLY_TOOLS, TERMINAL, Job, JobManager
from pdm import audit
from pdm.config_store import ConfigStore
from pdm.live import LiveManager
from pdm.refresh import DEFAULT_TIMEOUT_S, REFRESH_SOURCE, PlanError, build_argv, preview
from pdm.protocol import (
    PROTOCOL_VERSION,
    bar_topic,
    dumps,
    frame_to_arrow,
    job_topic,
    loads,
    parse_series_path,
)
from pdm.scheduler import Schedule, Scheduler, schedule_targets

log = logging.getLogger("pdm.server")

MUTATING = {
    "submit", "cancel", "reload", "refresh",
    "schedule_put", "schedule_remove", "schedule_pause", "schedule_resume", "schedule_run",
    "live_add", "live_remove",
}
MAX_CONCURRENT_DRY_RUNS = 2

#: Rows one `bars` reply may carry. Over it the request is refused in words
#: (never silently truncated): narrow start/end or pass `limit`.
MAX_BARS_ROWS = 100_000


#: Date columns of the older non-OHLCV layouts, in the order they're tried.
_DATE_COLUMNS = ("timestamp", "date", "datetime", "snapshot_date", "time")


def with_timestamp(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """A frame from a file without a `timestamp` column, with one: int64 ms UTC,
    first, derived from its date column or date index (macro files keep
    `Date` as the pandas index). The source date column is dropped; every other
    column is kept as stored. Refuses in words when there is no date to use."""
    if not isinstance(df.index, pd.RangeIndex):
        df = df.reset_index()
    by_lower = {str(c).lower(): c for c in df.columns}
    col = next((by_lower[c] for c in _DATE_COLUMNS if c in by_lower), None)
    if col is None:
        raise RequestError(f"{name} has no time column (columns: {list(map(str, df.columns))})")
    ts = pd.to_datetime(df[col], utc=True)
    epoch = pd.Timestamp(0, tz="UTC")
    out = df.drop(columns=[col])
    out.insert(0, "timestamp", ((ts - epoch) // pd.Timedelta(milliseconds=1)).astype("int64"))
    return out.sort_values("timestamp", kind="stable").reset_index(drop=True)


def needs_token(method: str, params: dict) -> bool:
    """Everything that changes state needs the token; a dry-run refresh reads
    only, so it stays open — an agent holding no secret can still preview."""
    if method == "refresh" and params.get("dry_run") is True:
        return False
    return method in MUTATING


#: Keys of a `series` row, for the `fields` projection.
SERIES_FIELDS = ("catalog", "asset_class", "symbol", "common_name", "timeframe", "provider",
                 "session", "start_date", "end_date", "adjustment", "volume_basis",
                 "file_path", "stale_days")
SERIES_FILTERS = ("catalog", "asset_class", "symbol", "name", "timeframe", "provider", "session")

TOPICS_DOC = {
    "bar/<asset-dir>/<provider>[/<universe>]/<SYMBOL>/[<session>-]<tf>":
        "new bars written to disk by any job or tool; mirrors the storage path",
    "live/<exchange>/<SYMBOL>/<tf>": "live exchange bars (closed: true|false)",
    "quote/<exchange>/<market>/<SYMBOL>": "live best bid/ask: symbol timestamp last bid ask bid_size ask_size",
    "prices": "py-qwant compatible quotes (qwant_compat): 2 frames, msgpack {symbol,timestamp,last,bid,ask}",
    "job/<state>/<tool>": "job lifecycle: queued, running, succeeded, failed, cancelled, timed_out, interrupted (requeued on restart)",
    "server/<event>": "server lifecycle: started, index, heartbeat (every 5s, with live feed ages)",
    "hint": "subscribe with a trailing '/' to select exactly one node, e.g. bar/stocks/yf/sp500/AAPL/",
}


class RequestError(Exception):
    pass


def to_ms(value) -> int | None:
    """Accept epoch-ms ints or ISO dates/datetimes (UTC unless they say otherwise)."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    ts = pd.Timestamp(str(value))
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return int(ts.value // 1_000_000)


class PdmServer:
    def __init__(
        self,
        *,
        data_root: Path,
        tools_dir: Path,
        tmp_root: Path,
        bind_host: str = "0.0.0.0",
        req_port: int = 5555,
        pub_port: int = 5556,
        token: str = "",
        config: dict | None = None,
        config_path: Path | None = None,
        scan_seconds: float = 60.0,
        refresh_timeout_s: float = DEFAULT_TIMEOUT_S,
        heartbeat_seconds: float = 5.0,
        http_port: int = 0,
        broadcast_max_rows: int = 5000,
        workers: int = 8,
        context: zmq.Context | None = None,
    ):
        from pdm.watch import DataWatcher

        self.data_root = Path(data_root)
        self.tools_dir = Path(tools_dir)
        self.bind_host, self.req_port, self.pub_port = bind_host, req_port, pub_port
        self.token = token
        # `config` (a dict) is for tests; a real server passes `config_path`
        # so runtime changes to schedules and live subscriptions persist.
        self.store = ConfigStore(config_path)
        if config is not None:
            self.store.data = dict(config)
        self.scan_seconds = scan_seconds
        self.refresh_timeout_s = min(refresh_timeout_s, DEFAULT_TIMEOUT_S)
        self.heartbeat_seconds = heartbeat_seconds
        self._refresh_lock = threading.Lock()   # busy check + submit, atomically
        # Dry runs need no token, so bound what an unauthenticated caller can spawn.
        self._dry_runs = threading.BoundedSemaphore(MAX_CONCURRENT_DRY_RUNS)
        self._audits = threading.BoundedSemaphore(audit.MAX_CONCURRENT_AUDITS)
        self.http_port = http_port
        self._http = None
        self.ctx = context or zmq.Context.instance()
        self._out_endpoint = f"inproc://pdm-out-{id(self)}"
        self._local = threading.local()
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="req")
        self._scan_lock = threading.Lock()
        self._reload_lock = threading.Lock()
        self.started_at = time.time()

        self.index = SeriesIndex()
        self.index_ready = threading.Event()
        self.watcher = DataWatcher(
            self.data_root, tmp_root / "pdm_watch.json", max_rows=broadcast_max_rows
        )
        self.jobs = JobManager(self.tools_dir, tmp_root / "pdm-jobs", on_event=self._on_job_event,
                               state_path=tmp_root / "pdm_jobs.json")
        schedules = [Schedule.from_config(s) for s in self.store.get("schedules") or []]
        self.scheduler = Scheduler(
            self.jobs, schedules, on_change=lambda raws: self.store.set("schedules", raws)
        )
        self.live = LiveManager(
            self.publish,
            self.store.get("live") or [],
            publish_raw=self.publish_raw,
            qwant_compat=bool(self.store.get("qwant_compat", False)),
            on_change=lambda cfgs: self.store.set("live", cfgs),
        )

    # --- outgoing messages (any thread) ---------------------------------------

    def _push(self) -> zmq.Socket:
        sock = getattr(self._local, "push", None)
        if sock is None:
            sock = self.ctx.socket(zmq.PUSH)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self._out_endpoint)
            self._local.push = sock
        return sock

    def publish(self, topic: str, header: dict, frame: pd.DataFrame | None = None) -> None:
        parts = [b"P", topic.encode(), dumps(header)]
        if frame is not None:
            parts.append(frame_to_arrow(frame))
        self._push().send_multipart(parts)

    def publish_raw(self, topic: bytes, payload: bytes) -> None:
        """A foreign-format message, verbatim: `[topic, payload]` (py-qwant's `prices`)."""
        self._push().send_multipart([b"P", topic, payload])

    def _reply(self, identity: bytes, header: dict, payload: bytes | None = None) -> None:
        parts = [b"R", identity, dumps(header)]
        if payload is not None:
            parts.append(payload)
        self._push().send_multipart(parts)

    # --- lifecycle ------------------------------------------------------------

    def serve(self, ready: threading.Event | None = None) -> None:
        router = self.ctx.socket(zmq.ROUTER)
        pub = self.ctx.socket(zmq.PUB)
        pull = self.ctx.socket(zmq.PULL)
        for s in (router, pub, pull):
            s.setsockopt(zmq.LINGER, 0)
        router.setsockopt(zmq.ROUTER_MANDATORY, 0)
        pub.setsockopt(zmq.SNDHWM, 100_000)
        pull.bind(self._out_endpoint)
        router.bind(f"tcp://{self.bind_host}:{self.req_port}")
        pub.bind(f"tcp://{self.bind_host}:{self.pub_port}")
        log.info("listening: requests tcp://%s:%s, broadcast tcp://%s:%s",
                 self.bind_host, self.req_port, self.bind_host, self.pub_port)

        threading.Thread(target=self._startup, name="startup", daemon=True).start()
        self.jobs.resume()          # before the scheduler, so it sees what was restored
        self.scheduler.start()
        self.live.start()
        threading.Thread(target=self._heartbeat, name="heartbeat", daemon=True).start()
        if self.http_port:
            from pdm.http_gateway import HttpGateway

            self._http = HttpGateway(self, self.bind_host, self.http_port)
            self._http.start()
            log.info("http gateway (read-only): http://%s:%s/v1/", self.bind_host, self.http_port)
        if ready:
            ready.set()

        poller = zmq.Poller()
        poller.register(router, zmq.POLLIN)
        poller.register(pull, zmq.POLLIN)
        try:
            while not self._stop.is_set():
                for sock, _ in poller.poll(250):
                    if sock is router:
                        identity, *frames = router.recv_multipart()
                        self._pool.submit(self._handle, identity, frames)
                    else:
                        kind, *rest = pull.recv_multipart()
                        if kind == b"R":
                            router.send_multipart(rest)
                        else:
                            pub.send_multipart(rest)
        finally:
            if self._http:
                self._http.stop()
            self.scheduler.stop()
            self.live.stop()
            self.jobs.stop(timeout=2)
            self._pool.shutdown(wait=False, cancel_futures=True)
            for s in (router, pub, pull):
                s.close()
            log.info("stopped")

    def stop(self) -> None:
        self._stop.set()

    def _startup(self) -> None:
        t0 = time.time()
        self._reload_index()
        log.info("index: %d series in %.1fs", len(self.index.rows), time.time() - t0)
        t0 = time.time()
        reads = self.watcher.prime()
        log.info("watcher: %d files tracked, %d footers read in %.1fs",
                 self.watcher.series_count(), reads, time.time() - t0)
        self.publish("server/started", {"type": "server", "event": "started",
                                         "series": len(self.index.rows)})
        while not self._stop.wait(self.scan_seconds):
            self.scan_and_broadcast(trigger="periodic")

    def _heartbeat(self) -> None:
        """`server/heartbeat` every few seconds: a subscriber that stops seeing it
        knows the link (not the market) went quiet; `live` carries each feed's
        age, so a stalled symbol is visible too."""
        while not self._stop.wait(self.heartbeat_seconds):
            self.publish("server/heartbeat", {
                "type": "server", "event": "heartbeat",
                "time": datetime.now(timezone.utc).isoformat(),
                "live": self.live.freshness(),
            })

    def _reload_index(self) -> None:
        with self._reload_lock:
            self.index.reload()
        self.index_ready.set()

    # --- change broadcast -----------------------------------------------------

    def scan_and_broadcast(self, trigger: str) -> int:
        with self._scan_lock:
            try:
                updates = self.watcher.scan_changes()
            except Exception:
                log.exception("scan failed")
                return 0
        for u in updates:
            header = {
                "type": "bars",
                **parse_series_path(u.path),
                "new_file": u.new_file,
                "rows": len(u.frame),
                "total_new_rows": u.total_new_rows,
                "truncated": u.truncated,
                "trigger": trigger,
            }
            self.publish(bar_topic(u.path), header, u.frame)
        if updates:
            log.info("broadcast %d updated series (%s)", len(updates), trigger)
        return len(updates)

    def _on_job_event(self, job: Job) -> None:
        self.publish(job_topic(job.state, job.tool), {"type": "job", **job.public()})
        # A failed or cancelled run may still have merged some series before it
        # stopped, so any job that actually started gets the disk diff.
        if job.state in TERMINAL and job.started_at and job.tool not in READ_ONLY_TOOLS:
            self.scan_and_broadcast(trigger=f"job:{job.id}")
            self._reload_index()
            self.publish("server/index", {"type": "server", "event": "index",
                                           "series": len(self.index.rows)})

    # --- request dispatch -----------------------------------------------------

    def _handle(self, identity: bytes, frames: list[bytes]) -> None:
        req_id = None
        try:
            req = loads(frames[0])
            req_id = req.get("id")
            result, payload = self.dispatch(req.get("method", ""), req.get("params") or {},
                                            req.get("token"))
            self._reply(identity, {"id": req_id, "ok": True, "result": result,
                                   "payload": "arrow" if payload is not None else None}, payload)
        except (RequestError, ValueError, TypeError, KeyError) as exc:
            self._reply(identity, {"id": req_id, "ok": False, "error": str(exc)})
        except Exception as exc:
            log.exception("request failed")
            self._reply(identity, {"id": req_id, "ok": False,
                                   "error": f"internal error: {type(exc).__name__}: {exc}"})

    def dispatch(self, method: str, params: dict, token: str | None) -> tuple:
        """Authorize and run one request → (result, arrow_payload | None).
        Shared by the ZMQ loop and the HTTP gateway."""
        if not isinstance(params, dict):
            raise RequestError("params must be an object")
        if self.token and needs_token(method, params):
            if not hmac.compare_digest(str(token or ""), self.token):
                raise RequestError("unauthorized: this method needs the server token")
        handler = getattr(self, f"m_{method}", None) if method.isidentifier() else None
        if handler is None:
            raise RequestError(f"unknown method {method!r}")
        out = handler(**params)
        return out if isinstance(out, tuple) else (out, None)

    def _need_index(self) -> None:
        if not self.index_ready.wait(timeout=60):
            raise RequestError("series index still loading; retry shortly")

    # --- methods --------------------------------------------------------------

    def m_ping(self) -> dict:
        return {
            "server": "pdm",
            "protocol": PROTOCOL_VERSION,
            "time": datetime.now(timezone.utc).isoformat(),
            "uptime_s": round(time.time() - self.started_at, 1),
            "pub_port": self.pub_port,
            "index_ready": self.index_ready.is_set(),
            "series_indexed": len(self.index.rows),
            "watch_ready": self.watcher.ready.is_set(),
            "files_tracked": self.watcher.series_count(),
            "auth_required_for": (sorted(MUTATING - {"refresh"}) + ["refresh (dry_run=false)"])
                                 if self.token else [],
            "schedules": len(self.scheduler.schedules),
            "live_subscriptions": len(self.live.list()),
            "lanes": self.jobs.lanes(),
        }

    def m_topics(self) -> dict:
        return TOPICS_DOC

    def m_catalogs(self) -> list[dict]:
        self._need_index()
        return self.index.catalogs

    def m_series(self, limit: int = 1000, stale_days: int | None = None,
                 fields: list[str] | None = None, **filters) -> dict:
        """Series rows matching the filters. `name` matches the asset's common
        name by leading whole words, case-insensitively ("apple" → "Apple Inc.").
        `fields` keeps only those keys and collapses rows that become identical,
        so `fields=[symbol, catalog, common_name]` is one row per asset."""
        self._need_index()
        unknown = set(filters) - set(SERIES_FILTERS)
        if unknown:
            raise RequestError(f"unknown filter(s): {sorted(unknown)}")
        try:
            rows = self.index.query(stale_days=stale_days, limit=None, **filters)
        except ValueError as exc:
            raise RequestError(str(exc)) from None
        if fields is not None:
            if isinstance(fields, str):
                fields = [fields]
            bad = set(fields) - set(SERIES_FIELDS) if isinstance(fields, list) else {"?"}
            if bad or not fields:
                raise RequestError(f"fields must be a non-empty list of {list(SERIES_FIELDS)}")
            seen, kept = set(), []
            for row in rows:
                out = {k: row[k] for k in fields}
                key = tuple(map(str, out.values()))
                if key not in seen:
                    seen.add(key)
                    kept.append(out)
            rows = kept
        return {"count": len(rows), "series": rows[:limit] if limit else rows,
                "truncated": bool(limit) and len(rows) > limit}

    def m_bars(self, path: str | None = None, start=None, end=None,
               limit: int | None = None, **filters):
        if path:
            target = (self.data_root / path).resolve()
            if not target.is_relative_to(self.data_root.resolve()) or target.suffix != ".parquet":
                raise RequestError("path must be a .parquet file under the data root")
            meta = parse_series_path(Path(path).as_posix())
        else:
            unknown = set(filters) - set(SERIES_FILTERS)
            if unknown:
                raise RequestError(f"unknown filter(s): {sorted(unknown)}")
            if not filters.get("symbol"):
                raise RequestError("bars needs a symbol (plus timeframe etc.) or a path")
            self._need_index()
            meta, target = self.index.resolve(**filters)
        if not target.exists():
            raise RequestError(f"file missing on disk: {target.name}")

        start_ms, end_ms = to_ms(start), to_ms(end)
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 1):
            raise RequestError("limit must be a positive integer")
        if limit and limit > MAX_BARS_ROWS:
            raise RequestError(f"limit {limit} is over the {MAX_BARS_ROWS}-row cap per reply")
        if "timestamp" in pq.read_schema(target).names:
            flt = []
            if start_ms is not None:
                flt.append(("timestamp", ">=", start_ms))
            if end_ms is not None:
                flt.append(("timestamp", "<=", end_ms))
            df = pq.read_table(target, filters=flt or None).to_pandas()
        else:
            # Older layouts (macro's Value + Date index, market-cap snapshots) are
            # small: read whole, derive the canonical timestamp, filter here.
            df = with_timestamp(pq.read_table(target).to_pandas(), target.name)
            if start_ms is not None:
                df = df[df["timestamp"] >= start_ms]
            if end_ms is not None:
                df = df[df["timestamp"] <= end_ms]
        if not limit and len(df) > MAX_BARS_ROWS:
            raise RequestError(
                f"{len(df)} rows match, over the {MAX_BARS_ROWS}-row cap per reply; "
                "narrow start/end or pass limit (last N rows), or page by start/end"
            )
        if limit:
            df = df.tail(limit).reset_index(drop=True)
        result = {"series": meta, "rows": len(df), "columns": list(df.columns)}
        return result, frame_to_arrow(df)

    def m_status(self, catalog: str | None = None, stale_days: int | None = None) -> dict:
        argv = [sys.executable, str(self.tools_dir / "status.py"), "--json"]
        if catalog:
            argv.append(f"--catalog={catalog}")
        if stale_days is not None:
            argv.append(f"--stale-days={int(stale_days)}")
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=300,
                              cwd=self.tools_dir.parent)
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError:
            raise RequestError(f"status.py failed (exit {proc.returncode}): {proc.stderr[-500:]}")

    def m_submit(self, tool: str, args: list | None = None) -> dict:
        return self.jobs.submit(tool, args or []).public()

    def m_jobs(self, state: str | None = None, limit: int = 50) -> list[dict]:
        return self.jobs.list(state=state, limit=limit)

    def m_job(self, id: str, log_lines: int = 0) -> dict:
        job = self.jobs.get(id)
        if job is None:
            raise RequestError(f"unknown job {id}")
        out = job.public()
        if log_lines:
            out["log"] = self.jobs.log_tail(id, int(log_lines))
        return out

    def m_cancel(self, id: str) -> dict:
        return self.jobs.cancel(id).public()

    # --- refresh --------------------------------------------------------------

    def m_refresh(self, **params) -> dict:
        try:
            tool, argv, plan = build_argv(params)
        except PlanError as exc:
            raise RequestError(f"invalid refresh plan: {exc}") from None
        timeout_s = params.get("timeout_s") or self.refresh_timeout_s
        if not isinstance(timeout_s, (int, float)) or not 0 < timeout_s <= self.refresh_timeout_s:
            raise RequestError(f"timeout_s must be in (0, {self.refresh_timeout_s:g}]")

        if params["dry_run"]:
            if not self._dry_runs.acquire(blocking=False):
                raise RequestError(f"busy: {MAX_CONCURRENT_DRY_RUNS} dry runs already in progress; retry shortly")
            try:
                return preview(self.tools_dir, tool, argv)
            finally:
                self._dry_runs.release()

        if not self._refresh_lock.acquire(blocking=False):
            raise RequestError("busy: another refresh request is being checked")
        try:
            busy = self.jobs.active_from(REFRESH_SOURCE)
            if busy:
                raise RequestError(
                    f"busy: refresh job {busy.id} is {busy.state}; one refresh at a time")
            checked = preview(self.tools_dir, tool, argv)   # refuse bad plans before queueing
            if checked["refused"]:
                raise RequestError(f"refused by dry run: {checked['error']}")
            job = self.jobs.submit(
                tool, argv, source=REFRESH_SOURCE, timeout_s=float(timeout_s),
                json_stdout="--json" in argv,
                meta={"plan": plan, "preview": {k: checked[k] for k in ("plan", "argv")}},
            )
        finally:
            self._refresh_lock.release()
        return job.public()

    # --- audits -----------------------------------------------------------------

    def m_audit(self, tool: str, options: dict | None = None) -> dict:
        """Run a read-only check on the host (no token; see pdm/audit.py)."""
        try:
            argv = audit.build_argv(tool, options, self.data_root)
        except audit.AuditError as exc:
            raise RequestError(f"invalid audit: {exc}") from None
        if not self._audits.acquire(blocking=False):
            raise RequestError(f"busy: {audit.MAX_CONCURRENT_AUDITS} audits already running; retry shortly")
        try:
            return audit.run(self.tools_dir, tool, argv)
        finally:
            self._audits.release()

    # --- schedules --------------------------------------------------------------

    def _covers(self, s: Schedule, symbol: str) -> bool:
        t = schedule_targets(s)
        wanted = symbol.replace("/", "_")
        if t["symbols"] is not None:
            return wanted in t["symbols"]
        if t["catalog"]:
            self._need_index()
            return any(r["catalog"] == t["catalog"] and r["symbol"] == wanted
                       for r in self.index.query(symbol=wanted))
        return False

    def m_schedules(self, symbol: str | None = None, catalog: str | None = None) -> list[dict]:
        out = []
        for s in self.scheduler.schedules:
            t = schedule_targets(s)
            if catalog and t["catalog"] != catalog:
                continue
            if symbol and not self._covers(s, symbol):
                continue
            out.append({**s.public(), "targets": t})
        return out

    def m_schedule_put(self, schedule: dict) -> dict:
        s, created = self.scheduler.put(schedule)
        return {**s.public(), "created": created, "targets": schedule_targets(s)}

    def m_schedule_remove(self, name: str) -> dict:
        return {"removed": self.scheduler.remove(name).name}

    def m_schedule_pause(self, name: str) -> dict:
        return self.scheduler.set_enabled(name, False).public()

    def m_schedule_resume(self, name: str) -> dict:
        return self.scheduler.set_enabled(name, True).public()

    def m_schedule_run(self, name: str) -> dict:
        return self.scheduler.run_now(name).public()

    # --- live ---------------------------------------------------------------------

    def m_live(self, symbol: str | None = None, exchange: str | None = None) -> dict:
        return {"qwant_compat": self.live.qwant_compat,
                "subscriptions": self.live.list(symbol=symbol, exchange=exchange)}

    def m_live_add(self, **sub) -> dict:
        return self.live.add(sub)

    def m_live_remove(self, **sub) -> dict:
        return self.live.remove(sub)

    # --- coverage ------------------------------------------------------------------

    def m_coverage(self, symbol: str) -> dict:
        """Everything about one symbol: what's stored, what refreshes it, what streams it."""
        self._need_index()
        wanted = symbol.replace("/", "_")
        stored = self.index.query(symbol=wanted)
        return {
            "symbol": wanted,
            "stored": [{k: r[k] for k in ("catalog", "timeframe", "provider", "session",
                                          "start_date", "end_date", "stale_days")} for r in stored],
            "schedules": self.m_schedules(symbol=wanted),
            "live": self.live.list(symbol=wanted),
        }

    def m_reload(self) -> dict:
        self._reload_index()
        return {"series": len(self.index.rows)}
