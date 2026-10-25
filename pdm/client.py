"""Python client for the PDM server.

```python
from pdm.client import PriceDataClient

with PriceDataClient("<host-tailscale-ip>") as pdm:
    pdm.series(symbol="AAPL")                       # what exists
    df = pdm.bars(symbol="BTC_USDT", catalog="crypto_binance", timeframe="1h",
                  start="2026-10-01")
    job = pdm.submit("download_yf", ["--universe", "sp500", "--timeframes", "1d"])
    pdm.wait(job["id"])

    for topic, header, frame in pdm.subscribe("bar/cryptos/binance/", "live/", "quote/"):
        ...

    pdm.coverage("AAPL")                            # stored + scheduled + live
    pdm.refresh(provider="yf", target={"universe": "sp500"}, timeframes=["1d"],
                symbols=["AAPL"], dry_run=True)     # preview; dry_run=False queues
    pdm.live_add("binance", ["SOL/USDT"], market="swap", kind="quotes")
```

Requests use a DEALER socket with per-request ids, not REQ: a REQ socket
wedges forever after one lost reply, while a DEALER just times the request
out and carries on.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import msgpack
import pandas as pd
import zmq

from pdm.protocol import arrow_to_frame, dumps, loads, new_id
from utils.config import PDM_PUB_PORT, PDM_REQ_PORT, PDM_SERVER, PDM_TOKEN


#: Final job states (mirrors pdm.jobs.TERMINAL; kept here so the client stays
#: copyable with only protocol.py).
TERMINAL = ("succeeded", "failed", "cancelled", "timed_out")


class PdmError(RuntimeError):
    pass


class PriceDataClient:
    def __init__(
        self,
        host: str = PDM_SERVER,
        req_port: int = PDM_REQ_PORT,
        pub_port: int = PDM_PUB_PORT,
        token: str = PDM_TOKEN,
        timeout: float = 60.0,
        context: zmq.Context | None = None,
    ):
        self.host, self.req_port, self.pub_port = host, req_port, pub_port
        self.token = token
        self.timeout = timeout
        self.ctx = context or zmq.Context.instance()
        self._sock = self.ctx.socket(zmq.DEALER)
        self._sock.setsockopt(zmq.LINGER, 0)
        self._sock.connect(f"tcp://{host}:{req_port}")

    def close(self) -> None:
        self._sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- transport ------------------------------------------------------------

    def call(self, method: str, timeout: float | None = None, **params):
        """Returns `result`, or `(result, DataFrame)` when the reply carries a table."""
        req_id = new_id()
        header = {"id": req_id, "method": method, "params": params}
        if self.token:
            header["token"] = self.token
        self._sock.send_multipart([dumps(header)])
        deadline = time.monotonic() + (timeout or self.timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self._sock.poll(int(remaining * 1000)):
                raise TimeoutError(f"{method}: no reply from {self.host}:{self.req_port}")
            frames = self._sock.recv_multipart()
            reply = loads(frames[0])
            if reply.get("id") != req_id:
                continue  # a late reply to an earlier, timed-out request
            if not reply.get("ok"):
                raise PdmError(reply.get("error", "unknown error"))
            if reply.get("payload") == "arrow":
                return reply["result"], arrow_to_frame(frames[1])
            return reply["result"]

    # --- methods --------------------------------------------------------------

    def ping(self) -> dict:
        return self.call("ping", timeout=10)

    def catalogs(self) -> list[dict]:
        return self.call("catalogs")

    def series(self, **filters) -> list[dict]:
        return self.call("series", **filters)["series"]

    def bars(self, *, start=None, end=None, limit: int | None = None,
             with_meta: bool = False, **filters):
        """One series as a canonical frame (`timestamp` int64 ms UTC + OHLCV)."""
        meta, frame = self.call("bars", start=start, end=end, limit=limit, **filters)
        return (frame, meta) if with_meta else frame

    def status(self, **params) -> dict:
        return self.call("status", timeout=300, **params)

    def submit(self, tool: str, args: list[str] | None = None) -> dict:
        return self.call("submit", tool=tool, args=args or [])

    def jobs(self, **params) -> list[dict]:
        return self.call("jobs", **params)

    def job(self, job_id: str, log_lines: int = 0) -> dict:
        return self.call("job", id=job_id, log_lines=log_lines)

    def cancel(self, job_id: str) -> dict:
        return self.call("cancel", id=job_id)

    def wait(self, job_id: str, poll: float = 2.0, timeout: float | None = None) -> dict:
        deadline = time.monotonic() + timeout if timeout else None
        while True:
            job = self.job(job_id)
            if job["state"] in TERMINAL:
                return job
            if deadline and time.monotonic() > deadline:
                raise TimeoutError(f"job {job_id} still {job['state']}")
            time.sleep(poll)

    # --- refresh, coverage, schedules, live ----------------------------------

    def refresh(self, *, dry_run: bool, timeout: float | None = None, **plan) -> dict:
        """Approved one-shot refresh (see pdm/refresh.py). `dry_run` is required:
        True returns the preview, False queues the run and returns the job."""
        if dry_run:   # a dry run spawns the tool and can take a while on a big universe
            timeout = timeout or max(self.timeout, 150)
        return self.call("refresh", timeout=timeout, dry_run=dry_run, **plan)

    def audit(self, tool: str, timeout: float | None = 660, **options) -> dict:
        """Read-only check on the host, no token: status, query, check_gaps,
        check_schema, check_catalog (never --fix), check_splits. Options are the
        tool's flags with `_` for `-` (see pdm/audit.py)."""
        return self.call("audit", timeout=timeout, tool=tool, options=options)

    def coverage(self, symbol: str) -> dict:
        return self.call("coverage", symbol=symbol)

    def schedules(self, symbol: str | None = None, catalog: str | None = None) -> list[dict]:
        return self.call("schedules", **_given(symbol=symbol, catalog=catalog))

    def schedule_put(self, schedule: dict) -> dict:
        return self.call("schedule_put", schedule=schedule)

    def schedule_remove(self, name: str) -> dict:
        return self.call("schedule_remove", name=name)

    def schedule_pause(self, name: str) -> dict:
        return self.call("schedule_pause", name=name)

    def schedule_resume(self, name: str) -> dict:
        return self.call("schedule_resume", name=name)

    def schedule_run(self, name: str) -> dict:
        return self.call("schedule_run", name=name)

    def live(self, symbol: str | None = None, exchange: str | None = None) -> dict:
        return self.call("live", **_given(symbol=symbol, exchange=exchange))

    def live_add(self, exchange: str, symbols: list[str], *, market: str = "spot",
                 kind: str = "bars", timeframe: str | None = None, **opts) -> dict:
        return self.call("live_add", exchange=exchange, market=market, kind=kind,
                         symbols=list(symbols), **_given(timeframe=timeframe), **opts)

    def live_remove(self, exchange: str, symbols: list[str] | None = None, *, market: str = "spot",
                    kind: str = "bars", timeframe: str | None = None) -> dict:
        return self.call("live_remove", exchange=exchange, market=market, kind=kind,
                         symbols=list(symbols or []), **_given(timeframe=timeframe))

    # --- broadcast ------------------------------------------------------------

    def subscribe(self, *prefixes: str, timeout: float | None = None
                  ) -> Iterator[tuple[str, dict, pd.DataFrame | None]]:
        """Yield `(topic, header, frame_or_None)` for every matching broadcast.

        No prefix subscribes to everything. With `timeout`, stops after that
        many seconds without a message.
        """
        sub = self.ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.LINGER, 0)
        sub.setsockopt(zmq.RCVHWM, 100_000)
        for p in prefixes or ("",):
            sub.setsockopt(zmq.SUBSCRIBE, p.encode())
        sub.connect(f"tcp://{self.host}:{self.pub_port}")
        try:
            while True:
                if timeout is not None and not sub.poll(int(timeout * 1000)):
                    return
                topic, header, *rest = sub.recv_multipart()
                if topic == b"prices":   # py-qwant compat feed: msgpack, not JSON
                    yield "prices", msgpack.unpackb(header), None
                    continue
                frame = arrow_to_frame(rest[0]) if rest else None
                yield topic.decode(), loads(header), frame
        finally:
            sub.close()


def _given(**kw) -> dict:
    return {k: v for k, v in kw.items() if v is not None}


def with_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Canonical frame → UTC DatetimeIndex, for analysis code that wants one."""
    out = df.copy()
    out.index = pd.to_datetime(out.pop("timestamp"), unit="ms", utc=True)
    return out
