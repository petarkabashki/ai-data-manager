"""Read-only HTTP face of the PDM server, for clients that can't speak ZMQ
(sandboxed agents whose only egress is HTTP).

It runs inside the server process and calls the same `dispatch` the ZMQ loop
uses, so it shares the index, the caps and the validation. It never takes a
token: only methods that change nothing are routed, plus `refresh` with
`dry_run: true` (a preview). Anything else is 403.

    GET  /v1/<method>?key=value&key=value     simple params; repeat a key for a list
    POST /v1/<method>   {"key": value, ...}    JSON body, typed (use for audit options)

    GET /v1/series?symbol=AAPL&timeframe=1d
    GET /v1/bars?symbol=AAPL&catalog=sp500&timeframe=1d&start=2026-01-01
    GET /v1/bars?...&format=arrow              Arrow IPC stream (zstd) instead of JSON
    POST /v1/audit  {"tool": "check_gaps", "options": {"catalog": ["sp500"], "timeframe": "1d"}}
    POST /v1/refresh {"dry_run": true, "target": {"universe": "sp500"}, "timeframes": ["1d"]}

Replies: 200 `{"ok": true, "result": ...}`; `bars` as JSON is
`{"ok": true, "result": {"series", "rows", "columns", "data": [[ts, o, h, l, c, v], ...]}}`.
Errors: 400 (bad request / refused, with the reason), 403 (not routed here),
404 (unknown path), 503 (busy / still loading), all `{"ok": false, "error": ...}`.
"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

log = logging.getLogger("pdm.http")

#: Methods the gateway routes. `refresh` additionally requires dry_run=true.
READ_METHODS = {
    "ping", "topics", "catalogs", "series", "bars", "status", "coverage",
    "schedules", "live", "jobs", "job", "audit", "refresh",
}
_INT_PARAMS = {"limit", "stale_days", "log_lines", "timeout_s"}
_LIST_PARAMS = {"timeframes", "symbols"}
_BOOL_PARAMS = {"dry_run", "full"}
MAX_BODY = 1 << 20


def _coerce(query: dict[str, list[str]]) -> dict:
    out = {}
    for key, values in query.items():
        if key in _LIST_PARAMS:
            out[key] = [v for raw in values for v in raw.split(",") if v]
            continue
        value = values[-1] if len(values) == 1 else values
        if key in _INT_PARAMS and isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                raise ValueError(f"{key} must be an integer") from None
        elif key in _BOOL_PARAMS and isinstance(value, str):
            if value.lower() not in ("true", "false", "1", "0"):
                raise ValueError(f"{key} must be true or false")
            value = value.lower() in ("true", "1")
        out[key] = value
    return out


def _bars_json(result: dict, payload: bytes) -> dict:
    from pdm.protocol import arrow_to_frame

    df = arrow_to_frame(payload)
    # astype(object) keeps timestamps as Python ints instead of floats
    return {**result, "data": df.astype(object).values.tolist()}


class _Handler(BaseHTTPRequestHandler):
    server_version = "pdm-http/1"
    gateway: "HttpGateway"

    def log_message(self, fmt, *args):  # route through logging, not stderr
        log.debug("%s %s", self.address_string(), fmt % args)

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj) -> None:
        self._send(status, json.dumps(obj, default=str).encode(), "application/json")

    def do_GET(self):
        url = urlsplit(self.path)
        try:
            params = _coerce(parse_qs(url.query, keep_blank_values=False))
        except ValueError as exc:
            return self._json(400, {"ok": False, "error": str(exc)})
        self._route(url.path, params)

    def do_POST(self):
        url = urlsplit(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY:
            return self._json(413, {"ok": False, "error": "body too large"})
        try:
            params = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            return self._json(400, {"ok": False, "error": f"body is not JSON: {exc}"})
        if not isinstance(params, dict):
            return self._json(400, {"ok": False, "error": "body must be a JSON object"})
        params.update(_coerce(parse_qs(url.query)))   # e.g. ?format=arrow on a POST
        self._route(url.path, params)

    def _route(self, path: str, params: dict) -> None:
        from pdm.server import RequestError

        parts = path.strip("/").split("/")
        if len(parts) != 2 or parts[0] != "v1":
            return self._json(404, {"ok": False, "error": "use /v1/<method>",
                                    "methods": sorted(READ_METHODS)})
        method = parts[1]
        if method not in READ_METHODS:
            return self._json(403, {"ok": False, "error": f"{method!r} is not available over HTTP "
                                    "(read-only gateway); use the ZMQ client with the token"})
        if method == "refresh" and params.get("dry_run") is not True:
            return self._json(403, {"ok": False, "error": "HTTP only previews: refresh needs dry_run=true"})
        fmt = params.pop("format", "json")
        try:
            result, payload = self.gateway.server.dispatch(method, params, None)
        except RequestError as exc:
            msg = str(exc)
            status = 503 if msg.startswith(("busy", "series index still loading")) else 400
            return self._json(status, {"ok": False, "error": msg})
        except (ValueError, TypeError, KeyError) as exc:
            return self._json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            log.exception("http request failed")
            return self._json(500, {"ok": False, "error": f"internal error: {type(exc).__name__}"})
        if payload is not None and fmt == "arrow":
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-PDM-Result", json.dumps(result, default=str))
            self.end_headers()
            self.wfile.write(payload)
            return
        if payload is not None:
            result = _bars_json(result, payload)
        self._json(200, {"ok": True, "result": result})


class HttpGateway:
    def __init__(self, server, host: str, port: int):
        self.server = server
        handler = type("Handler", (_Handler,), {"gateway": self})
        self.httpd = ThreadingHTTPServer((host, port), handler)
        self.httpd.daemon_threads = True
        self.port = self.httpd.server_address[1]
        self._thread = threading.Thread(target=self.httpd.serve_forever, name="http", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
