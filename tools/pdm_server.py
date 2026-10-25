#!/usr/bin/env -S uv run python
"""Run the price data manager (PDM) server.

Serves `price-data/` to network clients over ZeroMQ: request/reply for
catalogs, series coverage, bars and download jobs; a PUB socket broadcasting
new bars (from any job or tool that writes to disk), job events and live
exchange feeds. Schedules and live subscriptions come from `pdm.yaml`
(see `pdm.example.yaml`); clients change them at runtime and the server
writes the file back.

Usage examples:
  uv run python tools/pdm_server.py                          # PDM_* from env/.env
  uv run python tools/pdm_server.py --bind <host-tailscale-ip>     # tailnet only
  uv run python tools/pdm_server.py --config pdm.yaml --req-port 5555 --pub-port 5556
  uv run python tools/pdm_server.py --check                  # validate config and exit

Clients: `tools/pdm_client.py`, or `pdm.client.PriceDataClient` from Python.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


from utils.config import (
    DATA_ROOT,
    PDM_BIND_HOST,
    PDM_CONFIG,
    PDM_HTTP_PORT,
    PDM_NOTIFY_CMD,
    PDM_PUB_PORT,
    PDM_REQ_PORT,
    PDM_STALE_ALERT_DAYS,
    PDM_TOKEN,
    REPO_ROOT,
    TMP_ROOT,
)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bind", default=PDM_BIND_HOST, help=f"interface to bind (default {PDM_BIND_HOST})")
    p.add_argument("--req-port", type=int, default=PDM_REQ_PORT, help="request/reply port")
    p.add_argument("--pub-port", type=int, default=PDM_PUB_PORT, help="broadcast port")
    p.add_argument("--http-port", type=int, default=PDM_HTTP_PORT,
                   help="read-only HTTP gateway port; 0 disables it (default PDM_HTTP_PORT)")
    p.add_argument("--config", type=Path, default=PDM_CONFIG, help="schedules + live streams YAML")
    p.add_argument("--scan-seconds", type=float, default=60.0,
                   help="how often to diff the data tree for writes made outside the server")
    p.add_argument("--broadcast-max-rows", type=int, default=5000,
                   help="cap on rows per bar broadcast; larger updates say truncated: true")
    p.add_argument("--check", action="store_true", help="validate the config and exit")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from pdm.server import PdmServer

    server = PdmServer(
        data_root=DATA_ROOT,
        tools_dir=REPO_ROOT / "tools",
        tmp_root=TMP_ROOT,
        bind_host=args.bind,
        req_port=args.req_port,
        pub_port=args.pub_port,
        http_port=args.http_port,
        token=PDM_TOKEN,
        config_path=args.config,
        scan_seconds=args.scan_seconds,
        broadcast_max_rows=args.broadcast_max_rows,
        notify_cmd=PDM_NOTIFY_CMD,
        stale_alert_days=PDM_STALE_ALERT_DAYS,
    )
    if args.check:
        print(f"config ok: {args.config} — {len(server.scheduler.schedules)} schedule(s), "
              f"{len(server.live.list())} live subscription(s); data root {DATA_ROOT}")
        for s in server.scheduler.schedules:
            when = f"next {s.next_run:%Y-%m-%d %H:%M} UTC" if s.enabled else "paused"
            print(f"  schedule {s.name}: {s.tool} {' '.join(s.args)} — {when}")
        return
    if not PDM_TOKEN:
        logging.getLogger("pdm").warning(
            "PDM_TOKEN is empty: anyone who can reach port %s can queue jobs", args.req_port
        )

    signal.signal(signal.SIGTERM, lambda *_: server.stop())
    signal.signal(signal.SIGINT, lambda *_: server.stop())
    server.serve()


if __name__ == "__main__":
    main()
