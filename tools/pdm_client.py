#!/usr/bin/env -S uv run python
"""Command-line client for the PDM server.

Usage examples:
  uv run python tools/pdm_client.py --host <host-tailscale-ip> ping
  uv run python tools/pdm_client.py catalogs
  uv run python tools/pdm_client.py series --symbol AAPL
  uv run python tools/pdm_client.py series --catalog crypto_binance --timeframe 1d --stale-days 7
  uv run python tools/pdm_client.py bars --symbol AAPL --catalog sp500 --timeframe 1d --start 2026-01-01
  uv run python tools/pdm_client.py bars --path cryptos/binance/BTC_USDT/BTC_USDT-1h.parquet --limit 24 --csv out.csv
  uv run python tools/pdm_client.py submit --wait download_yf -- --universe sp500 --timeframes 1d --limit 2
  uv run python tools/pdm_client.py jobs
  uv run python tools/pdm_client.py job <id> --log 40
  uv run python tools/pdm_client.py watch bar/cryptos/binance/ live/
  uv run python tools/pdm_client.py status
  uv run python tools/pdm_client.py coverage AAPL              # stored + scheduled + live
  uv run python tools/pdm_client.py audit check_gaps catalog=sp500 timeframe=1d   # read-only, no token
  uv run python tools/pdm_client.py refresh --universe sp500 -t 1d -s AAPL MSFT          # dry run
  uv run python tools/pdm_client.py refresh --universe sp500 -t 1d -s AAPL --run --wait
  uv run python tools/pdm_client.py schedules [--symbol AAPL] [--catalog sp500]
  uv run python tools/pdm_client.py schedule-add yf-sp500 --at 22:30 --days mon-fri \
      --refresh '{"provider":"yf","target":{"universe":"sp500"},"timeframes":["1d"]}'
  uv run python tools/pdm_client.py schedule-pause|schedule-resume|schedule-run|schedule-rm <name>
  uv run python tools/pdm_client.py live [--symbol BTC/USDT] [--exchange binance]
  uv run python tools/pdm_client.py live-add binance SOL/USDT --market swap --kind quotes
  uv run python tools/pdm_client.py live-rm binance SOL/USDT --market swap --kind quotes

Host, ports and token default to PDM_SERVER / PDM_REQ_PORT / PDM_PUB_PORT /
PDM_TOKEN from the environment or `.env`. Every command takes `--json`.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pdm.client import PdmError, PriceDataClient, with_datetime_index
from utils.config import PDM_PUB_PORT, PDM_REQ_PORT, PDM_SERVER, PDM_TOKEN
from utils.report import allow_broken_pipe, print_table


def emit(obj, as_json: bool, columns: list[str] | None = None) -> None:
    if as_json or not isinstance(obj, list) or not columns:
        print(json.dumps(obj, indent=2, default=str))
        return
    print_table(columns, [["" if row.get(c) is None else row.get(c) for c in columns] for row in obj])


def main() -> None:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default=PDM_SERVER)
    p.add_argument("--req-port", type=int, default=PDM_REQ_PORT)
    p.add_argument("--pub-port", type=int, default=PDM_PUB_PORT)
    p.add_argument("--token", default=PDM_TOKEN)
    p.add_argument("--timeout", type=float, default=60.0)
    p.add_argument("--json", action="store_true", help="machine-readable output")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping")
    sub.add_parser("catalogs")
    sub.add_parser("topics")
    sub.add_parser("reload")

    def add_filters(sp):
        sp.add_argument("--catalog")
        sp.add_argument("--asset-class")
        sp.add_argument("--symbol", "-s")
        sp.add_argument("--timeframe", "-t")
        sp.add_argument("--provider")
        sp.add_argument("--session")

    sp = sub.add_parser("series")
    add_filters(sp)
    sp.add_argument("--stale-days", type=int)
    sp.add_argument("--limit", type=int, default=1000)

    sp = sub.add_parser("bars")
    add_filters(sp)
    sp.add_argument("--path", help="series path under the data root, instead of filters")
    sp.add_argument("--start", help="ISO date/datetime (UTC) or epoch ms")
    sp.add_argument("--end")
    sp.add_argument("--limit", type=int, help="last N rows only")
    sp.add_argument("--csv", type=Path, help="write the frame to a CSV file")
    sp.add_argument("--parquet", type=Path, help="write the frame to a parquet file")

    sp = sub.add_parser("status")
    sp.add_argument("--catalog")
    sp.add_argument("--stale-days", type=int)

    sp = sub.add_parser("submit", help="queue a tool run: submit [--wait] <tool> -- <args...>")
    sp.add_argument("tool")
    sp.add_argument("tool_args", nargs=argparse.REMAINDER)
    sp.add_argument("--wait", action="store_true", help="block until the job finishes")

    sp = sub.add_parser("jobs")
    sp.add_argument("--state")
    sp.add_argument("--limit", type=int, default=20)

    sp = sub.add_parser("job")
    sp.add_argument("id")
    sp.add_argument("--log", type=int, default=0, metavar="LINES")

    sp = sub.add_parser("cancel")
    sp.add_argument("id")

    sp = sub.add_parser("coverage", help="what is stored, scheduled and live for a symbol")
    sp.add_argument("symbol")

    sp = sub.add_parser("audit", help="read-only check on the host: audit TOOL [option=value ...]")
    sp.add_argument("tool", help="status|query|check_gaps|check_schema|check_catalog|check_splits")
    sp.add_argument("options", nargs="*", metavar="option=value",
                    help="flag=value (bool: flag or flag=true; list: a,b,c)")

    sp = sub.add_parser("refresh", help="one-shot refresh; a dry run unless --run")
    tgt = sp.add_mutually_exclusive_group(required=True)
    tgt.add_argument("--universe")
    tgt.add_argument("--asset-class", choices=["Index", "Commodity"])
    tgt.add_argument("--exchange", help="crypto: binance|bitget|bybit|kucoin")
    sp.add_argument("--trading-mode", default="spot", choices=["spot", "futures"])
    sp.add_argument("--timeframes", "-t", nargs="+", required=True)
    sp.add_argument("--symbols", "-s", nargs="+")
    sp.add_argument("--since")
    sp.add_argument("--full", action="store_true")
    sp.add_argument("--run", action="store_true", help="queue the real run (default: dry run)")
    sp.add_argument("--wait", action="store_true", help="with --run: block until it finishes")

    sp = sub.add_parser("schedules")
    sp.add_argument("--symbol", "-s")
    sp.add_argument("--catalog")

    sp = sub.add_parser("schedule-add", help="create or replace a schedule")
    sp.add_argument("name")
    when = sp.add_mutually_exclusive_group(required=True)
    when.add_argument("--at", help="HH:MM UTC, daily")
    when.add_argument("--every", help="interval: 30s, 15m, 1h, 1d")
    sp.add_argument("--days", help="with --at: mon,wed,fri or mon-fri")
    what = sp.add_mutually_exclusive_group(required=True)
    what.add_argument("--refresh", help="refresh plan as JSON (see the refresh method)")
    what.add_argument("--tool", help="whitelisted tool; its args follow --")
    sp.add_argument("tool_args", nargs=argparse.REMAINDER)
    sp.add_argument("--paused", action="store_true", help="create it disabled")
    for name in ("schedule-rm", "schedule-pause", "schedule-resume", "schedule-run"):
        sub.add_parser(name).add_argument("name")

    sp = sub.add_parser("live", help="live subscriptions and per-symbol freshness")
    sp.add_argument("--symbol", "-s")
    sp.add_argument("--exchange")
    for name in ("live-add", "live-rm"):
        sp = sub.add_parser(name)
        sp.add_argument("exchange")
        sp.add_argument("symbols", nargs="*" if name == "live-rm" else "+")
        sp.add_argument("--market", default="spot", choices=["spot", "swap"])
        sp.add_argument("--kind", default="bars", choices=["bars", "quotes"])
        sp.add_argument("--timeframe", help="bars only (default 1m)")
        if name == "live-add":
            sp.add_argument("--throttle-ms", type=int, help="quotes: at most one per window")
            sp.add_argument("--transport", choices=["auto", "poll"])
            sp.add_argument("--poll-seconds", type=float)

    sp = sub.add_parser("watch", help="print broadcasts: watch [topic-prefix ...]")
    sp.add_argument("prefixes", nargs="*")
    sp.add_argument("--rows", type=int, default=3, help="rows of each frame to print")

    args = p.parse_args()
    client = PriceDataClient(args.host, args.req_port, args.pub_port, args.token, args.timeout)
    filters = {
        k: getattr(args, k)
        for k in ("catalog", "asset_class", "symbol", "timeframe", "provider", "session")
        if getattr(args, k, None)
    }

    try:
        if args.cmd in ("ping", "topics", "reload"):
            emit(client.call(args.cmd), True)
        elif args.cmd == "catalogs":
            emit(client.catalogs(), args.json, ["catalog", "asset_class", "symbols", "series"])
        elif args.cmd == "series":
            res = client.call("series", limit=args.limit, stale_days=args.stale_days, **filters)
            if args.json:
                emit(res, True)
            else:
                emit(res["series"], False, ["catalog", "symbol", "timeframe", "provider",
                                            "session", "start_date", "end_date", "stale_days"])
                print(f"{res['count']} series" + (" (truncated)" if res["truncated"] else ""))
        elif args.cmd == "bars":
            if args.path:
                filters = {"path": args.path}
            df, meta = client.bars(start=args.start, end=args.end, limit=args.limit,
                                   with_meta=True, **filters)
            if args.csv:
                df.to_csv(args.csv, index=False)
            if args.parquet:
                df.to_parquet(args.parquet, index=False)
            if args.json:
                emit({"meta": meta, "bars": df.to_dict(orient="records")}, True)
            else:
                print(json.dumps(meta["series"], default=str))
                print(with_datetime_index(df).tail(20).to_string() if not df.empty else "(no rows)")
                print(f"{len(df)} rows")
        elif args.cmd == "status":
            emit(client.status(catalog=args.catalog, stale_days=args.stale_days), True)
        elif args.cmd == "submit":
            # Everything after the tool name belongs to the tool, so `--wait`
            # must precede it; a leading `--` separator is optional.
            tool_args = args.tool_args[1:] if args.tool_args[:1] == ["--"] else args.tool_args
            job = client.submit(args.tool, tool_args)
            if args.wait:
                job = client.wait(job["id"])
                job = client.job(job["id"], log_lines=30)
            emit(job, True)
            if args.wait and job["state"] != "succeeded":
                sys.exit(1)
        elif args.cmd == "jobs":
            emit(client.jobs(state=args.state, limit=args.limit), args.json,
                 ["id", "state", "tool", "lane", "source", "returncode"])
        elif args.cmd == "job":
            job = client.job(args.id, log_lines=args.log)
            log_text = job.pop("log", None)
            emit(job, True)
            if log_text and not args.json:
                print(log_text)
        elif args.cmd == "cancel":
            emit(client.cancel(args.id), True)
        elif args.cmd == "coverage":
            emit(client.coverage(args.symbol), True)
        elif args.cmd == "audit":
            opts = {}
            for item in args.options:
                key, _, value = item.partition("=")
                key = key.lstrip("-").replace("-", "_")
                if not value or value.lower() == "true":
                    opts[key] = True
                elif value.lstrip("-").isdigit():
                    opts[key] = int(value)
                elif key in ("catalog", "asset_class", "symbols", "timeframes") or \
                        (key == "path" and "," in value):
                    opts[key] = value.split(",")
                else:
                    try:
                        opts[key] = float(value) if key == "tolerance" else value
                    except ValueError:
                        opts[key] = value
            res = client.audit(args.tool, **opts)
            emit(res if args.json else {k: res[k] for k in ("argv", "exit", "ok", "error", "report")}, True)
            if not res["ok"]:
                sys.exit(1)
        elif args.cmd == "refresh":
            if args.exchange:
                plan = {"provider": "crypto", "target": {"exchange": args.exchange,
                                                         "trading_mode": args.trading_mode}}
            else:
                plan = {"provider": "yf", "target": {"universe": args.universe} if args.universe
                        else {"asset_class": args.asset_class}}
            plan["timeframes"] = args.timeframes
            for k in ("symbols", "since"):
                if getattr(args, k):
                    plan[k] = getattr(args, k)
            if args.full:
                plan["full"] = True
            res = client.refresh(dry_run=not args.run, **plan)
            if args.run and args.wait:
                res = client.job(client.wait(res["id"])["id"], log_lines=30)
            if args.json or args.run:
                emit(res, True)
            else:
                print(res["text"])
                if res["plan"]:
                    print(f"planned: {res['plan'].get('planned')}  current: {res['plan'].get('current')}")
                if res["refused"]:
                    print(f"refused: {res['error']}", file=sys.stderr)
            if (not args.run and res["refused"]) or (args.run and args.wait and res["state"] != "succeeded"):
                sys.exit(1)
        elif args.cmd == "schedules":
            rows = client.schedules(symbol=args.symbol, catalog=args.catalog)
            for r in rows:
                r["when"] = f"{r['at']} {','.join(r['days'] or ['daily'])}" if r["at"] else f"every {r['every']}"
                r["covers"] = r["targets"]["catalog"] or "-"
                r["symbols"] = ",".join(r["targets"]["symbols"] or ["all"])
            emit(rows, args.json, ["name", "kind", "enabled", "when", "covers", "symbols",
                                   "next_run", "last_run", "last_job_id"])
        elif args.cmd == "schedule-add":
            sched = {"name": args.name, "enabled": not args.paused}
            if args.at:
                sched["at"] = args.at
            else:
                sched["every"] = args.every
            if args.days:
                days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
                if "-" in args.days:
                    a, b = args.days.split("-")
                    sched["days"] = days[days.index(a[:3]):days.index(b[:3]) + 1]
                else:
                    sched["days"] = args.days.split(",")
            if args.refresh:
                sched["refresh"] = json.loads(args.refresh)
            else:
                sched["tool"] = args.tool
                sched["args"] = args.tool_args[1:] if args.tool_args[:1] == ["--"] else args.tool_args
            emit(client.schedule_put(sched), True)
        elif args.cmd.startswith("schedule-"):
            action = {"rm": "remove"}.get(args.cmd[9:], args.cmd[9:])
            emit(client.call(f"schedule_{action}", name=args.name), True)
        elif args.cmd == "live":
            res = client.live(symbol=args.symbol, exchange=args.exchange)
            if args.json:
                emit(res, True)
            else:
                rows = [{"key": s["key"], **sym} for s in res["subscriptions"] for sym in s["symbols"]]
                emit(rows, False, ["key", "symbol", "topic", "transport", "updates", "age_s", "errors"])
                print(f"qwant_compat: {res['qwant_compat']}")
        elif args.cmd in ("live-add", "live-rm"):
            sub_args = {"market": args.market, "kind": args.kind, "timeframe": args.timeframe}
            if args.cmd == "live-add":
                opts = {k: getattr(args, k) for k in ("throttle_ms", "transport", "poll_seconds")
                        if getattr(args, k) is not None}
                emit(client.live_add(args.exchange, args.symbols, **sub_args, **opts), True)
            else:
                emit(client.live_remove(args.exchange, args.symbols, **sub_args), True)
        elif args.cmd == "watch":
            for topic, header, frame in client.subscribe(*args.prefixes):
                if args.json:
                    print(json.dumps({"topic": topic, "header": header,
                                      "bars": None if frame is None else frame.to_dict(orient="records")},
                                     default=str), flush=True)
                    continue
                print(f"{topic}  {json.dumps(header, default=str)}", flush=True)
                if frame is not None and args.rows:
                    print(with_datetime_index(frame).tail(args.rows).to_string(), flush=True)
    except (PdmError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
    except KeyboardInterrupt:
        pass
    finally:
        client.close()


if __name__ == "__main__":
    main()
