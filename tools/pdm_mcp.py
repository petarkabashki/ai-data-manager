#!/usr/bin/env -S uv run python
"""MCP server (stdio) that gives agents the PDM server as native tools.

A thin adapter over `pdm.client.PriceDataClient`: every tool is one request
to the PDM server named by PDM_SERVER / PDM_REQ_PORT / PDM_TOKEN (env or
`.env`). It holds no data and runs no downloads itself.

Reads are separate tools from writes, so a harness's permission prompt is the
approval step: allow the `read-only` tools once, keep the rest on "ask".
`pdm_refresh_preview` and `pdm_refresh_run` are two tools for that reason —
an agent can always see what a refresh would do, and only an approved call
can start one.

Register (Claude Code; `.mcp.json` in this repo does it for the project):
  claude mcp add pdm -- uv run --project <repo> python <repo>/tools/pdm_mcp.py

  uv run python tools/pdm_mcp.py            # stdio; normally launched by the harness
  uv run python tools/pdm_mcp.py --list     # print the tool names and exit
"""

from __future__ import annotations

import argparse
import functools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from pdm.client import PdmError, PriceDataClient
from utils.config import PDM_PUB_PORT, PDM_REQ_PORT, PDM_SERVER, PDM_TOKEN

#: Rows `pdm_bars` returns at most — an agent reading a whole 1m history into
#: its context helps no one; use the CLI or Python client for bulk pulls.
MAX_BARS = 500

READ = ToolAnnotations(read_only_hint=True, open_world_hint=False)
WRITE = ToolAnnotations(read_only_hint=False, destructive_hint=False, open_world_hint=True)
REMOVE = ToolAnnotations(read_only_hint=False, destructive_hint=True, open_world_hint=False)

mcp = MCPServer(
    "pdm",
    instructions=(
        "Price data manager: stored OHLCV across stocks, indexes, crypto, futures and options; "
        "refresh jobs, schedules and live exchange feeds. Start with pdm_catalogs or "
        "pdm_coverage(symbol). Timestamps are epoch ms UTC. Preview every refresh with "
        "pdm_refresh_preview before asking to run it."
    ),
)
_client: PriceDataClient | None = None


def client() -> PriceDataClient:
    global _client
    if _client is None:
        _client = PriceDataClient(PDM_SERVER, PDM_REQ_PORT, PDM_PUB_PORT, PDM_TOKEN, timeout=60)
    return _client


def tool(annotations: ToolAnnotations):
    """`mcp.tool`, with server refusals and bad arguments surfaced as tool errors
    whose message the agent can read and act on (e.g. the candidate list of an
    ambiguous `pdm_bars`), rather than an opaque internal failure."""
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except (PdmError, ValueError, TimeoutError) as exc:
                raise ToolError(str(exc)) from exc
        return mcp.tool(annotations=annotations)(wrapper)
    return deco


def _given(**kw) -> dict:
    return {k: v for k, v in kw.items() if v is not None}


# --- reads ----------------------------------------------------------------------


@tool(READ)
def pdm_ping() -> dict:
    """Server health: version, uptime, series indexed, job lanes, schedule and live counts."""
    return client().ping()


@tool(READ)
def pdm_catalogs() -> list[dict]:
    """Every catalog (sp500, nasdaq100, indexes, crypto_binance, ...) with symbol and series counts."""
    return client().catalogs()


@tool(READ)
def pdm_series(catalog: str | None = None, asset_class: str | None = None, symbol: str | None = None,
               timeframe: str | None = None, provider: str | None = None,
               stale_days: int | None = None, limit: int = 200) -> dict:
    """Find stored series. `stale_days=N` keeps only series whose last bar is >= N days old.
    Crypto symbols use underscores (BTC_USDT)."""
    return client().call("series", limit=min(limit, 2000), **_given(
        catalog=catalog, asset_class=asset_class, symbol=symbol, timeframe=timeframe,
        provider=provider, stale_days=stale_days))


@tool(READ)
def pdm_bars(symbol: str | None = None, timeframe: str | None = None, catalog: str | None = None,
             provider: str | None = None, session: str | None = None, path: str | None = None,
             start: str | None = None, end: str | None = None, limit: int = 100) -> dict:
    """OHLCV rows for one series, newest last, at most 500. Identify it by filters
    (must match exactly one series; the error lists candidates) or by `path`.
    start/end: ISO date or datetime (UTC)."""
    filters = {"path": path} if path else _given(symbol=symbol, timeframe=timeframe, catalog=catalog,
                                                 provider=provider, session=session)
    df, meta = client().bars(start=start, end=end, limit=max(1, min(limit, MAX_BARS)),
                             with_meta=True, **filters)
    return {"series": meta.get("series"), "rows": len(df), "columns": list(df.columns),
            "bars": df.astype(object).values.tolist()}   # object: timestamps stay ints


@tool(READ)
def pdm_status(catalog: str | None = None, stale_days: int | None = None) -> dict:
    """The workspace status report (tools/status.py --json): freshness per catalog."""
    return client().status(**_given(catalog=catalog, stale_days=stale_days))


@tool(READ)
def pdm_coverage(symbol: str) -> dict:
    """Everything about one symbol: stored series, the schedules that refresh it, live feeds."""
    return client().coverage(symbol)


@tool(READ)
def pdm_schedules(symbol: str | None = None, catalog: str | None = None) -> list[dict]:
    """Recurring jobs with next/last run; filter to those covering a symbol or catalog."""
    return client().schedules(symbol=symbol, catalog=catalog)


@tool(READ)
def pdm_live(symbol: str | None = None, exchange: str | None = None) -> dict:
    """Live subscriptions (bars/quotes) with per-symbol topic, transport, update count and age."""
    return client().live(symbol=symbol, exchange=exchange)


@tool(READ)
def pdm_jobs(state: str | None = None, limit: int = 20) -> list[dict]:
    """Recent jobs, newest first. state: queued|running|succeeded|failed|cancelled|timed_out."""
    return client().jobs(**_given(state=state), limit=limit)


@tool(READ)
def pdm_job(job_id: str, log_lines: int = 30) -> dict:
    """One job, with the tail of its log and (refresh jobs) the parsed result."""
    return client().job(job_id, log_lines=min(log_lines, 300))


@tool(READ)
def pdm_audit(tool: str, options: dict | None = None) -> dict:
    """Run a read-only check on the data host and return its JSON report.
    tool: status | query | check_gaps | check_schema | check_catalog | check_splits.
    options: the tool's flags with _ for -, e.g. {"catalog": ["sp500"], "timeframe": "1d"}
    for check_gaps, {"list_catalogs": true} for query. A non-zero `exit` with ok=true means
    the check found problems."""
    return client().audit(tool, **(options or {}))


@tool(READ)
def pdm_refresh_preview(timeframes: list[str], universe: str | None = None,
                        asset_class: str | None = None, exchange: str | None = None,
                        trading_mode: str = "spot", symbols: list[str] | None = None,
                        since: str | None = None, full: bool = False) -> dict:
    """Dry-run a refresh: what would be fetched, per symbol and timeframe. Writes nothing.
    Target exactly one of: universe (sp500|nasdaq100|russell2000|delisted_stocks),
    asset_class (Index|Commodity), or exchange (crypto: binance|bitget|bybit|kucoin)."""
    return client().refresh(dry_run=True, **_plan(timeframes, universe, asset_class, exchange,
                                                  trading_mode, symbols, since, full))


# --- writes (keep these on "ask") -------------------------------------------------


@tool(WRITE)
def pdm_refresh_run(timeframes: list[str], universe: str | None = None,
                    asset_class: str | None = None, exchange: str | None = None,
                    trading_mode: str = "spot", symbols: list[str] | None = None,
                    since: str | None = None, full: bool = False) -> dict:
    """Queue a real refresh (needs the user's approval). Same arguments as pdm_refresh_preview;
    the server re-checks it with a dry run, runs one refresh at a time, kills it after 30
    minutes. Returns the job; poll pdm_job."""
    return client().refresh(dry_run=False, **_plan(timeframes, universe, asset_class, exchange,
                                                   trading_mode, symbols, since, full))


@tool(REMOVE)
def pdm_cancel(job_id: str) -> dict:
    """Cancel a queued or running job."""
    return client().cancel(job_id)


@tool(WRITE)
def pdm_schedule_put(schedule: dict) -> dict:
    """Create or replace a schedule by name. Shape: {name, at: "HH:MM" (UTC) [, days: [mon..sun]]
    | every: "30m"|"6h"|"1d", refresh: {provider, target, timeframes[, symbols]} | tool, args,
    enabled}. Saved to pdm.yaml."""
    return client().schedule_put(schedule)


@tool(WRITE)
def pdm_schedule_set(name: str, action: str) -> dict:
    """Pause, resume or run-now one schedule. action: pause|resume|run."""
    if action not in ("pause", "resume", "run"):
        raise ValueError("action must be pause, resume or run")
    return client().call(f"schedule_{action}", name=name)


@tool(REMOVE)
def pdm_schedule_remove(name: str) -> dict:
    """Delete a schedule (removed from pdm.yaml)."""
    return client().schedule_remove(name)


@tool(WRITE)
def pdm_live_add(exchange: str, symbols: list[str], market: str = "spot", kind: str = "bars",
                 timeframe: str | None = None, throttle_ms: int | None = None) -> dict:
    """Start live feeds. kind bars (closed + forming candles, timeframe default 1m) or quotes
    (best bid/ask); market spot|swap; symbols like BTC/USDT. Saved to pdm.yaml."""
    return client().live_add(exchange, symbols, market=market, kind=kind, timeframe=timeframe,
                             **_given(throttle_ms=throttle_ms))


@tool(REMOVE)
def pdm_live_remove(exchange: str, symbols: list[str] | None = None, market: str = "spot",
                    kind: str = "bars", timeframe: str | None = None) -> dict:
    """Stop live feeds; no symbols removes the whole subscription."""
    return client().live_remove(exchange, symbols, market=market, kind=kind, timeframe=timeframe)


def _plan(timeframes, universe, asset_class, exchange, trading_mode, symbols, since, full) -> dict:
    if sum(x is not None for x in (universe, asset_class, exchange)) != 1:
        raise ValueError("give exactly one of universe, asset_class or exchange")
    if exchange:
        plan = {"provider": "crypto", "target": {"exchange": exchange, "trading_mode": trading_mode}}
    else:
        plan = {"provider": "yf",
                "target": {"universe": universe} if universe else {"asset_class": asset_class}}
    plan["timeframes"] = timeframes
    plan.update(_given(symbols=symbols or None, since=since))
    if full:
        plan["full"] = True
    return plan


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--list", action="store_true", help="print the tool names and exit")
    args = p.parse_args()
    if args.list:
        import asyncio

        for tool in asyncio.run(mcp.list_tools()):
            ro = (tool.annotations and tool.annotations.read_only_hint)
            print(f"{tool.name:22} {'read-only' if ro else 'write'}")
        return
    mcp.run("stdio")


if __name__ == "__main__":
    main()
