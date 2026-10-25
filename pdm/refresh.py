"""Approved one-shot refreshes: a typed plan in, one capped tool run out.

`submit` takes raw argv for any whitelisted tool. `refresh` is narrower on
purpose: the caller (forthkit's approval flow, or an agent through MCP) states
*what* to refresh, the server builds the argv, previews it with `--dry-run`,
and only then queues the real run — at most one refresh at a time, killed
after `timeout_s` (default and ceiling: 30 minutes).

Request params:

    provider    "yf" (default) | "crypto"
    target      yf:     {"universe": sp500|nasdaq100|russell2000|delisted_stocks}
                        | {"asset_class": "Index"|"Commodity"}
                crypto: {"exchange": binance|bitget|bybit|kucoin,
                         "trading_mode": "spot"|"futures"}
    timeframes  ["1d", ...]                required
    symbols     ["AAPL", ...] | ["BTC/USDT", ...]   optional; crypto default --all-pairs
    since       "YYYY-MM-DD"               optional
    full        bool                       default false
    dry_run     bool                       REQUIRED — no default, by design
    timeout_s   int                        optional, <= the server cap

Reply, dry_run=true:  {"dry_run": true, "refused": bool, "error": str|null,
                       "exit": int, "argv": [...], "plan": {...}|null, "text": str}
    `plan` is `download_yf --json` output (keys: target, provider, dry_run,
    adjusted, calendar, timeframes, symbols, plan[], planned, current,
    results[], updated, failures[]). Crypto has no JSON mode yet: plan is null
    and `text` carries the tool's preview.
Reply, dry_run=false: the queued job (see `job`), with `meta.preview` holding
    the dry-run reply it passed. Poll `job <id>`; when terminal, `result` is
    the real run's `download_yf --json` document. States: succeeded, failed,
    cancelled, timed_out.
"""

from __future__ import annotations

import re
import subprocess
import sys
from datetime import date
from pathlib import Path

from utils.catalog import CRYPTO_EXCHANGES, STOCK_UNIVERSES, TF_TO_MS
from utils.yf import OUR_TF_TO_YF_INTERVAL

DEFAULT_TIMEOUT_S = 1800
DRY_RUN_TIMEOUT_S = 120
REFRESH_SOURCE = "refresh"

_SYMBOL = re.compile(r"^[A-Za-z0-9.^=_/:-]{1,40}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class PlanError(ValueError):
    pass


def _list_of_str(value, name: str, pattern: re.Pattern | None = None, limit: int = 2000) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise PlanError(f"{name} must be a list of strings")
    if len(value) > limit:
        raise PlanError(f"{name}: at most {limit} entries")
    if pattern:
        bad = [v for v in value if not pattern.match(v)]
        if bad:
            raise PlanError(f"{name}: invalid entries {bad[:5]}")
    return list(value)


def build_argv(params: dict) -> tuple[str, list[str], dict]:
    """Validate a refresh plan → (tool, argv, normalized plan). Raises PlanError."""
    known = {"provider", "target", "timeframes", "symbols", "since", "full", "dry_run", "timeout_s"}
    unknown = set(params) - known
    if unknown:
        raise PlanError(f"unknown refresh field(s): {sorted(unknown)}")
    if not isinstance(params.get("dry_run"), bool):
        raise PlanError("dry_run is required and must be true or false")

    provider = params.get("provider", "yf")
    target = params.get("target")
    if not isinstance(target, dict):
        raise PlanError("target must be an object")
    timeframes = _list_of_str(params.get("timeframes"), "timeframes", limit=20)
    if not timeframes:
        raise PlanError("timeframes is required")
    symbols = _list_of_str(params.get("symbols"), "symbols", _SYMBOL)
    since = params.get("since")
    if since is not None:
        if not isinstance(since, str) or not _DATE.match(since):
            raise PlanError("since must be YYYY-MM-DD")
        date.fromisoformat(since)
    full = params.get("full", False)
    if not isinstance(full, bool):
        raise PlanError("full must be true or false")

    argv: list[str] = []
    if provider == "yf":
        tool = "download_yf"
        if set(target) == {"universe"} and target["universe"] in STOCK_UNIVERSES:
            argv.append(f"--universe={target['universe']}")
        elif set(target) == {"asset_class"} and target["asset_class"] in ("Index", "Commodity"):
            argv.append(f"--asset-class={target['asset_class']}")
        else:
            raise PlanError(
                f"yf target must be {{universe: {'|'.join(STOCK_UNIVERSES)}}} "
                "or {asset_class: Index|Commodity}"
            )
        bad = [tf for tf in timeframes if tf not in OUR_TF_TO_YF_INTERVAL]
        if bad:
            raise PlanError(f"Yahoo has no {bad} bar; valid: {list(OUR_TF_TO_YF_INTERVAL)}")
        argv += ["--timeframes", *timeframes]
        if symbols:
            argv += ["--symbols", *symbols]
        argv.append("--json")
    elif provider == "crypto":
        tool = "download_crypto"
        exchange = target.get("exchange")
        mode = target.get("trading_mode", "spot")
        if exchange not in CRYPTO_EXCHANGES or mode not in ("spot", "futures") \
                or set(target) - {"exchange", "trading_mode"}:
            raise PlanError(f"crypto target must be {{exchange: {'|'.join(CRYPTO_EXCHANGES)}, "
                            "trading_mode: spot|futures}")
        bad = [tf for tf in timeframes if tf not in TF_TO_MS]
        if bad:
            raise PlanError(f"unknown timeframe(s) {bad}")
        argv += [f"--exchange={exchange}", f"--trading-mode={mode}", "--timeframes", *timeframes]
        argv += ["--pairs", *symbols] if symbols else ["--all-pairs"]
    else:
        raise PlanError("provider must be yf or crypto")

    if since:
        argv.append(f"--since={since}")
    if full:
        argv.append("--full")

    plan = {"provider": provider, "target": target, "timeframes": timeframes,
            "symbols": symbols or None, "since": since, "full": full}
    return tool, argv, plan


def preview(tools_dir: Path, tool: str, argv: list[str], python: str = sys.executable,
            timeout_s: float = DRY_RUN_TIMEOUT_S) -> dict:
    """Run the tool with --dry-run and report what it would do."""
    import json

    full_argv = [*argv, "--dry-run"]
    try:
        proc = subprocess.run(
            [python, str(Path(tools_dir) / f"{tool}.py"), *full_argv],
            capture_output=True, text=True, timeout=timeout_s, cwd=Path(tools_dir).parent,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return {"dry_run": True, "refused": True, "exit": None, "argv": [tool, *full_argv],
                "plan": None, "text": "", "error": f"dry run exceeded {timeout_s:g}s"}

    plan = None
    if "--json" in argv and proc.stdout.strip():
        try:
            plan = json.loads(proc.stdout)
        except json.JSONDecodeError:
            plan = None
    text = proc.stderr if plan is not None else (proc.stdout + proc.stderr)
    # argparse refusals (unknown symbol, bad flag) exit 2 with "error: ..."
    error = None
    if proc.returncode == 2:
        error = next((ln.split("error:", 1)[1].strip() for ln in proc.stderr.splitlines()
                      if "error:" in ln), proc.stderr.strip()[-500:])
    elif proc.returncode != 0:
        error = (proc.stderr or proc.stdout).strip()[-500:] or f"exit {proc.returncode}"
    return {
        "dry_run": True,
        "refused": proc.returncode != 0,
        "exit": proc.returncode,
        "argv": [tool, *full_argv],
        "plan": plan,
        "text": "\n".join(text.splitlines()[-200:]),
        "error": error,
    }
