"""Read-only audits on the data host, open to callers without the token.

`audit` runs one of the workspace's checking tools with `--json` and returns
its report. The caller passes typed *options*, never argv: each tool has a
fixed option table below, so nothing that writes can be expressed —
`check_catalog --fix` has no option here, and the tools themselves write
nothing else (no output-file flags). Paths must lie under the data root.

Request:  {"tool": "check_gaps", "options": {"catalog": ["crypto_binance"], "timeframe": "1d"}}
Reply:    {"tool", "argv", "exit", "ok", "report": <the tool's JSON>|null, "error", "stderr"}
    `exit` != 0 is not a failed request: check tools exit non-zero when they
    find problems. `error` is set when the tool refused its arguments (exit 2)
    or produced no JSON.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

AUDIT_TIMEOUT_S = 600
MAX_CONCURRENT_AUDITS = 2

_TEXT = re.compile(r"^[A-Za-z0-9._^=:/-]{1,64}$")

#: tool → {option: kind}. Option names are the flags with `_` for `-`.
AUDIT_TOOLS: dict[str, dict[str, str]] = {
    "status": {"catalog": "list", "stale_days": "int", "deep": "bool"},
    "query": {
        "catalog": "list", "asset_class": "list", "symbol": "str", "symbols": "list",
        "timeframe": "str", "timeframes": "list", "provider": "str", "stale_days": "int",
        "rescan": "bool", "list_catalogs": "bool", "list_symbols": "bool", "limit": "int",
    },
    "check_gaps": {
        "exchange": "str", "catalog": "list", "symbol": "str", "symbols": "list",
        "timeframe": "str", "timeframes": "list", "all": "bool", "all_catalogs": "bool",
        "internal": "bool", "stale_days": "int", "trading_mode": "str", "calendar": "str",
    },
    "check_schema": {"path": "path"},
    "check_catalog": {"catalog": "list", "skip_dates": "bool", "skip_orphans": "bool"},
    "check_splits": {"path": "paths", "tolerance": "float"},
}


class AuditError(ValueError):
    pass


def _path(value, data_root: Path) -> str:
    if not isinstance(value, str) or not value:
        raise AuditError("a path must be a non-empty string")
    target = (data_root / value).resolve()
    if not target.is_relative_to(data_root.resolve()):
        raise AuditError(f"path {value!r} is not under the data root")
    return str(target)


def build_argv(tool: str, options: dict | None, data_root: Path) -> list[str]:
    """Typed options → argv for one audit tool (always with --json). Raises AuditError."""
    if tool not in AUDIT_TOOLS:
        raise AuditError(f"audit tool must be one of {sorted(AUDIT_TOOLS)}")
    options = options or {}
    if not isinstance(options, dict):
        raise AuditError("options must be an object")
    table = AUDIT_TOOLS[tool]
    unknown = set(options) - set(table)
    if unknown:
        raise AuditError(f"{tool}: unknown option(s) {sorted(unknown)}; allowed: {sorted(table)}")
    argv: list[str] = []
    for name, value in options.items():
        flag = "--" + name.replace("_", "-")
        kind = table[name]
        if value is None or value is False:
            continue
        if kind == "bool":
            if value is not True:
                raise AuditError(f"{name} must be true or false")
            argv.append(flag)
        elif kind in ("int", "float"):
            if isinstance(value, bool) or not isinstance(value, (int, float)) \
                    or (kind == "int" and not isinstance(value, int)):
                raise AuditError(f"{name} must be a{'n integer' if kind == 'int' else ' number'}")
            argv += [flag, str(value)]
        elif kind == "str":
            if not isinstance(value, str) or not _TEXT.match(value):
                raise AuditError(f"{name}: invalid value {value!r}")
            argv += [flag, value]
        elif kind == "list":
            if isinstance(value, str):
                value = [value]
            if not isinstance(value, list) or not value or len(value) > 2000 \
                    or not all(isinstance(v, str) and _TEXT.match(v) for v in value):
                raise AuditError(f"{name} must be a list of names")
            argv += [flag, *value]
        elif kind == "path":
            argv += [flag, _path(value, data_root)]
        elif kind == "paths":
            values = [value] if isinstance(value, str) else value
            if not isinstance(values, list) or not values or len(values) > 500:
                raise AuditError(f"{name} must be a path or a list of paths")
            for v in values:
                argv += [flag, _path(v, data_root)]
    argv.append("--json")
    return argv


def run(tools_dir: Path, tool: str, argv: list[str], python: str = sys.executable,
        timeout_s: float = AUDIT_TIMEOUT_S) -> dict:
    full = [tool, *argv]
    try:
        proc = subprocess.run(
            [python, str(Path(tools_dir) / f"{tool}.py"), *argv],
            capture_output=True, text=True, timeout=timeout_s, cwd=Path(tools_dir).parent,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return {"tool": tool, "argv": full, "exit": None, "ok": False, "report": None,
                "error": f"audit exceeded {timeout_s:g}s; narrow it or submit it as a job",
                "stderr": ""}
    report = None
    if proc.stdout.strip():
        try:
            report = json.loads(proc.stdout)
        except json.JSONDecodeError:
            report = None
    error = None
    if proc.returncode == 2:
        error = next((ln.split("error:", 1)[1].strip() for ln in proc.stderr.splitlines()
                      if "error:" in ln), proc.stderr.strip()[-500:] or "exit 2")
    elif report is None:
        error = (proc.stderr or proc.stdout).strip()[-500:] or f"no JSON report (exit {proc.returncode})"
    return {
        "tool": tool,
        "argv": full,
        "exit": proc.returncode,
        "ok": error is None,
        "report": report,
        "error": error,
        "stderr": "\n".join(proc.stderr.splitlines()[-50:]),
    }
