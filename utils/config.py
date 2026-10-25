"""Environment-resolved configuration.

Every machine-specific value lives here and is overridable by an environment
variable, so the repo runs unchanged on another checkout, another user, or in
CI. The defaults reproduce the historical hardcoded values, so nothing changes
for an existing working copy.

See `.env.example` for the full list.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """Apply `KEY=VALUE` lines from a repo `.env`, never overriding the real
    environment — so `.env` sets a machine's defaults and an exported variable
    still wins. Deliberately minimal (no interpolation, no `export`), which is
    all `.env.example` uses; no python-dotenv dependency for six lines.
    """
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        if key and value:
            os.environ.setdefault(key, value)


# PDM_NO_DOTENV lets the test suite opt out of a developer's local .env.
if not os.environ.get("PDM_NO_DOTENV"):
    _load_dotenv(REPO_ROOT / ".env")


def _path_from_env(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser() if raw else default


def _int_from_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


# --- data -------------------------------------------------------------------

#: Root of the price-data tree. Defaults to the `price-data/` symlink in the
#: repo; override with PRICE_DATA_ROOT to point at a checkout elsewhere.
DATA_ROOT = _path_from_env("PRICE_DATA_ROOT", REPO_ROOT / "price-data")

ASSETS_YAML = DATA_ROOT / "assets.yaml"

#: Scratch/checkpoint root for download progress files and provider logs.
TMP_ROOT = _path_from_env("TMP_ROOT", REPO_ROOT / "tmp")


# --- providers --------------------------------------------------------------

#: freqtrade executable used by tools/download_crypto.py for spot OHLCV.
FREQTRADE_BIN = _path_from_env(
    "FREQTRADE_BIN", Path.home() / "work" / "ai-quant" / ".venv" / "bin" / "freqtrade"
)

IBKR_HOST = os.environ.get("IBKR_HOST", "127.0.0.1")
IBKR_PORT = _int_from_env("IBKR_PORT", 7497)

#: Base client id. Each tool offsets from this so two tools can run against one
#: gateway without colliding (IBKR rejects duplicate client ids).
IBKR_CLIENT_ID = _int_from_env("IBKR_CLIENT_ID", 73)


# --- price data manager (PDM) server ----------------------------------------

#: Interface the server binds. 0.0.0.0 = every interface; set it to the
#: machine's Tailscale/LAN address to keep the service off other networks.
PDM_BIND_HOST = os.environ.get("PDM_BIND_HOST", "0.0.0.0")

#: Host clients connect to.
PDM_SERVER = os.environ.get("PDM_SERVER", "127.0.0.1")

#: ROUTER (request/reply) and PUB (broadcast) ports.
PDM_REQ_PORT = _int_from_env("PDM_REQ_PORT", 5555)
PDM_PUB_PORT = _int_from_env("PDM_PUB_PORT", 5556)

#: Read-only HTTP gateway port (same bind host); 0 disables it.
PDM_HTTP_PORT = _int_from_env("PDM_HTTP_PORT", 5580)

#: Command the server hands each alert to (pdm/alerts.py); empty = alerts are
#: only logged and broadcast. Often holds a secret (a ntfy topic): .env only.
PDM_NOTIFY_CMD = os.environ.get("PDM_NOTIFY_CMD", "")

#: A series this many days behind its catalog end_date counts as stale for
#: alerts. 4 rides out a weekend plus a holiday on a daily equity series.
PDM_STALE_ALERT_DAYS = _int_from_env("PDM_STALE_ALERT_DAYS", 4)

#: File holding the token, outside the checkout — so a sandbox that can read
#: the repo (an LLM agent running the audit scripts) can't read the secret.
PDM_TOKEN_FILE = _path_from_env(
    "PDM_TOKEN_FILE",
    Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config") / "ai-data-manager" / "token",
)


def _read_token() -> str:
    if os.environ.get("PDM_TOKEN"):
        return os.environ["PDM_TOKEN"]
    try:
        return PDM_TOKEN_FILE.read_text().strip()
    except OSError:
        return ""


#: Shared secret for requests that change state (refresh, submit, cancel,
#: schedule_*, live_*). `PDM_TOKEN` in the environment wins, else the first
#: line of `PDM_TOKEN_FILE`. Empty means unauthenticated — acceptable only on a
#: private network such as a tailnet.
PDM_TOKEN = _read_token()

#: Server config: schedules and live streams. See `pdm.example.yaml`.
PDM_CONFIG = _path_from_env("PDM_CONFIG", REPO_ROOT / "pdm.yaml")


#: Prefix every stored `file_path` carries, so a catalog written on one machine
#: resolves on another. Paths are anchored to DATA_ROOT, not to the checkout.
DATA_PREFIX = "price-data"


def relative_to_repo(path: Path) -> str:
    """Portable string for a path, for storage in catalog YAML.

    Anchored to DATA_ROOT rather than to the checkout, so a catalog stays valid
    when PRICE_DATA_ROOT points outside the repo — or, as in the default
    layout, when `price-data/` is a symlink whose target resolves elsewhere.
    Comparison is done on resolved paths so the symlink cannot defeat it.
    """
    candidate = Path(path)
    for base in (DATA_ROOT, DATA_ROOT.resolve(), REPO_ROOT):
        for probe in (candidate, candidate.resolve()):
            try:
                rel = probe.relative_to(base)
            except ValueError:
                continue
            # A REPO_ROOT-relative hit already starts with the prefix.
            return str(rel) if base is REPO_ROOT else str(Path(DATA_PREFIX) / rel)
    return str(candidate)


def resolve_from_repo(stored: str) -> Path:
    """Inverse of `relative_to_repo` — resolve a stored catalog path.

    A stored path is `price-data/...`; it resolves under DATA_ROOT wherever
    that points. Absolute paths (written by older versions of this function)
    are still honoured so existing catalogs keep working.
    """
    path = Path(stored)
    if path.is_absolute():
        return path
    if path.parts and path.parts[0] == DATA_PREFIX:
        return DATA_ROOT.joinpath(*path.parts[1:])
    return REPO_ROOT / path
