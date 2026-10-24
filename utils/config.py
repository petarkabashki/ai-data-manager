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
