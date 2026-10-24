"""Core data access utilities for price-data/."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from utils.config import ASSETS_YAML, DATA_ROOT, resolve_from_repo  # noqa: F401 (re-exported)


def load_assets() -> dict:
    """Load merged asset registry from assets.yaml (resolves all includes)."""
    with open(ASSETS_YAML) as f:
        top = yaml.safe_load(f)

    includes = top.pop("include", [])
    merged = {}
    for inc in includes:
        with open(DATA_ROOT / inc) as f:
            merged.update(yaml.safe_load(f) or {})
    merged.update(top)
    return merged


def load_ohlcv(file_path: str | Path) -> pd.DataFrame:
    """Load OHLCV data from a parquet or CSV file path (relative to repo root)."""
    path = resolve_from_repo(str(file_path))
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, index_col=0, parse_dates=True)


def _select_provider_node(node: dict, provider: str | None, session: str | None) -> dict | None:
    """Pick one node out of a nested-by-provider `timeframe_dates[tf]` dict.

    Node keys are `PROVIDER` or `PROVIDER:session` (see
    `utils.catalog.provider_node_key`). Matching is case-insensitive on the
    provider part so callers don't have to know that directories are lowercase
    while catalog nodes are upper.
    """
    if not node:
        return None
    if provider is None and session is None:
        return next(iter(node.values()), None)

    wanted_provider = provider.lower() if provider else None
    for key, value in node.items():
        key_provider, _, key_session = key.partition(":")
        if wanted_provider and key_provider.lower() != wanted_provider:
            continue
        if session is not None and (key_session or "all") != session:
            continue
        return value
    return None


def asset_path(
    symbol: str,
    timeframe: str = "1d",
    provider: str | None = None,
    session: str | None = None,
) -> Path | None:
    """Return resolved Path for a symbol/timeframe, or None if not found.

    Handles both the flat `timeframe_dates` shape (crypto, commodities, macro —
    a single provider per timeframe) and the nested-by-provider shape (stocks
    and indexes, where `timeframe_dates[tf]` is itself keyed by provider, and
    by session for IBKR intraday data).
    """
    assets = load_assets()
    entry = assets.get(symbol)
    if not entry:
        return None
    node = entry.get("timeframe_dates", {}).get(timeframe)
    if not node:
        return None

    if "file_path" in node:  # flat shape
        fp = node["file_path"]
    else:
        chosen = _select_provider_node(node, provider, session)
        fp = chosen.get("file_path") if chosen else None

    if not fp:
        return None
    return resolve_from_repo(fp)
