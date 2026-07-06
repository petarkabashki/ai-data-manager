"""Core data access utilities for price-data/."""

from pathlib import Path
import yaml
import pandas as pd

DATA_ROOT = Path(__file__).parent.parent / "price-data"
ASSETS_YAML = DATA_ROOT / "assets.yaml"


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
    path = Path(__file__).parent.parent / file_path
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, index_col=0, parse_dates=True)


def asset_path(symbol: str, timeframe: str = "1d", provider: str | None = None) -> Path | None:
    """Return resolved Path for a symbol/timeframe, or None if not found.

    Handles both the flat timeframe_dates shape (crypto/index/commodity/macro,
    single provider per timeframe) and the nested-by-provider shape (stocks,
    where timeframe_dates[tf] is itself a dict keyed by provider name)."""
    assets = load_assets()
    entry = assets.get(symbol)
    if not entry:
        return None
    node = entry.get("timeframe_dates", {}).get(timeframe)
    if not node:
        return None
    if "file_path" in node:
        fp = node["file_path"]
    else:
        chosen = node.get(provider) if provider else next(iter(node.values()), None)
        fp = chosen.get("file_path") if chosen else None
    if not fp:
        return None
    return Path(__file__).parent.parent / fp
