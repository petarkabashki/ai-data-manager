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


def asset_path(symbol: str, timeframe: str = "1d") -> Path | None:
    """Return resolved Path for a symbol/timeframe, or None if not found."""
    assets = load_assets()
    entry = assets.get(symbol)
    if not entry:
        return None
    fp = entry.get("timeframe_dates", {}).get(timeframe, {}).get("file_path")
    if not fp:
        return None
    return Path(__file__).parent.parent / fp
