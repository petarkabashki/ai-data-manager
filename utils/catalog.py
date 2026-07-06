"""Crypto catalog management — YAML metadata CRUD and parquet date scanning."""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import yaml

REPO_ROOT = Path(__file__).parent.parent
DATA_ROOT = REPO_ROOT / "price-data"

FREQTRADE_BIN = Path(
    os.environ.get("FREQTRADE_BIN", "/home/grenada/work/ai-quant/.venv/bin/freqtrade")
)

CRYPTO_EXCHANGES = ["binance", "bitget", "bybit", "kucoin"]

# Our timeframe names → milliseconds (None = variable, skip gap math)
TF_TO_MS: dict[str, int | None] = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 3_600_000,
    "2h": 2 * 3_600_000,
    "4h": 4 * 3_600_000,
    "6h": 6 * 3_600_000,
    "8h": 8 * 3_600_000,
    "12h": 12 * 3_600_000,
    "1d": 86_400_000,
    "3d": 3 * 86_400_000,
    "1w": 7 * 86_400_000,
    "1Mo": None,
}

# Our TF name → freqtrade TF name (only differences listed)
OUR_TF_TO_FT: dict[str, str] = {"1Mo": "1M"}

# Freqtrade TF name → our TF name
FT_TF_TO_OURS: dict[str, str] = {v: k for k, v in OUR_TF_TO_FT.items()}


CANONICAL_COLS = ["timestamp", "open", "high", "low", "close", "volume"]


def normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize any OHLCV DataFrame to canonical schema: [timestamp, open, high, low, close, volume].

    Source layouts handled (priority order — first match wins):
      1. Empty DataFrame          → empty canonical DataFrame
      2. Numeric '0'..'5'         → freqtrade legacy (also handles mixed 0+date from bad merges)
      3. 'timestamp' + OHLCV      → canonical or near-canonical (extra 'date' col dropped)
      4. 'Timestamp' + OHLCV      → capitalized variant
      5. 'date' + OHLCV           → freqtrade current output

    Output: timestamp int64 (ms UTC), OHLCV float64, deduped + sorted ascending.
    """
    if df.empty or len(df.columns) == 0:
        return pd.DataFrame(columns=CANONICAL_COLS).astype(
            {"timestamp": "int64", "open": "float64", "high": "float64",
             "low": "float64", "close": "float64", "volume": "float64"}
        )

    cols = df.columns.tolist()
    col_set = set(cols)

    if "0" in col_set:
        # Numeric legacy — col '0' is timestamp_ms; ignore any extra named cols
        result = df[df["0"].notna()][["0", "1", "2", "3", "4", "5"]].copy()
        result.columns = pd.Index(CANONICAL_COLS)
        result["timestamp"] = result["timestamp"].astype("float64").astype("int64")

    elif "timestamp" in col_set:
        # Named schema — drop any extra cols (e.g. redundant 'date')
        result = df[["timestamp", "open", "high", "low", "close", "volume"]].copy()
        if pd.api.types.is_datetime64_any_dtype(result["timestamp"]):
            # datetime64 can be s/ms/us/ns resolution — cast via datetime64[ms]
            # rather than assuming ns and dividing, which silently corrupts
            # non-ns inputs (e.g. pandas' default datetime64[us] from to_datetime).
            result["timestamp"] = result["timestamp"].astype("datetime64[ms]").astype("int64")
        else:
            result["timestamp"] = result["timestamp"].astype("float64").astype("int64")

    elif "Timestamp" in col_set:
        result = df[["Timestamp", "Open", "High", "Low", "Close", "Volume"]].copy()
        result.columns = pd.Index(CANONICAL_COLS)
        result["timestamp"] = result["timestamp"].astype("float64").astype("int64")

    elif "date" in col_set:
        result = df[["date", "open", "high", "low", "close", "volume"]].copy()
        result.rename(columns={"date": "timestamp"}, inplace=True)
        if pd.api.types.is_datetime64_any_dtype(result["timestamp"]):
            result["timestamp"] = result["timestamp"].astype("datetime64[ms]").astype("int64")
        else:
            result["timestamp"] = result["timestamp"].astype("float64").astype("int64")

    elif "Date" in col_set and "Open" in col_set:
        # Yahoo Finance stock schema: Date, Open, High, Low, Close, [Adj Close], Volume
        result = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        result.columns = pd.Index(CANONICAL_COLS)
        if not pd.api.types.is_datetime64_any_dtype(result["timestamp"]):
            result["timestamp"] = pd.to_datetime(result["timestamp"])
        result["timestamp"] = result["timestamp"].astype("datetime64[ms]").astype("int64")

    else:
        raise ValueError(f"Unrecognized OHLCV schema: {cols}")

    result = result.dropna(subset=["timestamp"])
    result["timestamp"] = result["timestamp"].astype("int64")
    for c in ["open", "high", "low", "close", "volume"]:
        result[c] = result[c].astype("float64")
    return (
        result
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )


def ft_timeframe(tf: str) -> str:
    """Convert our timeframe name to freqtrade's naming."""
    return OUR_TF_TO_FT.get(tf, tf)


def our_timeframe(ft_tf: str) -> str:
    """Convert freqtrade timeframe name to ours."""
    return FT_TF_TO_OURS.get(ft_tf, ft_tf)


def catalog_path(exchange: str) -> Path:
    return DATA_ROOT / f"crypto_{exchange}.yaml"


def futures_catalog_path(exchange: str) -> Path:
    return DATA_ROOT / f"crypto_futures_{exchange}.yaml"


def catalog_key(symbol: str, exchange: str) -> str:
    """YAML key for a symbol. Binance uses plain symbol; others append _exchange."""
    if exchange == "binance":
        return symbol
    return f"{symbol}_{exchange}"


def symbol_from_key(key: str, exchange: str) -> str:
    if exchange == "binance":
        return key
    suffix = f"_{exchange}"
    return key[: -len(suffix)] if key.endswith(suffix) else key


def pair_to_symbol(pair: str) -> str:
    """BTC/USDT → BTC_USDT"""
    return pair.replace("/", "_")


def symbol_to_pair(symbol: str) -> str:
    """BTC_USDT → BTC/USDT"""
    return symbol.replace("_", "/", 1)


def futures_pair_to_symbol(pair: str) -> str:
    """BTC/USDT:USDT → BTC_USDT  (strip settlement, canonical storage symbol)."""
    return pair.split(":")[0].replace("/", "_")


def futures_pair_to_ft_symbol(pair: str) -> str:
    """BTC/USDT:USDT → BTC_USDT_USDT  (freqtrade filename in futures/ subdir)."""
    return pair.replace("/", "_").replace(":", "_")


def load_crypto_catalog(exchange: str) -> dict:
    path = catalog_path(exchange)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_crypto_catalog(exchange: str, data: dict) -> None:
    """Atomically overwrite the catalog YAML."""
    path = catalog_path(exchange)
    tmp = path.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    tmp.replace(path)


def load_crypto_futures_catalog(exchange: str) -> dict:
    path = futures_catalog_path(exchange)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_crypto_futures_catalog(exchange: str, data: dict) -> None:
    """Atomically overwrite the futures catalog YAML."""
    path = futures_catalog_path(exchange)
    tmp = path.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    tmp.replace(path)


def read_parquet_range(path: Path) -> tuple[str, str, int] | None:
    """Return (start_date_str, end_date_str, row_count) from any OHLCV parquet file.
    Returns None if file doesn't exist, isn't parquet, or can't be parsed."""
    if not path.exists() or path.suffix != ".parquet":
        return None
    try:
        df = pd.read_parquet(path)
        normed = normalize_ohlcv(df)
        if normed.empty:
            return None
        ts = normed["timestamp"]
        start = datetime.fromtimestamp(ts.iloc[0] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        end = datetime.fromtimestamp(ts.iloc[-1] / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        return start, end, len(normed)
    except Exception:
        return None


def crypto_file_path(exchange: str, symbol: str, timeframe: str) -> Path:
    """Canonical path for a crypto parquet file."""
    return DATA_ROOT / "cryptos" / exchange / symbol / f"{symbol}-{timeframe}.parquet"


def crypto_futures_file_path(exchange: str, symbol: str, timeframe: str) -> Path:
    """Canonical path for a crypto-futures parquet file."""
    return DATA_ROOT / "crypto-futures" / exchange / symbol / f"{symbol}-{timeframe}.parquet"


def update_catalog_entry(
    catalog: dict,
    exchange: str,
    symbol: str,
    timeframe: str,
    file_path: Path,
    start_date: str,
    end_date: str,
    categories: list[str] | None = None,
    common_name: str | None = None,
) -> None:
    """Update a single timeframe entry in a loaded catalog dict (mutates in place)."""
    key = catalog_key(symbol, exchange)
    rel_path = str(file_path.relative_to(REPO_ROOT))

    if key not in catalog:
        catalog[key] = {
            "name": symbol,
            "common_name": common_name or symbol_to_pair(symbol),
            "provider": exchange,
            "categories": categories or ["Crypto"],
            "timeframes": [],
            "timeframe_dates": {},
        }

    entry = catalog[key]

    if timeframe not in entry.get("timeframes", []):
        entry.setdefault("timeframes", []).append(timeframe)
        entry["timeframes"] = sorted(entry["timeframes"])

    entry.setdefault("timeframe_dates", {})[timeframe] = {
        "start_date": start_date,
        "end_date": end_date,
        "file_path": rel_path,
    }

    # Recompute top-level start/end across all timeframes with known dates
    all_starts = [
        v["start_date"]
        for v in entry["timeframe_dates"].values()
        if v.get("start_date")
    ]
    all_ends = [
        v["end_date"]
        for v in entry["timeframe_dates"].values()
        if v.get("end_date")
    ]
    if all_starts:
        entry["start_date"] = min(all_starts)
    if all_ends:
        entry["end_date"] = max(all_ends)


def stock_catalog_path(universe: str) -> Path:
    """Return the YAML catalog path for a stock universe."""
    return DATA_ROOT / f"{universe}.yaml"


def load_stock_catalog(universe: str) -> dict:
    path = stock_catalog_path(universe)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.safe_load(f) or {}


def save_stock_catalog(universe: str, data: dict) -> None:
    """Atomically overwrite a stock-universe catalog YAML."""
    path = stock_catalog_path(universe)
    tmp = path.with_suffix(".yaml.tmp")
    with open(tmp, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    tmp.replace(path)


def stock_file_path(
    provider: str,
    universe: str,
    symbol: str,
    timeframe: str,
    session: str | None = None,
) -> Path:
    """Canonical path for a stock parquet file, for either provider.
    session is IBKR-only (intraday all/overnight filter) — pass None for YF."""
    provider_dir = provider.lower()
    fname = f"{symbol}-{session}-{timeframe}.parquet" if session else f"{symbol}-{timeframe}.parquet"
    return DATA_ROOT / "stocks" / provider_dir / universe / symbol / fname


def update_stock_timeframe_entry(
    catalog: dict,
    symbol: str,
    timeframe: str,
    provider: str,
    file_path: Path,
    start_date: str,
    end_date: str,
    session: str | None = None,
    categories: list[str] | None = None,
    common_name: str | None = None,
    sector: str | None = None,
) -> None:
    """Update a stock catalog entry. timeframe_dates is keyed by (timeframe, provider)
    so multiple providers can register data for the same timeframe without collision."""
    entry = catalog.setdefault(
        symbol,
        {
            "name": symbol,
            "common_name": common_name or symbol,
            "description": f"Historical data for {symbol}",
            "hide_from_frontend": False,
            "categories": categories or ["Stock"],
            "timeframes": [],
            "timeframe_dates": {},
        },
    )
    if sector and not entry.get("sector"):
        entry["sector"] = sector

    rel_path = str(file_path.relative_to(REPO_ROOT))
    if timeframe not in entry.get("timeframes", []):
        entry.setdefault("timeframes", []).append(timeframe)
        entry["timeframes"] = sorted(
            entry["timeframes"],
            key=lambda tf: TF_TO_MS.get(tf) if TF_TO_MS.get(tf) is not None else float("inf"),
        )

    node = {"start_date": start_date, "end_date": end_date, "file_path": rel_path}
    if session:
        node["session"] = session
    entry.setdefault("timeframe_dates", {}).setdefault(timeframe, {})[provider] = node

    all_starts = [
        n["start_date"]
        for tf_node in entry["timeframe_dates"].values()
        for n in tf_node.values()
        if n.get("start_date")
    ]
    all_ends = [
        n["end_date"]
        for tf_node in entry["timeframe_dates"].values()
        for n in tf_node.values()
        if n.get("end_date")
    ]
    if all_starts:
        entry["start_date"] = min(all_starts)
    if all_ends:
        entry["end_date"] = max(all_ends)


def refresh_stock_catalog_dates(universe: str) -> int:
    """Scan parquet files and update start/end dates in a stock catalog.
    Returns count of (timeframe, provider) nodes updated."""
    catalog = load_stock_catalog(universe)
    updated = 0

    for symbol, entry in catalog.items():
        changed = False
        for tf, tf_node in entry.get("timeframe_dates", {}).items():
            for provider, node in tf_node.items():
                fp_str = node.get("file_path", "")
                if not fp_str:
                    continue
                result = read_parquet_range(REPO_ROOT / fp_str)
                if result:
                    start, end, _ = result
                    if node.get("start_date") != start or node.get("end_date") != end:
                        node["start_date"] = start
                        node["end_date"] = end
                        changed = True
        if changed:
            all_starts = [
                n["start_date"]
                for tf_node in entry["timeframe_dates"].values()
                for n in tf_node.values()
                if n.get("start_date")
            ]
            all_ends = [
                n["end_date"]
                for tf_node in entry["timeframe_dates"].values()
                for n in tf_node.values()
                if n.get("end_date")
            ]
            if all_starts:
                entry["start_date"] = min(all_starts)
            if all_ends:
                entry["end_date"] = max(all_ends)
            updated += 1

    if updated:
        save_stock_catalog(universe, catalog)
    return updated


def refresh_catalog_dates(exchange: str, symbols: list[str] | None = None) -> int:
    """Scan parquet files and update start/end dates in the catalog.
    Returns count of entries updated."""
    catalog = load_crypto_catalog(exchange)
    updated = 0
    keys = list(catalog.keys()) if symbols is None else [catalog_key(s, exchange) for s in symbols]

    for key in keys:
        entry = catalog.get(key)
        if not entry:
            continue
        symbol = entry.get("name", symbol_from_key(key, exchange))
        changed = False
        for tf, td in entry.get("timeframe_dates", {}).items():
            fp_str = td.get("file_path", "")
            fp = REPO_ROOT / fp_str if fp_str else crypto_file_path(exchange, symbol, tf)
            result = read_parquet_range(fp)
            if result:
                start, end, _ = result
                if td.get("start_date") != start or td.get("end_date") != end:
                    td["start_date"] = start
                    td["end_date"] = end
                    td["file_path"] = str(fp.relative_to(REPO_ROOT))
                    changed = True
        if changed:
            # recompute top-level dates
            all_starts = [v["start_date"] for v in entry["timeframe_dates"].values() if v.get("start_date")]
            all_ends = [v["end_date"] for v in entry["timeframe_dates"].values() if v.get("end_date")]
            if all_starts:
                entry["start_date"] = min(all_starts)
            if all_ends:
                entry["end_date"] = max(all_ends)
            updated += 1

    if updated:
        save_crypto_catalog(exchange, catalog)
    return updated
