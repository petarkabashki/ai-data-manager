"""Catalog management — YAML metadata CRUD, canonical schema, parquet IO.

This module owns three things nothing else may re-implement:

  * the canonical OHLCV schema and `normalize_ohlcv()`, the single point every
    byte of price data passes through before it is written;
  * atomic parquet IO (`write_parquet_atomic`, `merge_ohlcv_parquet`);
  * the catalog YAML read/write helpers and path builders.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from utils.semantics import (
    Semantics,
    apply_semantics,
    asset_class_from_categories,
    semantics_for,
)
from utils.config import (  # noqa: F401  (DATA_ROOT/REPO_ROOT/FREQTRADE_BIN re-exported)
    DATA_ROOT,
    FREQTRADE_BIN,
    REPO_ROOT,
    relative_to_repo,
    resolve_from_repo,
)

CRYPTO_EXCHANGES = ["binance", "bitget", "bybit", "kucoin"]

#: libyaml's C loader when available: same safe semantics as `yaml.safe_load`,
#: roughly 10x faster — loading every catalog drops from ~11s to ~1s, which
#: matters to the PDM server that reloads them after each job.
_YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def load_yaml(path: Path) -> dict:
    """Load one catalog YAML file; a missing or empty file is `{}`."""
    path = Path(path)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.load(f, Loader=_YAML_LOADER) or {}

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

CANONICAL_DTYPES: dict[str, str] = {
    "timestamp": "int64",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
}

#: The same canonical schema expressed as Arrow type names, for checking a
#: parquet file's schema without reading it. `float64` is `double` in Arrow.
#: Derived here so a check can never drift from the dtypes it checks against.
CANONICAL_ARROW_TYPES: dict[str, str] = {
    name: "double" if dtype == "float64" else dtype
    for name, dtype in CANONICAL_DTYPES.items()
}


def empty_ohlcv_frame() -> pd.DataFrame:
    """A zero-row DataFrame with the canonical columns *and* dtypes.

    Use this instead of `pd.DataFrame(columns=CANONICAL_COLS)`, which produces
    object-typed columns that silently break dtype-sensitive callers.
    """
    return pd.DataFrame(columns=CANONICAL_COLS).astype(CANONICAL_DTYPES)


def write_parquet_atomic(df: pd.DataFrame, dst: Path, compression: str = "snappy") -> None:
    """Write `df` to `dst` atomically: sibling temp file, then rename.

    A merge reads the destination's existing rows into memory before writing.
    Writing straight onto `dst` therefore means a crash mid-write destroys the
    only copy of that data. `Path.replace()` is atomic within a filesystem, so
    the destination is either the old file or the complete new one.
    """
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        df.to_parquet(tmp, index=False, compression=compression)
        tmp.replace(dst)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def merge_ohlcv_parquet(src: pd.DataFrame | Path | str, dst: Path) -> tuple[int, int]:
    """Merge OHLCV data into `dst`, returning (rows_before, rows_after).

    Both sides are normalized to the canonical schema first, concatenated,
    deduped on `timestamp` (new data wins) and sorted ascending. The write is
    atomic. This is the only merge implementation in the repo — crypto,
    futures, IBKR and the legacy JSON converter all route through it.
    """
    new_df = normalize_ohlcv(src if isinstance(src, pd.DataFrame) else pd.read_parquet(src))

    dst = Path(dst)
    if dst.exists():
        old_df = normalize_ohlcv(pd.read_parquet(dst))
        old_rows = len(old_df)
        merged = (
            pd.concat([old_df, new_df])
            .drop_duplicates(subset=["timestamp"], keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
    else:
        old_rows = 0
        merged = new_df

    write_parquet_atomic(merged, dst)
    return old_rows, len(merged)


def _datetime_to_ms_int64(s: pd.Series) -> pd.Series:
    """datetime64 Series (tz-aware or naive, any resolution) -> int64 ms since epoch."""
    if isinstance(s.dtype, pd.DatetimeTZDtype):
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s.astype("datetime64[ms]").astype("int64")


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
        return empty_ohlcv_frame()

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
            # datetime64 can be s/ms/us/ns resolution and tz-aware or naive —
            # normalize via _datetime_to_ms_int64 rather than assuming ns and
            # dividing, which silently corrupts non-ns inputs (e.g. pandas'
            # default datetime64[us] from to_datetime).
            result["timestamp"] = _datetime_to_ms_int64(result["timestamp"])
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
            result["timestamp"] = _datetime_to_ms_int64(result["timestamp"])
        else:
            result["timestamp"] = result["timestamp"].astype("float64").astype("int64")

    elif "Date" in col_set and "Open" in col_set:
        # Yahoo Finance stock schema: Date, Open, High, Low, Close, [Adj Close], Volume
        result = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
        result.columns = pd.Index(CANONICAL_COLS)
        if not pd.api.types.is_datetime64_any_dtype(result["timestamp"]):
            result["timestamp"] = pd.to_datetime(result["timestamp"])
        result["timestamp"] = _datetime_to_ms_int64(result["timestamp"])

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


def _dump_yaml(path: Path, data: dict) -> None:
    """Atomically overwrite a catalog YAML: temp sibling, then rename.

    Every catalog saver routes through here so durability is one decision
    rather than four near-identical copies.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        with open(tmp, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


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
        return yaml.load(f, Loader=_YAML_LOADER) or {}


def save_crypto_catalog(exchange: str, data: dict) -> None:
    _dump_yaml(catalog_path(exchange), data)


def load_crypto_futures_catalog(exchange: str) -> dict:
    path = futures_catalog_path(exchange)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.load(f, Loader=_YAML_LOADER) or {}


def save_crypto_futures_catalog(exchange: str, data: dict) -> None:
    _dump_yaml(futures_catalog_path(exchange), data)


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


def provider_node_key(provider: str, session: str | None = None) -> str:
    """Catalog key for a provider's node within `timeframe_dates[tf]`.

    `session` is part of the *filename* for IBKR intraday data, so it must also
    be part of the catalog key — otherwise downloading `--session all` and then
    `--session overnight` for one (symbol, timeframe) leaves two parquet files
    on disk but only one catalog node, and the first file is orphaned.

    The default session maps to the bare provider name so existing catalogs
    (which stored `IBKR` with `session: all`) keep resolving unchanged.
    """
    if session in (None, "", "all"):
        return provider
    return f"{provider}:{session}"


def _iter_provider_nodes(timeframe_dates: dict) -> list[dict]:
    """Every leaf node in a `timeframe_dates` dict, flat or nested-by-provider."""
    nodes: list[dict] = []
    for tf_node in timeframe_dates.values():
        if not isinstance(tf_node, dict):
            continue
        if "file_path" in tf_node or "start_date" in tf_node:
            nodes.append(tf_node)  # flat shape
        else:
            nodes.extend(n for n in tf_node.values() if isinstance(n, dict))
    return nodes


def iter_timeframe_nodes(entry: dict) -> list[tuple[str, str, dict]]:
    """Every (timeframe, node_key, node) in an entry, whichever shape it uses.

    A flat node reports a node_key of "" so callers can tell the shapes apart
    without re-deriving the test.
    """
    out: list[tuple[str, str, dict]] = []
    for timeframe, tf_node in entry.get("timeframe_dates", {}).items():
        if not isinstance(tf_node, dict):
            continue
        if "file_path" in tf_node or "start_date" in tf_node:
            out.append((timeframe, "", tf_node))
        else:
            out.extend(
                (timeframe, key, node)
                for key, node in tf_node.items()
                if isinstance(node, dict)
            )
    return out


def recompute_entry_bounds(entry: dict) -> None:
    """Recompute an entry's top-level start_date/end_date across every node.

    Handles both catalog shapes, so callers don't need to know which one this
    asset class uses.
    """
    nodes = _iter_provider_nodes(entry.get("timeframe_dates", {}))
    starts = [n["start_date"] for n in nodes if n.get("start_date")]
    ends = [n["end_date"] for n in nodes if n.get("end_date")]
    if starts:
        entry["start_date"] = min(starts)
    if ends:
        entry["end_date"] = max(ends)


def _sort_timeframes(timeframes: list[str]) -> list[str]:
    return sorted(
        timeframes,
        key=lambda tf: TF_TO_MS.get(tf) if TF_TO_MS.get(tf) is not None else float("inf"),
    )


def _register_timeframe(entry: dict, timeframe: str) -> None:
    if timeframe not in entry.get("timeframes", []):
        entry.setdefault("timeframes", []).append(timeframe)
        entry["timeframes"] = _sort_timeframes(entry["timeframes"])


def _set_provider_node(
    entry: dict,
    timeframe: str,
    node_key: str,
    node: dict,
) -> None:
    """Write a provider node under `timeframe_dates[tf]`, nesting if needed.

    If the timeframe currently holds a *flat* node (an older catalog written
    before this class nested by provider), it is migrated in place: the
    existing node is re-filed under its own provider name, then the new node is
    added alongside it. This is what stops an IBKR write from silently
    replacing YF's registration for the same timeframe.
    """
    tf_map = entry.setdefault("timeframe_dates", {})
    existing = tf_map.get(timeframe)

    if isinstance(existing, dict) and ("file_path" in existing or "start_date" in existing):
        legacy_key = provider_node_key(
            existing.get("provider") or entry.get("provider") or "UNKNOWN",
            existing.get("session"),
        )
        tf_map[timeframe] = {legacy_key: existing}

    tf_map.setdefault(timeframe, {})[node_key] = node


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
    semantics: Semantics | None = None,
) -> None:
    """Update a single timeframe entry in a loaded catalog dict (mutates in place).

    Crypto and crypto-futures keep the flat shape: exactly one provider per
    (symbol, timeframe), because the exchange is already part of the catalog
    key and of the path.
    """
    key = catalog_key(symbol, exchange)
    rel_path = relative_to_repo(file_path)

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
    _register_timeframe(entry, timeframe)

    node = {
        "start_date": start_date,
        "end_date": end_date,
        "file_path": rel_path,
    }
    asset_class = asset_class_from_categories(entry.get("categories")) or "Crypto"
    apply_semantics(node, semantics or semantics_for(asset_class, exchange))

    entry.setdefault("timeframe_dates", {})[timeframe] = node
    recompute_entry_bounds(entry)


def stock_catalog_path(universe: str) -> Path:
    """Return the YAML catalog path for a stock universe."""
    return DATA_ROOT / f"{universe}.yaml"


def load_stock_catalog(universe: str) -> dict:
    path = stock_catalog_path(universe)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.load(f, Loader=_YAML_LOADER) or {}


def save_stock_catalog(universe: str, data: dict) -> None:
    _dump_yaml(stock_catalog_path(universe), data)


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
    what_to_show: str | None = None,
    semantics: Semantics | None = None,
) -> None:
    """Update a stock catalog entry.

    `timeframe_dates[tf]` is keyed by `(provider, session)` — see
    `provider_node_key()` — so neither another provider's registration nor the
    same provider's other session can be clobbered.
    """
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

    _register_timeframe(entry, timeframe)

    node = {
        "start_date": start_date,
        "end_date": end_date,
        "file_path": relative_to_repo(file_path),
        "provider": provider,
    }
    if session:
        node["session"] = session
    apply_semantics(node, semantics or semantics_for("Stock", provider, what_to_show))

    _set_provider_node(entry, timeframe, provider_node_key(provider, session), node)
    recompute_entry_bounds(entry)


def index_catalog_path() -> Path:
    return DATA_ROOT / "indexes.yaml"


def load_index_catalog() -> dict:
    path = index_catalog_path()
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.load(f, Loader=_YAML_LOADER) or {}


def save_index_catalog(data: dict) -> None:
    _dump_yaml(index_catalog_path(), data)


def index_file_path(provider: str, symbol: str, timeframe: str) -> Path:
    """Canonical path for an index parquet file."""
    return DATA_ROOT / "indexes" / provider.lower() / symbol / f"{symbol}-{timeframe}.parquet"


def update_index_timeframe_entry(
    catalog: dict,
    symbol: str,
    timeframe: str,
    provider: str,
    file_path: Path,
    start_date: str,
    end_date: str,
    session: str | None = None,
    common_name: str | None = None,
    what_to_show: str | None = None,
    semantics: Semantics | None = None,
) -> None:
    """Update an index entry's node for one (timeframe, provider).

    Indexes nest by provider, exactly like stocks. They used to be flat on the
    assumption that only one provider ever registers a given (symbol,
    timeframe) — but `download_ibkr_index.py` accepts `--timeframes 1d`, and
    YF already owns daily bars for every index, so a flat node meant an IBKR
    daily download silently replaced YF's registration and orphaned its
    parquet file.

    A pre-existing flat node is migrated in place on first write, so older
    `indexes.yaml` files keep working and upgrade lazily.
    """
    entry = catalog.setdefault(
        symbol,
        {
            "name": symbol,
            "common_name": common_name or symbol,
            "description": f"Historical data for {symbol}",
            "hide_from_frontend": False,
            "provider": provider,
            "categories": ["Index"],
            "timeframes": [],
            "timeframe_dates": {},
        },
    )

    _register_timeframe(entry, timeframe)

    node = {
        "start_date": start_date,
        "end_date": end_date,
        "file_path": relative_to_repo(file_path),
        "provider": provider,
    }
    if session:
        node["session"] = session
    apply_semantics(node, semantics or semantics_for("Index", provider, what_to_show))

    _set_provider_node(entry, timeframe, provider_node_key(provider, session), node)
    recompute_entry_bounds(entry)


def simple_catalog_path(name: str) -> Path:
    """Path for a single-file catalog that isn't per-exchange or per-universe."""
    return DATA_ROOT / f"{name}.yaml"


def load_simple_catalog(name: str) -> dict:
    """Load `commodities.yaml` / `macro.yaml` — the two catalogs that had no CRUD."""
    path = simple_catalog_path(name)
    if not path.exists():
        return {}
    with open(path) as f:
        return yaml.load(f, Loader=_YAML_LOADER) or {}


def save_simple_catalog(name: str, data: dict) -> None:
    _dump_yaml(simple_catalog_path(name), data)


def commodity_file_path(provider: str, symbol: str, timeframe: str) -> Path:
    return DATA_ROOT / "commodities" / provider.lower() / symbol / f"{symbol}-{timeframe}.parquet"


def macro_file_path(provider: str, symbol: str, timeframe: str) -> Path:
    return DATA_ROOT / "macro" / provider.lower() / symbol / f"{symbol}-{timeframe}.parquet"


# --- rate series (macro) -------------------------------------------------------
# `schema: rate_series` files are not OHLCV: one `Value` column on a naive
# `DatetimeIndex` named `Date`. They never go through normalize_ohlcv(); these
# helpers read and merge them while keeping that layout byte-for-byte in kind.

def read_rate_series(path: Path) -> pd.Series:
    """The `Value` series of a rate file (empty if the file is absent)."""
    path = Path(path)
    if not path.exists():
        return pd.Series(dtype="float64", index=pd.DatetimeIndex([], name="Date"), name="Value")
    df = pd.read_parquet(path)
    if "Value" not in df.columns or not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError(f"{path.name} is not a rate series (Value on a Date index)")
    return df["Value"].sort_index()


def merge_rate_series(new: pd.Series, dst: Path) -> tuple[int, int]:
    """Merge dated values into a rate file, returning (rows_before, rows_after).

    The union of both sides, new values winning on a shared date, written
    atomically in the stored layout. Dates the new data lacks are kept: Yahoo
    has dropped a few historical days that the file still holds.
    """
    old = read_rate_series(dst)
    new = new.astype("float64")
    new.index = pd.DatetimeIndex(new.index, name="Date")
    merged = pd.concat([old, new])
    merged = merged[~merged.index.duplicated(keep="last")].sort_index()
    frame = merged.rename("Value").to_frame()
    frame.index.name = "Date"
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        frame.to_parquet(tmp, index=True)
        tmp.replace(dst)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return len(old), len(merged)


def update_simple_timeframe_entry(
    catalog: dict,
    symbol: str,
    timeframe: str,
    provider: str,
    file_path: Path,
    start_date: str,
    end_date: str,
    asset_class: str,
    common_name: str | None = None,
    semantics: Semantics | None = None,
) -> None:
    """Write a flat `timeframe_dates[tf]` node for a commodity or macro entry.

    These classes have exactly one provider per (symbol, timeframe) and — for
    macro — aren't OHLCV at all, which is why the node carries `schema`.
    """
    entry = catalog.setdefault(
        symbol,
        {
            "name": symbol,
            "common_name": common_name or symbol,
            "description": f"Historical data for {symbol}",
            "provider": provider,
            "categories": [asset_class],
            "timeframes": [],
            "timeframe_dates": {},
        },
    )
    _register_timeframe(entry, timeframe)

    node = {
        "start_date": start_date,
        "end_date": end_date,
        "file_path": relative_to_repo(file_path),
        "provider": provider,
    }
    apply_semantics(node, semantics or semantics_for(asset_class, provider))

    entry.setdefault("timeframe_dates", {})[timeframe] = node
    recompute_entry_bounds(entry)


@dataclass(frozen=True, slots=True)
class CatalogSpec:
    """How to load, save and interpret one catalog file."""

    name: str
    asset_class: str
    nested: bool
    load: Callable[[], dict]
    save: Callable[[dict], None]
    #: Canonical path builder, for checking the provider-first rule.
    path_for: Callable[..., Path] | None = None


STOCK_UNIVERSES = ["sp500", "nasdaq100", "russell2000", "delisted_stocks"]
CRYPTO_FUTURES_EXCHANGES = ["binance", "bybit", "kucoin"]


def catalog_registry() -> list[CatalogSpec]:
    """Every catalog in the repo, with its shape and asset class.

    One place that knows the full set, so audits and refreshes stop hardcoding
    partial lists (`refresh_catalog_dates` covered crypto spot only).
    """
    specs: list[CatalogSpec] = []

    for exchange in CRYPTO_EXCHANGES:
        specs.append(
            CatalogSpec(
                name=f"crypto_{exchange}",
                asset_class="Crypto",
                nested=False,
                load=lambda ex=exchange: load_crypto_catalog(ex),
                save=lambda data, ex=exchange: save_crypto_catalog(ex, data),
            )
        )

    for exchange in CRYPTO_FUTURES_EXCHANGES:
        specs.append(
            CatalogSpec(
                name=f"crypto_futures_{exchange}",
                asset_class="Crypto Futures",
                nested=False,
                load=lambda ex=exchange: load_crypto_futures_catalog(ex),
                save=lambda data, ex=exchange: save_crypto_futures_catalog(ex, data),
            )
        )

    for universe in STOCK_UNIVERSES:
        specs.append(
            CatalogSpec(
                name=universe,
                asset_class="Stock",
                nested=True,
                load=lambda u=universe: load_stock_catalog(u),
                save=lambda data, u=universe: save_stock_catalog(u, data),
            )
        )

    specs.append(
        CatalogSpec("indexes", "Index", True, load_index_catalog, save_index_catalog)
    )
    specs.append(
        CatalogSpec(
            "commodities", "Commodity", False,
            lambda: load_simple_catalog("commodities"),
            lambda data: save_simple_catalog("commodities", data),
        )
    )
    specs.append(
        CatalogSpec(
            "macro", "Macro", False,
            lambda: load_simple_catalog("macro"),
            lambda data: save_simple_catalog("macro", data),
        )
    )

    # Imported lazily: both modules import back from this one, and both
    # catalogs are legitimately absent until something has been downloaded.
    from utils.futures import load_futures_catalog, save_futures_catalog
    from utils.options import load_options_catalog, save_options_catalog

    specs.append(
        CatalogSpec("futures", "Futures", True, load_futures_catalog, save_futures_catalog)
    )
    specs.append(
        CatalogSpec(
            "options", "Crypto Options", True,
            load_options_catalog, save_options_catalog,
        )
    )
    return specs


def refresh_entry_dates(entry: dict) -> bool:
    """Rescan every node's parquet file and update its dates. Returns True if changed.

    Shape-agnostic: works for flat (crypto, commodities, macro) and
    nested-by-provider (stocks, indexes) catalogs alike.
    """
    changed = False
    for node in _iter_provider_nodes(entry.get("timeframe_dates", {})):
        fp_str = node.get("file_path", "")
        if not fp_str:
            continue
        result = read_parquet_range(resolve_from_repo(fp_str))
        if not result:
            continue
        start, end, _ = result
        if node.get("start_date") != start or node.get("end_date") != end:
            node["start_date"] = start
            node["end_date"] = end
            changed = True
    if changed:
        recompute_entry_bounds(entry)
    return changed


def refresh_stock_catalog_dates(universe: str) -> int:
    """Scan parquet files and update start/end dates in a stock catalog.
    Returns count of entries updated."""
    catalog = load_stock_catalog(universe)
    updated = sum(1 for entry in catalog.values() if refresh_entry_dates(entry))
    if updated:
        save_stock_catalog(universe, catalog)
    return updated


def refresh_index_catalog_dates() -> int:
    """Scan parquet files and update start/end dates in indexes.yaml."""
    catalog = load_index_catalog()
    updated = sum(1 for entry in catalog.values() if refresh_entry_dates(entry))
    if updated:
        save_index_catalog(catalog)
    return updated


def refresh_futures_catalog_dates(exchange: str) -> int:
    """Scan parquet files and update start/end dates in a crypto-futures catalog."""
    catalog = load_crypto_futures_catalog(exchange)
    updated = sum(1 for entry in catalog.values() if refresh_entry_dates(entry))
    if updated:
        save_crypto_futures_catalog(exchange, catalog)
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
            # Legacy catalogs can record a .json path; fall back to the
            # canonical parquet location for this (exchange, symbol, tf).
            fp = resolve_from_repo(fp_str) if fp_str else crypto_file_path(exchange, symbol, tf)
            result = read_parquet_range(fp)
            if result:
                start, end, _ = result
                if td.get("start_date") != start or td.get("end_date") != end:
                    td["start_date"] = start
                    td["end_date"] = end
                    td["file_path"] = relative_to_repo(fp)
                    changed = True
        if changed:
            recompute_entry_bounds(entry)
            updated += 1

    if updated:
        save_crypto_catalog(exchange, catalog)
    return updated
