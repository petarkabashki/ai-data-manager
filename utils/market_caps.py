"""CoinGecko historical market-cap helpers and local cache management."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from utils.catalog import DATA_ROOT


MARKET_CAP_ROOT = DATA_ROOT / "market-cap" / "coingecko"
REFERENCE_DIR = MARKET_CAP_ROOT / "reference"
HISTORY_DIR = MARKET_CAP_ROOT / "history"
EXPORT_DIR = MARKET_CAP_ROOT / "exports"

COIN_LIST_CACHE = REFERENCE_DIR / "coins.parquet"

PUBLIC_BASE_URL = "https://api.coingecko.com/api/v3"
PRO_BASE_URL = "https://pro-api.coingecko.com/api/v3"

PUBLIC_HISTORY_LIMIT_DAYS = 365

DEFAULT_STABLECOIN_IDS = frozenset(
    {
        "tether",
        "usd-coin",
        "ethena-usde",
        "dai",
        "binance-usd",
        "paypal-usd",
        "true-usd",
        "first-digital-usd",
        "pax-dollar",
        "frax",
        "usdd",
        "usdb",
        "gemini-dollar",
        "liquity-usd",
        "susds",
        "mountain-protocol-usdm",
        "euro-coin",
        "tether-eurt",
        "stasis-eurs",
    }
)


class CoinGeckoError(RuntimeError):
    """CoinGecko request or data-shape failure."""


@dataclass(frozen=True)
class CoinGeckoConfig:
    base_url: str
    headers: dict[str, str]
    has_full_history: bool
    source_label: str


def ensure_market_cap_dirs() -> None:
    for path in (REFERENCE_DIR, HISTORY_DIR, EXPORT_DIR):
        path.mkdir(parents=True, exist_ok=True)


def coin_history_path(coin_id: str) -> Path:
    return HISTORY_DIR / f"{coin_id}.parquet"


def export_path(stem: str, output_format: str) -> Path:
    ensure_market_cap_dirs()
    return EXPORT_DIR / f"{stem}.{output_format}"


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Expected YYYY-MM-DD date, got {value!r}") from exc


def daterange(start: date, end: date, frequency: str) -> list[date]:
    if end < start:
        raise ValueError("--end must be on or after --start")

    if frequency == "daily":
        step = timedelta(days=1)
        out: list[date] = []
        current = start
        while current <= end:
            out.append(current)
            current += step
        return out

    if frequency == "weekly":
        step = timedelta(days=7)
        out = []
        current = start
        while current <= end:
            out.append(current)
            current += step
        return out

    if frequency == "monthly":
        out = []
        current = start.replace(day=1)
        while current <= end:
            out.append(current)
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
        return [d for d in out if d >= start]

    raise ValueError(f"Unsupported frequency: {frequency}")


def get_coingecko_config() -> CoinGeckoConfig:
    api_key = os.environ.get("COINGECKO_PRO_API_KEY")
    if api_key:
        return CoinGeckoConfig(
            base_url=PRO_BASE_URL,
            headers={"x-cg-pro-api-key": api_key},
            has_full_history=True,
            source_label="coingecko-pro",
        )

    return CoinGeckoConfig(
        base_url=PUBLIC_BASE_URL,
        headers={},
        has_full_history=False,
        source_label="coingecko-public",
    )


def _read_json_response(request: Request) -> Any:
    try:
        with urlopen(request, timeout=60) as response:
            payload = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise CoinGeckoError(f"HTTP {exc.code}: {body}") from exc
    except URLError as exc:
        raise CoinGeckoError(f"Network error: {exc}") from exc

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise CoinGeckoError("CoinGecko returned non-JSON content") from exc

    if isinstance(data, dict) and "error" in data:
        raise CoinGeckoError(str(data["error"]))
    return data


class CoinGeckoClient:
    def __init__(
        self,
        config: CoinGeckoConfig | None = None,
        pause_seconds: float | None = None,
        max_retries: int = 5,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config or get_coingecko_config()
        self.pause_seconds = (
            pause_seconds
            if pause_seconds is not None
            else (1.25 if not self.config.has_full_history else 0.0)
        )
        self.max_retries = max_retries
        self.log = log

    def _request(self, path: str, params: dict[str, Any]) -> Any:
        query = urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{self.config.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        last_error: CoinGeckoError | None = None
        for attempt in range(1, self.max_retries + 1):
            request = Request(url, headers=self.config.headers)
            try:
                data = _read_json_response(request)
                if self.pause_seconds:
                    time.sleep(self.pause_seconds)
                return data
            except CoinGeckoError as exc:
                last_error = exc
                if "HTTP 429:" not in str(exc) or attempt == self.max_retries:
                    raise
                backoff_seconds = min(60.0, self.pause_seconds + (2 ** (attempt - 1)))
                if self.log is not None:
                    self.log(
                        f"  rate limit hit for {path}; retry {attempt}/{self.max_retries} "
                        f"after {backoff_seconds:.1f}s"
                    )
                time.sleep(backoff_seconds)

        assert last_error is not None
        raise last_error

    def fetch_coin_list(self, status: str = "active") -> pd.DataFrame:
        data = self._request(
            "/coins/list",
            {
                "include_platform": "false",
                "status": status,
            },
        )
        if not isinstance(data, list):
            raise CoinGeckoError("Unexpected /coins/list response shape")

        df = pd.DataFrame(data)
        for col in ("id", "symbol", "name"):
            if col not in df.columns:
                raise CoinGeckoError(f"/coins/list missing {col!r}")
        df = df[["id", "symbol", "name"]].copy()
        df["status"] = status
        df["symbol"] = df["symbol"].astype(str).str.lower()
        df["name"] = df["name"].astype(str)
        return df

    def fetch_market_chart_range(
        self,
        coin_id: str,
        start: date,
        end: date,
        vs_currency: str = "usd",
    ) -> pd.DataFrame:
        data = self._request(
            f"/coins/{coin_id}/market_chart/range",
            {
                "vs_currency": vs_currency,
                "from": start.isoformat(),
                "to": end.isoformat(),
                "interval": "daily",
            },
        )
        return market_chart_to_frame(
            coin_id=coin_id,
            vs_currency=vs_currency,
            source=self.config.source_label,
            data=data,
        )


def market_chart_to_frame(
    *,
    coin_id: str,
    vs_currency: str,
    source: str,
    data: dict[str, Any],
) -> pd.DataFrame:
    market_caps = data.get("market_caps", [])
    prices = data.get("prices", [])
    volumes = data.get("total_volumes", [])

    caps_df = pd.DataFrame(market_caps, columns=["timestamp_ms", "market_cap"])
    if caps_df.empty:
        return pd.DataFrame(
            columns=[
                "snapshot_date",
                "coin_id",
                "vs_currency",
                "price",
                "market_cap",
                "total_volume",
                "source",
                "retrieved_at_utc",
            ]
        )

    prices_df = pd.DataFrame(prices, columns=["timestamp_ms", "price"])
    volumes_df = pd.DataFrame(volumes, columns=["timestamp_ms", "total_volume"])

    merged = caps_df.merge(prices_df, on="timestamp_ms", how="left").merge(
        volumes_df, on="timestamp_ms", how="left"
    )
    merged["snapshot_date"] = pd.to_datetime(merged["timestamp_ms"], unit="ms", utc=True).dt.date
    merged["coin_id"] = coin_id
    merged["vs_currency"] = vs_currency
    merged["source"] = source
    merged["retrieved_at_utc"] = datetime.now(tz=timezone.utc)
    merged = merged[
        [
            "snapshot_date",
            "coin_id",
            "vs_currency",
            "price",
            "market_cap",
            "total_volume",
            "source",
            "retrieved_at_utc",
        ]
    ].copy()
    merged = merged.drop_duplicates(subset=["snapshot_date"]).sort_values("snapshot_date").reset_index(drop=True)
    return merged


def load_coin_list_cache() -> pd.DataFrame | None:
    if not COIN_LIST_CACHE.exists():
        return None
    df = pd.read_parquet(COIN_LIST_CACHE)
    expected = {"id", "symbol", "name", "status"}
    if not expected.issubset(df.columns):
        return None
    return df


def refresh_coin_list_cache(client: CoinGeckoClient) -> pd.DataFrame:
    ensure_market_cap_dirs()
    active = client.fetch_coin_list("active")
    frames = [active]
    try:
        inactive = client.fetch_coin_list("inactive")
    except CoinGeckoError:
        inactive = pd.DataFrame(columns=active.columns)
    frames.append(inactive)
    combined = pd.concat(frames, ignore_index=True)
    combined = (
        combined.drop_duplicates(subset=["id"], keep="first")
        .sort_values(["status", "symbol", "id"])
        .reset_index(drop=True)
    )
    combined.to_parquet(COIN_LIST_CACHE, index=False)
    return combined


def get_coin_list(client: CoinGeckoClient, refresh: bool = False) -> pd.DataFrame:
    cached = None if refresh else load_coin_list_cache()
    if cached is not None:
        return cached
    return refresh_coin_list_cache(client)


def resolve_coin_ids(
    coin_list: pd.DataFrame,
    *,
    coin_ids: Sequence[str] | None = None,
    symbols: Sequence[str] | None = None,
    all_active: bool = False,
    all_coins: bool = False,
) -> pd.DataFrame:
    if all_coins:
        if (coin_list["status"] == "inactive").sum() == 0:
            raise ValueError(
                "--all-coins requires inactive CoinGecko ids, but the cached/public coin list "
                "does not include them. Set COINGECKO_PRO_API_KEY and rerun with --refresh-coin-list."
            )
        resolved = coin_list.copy()
    elif all_active:
        resolved = coin_list[coin_list["status"] == "active"].copy()
    else:
        frames: list[pd.DataFrame] = []

        if coin_ids:
            wanted = {value.strip() for value in coin_ids if value.strip()}
            frame = coin_list[coin_list["id"].isin(wanted)].copy()
            missing = sorted(wanted - set(frame["id"]))
            if missing:
                raise ValueError(f"Unknown CoinGecko ids: {', '.join(missing)}")
            frames.append(frame)

        if symbols:
            norm_symbols = [value.strip().lower() for value in symbols if value.strip()]
            for symbol in norm_symbols:
                matches = coin_list[coin_list["symbol"] == symbol].copy()
                if matches.empty:
                    raise ValueError(f"No CoinGecko symbol match for {symbol!r}")
                if len(matches) > 1:
                    preview = ", ".join(matches["id"].head(8))
                    raise ValueError(
                        f"Symbol {symbol!r} is ambiguous on CoinGecko. Use --coin-ids instead. "
                        f"Matches include: {preview}"
                    )
                frames.append(matches)

        if not frames:
            raise ValueError(
                "Specify --coin-ids, --symbols, --all-active, or --all-coins"
            )

        resolved = pd.concat(frames, ignore_index=True)

    resolved = resolved.drop_duplicates(subset=["id"]).sort_values("id").reset_index(drop=True)
    return resolved[["id", "symbol", "name", "status"]]


def load_coin_history(coin_id: str) -> pd.DataFrame | None:
    path = coin_history_path(coin_id)
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if "snapshot_date" not in df.columns:
        return None
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    return df.sort_values("snapshot_date").reset_index(drop=True)


def save_coin_history(coin_id: str, df: pd.DataFrame) -> None:
    ensure_market_cap_dirs()
    path = coin_history_path(coin_id)
    df.to_parquet(path, index=False)


def merge_histories(existing: pd.DataFrame | None, new_data: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        merged = new_data.copy()
    else:
        merged = pd.concat([existing, new_data], ignore_index=True)
    merged["snapshot_date"] = pd.to_datetime(merged["snapshot_date"]).dt.date
    merged = merged.drop_duplicates(subset=["snapshot_date"], keep="last").sort_values("snapshot_date").reset_index(drop=True)
    return merged


def history_covers_range(df: pd.DataFrame | None, start: date, end: date) -> bool:
    if df is None or df.empty:
        return False
    min_date = min(df["snapshot_date"])
    max_date = max(df["snapshot_date"])
    return min_date <= start and max_date >= end


def fetch_or_load_history(
    client: CoinGeckoClient,
    *,
    coin_id: str,
    start: date,
    end: date,
    vs_currency: str = "usd",
    refresh: bool = False,
) -> tuple[pd.DataFrame, str]:
    cached = None if refresh else load_coin_history(coin_id)
    if not refresh and history_covers_range(cached, start, end):
        assert cached is not None
        sliced = cached[(cached["snapshot_date"] >= start) & (cached["snapshot_date"] <= end)].copy()
        return sliced.reset_index(drop=True), "cache"

    fetched = client.fetch_market_chart_range(coin_id, start, end, vs_currency=vs_currency)
    merged = merge_histories(cached, fetched)
    save_coin_history(coin_id, merged)
    sliced = merged[(merged["snapshot_date"] >= start) & (merged["snapshot_date"] <= end)].copy()
    return sliced.reset_index(drop=True), "api"


def validate_history_window(config: CoinGeckoConfig, start: date, end: date) -> None:
    if config.has_full_history:
        return

    today = datetime.now(tz=timezone.utc).date()
    oldest_allowed = today - timedelta(days=PUBLIC_HISTORY_LIMIT_DAYS)
    if start < oldest_allowed or end < oldest_allowed:
        raise ValueError(
            "CoinGecko public API historical chart requests are currently limited to the "
            f"most recent {PUBLIC_HISTORY_LIMIT_DAYS} days. "
            f"Requested window: {start} to {end}; oldest allowed today: {oldest_allowed}. "
            "Set COINGECKO_PRO_API_KEY for deeper history."
        )


def read_lines(path: str | Path) -> list[str]:
    with open(path) as handle:
        return [line.strip() for line in handle if line.strip()]


def combine_histories(
    histories: Iterable[pd.DataFrame],
    resolved: pd.DataFrame,
) -> pd.DataFrame:
    meta = resolved.rename(columns={"id": "coin_id"})
    combined = pd.concat(list(histories), ignore_index=True)
    if combined.empty:
        return combined
    combined = combined.merge(meta, on="coin_id", how="left")
    combined = combined[
        [
            "snapshot_date",
            "coin_id",
            "symbol",
            "name",
            "status",
            "vs_currency",
            "price",
            "market_cap",
            "total_volume",
            "source",
            "retrieved_at_utc",
        ]
    ].copy()
    combined = combined.sort_values(["snapshot_date", "market_cap"], ascending=[True, False]).reset_index(drop=True)
    return combined


def snapshot_for_dates(history_df: pd.DataFrame, dates: Sequence[date]) -> pd.DataFrame:
    wanted = set(dates)
    snapshot = history_df[history_df["snapshot_date"].isin(wanted)].copy()
    if snapshot.empty:
        return snapshot
    snapshot = snapshot.sort_values(["snapshot_date", "market_cap"], ascending=[True, False]).reset_index(drop=True)
    return snapshot


def apply_exclusions(
    snapshot_df: pd.DataFrame,
    *,
    exclude_ids: Sequence[str] | None = None,
    exclude_symbols: Sequence[str] | None = None,
    exclude_stablecoins: bool = False,
) -> pd.DataFrame:
    filtered = snapshot_df.copy()
    if exclude_ids:
        excluded = {value.strip() for value in exclude_ids if value.strip()}
        filtered = filtered[~filtered["coin_id"].isin(excluded)]
    if exclude_symbols:
        excluded = {value.strip().lower() for value in exclude_symbols if value.strip()}
        filtered = filtered[~filtered["symbol"].str.lower().isin(excluded)]
    if exclude_stablecoins:
        filtered = filtered[~filtered["coin_id"].isin(DEFAULT_STABLECOIN_IDS)]
    return filtered.reset_index(drop=True)


def rank_top_n(snapshot_df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    ranked = snapshot_df.copy()
    ranked["rank"] = ranked.groupby("snapshot_date")["market_cap"].rank(method="first", ascending=False).astype("int64")
    ranked = ranked[ranked["rank"] <= top_n].copy()
    ranked = ranked.sort_values(["snapshot_date", "rank"]).reset_index(drop=True)
    cols = ["snapshot_date", "rank"] + [col for col in ranked.columns if col not in {"snapshot_date", "rank"}]
    return ranked[cols]
