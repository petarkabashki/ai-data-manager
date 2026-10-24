"""IBKR historical-bar helpers for intraday stock downloads."""

from __future__ import annotations

import threading
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import count
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from utils.catalog import (
    empty_ohlcv_frame,
    merge_ohlcv_parquet,
    normalize_ohlcv,
)
from utils.planning import (  # noqa: F401  (re-exported for existing callers)
    DownloadRange,
    daterange_chunks,
    dt_from_ms,
    duration_days_or_years,
    format_ranges,
    merge_ranges,
    parse_date,
    plan_download_ranges,
    read_timestamp_bounds,
    throttle_sleep,
    utc_now,
)

NY_TZ = ZoneInfo("America/New_York")
RTH_OPEN_HOUR = 9
RTH_OPEN_MINUTE = 30
RTH_CLOSE_HOUR = 16
INFO_ERROR_CODES = {
    2104,
    2106,
    2107,
    2108,
    2158,
}

OUR_TF_TO_IBKR_BAR_SIZE = {
    "1h": "1 hour",
    "4h": "4 hours",
    "8h": "8 hours",
    "1d": "1 day",
    "1w": "1 week",
}

# Our index symbol -> (IBKR IND contract symbol, exchange). currency is always USD.
# Covers the US indices in indexes.yaml; non-US indices (STOXX50E, FTSE, GDAXI,
# N225, HSI) trade on different venues/currencies and aren't mapped here.
INDEX_CONTRACT_MAP: dict[str, tuple[str, str]] = {
    "GSPC": ("SPX", "CBOE"),
    "DJI": ("INDU", "CME"),
    "IXIC": ("COMP", "NASDAQ"),
    "NDX": ("NDX", "NASDAQ"),
    "RUT": ("RUT", "RUSSELL"),
    "VIX": ("VIX", "CBOE"),
}


def _load_ibapi() -> tuple[Any, Any, Any]:
    try:
        from ibapi.client import EClient
        from ibapi.contract import Contract
        from ibapi.wrapper import EWrapper
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "ibapi is not installed. Run `uv sync` or `uv add ibapi` before using IBKR tools."
        ) from exc
    return EClient, Contract, EWrapper


@dataclass(slots=True)
class HistoricalRequest:
    contract: Any
    end: datetime
    duration: str
    bar_size: str
    what_to_show: str = "TRADES"
    use_rth: bool = False


def ibkr_bar_size(timeframe: str) -> str:
    try:
        return OUR_TF_TO_IBKR_BAR_SIZE[timeframe]
    except KeyError as exc:
        raise ValueError(f"Unsupported IBKR bar size timeframe: {timeframe}") from exc


class IbkrHistoricalClient:
    """Minimal synchronous wrapper over the official IBKR API."""

    def __init__(self, host: str, port: int, client_id: int, timeout: float = 60.0):
        EClient, _, EWrapper = _load_ibapi()

        class _App(EWrapper, EClient):  # type: ignore[misc]
            def __init__(self, outer: "IbkrHistoricalClient"):
                EClient.__init__(self, self)
                self._outer = outer

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                self._outer._next_valid_id = max(self._outer._next_valid_id, orderId)
                self._outer._connected.set()

            def error(  # noqa: N802
                self,
                reqId: int,
                errorCode: int,
                errorString: str,
                advancedOrderRejectJson: str = "",
            ) -> None:
                self._outer._on_error(reqId, errorCode, errorString)

            def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
                self._outer._contract_details[reqId].append(contractDetails)

            def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
                self._outer._set_event(reqId)

            def historicalData(self, reqId: int, bar: Any) -> None:  # noqa: N802
                self._outer._historical_bars[reqId].append(bar)

            def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:  # noqa: N802
                self._outer._set_event(reqId)

        self.host = host
        self.port = port
        self.client_id = client_id
        self.timeout = timeout
        self._app = _App(self)
        self._connected = threading.Event()
        self._events: dict[int, threading.Event] = {}
        self._errors: dict[int, list[tuple[int, str]]] = defaultdict(list)
        self._contract_details: dict[int, list[Any]] = defaultdict(list)
        self._historical_bars: dict[int, list[Any]] = defaultdict(list)
        self._next_valid_id = 1
        self._counter = count(10_000)
        self._thread: threading.Thread | None = None

    def connect(self) -> None:
        self._app.connect(self.host, self.port, clientId=self.client_id)
        self._thread = threading.Thread(target=self._app.run, daemon=True)
        self._thread.start()
        if not self._connected.wait(timeout=self.timeout):
            raise TimeoutError(
                f"Timed out connecting to IBKR at {self.host}:{self.port} with client_id={self.client_id}"
            )

    def disconnect(self) -> None:
        if self._app.isConnected():
            self._app.disconnect()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def __enter__(self) -> "IbkrHistoricalClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    def _new_req_id(self) -> int:
        return max(next(self._counter), self._next_valid_id + 1)

    def _set_event(self, req_id: int) -> None:
        event = self._events.get(req_id)
        if event:
            event.set()

    def _wait(self, req_id: int) -> None:
        event = self._events[req_id]
        if not event.wait(timeout=self.timeout):
            raise TimeoutError(f"IBKR request {req_id} timed out")
        errors = self._errors.get(req_id, [])
        hard_errors = [msg for code, msg in errors if code not in INFO_ERROR_CODES]
        if hard_errors:
            raise RuntimeError(f"IBKR request {req_id} failed: {hard_errors[-1]}")

    def _on_error(self, req_id: int, error_code: int, error_string: str) -> None:
        self._errors[req_id].append((error_code, error_string))
        if error_code not in INFO_ERROR_CODES and req_id in self._events:
            self._events[req_id].set()

    def resolve_us_stock_contract(self, symbol: str) -> Any:
        _, Contract, _ = _load_ibapi()
        candidates = []
        for candidate in [symbol, symbol.replace(".", " "), symbol.replace("-", " ")]:
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        # Why each candidate failed, so the caller can tell a delisting from a
        # flaky connection. Both used to surface as "could not resolve", which
        # sent a transient timeout to the same place as a dead ticker.
        reasons: list[str] = []

        for candidate in candidates:
            contract = Contract()
            contract.symbol = candidate
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"

            req_id = self._new_req_id()
            self._events[req_id] = threading.Event()
            self._contract_details.pop(req_id, None)
            self._app.reqContractDetails(req_id, contract)
            try:
                self._wait(req_id)
            except TimeoutError as exc:
                reasons.append(f"{candidate}: timed out ({exc})")
                continue
            except RuntimeError as exc:
                reasons.append(f"{candidate}: {exc}")
                continue

            details = self._contract_details.get(req_id, [])
            if details:
                preferred = sorted(
                    details,
                    key=lambda d: (
                        0
                        if getattr(d.contract, "primaryExchange", "") in {"NASDAQ", "NYSE", "ARCA", "AMEX"}
                        else 1,
                        getattr(d.contract, "primaryExchange", ""),
                    ),
                )
                return preferred[0].contract

        detail = "; ".join(reasons) if reasons else "no contract details returned"
        raise RuntimeError(f"Could not resolve IBKR contract for {symbol} ({detail})")

    def resolve_index_contract(self, symbol: str) -> Any:
        _, Contract, _ = _load_ibapi()
        try:
            ibkr_symbol, exchange = INDEX_CONTRACT_MAP[symbol]
        except KeyError as exc:
            raise ValueError(
                f"No IBKR index contract mapping for {symbol!r}; add it to INDEX_CONTRACT_MAP"
            ) from exc

        contract = Contract()
        contract.symbol = ibkr_symbol
        contract.secType = "IND"
        contract.exchange = exchange
        contract.currency = "USD"

        req_id = self._new_req_id()
        self._events[req_id] = threading.Event()
        self._contract_details.pop(req_id, None)
        self._app.reqContractDetails(req_id, contract)
        self._wait(req_id)

        details = self._contract_details.get(req_id, [])
        if not details:
            raise RuntimeError(f"Could not resolve IBKR index contract for {symbol} ({ibkr_symbol}/{exchange})")
        return details[0].contract

    def fetch_historical_bars(self, request: HistoricalRequest) -> list[Any]:
        req_id = self._new_req_id()
        self._events[req_id] = threading.Event()
        self._historical_bars.pop(req_id, None)
        end_str = request.end.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S")
        self._app.reqHistoricalData(
            req_id,
            request.contract,
            end_str,
            request.duration,
            request.bar_size,
            request.what_to_show,
            1 if request.use_rth else 0,
            2,
            False,
            [],
        )
        self._wait(req_id)
        return list(self._historical_bars.get(req_id, []))


def _parse_bar_timestamp_ms(raw: Any) -> int:
    text = str(raw).strip()
    if text.isdigit():
        if len(text) == 8:
            dt = datetime.strptime(text, "%Y%m%d").replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        if len(text) <= 10:
            return int(text) * 1000
        return int(text)
    for fmt in ("%Y%m%d  %H:%M:%S", "%Y%m%d-%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized IBKR bar timestamp: {raw!r}")


def bars_to_dataframe(bars: list[Any]) -> pd.DataFrame:
    rows = []
    for bar in bars:
        rows.append(
            {
                "timestamp": _parse_bar_timestamp_ms(bar.date),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            }
        )
    if not rows:
        return empty_ohlcv_frame()
    return normalize_ohlcv(pd.DataFrame(rows))


def filter_full_bars_outside_rth(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    bar_delta = _timeframe_delta(timeframe)

    def is_overnight(timestamp_ms: int) -> bool:
        start = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).astimezone(NY_TZ)
        end = start + bar_delta
        for offset in range((end.date() - start.date()).days + 1):
            current_date = start.date() + timedelta(days=offset)
            rth_open = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                RTH_OPEN_HOUR,
                RTH_OPEN_MINUTE,
                tzinfo=NY_TZ,
            )
            rth_close = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                RTH_CLOSE_HOUR,
                0,
                tzinfo=NY_TZ,
            )
            if start < rth_close and end > rth_open:
                return False
        return True

    if df.empty:
        return df.copy()
    mask = df["timestamp"].map(is_overnight)
    return df.loc[mask].reset_index(drop=True)


def merge_canonical_parquet(src_df: pd.DataFrame, dst: Path) -> tuple[int, int]:
    """Deprecated alias for `utils.catalog.merge_ohlcv_parquet`."""
    return merge_ohlcv_parquet(src_df, dst)


def read_last_timestamp(path: Path) -> int | None:
    if not path.exists():
        return None
    df = normalize_ohlcv(pd.read_parquet(path))
    if df.empty:
        return None
    return int(df["timestamp"].iloc[-1])


def _timeframe_delta(timeframe: str) -> timedelta:
    if timeframe.endswith("h"):
        return timedelta(hours=int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return timedelta(days=int(timeframe[:-1]))
    if timeframe.endswith("w"):
        return timedelta(weeks=int(timeframe[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")


# --- shared download planning -------------------------------------------------
#
# The provider-neutral half of this now lives in utils/planning.py so the YF
# downloader can share it without importing an IBKR module. The names are
# re-exported above for existing callers.

#: Calendar days requested per historical-data call, by timeframe. IBKR's
#: per-request limits are bar-size dependent; these are conservative.
DEFAULT_CHUNK_DAYS: dict[str, int] = {
    "1h": 365,
    "4h": 730,
    "8h": 730,
    "1d": 365,
    "1w": 3650,
}


def chunk_days_for_timeframe(timeframe: str, configured: int | None = None) -> int:
    if configured is not None:
        return configured
    try:
        return DEFAULT_CHUNK_DAYS[timeframe]
    except KeyError as exc:
        raise ValueError(f"No default chunk size for timeframe {timeframe!r}") from exc


def ibkr_duration_str(chunk_start: datetime, chunk_end: datetime) -> str:
    """IBKR `durationStr` for a window, in days or whole years."""
    return duration_days_or_years(chunk_start, chunk_end)


def is_hmds_no_data_error(exc: Exception) -> bool:
    return "HMDS query returned no data" in str(exc)


def is_retryable_chunk_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return (
        isinstance(exc, TimeoutError)
        or "timed out" in text
        or "hmds server disconnect occurred" in text
    )


def download_ranges(
    client: IbkrHistoricalClient,
    contract: Any,
    *,
    label: str,
    timeframe: str,
    ranges: list[DownloadRange],
    chunk_days: int,
    what_to_show: str = "TRADES",
    use_rth: bool = False,
    sleep_seconds: float = 0.35,
    max_chunk_retries: int = 0,
    post_filter: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
    log: Callable[[str], None] = print,
) -> pd.DataFrame:
    """Fetch every chunk of every range and return one canonical DataFrame.

    Chunks whose error says IBKR simply has no data for the window are skipped;
    transient timeouts and HMDS disconnects are retried up to
    `max_chunk_retries` times with linear backoff. Anything else propagates, so
    the caller can record the symbol as failed and move on.
    """
    frames: list[pd.DataFrame] = []
    total_chunks = 0
    skipped_no_data = 0

    for planned in ranges:
        for chunk_start, chunk_end in daterange_chunks(planned.start, planned.end, chunk_days):
            total_chunks += 1
            attempts = max(1, max_chunk_retries + 1)
            bars: list[Any] | None = None

            for attempt in range(1, attempts + 1):
                try:
                    bars = client.fetch_historical_bars(
                        HistoricalRequest(
                            contract=contract,
                            end=chunk_end,
                            duration=ibkr_duration_str(chunk_start, chunk_end),
                            bar_size=ibkr_bar_size(timeframe),
                            what_to_show=what_to_show,
                            use_rth=use_rth,
                        )
                    )
                    break
                except Exception as exc:
                    window = f"{chunk_start.strftime('%Y-%m-%d')}->{chunk_end.strftime('%Y-%m-%d')}"
                    if is_hmds_no_data_error(exc):
                        skipped_no_data += 1
                        log(f"  {label}/{timeframe}: no data for chunk {window}")
                        bars = []
                        break
                    if is_retryable_chunk_error(exc) and attempt < attempts:
                        log(
                            f"  {label}/{timeframe}: retry {attempt}/{attempts - 1} "
                            f"after transient error for {window}"
                        )
                        throttle_sleep(max(sleep_seconds, 0.35) * attempt)
                        continue
                    raise

            if bars is None:
                raise RuntimeError(f"{label}/{timeframe}: no chunk result returned")

            df = bars_to_dataframe(bars)
            if not df.empty:
                start_ms = int(chunk_start.timestamp() * 1000)
                end_ms = int(chunk_end.timestamp() * 1000)
                df = df[(df["timestamp"] >= start_ms) & (df["timestamp"] <= end_ms)]
                if post_filter is not None and not df.empty:
                    df = post_filter(df)
                if not df.empty:
                    frames.append(df)
            throttle_sleep(sleep_seconds)

    detail = f" ({skipped_no_data} no-data chunks skipped)" if skipped_no_data else ""
    if not frames:
        log(f"  {label}/{timeframe}: no bars returned across {total_chunks} chunks{detail}")
        return empty_ohlcv_frame()

    merged = (
        pd.concat(frames)
        .drop_duplicates(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    log(f"  {label}/{timeframe}: fetched {len(merged)} bars across {total_chunks} chunks{detail}")
    return merged
