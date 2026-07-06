"""IBKR historical-bar helpers for intraday stock downloads."""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import count
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from utils.catalog import CANONICAL_COLS, normalize_ohlcv

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
            except (TimeoutError, RuntimeError):
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

        raise RuntimeError(f"Could not resolve IBKR contract for {symbol}")

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
        return pd.DataFrame(columns=CANONICAL_COLS).astype(
            {
                "timestamp": "int64",
                "open": "float64",
                "high": "float64",
                "low": "float64",
                "close": "float64",
                "volume": "float64",
            }
        )
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
    new_df = normalize_ohlcv(src_df)
    if dst.exists():
        old_df = normalize_ohlcv(pd.read_parquet(dst))
        old_rows = len(old_df)
        merged = (
            pd.concat([old_df, new_df])
            .drop_duplicates(subset=["timestamp"])
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
    else:
        dst.parent.mkdir(parents=True, exist_ok=True)
        old_rows = 0
        merged = new_df
    merged.to_parquet(dst, index=False)
    return old_rows, len(merged)


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


def daterange_chunks(
    start: datetime,
    end: datetime,
    chunk_days: int,
) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    cursor = end
    while cursor > start:
        chunk_start = max(start, cursor - timedelta(days=chunk_days))
        chunks.append((chunk_start, cursor))
        cursor = chunk_start
    chunks.reverse()
    return chunks


def throttle_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
