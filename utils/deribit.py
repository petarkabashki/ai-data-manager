"""Deribit public REST client, for option history no other source carries.

Deliberately not ccxt. ccxt's `fetch_ohlcv` only works for *active*
instruments, so every expired option — which is to say all of the history — is
invisible through it. The one call that does reach history,
`public/get_tradingview_chart_data`, is exposed by ccxt only as an unnormalized
passthrough, so going through ccxt bought nothing and cost an error wrapper
that buried the reason a request failed. This module talks to the REST API
directly.

Three behaviours of that endpoint will silently corrupt an import if the
caller doesn't know about them, and this module handles all three:

  * **It pads past expiry.** Ask for a contract that expired in 2019 with
    `end_timestamp=now` and it returns bars right up to today — 2731 of them,
    of which 191 have any volume. The rest are flat zero-volume filler for an
    instrument that stopped existing seven years ago. `fetch_bars` clamps the
    window to the contract's life and drops untraded bars by default.
  * **It caps at 5001 bars per request** and anchors that window to
    `end_timestamp`, not to the start. A naive 1h request over a whole
    contract silently returns only the last 5001 hours — and if the contract
    is long expired, all 5001 are padding. `fetch_bars` pages backwards.
  * **It refuses a start before the instrument was listed**, with an error
    naming the earliest acceptable timestamp. `get_instrument` reports the
    listing date even for contracts that expired years ago, so the window is
    built from that rather than discovered by failing.

`get_instrument` is also how discovery works. There is no bulk listing of
expired instruments — `get_instruments(expired=true)` retains about a day — so
candidate names are constructed and validated one at a time. That is cheap
enough to be practical: measured at ~39ms per probe against the live API.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import requests

BASE_URL = "https://www.deribit.com/api/v2"

#: Our timeframe name → Deribit `resolution`. Deribit counts in minutes, with
#: `1D` the one non-numeric value; `"1h"` is not a resolution it accepts.
TIMEFRAME_TO_RESOLUTION: dict[str, str] = {
    "1m": "1", "3m": "3", "5m": "5", "10m": "10", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "3h": "180", "6h": "360", "12h": "720", "1d": "1D",
}

#: The documented per-request tick limit. Requests are sized just under it so
#: a boundary bar can never be dropped between pages.
MAX_TICKS = 5000

_MS_PER_DAY = 86_400_000


class DeribitError(RuntimeError):
    """A JSON-RPC error from Deribit, with its code preserved.

    The code matters: `-32602 Invalid params` is how a nonexistent instrument
    reports itself, which is what makes grid discovery possible. Collapsing it
    into a generic failure would make "this strike never existed"
    indistinguishable from "the network hiccuped".
    """

    def __init__(self, code: int | None, message: str, data: Any = None) -> None:
        super().__init__(f"{code}: {message}" if code is not None else message)
        self.code = code
        self.message = message
        self.data = data

    @property
    def is_unknown_instrument(self) -> bool:
        return self.code == -32602


@dataclass(frozen=True, slots=True)
class InstrumentInfo:
    """What Deribit knows about one instrument, including expired ones."""

    name: str
    listed: date
    expiry: date
    tick_size: float
    contract_size: float

    @property
    def listed_ms(self) -> int:
        return _to_ms(self.listed)

    @property
    def expiry_ms(self) -> int:
        return _to_ms(self.expiry)


def _to_ms(when: date) -> int:
    return int(datetime(when.year, when.month, when.day, tzinfo=timezone.utc).timestamp() * 1000)


def _ms_to_date(value: Any) -> date:
    return pd.to_datetime(int(value), unit="ms", utc=True).date()


class DeribitClient:
    """Thin public-endpoint client. No credentials: all of this is public data."""

    def __init__(
        self,
        base_url: str = BASE_URL,
        *,
        timeout: float = 30.0,
        sleep_seconds: float = 0.05,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries
        self._session = requests.Session()

    def __enter__(self) -> "DeribitClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self._session.close()

    def get(self, path: str, **params: Any) -> Any:
        """One public GET, unwrapping the JSON-RPC envelope.

        Retries transport errors and rate limiting, but never a JSON-RPC
        error: a nonexistent instrument would return the same answer every
        time, and retrying it would multiply the cost of a discovery sweep by
        `max_retries` for exactly the candidates that don't exist.
        """
        last: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = self._session.get(
                    f"{self.base_url}/{path}", params=params, timeout=self.timeout
                )
                if response.status_code == 429:
                    time.sleep(self.sleep_seconds * (2 ** attempt) + 0.5)
                    continue
                payload = response.json()
            except (requests.RequestException, ValueError) as exc:
                last = exc
                time.sleep(self.sleep_seconds * (2 ** attempt))
                continue

            if "error" in payload:
                error = payload["error"] or {}
                raise DeribitError(error.get("code"), error.get("message", "unknown"),
                                   error.get("data"))
            if self.sleep_seconds:
                time.sleep(self.sleep_seconds)
            return payload["result"]

        raise DeribitError(None, f"{path} failed after {self.max_retries} attempts: {last}")

    # --- discovery ------------------------------------------------------------

    def get_instrument(self, name: str) -> InstrumentInfo | None:
        """Metadata for one instrument, or None if it never existed.

        Works for expired instruments, which is the whole reason this is the
        discovery primitive rather than `get_instruments`.
        """
        try:
            result = self.get("public/get_instrument", instrument_name=name)
        except DeribitError as exc:
            if exc.is_unknown_instrument:
                return None
            raise
        return InstrumentInfo(
            name=result["instrument_name"],
            listed=_ms_to_date(result["creation_timestamp"]),
            expiry=_ms_to_date(result["expiration_timestamp"]),
            tick_size=float(result.get("tick_size") or 0.0),
            contract_size=float(result.get("contract_size") or 1.0),
        )

    def active_option_names(self, currency: str) -> list[str]:
        """Currently listed option instruments for one currency.

        Only the live board — Deribit retains expired listings for about a
        day, so anything older has to be reached by constructing names.
        """
        result = self.get("public/get_instruments", currency=currency.upper(), kind="option")
        return [i["instrument_name"] for i in result]

    def active_expiries(self, currency: str) -> list[date]:
        """Expiry dates currently on the board, soonest first."""
        seen = set()
        for name in self.active_option_names(currency):
            from utils.options import parse_deribit_name

            seen.add(parse_deribit_name(name)[1])
        return sorted(seen)

    # --- bars -----------------------------------------------------------------

    def chart(self, name: str, start_ms: int, end_ms: int, resolution: str) -> pd.DataFrame:
        """One raw chart request. Returns a frame in the canonical column order."""
        result = self.get(
            "public/get_tradingview_chart_data",
            instrument_name=name,
            start_timestamp=int(start_ms),
            end_timestamp=int(end_ms),
            resolution=resolution,
        )
        ticks = result.get("ticks") or []
        if not ticks:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
        # Deribit returns every field as a string, and the timestamps overflow
        # anything that tries to parse them as date literals. Cast explicitly.
        return pd.DataFrame({
            "timestamp": [int(t) for t in ticks],
            "open": [float(v) for v in result["open"]],
            "high": [float(v) for v in result["high"]],
            "low": [float(v) for v in result["low"]],
            "close": [float(v) for v in result["close"]],
            "volume": [float(v) for v in result["volume"]],
        })

    def fetch_bars(
        self,
        instrument: InstrumentInfo,
        timeframe: str,
        *,
        keep_untraded: bool = False,
        since_ms: int | None = None,
    ) -> pd.DataFrame:
        """Every bar this contract ever printed, paged and trimmed.

        The window is the contract's own life — listing to expiry — never
        `now`, because anything past expiry is padding. Set `keep_untraded` to
        keep zero-volume bars; the default drops them, since Deribit fills a
        session with no trades by repeating the last close and those bars are
        quotes at best and invention at worst.
        """
        try:
            resolution = TIMEFRAME_TO_RESOLUTION[timeframe]
        except KeyError as exc:
            raise ValueError(
                f"Unsupported timeframe {timeframe!r}; expected one of "
                f"{sorted(TIMEFRAME_TO_RESOLUTION)}"
            ) from exc

        start = instrument.listed_ms
        if since_ms is not None:
            start = max(start, since_ms)
        # One day past expiry so the final settlement bar is inside the window,
        # whatever hour of the expiry day the contract actually stopped.
        end = min(instrument.expiry_ms + _MS_PER_DAY, int(time.time() * 1000))
        if start >= end:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

        step = _window_ms(timeframe)
        frames: list[pd.DataFrame] = []
        cursor = end
        while cursor > start:
            window_start = max(start, cursor - step)
            frames.append(self.chart(instrument.name, window_start, cursor, resolution))
            if window_start <= start:
                break
            # Overlap by one bar rather than stepping exactly, so a bar landing
            # on the boundary is never lost between two pages; the merge dedups.
            cursor = window_start

        bars = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        if bars.empty:
            return bars

        bars = bars.drop_duplicates(subset="timestamp").sort_values("timestamp")
        if not keep_untraded:
            bars = bars[bars["volume"] > 0]
        return bars.reset_index(drop=True)


def _window_ms(timeframe: str) -> int:
    """How much time `MAX_TICKS` bars of this timeframe covers."""
    from utils.catalog import TF_TO_MS

    bar_ms = TF_TO_MS.get(timeframe)
    if not bar_ms:
        raise ValueError(f"No bar length known for timeframe {timeframe!r}")
    return bar_ms * MAX_TICKS
