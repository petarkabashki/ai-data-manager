"""Yahoo Finance ingestion.

YF is the largest provider in this repo — every stock universe's daily bars,
all of the indexes, commodities — and until now nothing here could download it.
The data arrived out of band, so the biggest dataset in `price-data/` had no
reproducible refresh path.

Two things this module is careful about:

**Ticker spelling.** Our symbols are not Yahoo's. Indexes carry a `^` prefix
there (`GSPC` -> `^GSPC`), and share classes use a dash rather than a dot
(`BRK.B` -> `BRK-B`). Getting this wrong doesn't error — Yahoo returns an empty
frame for an unknown ticker — so the mapping is explicit and tested.

**Adjustment.** `auto_adjust` decides whether `Close` is back-adjusted for
splits and dividends. Historically this repo dropped `Adj Close` at ingestion,
so stored YF closes are raw. That is now a recorded, deliberate choice rather
than an accident: the caller picks, and the choice is written to the catalog
node's `adjustment` field.

The network call is isolated in `YahooFetcher.fetch()` so everything around it
— ticker mapping, interval mapping, lookback limits, normalization — is
testable without touching the network.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

import pandas as pd

from utils.catalog import empty_ohlcv_frame, normalize_ohlcv

#: Our timeframe -> Yahoo `interval`. Yahoo has no 4h or 8h bar.
OUR_TF_TO_YF_INTERVAL: dict[str, str] = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "60m",
    "1d": "1d",
    "1w": "1wk",
    "1Mo": "1mo",
}

#: How far back Yahoo serves each interval. Intraday history is sharply
#: limited; requesting more silently returns a short frame rather than failing,
#: so the planner clamps instead of discovering it the hard way.
YF_MAX_LOOKBACK_DAYS: dict[str, int | None] = {
    "1m": 7,
    "5m": 60,
    "15m": 60,
    "30m": 60,
    "60m": 730,
    "1d": None,
    "1wk": None,
    "1mo": None,
}

#: Our index symbol -> Yahoo ticker. Yahoo prefixes indexes with '^'.
INDEX_TICKERS: dict[str, str] = {
    "GSPC": "^GSPC",
    "DJI": "^DJI",
    "IXIC": "^IXIC",
    "NDX": "^NDX",
    "RUT": "^RUT",
    "VIX": "^VIX",
    "STOXX50E": "^STOXX50E",
    "FTSE": "^FTSE",
    "GDAXI": "^GDAXI",
    "N225": "^N225",
    "HSI": "^HSI",
}

#: Symbols whose Yahoo spelling can't be derived by rule.
TICKER_OVERRIDES: dict[str, str] = {}


class UnsupportedTimeframe(ValueError):
    """Our timeframe has no Yahoo equivalent (4h, 8h…)."""


def yf_interval(timeframe: str) -> str:
    try:
        return OUR_TF_TO_YF_INTERVAL[timeframe]
    except KeyError as exc:
        raise UnsupportedTimeframe(
            f"Yahoo has no {timeframe!r} bar; supported: {sorted(OUR_TF_TO_YF_INTERVAL)}"
        ) from exc


def yf_ticker(symbol: str, asset_class: str) -> str:
    """Yahoo ticker for one of our symbols.

    Indexes get the '^' prefix. Stock share classes use a dash where we (and
    IBKR) use a dot: BRK.B -> BRK-B. Commodities and futures symbols like
    `GC=F` are already Yahoo's own spelling and pass through.
    """
    if symbol in TICKER_OVERRIDES:
        return TICKER_OVERRIDES[symbol]
    if asset_class == "Index":
        return INDEX_TICKERS.get(symbol, f"^{symbol}" if not symbol.startswith("^") else symbol)
    if asset_class == "Stock":
        return symbol.replace(".", "-")
    return symbol


def clamp_start(start: datetime, timeframe: str, now: datetime | None = None) -> datetime:
    """Pull `start` forward to the oldest date Yahoo will serve for this interval."""
    limit_days = YF_MAX_LOOKBACK_DAYS.get(yf_interval(timeframe))
    if limit_days is None:
        return start
    oldest = (now or datetime.now(tz=timezone.utc)) - timedelta(days=limit_days)
    return max(start, oldest)


def frame_from_yahoo(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize a yfinance history frame to the canonical schema.

    yfinance returns the timestamp as the index — named `Date` for daily bars
    and `Datetime` for intraday — so it has to be reset into a column before
    `normalize_ohlcv()` can recognise the Yahoo shape.
    """
    if raw is None or len(raw) == 0:
        return empty_ohlcv_frame()

    frame = raw.reset_index()
    for candidate in ("Datetime", "index", "date"):
        if candidate in frame.columns and "Date" not in frame.columns:
            frame = frame.rename(columns={candidate: "Date"})
            break

    if isinstance(frame.columns, pd.MultiIndex):
        # A single-ticker download can still come back column-multi-indexed.
        frame.columns = [c[0] if isinstance(c, tuple) else c for c in frame.columns]

    return normalize_ohlcv(frame)


class Fetcher(Protocol):
    """Anything that can return a raw Yahoo-shaped frame.

    Exists so the downloader can be exercised end-to-end — planning, merging,
    catalog writes — against a recorded frame instead of the network.
    """

    def fetch(
        self, ticker: str, interval: str, start: datetime, end: datetime, auto_adjust: bool
    ) -> pd.DataFrame: ...


@dataclass(slots=True)
class YahooFetcher:
    """The real thing. `yfinance` is imported lazily so the module stays
    importable — and testable — without it installed."""

    timeout: float = 30.0

    def fetch(
        self, ticker: str, interval: str, start: datetime, end: datetime, auto_adjust: bool
    ) -> pd.DataFrame:
        try:
            import yfinance
        except ModuleNotFoundError as exc:  # pragma: no cover - env-dependent
            raise RuntimeError(
                "yfinance is not installed. Run `uv sync` before using the YF tools."
            ) from exc

        return yfinance.Ticker(ticker).history(
            start=start.date().isoformat(),
            end=(end.date() + timedelta(days=1)).isoformat(),  # Yahoo's end is exclusive
            interval=interval,
            auto_adjust=auto_adjust,
            actions=False,
            raise_errors=True,
        )


def fetch_ohlcv(
    fetcher: Fetcher,
    symbol: str,
    asset_class: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    auto_adjust: bool = False,
) -> pd.DataFrame:
    """Fetch one window for one symbol, canonical schema out.

    An unknown ticker is not an error at Yahoo — it comes back empty — so
    callers must treat an empty frame as "nothing returned", not "up to date".
    """
    interval = yf_interval(timeframe)
    ticker = yf_ticker(symbol, asset_class)
    raw = fetcher.fetch(ticker, interval, clamp_start(start, timeframe), end, auto_adjust)
    return frame_from_yahoo(raw)
