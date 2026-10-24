"""Gap detection for OHLCV parquet files.

`detect_gaps` takes a session calendar (see `utils/calendars.py`) that decides
which missing bars are real. It defaults to the 24/7 calendar, which is correct
for crypto; anything with a trading calendar must pass its own or every weekend
and holiday reads as a gap.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from utils.calendars import CONTINUOUS, calendar_for
from utils.catalog import (
    TF_TO_MS,
    crypto_file_path,
    crypto_futures_file_path,
    iter_timeframe_nodes,
    load_crypto_catalog,
    load_crypto_futures_catalog,
    normalize_ohlcv,
    read_parquet_range,
    resolve_from_repo,
)


@dataclass
class Gap:
    start_ms: int
    end_ms: int
    missing_candles: int

    @property
    def start(self) -> str:
        return datetime.fromtimestamp(self.start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

    @property
    def end(self) -> str:
        return datetime.fromtimestamp(self.end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")

    def __str__(self) -> str:
        return f"{self.start} → {self.end} ({self.missing_candles} candles)"


@dataclass
class SymbolAudit:
    symbol: str
    exchange: str
    timeframe: str
    file_path: Path
    start_date: str | None = None
    end_date: str | None = None
    row_count: int = 0
    trailing_candles: int = 0       # candles missing from end to now
    trailing_days: float = 0.0      # approx days behind
    internal_gaps: list[Gap] = field(default_factory=list)
    error: str | None = None
    provider: str | None = None
    #: Name of the calendar the gaps were judged against, so a surprising
    #: result can be traced to the rule that produced it.
    calendar: str = "continuous"

    @property
    def ok(self) -> bool:
        return self.error is None and self.trailing_candles == 0 and not self.internal_gaps


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def detect_gaps(
    path: Path,
    timeframe: str,
    calendar=None,
) -> tuple[list[Gap], int]:
    """Read a parquet file and return (internal_gaps, trailing_candles_behind_now).

    `calendar` decides which missing bars actually count. The default is
    `CONTINUOUS` (24/7), which is correct for crypto and reproduces the old
    behaviour exactly. Pass a `UsEquityCalendar` for anything with a trading
    calendar, or every weekend and holiday is reported as a gap — and, in the
    IBKR planner, re-requested forever.

    Returns ([], 0) for variable-interval timeframes (1Mo) or missing files.
    """
    calendar = calendar or CONTINUOUS
    interval_ms = TF_TO_MS.get(timeframe)
    if interval_ms is None or not path.exists():
        return [], 0

    try:
        normed = normalize_ohlcv(pd.read_parquet(path))
    except Exception:
        return [], 0

    if normed.empty:
        return [], 0

    ts = normed["timestamp"].values.astype("int64")

    # Candidate gaps: consecutive diff > 1.5× interval. A candidate is only a
    # real gap if the calendar says bars were expected inside it.
    diffs = ts[1:] - ts[:-1]
    threshold = int(interval_ms * 1.5)
    gap_indices = (diffs > threshold).nonzero()[0]

    gaps: list[Gap] = []
    for i in gap_indices:
        expected = calendar.expected_starts(int(ts[i]), int(ts[i + 1]), interval_ms)
        if not expected:
            continue  # entirely explained by a closure
        gaps.append(Gap(expected[0], expected[-1], len(expected)))

    # Trailing gap: expected bars between the last one stored and now. One
    # interval is subtracted so the currently-forming candle isn't counted.
    last_ms = int(ts[-1])
    now_ms = _now_ms()
    trailing = len(calendar.expected_starts(last_ms, now_ms - interval_ms + 1, interval_ms))

    return gaps, trailing


def audit_symbol(
    exchange: str,
    symbol: str,
    timeframes: list[str],
    catalog_entry: dict | None = None,
    trading_mode: str = "spot",
) -> list[SymbolAudit]:
    results: list[SymbolAudit] = []
    for tf in timeframes:
        fp: Path | None = None
        if catalog_entry:
            td = catalog_entry.get("timeframe_dates", {}).get(tf, {})
            fp_str = td.get("file_path", "")
            if fp_str and fp_str.endswith(".parquet"):
                fp = resolve_from_repo(fp_str)
        if fp is None:
            fp = (
                crypto_futures_file_path(exchange, symbol, tf)
                if trading_mode == "futures"
                else crypto_file_path(exchange, symbol, tf)
            )

        audit = SymbolAudit(
            symbol=symbol,
            exchange=exchange,
            timeframe=tf,
            file_path=fp,
        )

        if not fp.exists():
            audit.error = "file_missing"
            results.append(audit)
            continue

        range_result = read_parquet_range(fp)
        if range_result is None:
            audit.error = "unreadable"
            results.append(audit)
            continue

        audit.start_date, audit.end_date, audit.row_count = range_result
        audit.internal_gaps, audit.trailing_candles = detect_gaps(fp, tf)

        interval_ms = TF_TO_MS.get(tf)
        if interval_ms:
            audit.trailing_days = audit.trailing_candles * interval_ms / 86_400_000

        results.append(audit)
    return results


def audit_catalog_series(
    spec,
    timeframes: list[str] | None = None,
    symbols: list[str] | None = None,
    calendar_override=None,
) -> list[SymbolAudit]:
    """Audit every series in one catalog, whatever its asset class or shape.

    The crypto-only `audit_exchange()` below predates this; it's kept because
    `--trading-mode futures` and the exchange-shaped CLI still use it. This one
    works off `catalog_registry()`, so stocks, indexes, commodities and macro
    are auditable for the first time — each with the calendar its asset class
    implies, unless overridden.
    """
    results: list[SymbolAudit] = []

    for symbol, entry in spec.load().items():
        if not isinstance(entry, dict):
            continue
        name = entry.get("name", symbol)
        if symbols and name not in symbols:
            continue

        for timeframe, node_key, node in iter_timeframe_nodes(entry):
            if timeframes and timeframe not in timeframes:
                continue
            if TF_TO_MS.get(timeframe) is None and not (timeframes and timeframe in timeframes):
                continue  # variable-interval, skip unless asked for explicitly

            if node.get("schema") == "rate_series":
                continue  # not OHLCV; bar gaps are meaningless

            provider = node.get("provider") or (node_key.split(":")[0] if node_key else None)
            calendar = calendar_override or calendar_for(spec.asset_class, node.get("session"))

            fp_str = node.get("file_path", "")
            audit = SymbolAudit(
                symbol=name,
                exchange=spec.name,
                timeframe=timeframe,
                file_path=resolve_from_repo(fp_str) if fp_str else Path(""),
                provider=provider,
                calendar=calendar.name,
            )

            if not fp_str or not audit.file_path.exists():
                audit.error = "file_missing"
                results.append(audit)
                continue

            range_result = read_parquet_range(audit.file_path)
            if range_result is None:
                audit.error = "unreadable"
                results.append(audit)
                continue

            audit.start_date, audit.end_date, audit.row_count = range_result
            audit.internal_gaps, audit.trailing_candles = detect_gaps(
                audit.file_path, timeframe, calendar
            )
            interval_ms = TF_TO_MS.get(timeframe)
            if interval_ms:
                audit.trailing_days = audit.trailing_candles * interval_ms / 86_400_000
            results.append(audit)

    return results


def audit_exchange(
    exchange: str,
    timeframes: list[str] | None = None,
    symbols: list[str] | None = None,
    trading_mode: str = "spot",
) -> list[SymbolAudit]:
    """Audit all (or specified) symbols for an exchange. Returns flat list of SymbolAudit."""
    catalog = (
        load_crypto_futures_catalog(exchange)
        if trading_mode == "futures"
        else load_crypto_catalog(exchange)
    )
    all_results: list[SymbolAudit] = []

    for key, entry in catalog.items():
        sym = entry.get("name", key)
        if symbols and sym not in symbols:
            continue
        tfs = timeframes or entry.get("timeframes", [])
        # skip variable timeframes if not explicitly requested
        tfs = [t for t in tfs if TF_TO_MS.get(t) is not None or t in (timeframes or [])]
        all_results.extend(audit_symbol(exchange, sym, tfs, entry, trading_mode=trading_mode))

    return all_results
