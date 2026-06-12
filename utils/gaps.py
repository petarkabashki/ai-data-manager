"""Gap detection for OHLCV parquet files (freqtrade format: col 0 = timestamp_ms)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from utils.catalog import TF_TO_MS, DATA_ROOT, catalog_key, load_crypto_catalog, normalize_ohlcv, read_parquet_range


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

    @property
    def ok(self) -> bool:
        return self.error is None and self.trailing_candles == 0 and not self.internal_gaps


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def detect_gaps(path: Path, timeframe: str) -> tuple[list[Gap], int]:
    """Read parquet file and return (internal_gaps, trailing_candles_behind_now).
    Returns ([], 0) for variable-interval timeframes (1Mo) or missing files."""
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

    # Internal gaps: consecutive diff > 1.5× interval
    diffs = ts[1:] - ts[:-1]
    threshold = int(interval_ms * 1.5)
    gap_indices = (diffs > threshold).nonzero()[0]

    gaps: list[Gap] = []
    for i in gap_indices:
        gap_start_ms = int(ts[i]) + interval_ms   # first missing candle
        gap_end_ms = int(ts[i + 1]) - interval_ms  # last missing candle
        missing = max(1, round((int(ts[i + 1]) - int(ts[i])) / interval_ms) - 1)
        gaps.append(Gap(gap_start_ms, gap_end_ms, missing))

    # Trailing gap: candles from last timestamp to now
    last_ms = int(ts[-1])
    now_ms = _now_ms()
    # subtract one interval so we don't count the currently-forming candle
    trailing = max(0, round((now_ms - last_ms) / interval_ms) - 1)

    return gaps, trailing


def audit_symbol(
    exchange: str,
    symbol: str,
    timeframes: list[str],
    catalog_entry: dict | None = None,
) -> list[SymbolAudit]:
    results: list[SymbolAudit] = []
    for tf in timeframes:
        fp: Path | None = None
        if catalog_entry:
            td = catalog_entry.get("timeframe_dates", {}).get(tf, {})
            fp_str = td.get("file_path", "")
            if fp_str and fp_str.endswith(".parquet"):
                fp = DATA_ROOT.parent / fp_str  # file_path is relative to repo root
        if fp is None:
            from utils.catalog import crypto_file_path
            fp = crypto_file_path(exchange, symbol, tf)

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


def audit_exchange(
    exchange: str,
    timeframes: list[str] | None = None,
    symbols: list[str] | None = None,
) -> list[SymbolAudit]:
    """Audit all (or specified) symbols for an exchange. Returns flat list of SymbolAudit."""
    catalog = load_crypto_catalog(exchange)
    all_results: list[SymbolAudit] = []

    for key, entry in catalog.items():
        sym = entry.get("name", key)
        if symbols and sym not in symbols:
            continue
        tfs = timeframes or entry.get("timeframes", [])
        # skip variable timeframes if not explicitly requested
        tfs = [t for t in tfs if TF_TO_MS.get(t) is not None or t in (timeframes or [])]
        all_results.extend(audit_symbol(exchange, sym, tfs, entry))

    return all_results
