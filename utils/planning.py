"""Deciding what to download, independent of who you're downloading from.

Every downloader faces the same question: given what's already on disk, which
windows still need requesting? That logic was duplicated between the two IBKR
tools; it now lives here so the YF downloader — and the next provider — share
one implementation and one set of tests.

Nothing in this module knows about any provider. `utils/ibkr.py` re-exports the
names it used to own, so existing imports keep working.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from utils.catalog import normalize_ohlcv


@dataclass(slots=True)
class DownloadRange:
    """A contiguous window to request, plus why the planner asked for it."""

    start: datetime
    end: datetime
    reason: str


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def days_stale(end_date: str | None) -> int | None:
    """Days between a catalog `end_date` and now, or None if it isn't a date.

    Shared by `status.py` and `query.py`, which must agree on what "stale"
    means or their two tables disagree about the same series.
    """
    if not end_date:
        return None
    try:
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (utc_now() - end).days


def parse_date(value: str) -> datetime:
    """'YYYY-MM-DD' -> midnight UTC."""
    return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def read_timestamp_bounds(path: Path) -> tuple[int, int] | None:
    """(first_ms, last_ms) of an existing parquet file, or None if absent/empty."""
    if not Path(path).exists():
        return None
    normed = normalize_ohlcv(pd.read_parquet(path))
    if normed.empty:
        return None
    ts = normed["timestamp"].astype("int64")
    return int(ts.iloc[0]), int(ts.iloc[-1])


def merge_ranges(ranges: list[DownloadRange]) -> list[DownloadRange]:
    """Sort and coalesce overlapping windows, keeping a combined reason."""
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda r: (r.start, r.end))
    merged: list[DownloadRange] = [ordered[0]]
    for item in ordered[1:]:
        prev = merged[-1]
        if item.start <= prev.end:
            merged[-1] = DownloadRange(
                start=prev.start,
                end=max(prev.end, item.end),
                reason=f"{prev.reason},{item.reason}",
            )
        else:
            merged.append(item)
    return merged


def format_ranges(ranges: list[DownloadRange]) -> str:
    return "; ".join(
        f"{r.start.strftime('%Y-%m-%d')}->{r.end.strftime('%Y-%m-%d')} ({r.reason})"
        for r in ranges
    )


def plan_download_ranges(
    path: Path,
    *,
    since: str | None,
    full: bool,
    download_end: datetime,
    overlap_days: int = 5,
    timeframe: str | None = None,
    extra_gap_ranges: list[DownloadRange] | None = None,
) -> list[DownloadRange]:
    """Decide what to request for one (symbol, timeframe), given what's on disk.

    Returns an empty list when the file is already current. `full` rebuilds
    from `since`; otherwise the planner asks only for a prepend window (when
    `since` predates the file's first bar) and a tail window overlapping the
    last `overlap_days`. `extra_gap_ranges` lets a caller splice in
    internal-gap windows it detected itself.
    """
    if full:
        if since is None:
            raise ValueError("--full requires --since so the initial historical range is explicit")
        return [DownloadRange(start=parse_date(since), end=download_end, reason="full")]

    fallback_start = parse_date(since) if since else utc_now() - timedelta(days=365)
    path = Path(path)

    if not path.exists():
        return [DownloadRange(start=fallback_start, end=download_end, reason="missing_file")]

    bounds = read_timestamp_bounds(path)
    if bounds is None:
        return [DownloadRange(start=fallback_start, end=download_end, reason="unreadable_file")]

    first_ms, last_ms = bounds
    ranges: list[DownloadRange] = list(extra_gap_ranges or [])

    if since:
        requested_start = parse_date(since)
        if int(requested_start.timestamp() * 1000) < first_ms:
            ranges.append(
                DownloadRange(start=requested_start, end=dt_from_ms(first_ms), reason="prepend")
            )

    tail_start = dt_from_ms(last_ms) - timedelta(days=overlap_days)
    if since:
        tail_start = max(tail_start, parse_date(since))
    if tail_start < download_end:
        ranges.append(DownloadRange(start=tail_start, end=download_end, reason="tail_overlap"))

    return merge_ranges([r for r in ranges if r.start < r.end])


def daterange_chunks(
    start: datetime,
    end: datetime,
    chunk_days: int,
) -> list[tuple[datetime, datetime]]:
    """Tile [start, end] backwards from the end, in chunk_days-long pieces."""
    chunks: list[tuple[datetime, datetime]] = []
    cursor = end
    while cursor > start:
        chunk_start = max(start, cursor - timedelta(days=chunk_days))
        chunks.append((chunk_start, cursor))
        cursor = chunk_start
    chunks.reverse()
    return chunks


def duration_days_or_years(chunk_start: datetime, chunk_end: datetime) -> str:
    """IBKR-style `durationStr` for a window, in days or whole years."""
    days = max(1, (chunk_end - chunk_start).days)
    if days > 365:
        return f"{math.ceil(days / 365)} Y"
    return f"{days} D"


def throttle_sleep(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)
