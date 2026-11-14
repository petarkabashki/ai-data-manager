"""The exchange-empty record: windows an exchange answered with nothing.

Some holes in a stored series are real absences. Without a record every repair
run would request them again, and every audit would report them as failures.
`tools/fill_crypto_holes.py` writes the record and `tools/check_gaps.py` reads it.

One file, `<data root>/exchange_empty.yaml`, beside the catalogs so that the
host backup copies it with them. Under `series`, the key is the parquet path
below the data root:

    version: 1
    series:
      cryptos/bitget/DOGE_USDT/DOGE_USDT-1h.parquet:
      - start_ms: 1704067200000      # open time of the first absent bar
        end_ms: 1704135600000        # open time of the last absent bar, inclusive
        candles: 20
        start: 2024-01-01 00:00      # the same two, for a reader
        end: 2024-01-01 19:00
        checked: '2026-11-06'        # the day the exchange returned nothing
        reason: ...                  # optional: why the window is empty

Only an answer from the exchange belongs here. An error is not an answer, and a
window in the record is never requested again: delete its entry to retry it.
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import datetime, timezone
from pathlib import Path

from utils.catalog import _dump_yaml, load_yaml
from utils.config import DATA_ROOT

RECORD_NAME = "exchange_empty.yaml"
RECORD_VERSION = 1


def record_path() -> Path:
    return DATA_ROOT / RECORD_NAME


def series_key(file_path: Path | str) -> str:
    """`<dir>/<exchange>/<symbol>/<file>`, from a path however it was rooted.

    Taken from the path's tail rather than relative to the data root: the root
    is a symlink by default, so the same file is reachable under two prefixes.
    """
    return "/".join(Path(file_path).parts[-4:])


def bar_runs(bars: list[int], interval_ms: int) -> list[tuple[int, int, int]]:
    """Sorted bar open times -> (first, last, count) for each unbroken run."""
    runs: list[tuple[int, int, int]] = []
    for bar in bars:
        if runs and bar - runs[-1][1] == interval_ms:
            runs[-1] = (runs[-1][0], bar, runs[-1][2] + 1)
        else:
            runs.append((bar, bar, 1))
    return runs


def split_bars(
    start_ms: int, end_ms: int, interval_ms: int, windows: list[dict]
) -> tuple[list[tuple[int, int, int]], list[tuple[int, int, int]]]:
    """Split the bars start..end (inclusive) into runs (outside, inside) `windows`."""
    ordered = sorted(windows, key=lambda w: w["start_ms"])
    starts = [w["start_ms"] for w in ordered]
    outside: list[int] = []
    inside: list[int] = []
    for bar in range(start_ms, end_ms + 1, interval_ms):
        i = bisect_right(starts, bar) - 1
        (inside if i >= 0 and bar <= ordered[i]["end_ms"] else outside).append(bar)
    return bar_runs(outside, interval_ms), bar_runs(inside, interval_ms)


def _stamp(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


class ExchangeEmptyRecord:
    """The record in memory. `save()` writes it atomically if anything changed."""

    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else record_path()
        self.series: dict[str, list[dict]] = {}
        self.dirty = False
        if self.path.exists():
            self.series = load_yaml(self.path).get("series") or {}

    def windows(self, file_path: Path | str) -> list[dict]:
        return self.series.get(series_key(file_path), [])

    def add(self, file_path: Path | str, start_ms: int, end_ms: int, candles: int,
            reason: str | None = None) -> None:
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        window = {"start_ms": int(start_ms), "end_ms": int(end_ms), "candles": int(candles),
                  "start": _stamp(start_ms), "end": _stamp(end_ms), "checked": today}
        if reason:
            window["reason"] = reason
        windows = self.series.setdefault(series_key(file_path), [])
        windows.append(window)
        windows.sort(key=lambda w: w["start_ms"])
        self.dirty = True

    def save(self) -> None:
        if self.dirty:
            _dump_yaml(self.path, {"version": RECORD_VERSION, "series": self.series})
            self.dirty = False
