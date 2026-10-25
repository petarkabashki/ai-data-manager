"""Detect new bars on disk, whichever tool wrote them.

The server never needs to know what a download tool did: it remembers each
series file's `(mtime_ns, size, last_timestamp)`, and after a job finishes it
re-stats the tree. A file whose stat changed is re-read for rows strictly
newer than its remembered last timestamp — exactly the bars to broadcast.

Catalog `end_date`s can't do this job: they are date-only, even for intraday
series, so they can't say which 5m bars of the last day are new.

The per-file state is persisted to `TMP_ROOT/pdm_watch.json`, so a restart only
re-reads the footers of files that changed while the server was down.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

#: Asset-class directories whose parquet files carry the canonical OHLCV
#: `timestamp`. `macro` (a rate series) and `market-cap` (exports) do not.
OHLCV_DIRS = ("cryptos", "crypto-futures", "stocks", "indexes", "commodities", "futures", "options")


@dataclass(slots=True)
class SeriesUpdate:
    path: str             # relative to the data root, `/`-separated
    frame: pd.DataFrame   # the new rows only (possibly truncated, see below)
    new_file: bool
    total_new_rows: int   # before truncation
    truncated: bool


def last_timestamp(path: Path) -> int | None:
    """Max `timestamp` from the footer's row-group statistics — no data read.

    Falls back to reading the one column when statistics are absent.
    """
    try:
        pf = pq.ParquetFile(path)
    except Exception:
        return None
    meta = pf.metadata
    if meta.num_rows == 0:
        return None
    try:
        col = pf.schema_arrow.get_field_index("timestamp")
    except KeyError:
        col = -1
    if col < 0:
        return None
    best = None
    for rg in range(meta.num_row_groups):
        stats = meta.row_group(rg).column(col).statistics
        if stats is None or not stats.has_min_max:
            best = None
            break
        best = stats.max if best is None else max(best, stats.max)
    if best is not None:
        return int(best)
    ts = pq.read_table(path, columns=["timestamp"]).column(0)
    return int(pc.max(ts).as_py()) if len(ts) else None


def read_rows_after(path: Path, after_ms: int | None) -> pd.DataFrame:
    filters = [("timestamp", ">", after_ms)] if after_ms is not None else None
    return pq.read_table(path, filters=filters).to_pandas()


class DataWatcher:
    def __init__(self, root: Path, state_file: Path | None = None, max_rows: int = 5000):
        self.root = Path(root)
        self.state_file = state_file
        self.max_rows = max_rows
        self._state: dict[str, tuple[int, int, int | None]] = {}
        self._lock = threading.Lock()
        self.ready = threading.Event()

    # --- walking --------------------------------------------------------------

    def _walk(self) -> dict[str, tuple[int, int]]:
        found: dict[str, tuple[int, int]] = {}
        for top in OHLCV_DIRS:
            base = self.root / top
            if not base.is_dir():
                continue
            for dirpath, _dirs, files in os.walk(base, followlinks=True):
                for name in files:
                    if not name.endswith(".parquet"):
                        continue
                    full = Path(dirpath) / name
                    try:
                        st = full.stat()
                    except FileNotFoundError:
                        continue
                    found[full.relative_to(self.root).as_posix()] = (st.st_mtime_ns, st.st_size)
        return found

    # --- priming --------------------------------------------------------------

    def prime(self) -> int:
        """Build the baseline. Returns how many footers had to be read."""
        cached = self._load_state()
        current = self._walk()
        state: dict[str, tuple[int, int, int | None]] = {}
        reads = 0
        for rel, (mtime, size) in current.items():
            old = cached.get(rel)
            if old and old[0] == mtime and old[1] == size:
                state[rel] = old
            else:
                state[rel] = (mtime, size, last_timestamp(self.root / rel))
                reads += 1
        with self._lock:
            self._state = state
        self._save_state()
        self.ready.set()
        return reads

    def series_count(self) -> int:
        return len(self._state)

    # --- diffing --------------------------------------------------------------

    def scan_changes(self) -> list[SeriesUpdate]:
        """Every series whose file changed since the last scan, with its new rows."""
        self.ready.wait()
        current = self._walk()
        updates: list[SeriesUpdate] = []
        with self._lock:
            for rel in set(self._state) - set(current):
                del self._state[rel]
            for rel, (mtime, size) in current.items():
                old = self._state.get(rel)
                if old and old[0] == mtime and old[1] == size:
                    continue
                path = self.root / rel
                after = old[2] if old else None
                try:
                    rows = read_rows_after(path, after)
                except Exception:
                    continue  # mid-replace or not a canonical file; next scan retries
                last = last_timestamp(path)
                self._state[rel] = (mtime, size, last if last is not None else after)
                if rows.empty:
                    continue
                total = len(rows)
                truncated = total > self.max_rows
                if truncated:
                    rows = rows.tail(self.max_rows).reset_index(drop=True)
                updates.append(SeriesUpdate(rel, rows, old is None, total, truncated))
        if updates:
            self._save_state()
        return updates

    # --- persistence ----------------------------------------------------------

    def _load_state(self) -> dict[str, tuple[int, int, int | None]]:
        if not self.state_file or not self.state_file.exists():
            return {}
        try:
            raw = json.loads(self.state_file.read_text())
            return {k: tuple(v) for k, v in raw.items()}
        except Exception:
            return {}

    def _save_state(self) -> None:
        if not self.state_file:
            return
        with self._lock:
            payload = json.dumps(self._state)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(payload)
        tmp.replace(self.state_file)
