"""The deleted-bars record: stock bars removed on purpose, kept out for good.

Yahoo still serves the bars `delete_stock_bars.py` removes (TBBK's flat 16.72
closes over 2004-12-23..2005-01-18), so without a record the next download or
re-download would merge them straight back. This is the stock-side companion of
the exchange-empty record (`utils/exchange_empty.py`).

One file, `<data root>/deleted_bars.yaml`, beside the catalogs so that the host
backup copies it with them. Under `series`, the key is the file path's last
four parts (`yf/russell2000/TBBK/TBBK-1d.parquet`):

    version: 1
    series:
      yf/russell2000/TBBK/TBBK-1d.parquet:
      - start_ms: 1103760000000    # first deleted bar, inclusive
        end_ms: 1106006400000      # last deleted bar, inclusive
        start: '2004-12-23'
        end: '2005-01-18'
        rows: 17
        run_id: stock-delete-20261113T160000Z
        reason: flat zero-volume placeholder bars
        deleted: '2026-11-13'

`merge_ohlcv_parquet`, the one merge every downloader uses, and
`redownload_yf.py` drop any row inside a recorded window before they write.
Delete an entry to let a window be filled again.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

RECORD_NAME = "deleted_bars.yaml"
RECORD_VERSION = 1

_cache: dict[Path, tuple[float, dict]] = {}


def record_path(data_root: Path | None = None) -> Path:
    if data_root is None:
        from utils.config import DATA_ROOT
        data_root = DATA_ROOT
    return Path(data_root) / RECORD_NAME


def series_key(file_path: Path | str) -> str:
    """`<provider>/<universe>/<symbol>/<file>`, however the path was rooted."""
    return "/".join(Path(file_path).parts[-4:])


def load(path: Path) -> dict[str, list[dict]]:
    """key → windows. Cached per file modification time; {} when there is no record."""
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return {}
    hit = _cache.get(path)
    if hit and hit[0] == mtime:
        return hit[1]
    import yaml
    series = (yaml.safe_load(path.read_text()) or {}).get("series") or {}
    _cache[path] = (mtime, series)
    return series


def windows(file_path: Path | str, record: Path | None = None) -> list[dict]:
    """The deleted windows of the file at `file_path`. Its data root holds the record."""
    record = record or _record_for(Path(file_path))
    return load(record).get(series_key(file_path), [])


def _record_for(path: Path) -> Path:
    """The record beside the catalogs: the nearest parent that holds one, else DATA_ROOT's."""
    for parent in path.parents:
        if (parent / RECORD_NAME).exists():
            return parent / RECORD_NAME
    return record_path()


def drop_deleted(frame: pd.DataFrame, file_path: Path | str, record: Path | None = None) -> pd.DataFrame:
    """`frame` (canonical OHLCV, ms timestamps) without the rows in the file's deleted windows."""
    spans = windows(file_path, record)
    if not spans or frame.empty:
        return frame
    ts = frame["timestamp"].astype("int64")
    gone = pd.Series(False, index=frame.index)
    for w in spans:
        gone |= (ts >= int(w["start_ms"])) & (ts <= int(w["end_ms"]))
    return frame[~gone].reset_index(drop=True) if gone.any() else frame


def add(record: Path, file_path: str, window: dict) -> None:
    """Append a window for `file_path` and write the record atomically."""
    from utils.catalog import _dump_yaml
    series = {k: list(v) for k, v in load(record).items()}
    spans = series.setdefault(series_key(file_path), [])
    spans[:] = [w for w in spans if (w["start_ms"], w["end_ms"]) != (window["start_ms"], window["end_ms"])]
    spans.append(window)
    spans.sort(key=lambda w: w["start_ms"])
    _dump_yaml(record, {"version": RECORD_VERSION, "series": series})
    _cache.pop(record, None)
