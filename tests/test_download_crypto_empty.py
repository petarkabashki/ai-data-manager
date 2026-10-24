"""A rejected download must not become a catalogued series.

Bitget advertises a 2h timeframe and then refuses the request, so freqtrade
writes a zero-column parquet. Merging that produced an empty destination file
and a dateless catalog node — a series that looks real and holds nothing. Every
one of bitget's 792 symbols acquired one that way.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "tools"))

from download_crypto import _is_empty_parquet  # noqa: E402


def test_zero_column_parquet_is_empty(tmp_path):
    """What a rejected freqtrade download actually leaves on disk."""
    path = tmp_path / "rejected.parquet"
    pq.write_table(pa.table({}), path)
    assert _is_empty_parquet(path)


def test_zero_row_parquet_is_empty(tmp_path, ohlcv):
    path = tmp_path / "norows.parquet"
    ohlcv(n=0).to_parquet(path, index=False)
    assert _is_empty_parquet(path)


def test_a_real_frame_is_not_empty(tmp_path, ohlcv):
    path = tmp_path / "real.parquet"
    ohlcv(n=5).to_parquet(path, index=False)
    assert not _is_empty_parquet(path)


def test_an_unreadable_file_counts_as_empty(tmp_path):
    """Never merge something we can't read — treat it as nothing, not as data."""
    missing = tmp_path / "gone.parquet"
    assert _is_empty_parquet(missing)

    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"not a parquet file")
    assert _is_empty_parquet(corrupt)
