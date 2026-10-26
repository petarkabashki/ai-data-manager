"""A rejected download must not become a catalogued series.

Bitget advertises a 2h timeframe and then refuses the request, so freqtrade
writes a zero-column parquet. Merging that produced an empty destination file
and a dateless catalog node — a series that looks real and holds nothing. Every
one of bitget's 792 symbols acquired one that way.
"""

from __future__ import annotations

import importlib
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


def _fresh_download_crypto(monkeypatch, root):
    """download_crypto against a tmp data root (the suite's re-import convention)."""
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in [m for m in sys.modules if m.startswith("utils.") or m == "download_crypto"]:
        monkeypatch.delitem(sys.modules, name)
    return importlib.import_module("download_crypto")


def test_process_downloads_skips_a_rejected_download(tmp_path, monkeypatch, ohlcv):
    """The bitget 2h case end to end: no destination file, no catalog node, and
    an existing series' data left untouched."""
    root = tmp_path / "price-data"
    root.mkdir()
    dc = _fresh_download_crypto(monkeypatch, root)
    datadir = tmp_path / "ft"
    datadir.mkdir()
    pq.write_table(pa.table({}), datadir / "RAY_USDT-2h.parquet")       # rejected timeframe
    ohlcv(n=5).to_parquet(datadir / "RAY_USDT-1d.parquet", index=False)  # a real one

    catalog: dict = {}
    changes = dc.process_downloads("bitget", ["RAY/USDT"], ["2h", "1d"], datadir, catalog)

    assert not dc.crypto_file_path("bitget", "RAY_USDT", "2h").exists()
    assert dc.crypto_file_path("bitget", "RAY_USDT", "1d").exists()
    assert [(s, tf) for s, tf, _, _ in changes] == [("RAY_USDT", "1d")]
    nodes = {tf for e in catalog.values() for tf in (e.get("timeframe_dates") or {})}
    assert nodes == {"1d"}


def test_process_downloads_reports_a_merge_that_adds_nothing(tmp_path, monkeypatch, ohlcv, capsys):
    """Re-merging the same rows used to raise NameError (zero_delta was only
    defined in the futures path)."""
    root = tmp_path / "price-data"
    root.mkdir()
    dc = _fresh_download_crypto(monkeypatch, root)
    datadir = tmp_path / "ft"
    datadir.mkdir()
    ohlcv(n=5).to_parquet(datadir / "RAY_USDT-1d.parquet", index=False)

    catalog: dict = {}
    dc.process_downloads("bitget", ["RAY/USDT"], ["1d"], datadir, catalog)
    changes = dc.process_downloads("bitget", ["RAY/USDT"], ["1d"], datadir, catalog)
    assert changes == [("RAY_USDT", "1d", 5, 5)]
    assert "1 series gained no rows" in capsys.readouterr().out


def test_the_catalog_is_saved_while_merging_not_only_at_the_end(tmp_path, monkeypatch, ohlcv):
    """A crash or a backup mid-merge must not leave files with no catalog node."""
    root = tmp_path / "price-data"
    root.mkdir()
    dc = _fresh_download_crypto(monkeypatch, root)
    datadir = tmp_path / "ft"
    datadir.mkdir()
    for sym in ("RAY_USDT", "VET_USDT"):
        ohlcv(n=5).to_parquet(datadir / f"{sym}-1d.parquet", index=False)

    saved: list[set] = []
    catalog: dict = {}
    checkpoint = dc.CatalogCheckpoint(lambda: saved.append(set(catalog)), every_s=0)
    dc.process_downloads("bitget", ["RAY/USDT", "VET/USDT"], ["1d"], datadir, catalog, checkpoint=checkpoint)
    assert len(saved) == 2 and len(saved[0]) == 1          # the first pair was saved before the second merged

    quiet = dc.CatalogCheckpoint(lambda: saved.append(set()), every_s=3600)
    quiet()
    assert quiet.saves == 0                                 # throttled: one save a minute at most
