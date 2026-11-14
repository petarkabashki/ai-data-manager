"""A catalog apply never runs without its backup.

Every tool that changes a stock catalog backs the catalogs up first. When that
backup cannot be written, the tool exits non-zero and the catalog is byte for
byte what it was.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "tools"))

import flag_unadjusted_splits  # noqa: E402
import record_known_adjustment  # noqa: E402
import relabel_adjustment  # noqa: E402
import utils.catalog as cat  # noqa: E402

YF = "price-data/stocks/yf/sp500/VZ/VZ-1d.parquet"
IBKR = "price-data/stocks/ibkr/sp500/VZ/VZ-all-1d.parquet"
CATALOG = (
    "VZ:\n  name: VZ\n  timeframe_dates:\n    1d:\n"
    f"      YF:\n        file_path: {YF}\n        adjustment: raw\n"
    f"      IBKR:\n        file_path: {IBKR}\n        adjustment: raw\n"
)
REPORT = {"files": [
    {"provider": "YF", "symbol": "VZ", "file_path": YF, "verdict": "anomaly",
     "vs_ibkr": {"drift": -0.0033, "level": 1.0, "last": "2026-11-12"}, "vs_yf": []},
    {"provider": "IBKR", "symbol": "VZ", "file_path": IBKR, "verdict": "no_jump_found",
     "split": "split_jump", "split_jumps": [{"date": "2010-07-01"}]},
]}


@pytest.fixture
def data(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    (root / "sp500.yaml").write_text(CATALOG)
    monkeypatch.setattr(cat, "DATA_ROOT", root)
    monkeypatch.setattr(cat, "TMP_ROOT", tmp_path / "tmp")
    (tmp_path / "audit.json").write_text(json.dumps(REPORT))
    return root


def blocked(tmp_path: Path) -> Path:
    """A backup directory that can't be created: its parent is a file."""
    (tmp_path / "not-a-dir").write_text("")
    return tmp_path / "not-a-dir" / "backup"


TOOLS = {
    "record_known_adjustment": lambda t: [record_known_adjustment.main, "--audit", str(t / "audit.json"),
                                          "--symbols", "VZ", "--reason", "x"],
    "relabel_adjustment": lambda t: [relabel_adjustment.main, "--files", YF, "--adjustment", "adjusted"],
    "flag_unadjusted_splits": lambda t: [flag_unadjusted_splits.main, "--audit", str(t / "audit.json")],
}


def run(tool: str, tmp_path: Path, backup_dir: Path) -> int:
    main, *argv = TOOLS[tool](tmp_path)
    return main([*argv, "--catalog", "sp500", "--apply", "--catalog-backup-dir", str(backup_dir)])


@pytest.mark.parametrize("tool", sorted(TOOLS))
def test_a_backup_that_cant_be_written_leaves_the_catalog_unchanged(tool, data, tmp_path):
    before = (data / "sp500.yaml").read_bytes()
    assert run(tool, tmp_path, blocked(tmp_path)) != 0
    assert (data / "sp500.yaml").read_bytes() == before


@pytest.mark.parametrize("tool", sorted(TOOLS))
def test_an_apply_keeps_the_catalog_as_it_was_before(tool, data, tmp_path):
    before = (data / "sp500.yaml").read_bytes()
    backup = tmp_path / "backup"
    assert run(tool, tmp_path, backup) == 0
    assert (data / "sp500.yaml").read_bytes() != before
    assert (backup / "sp500.yaml").read_bytes() == before


def test_an_unlisted_catalog_is_backed_up_before_its_first_write(data, tmp_path):
    other = data / "indexes.yaml"
    other.write_text("SPX: {}\n")
    with cat.catalog_backup(tmp_path / "b"):
        cat._dump_yaml(other, {"SPX": {"name": "SPX"}})
        cat._dump_yaml(other, {"SPX": {"name": "changed again"}})
    assert (tmp_path / "b" / "indexes.yaml").read_text() == "SPX: {}\n"      # the first pre-image


def test_a_failed_backup_blocks_the_write_itself(data, tmp_path):
    with pytest.raises(cat.CatalogBackupError), cat.catalog_backup(blocked(tmp_path)):
        with cat.stock_catalog_txn("sp500") as fresh:
            fresh["VZ"]["name"] = "changed"
    assert (data / "sp500.yaml").read_text() == CATALOG


def test_outside_a_scope_the_first_write_keeps_a_pre_image_beside_the_data_root(data, tmp_path):
    with cat.stock_catalog_txn("sp500") as fresh:
        fresh["VZ"]["name"] = "changed"
    with cat.stock_catalog_txn("sp500") as fresh:
        fresh["VZ"]["name"] = "changed again"
    copies = list((tmp_path / "data.catalog-autobackup").glob("sp500.yaml.*"))
    assert len(copies) == 1                                   # once per process
    assert copies[0].read_text() == CATALOG                   # the pre-image
    assert not (tmp_path / "tmp").exists()


def test_outside_a_scope_only_the_newest_copies_are_kept(data, tmp_path, monkeypatch):
    monkeypatch.setattr(cat, "UNSCOPED_KEEP", 2)
    for i in range(4):
        cat._unscoped_done.clear()                            # as if a new process
        with cat.stock_catalog_txn("sp500") as fresh:
            fresh["VZ"]["name"] = f"v{i}"
    copies = sorted((tmp_path / "data.catalog-autobackup").glob("sp500.yaml.*"))
    assert len(copies) == 2
    assert "name: v2" in copies[-1].read_text()               # newest pre-image kept


def test_outside_a_scope_a_failed_backup_blocks_the_write(data, tmp_path):
    (tmp_path / "data.catalog-autobackup").write_text("")     # a file where the dir goes
    with pytest.raises(cat.CatalogBackupError):
        with cat.stock_catalog_txn("sp500") as fresh:
            fresh["VZ"]["name"] = "changed"
    assert (data / "sp500.yaml").read_text() == CATALOG
