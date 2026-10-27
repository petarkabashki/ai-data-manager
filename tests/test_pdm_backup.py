"""deploy/pdm-backup.sh: dated hard-linked snapshots, --again, pruning.

The script runs from a copy in a scratch repo (no .env), pulling from a local
directory, with `date` pinned through a shim on PATH.
"""

import os
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

pytestmark = pytest.mark.skipif(
    not (shutil.which("rsync") and shutil.which("flock")), reason="needs rsync and flock")


@pytest.fixture
def backup(tmp_path):
    repo = tmp_path / "repo"
    (repo / "deploy").mkdir(parents=True)
    shutil.copy(REPO / "deploy/pdm-backup.sh", repo / "deploy/pdm-backup.sh")
    (repo / "tools").mkdir()
    (repo / "tools/backup_delta.py").symlink_to(REPO / "tools/backup_delta.py")
    (repo / "utils").symlink_to(REPO / "utils")
    shim = tmp_path / "bin"
    shim.mkdir()
    real_date = shutil.which("date")
    (shim / "date").write_text(f'#!/bin/sh\nexec {real_date} -d "$FAKE_NOW" "$@"\n')
    (shim / "date").chmod(0o755)
    src = tmp_path / "src"
    (src / "crypto").mkdir(parents=True)
    (src / "crypto/BTC_USDT-1d.parquet").write_bytes(b"old bars")
    (src / "crypto/ETH_USDT-1d.parquet").write_bytes(b"untouched")
    (src / "assets.yaml").write_text("catalog\n")
    dest = tmp_path / "snap"

    def run(now, *args, keep=None, base_every=None):
        env = {k: v for k, v in os.environ.items() if not k.startswith("PDM_")}
        env.update(PATH=f"{shim}:{env['PATH']}", FAKE_NOW=now, PDM_NO_DOTENV="1",
                   PDM_BACKUP_SOURCE=str(src), PDM_BACKUP_DIR=str(dest),
                   PDM_BACKUP_PYTHON=sys.executable)
        if keep:
            env["PDM_BACKUP_KEEP"] = str(keep)
        if base_every:
            env["PDM_BACKUP_BASE_EVERY"] = str(base_every)
        return subprocess.run(["bash", str(repo / "deploy/pdm-backup.sh"), *args],
                              env=env, capture_output=True, text=True, check=False)

    run.src, run.dest = src, dest
    return run


def snaps(dest):
    return sorted(p.name for p in dest.iterdir() if p.is_dir() and not p.is_symlink())


def test_a_second_daily_run_is_a_no_op(backup):
    assert backup("2026-10-27 04:30").returncode == 0
    r = backup("2026-10-27 16:40")
    assert r.returncode == 0 and "already exists" in r.stdout
    assert snaps(backup.dest) == ["2026-10-27"]


def test_again_takes_another_snapshot_today_and_the_next_night_builds_on_it(backup):
    for day in ("2026-10-25", "2026-10-26", "2026-10-27"):
        assert backup(f"{day} 04:30", keep=3).returncode == 0
    (backup.src / "crypto/BTC_USDT-1d.parquet").write_bytes(b"old bars + one more")

    r = backup("2026-10-27 16:40", "--again", keep=3)
    assert r.returncode == 0, r.stderr
    assert "snapshot 2026-10-27T1640 complete" in r.stdout
    assert "pruning 2026-10-25" in r.stdout
    assert snaps(backup.dest) == ["2026-10-26", "2026-10-27", "2026-10-27T1640"]
    assert os.readlink(backup.dest / "latest") == "2026-10-27T1640"
    again = backup.dest / "2026-10-27T1640"
    assert (again / "crypto/BTC_USDT-1d.parquet").read_bytes() == b"old bars + one more"
    # the unchanged file is a hard link into the morning's snapshot
    assert (again / "crypto/ETH_USDT-1d.parquet").stat().st_ino == \
        (backup.dest / "2026-10-27/crypto/ETH_USDT-1d.parquet").stat().st_ino

    r = backup("2026-10-28 04:30", keep=3)
    assert r.returncode == 0, r.stderr
    assert snaps(backup.dest) == ["2026-10-27", "2026-10-27T1640", "2026-10-28"]
    # links against the --again snapshot, the newest, not the morning's
    assert (backup.dest / "2026-10-28/crypto/BTC_USDT-1d.parquet").stat().st_ino == \
        (again / "crypto/BTC_USDT-1d.parquet").stat().st_ino


def test_an_interrupted_again_run_is_resumed_by_the_next_night(backup):
    assert backup("2026-10-27 04:30").returncode == 0
    (backup.dest / "2026-10-27T1640.partial").mkdir()
    r = backup("2026-10-28 04:30")
    assert r.returncode == 0 and "resuming from 2026-10-27T1640.partial" in r.stdout
    assert snaps(backup.dest) == ["2026-10-27", "2026-10-28"]


def test_an_unknown_argument_is_refused(backup):
    r = backup("2026-10-27 16:40", "--now")
    assert r.returncode == 2 and "usage" in r.stderr
    assert not backup.dest.exists()


def test_delta_mode_takes_one_base_then_pulls_only_deltas(backup, tmp_path, monkeypatch):
    import importlib

    import pandas as pd
    monkeypatch.syspath_prepend(str(REPO / "tools"))
    bd = importlib.import_module("backup_delta")
    src, dest = backup.src, backup.dest
    btc = src / "crypto/BTC_USDT-1h.parquet"

    def bars(n):
        pd.DataFrame({"timestamp": pd.Series([i * 3_600_000 for i in range(n)], dtype="int64"),
                      "close": [float(i) for i in range(n)]}).to_parquet(btc, index=False)

    def export(day):
        _, rc = bd.export(src, 1, 14, now=datetime(2026, 10, day, 7, 15, tzinfo=UTC))
        assert rc == 0

    bars(100)
    export(27)
    r = backup("2026-10-27 09:00 UTC", base_every=30)
    assert r.returncode == 0, r.stderr
    assert "snapshot 2026-10-27 complete" in r.stdout
    assert (dest / "2026-10-27/.backup-started").read_text().strip() == "20261027T090000Z"
    assert not (dest / "2026-10-27/.deltas").exists()
    assert sorted(p.name for p in (dest / "deltas").iterdir()) == ["20261027T071500Z"]

    bars(124)
    (src / "assets.yaml").write_text("catalog v2\n")
    export(28)
    r = backup("2026-10-28 09:00 UTC", base_every=30)
    assert r.returncode == 0, r.stderr
    assert "deltas pulled; no base needed" in r.stdout
    assert not (dest / "2026-10-28").exists()
    assert not (dest / "deltas/manifest.parquet").exists()

    doc, rc = bd.restore(dest, tmp_path / "restored")
    assert rc == 0, doc["problems"]
    assert doc["deltas"] == ["20261027T071500Z", "20261028T071500Z"]
    for rel in bd.scan(src):
        assert bd.hash_file(src / rel) == bd.hash_file(tmp_path / "restored" / rel), rel
