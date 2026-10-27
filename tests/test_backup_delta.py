"""tools/backup_delta.py: row-level export, the hub's plan and prune, restore."""

from __future__ import annotations

import importlib
import json
import shutil
import sys
import time
from datetime import UTC, datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq
import pytest

from utils.config import REPO_ROOT

H = 3_600_000
T0 = datetime(2026, 10, 1, 7, 15, tzinfo=UTC)
BTC = "cryptos/binance/BTC_USDT/BTC_USDT-1h.parquet"
ETH = "cryptos/binance/ETH_USDT/ETH_USDT-1h.parquet"
CAT = "crypto_binance.yaml"


@pytest.fixture
def bd(tmp_path, monkeypatch):
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in [m for m in sys.modules if m.startswith("utils.") or m == "backup_delta"]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.syspath_prepend(str(REPO_ROOT / "tools"))
    mod = importlib.import_module("backup_delta")
    mod.root, mod.hub = root, tmp_path / "hub"
    mod.hub.mkdir()
    return mod


def bars(n, start=0, close=1.0):
    return pd.DataFrame({"timestamp": pd.Series([start + i * H for i in range(n)], dtype="int64"),
                         "open": 1.0, "high": 2.0, "low": 0.5,
                         "close": [close + i for i in range(n)], "volume": 100.0})


def put(bd, rel, df):
    from utils.catalog import write_parquet_atomic
    write_parquet_atomic(df, bd.root / rel)


def read(path):
    return pd.read_parquet(path)


def export(bd, day, **kw):
    doc, rc = bd.export(bd.root, workers=1, keep_days=kw.pop("keep_days", 14),
                        now=T0 + timedelta(days=day), **kw)
    assert rc == 0, doc["failures"]
    return doc


def index(bd, stamp):
    return json.loads((bd.root / ".deltas" / stamp / "index.json").read_text())


def entry(bd, stamp, rel):
    return next(e for e in index(bd, stamp)["entries"] if e["path"] == rel)


def take_base(bd, name, day, hour=9):
    """What pdm-backup.sh does on a base night: copy the tree, minus .deltas."""
    dst = bd.hub / name
    shutil.copytree(bd.root, dst, ignore=shutil.ignore_patterns(".deltas"))
    started = T0.replace(hour=hour, minute=0) + timedelta(days=day)
    (dst / ".backup-started").write_text(started.strftime(bd.STAMP_FMT) + "\n")


def pull(bd):
    """What pdm-backup.sh does every night: copy the host's deltas, no --delete."""
    (bd.hub / "deltas").mkdir(exist_ok=True)
    for d in (bd.root / ".deltas").iterdir():
        if d.is_dir() and not d.name.endswith(".tmp"):
            shutil.copytree(d, bd.hub / "deltas" / d.name, dirs_exist_ok=True)


def restore_matches_live(bd, tmp_path, name="restored", **kw):
    dest = tmp_path / name
    doc, rc = bd.restore(bd.hub, dest, **kw)
    assert rc == 0, doc["problems"]
    live, got = bd.scan(bd.root), bd.scan(dest)
    assert sorted(live) == sorted(got)
    for rel in live:
        assert bd.hash_file(bd.root / rel) == bd.hash_file(dest / rel), rel
        if rel.endswith(".parquet"):
            assert pq.read_schema(bd.root / rel).metadata == pq.read_schema(dest / rel).metadata
    return doc


def seed(bd):
    put(bd, BTC, bars(200))
    put(bd, ETH, bars(100))
    (bd.root / CAT).write_text("nodes: 2\n")


def test_the_first_export_records_the_manifest_only(bd):
    seed(bd)
    doc = export(bd, 0)
    assert doc["init"] and doc["parent"] is None and doc["read"] == 3
    assert index(bd, doc["stamp"])["entries"] == []
    assert not (bd.root / ".deltas" / doc["stamp"] / "data").exists()
    # untouched files are not read again
    assert export(bd, 1)["read"] == 0


def test_new_bars_and_a_revised_last_bar_export_as_an_append(bd, tmp_path):
    seed(bd)
    export(bd, 0)
    take_base(bd, "2026-10-01", 0)
    df = bars(203)                                    # 3 new bars
    df.loc[199, "close"] = 999.0                      # and the old last bar revised
    put(bd, BTC, df)
    doc = export(bd, 1)
    assert (doc["append"], doc["full"], doc["deleted"]) == (1, 0, 0)
    e = entry(bd, doc["stamp"], BTC)
    assert e["kind"] == "append" and e["keep_until"] == 198 * H and e["tail_rows"] == 4
    assert len(read(bd.root / ".deltas" / doc["stamp"] / "data" / BTC)) == 4
    pull(bd)
    restore_matches_live(bd, tmp_path)


def test_changed_history_a_new_file_a_deletion_and_a_catalog_go_whole(bd, tmp_path):
    seed(bd)
    export(bd, 0)
    take_base(bd, "2026-10-01", 0)
    df = bars(200)
    df.loc[10, "close"] = -1.0                        # older than the hashed tail
    put(bd, BTC, df)
    (bd.root / ETH).unlink()
    put(bd, "cryptos/binance/SOL_USDT/SOL_USDT-1h.parquet", bars(50))
    (bd.root / CAT).write_text("nodes: 3\n")
    doc = export(bd, 1)
    kinds = {e["path"]: e["kind"] for e in index(bd, doc["stamp"])["entries"]}
    assert kinds == {BTC: "full", ETH: "deleted", CAT: "full",
                     "cryptos/binance/SOL_USDT/SOL_USDT-1h.parquet": "full"}
    pull(bd)
    restore_matches_live(bd, tmp_path)


def test_a_file_without_a_sorted_timestamp_goes_whole(bd):
    put(bd, "macro/yf/X/X-1d.parquet", pd.DataFrame({"Value": [1.0, 2.0]}))
    put(bd, BTC, bars(5).iloc[::-1])
    export(bd, 0)
    put(bd, "macro/yf/X/X-1d.parquet", pd.DataFrame({"Value": [1.0, 2.0, 3.0]}))
    put(bd, BTC, bars(6).iloc[::-1])
    doc = export(bd, 1)
    assert doc["full"] == 2 and doc["append"] == 0


def test_a_touched_but_identical_file_exports_nothing(bd):
    seed(bd)
    export(bd, 0)
    put(bd, BTC, bars(200))                           # rewritten, same rows
    doc = export(bd, 1)
    assert doc["read"] == 1 and (doc["append"], doc["full"]) == (0, 0)


def test_a_week_of_nights_restores_to_any_of_them(bd, tmp_path):
    seed(bd)
    export(bd, 0)
    take_base(bd, "2026-10-01", 0)
    stamps = []
    for day in range(1, 8):
        put(bd, BTC, bars(200 + 24 * day))
        put(bd, ETH, bars(100 + day))
        stamps.append(export(bd, day)["stamp"])
        if day == 4:
            mid = {rel: bd.hash_file(bd.root / rel) for rel in bd.scan(bd.root)}
    pull(bd)
    restore_matches_live(bd, tmp_path)
    doc, rc = bd.restore(bd.hub, tmp_path / "day4", until=stamps[3])
    assert rc == 0 and doc["deltas"][-1] == stamps[3]
    assert {rel: bd.hash_file(tmp_path / "day4" / rel) for rel in bd.scan(tmp_path / "day4")} == mid


def test_a_base_taken_between_exports_restores_correctly(bd, tmp_path):
    seed(bd)
    export(bd, 0)
    put(bd, BTC, bars(224))
    export(bd, 1)
    put(bd, BTC, bars(240))                           # written after the export...
    take_base(bd, "2026-10-02", 1)                    # ...and caught by the base
    put(bd, BTC, bars(260))
    export(bd, 2)
    pull(bd)
    doc = restore_matches_live(bd, tmp_path)
    assert len(doc["deltas"]) == 3                    # replays from before the base


def test_an_export_that_died_before_its_manifest_is_harmless(bd, tmp_path, monkeypatch):
    seed(bd)
    export(bd, 0)
    take_base(bd, "2026-10-01", 0)
    put(bd, BTC, bars(224))
    real = bd.write_manifest
    monkeypatch.setattr(bd, "write_manifest", lambda *a: (_ for _ in ()).throw(OSError("disk")))
    with pytest.raises(OSError):
        export(bd, 1)
    monkeypatch.setattr(bd, "write_manifest", real)
    put(bd, BTC, bars(248))
    doc = export(bd, 2)
    assert index(bd, doc["stamp"])["parent"] == index(bd, "20261002T071500Z")["parent"]
    pull(bd)
    restore_matches_live(bd, tmp_path)


def test_a_gap_in_the_deltas_needs_a_new_base(bd):
    seed(bd)
    export(bd, 0)
    take_base(bd, "2026-10-01", 0)
    for day in (1, 2, 3):
        put(bd, BTC, bars(200 + day))
        export(bd, day)
    pull(bd)
    now = T0 + timedelta(days=3, hours=2)
    assert bd.hub_plan(bd.hub, 30, now=now)[0] == "delta"
    assert bd.hub_plan(bd.hub, 2, now=now)[0] == "base"       # too old
    shutil.rmtree(bd.hub / "deltas" / "20261003T071500Z")     # a night the hub never got
    mode, why = bd.hub_plan(bd.hub, 30, now=now)
    assert mode == "base" and "broken" in why
    with pytest.raises(bd.BackupError, match="broken"):
        bd.restore(bd.hub, bd.hub.parent / "r")


def test_a_base_older_than_the_chain_is_refused(bd):
    seed(bd)
    take_base(bd, "2026-09-30", -1)                   # before the first export
    export(bd, 0)
    put(bd, BTC, bars(224))
    export(bd, 1)
    pull(bd)
    assert bd.hub_plan(bd.hub, 30, now=T0 + timedelta(days=1))[0] == "base"
    with pytest.raises(bd.BackupError, match="new base"):
        bd.restore(bd.hub, bd.hub.parent / "r")


def test_pruning_on_the_host_and_on_the_hub(bd):
    seed(bd)
    for day in range(20):
        put(bd, BTC, bars(200 + day))
        export(bd, day, keep_days=14)
    host = sorted(p.name for p in (bd.root / ".deltas").iterdir() if p.is_dir())
    assert host[0] == "20261006T071500Z" and len(host) == 15
    pull(bd)
    take_base(bd, "2026-10-15", 14)
    assert bd.hub_prune(bd.hub) == [f"202610{d:02d}T071500Z" for d in range(6, 14)]


def test_restore_refuses_the_live_tree_and_a_non_empty_dest(bd, tmp_path):
    seed(bd)
    export(bd, 0)
    take_base(bd, "2026-10-01", 0)
    with pytest.raises(bd.BackupError, match="live data root"):
        bd.restore(bd.hub, bd.root / "restored")
    (tmp_path / "busy").mkdir()
    (tmp_path / "busy" / "x").write_text("")
    with pytest.raises(bd.BackupError, match="not empty"):
        bd.restore(bd.hub, tmp_path / "busy")


def test_cli_hub_plan_prints_one_word(bd, capsys):
    assert bd.main(["hub-plan", "--backup-dir", str(bd.hub), "--base-every", "30"]) == 0
    assert capsys.readouterr().out == "base\n"
    assert bd.main(["export", "--root", str(bd.root), "--workers", "1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["init"] is True


def test_cli_export_with_worker_processes(bd, tmp_path):
    import subprocess
    seed(bd)
    run = [sys.executable, str(REPO_ROOT / "tools/backup_delta.py"), "export",
           "--root", str(bd.root), "--workers", "2", "--json"]

    def go():
        r = subprocess.run(run, capture_output=True, text=True, check=False)
        assert r.returncode == 0, r.stderr[-2000:]
        return json.loads(r.stdout)

    assert go()["init"]
    put(bd, BTC, bars(224))
    put(bd, ETH, bars(101))
    time.sleep(1.1)                                   # stamps have one-second resolution
    doc = go()
    assert (doc["append"], doc["tail_rows"]) == (2, 25)
