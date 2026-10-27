#!/usr/bin/env -S uv run python
"""Row-level backup deltas: export the rows that changed, restore any night.

    uv run python tools/backup_delta.py export [--json]                  on the data host
    uv run python tools/backup_delta.py restore --backup-dir D --dest OUT [--until STAMP]
    uv run python tools/backup_delta.py hub-plan --backup-dir D --base-every DAYS
    uv run python tools/backup_delta.py hub-prune --backup-dir D

Each nightly update adds a bar or so to most parquets, and rsync's hard links
work on whole files, so a dated snapshot copies most files again (8.5 GB a
night in 2026-10, for a few MB of new rows). This tool backs up the rows.

`export` (on the host, as a PDM job) compares every file under the data root
with `.deltas/manifest.parquet`, written by the previous export, and writes
what changed to `.deltas/<stamp>/`:

- a parquet whose old rows are intact gets only its new rows (`append`). The
  last few bars may have been revised: a download restarts a day before the
  last bar. So the cut is the first row that differs, found from per-row
  hashes of each file's last 64 rows, and the delta records the `timestamp`
  of the last row kept (`keep_until`), not a row count;
- a parquet whose older rows changed (a split re-download, a stamp repair),
  or that has no strictly increasing int64 `timestamp`, is written whole
  (`full`), as is a new file. Other files (catalogs, csv) are copied whole
  when their bytes change;
- a deleted file is listed (`deleted`), and `paths.txt.gz` lists every file.

Hidden names and `*.tmp` are skipped. The delta directory is written as
`<stamp>.tmp` and renamed, then the manifest is replaced. A crash between the
two makes the next export diff against the same parent again, and restoring
both deltas gives the same result. The first export (no manifest) records the
manifest only: a base copy must be taken after it. Deltas older than
`--keep-days` are deleted from the host; the hub keeps its own.

`restore` builds the tree as of a delta: it hard-links (or copies) a base
snapshot into `--dest`, then applies, in order, every delta from two days
before the base began, up to `--until`. An append keeps the rows up to
`keep_until` and adds the delta's rows, so a base taken between two exports
restores correctly, and so does replaying a delta the base already holds.
Each touched file is then checked against the row hash its last delta
recorded, and the file list against `paths.txt.gz`. Files are rewritten with
pyarrow, through a temp file and a rename, keeping each file's schema and
metadata. The base is never modified.

A base is a snapshot directory from `deploy/pdm-backup.sh` holding a
`.backup-started` file (the UTC stamp at which its rsync began). `hub-plan`
prints `base` or `delta`: `base` when there is none, the newest is
`--base-every` days old, or the deltas since it don't chain. `hub-prune`
deletes pulled deltas that no remaining base needs.

Exit status: 0 on success, 1 when a file failed (export) or a check failed
(restore), 2 on a bad argument or an unusable backup dir. `--json` writes
one JSON document to stdout and the progress log to stderr.
"""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import os
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import DATA_ROOT

DELTAS = ".deltas"
MANIFEST = "manifest.parquet"
STARTED = ".backup-started"
STAMP_FMT = "%Y%m%dT%H%M%SZ"
#: per-row hashes kept for each file's last rows: how far back a revision can
#: reach and still be an append (a download re-fetches the last day: 24 1h bars)
TAIL_ROWS = 64
#: a restore replays deltas from this long before its base began
REPLAY_MARGIN = timedelta(days=2)
FORMAT = 1

MANIFEST_SCHEMA = pa.schema([
    ("path", pa.string()), ("size", pa.int64()), ("mtime_ns", pa.int64()),
    ("kind", pa.string()), ("rows", pa.int64()), ("pre_rows", pa.int64()),
    ("pre_hash", pa.binary()), ("tail", pa.binary()), ("hash", pa.binary()),
    ("schema", pa.binary()), ("splice", pa.bool_()),
])


class BackupError(Exception):
    """An unusable backup dir or a broken delta chain (exit 2)."""


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def _h(data: bytes) -> bytes:
    return hashlib.blake2b(data, digest_size=16).digest()


def parse_stamp(stamp: str) -> datetime:
    return datetime.strptime(stamp, STAMP_FMT).replace(tzinfo=UTC)


# --- hashing ---------------------------------------------------------------------

def row_hashes(table: pa.Table) -> np.ndarray:
    """One uint64 per row over every column, index columns included."""
    if table.num_rows == 0:
        return np.zeros(0, dtype=np.uint64)
    df = table.to_pandas(ignore_metadata=True)
    return pd.util.hash_pandas_object(df, index=False).to_numpy(dtype=np.uint64)


def schema_fp(table: pa.Table) -> bytes:
    """Column names, types and metadata: any change means a whole-file copy."""
    s = table.schema
    meta = sorted((s.metadata or {}).items())
    return _h(repr(([(f.name, str(f.type)) for f in s], meta)).encode())


def full_hash(fp: bytes, rh: np.ndarray) -> bytes:
    return _h(fp + rh.tobytes())


def spliceable(table: pa.Table) -> bool:
    """A non-null int64 `timestamp`, strictly increasing: rows can be cut by time."""
    if "timestamp" not in table.column_names:
        return False
    col = table.column("timestamp")
    if col.type != pa.int64() or col.null_count:
        return False
    ts = col.to_numpy()
    return bool(len(ts) < 2 or np.all(ts[1:] > ts[:-1]))


def hash_file(path: Path) -> tuple[str, bytes]:
    """(kind, hash) of a file as export records it: rows for a parquet, else bytes."""
    if path.name.endswith(".parquet"):
        table = _read_table(path)
        if table is not None:
            return "rows", full_hash(schema_fp(table), row_hashes(table))
    return "bytes", _h(path.read_bytes())


def _read_table(path: Path) -> pa.Table | None:
    try:
        table = pq.read_table(path)
    except FileNotFoundError:
        raise
    except Exception:  # noqa: BLE001 - unreadable: backed up as bytes
        return None
    return table if table.num_columns else None


def _write_table(table: pa.Table, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        pq.write_table(table, tmp)
        tmp.replace(dst)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _write_bytes(data: bytes, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        tmp.write_bytes(data)
        tmp.replace(dst)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


# --- export ------------------------------------------------------------------------

def scan(root: Path) -> dict[str, os.stat_result]:
    """Every file under root by relative path, minus hidden names and *.tmp."""
    found = {}
    for dirpath, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for name in files:
            if name.startswith(".") or name.endswith(".tmp"):
                continue
            full = Path(dirpath) / name
            try:
                found[full.relative_to(root).as_posix()] = full.stat()
            except FileNotFoundError:
                continue
    return found


def _process(job: tuple) -> dict:
    """Diff one file against its manifest entry; write its delta file if any.
    With no out_data (the first export) it only computes the manifest entry."""
    root, rel, size, mtime_ns, old, out_data = job
    path = Path(root) / rel
    try:
        table = _read_table(path) if rel.endswith(".parquet") else None
        if table is None:
            data = path.read_bytes()
            h = _h(data)
            entry = {"path": rel, "size": size, "mtime_ns": mtime_ns, "kind": "bytes", "rows": 0,
                     "pre_rows": 0, "pre_hash": b"", "tail": b"", "hash": h, "schema": b"",
                     "splice": False}
            if not out_data or (old is not None and old["kind"] == "bytes" and old["hash"] == h):
                return {"entry": entry}
            _write_bytes(data, Path(out_data) / rel)
            return {"entry": entry, "delta": {"path": rel, "kind": "full", "hash_kind": "bytes",
                                              "hash": h.hex()}, "bytes": len(data)}
        return _diff_table(rel, size, mtime_ns, table, old, Path(out_data) if out_data else None)
    except FileNotFoundError:
        return {"gone": rel}
    except Exception as e:  # noqa: BLE001 - keep the old entry: the next export retries it
        return {"failed": rel, "error": f"{type(e).__name__}: {e}"}


def _diff_table(rel, size, mtime_ns, table, old, out_data: Path) -> dict:
    rh = row_hashes(table)
    n = len(rh)
    fp = schema_fp(table)
    h = full_hash(fp, rh)
    splice = spliceable(table)
    pre = max(0, n - TAIL_ROWS)
    entry = {"path": rel, "size": size, "mtime_ns": mtime_ns, "kind": "rows", "rows": n,
             "pre_rows": pre, "pre_hash": _h(rh[:pre].tobytes()), "tail": rh[pre:].tobytes(),
             "hash": h, "schema": fp, "splice": splice}
    if not out_data or (old is not None and old["kind"] == "rows" and old["hash"] == h):
        return {"entry": entry}

    common = None
    if (old is not None and old["kind"] == "rows" and old["schema"] == fp
            and old["splice"] and splice):
        m0 = old["pre_rows"]
        if n >= m0 and _h(rh[:m0].tobytes()) == old["pre_hash"]:
            t_old = np.frombuffer(old["tail"], dtype=np.uint64)
            seg = rh[m0:m0 + len(t_old)]
            k = len(seg)
            diff = np.nonzero(seg != t_old[:k])[0]
            common = m0 + (int(diff[0]) if len(diff) else k)
    delta = {"path": rel, "rows": n, "hash_kind": "rows", "hash": h.hex()}
    if common and n - common <= max(n // 2, TAIL_ROWS):
        ts = table.column("timestamp").to_numpy()
        tail = table.slice(common)
        delta.update(kind="append", keep_until=int(ts[common - 1]), tail_rows=tail.num_rows)
        written = 0
        if tail.num_rows:
            _write_table(tail, out_data / rel)
            written = (out_data / rel).stat().st_size
        return {"entry": entry, "delta": delta, "bytes": written}
    _write_table(table, out_data / rel)
    delta["kind"] = "full"
    return {"entry": entry, "delta": delta, "bytes": (out_data / rel).stat().st_size}


class _Inline:
    """map() in this process: --workers 1, and tests."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def map(self, fn, items, chunksize=1):
        return map(fn, items)


def load_manifest(path: Path) -> tuple[str | None, dict[str, dict]]:
    if not path.exists():
        return None, {}
    t = pq.read_table(path)
    stamp = (t.schema.metadata or {}).get(b"stamp", b"").decode() or None
    return stamp, {r["path"]: r for r in t.to_pylist()}


def write_manifest(path: Path, stamp: str, entries: dict[str, dict]) -> None:
    rows = [entries[k] for k in sorted(entries)]
    t = pa.Table.from_pylist(rows, schema=MANIFEST_SCHEMA).replace_schema_metadata(
        {"stamp": stamp, "format": str(FORMAT)})
    _write_table(t, path)


def export(root: Path, workers: int, keep_days: int, reinit: bool = False,
           now: datetime | None = None) -> tuple[dict, int]:
    t0 = time.monotonic()
    root = root.resolve()
    ddir = root / DELTAS
    ddir.mkdir(exist_ok=True)
    lock = open(ddir / ".lock", "w")  # noqa: SIM115 - held until the process exits
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise BackupError("another export is running")
    now = now or datetime.now(UTC)
    stamp = now.strftime(STAMP_FMT)
    parent, manifest = (None, {}) if reinit else load_manifest(ddir / MANIFEST)
    if (parent is not None and parent >= stamp) or (ddir / stamp).exists():
        raise BackupError(f"an export already ran at {parent or stamp}: "
                          f"the next one must come after it ({stamp})")
    init = parent is None
    for stale in ddir.glob("*.tmp"):
        shutil.rmtree(stale, ignore_errors=True)
    out = ddir / f"{stamp}.tmp"
    out.mkdir()
    out_data = out / "data"

    files = scan(root)
    entries: dict[str, dict] = {}
    deltas: list[dict] = []
    failures: list[dict] = []
    jobs = []
    for rel, st in files.items():
        old = manifest.get(rel)
        if old is not None and old["size"] == st.st_size and old["mtime_ns"] == st.st_mtime_ns:
            entries[rel] = old
        else:
            jobs.append((str(root), rel, st.st_size, st.st_mtime_ns,
                         old, "" if init else str(out_data)))
    log(f"export {stamp}: {len(files)} files, {len(jobs)} to read"
        + (" (first export: manifest only)" if init else f", parent {parent}"))
    nbytes = 0
    with ProcessPoolExecutor(max_workers=workers) if workers > 1 else _Inline() as pool:
        for i, r in enumerate(pool.map(_process, jobs, chunksize=32), 1):
            if "gone" in r:
                files.pop(r["gone"], None)
            elif "failed" in r:
                failures.append({"path": r["failed"], "error": r["error"]})
                if r["failed"] in manifest:
                    entries[r["failed"]] = manifest[r["failed"]]
            else:
                entries[r["entry"]["path"]] = r["entry"]
                if not init and "delta" in r:
                    deltas.append(r["delta"])
                    nbytes += r.get("bytes", 0)
            if i % 5000 == 0:
                log(f"  {i}/{len(jobs)} read")
    if not init:
        for rel in sorted(set(manifest) - set(entries)):
            deltas.append({"path": rel, "kind": "deleted"})
    deltas.sort(key=lambda d: d["path"])
    paths = sorted(p for p in entries)
    with gzip.open(out / "paths.txt.gz", "wt") as f:
        f.write("".join(p + "\n" for p in paths))
    index = {"format": FORMAT, "stamp": stamp, "parent": parent, "init": init,
             "files": len(paths), "entries": deltas}
    (out / "index.json").write_text(json.dumps(index, indent=0))
    out.rename(ddir / stamp)
    write_manifest(ddir / MANIFEST, stamp, entries)

    pruned = []
    cutoff = now - timedelta(days=keep_days)
    for d in sorted(ddir.iterdir()):
        try:
            when = parse_stamp(d.name)
        except ValueError:
            continue
        if d.is_dir() and d.name != stamp and when < cutoff:
            shutil.rmtree(d)
            pruned.append(d.name)
    kinds = {k: sum(1 for d in deltas if d["kind"] == k) for k in ("append", "full", "deleted")}
    doc = {"stamp": stamp, "parent": parent, "init": init, "scanned": len(files),
           "read": len(jobs), "append": kinds["append"], "full": kinds["full"],
           "deleted": kinds["deleted"],
           "tail_rows": sum(d.get("tail_rows", 0) for d in deltas),
           "bytes_written": nbytes, "pruned": pruned, "failures": failures,
           "seconds": round(time.monotonic() - t0, 1)}
    log(f"export {stamp}: {kinds['append']} append, {kinds['full']} full, "
        f"{kinds['deleted']} deleted, {nbytes / 1e6:.1f} MB, {doc['seconds']}s"
        + (f", {len(failures)} FAILED" if failures else ""))
    return doc, 1 if failures else 0


# --- the hub side ------------------------------------------------------------------

def bases(backup_dir: Path) -> list[tuple[datetime, Path]]:
    """Snapshot dirs that can anchor deltas, oldest first, with their start time."""
    out = []
    for d in backup_dir.iterdir():
        f = d / STARTED
        if d.is_dir() and not d.is_symlink() and not d.name.endswith(".partial") and f.is_file():
            try:
                out.append((parse_stamp(f.read_text().strip()), d))
            except ValueError:
                continue
    return sorted(out)


def load_deltas(backup_dir: Path) -> list[dict]:
    out = []
    root = backup_dir / "deltas"
    if not root.is_dir():
        return out
    for d in sorted(root.iterdir()):
        idx = d / "index.json"
        if d.is_dir() and idx.is_file():
            doc = json.loads(idx.read_text())
            doc["dir"] = d
            out.append(doc)
    return out


def select(base_start: datetime, deltas: list[dict], until: str | None) -> list[dict]:
    """The deltas to replay over a base begun at base_start, checked to chain."""
    chosen = [d for d in deltas
              if parse_stamp(d["stamp"]) > base_start - REPLAY_MARGIN
              and (until is None or d["stamp"] <= until)]
    if not chosen:
        return chosen
    first = chosen[0]
    anchor = first["parent"] or first["stamp"]
    if parse_stamp(anchor) > base_start:
        raise BackupError(f"delta {first['stamp']} builds on {anchor}, after the base "
                          f"began ({base_start.strftime(STAMP_FMT)}): take a new base")
    known = {anchor, first["stamp"]}
    for d in chosen[1:]:
        if d["init"] or d["parent"] not in known:
            raise BackupError(f"delta {d['stamp']} builds on {d['parent']}, which is "
                              "missing: the chain is broken, take a new base")
        known.add(d["stamp"])
    return chosen


def hub_plan(backup_dir: Path, base_every: int, now: datetime | None = None) -> tuple[str, str]:
    now = now or datetime.now(UTC)
    bs = bases(backup_dir)
    if not bs:
        return "base", "no base yet"
    start, d = bs[-1]
    if now - start >= timedelta(days=base_every):
        return "base", f"newest base {d.name} is {(now - start).days} days old"
    try:
        chosen = select(start, load_deltas(backup_dir), None)
    except BackupError as e:
        return "base", str(e)
    last = chosen[-1]["stamp"] if chosen else "none"
    return "delta", f"base {d.name}, deltas chain to {last}"


def hub_prune(backup_dir: Path) -> list[str]:
    bs = bases(backup_dir)
    if not bs:
        return []
    cutoff = bs[0][0] - REPLAY_MARGIN
    pruned = []
    for d in load_deltas(backup_dir):
        if parse_stamp(d["stamp"]) <= cutoff:
            shutil.rmtree(d["dir"])
            pruned.append(d["stamp"])
    return pruned


# --- restore -----------------------------------------------------------------------

def _link_tree(src: Path, dst: Path) -> int:
    n = 0
    for dirpath, dirs, files in os.walk(src):
        rel = Path(dirpath).relative_to(src)
        (dst / rel).mkdir(parents=True, exist_ok=True)
        for name in files:
            if rel == Path(".") and name == STARTED:
                continue
            s, t = Path(dirpath) / name, dst / rel / name
            try:
                os.link(s, t)
            except OSError:
                shutil.copy2(s, t)
            n += 1
    return n


def apply_delta(dest: Path, delta: dict) -> dict[str, dict]:
    """Apply one delta to the tree at dest; returns path → entry for what it touched."""
    touched = {}
    data = delta["dir"] / "data"
    for e in delta["entries"]:
        target = dest / e["path"]
        if e["kind"] == "deleted":
            target.unlink(missing_ok=True)
        elif e["kind"] == "full":
            src = data / e["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(target.name + ".tmp")
            shutil.copyfile(src, tmp)
            tmp.replace(target)
        elif e["kind"] == "append":
            if not target.exists():
                raise BackupError(f"{e['path']}: delta {delta['stamp']} appends to a file "
                                  "the restore does not have")
            table = pq.read_table(target)
            ts = table.column("timestamp").to_numpy()
            keep = int(np.searchsorted(ts, e["keep_until"], side="right"))
            parts = [table.slice(0, keep)]
            if e.get("tail_rows"):
                tail = pq.read_table(data / e["path"])
                parts.append(tail.cast(table.schema) if tail.schema != table.schema else tail)
            _write_table(pa.concat_tables(parts) if len(parts) > 1 else parts[0], target)
        else:
            raise BackupError(f"unknown delta kind {e['kind']!r} in {delta['stamp']}")
        touched[e["path"]] = e
    return touched


def restore(backup_dir: Path, dest: Path, until: str | None = None,
            base: str | None = None) -> tuple[dict, int]:
    t0 = time.monotonic()
    backup_dir, dest = backup_dir.resolve(), dest.resolve()
    if dest.is_relative_to(DATA_ROOT.resolve()):
        raise BackupError(f"--dest {dest} is inside the live data root")
    if dest.exists() and any(dest.iterdir()):
        raise BackupError(f"--dest {dest} is not empty")
    if until is not None:
        parse_stamp(until)
    bs = bases(backup_dir)
    if base is not None:
        bs = [b for b in bs if b[1].name == base]
        if not bs:
            raise BackupError(f"no base {base!r} (a snapshot with {STARTED}) in {backup_dir}")
    elif until is not None:
        bs = [b for b in bs if b[0] <= parse_stamp(until)]
    if not bs:
        raise BackupError(f"no base in {backup_dir} to restore from")
    start, bdir = bs[-1]
    chosen = select(start, load_deltas(backup_dir), until)
    log(f"restore: base {bdir.name} (began {start.strftime(STAMP_FMT)}), "
        f"{len(chosen)} deltas" + (f" to {chosen[-1]['stamp']}" if chosen else ""))
    n = _link_tree(bdir, dest)
    final: dict[str, dict] = {}
    for d in chosen:
        final.update(apply_delta(dest, d))

    problems = []
    for rel, e in sorted(final.items()):
        if e["kind"] == "deleted":
            continue
        kind, h = hash_file(dest / rel)
        if kind != e["hash_kind"] or h.hex() != e["hash"]:
            problems.append({"path": rel, "issue": "hash differs from the delta's"})
    if chosen:
        with gzip.open(chosen[-1]["dir"] / "paths.txt.gz", "rt") as f:
            want = {line.rstrip("\n") for line in f if line.strip()}
        have = set(scan(dest))
        problems += [{"path": p, "issue": "missing"} for p in sorted(want - have)]
        problems += [{"path": p, "issue": "not in the delta's file list"}
                     for p in sorted(have - want)]
    doc = {"base": bdir.name, "base_started": start.strftime(STAMP_FMT),
           "deltas": [d["stamp"] for d in chosen], "files": n,
           "touched": len(final), "problems": problems,
           "seconds": round(time.monotonic() - t0, 1)}
    log(f"restore: {n} files from the base, {len(final)} touched by deltas, "
        f"{len(problems)} problems, {doc['seconds']}s")
    return doc, 1 if problems else 0


# --- cli ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export", help="write tonight's delta (data host)")
    e.add_argument("--root", type=Path, default=DATA_ROOT)
    e.add_argument("--workers", type=int, default=max(1, min(4, (os.cpu_count() or 2) // 2)))
    e.add_argument("--keep-days", type=int, default=14,
                   help="delete host deltas older than this (default 14)")
    e.add_argument("--reinit", action="store_true",
                   help="start a new chain: record the manifest only")
    r = sub.add_parser("restore", help="build the tree as of a delta")
    r.add_argument("--backup-dir", type=Path, required=True)
    r.add_argument("--dest", type=Path, required=True)
    r.add_argument("--until", help="newest delta stamp to apply (default: all)")
    r.add_argument("--base", help="base snapshot name (default: newest before --until)")
    hp = sub.add_parser("hub-plan", help="print `base` or `delta` for tonight's run")
    hp.add_argument("--backup-dir", type=Path, required=True)
    hp.add_argument("--base-every", type=int, required=True)
    hr = sub.add_parser("hub-prune", help="delete pulled deltas no base needs")
    hr.add_argument("--backup-dir", type=Path, required=True)
    for s in (e, r, hp, hr):
        s.add_argument("--json", action="store_true", help="one JSON document on stdout")
    args = p.parse_args(argv)

    try:
        if args.cmd == "export":
            doc, rc = export(args.root, args.workers, args.keep_days, args.reinit)
        elif args.cmd == "restore":
            doc, rc = restore(args.backup_dir, args.dest, args.until, args.base)
        elif args.cmd == "hub-plan":
            if args.base_every < 1:
                raise BackupError("--base-every must be at least 1")
            mode, why = hub_plan(args.backup_dir, args.base_every)
            doc, rc = {"mode": mode, "reason": why}, 0
            if not args.json:
                print(mode)
                log(why)
                return 0
        else:
            doc, rc = {"pruned": hub_prune(args.backup_dir)}, 0
            for s in doc["pruned"]:
                log(f"pruning delta {s}")
    except BackupError as err:
        if args.json:
            print(json.dumps({"error": str(err)}))
        log(f"error: {err}")
        return 2
    if args.json:
        print(json.dumps(doc))
    return rc


if __name__ == "__main__":
    sys.exit(main())
