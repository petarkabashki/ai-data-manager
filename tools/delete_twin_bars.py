#!/usr/bin/env -S uv run python
"""Delete bars one venue's files hold copied from another venue's feed.

`check_twins.py` finds blocks of bars identical, close and volume, to another
venue's. They are not that venue's trades, so they come out, and the holes
they leave are refilled from the venue itself with `fill_crypto_holes.py`
(history the venue no longer serves stays absent and lands on the
exchange-empty record).

For every series of `--exchange`/`--trading-mode`, the tool joins the file
with the `--source` venue's file of the same symbol and timeframe and marks
the traded bars (volume > 0) whose close and volume are equal. Marked bars
less than `--max-gap` rows apart form one span; a span with `--min-bars` or
more marked bars is deleted from its first marked bar to its last, but only
rows at timestamps the source file also has. So a bar the source later
revised inside a copied page goes too, while the venue's own bars on another
anchor stay (kucoin's weekly bars open on Thursday, binance's on Monday, and
bitget's 3d bars sit between binance's: both kinds are interleaved with the
copies in the same file).

Dry run by default: prints every span (venue, series, timeframe, first and
last bar in UTC, rows, of which marked) and writes nothing. `--apply`:

- copies each original into `--backup-dir` (default
  `<data root>.twin-backup-<today>`, beside the data root, never inside it),
- rewrites the file without the spans, atomically,
- writes the revision record `revisions/<run id>.parquet` at the data root,
  one row per deleted bar with its old values and a content hash
  (sha256 of timestamp and OHLCV), plus `revisions/<run id>.yaml` with the
  spans,
- refreshes the dates of the catalog nodes of the rewritten files.

After the refill, `--log-refill <run id>` adds to that record one row per
deleted bar with what the file holds now: `refetched` with the new values
and hash, or `not_served` when the venue has no bar there. Then run
`check_twins.py --record` so the known list shrinks to what remains.

Rule (product rule, 2026-11-12): a bar the venue doesn't serve stays a
recorded gap (`not_served` here, the exchange-empty record for the fill),
never filled from another venue. No fill, fallback or merge may put another
venue's bar into a venue's file; that is the fault this tool removes.

Usage examples:
  uv run python tools/delete_twin_bars.py --exchange bybit --trading-mode futures
  uv run python tools/delete_twin_bars.py --exchange bybit --trading-mode futures --apply
  uv run python tools/delete_twin_bars.py --log-refill twin-delete-20261112T101500Z-bybit-perps
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import CRYPTO_EXCHANGES, refresh_dates_for_files, write_parquet_atomic  # noqa: E402
from utils.config import DATA_ROOT  # noqa: E402
from utils.report import allow_broken_pipe, print_table  # noqa: E402

SUBDIRS = {"spot": "cryptos", "perps": "crypto-futures"}
MODES = {"spot": "spot", "futures": "perps"}
DEFAULT_TIMEFRAMES = ["1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"]
DEFAULT_SOURCE = "binance-perps"
DEFAULT_MIN_BARS = 24
DEFAULT_MAX_GAP = 24
OHLCV = ["timestamp", "open", "high", "low", "close", "volume"]


@dataclass
class Span:
    """A run of copied bars in one file, deleted whole."""

    file: str  # relative to the data root
    source: str  # the source file, relative to the data root
    symbol: str
    timeframe: str
    first: int  # bar open time, epoch ms
    last: int
    rows: int  # rows deleted: in [first, last] at a timestamp the source has
    matched: int  # of which equal to the source in close and volume

    @property
    def first_utc(self) -> str:
        return _utc(self.first)

    @property
    def last_utc(self) -> str:
        return _utc(self.last)


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


def row_hash(row) -> str:
    """Content hash of one bar: timestamp and OHLCV, floats in repr form."""
    text = "|".join([str(int(row.timestamp))] + [repr(float(getattr(row, c))) for c in OHLCV[1:]])
    return hashlib.sha256(text.encode()).hexdigest()


def venue_dir(data_root: Path, label: str) -> Path:
    venue, cls = label.rsplit("-", 1)
    if venue not in CRYPTO_EXCHANGES or cls not in SUBDIRS:
        raise ValueError(f"not a venue label: {label!r} (e.g. binance-perps, kucoin-spot)")
    return data_root / SUBDIRS[cls] / venue


def copied_spans(target: pd.DataFrame, source: pd.DataFrame, min_bars: int, max_gap: int) -> list[tuple[int, int, int]]:
    """(first row, last row, marked bars) of each span of `target` copied from `source`."""
    target = target.reset_index(drop=True)
    joined = target[["timestamp", "close", "volume"]].reset_index().merge(
        source[["timestamp", "close", "volume"]], on="timestamp", suffixes=("", "_src"))
    marked = joined[(joined["volume"] > 0) & (joined["close"] == joined["close_src"])
                    & (joined["volume"] == joined["volume_src"])]["index"].sort_values().tolist()
    spans: list[list[int]] = []
    for i in marked:
        if spans and i - spans[-1][1] <= max_gap:
            spans[-1][1] = i
            spans[-1][2] += 1
        else:
            spans.append([i, i, 1])
    return [(a, b, n) for a, b, n in spans if n >= min_bars]


def plan(
    data_root: Path,
    target: str,
    source: str,
    timeframes: list[str],
    symbols: set[str] | None = None,
    min_bars: int = DEFAULT_MIN_BARS,
    max_gap: int = DEFAULT_MAX_GAP,
) -> list[Span]:
    """Every span to delete from `target`'s files, oldest series first."""
    tdir, sdir = venue_dir(data_root, target), venue_dir(data_root, source)
    if tdir == sdir:
        raise ValueError("target and source are the same venue")
    spans: list[Span] = []
    wanted = set(timeframes)
    for file in sorted(tdir.glob("*/*.parquet")):
        stem = file.name[: -len(".parquet")]
        if "-" not in stem:
            continue
        symbol, timeframe = stem.rsplit("-", 1)
        if timeframe not in wanted or (symbols and symbol not in symbols):
            continue
        twin = sdir / symbol / file.name
        if not twin.exists():
            continue
        frame = pd.read_parquet(file)
        src = pd.read_parquet(twin, columns=["timestamp", "close", "volume"])
        on_source = frame["timestamp"].isin(src["timestamp"]).to_numpy()
        rel, src_rel = str(file.relative_to(data_root)), str(twin.relative_to(data_root))
        for a, b, n in copied_spans(frame, src, min_bars, max_gap):
            spans.append(Span(rel, src_rel, symbol, timeframe, int(frame["timestamp"].iloc[a]),
                              int(frame["timestamp"].iloc[b]), int(on_source[a:b + 1].sum()), n))
    return spans


def revisions_dir(data_root: Path) -> Path:
    return data_root / "revisions"


def apply(data_root: Path, spans: list[Span], run_id: str, backup_dir: Path, meta: dict) -> dict:
    """Back up, delete the spans, write the revision record. Returns the summary."""
    by_file: dict[str, list[Span]] = {}
    for span in spans:
        by_file.setdefault(span.file, []).append(span)
    deleted_rows = []
    rewritten = []
    for rel, file_spans in by_file.items():
        path = data_root / rel
        frame = pd.read_parquet(path)
        on_source = frame["timestamp"].isin(pd.read_parquet(data_root / file_spans[0].source,
                                                             columns=["timestamp"])["timestamp"])
        drop = pd.Series(False, index=frame.index)
        for s in file_spans:
            drop |= (frame["timestamp"] >= s.first) & (frame["timestamp"] <= s.last)
        drop &= on_source
        dst = backup_dir / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dst)
        gone = frame[drop]
        for row in gone.itertuples(index=False):
            deleted_rows.append({"file": rel, "timestamp": int(row.timestamp), "action": "deleted",
                                 **{c: float(getattr(row, c)) for c in OHLCV[1:]},
                                 "row_hash": row_hash(row)})
        write_parquet_atomic(frame[~drop].reset_index(drop=True), path)
        rewritten.append(path)

    out = revisions_dir(data_root)
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(deleted_rows).to_parquet(out / f"{run_id}.parquet", index=False)
    summary = {**meta, "run_id": run_id, "backup_dir": str(backup_dir),
               "rows_deleted": len(deleted_rows), "files": len(by_file),
               "spans": [{**asdict(s), "first_utc": s.first_utc, "last_utc": s.last_utc} for s in spans]}
    (out / f"{run_id}.yaml").write_text(yaml.safe_dump(summary, sort_keys=False))
    try:
        summary["catalogs_written"] = refresh_dates_for_files(rewritten)
    except Exception as exc:  # noqa: BLE001 — the files are rewritten; say what's left
        summary["catalogs_error"] = f"{type(exc).__name__}: {exc}; run check_catalog.py --fix"
    return summary


def log_refill(data_root: Path, run_id: str) -> dict:
    """Add `refetched` / `not_served` rows for every deleted bar of a run."""
    path = revisions_dir(data_root) / f"{run_id}.parquet"
    record = pd.read_parquet(path)
    if (record["action"] != "deleted").any():
        raise ValueError(f"{run_id} already has its refill logged")
    rows = []
    for rel, deleted in record.groupby("file", sort=False):
        current = pd.read_parquet(data_root / rel).set_index("timestamp")
        for ts in deleted["timestamp"]:
            if ts in current.index:
                bar = current.loc[ts]
                row = pd.Series({"timestamp": ts, **{c: float(bar[c]) for c in OHLCV[1:]}})
                rows.append({"file": rel, "timestamp": int(ts), "action": "refetched",
                             **{c: row[c] for c in OHLCV[1:]}, "row_hash": row_hash(row)})
            else:
                rows.append({"file": rel, "timestamp": int(ts), "action": "not_served",
                             **{c: float("nan") for c in OHLCV[1:]}, "row_hash": ""})
    added = pd.DataFrame(rows, columns=record.columns)
    pd.concat([record, added], ignore_index=True).to_parquet(path, index=False)
    refetched = int((added["action"] == "refetched").sum())
    return {"run_id": run_id, "deleted": len(record), "refetched": refetched,
            "not_served": len(added) - refetched}


def report(spans: list[Span], target: str, source: str) -> None:
    if not spans:
        print(f"{target}: no bars copied from {source}.")
        return
    rows = [[s.symbol, s.timeframe, s.first_utc, s.last_utc, s.rows, s.matched] for s in spans]
    print(f"{target}: bars copied from {source}, by span (UTC, bar open times, inclusive):")
    print_table(["symbol", "tf", "first", "last", "rows", "equal"], rows)
    per_tf: dict[str, list[int]] = {}
    for s in spans:
        t = per_tf.setdefault(s.timeframe, [0, 0, 0])
        t[0] += 1
        t[1] += s.rows
        t[2] += s.matched
    print("\nBy timeframe:")
    print_table(["tf", "spans", "rows", "equal"], [[tf, *v] for tf, v in sorted(per_tf.items())])
    print(f"\nTotal: {len(spans)} spans, {sum(s.rows for s in spans)} rows in "
          f"{len({s.file for s in spans})} files, {len({s.symbol for s in spans})} symbols.")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", choices=CRYPTO_EXCHANGES, help="Venue whose files hold the copies")
    p.add_argument("--trading-mode", choices=list(MODES), default="spot")
    p.add_argument("--source", default=DEFAULT_SOURCE,
                   help=f"Venue the bars were copied from (default {DEFAULT_SOURCE})")
    p.add_argument("--timeframes", nargs="+", default=DEFAULT_TIMEFRAMES, metavar="TF")
    p.add_argument("--symbols", nargs="+")
    p.add_argument("--min-bars", type=int, default=DEFAULT_MIN_BARS, metavar="N",
                   help=f"Equal bars that make a span (default {DEFAULT_MIN_BARS})")
    p.add_argument("--max-gap", type=int, default=DEFAULT_MAX_GAP, metavar="ROWS",
                   help=f"Rows between equal bars still in one span (default {DEFAULT_MAX_GAP})")
    p.add_argument("--apply", action="store_true", help="Back up, delete and write the revision record")
    p.add_argument("--backup-dir", type=Path,
                   help="Where originals are copied (default: <data root>.twin-backup-<today>)")
    p.add_argument("--log-refill", metavar="RUN_ID", help="Record what the refill put back for a run")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = p.parse_args(argv)

    if args.log_refill:
        result = log_refill(DATA_ROOT, args.log_refill)
        print(json.dumps(result, indent=2) if args.json else
              f"{result['run_id']}: {result['deleted']} deleted, {result['refetched']} refetched, "
              f"{result['not_served']} not served")
        return 0
    if not args.exchange:
        p.error("--exchange is required (or --log-refill)")

    target = f"{args.exchange}-{MODES[args.trading_mode]}"
    try:
        spans = plan(DATA_ROOT, target, args.source, args.timeframes,
                     set(args.symbols) if args.symbols else None, args.min_bars, args.max_gap)
    except ValueError as exc:
        p.error(str(exc))

    if not args.apply:
        if args.json:
            print(json.dumps({"target": target, "source": args.source, "dry_run": True,
                              "rows": sum(s.rows for s in spans),
                              "spans": [{**asdict(s), "first_utc": s.first_utc, "last_utc": s.last_utc}
                                        for s in spans]}, indent=2))
        else:
            report(spans, target, args.source)
            print("Dry run: nothing written. Add --apply to delete.")
        return 0

    real = DATA_ROOT.resolve()
    backup_dir = args.backup_dir or real.with_name(f"{real.name}.twin-backup-{date.today().isoformat()}")
    if backup_dir.resolve().is_relative_to(real):
        p.error(f"--backup-dir must be outside the data root: {backup_dir}")
    if not spans:
        print(f"{target}: nothing to delete.")
        return 0
    run_id = f"twin-delete-{datetime.now(tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{target}"
    meta = {"target": target, "source": args.source, "timeframes": args.timeframes,
            "min_bars": args.min_bars, "max_gap": args.max_gap}
    summary = apply(DATA_ROOT, spans, run_id, backup_dir, meta)
    if args.json:
        print(json.dumps({k: v for k, v in summary.items() if k != "spans"}, indent=2))
    else:
        report(spans, target, args.source)
        print(f"\nDeleted {summary['rows_deleted']} rows from {summary['files']} files; originals in "
              f"{backup_dir}; record {revisions_dir(DATA_ROOT) / run_id}.parquet")
        if summary.get("catalogs_error"):
            print(f"Catalog dates NOT refreshed: {summary['catalogs_error']}")
        print(f"Next: fill_crypto_holes --exchange {args.exchange} --trading-mode {args.trading_mode} "
              f"--apply, then delete_twin_bars --log-refill {run_id}, then check_twins --record.")
    return 1 if summary.get("catalogs_error") else 0


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
