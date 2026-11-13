#!/usr/bin/env -S uv run python
"""Record an explained check_adjustment finding on its YF node, so the daily audit passes it.

Some findings are explained and kept as they are: a spin-off Yahoo scaled
slightly differently from its own ratio (VZ/Frontier 2010), a stock dividend
IBKR never applied, a split IBKR hasn't reached yet (CTVA 2026-10-01). Each is
recorded with the number that was measured, so the file fails again if it moves:

    1d:
      YF:
        file_path: price-data/stocks/yf/sp500/VZ/VZ-1d.parquet
        adjustment: raw
        known_adjustment:
          verdict: anomaly
          drift: -0.0033
          reason: "Frontier spin-off 2010-07-01: Yahoo's factor leaves 0.3%"
          recorded: "2026-11-13"
          until_ibkr: "2026-10-01"     # optional: lapses once IBKR reaches it

The verdict and number come from a `check_adjustment.py --json` report; only a
YF row with a finding can be recorded. Real data errors are not recorded: they
go to the owner as repair candidates.

Dry run by default. `--apply` writes every stock catalog with a YF node on
that file, each under its lock. `--clear` removes the record.

    uv run python tools/record_known_adjustment.py --audit adj.json \\
        --symbols VZ NOC --reason "spin-off factor leaves a small residue"
    uv run python tools/record_known_adjustment.py --audit adj.json --symbols CTVA \\
        --reason "IBKR file predates the 2026-10-01 split" --until-ibkr 2026-10-01 --apply
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from check_adjustment import PROBLEMS, measure  # noqa: E402
from utils.catalog import (  # noqa: E402
    STOCK_UNIVERSES, iter_timeframe_nodes, load_stock_catalog, stock_catalog_txn,
)
from utils.report import allow_broken_pipe  # noqa: E402

FIELD = "known_adjustment"


def records(report: dict, symbols: set[str], files: set[str], reason: str,
            until_ibkr: str | None, today: str) -> tuple[dict[str, dict], list[str]]:
    """YF file_path → its record, and the selected rows that have nothing to record."""
    out, skipped = {}, []
    for row in report["files"]:
        if row["provider"] != "YF" or not (row["symbol"] in symbols or row["file_path"] in files):
            continue
        verdict = row.get("known", {}).get("measured_verdict") or row["verdict"]
        result = row.get("vs_ibkr") or next(iter(row.get("vs_yf") or []), None)
        got = measure(verdict, result) if verdict in PROBLEMS else None
        if got is None:
            skipped.append(f"{row['file_path']} ({row['verdict']})")
            continue
        record = {"verdict": verdict, got[0]: round(got[1], 5), "reason": reason, "recorded": today}
        if until_ibkr:
            record["until_ibkr"] = until_ibkr
        out[row["file_path"]] = record
    return out, skipped


def apply_records(catalog: dict, wanted: dict[str, dict | None]) -> list[tuple[str, str, dict | None, dict | None]]:
    """Set (or, for None, clear) the record on matching YF nodes. Returns (symbol, file, old, new)."""
    changes = []
    for key, entry in catalog.items():
        if not isinstance(entry, dict):
            continue
        for _tf, node_key, node in iter_timeframe_nodes(entry):
            provider = (node.get("provider") or node_key.split(":")[0]).upper()
            fp = node.get("file_path")
            if provider != "YF" or fp not in wanted:
                continue
            old, new = node.get(FIELD), wanted[fp]
            if old == new:
                continue
            if new is None:
                node.pop(FIELD, None)
            else:
                node[FIELD] = new
            changes.append((entry.get("name", key), fp, old, new))
    return changes


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--audit", type=Path, required=True, help="A check_adjustment.py --json report")
    p.add_argument("--symbols", nargs="+", default=[], help="Every YF file of these symbols")
    p.add_argument("--files", nargs="+", default=[], help="YF file_paths, as stored")
    p.add_argument("--reason", help="Why the finding is not a data error (required unless --clear)")
    p.add_argument("--until-ibkr", help="YYYY-MM-DD: the record lapses once the IBKR file reaches it")
    p.add_argument("--clear", action="store_true", help="Remove the record instead")
    p.add_argument("--catalog", nargs="+", choices=STOCK_UNIVERSES, default=STOCK_UNIVERSES)
    p.add_argument("--apply", action="store_true", help="Write the catalogs")
    args = p.parse_args(argv)
    if not (args.symbols or args.files):
        p.error("name --symbols or --files")
    if not (args.clear or args.reason):
        p.error("--reason is required")

    report = json.loads(args.audit.read_text())
    if args.clear:
        wanted: dict[str, dict | None] = {
            r["file_path"]: None for r in report["files"]
            if r["provider"] == "YF" and (r["symbol"] in args.symbols or r["file_path"] in args.files)}
        skipped: list[str] = []
    else:
        wanted, skipped = records(report, set(args.symbols), set(args.files), args.reason,
                                  args.until_ibkr, dt.date.today().isoformat())
    total = 0
    for universe in args.catalog:
        if args.apply:
            with stock_catalog_txn(universe) as fresh:
                changes = apply_records(fresh, wanted)
        else:
            changes = apply_records(load_stock_catalog(universe), wanted)
        for symbol, fp, _old, new in changes:
            what = "clear" if new is None else f"{new['verdict']} " + " ".join(
                f"{k}={new[k]}" for k in ("drift", "level", "until_ibkr") if k in new)
            print(f"{universe:<16} {symbol:<8} {what}  {fp}")
        total += len(changes)
    for line in skipped:
        print(f"  nothing to record: {line}")
    verb = "changed" if args.apply else "would change (dry run; add --apply)"
    print(f"\n{total} nodes {verb}")
    return 1 if skipped and not wanted else 0


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
