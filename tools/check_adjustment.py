#!/usr/bin/env -S uv run python
"""Check each stock and index YF node's `adjustment` label against its contents.

A node's `adjustment` records what the downloader asked for, not what came back.
On the stored tree `yf/sp500/AAPL` is labelled `raw` but closes at 72.40 on
2020-01-02, where `yf/nasdaq100/AAPL` and IBKR close at 75.09: it is
dividend-adjusted. IBKR TRADES, labelled `raw` too, is split-adjusted for recent
splits but not old ones, so `ibkr/sp500/XOM` falls 49% on 2001-07-19 and
`ibkr/sp500/PG` 50% on 2004-06-21, days nobody traded at those prices.

The test compares closes on the days both files hold (daily bars only):

- **Dividend adjustment drifts.** A back-adjusted series sits below the raw one
  in the past and converges to it at the present, so the ratio YF / reference
  rises over the overlap. IBKR TRADES is never dividend-adjusted, so it is the
  reference; another universe's YF file of the same ticker is the fallback.
- **A one-day step is not drift.** A ratio change over 2.5% in a day is a
  corporate action applied on one side only: a spin-off (A 2014-11-03, PFE
  2020-11-17), a stock dividend, or a split IBKR left alone (ABCB 6:5 on
  2005-03-11). It is taken out of `drift` and listed under `one_day_steps`,
  with the total in `step_drift`.
- **An unadjusted split steps.** The ratio jumps by a split factor in one day.
  The file whose own one-day move is the jump owns it.
- **No overlap, no verdict.** Such nodes are counted as unverifiable, never as
  passed.

Verdicts per YF node: `ok`, `mislabelled_raw_is_adjusted`,
`mislabelled_adjusted_is_raw` (only provable against another YF file),
`adjusted_label_no_drift` (no dividends in the overlap, or not adjusted),
`level_mismatch` (the files disagree on today's price), `anomaly`,
`unverifiable_no_overlap`, `unverifiable_yf_agree`. Every compared file, YF or
IBKR, also gets `split_jump` or `no_jump_found`.

Read-only. Fixing a finding is a re-download under the owner's chosen YF
convention, never an in-place patch.

    uv run python tools/check_adjustment.py
    uv run python tools/check_adjustment.py --catalog sp500 --symbols AAPL XOM
    uv run python tools/check_adjustment.py --json > adjustment.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from check_splits import nearest_simple_ratio  # noqa: E402

from utils.catalog import DATA_ROOT, STOCK_UNIVERSES, catalog_registry, iter_timeframe_nodes  # noqa: E402
from utils.report import allow_broken_pipe  # noqa: E402

CATALOGS = [*STOCK_UNIVERSES, "indexes"]
TIMEFRAME = "1d"

#: Days at each end of the overlap whose median ratio is compared. A median, so
#: one bad close at either end can't fake a drift.
EDGE_DAYS = 20
#: Fewer overlapping days than this gives no verdict.
MIN_OVERLAP = 2 * EDGE_DAYS
#: Relative drift of the ratio over the overlap above which the YF file is
#: dividend-adjusted against its reference. One year of a 1% yield drifts
#: about 1%; provider close noise stays well under 0.1% in the median.
DRIFT_TOL = 0.003
#: The ratio at the end of the overlap must be this close to 1.
LEVEL_TOL = 0.02
#: A one-day change in the ratio at least this large is tested as a split.
STEP_MIN = 1.25
#: How close the step must sit to a split factor. Two providers' closes differ
#: by a few tenths of a percent, so this is looser than check_splits' 0.3%.
STEP_TOL = 0.02
#: A one-day ratio change larger than this (in log) is a corporate action on one
#: side (a spin-off, a stock dividend, a split IBKR left alone), not dividend
#: drift, and is taken out of `drift`. A regular dividend moves the ratio well
#: under 2.5% on its ex-date; a special one above it is listed, not counted.
STEP_CUT = 0.025
MAX_STEPS = 3   # more held one-day steps than this: a noisy ratio, steps not removed

PROBLEMS = {"mislabelled_raw_is_adjusted", "mislabelled_adjusted_is_raw",
            "level_mismatch", "anomaly", "split_jump"}


@dataclass(frozen=True)
class Node:
    catalog: str
    symbol: str
    provider: str          # "YF" or "IBKR"
    file_path: str         # as stored, "price-data/..."
    adjustment: str | None
    flagged: tuple[str, ...] = ()  # IBKR `unadjusted_splits`: jumps already on record


def resolve(data_root: Path, file_path: str) -> Path:
    path = Path(file_path)
    return path if path.is_absolute() else data_root / file_path.split("/", 1)[1]


def daily_nodes(catalogs: dict[str, dict]) -> dict[str, list[Node]]:
    """Symbol → its YF nodes and default-session IBKR nodes at 1d, in catalog order."""
    by_symbol: dict[str, list[Node]] = defaultdict(list)
    for catalog, data in catalogs.items():
        for key, entry in data.items():
            if not isinstance(entry, dict):
                continue
            symbol = entry.get("name", key)
            for timeframe, node_key, node in iter_timeframe_nodes(entry):
                provider = (node.get("provider") or node_key.split(":")[0]).upper()
                if timeframe != TIMEFRAME or provider not in ("YF", "IBKR") or ":" in node_key:
                    continue
                if node.get("file_path"):
                    by_symbol[symbol].append(
                        Node(catalog, symbol, provider, node["file_path"], node.get("adjustment"),
                             tuple(str(d) for d in node.get("unadjusted_splits") or ())))
    return by_symbol


def load_close(path: Path) -> pd.Series | None:
    try:
        frame = pd.read_parquet(path, columns=["timestamp", "close"])
    except Exception:
        return None
    dates = pd.to_datetime(frame["timestamp"].astype("int64"), unit="ms").dt.normalize()
    close = pd.Series(frame["close"].to_numpy(dtype=float), index=dates)
    close = close[~close.index.duplicated(keep="last")].sort_index()
    return close[np.isfinite(close) & (close > 0)]


def _move(close: pd.Series, day: pd.Timestamp) -> float:
    """The file's own one-day close ratio into `day`."""
    pos = close.index.get_loc(day)
    return float(close.iloc[pos] / close.iloc[pos - 1]) if pos > 0 else 1.0


def compare(this: pd.Series, ref: pd.Series) -> dict | None:
    """Drift, end level and split steps of this / ref over their common days."""
    ratio = (this / ref).dropna()
    if len(ratio) < MIN_OVERLAP:
        return None
    steps = []
    change = (ratio / ratio.shift(1)).iloc[1:]
    for day, q in change[(change >= STEP_MIN) | (change <= 1 / STEP_MIN)].items():
        if nearest_simple_ratio(float(q), STEP_TOL) is None:
            continue
        # A step undone on the next shared day is one bad bar, not a split.
        pos = change.index.get_loc(day)
        if any(0 <= i < len(change) and abs(float(change.iloc[i]) * float(q) - 1) <= STEP_TOL
               for i in (pos - 1, pos + 1)):
            steps.append({"date": str(day.date()), "owner": "", "bad_bar": True})
            continue
        own_this, own_ref = _move(this, day), _move(ref, day)
        owner = "this" if abs(np.log(own_this)) >= abs(np.log(own_ref)) else "ref"
        move = own_this if owner == "this" else own_ref
        # The ratio step cancels the day's market move, so it names the factor
        # more exactly than the owner's own return does.
        factor = nearest_simple_ratio(float(q) if owner == "this" else 1 / float(q), STEP_TOL)
        steps.append({
            "date": str(day.date()), "owner": owner, "move": round(move, 4),
            "factor": f"{factor.numerator}:{factor.denominator}" if factor else "",
        })
    # Drift and level are read after the last step, where both files agree on
    # the share count; before it the ratio is off by the split factor.
    splits = [s for s in steps if not s.get("bad_bar")]
    segment = ratio[ratio.index >= pd.Timestamp(splits[-1]["date"])] if splits else ratio
    if len(segment) < MIN_OVERLAP:
        segment = ratio
    early = float(segment.iloc[:EDGE_DAYS].median())
    late = float(segment.iloc[-EDGE_DAYS:].median())
    log_ratio = np.log(segment.astype(float))
    log_change = log_ratio.diff()
    jumps = {}
    # A step inside an edge window moves its median only partly, so look inside.
    for pos in range(EDGE_DAYS + 1, len(segment) - EDGE_DAYS):
        one_day = float(log_change.iloc[pos])
        if abs(one_day) <= STEP_CUT:
            continue
        # Only the part that stays is a level change; a move that drifts back
        # (ABBV 2024-08-07) is noise, and taking it out would invent drift.
        held = float(log_ratio.iloc[pos:pos + EDGE_DAYS].median() - log_ratio.iloc[pos - EDGE_DAYS:pos].median())
        if held * one_day > 0 and abs(held) >= abs(one_day) / 2:
            jumps[segment.index[pos]] = held
    jumps = pd.Series(jumps, dtype=float)
    # A corporate action is one or two steps. More means a noisy ratio (WBD vs
    # IBKR's Discovery listing in 2021), where steps can't be told from drift.
    noisy = len(jumps) > MAX_STEPS
    removed = 0.0 if noisy else float(jumps.sum())
    return {
        "overlap_days": int(len(ratio)),
        "first": str(ratio.index[0].date()), "last": str(ratio.index[-1].date()),
        "drift": round(late / early / float(np.exp(removed)) - 1, 5), "level": round(late, 5),
        "step_drift": round(float(np.exp(removed)) - 1, 5),
        "noisy_ratio": noisy,
        "one_day_steps": [{"date": str(d.date()), "change": round(float(np.exp(v)) - 1, 4)}
                          for d, v in jumps.items()][:20],
        "steps": steps,
    }


def _content(drift: float) -> str:
    if drift > DRIFT_TOL:
        return "adjusted"
    if drift < -DRIFT_TOL:
        return "raw_vs_adjusted_ref"
    return "same"


def label_verdict(node: Node, vs_ibkr: dict | None, vs_yf: list[tuple[Node, dict]]) -> str:
    label = node.adjustment
    if vs_ibkr is not None:
        if abs(vs_ibkr["level"] - 1) > LEVEL_TOL:
            return "level_mismatch"
        content = _content(vs_ibkr["drift"])
        if content == "raw_vs_adjusted_ref":
            return "anomaly"            # IBKR TRADES is never dividend-adjusted
        if label == "adjusted":
            return "ok" if content == "adjusted" else "adjusted_label_no_drift"
        if content == "adjusted":
            return "mislabelled_raw_is_adjusted" if label == "raw" else "anomaly"
        return "ok"
    if not vs_yf:
        return "unverifiable_no_overlap"
    for other, result in vs_yf:
        if abs(result["level"] - 1) > LEVEL_TOL:
            return "level_mismatch"
        content = _content(result["drift"])
        if content == "adjusted":
            return "ok" if label == "adjusted" else "mislabelled_raw_is_adjusted"
        if content == "raw_vs_adjusted_ref":
            return "mislabelled_adjusted_is_raw" if label == "adjusted" else "ok"
    return "unverifiable_yf_agree"


def audit(catalogs: dict[str, dict], data_root: Path, symbols: set[str] | None = None) -> list[dict]:
    """One row per stored daily file: its verdict, comparisons and split jumps."""
    rows: list[dict] = []
    for symbol, nodes in sorted(daily_nodes(catalogs).items()):
        if symbols and symbol not in symbols:
            continue
        files: dict[str, Node] = {}
        for node in nodes:                      # IBKR is stored once, shared by universes
            files.setdefault(node.file_path, node)
        closes = {fp: load_close(resolve(data_root, fp)) for fp in files}
        jumps: dict[str, dict[str, dict]] = defaultdict(dict)
        bad_bars: dict[str, set[str]] = defaultdict(set)
        compared: set[str] = set()

        def run(a: str, b: str) -> dict | None:
            if closes[a] is None or closes[b] is None:
                return None
            result = compare(closes[a], closes[b])
            if result is not None:
                compared.update((a, b))
                for step in result["steps"]:
                    if step.get("bad_bar"):     # owner unknown: listed on the YF row only
                        bad_bars[a].add(step["date"])
                        continue
                    owner = a if step["owner"] == "this" else b
                    jumps[owner].setdefault(step["date"], step)
            return result

        ibkr = [fp for fp, n in files.items() if n.provider == "IBKR"]
        yf = [fp for fp, n in files.items() if n.provider == "YF"]
        for node in nodes:
            if node.provider != "YF":
                continue
            vs_ibkr = next((r for r in (run(node.file_path, fp) for fp in ibkr) if r), None)
            vs_yf = [(files[fp], r) for fp in yf if fp != node.file_path
                     for r in [run(node.file_path, fp)] if r]
            rows.append({
                "catalog": node.catalog, "symbol": symbol, "provider": "YF",
                "file_path": node.file_path, "adjustment": node.adjustment,
                "verdict": label_verdict(node, vs_ibkr, vs_yf if vs_ibkr is None else []),
                "vs_ibkr": vs_ibkr,
                "vs_yf": [{"file_path": o.file_path, **{k: v for k, v in r.items() if k != "steps"}}
                          for o, r in vs_yf],
            })
        for fp in ibkr:
            if not yf:
                continue
            node = files[fp]
            rows.append({
                "catalog": node.catalog, "symbol": symbol, "provider": "IBKR",
                "file_path": fp, "adjustment": node.adjustment, "verdict": None,
            })
        for row in rows:
            if row["symbol"] != symbol or "split" in row:
                continue
            fp = row["file_path"]
            row["split_jumps"] = sorted(jumps[fp].values(), key=lambda s: s["date"])
            row["bad_bar_dates"] = sorted(bad_bars[fp])
            new = {s["date"] for s in row["split_jumps"]} - set(files[fp].flagged)
            row["split"] = ("split_jump" if new
                            else "flagged_unadjusted" if row["split_jumps"]
                            else "no_jump_found" if fp in compared else "unverifiable_no_overlap")
            if row["verdict"] is None:
                row["verdict"] = row["split"]
    return rows


def counts(rows: list[dict]) -> dict[str, dict[str, dict[str, int]]]:
    """catalog → provider → verdict → files. A YF row counts its label verdict and its split result."""
    tally: dict[tuple[str, str], Counter] = defaultdict(Counter)
    for row in rows:
        bucket = tally[(row["catalog"], row["provider"])]
        bucket[row["verdict"]] += 1
        if row["provider"] == "YF":
            bucket[row["split"]] += 1
        if row.get("bad_bar_dates"):
            bucket["has_bad_bar"] += 1
    out: dict[str, dict[str, dict[str, int]]] = defaultdict(dict)
    for (catalog, provider), bucket in sorted(tally.items()):
        out[catalog][provider] = dict(sorted(bucket.items()))
    return dict(out)


def report(rows: list[dict]) -> None:
    print(f"{'catalog':<16} {'provider':<8} verdicts")
    for catalog, by_provider in counts(rows).items():
        for provider, bucket in by_provider.items():
            print(f"{catalog:<16} {provider:<8} " + ", ".join(f"{k} {v}" for k, v in bucket.items()))
    problems = [r for r in rows if r["verdict"] in PROBLEMS or r.get("split") == "split_jump"]
    if problems:
        print(f"\n{len(problems)} file(s) with findings (first 40):")
        for row in problems[:40]:
            jumps = " ".join(f"{s['date']} {s['factor'] or s['move']}" for s in row["split_jumps"])
            print(f"  {row['verdict']:<28} {row['file_path']}  {jumps}")


def main() -> int:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--catalog", nargs="+", choices=CATALOGS, default=CATALOGS)
    p.add_argument("--symbols", nargs="+", help="Only these symbols (bare names, e.g. AAPL GSPC)")
    p.add_argument("--json", action="store_true", help="Counts and the per-file list as JSON")
    args = p.parse_args()

    specs = {s.name: s for s in catalog_registry()}
    catalogs = {name: specs[name].load() for name in args.catalog}
    rows = audit(catalogs, DATA_ROOT, set(args.symbols) if args.symbols else None)
    if args.json:
        print(json.dumps({"counts": counts(rows), "files": rows}, indent=1))
    else:
        report(rows)
    return 1 if any(r["verdict"] in PROBLEMS or r.get("split") == "split_jump" for r in rows) else 0


if __name__ == "__main__":
    sys.exit(main())
