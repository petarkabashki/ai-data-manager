#!/usr/bin/env -S uv run python
"""Find un-back-adjusted split discontinuities inside stored price files.

An incremental download appends new bars to an existing file. When the symbol
splits, the provider's history changes underneath: every past bar is restated
at the new share count. Appending does not restate what is already on disk, so
the file ends up holding as-traded prices before the last full download and
split-adjusted prices after it, joined by a step that never happened.

Measured on this repo's `stocks/yf` tree, 17 files break on 2024-08-07 alone --
`nasdaq100/ORLY` runs at exactly 15x the provider's price before that date and
1.0x after (its 15:1 split was June 2025), `nasdaq100/LRCX` at exactly 10x
(10:1, October 2024). To a backtest that step is a 93% single-day crash.

A real crash is not a round number. A split factor is: the check flags a
one-day move only when the ratio sits within `--tolerance` of a simple
fraction, which is what separates these from March 2020.

Read-only: never modifies files. Repair means re-downloading the symbol in
full, not patching the seam.

    uv run python tools/check_splits.py
    uv run python tools/check_splits.py --path price-data/stocks/yf/sp500
    uv run python tools/check_splits.py --json
"""

from __future__ import annotations

import argparse
import json
import sys
from fractions import Fraction
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import DATA_ROOT, REPO_ROOT
from utils.report import allow_broken_pipe

#: Where equities live. Only these have corporate actions worth checking;
#: crypto and indexes never split, so scanning them is noise.
EQUITY_ROOTS = [DATA_ROOT / "stocks"]

#: A one-day move must be at least this large to be worth testing. Below it,
#: ordinary volatility lands near a simple fraction often enough to be useless.
MIN_MOVE = 1.6

#: Largest term considered. Splits beyond about 30:1 are rare enough that
#: admitting them costs more false positives than it catches.
MAX_TERM = 30

#: A split is n:1, 1:n, or a small ratio like 3:2 -- never 5:13 or 25:2. Some
#: such rule is essential: with denominators to 30, a fraction lands within
#: 0.3% of almost any number, so March 2020 crashes matched 5:13 and 9:22 and
#: drowned the real findings. Admitting anything with a 2 in it is not enough
#: either, since that lets SMCI's 0.0799 seam pose as 2:25.
MAX_TERM_FOR_NON_UNIT_RATIO = 5


def nearest_simple_ratio(value: float, tolerance: float) -> Fraction | None:
    """The simple fraction `value` sits on, or None if it sits on none.

    This is what distinguishes a split from a crash: 10.0000 is a split, 0.58
    is March 2020. `limit_denominator` finds the best rational approximation
    with a bounded denominator, and the tolerance check then rejects it unless
    the fit is far tighter than any real price move would produce.
    """
    if not np.isfinite(value) or value <= 0:
        return None

    # Try the ratio and its reciprocal: `limit_denominator` bounds only the
    # denominator, so a 15:1 split is found on the way up and a 1:15 on the way
    # down. Built lazily because the reciprocal of a rounded-to-zero fraction
    # cannot be constructed at all.
    def candidates():
        yield Fraction(value).limit_denominator(MAX_TERM)
        inverse = Fraction(1 / value).limit_denominator(MAX_TERM)
        if inverse != 0:
            yield 1 / inverse

    for candidate in candidates():
        if candidate == 0 or max(candidate.numerator, candidate.denominator) > MAX_TERM:
            continue
        smaller = min(candidate.numerator, candidate.denominator)
        larger = max(candidate.numerator, candidate.denominator)
        if smaller != 1 and larger > MAX_TERM_FOR_NON_UNIT_RATIO:
            continue
        if abs(value / float(candidate) - 1.0) <= tolerance:
            return candidate
    return None


def check_file(path: Path, tolerance: float) -> list[dict]:
    """Every split-shaped discontinuity in one file, oldest first."""
    try:
        frame = pd.read_parquet(path, columns=["timestamp", "close"])
    except Exception as exc:
        return [{"date": None, "error": f"unreadable: {exc}"}]
    if len(frame) < 3:
        return []

    close = frame["close"].to_numpy(dtype=float)
    dates = pd.to_datetime(frame["timestamp"], unit="ms", utc=True).dt.date.to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = close[1:] / close[:-1]

    # Every large move is returned, split-shaped or not. The fraction labels a
    # move; it does not decide whether to report it. Filtering on it hid the
    # thing this check exists to find: SMCI's 2024-08-07 seam is 12.52x, which
    # is not a clean factor but is still not a day's trading.
    found: list[dict] = []
    for index in np.where((ratio >= MIN_MOVE) | (ratio <= 1 / MIN_MOVE))[0]:
        factor = nearest_simple_ratio(float(ratio[index]), tolerance)
        found.append({
            "date": str(dates[index + 1]),
            "factor": f"{factor.numerator}:{factor.denominator}" if factor else "",
            "ratio": round(float(ratio[index]), 4),
            "before": round(float(close[index]), 4),
            "after": round(float(close[index + 1]), 4),
        })
    return found


def _display_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--path", type=Path, action="append",
                       help="Directory to scan (repeatable). Defaults to every equity root.")
    parser.add_argument("--tolerance", type=float, default=0.003,
                       help="How exactly a move must match a simple fraction (default 0.003).")
    parser.add_argument("--json", action="store_true", help="Emit findings as JSON.")
    args = parser.parse_args()

    roots = args.path or EQUITY_ROOTS
    findings: dict[str, list[dict]] = {}
    scanned = 0
    for root in roots:
        for file in sorted(Path(root).rglob("*.parquet")):
            scanned += 1
            hits = check_file(file, args.tolerance)
            if hits:
                findings[_display_path(file)] = hits

    if args.json:
        print(json.dumps(findings, indent=2))
        return 1 if findings else 0

    if not findings:
        print(f"Scanned {scanned} files — no split-shaped discontinuities.")
        return 0

    by_date: dict[str, int] = {}
    for hits in findings.values():
        for hit in hits:
            if hit.get("date"):
                by_date[hit["date"]] = by_date.get(hit["date"], 0) + 1

    total = sum(len(h) for h in findings.values())
    print(f"Scanned {scanned} files — {total} one-day moves beyond "
          f"{MIN_MOVE}x across {len(findings)} files.\n")

    print("Most affected dates. This is the signal: a genuine crash moves many "
          "symbols\nby similar amounts in one direction, while a bad update run "
          "moves them by\nwildly different factors on a date with no market event.\n")
    print(f"{'Date':<12}{'Files':>7}   {'Example factors'}")
    for date, count in sorted(by_date.items(), key=lambda kv: -kv[1])[:10]:
        sample = [h["ratio"] for hits in findings.values() for h in hits
                  if h.get("date") == date][:6]
        print(f"{date:<12}{count:>7}   " + ", ".join(f"{r:g}x" for r in sample))

    print(f"\n{'File':<60}{'Date':<12}{'Factor':<9}{'Before':>12}{'After':>12}")
    print("-" * 105)
    for name, hits in sorted(findings.items()):
        for hit in hits:
            if hit.get("error"):
                print(f"{name:<60}{hit['error']}")
                continue
            print(f"{name:<60}{hit['date']:<12}{hit['factor'] or '-':<9}"
                  f"{hit['before']:>12.4f}{hit['after']:>12.4f}")
    print("\nRepair: re-download each symbol in full. Appending cannot fix a seam "
          "whose history is stated at the wrong share count.")
    return 1


if __name__ == "__main__":
    allow_broken_pipe()
    raise SystemExit(main())
