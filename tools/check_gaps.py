#!/usr/bin/env -S uv run python
"""Audit existing crypto data for gaps and staleness.

Usage examples:
  uv run python tools/check_gaps.py --exchange binance --timeframe 1d
  uv run python tools/check_gaps.py --exchange binance --symbol BTC_USDT
  uv run python tools/check_gaps.py --exchange binance --timeframe 4h --stale-days 7
  uv run python tools/check_gaps.py --all                # all exchanges, 1d only
  uv run python tools/check_gaps.py --exchange binance --internal   # show internal gaps too
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import CRYPTO_EXCHANGES, load_crypto_catalog
from utils.gaps import SymbolAudit, audit_exchange


def _fmt_row(cols: list, widths: list[int]) -> str:
    return "  ".join(str(c).ljust(w) for c, w in zip(cols, widths))


def _print_table(headers: list[str], rows: list[list]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    print(_fmt_row(headers, widths))
    print("  ".join("-" * w for w in widths))
    for row in rows:
        print(_fmt_row(row, widths))


def report(
    audits: list[SymbolAudit],
    show_internal: bool = False,
    min_stale_days: int = 1,
) -> None:
    errors = [a for a in audits if a.error]
    stale = [a for a in audits if not a.error and a.trailing_days >= min_stale_days]
    with_internal = [a for a in audits if not a.error and a.internal_gaps]
    ok = [a for a in audits if a.ok]

    print(f"\n{'─'*60}")
    print(f"  Total audited : {len(audits)}")
    print(f"  OK            : {len(ok)}")
    print(f"  Stale (≥{min_stale_days}d)  : {len(stale)}")
    print(f"  Internal gaps : {len(with_internal)}")
    print(f"  Errors        : {len(errors)}")
    print(f"{'─'*60}\n")

    if errors:
        print("ERRORS (missing or unreadable files):")
        rows = [[a.exchange, a.symbol, a.timeframe, a.error] for a in errors]
        _print_table(["Exchange", "Symbol", "TF", "Error"], rows)
        print()

    if stale:
        print(f"STALE DATA (≥{min_stale_days}d behind) — sorted by staleness:")
        rows = []
        for a in sorted(stale, key=lambda x: -x.trailing_days):
            rows.append([
                a.exchange,
                a.symbol,
                a.timeframe,
                a.end_date or "—",
                f"{a.trailing_days:.0f}d",
                str(a.trailing_candles),
            ])
        _print_table(["Exchange", "Symbol", "TF", "Last Date", "Stale", "Missing Candles"], rows)
        print()

    if show_internal and with_internal:
        print("INTERNAL GAPS:")
        for a in with_internal:
            total_missing = sum(g.missing_candles for g in a.internal_gaps)
            print(f"  {a.exchange}/{a.symbol}/{a.timeframe} — {len(a.internal_gaps)} gap(s), {total_missing} candles missing")
            for g in a.internal_gaps:
                print(f"    {g}")
        print()
    elif with_internal and not show_internal:
        print(f"INTERNAL GAPS: {len(with_internal)} series affected (use --internal for details)\n")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", choices=CRYPTO_EXCHANGES, help="Exchange to audit")
    p.add_argument("--symbol", "-s", help="Single symbol (e.g. BTC_USDT)")
    p.add_argument("--timeframe", "-t", help="Single timeframe (e.g. 1d)")
    p.add_argument("--timeframes", nargs="+", help="Multiple timeframes")
    p.add_argument("--all", action="store_true", help="Audit all exchanges (1d only by default)")
    p.add_argument("--internal", action="store_true", help="Show internal gap details")
    p.add_argument("--stale-days", type=int, default=1, metavar="N",
                   help="Minimum staleness to report (default: 1)")
    args = p.parse_args()

    exchanges = CRYPTO_EXCHANGES if args.all else ([args.exchange] if args.exchange else None)
    if not exchanges:
        p.error("Specify --exchange or --all")

    timeframes: list[str] | None = None
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = args.timeframes
    elif args.all:
        timeframes = ["1d"]

    symbols = [args.symbol] if args.symbol else None

    all_audits: list[SymbolAudit] = []
    for ex in exchanges:
        print(f"Auditing {ex}…", end=" ", flush=True)
        audits = audit_exchange(ex, timeframes=timeframes, symbols=symbols)
        print(f"{len(audits)} series")
        all_audits.extend(audits)

    report(all_audits, show_internal=args.internal, min_stale_days=args.stale_days)


if __name__ == "__main__":
    main()
