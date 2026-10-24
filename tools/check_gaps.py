#!/usr/bin/env -S uv run python
"""Audit stored series for gaps and staleness, against the right trading calendar.

Crypto trades 24/7, so any missing bar is a gap. Everything else has a trading
calendar, and judging it by the 24/7 rule reports every weekend and market
holiday as a hole — which is why this tool used to cover crypto only.

Usage examples:
  uv run python tools/check_gaps.py --exchange binance --timeframe 1d
  uv run python tools/check_gaps.py --all                       # all crypto exchanges, 1d
  uv run python tools/check_gaps.py --catalog sp500 --timeframe 1d
  uv run python tools/check_gaps.py --catalog indexes --internal
  uv run python tools/check_gaps.py --catalog sp500 --calendar us_equity_extended
  uv run python tools/check_gaps.py --exchange binance --trading-mode futures
  uv run python tools/check_gaps.py --catalog sp500 --json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.calendars import CALENDAR_NAMES, calendar_by_name
from utils.catalog import CRYPTO_EXCHANGES, catalog_registry
from utils.gaps import SymbolAudit, audit_catalog_series, audit_exchange
from utils.report import allow_broken_pipe, print_table


def report(
    audits: list[SymbolAudit],
    show_internal: bool = False,
    min_stale_days: int = 1,
) -> None:
    errors = [a for a in audits if a.error]
    stale = [a for a in audits if not a.error and a.trailing_days >= min_stale_days]
    with_internal = [a for a in audits if not a.error and a.internal_gaps]
    ok = [a for a in audits if a.ok]
    calendars = sorted({a.calendar for a in audits})

    print(f"\n{'─'*60}")
    print(f"  Total audited : {len(audits)}")
    print(f"  OK            : {len(ok)}")
    print(f"  Stale (≥{min_stale_days}d)  : {len(stale)}")
    print(f"  Internal gaps : {len(with_internal)}")
    print(f"  Errors        : {len(errors)}")
    print(f"  Calendar(s)   : {', '.join(calendars) or '—'}")
    print(f"{'─'*60}\n")

    if errors:
        print("ERRORS (missing or unreadable files):")
        print_table(
            ["Source", "Symbol", "TF", "Error"],
            [[a.exchange, a.symbol, a.timeframe, a.error] for a in errors],
        )
        print()

    if stale:
        print(f"STALE DATA (≥{min_stale_days}d behind) — sorted by staleness:")
        print_table(
            ["Source", "Symbol", "TF", "Last Date", "Stale", "Missing Candles"],
            [
                [a.exchange, a.symbol, a.timeframe, a.end_date or "—",
                 f"{a.trailing_days:.0f}d", str(a.trailing_candles)]
                for a in sorted(stale, key=lambda x: -x.trailing_days)
            ],
        )
        print()

    if show_internal and with_internal:
        print("INTERNAL GAPS:")
        for a in with_internal:
            total_missing = sum(g.missing_candles for g in a.internal_gaps)
            print(f"  {a.exchange}/{a.symbol}/{a.timeframe} [{a.calendar}] — "
                  f"{len(a.internal_gaps)} gap(s), {total_missing} candles missing")
            for g in a.internal_gaps:
                print(f"    {g}")
        print()
    elif with_internal and not show_internal:
        print(f"INTERNAL GAPS: {len(with_internal)} series affected (use --internal for details)\n")


def emit_json(audits: list[SymbolAudit], min_stale_days: int) -> None:
    json.dump(
        {
            "audited": len(audits),
            "series": [
                {
                    "source": a.exchange,
                    "symbol": a.symbol,
                    "provider": a.provider,
                    "timeframe": a.timeframe,
                    "calendar": a.calendar,
                    "start_date": a.start_date,
                    "end_date": a.end_date,
                    "rows": a.row_count,
                    "trailing_candles": a.trailing_candles,
                    "trailing_days": round(a.trailing_days, 2),
                    "stale": a.trailing_days >= min_stale_days,
                    "internal_gaps": [
                        {"start": g.start, "end": g.end, "missing_candles": g.missing_candles}
                        for g in a.internal_gaps
                    ],
                    "error": a.error,
                    "ok": a.ok,
                }
                for a in audits
            ],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")


def main() -> None:
    allow_broken_pipe()
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--exchange", "-e", choices=CRYPTO_EXCHANGES, help="Crypto exchange to audit")
    p.add_argument("--catalog", nargs="+",
                   help="Audit these catalogs by name (sp500, indexes, commodities, crypto_binance…)")
    p.add_argument("--symbol", "-s", help="Single symbol (e.g. BTC_USDT)")
    p.add_argument("--symbols", nargs="+", help="Multiple symbols")
    p.add_argument("--timeframe", "-t", help="Single timeframe (e.g. 1d)")
    p.add_argument("--timeframes", nargs="+", help="Multiple timeframes")
    p.add_argument("--all", action="store_true", help="Audit all crypto exchanges (1d only by default)")
    p.add_argument("--all-catalogs", action="store_true", help="Audit every catalog in the repo")
    p.add_argument("--internal", action="store_true", help="Show internal gap details")
    p.add_argument("--stale-days", type=int, default=1, metavar="N",
                   help="Minimum staleness to report (default: 1)")
    p.add_argument("--trading-mode", default="spot", choices=["spot", "futures"],
                   help="Crypto trading mode for --exchange/--all (default: spot)")
    p.add_argument("--calendar", choices=CALENDAR_NAMES,
                   help="Override the calendar instead of deriving it from the asset class")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = p.parse_args()

    if not (args.exchange or args.all or args.catalog or args.all_catalogs):
        p.error("Specify --exchange, --all, --catalog or --all-catalogs")

    timeframes: list[str] | None = None
    if args.timeframe:
        timeframes = [args.timeframe]
    elif args.timeframes:
        timeframes = args.timeframes
    elif args.all:
        timeframes = ["1d"]

    symbols = [args.symbol] if args.symbol else args.symbols
    calendar_override = calendar_by_name(args.calendar) if args.calendar else None

    all_audits: list[SymbolAudit] = []

    # Crypto path: keeps the exchange-shaped CLI and futures support.
    exchanges = CRYPTO_EXCHANGES if args.all else ([args.exchange] if args.exchange else [])
    for exchange in exchanges:
        if not args.json:
            print(f"Auditing {exchange}…", end=" ", flush=True)
        audits = audit_exchange(
            exchange, timeframes=timeframes, symbols=symbols, trading_mode=args.trading_mode
        )
        if not args.json:
            print(f"{len(audits)} series")
        all_audits.extend(audits)

    # Catalog path: every asset class, each with its own calendar.
    specs = catalog_registry()
    if args.all_catalogs:
        wanted = specs
    elif args.catalog:
        names = set(args.catalog)
        unknown = names - {s.name for s in specs}
        if unknown:
            p.error(f"unknown catalog(s): {', '.join(sorted(unknown))}")
        wanted = [s for s in specs if s.name in names]
    else:
        wanted = []

    for spec in wanted:
        if not args.json:
            print(f"Auditing {spec.name}…", end=" ", flush=True)
        audits = audit_catalog_series(
            spec, timeframes=timeframes, symbols=symbols, calendar_override=calendar_override
        )
        if not args.json:
            print(f"{len(audits)} series")
        all_audits.extend(audits)

    if args.json:
        emit_json(all_audits, args.stale_days)
    else:
        report(all_audits, show_internal=args.internal, min_stale_days=args.stale_days)


if __name__ == "__main__":
    main()
