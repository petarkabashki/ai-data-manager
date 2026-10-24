#!/usr/bin/env -S uv run python
"""Retry failed IBKR stock downloads sequentially, one process at a time.

Reads the failure list from the progress files
`tools/download_ibkr_stock_overnight.py` actually writes — one per
(session, timeframe-set) — and re-runs the downloader for just those symbols,
one timeframe at a time. Never in parallel: IBKR's historical-data pacing
limiter has caused both burst lockouts and multi-hour dead connections under
concurrent load.

  uv run python tools/retry_ibkr_failed_sequentially.py --universe nasdaq100 --timeframes 4h 1h
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config import IBKR_CLIENT_ID, IBKR_PORT, REPO_ROOT, TMP_ROOT

DOWNLOADER = REPO_ROOT / "tools" / "download_ibkr_stock_overnight.py"


def _progress_candidates(timeframe: str, session: str) -> list[Path]:
    """Progress files that might hold failures for this timeframe.

    A run covering several timeframes writes one combined file, so look for the
    single-timeframe file first, then any file whose name includes it.
    """
    exact = TMP_ROOT / f"ibkr_stock_progress_{session}_{timeframe}.json"
    candidates = [exact] if exact.exists() else []
    candidates.extend(
        sorted(
            path
            for path in TMP_ROOT.glob(f"ibkr_stock_progress_{session}_*.json")
            if path not in candidates and timeframe in path.stem.split("_")[-1].split("-")
        )
    )
    legacy = TMP_ROOT / "ibkr_stock_download_progress.json"
    if legacy.exists():
        candidates.append(legacy)
    return candidates


def load_failed_symbols(timeframe: str, session: str, universe: str) -> list[str]:
    seen: list[str] = []
    for progress_file in _progress_candidates(timeframe, session):
        try:
            payload = json.loads(progress_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"  [warn] could not read {progress_file.name}: {exc}")
            continue
        for item in payload.get("failures", []):
            if item.get("universe") != universe:
                continue
            symbol = item.get("symbol")
            if symbol and symbol not in seen:
                seen.append(symbol)
    return seen


def run_retry(timeframe: str, symbols: list[str], args: argparse.Namespace, client_id: int) -> int:
    if not symbols:
        print(f"[{timeframe}] no failed symbols to retry")
        return 0

    cmd = [
        "uv", "run", "python", str(DOWNLOADER),
        "--universe", args.universe,
        "--symbols", *symbols,
        "--timeframes", timeframe,
        "--since", args.since,
        "--session", args.session,
        "--port", str(args.port),
        "--client-id", str(client_id),
        "--sleep-seconds", str(args.sleep_seconds),
        "--request-timeout", str(args.request_timeout),
        "--max-chunk-retries", str(args.max_chunk_retries),
        "--resume-log", str(TMP_ROOT / f"ibkr_retry_{timeframe}_resume.jsonl"),
        "--progress-file", str(TMP_ROOT / f"ibkr_retry_{timeframe}_progress.json"),
    ]
    print(f"\n[{timeframe}] retrying {len(symbols)} symbols: {', '.join(symbols)}")
    if args.dry_run:
        print("  [dry-run] " + " ".join(cmd))
        return 0
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--universe", default="nasdaq100", choices=["sp500", "nasdaq100"],
                   help="Universe whose failures to retry (default: nasdaq100)")
    p.add_argument("--timeframes", nargs="+", default=["4h", "1h"],
                   choices=["1h", "4h", "8h", "1d", "1w"],
                   help="Retry these timeframes' failed symbols, sequentially in the given order")
    p.add_argument("--session", default="all", choices=["all", "overnight"],
                   help="Session of the original run (selects which progress files to read)")
    p.add_argument("--port", type=int, default=IBKR_PORT, help="IBKR TWS/Gateway port (env: IBKR_PORT)")
    p.add_argument("--base-client-id", type=int, default=IBKR_CLIENT_ID + 20,
                   help="First client id; each timeframe uses the next one up")
    p.add_argument("--since", default="2020-01-01")
    p.add_argument("--sleep-seconds", type=float, default=1.0)
    p.add_argument("--request-timeout", type=float, default=25.0)
    p.add_argument("--max-chunk-retries", type=int, default=2)
    p.add_argument("--dry-run", action="store_true", help="Print the commands without running them")
    args = p.parse_args()

    for idx, timeframe in enumerate(args.timeframes):
        symbols = load_failed_symbols(timeframe, args.session, args.universe)
        rc = run_retry(timeframe, symbols, args, args.base_client_id + idx)
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
