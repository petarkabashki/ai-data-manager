#!/usr/bin/env -S uv run python
"""Retry failed IBKR stock downloads sequentially, one process at a time."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOWNLOADER = REPO_ROOT / "tools" / "download_ibkr_stock_overnight.py"


def _load_failed_symbols(progress_file: Path, universe: str) -> list[str]:
    if not progress_file.exists():
        return []
    payload = json.loads(progress_file.read_text(encoding="utf-8"))
    seen: list[str] = []
    for item in payload.get("failures", []):
        if item.get("universe") != universe:
            continue
        symbol = item.get("symbol")
        if symbol and symbol not in seen:
            seen.append(symbol)
    return seen


def _run_retry(
    timeframe: str,
    symbols: list[str],
    port: int,
    client_id: int,
    sleep_seconds: float,
    since: str,
    request_timeout: float,
    max_chunk_retries: int,
) -> int:
    if not symbols:
        print(f"[{timeframe}] no failed symbols to retry")
        return 0

    progress_file = REPO_ROOT / "tmp" / f"ibkr_retry_{timeframe}_progress.json"
    resume_log = REPO_ROOT / "tmp" / f"ibkr_retry_{timeframe}_resume.jsonl"
    cmd = [
        "uv",
        "run",
        "python",
        str(DOWNLOADER),
        "--universe",
        "nasdaq100",
        "--symbols",
        *symbols,
        "--timeframes",
        timeframe,
        "--since",
        since,
        "--session",
        "all",
        "--port",
        str(port),
        "--client-id",
        str(client_id),
        "--sleep-seconds",
        str(sleep_seconds),
        "--request-timeout",
        str(request_timeout),
        "--max-chunk-retries",
        str(max_chunk_retries),
        "--resume-log",
        str(resume_log),
        "--progress-file",
        str(progress_file),
    ]
    print(f"\n[{timeframe}] retrying {len(symbols)} symbols: {', '.join(symbols)}")
    return subprocess.run(cmd, cwd=REPO_ROOT).returncode


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--port", type=int, default=4001)
    p.add_argument("--since", default="2020-01-01")
    p.add_argument("--base-client-id", type=int, default=90)
    p.add_argument("--sleep-seconds", type=float, default=1.0)
    p.add_argument("--request-timeout", type=float, default=25.0)
    p.add_argument("--max-chunk-retries", type=int, default=2)
    p.add_argument(
        "--timeframes",
        nargs="+",
        choices=["4h", "1h"],
        default=["4h", "1h"],
        help="Retry only the selected failed timeframe sets, sequentially in the given order",
    )
    args = p.parse_args()

    symbols_by_timeframe = {
        "4h": _load_failed_symbols(REPO_ROOT / "tmp" / "ibkr_stock_download_progress_4h.json", "nasdaq100"),
        "1h": _load_failed_symbols(REPO_ROOT / "tmp" / "ibkr_stock_download_progress_1h.json", "nasdaq100"),
    }

    for idx, timeframe in enumerate(args.timeframes):
        rc = _run_retry(
            timeframe,
            symbols_by_timeframe[timeframe],
            args.port,
            args.base_client_id + idx,
            args.sleep_seconds,
            args.since,
            args.request_timeout,
            args.max_chunk_retries,
        )
        if rc != 0:
            return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
