#!/usr/bin/env -S uv run python
"""Find bars that one crypto venue's file shares, to the digit, with another venue's.

Two venues never print the same close *and* the same volume for three bars
in a row: the trades behind them are different. A run of identical bars means one
file holds bars copied from the other venue's feed, a provenance fault that any
cross-venue agreement check would read as perfect agreement (see ROADMAP E15,
"bars from another venue").

The scan groups every spot and perps file by (symbol, timeframe), joins each
venue pair on timestamp and counts bars whose close and volume are equal
(volume > 0, so flat untraded stretches don't count). A pair with that many
such bars is a twin block when 3 of them are consecutive (`--min-run`) or 24
are scattered (`--min-bars`), at every timeframe. The run rule replaced a flat
24 total on 2026-11-14, when 1-8 copied daily and weekly bars per file were
found below it. Its control, the 2026-11-14 sweep of every venue pair at
15m-1w (2.8e8 compared bars): isolated equal bars occur about once per 1e7
compared bars at 1h and never at 8h-3d, so chance runs of 3 are expected
about 1e-12 times over the whole store; every scattered chance hit seen had a
longest run of 1.

Blocks already recorded in `twin_blocks.yaml` at the data root are known and
reported separately; the tool exits 1 only when a block is new or has grown
beyond its recorded bar count. `--record` rewrites that file from the current
scan, which is the way to acknowledge the blocks found so far (a block is
cleared by deleting its bars and refilling from the venue, not by recording it).

Usage examples:
  uv run python tools/check_twins.py                     # every venue pair, 15m-1w
  uv run python tools/check_twins.py --timeframes 1d 1w
  uv run python tools/check_twins.py --symbol BTC_USDT --json
  uv run python tools/check_twins.py --record            # acknowledge current blocks
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.catalog import CRYPTO_EXCHANGES  # noqa: E402
from utils.config import DATA_ROOT  # noqa: E402
from utils.report import allow_broken_pipe, print_table  # noqa: E402

#: Venue classes scanned: directory under the data root → label suffix.
CLASSES = {"cryptos": "spot", "crypto-futures": "perps"}
DEFAULT_TIMEFRAMES = ["15m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w"]
#: Consecutive identical bars that make a twin block.
DEFAULT_MIN_RUN = 3
#: Identical bars, consecutive or not, that make a twin block.
DEFAULT_MIN_BARS = 24

RECORD_NAME = "twin_blocks.yaml"


@dataclass(frozen=True)
class TwinBlock:
    """Bars one series shares, close and volume equal, with another venue's series."""

    symbol: str
    timeframe: str
    a: str  # e.g. "binance-perps"
    b: str  # e.g. "kucoin-perps"
    bars: int
    run: int  # longest run of consecutive shared bars
    first: str  # UTC date of the first shared bar
    last: str  # UTC date of the last shared bar

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.symbol, self.timeframe, self.a, self.b)


def _date(ms: int) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def series_files(
    data_root: Path, timeframes: list[str], symbols: set[str] | None = None
) -> dict[tuple[str, str], dict[str, Path]]:
    """(symbol, timeframe) → {venue-class: file} over every spot and perps venue."""
    groups: dict[tuple[str, str], dict[str, Path]] = defaultdict(dict)
    wanted = set(timeframes)
    for subdir, cls in CLASSES.items():
        for venue in CRYPTO_EXCHANGES:
            base = data_root / subdir / venue
            if not base.is_dir():
                continue
            for file in base.glob("*/*.parquet"):
                stem = file.name[: -len(".parquet")]
                if "-" not in stem:
                    continue
                symbol, timeframe = stem.rsplit("-", 1)
                if timeframe not in wanted or (symbols and symbol not in symbols):
                    continue
                groups[(symbol, timeframe)][f"{venue}-{cls}"] = file
    return {key: files for key, files in groups.items() if len(files) > 1}


def _traded_bars(file: Path) -> pd.DataFrame:
    frame = pq.read_table(file, columns=["timestamp", "close", "volume"]).to_pandas()
    frame = frame[frame["volume"] > 0]
    return frame.set_index("timestamp")


def longest_run(flags: pd.Series) -> int:
    """Longest stretch of consecutive True values."""
    if not flags.any():
        return 0
    groups = (flags != flags.shift()).cumsum()
    return int(flags.groupby(groups).sum().max())


def twin_blocks(
    files: dict[str, Path], symbol: str, timeframe: str,
    min_run: int = DEFAULT_MIN_RUN, min_bars: int = DEFAULT_MIN_BARS,
) -> list[TwinBlock]:
    """Twin blocks among one (symbol, timeframe) group's venue files."""
    data = {label: _traded_bars(file) for label, file in files.items()}
    found = []
    for a, b in itertools.combinations(sorted(data), 2):
        joined = data[a].join(data[b], rsuffix="_b", how="inner")
        if joined.empty:
            continue
        equal = (joined["close"] == joined["close_b"]) & (joined["volume"] == joined["volume_b"])
        bars, run = int(equal.sum()), longest_run(equal)
        if run < min_run and bars < min_bars:
            continue
        stamps = joined.index[equal]
        found.append(TwinBlock(symbol, timeframe, a, b, bars, run, _date(stamps.min()), _date(stamps.max())))
    return found


def scan(
    data_root: Path,
    timeframes: list[str],
    min_bars: int = DEFAULT_MIN_BARS,
    symbols: set[str] | None = None,
    min_run: int = DEFAULT_MIN_RUN,
) -> tuple[list[TwinBlock], int]:
    """Every twin block under the data root, and how many files were compared."""
    blocks: list[TwinBlock] = []
    files_read = 0
    for (symbol, timeframe), files in sorted(series_files(data_root, timeframes, symbols).items()):
        files_read += len(files)
        blocks.extend(twin_blocks(files, symbol, timeframe, min_run, min_bars))
    return blocks, files_read


def record_path(data_root: Path) -> Path:
    return data_root / RECORD_NAME


def load_known(path: Path) -> dict[tuple[str, str, str, str], int]:
    """Known blocks → recorded bar count."""
    if not path.exists():
        return {}
    doc = yaml.safe_load(path.read_text()) or {}
    known = {}
    for row in doc.get("blocks") or []:
        known[(row["symbol"], row["timeframe"], row["a"], row["b"])] = int(row["bars"])
    return known


def save_known(path: Path, blocks: list[TwinBlock]) -> None:
    doc = {
        "recorded": datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": "Blocks of bars identical (close and volume) across venues, acknowledged "
                "by check_twins --record. Clearing one means deleting its bars and refilling "
                "from the venue, then recording again.",
        "blocks": [asdict(b) for b in sorted(blocks, key=lambda b: b.key)],
    }
    path.write_text(yaml.safe_dump(doc, sort_keys=False))


def split_known(
    blocks: list[TwinBlock], known: dict[tuple[str, str, str, str], int]
) -> tuple[list[TwinBlock], list[TwinBlock]]:
    """(new or grown, known) — a block is known while its bar count has not grown."""
    new, old = [], []
    for block in blocks:
        recorded = known.get(block.key)
        (old if recorded is not None and block.bars <= recorded else new).append(block)
    return new, old


def _rows(blocks: list[TwinBlock]) -> list[list]:
    return [[b.symbol, b.timeframe, b.a, b.b, b.bars, b.run, b.first, b.last]
            for b in sorted(blocks, key=lambda b: (-b.bars, b.key))]


def report(new: list[TwinBlock], old: list[TwinBlock], files_read: int, min_run: int, min_bars: int) -> None:
    headers = ["symbol", "tf", "series", "twin", "bars", "run", "first", "last"]
    print(f"Compared {files_read} files; twin block = bars equal in close and volume, "
          f"{min_run}+ in a row or {min_bars}+ in all.")
    if new:
        print(f"\nNEW twin blocks ({len(new)}, {sum(b.bars for b in new)} bars):")
        print_table(headers, _rows(new))
    if old:
        print(f"\nKnown twin blocks ({len(old)}, {sum(b.bars for b in old)} bars, in {RECORD_NAME}):")
        print_table(headers, _rows(old))
    if not new and not old:
        print("No twin blocks.")
    elif not new:
        print("\nNo new twin blocks.")
    else:
        print(f"\n{len(new)} new twin block(s): bars from another venue, or a known block grew.")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--timeframes", nargs="+", default=DEFAULT_TIMEFRAMES, metavar="TF",
                   help=f"Timeframes to compare (default: {' '.join(DEFAULT_TIMEFRAMES)})")
    p.add_argument("--symbol", "-s", help="Single symbol (e.g. BTC_USDT)")
    p.add_argument("--symbols", nargs="+", help="Multiple symbols")
    p.add_argument("--min-run", type=int, default=DEFAULT_MIN_RUN, metavar="N",
                   help=f"Consecutive identical bars that make a twin block (default {DEFAULT_MIN_RUN})")
    p.add_argument("--min-bars", type=int, default=DEFAULT_MIN_BARS, metavar="N",
                   help=f"Identical bars in all that make a twin block (default {DEFAULT_MIN_BARS})")
    p.add_argument("--record", action="store_true",
                   help=f"Rewrite {RECORD_NAME} at the data root from this scan's blocks")
    p.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = p.parse_args()

    symbols = set(args.symbols or ([args.symbol] if args.symbol else [])) or None
    blocks, files_read = scan(DATA_ROOT, args.timeframes, args.min_bars, symbols, args.min_run)
    known = load_known(record_path(DATA_ROOT))
    new, old = split_known(blocks, known)

    if args.record:
        if symbols:
            p.error("--record needs a whole scan: drop --symbol/--symbols")
        save_known(record_path(DATA_ROOT), blocks)

    if args.json:
        print(json.dumps({
            "files": files_read,
            "min_run": args.min_run,
            "min_bars": args.min_bars,
            "new": [asdict(b) for b in sorted(new, key=lambda b: b.key)],
            "known": [asdict(b) for b in sorted(old, key=lambda b: b.key)],
        }, indent=2))
    else:
        report(new, old, files_read, args.min_run, args.min_bars)
        if args.record:
            print(f"Recorded {len(blocks)} block(s) in {record_path(DATA_ROOT)}")
    return 0 if args.record or not new else 1


if __name__ == "__main__":
    allow_broken_pipe()
    sys.exit(main())
