"""Wire format shared by server and client.

Every message is a ZeroMQ multipart message whose control frame is UTF-8 JSON
and whose optional last frame is a table, encoded as an Arrow IPC stream
(zstd-compressed). Bars therefore cross the wire in the canonical schema —
`timestamp` int64 ms + OHLCV float64 — with no lossy text round-trip.

Request  (DEALER → ROUTER): [header]
    header = {"id": str, "method": str, "params": {...}, "token": str?}
Reply    (ROUTER → DEALER): [header, payload?]
    header = {"id": str, "ok": bool, "result": ..., "error": str?,
              "payload": "arrow" | null}
Broadcast (PUB): [topic, header, payload?]
    header = {"type": "bars" | "job" | "live", ...}

Topics are `/`-separated, never `.`: symbols such as `BRK.B` contain dots,
and a dotted topic would make the prefix `bar/stocks/yf/sp500/BRK` match
`BRK.B` too. Subscribe with a trailing `/` to select exactly one node.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import pandas as pd
import pyarrow as pa

PROTOCOL_VERSION = 1

_IPC_OPTIONS = pa.ipc.IpcWriteOptions(compression="zstd")


def new_id() -> str:
    return uuid.uuid4().hex[:12]


def dumps(obj: Any) -> bytes:
    return json.dumps(obj, default=str, separators=(",", ":")).encode()


def loads(raw: bytes) -> Any:
    return json.loads(raw.decode())


def frame_to_arrow(df: pd.DataFrame) -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema, options=_IPC_OPTIONS) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def arrow_to_frame(raw: bytes) -> pd.DataFrame:
    with pa.ipc.open_stream(raw) as reader:
        return reader.read_all().to_pandas()


# --- topics -----------------------------------------------------------------

def topic_part(text: str) -> str:
    """One topic segment: `/` would split it, so `BTC/USDT` becomes `BTC_USDT`."""
    return str(text).replace("/", "_")


def bar_topic(rel_path: str) -> str:
    """Topic for a stored series, derived from its path under DATA_ROOT.

    `stocks/yf/sp500/AAPL/AAPL-1d.parquet`         → `bar/stocks/yf/sp500/AAPL/1d`
    `stocks/ibkr/sp500/AAPL/AAPL-all-1h.parquet`   → `bar/stocks/ibkr/sp500/AAPL/all-1h`
    `cryptos/binance/BTC_USDT/BTC_USDT-1d.parquet` → `bar/cryptos/binance/BTC_USDT/1d`

    Mirroring the path keeps one naming rule for the whole system: whatever
    the provider-first layout can express, a subscription can select.
    """
    parts = rel_path.split("/")
    *dirs, filename = parts
    stem = filename.rsplit(".", 1)[0]
    symbol = dirs[-1] if dirs else ""
    suffix = stem[len(symbol) + 1:] if symbol and stem.startswith(symbol + "-") else stem
    return "/".join(["bar", *dirs, suffix])


def parse_series_path(rel_path: str) -> dict:
    """Identity fields for a stored series, from its path under DATA_ROOT."""
    *dirs, filename = rel_path.split("/")
    stem = filename.rsplit(".", 1)[0]
    asset_dir = dirs[0] if dirs else ""
    provider = dirs[1] if len(dirs) > 1 else None
    symbol = dirs[-1] if len(dirs) > 2 else stem
    universe = dirs[2] if asset_dir == "stocks" and len(dirs) > 3 else None
    rest = stem[len(symbol) + 1:] if stem.startswith(symbol + "-") else stem
    session, _, timeframe = rest.rpartition("-")
    return {
        "asset_dir": asset_dir,
        "provider": provider,
        "universe": universe,
        "symbol": symbol,
        "session": session or None,
        "timeframe": timeframe,
        "path": rel_path,
    }


def live_topic(exchange: str, symbol: str, timeframe: str) -> str:
    return f"live/{topic_part(exchange)}/{topic_part(symbol)}/{timeframe}"


def job_topic(state: str, tool: str) -> str:
    return f"job/{state}/{tool}"
