"""What each crypto venue lists today, and since when.

`--all-pairs` means "the pairs already in the catalog"; this module answers
the other question — "every pair the venue trades" — for `download_crypto
--universe`, and tells delisted catalog pairs apart so they can be marked
retired instead of re-requested every night.

The universe is every active spot pair, whatever its quote (USDT, USDC, BTC,
TRY, EUR…), and every active perpetual, linear or inverse, whatever it
settles in. It excludes leveraged tokens (`BTC3L`, `ETHUP`), bitget's
"innovation zone" (`areaSymbol: yes`: ~3,000 on-chain tokens that bitget lists
as spot pairs, `rNNNN/USDT`, against ~550 on its main board) and bitget's
demo-trading contracts (settled in `SUSDT`, `SUSDC`, `SBTC`…). Dated futures
are not part of it: they expire, and have no catalog shape here yet.

Listing dates matter because kucoin and bitget answer a request that starts
before a pair existed with an empty page rather than with the first bars, so
"download from 2017" silently yields nothing. Where the venue publishes the
listing time (`created`, bitget's `openTime`) it is used directly; otherwise
the first monthly candle marks it — one request per new pair.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

EARLIEST = "2017-01-01"          # before any pair we track was listed

# Leveraged and directional tokens: BTC3L, ETH5S, BNBUP, XRPBEAR.
_LEVERAGED = re.compile(r"\d+[LS]$|(UP|DOWN|BULL|BEAR)$")

# bitget's simulated-trading settlement coins.
_BITGET_DEMO = re.compile(r"^S(USDT|USDC|BTC|ETH)$")

# ccxt classes where the spot class can't serve futures.
_FUTURES_CLASS = {"kucoin": "kucoinfutures"}


def ccxt_exchange(exchange: str, trading_mode: str = "spot"):
    """A ccxt client for the venue's spot or USDT-perpetual market."""
    import ccxt

    if trading_mode == "futures":
        name = _FUTURES_CLASS.get(exchange, exchange)
        options = {} if name in _FUTURES_CLASS.values() else {"defaultType": "swap"}
        return getattr(ccxt, name)({"enableRateLimit": True, "options": options})
    return getattr(ccxt, exchange)({"enableRateLimit": True})


def in_universe(market: dict, exchange: str, trading_mode: str) -> bool:
    """Whether a ccxt market belongs to the tracked universe."""
    if market.get("active") is False:
        return False
    if trading_mode == "futures":
        if not market.get("swap") or not market.get("settle"):
            return False
        return not (exchange == "bitget" and _BITGET_DEMO.match(market["settle"]))
    if not market.get("spot") or not market.get("quote") or _LEVERAGED.search(market.get("base") or ""):
        return False
    if exchange == "bitget" and (market.get("info") or {}).get("areaSymbol") == "yes":
        return False
    return True


def listed_markets(ccxt_ex, exchange: str, trading_mode: str) -> dict[str, dict]:
    """{pair: market} for the universe; spot pairs `ETH/BTC`, futures `BTC/USDC:USDC`."""
    markets = ccxt_ex.load_markets()
    return {m["symbol"]: m for m in markets.values() if in_universe(m, exchange, trading_mode)}


def published_listing_ms(market: dict) -> int | None:
    """The listing time the venue publishes, if it does."""
    created = market.get("created")
    if created:
        return int(created)
    open_time = (market.get("info") or {}).get("openTime")
    try:
        ms = int(open_time)
    except (TypeError, ValueError):
        return None
    return ms if ms > 0 else None


def listing_date(ccxt_ex, market: dict) -> str | None:
    """First trading day as YYYY-MM-DD: published, else the first monthly candle.

    None when neither is available (the caller falls back to EARLIEST).
    """
    ms = published_listing_ms(market)
    if ms is None and "1M" in (ccxt_ex.timeframes or {}):
        try:
            rows = ccxt_ex.fetch_ohlcv(market["symbol"], "1M", since=ccxt_ex.parse8601(f"{EARLIEST}T00:00:00Z"),
                                       limit=200)
        except Exception:
            rows = []
        ms = rows[0][0] if rows else None
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def mark_retired(catalog: dict, listed_names: set[str], exchange: str, today: str) -> tuple[list[str], list[str]]:
    """Flag catalog entries the venue no longer lists; clear the flag on a relisting.

    `listed_names` are catalog names (`BTC_USDT`). Nothing is deleted: the
    files and dates stay, the entry just stops being downloaded and stops
    counting as stale. Returns (newly retired, relisted) names.
    """
    retired, relisted = [], []
    for entry in catalog.values():
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if name in listed_names:
            if entry.pop("retired", None):
                entry.pop("retired_reason", None)
                relisted.append(name)
        elif not entry.get("retired"):
            entry["retired"] = today
            entry["retired_reason"] = f"not listed on {exchange}"
            retired.append(name)
    return sorted(retired), sorted(relisted)
