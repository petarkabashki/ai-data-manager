"""Live exchange data: websocket bars and quotes, managed at runtime.

```yaml
live:
  - exchange: binance
    market: spot                 # spot | swap (perpetual futures)
    kind: bars                   # bars | quotes
    timeframe: 1m                # bars only
    symbols: [BTC/USDT, ETH/USDT]
  - exchange: binance
    market: swap
    kind: quotes                 # best bid/ask (Binance: bookTicker)
    symbols: [BTC/USDT, SOL/USDT]
    throttle_ms: 0               # >0 publishes at most one quote per symbol per window
qwant_compat: true               # also publish py-qwant's `prices` feed (below)
```

Clients add and remove symbols or whole subscriptions while the server runs
(`live_add` / `live_remove`); every change is written back to `pdm.yaml`.

Topics:
  * `live/<exchange>/<SYMBOL>/<tf>`        bars: every newly **closed** bar once
    (`closed: true`), and the forming bar whenever it changes (`closed: false`);
  * `quote/<exchange>/<market>/<SYMBOL>`   quotes: `{symbol, timestamp, last,
    bid, ask, bid_size, ask_size}`;
  * `prices` (with `qwant_compat`)         py-qwant's ingest-daemon format,
    byte-compatible: `[b"prices", msgpack({symbol, timestamp, last, bid, ask})]`
    — point py-qwant's SUB at `tcp://<host>:5556` instead of its local
    `qwant_prices.ipc` and nothing else changes.

Transport is websockets via ccxt.pro (`watch_ohlcv`, `watch_bids_asks`,
`watch_ticker`) on every exchange that has them, REST polling otherwise or
when `transport: poll` is set. Disconnects back off 1s → 60s and resume.

Live data is broadcast only, never written to `price-data/`: stored history
stays the product of the download tools and their planners. A client that
needs both reads history over request/reply, then follows the live topic.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from pdm.protocol import live_topic, topic_part
from utils.catalog import CANONICAL_COLS, CRYPTO_EXCHANGES, TF_TO_MS, normalize_ohlcv

log = logging.getLogger("pdm.live")

Publish = Callable[[str, dict, pd.DataFrame | None], None]
PublishRaw = Callable[[bytes, bytes], None]

KINDS = ("bars", "quotes")
MARKETS = ("spot", "swap")


def quote_topic(exchange: str, market: str, symbol: str) -> str:
    return f"quote/{topic_part(exchange)}/{market}/{topic_part(symbol)}"


def ccxt_symbol(symbol: str, market: str) -> str:
    """`BTC/USDT` (or our `BTC_USDT`) → ccxt's spelling for the market."""
    pair = symbol.replace("_", "/", 1) if "/" not in symbol else symbol
    if market == "swap" and ":" not in pair:
        pair = f"{pair}:{pair.split('/')[1]}"   # linear perpetual: BTC/USDT:USDT
    return pair


def our_symbol(symbol: str) -> str:
    """`BTC/USDT:USDT` → `BTC_USDT` — the workspace symbol, as py-qwant uses."""
    return symbol.split(":")[0].replace("/", "_")


@dataclass
class SymbolState:
    updates: int = 0
    last_update: float | None = None
    last_closed: int | None = None
    last_forming: tuple | None = None
    last_published: float = 0.0
    errors: int = 0
    last_error: str | None = None
    transport: str | None = None


@dataclass
class Subscription:
    exchange: str
    market: str
    kind: str
    symbols: list[str]
    timeframe: str | None = None
    poll_seconds: float = 10.0
    throttle_ms: int = 0
    transport: str = "auto"              # auto (websocket when available) | poll
    state: dict[str, SymbolState] = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.market}:{self.kind}:{self.timeframe or '-'}"

    @classmethod
    def from_config(cls, raw: dict) -> "Subscription":
        exchange = str(raw.get("exchange", "")).lower()
        if exchange not in CRYPTO_EXCHANGES:
            raise ValueError(f"exchange must be one of {CRYPTO_EXCHANGES}")
        market = raw.get("market", "spot")
        if market not in MARKETS:
            raise ValueError(f"market must be one of {MARKETS}")
        kind = raw.get("kind", "bars")
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}")
        timeframe = raw.get("timeframe", "1m") if kind == "bars" else None
        if kind == "bars" and not TF_TO_MS.get(timeframe):
            raise ValueError(f"bars timeframe {timeframe!r} has no fixed length")
        symbols = raw.get("symbols") or []
        if not isinstance(symbols, list) or not all(isinstance(s, str) and s for s in symbols):
            raise ValueError("symbols must be a list of strings like BTC/USDT")
        transport = raw.get("transport", "auto")
        if transport not in ("auto", "poll"):
            raise ValueError("transport must be auto or poll")
        return cls(
            exchange=exchange, market=market, kind=kind, timeframe=timeframe,
            symbols=[ccxt_symbol(s, "spot") for s in symbols],   # stored as BASE/QUOTE
            poll_seconds=max(1.0, float(raw.get("poll_seconds", 10))),
            throttle_ms=max(0, int(raw.get("throttle_ms", 0))),
            transport=transport,
        )

    def to_config(self) -> dict:
        out = {"exchange": self.exchange, "market": self.market, "kind": self.kind}
        if self.timeframe:
            out["timeframe"] = self.timeframe
        out["symbols"] = list(self.symbols)
        if self.kind == "bars" or self.transport == "poll":
            out["poll_seconds"] = self.poll_seconds
        if self.throttle_ms:
            out["throttle_ms"] = self.throttle_ms
        if self.transport != "auto":
            out["transport"] = self.transport
        return out

    def public(self, now: float | None = None) -> dict:
        now = now or time.time()
        symbols = []
        for s in self.symbols:
            st = self.state.get(s, SymbolState())
            symbols.append({
                "symbol": s,
                "topic": self.topic(s),
                "transport": st.transport,
                "updates": st.updates,
                "age_s": round(now - st.last_update, 1) if st.last_update else None,
                "errors": st.errors,
                "last_error": st.last_error,
            })
        return {**self.to_config(), "key": self.key, "symbols": symbols}

    def topic(self, symbol: str) -> str:
        if self.kind == "bars":
            return live_topic(self.exchange, symbol, self.timeframe)
        return quote_topic(self.exchange, self.market, symbol)


class LiveManager:
    """Owns one asyncio loop (own thread) and one task per (subscription, symbol)."""

    def __init__(
        self,
        publish: Publish,
        configs: list[dict] | None = None,
        *,
        publish_raw: PublishRaw | None = None,
        qwant_compat: bool = False,
        on_change: Callable[[list[dict]], None] | None = None,
        exchange_factory: Callable[[str, str], object] | None = None,
    ):
        self.publish = publish
        self.publish_raw = publish_raw
        self.qwant_compat = qwant_compat
        self.on_change = on_change or (lambda cfgs: None)
        self._factory = exchange_factory or self._make_exchange
        self._subs: dict[str, Subscription] = {}
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._exchanges: dict[tuple[str, str], object] = {}
        self._lock = threading.RLock()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, name="live", daemon=True)
        for raw in configs or []:
            sub = Subscription.from_config(raw)
            self._merge(sub)

    # --- lifecycle ------------------------------------------------------------

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def start(self) -> None:
        self._thread.start()
        with self._lock:
            for sub in self._subs.values():
                for symbol in sub.symbols:
                    self._start_task(sub, symbol)

    def stop(self) -> None:
        if not self._thread.is_alive():
            return

        async def _shutdown():
            for task in list(self._tasks.values()):
                task.cancel()
            for ex in list(self._exchanges.values()):
                close = getattr(ex, "close", None)
                if close:
                    try:
                        await close()
                    except Exception:
                        pass

        try:
            asyncio.run_coroutine_threadsafe(_shutdown(), self._loop).result(timeout=5)
        except Exception:
            pass
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)

    def _make_exchange(self, exchange: str, market: str):
        import ccxt.pro as ccxtpro

        cls = getattr(ccxtpro, exchange)
        return cls({"enableRateLimit": True, "options": {"defaultType": market}})

    # --- management (any thread) ----------------------------------------------

    def list(self, symbol: str | None = None, exchange: str | None = None) -> list[dict]:
        now = time.time()
        wanted = ccxt_symbol(symbol, "spot") if symbol else None
        with self._lock:
            subs = list(self._subs.values())
        out = []
        for sub in subs:
            if exchange and sub.exchange != exchange.lower():
                continue
            if wanted and wanted not in sub.symbols:
                continue
            out.append(sub.public(now))
        return out

    def add(self, raw: dict) -> dict:
        """Add symbols to a subscription, creating it if new. Idempotent."""
        sub = Subscription.from_config(raw)
        if not sub.symbols:
            raise ValueError("live_add needs at least one symbol")
        with self._lock:
            merged, added = self._merge(sub)
            if self._thread.is_alive():
                for symbol in added:
                    self._start_task(merged, symbol)
            self._changed()
            return {**merged.public(), "added": added}

    def remove(self, raw: dict) -> dict:
        """Remove symbols (or, with no symbols, the whole subscription)."""
        probe = Subscription.from_config({**raw, "symbols": raw.get("symbols") or []})
        with self._lock:
            sub = self._subs.get(probe.key)
            if sub is None:
                raise ValueError(f"no live subscription {probe.key}")
            gone = list(sub.symbols) if not probe.symbols else [s for s in probe.symbols if s in sub.symbols]
            missing = [s for s in probe.symbols if s not in sub.symbols]
            if missing:
                raise ValueError(f"not subscribed in {probe.key}: {missing}")
            for symbol in gone:
                sub.symbols.remove(symbol)
                sub.state.pop(symbol, None)
                self._stop_task(sub.key, symbol)
            if not sub.symbols:
                del self._subs[sub.key]
            self._changed()
            return {"key": probe.key, "removed": gone, "remaining": list(sub.symbols)}

    def configs(self) -> list[dict]:
        with self._lock:
            return [s.to_config() for s in self._subs.values()]

    def freshness(self) -> dict[str, float | None]:
        """{topic: seconds since last update} — for the heartbeat."""
        now = time.time()
        with self._lock:
            return {
                sub.topic(s): (round(now - st.last_update, 1) if (st := sub.state.get(s)) and st.last_update else None)
                for sub in self._subs.values() for s in sub.symbols
            }

    def _merge(self, sub: Subscription) -> tuple[Subscription, list[str]]:
        existing = self._subs.get(sub.key)
        if existing is None:
            self._subs[sub.key] = sub
            return sub, list(sub.symbols)
        added = [s for s in sub.symbols if s not in existing.symbols]
        existing.symbols.extend(added)
        existing.poll_seconds, existing.throttle_ms = sub.poll_seconds, sub.throttle_ms
        existing.transport = sub.transport
        return existing, added

    def _changed(self) -> None:
        self.on_change(self.configs())

    def _start_task(self, sub: Subscription, symbol: str) -> None:
        def create():
            key = (sub.key, symbol)
            if key not in self._tasks or self._tasks[key].done():
                runner = self._bars if sub.kind == "bars" else self._quotes
                self._tasks[key] = self._loop.create_task(runner(sub, symbol))
        self._loop.call_soon_threadsafe(create)

    def _stop_task(self, key: str, symbol: str) -> None:
        def cancel():
            task = self._tasks.pop((key, symbol), None)
            if task:
                task.cancel()
        self._loop.call_soon_threadsafe(cancel)

    # --- streaming (loop thread) ----------------------------------------------

    def _exchange(self, sub: Subscription):
        k = (sub.exchange, sub.market)
        if k not in self._exchanges:
            self._exchanges[k] = self._factory(sub.exchange, sub.market)
        return self._exchanges[k]

    async def _guarded(self, sub: Subscription, symbol: str, step) -> None:
        """Run `step()` forever with exponential backoff on errors."""
        st = sub.state.setdefault(symbol, SymbolState())
        backoff = 1.0
        while symbol in sub.symbols:
            try:
                await step(st)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                st.errors += 1
                st.last_error = f"{type(exc).__name__}: {exc}"[:300]
                log.warning("%s %s: %s (retry in %.0fs)", sub.key, symbol, st.last_error, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60.0)

    async def _bars(self, sub: Subscription, symbol: str) -> None:
        ex = self._exchange(sub)
        sym = ccxt_symbol(symbol, sub.market)
        use_ws = sub.transport == "auto" and bool(getattr(ex, "has", {}).get("watchOHLCV"))

        async def step(st: SymbolState):
            if use_ws:
                st.transport = "websocket"
                raw = await ex.watch_ohlcv(sym, sub.timeframe, limit=5)
            else:
                st.transport = "poll"
                raw = await ex.fetch_ohlcv(sym, sub.timeframe, limit=5)
            self.handle_bars(sub, symbol, raw)
            if not use_ws:
                await asyncio.sleep(sub.poll_seconds)

        await self._guarded(sub, symbol, step)

    async def _quotes(self, sub: Subscription, symbol: str) -> None:
        ex = self._exchange(sub)
        sym = ccxt_symbol(symbol, sub.market)
        has = getattr(ex, "has", {})
        mode = "poll"
        if sub.transport == "auto":
            mode = "bidsasks" if has.get("watchBidsAsks") else "ticker" if has.get("watchTicker") else "poll"

        async def step(st: SymbolState):
            if mode == "bidsasks":
                st.transport = "websocket"
                book = await ex.watch_bids_asks([sym])
                ticker = book.get(sym) or next(iter(book.values()), None)
            elif mode == "ticker":
                st.transport = "websocket"
                ticker = await ex.watch_ticker(sym)
            else:
                st.transport = "poll"
                ticker = await ex.fetch_ticker(sym)
            if ticker:
                self.handle_quote(sub, symbol, ticker)
            if mode == "poll":
                await asyncio.sleep(sub.poll_seconds)

        await self._guarded(sub, symbol, step)

    # --- message building (pure; unit-tested) ---------------------------------

    def handle_bars(self, sub: Subscription, symbol: str, raw: list, now_ms: int | None = None) -> int:
        st = sub.state.setdefault(symbol, SymbolState())
        st.updates += 1
        st.last_update = time.time()
        if not raw:
            return 0
        now_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        tf_ms = TF_TO_MS[sub.timeframe]
        df = normalize_ohlcv(pd.DataFrame([r[:6] for r in raw], columns=CANONICAL_COLS))
        df = df.drop_duplicates("timestamp", keep="last").sort_values("timestamp")
        closed = df[df["timestamp"] + tf_ms <= now_ms]
        forming = df[df["timestamp"] + tf_ms > now_ms]
        topic = sub.topic(symbol)
        base = {"type": "live", "exchange": sub.exchange, "market": sub.market,
                "symbol": symbol, "timeframe": sub.timeframe}
        sent = 0
        new_closed = closed if st.last_closed is None else closed[closed["timestamp"] > st.last_closed]
        if st.last_closed is None:
            new_closed = new_closed.tail(1)   # on (re)start, announce only the latest close
        if not new_closed.empty:
            st.last_closed = int(new_closed["timestamp"].iloc[-1])
            self.publish(topic, {**base, "closed": True, "rows": len(new_closed)},
                         new_closed.reset_index(drop=True))
            sent += 1
        if not forming.empty:
            sig = tuple(forming.iloc[-1])
            if st.last_forming != sig:
                st.last_forming = sig
                self.publish(topic, {**base, "closed": False, "rows": 1},
                             forming.tail(1).reset_index(drop=True))
                sent += 1
        return sent

    def handle_quote(self, sub: Subscription, symbol: str, ticker: dict, now: float | None = None) -> bool:
        st = sub.state.setdefault(symbol, SymbolState())
        now = now if now is not None else time.time()
        st.updates += 1
        st.last_update = now
        if sub.throttle_ms and (now - st.last_published) * 1000 < sub.throttle_ms:
            return False
        bid = ticker.get("bid")
        ask = ticker.get("ask")
        last = ticker.get("last")
        if not last:
            # py-qwant's rule: the mid, else whichever side is non-zero
            last = (bid + ask) / 2 if (bid and ask) else (bid or ask)
        ts_ms = ticker.get("timestamp") or int(now * 1000)
        iso = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()
        sym = our_symbol(symbol)
        quote = {"symbol": sym, "timestamp": iso, "last": last, "bid": bid, "ask": ask}
        self.publish(sub.topic(symbol), {
            "type": "quote", "exchange": sub.exchange, "market": sub.market,
            **quote, "bid_size": ticker.get("bidVolume"), "ask_size": ticker.get("askVolume"),
        })
        if self.qwant_compat and self.publish_raw:
            import msgpack

            self.publish_raw(b"prices", msgpack.packb(quote))
        st.last_published = now
        return True
