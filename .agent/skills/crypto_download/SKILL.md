---
name: crypto_download
description: Refresh crypto OHLCV data from a CEX via freqtrade — find what's stale or missing, download and merge it, and keep the YAML catalog in sync. Use when asked to update, backfill or add crypto pairs on binance, bitget, bybit or kucoin.
---

# Crypto download

Download crypto OHLCV via freqtrade and keep `price-data/` current.

## Tool chain

```
status.py / check_gaps.py  →  what's stale or missing
download_crypto.py         →  download + merge + update the catalog
check_catalog.py           →  confirm the catalog matches what landed
```

## Standard refresh

```bash
# 1. What's stale on binance?
uv run python tools/check_gaps.py --exchange binance --timeframe 1d

# 2. Append new data for every catalogued pair (1d + 4h)
uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d 4h

# 3. Verify
uv run python tools/query.py --catalog crypto_binance --timeframe 1d --stale-days 2
uv run python tools/check_catalog.py --catalog crypto_binance
```

## Add a new symbol

```bash
uv run python tools/download_crypto.py \
  --exchange binance --pairs SOL/USDT \
  --timeframes 1d 4h 1h 15m --full --since 2020-01-01
```

## Backfill history

```bash
uv run python tools/download_crypto.py \
  --exchange binance --pairs BTC/USDT ETH/USDT \
  --timeframes 1d --prepend --since 2017-01-01
```

`--prepend` requests only data *before* the file's current start; existing rows
are never re-fetched.

## Futures

```bash
uv run python tools/download_crypto.py --exchange bybit --all-pairs \
  --timeframes 1d --trading-mode futures
```

KuCoin futures goes through ccxt directly — freqtrade doesn't support it in
futures mode — and the tool switches backend automatically.

## Rules that apply to every write

- **Canonical schema, always**: `timestamp` (int64 ms UTC) + `open/high/low/
  close/volume` (float64). `merge_ohlcv_parquet()` normalizes both sides,
  dedups on timestamp (newest wins) and writes atomically. Never call
  `df.to_parquet()` on a path under `price-data/`.
- **Crypto catalogs stay flat.** One provider per (symbol, timeframe) — the
  exchange is already in the catalog key and the path — so unlike stocks and
  indexes, `timeframe_dates[tf]` is a single node. Don't add nesting.
- **Gaps use the continuous calendar.** Crypto trades 24/7, so any missing bar
  is a real gap. That's the default; don't pass an equity calendar.

## Key facts

| Thing | Value |
|---|---|
| freqtrade binary | `$FREQTRADE_BIN` (see `.env.example`) |
| Pair format | `BTC/USDT` to freqtrade, stored as `BTC_USDT` |
| Monthly timeframe | `1M` to freqtrade, `1Mo` to us |
| Data location | `price-data/cryptos/{exchange}/{symbol}/{symbol}-{tf}.parquet` |
| Catalog key | binance uses `BTC_USDT`; others append the exchange (`BTC_USDT_bitget`) |
| Supported exchanges | binance, bitget, bybit, kucoin (`crypto_{exchange}.yaml`) |

Current symbol counts: `uv run python tools/query.py --list-catalogs`.

## Caveats

- Spot OHLCV is public — no API keys needed.
- `1Mo` candles are skipped by gap detection (variable interval).
- Legacy `.json` entries in a catalog are not updated by these tools; convert
  with `uv run python tools/convert_crypto.py` first.
- For bulk runs (>10 pairs) freqtrade output is redirected to a log under
  `tmp/`; the path is printed at the start.
