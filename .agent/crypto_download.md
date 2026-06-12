# Skill: crypto_download

Download crypto OHLCV data via freqtrade, keep price-data/ current.

## What this skill covers

- Identify stale or missing crypto data
- Download updates via freqtrade (Binance, Bitget, Bybit, KuCoin)
- Merge into existing parquet files without duplicates
- Keep `price-data/crypto_{exchange}.yaml` metadata in sync

## Tool chain

```
tools/check_gaps.py  →  identify what's stale/missing
tools/download_crypto.py  →  download + merge + update metadata
tools/query.py  →  verify result
```

## Standard refresh workflow

```bash
# 1. Check what's stale on binance (1d timeframe)
uv run python tools/check_gaps.py --exchange binance --timeframe 1d

# 2. Append new data for all binance pairs (1d + 4h)
uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d 4h

# 3. Verify
uv run python tools/query.py --exchange binance --timeframe 1d --stale-days 2
```

## Download a new symbol

```bash
# Full download of a pair not yet in catalog
uv run python tools/download_crypto.py \
  --exchange binance \
  --pairs SOL/USDT \
  --timeframes 1d 4h 1h 15m \
  --full --since 2020-01-01
```

## Prepend historical data

```bash
uv run python tools/download_crypto.py \
  --exchange binance \
  --pairs BTC/USDT ETH/USDT \
  --timeframes 1d \
  --prepend --since 2017-01-01
```

## Canonical parquet schema (enforce on every write)

All crypto parquet files must use this schema — no exceptions:

| Column      | dtype   | Notes                          |
|-------------|---------|--------------------------------|
| `timestamp` | int64   | Unix ms UTC                    |
| `open`      | float64 |                                |
| `high`      | float64 |                                |
| `low`       | float64 |                                |
| `close`     | float64 |                                |
| `volume`    | float64 | base asset                     |

After any download, data is normalized automatically via `normalize_ohlcv()` in `merge_parquet()`.
If you ever write parquet directly, always call `normalize_ohlcv(df)` first.

To fix non-canonical files (e.g. after manual import):
```bash
uv run python tools/convert_crypto.py [--exchange X] [--dry-run]
```

## Key facts for agents

- **freqtrade binary**: `$FREQTRADE_BIN` (default `/home/grenada/work/ai-quant/.venv/bin/freqtrade`)
- **freqtrade pair format**: `BTC/USDT` (slash) → stored as `BTC_USDT` (underscore)
- **freqtrade monthly TF**: `1M` (not `1Mo` — our name for it)
- **Data location**: `price-data/cryptos/{exchange}/{symbol}/{symbol}-{tf}.parquet`
- **Catalog keys**: binance uses `BTC_USDT`; others append exchange: `BTC_USDT_bitget`
- **Merge strategy**: `normalize_ohlcv()` both sides, dedup on `timestamp`, sort ascending

## Supported exchanges

| Exchange | Catalog file               | Symbols |
|----------|---------------------------|---------|
| binance  | crypto_binance.yaml       | ~392    |
| bitget   | crypto_bitget.yaml        | ~796    |
| bybit    | crypto_bybit.yaml         | ~82     |
| kucoin   | crypto_kucoin.yaml        | ~903    |

## Caveats

- freqtrade downloads public (unauthenticated) data — no API keys needed for spot OHLCV
- Rate limits apply; for large batch downloads freqtrade handles this internally
- `1Mo` (monthly) candles are not checked for gaps (variable interval)
- Legacy `.json` entries in the YAML are not updated by these tools (parquet only)
