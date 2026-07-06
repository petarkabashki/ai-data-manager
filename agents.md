# Agents

This workspace manages price data across asset classes (equities, crypto, indexes,
commodities, macro) and providers (YF, IBKR, Binance, Bitget, Bybit, KuCoin).

## Environment

- **Python**: `uv run python` or activate `.venv/bin/activate`
- **Data root**: `price-data/` (symlink → `../price-data/`)
- **Asset metadata**: `price-data/assets.yaml` (master index with includes)

## Layout

```
ai-data-manager/
├── .agent/          # Agent skills, prompts, and agentic configs
├── tools/           # Standalone executable tools (CLI scripts)
├── utils/           # Shared Python utilities (importable modules)
├── price-data/      # Symlink → ../price-data/
│   ├── assets.yaml          # Master asset registry (include: list of per-class YAMLs)
│   ├── stocks/
│   │   ├── yf/<universe>/<SYMBOL>/<SYMBOL>-1d.parquet
│   │   └── ibkr/<universe>/<SYMBOL>/<SYMBOL>-<session>-<TF>.parquet
│   ├── cryptos/<exchange>/<symbol>/<symbol>-<TF>.parquet
│   ├── crypto-futures/<exchange>/<symbol>/<symbol>-<TF>.parquet
│   ├── indexes/yf/<SYMBOL>/<SYMBOL>-1d.parquet
│   ├── commodities/yf/<SYMBOL>/<SYMBOL>-1d.parquet
│   ├── macro/yf/<SYMBOL>/<SYMBOL>-1d.parquet
│   ├── market-cap/coingecko/{exports,history,reference}/  # not in assets.yaml (not per-symbol OHLCV)
│   └── *.yaml               # Per-asset-class metadata catalogs
├── pyproject.toml
└── agents.md        # This file
```

## Uniform provider-first layout (RULE — never break this)

Every asset class follows one path rule, with the provider always the first
directory level under the class:

```
<asset-class>/<provider>[/<universe>]/<symbol>/<symbol>[-<session>]-<timeframe>.parquet
```

- `universe` applies only to stocks (`sp500`, `nasdaq100`, `russell2000`,
  `delisted_stocks` for tickers no longer in any tracked index).
- `session` applies only to IBKR intraday data (`all` / `overnight`).
- Never write one provider's data into another provider's path, and never let
  one provider's download overwrite another provider's *catalog* registration
  for the same symbol/timeframe — see schema rule below.

## Data conventions

- Metadata catalogs: YAML keyed by symbol, with `categories`, `timeframes`,
  `timeframe_dates` (per-timeframe date range + file path).
- **Stock catalogs** (`sp500.yaml`, `nasdaq100.yaml`, `russell2000.yaml`,
  `delisted_stocks.yaml`) key `timeframe_dates` by **provider within each
  timeframe**, so two providers can register the same timeframe without
  collision:
  ```yaml
  AAPL:
    timeframe_dates:
      1d:
        YF:   {start_date: ..., end_date: ..., file_path: ...}
        IBKR: {start_date: ..., end_date: ..., file_path: ..., session: all}
      1h:
        IBKR: {start_date: ..., end_date: ..., file_path: ..., session: all}
  ```
  Use `utils.catalog.update_stock_timeframe_entry()` to write these — it
  cannot clobber another provider's node because they're different dict keys.
  Never manually flatten this back to a single node per timeframe.
- **All other catalogs** (indexes, commodities, macro, crypto, crypto-futures)
  keep the simpler flat shape — `timeframe_dates[tf]` is one node directly
  (`{start_date, end_date, file_path}`) — because each of those only ever has
  one provider per (symbol, timeframe); there's no contention to guard
  against, so don't add nesting there.
- File formats: `.parquet` everywhere (indexes' old dual CSV/parquet files
  were merged into one canonical parquet per symbol during the 2026-07
  provider-first migration).
- File paths in YAML are relative to the repo root.
- Asset classes: `Index`, `Crypto`, `Stock`, `Commodity`, `Macro`.
- Providers: `YF` (Yahoo Finance), `IBKR` (Interactive Brokers), `Binance`,
  `Bitget`, `Bybit`, `KuCoin`.
- Every OHLCV-shaped asset class — crypto, crypto-futures, stocks (YF and
  IBKR), indexes, and commodities — is stored on disk in the canonical
  schema below (`timestamp` int64 ms UTC + lowercase OHLCV float64). Raw YF
  exports still arrive as `Date` (datetime64) + `Open/High/Low/Close/
  [Adj Close]/Volume`; `normalize_ohlcv()` converts that shape on ingestion
  before anything is written. Macro's `WTB3MS` is the one exception (a plain
  rate series with `Value` in a DatetimeIndex, not OHLCV — don't push it
  through `normalize_ohlcv()`/`read_parquet_range()`).

## Canonical crypto OHLCV format (RULE — never break this)

All crypto parquet files **must** use this exact schema:

| Column      | dtype   | Description                        |
|-------------|---------|------------------------------------|
| `timestamp` | int64   | Unix timestamp in **milliseconds** UTC |
| `open`      | float64 | Open price                         |
| `high`      | float64 | High price                         |
| `low`       | float64 | Low price                          |
| `close`     | float64 | Close price                        |
| `volume`    | float64 | Volume (base asset)                |

Rules:
- No extra columns (`date`, `0-5`, `Timestamp`, etc.).
- No JSON files in `price-data/cryptos/` — convert any to parquet immediately.
- Newly downloaded data **must** go through `normalize_ohlcv()` before saving.
- Empty parquet files (no columns) are tolerated but indicate failed downloads.

To normalize: `from utils.catalog import normalize_ohlcv`

To bulk-convert legacy files or newly introduced non-canonical files:
```bash
uv run python tools/convert_crypto.py [--exchange X] [--dry-run]
```

## Asset class coverage

| Class      | Location                        | Format  | Notes                                    |
|------------|----------------------------------|---------|-------------------------------------------|
| Indexes    | price-data/indexes/yf/           | parquet | DJI, GSPC, IXIC, NDX, RUT, VIX…            |
| Stocks     | price-data/stocks/{yf,ibkr}/     | parquet | sp500, nasdaq100, russell2000, delisted_stocks |
| Crypto     | price-data/cryptos/<exch>/       | parquet | Binance, Bitget, Bybit, KuCoin             |
| Crypto-futures | price-data/crypto-futures/<exch>/ | parquet | Binance, Bybit, KuCoin                 |
| Commodities| price-data/commodities/yf/       | parquet | GC=F (Gold)                                |
| Macro      | price-data/macro/yf/             | parquet | WTB3MS (T-bill; not OHLCV-shaped)          |
| Market-cap | price-data/market-cap/coingecko/ | parquet | Not in assets.yaml — snapshot/history exports, not per-symbol OHLCV |

## Tools

| Tool | Purpose |
|------|---------|
| `tools/query.py` | Query available assets, timeframes, date ranges, staleness |
| `tools/check_gaps.py` | Audit data for internal gaps and trailing staleness |
| `tools/check_schema.py` | Verify every OHLCV-classed parquet file matches the canonical schema |
| `tools/download_crypto.py` | Download via freqtrade, merge parquet, update YAML metadata |
| `tools/convert_crypto.py` | Bulk normalize all crypto parquet to canonical schema + JSON→parquet |
| `tools/download_market_caps.py` | Fetch CoinGecko historical market-cap data and export snapshot/top-N universes |
| `tools/download_ibkr_stock_overnight.py` | Download IBKR US stock bars for `sp500` / `nasdaq100`, with optional overnight-only filtering |

### query.py

```bash
uv run python tools/query.py --exchange binance --timeframe 1d
uv run python tools/query.py --exchange binance --symbol BTC_USDT
uv run python tools/query.py --stale-days 30          # only stale entries
uv run python tools/query.py --list-exchanges
uv run python tools/query.py --list-symbols --exchange binance --timeframe 1d
```

### check_gaps.py

```bash
uv run python tools/check_gaps.py --exchange binance --timeframe 1d
uv run python tools/check_gaps.py --all                   # all exchanges, 1d
uv run python tools/check_gaps.py --exchange binance --internal  # show gap details
uv run python tools/check_gaps.py --exchange binance --stale-days 7
```

### check_schema.py

```bash
uv run python tools/check_schema.py                              # all OHLCV classes
uv run python tools/check_schema.py --path price-data/commodities # one class only
```

Read-only. Flags any parquet file under crypto/crypto-futures/stocks/indexes/
commodities whose on-disk columns or dtypes don't exactly match
`CANONICAL_COLS` (`timestamp` int64 ms + OHLCV float64) — e.g. a tool that
wrote raw provider output straight to disk without routing it through
`normalize_ohlcv()` first. Exits non-zero if any mismatches are found.

### download_crypto.py

```bash
# Append new data for existing pairs
uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d 4h

# Download a new symbol from scratch
uv run python tools/download_crypto.py --exchange binance --pairs SOL/USDT --timeframes 1d 4h 1h --full

# Prepend historical data
uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT --timeframes 1d --prepend --since 2017-01-01
```

### download_market_caps.py

```bash
# Hand-picked recent universe
uv run python tools/download_market_caps.py \
  --symbols sol ada xrp doge \
  --start 2025-07-01 --end 2026-06-01

# Monthly top-20 snapshots across active coins, excluding BTC + stablecoins
uv run python tools/download_market_caps.py \
  --all-active \
  --start 2025-07-01 --end 2026-06-01 --frequency monthly \
  --top-n 20 --exclude-ids bitcoin --exclude-stablecoins
```

### download_ibkr_stock_overnight.py

```bash
# S&P 500 intraday bars from IBKR
uv run python tools/download_ibkr_stock_overnight.py \
  --universe sp500 --timeframes 1h 4h --since 2024-01-01

# Nasdaq 100 overnight-only smoke test on a few symbols
uv run python tools/download_ibkr_stock_overnight.py \
  --universe nasdaq100 --symbols AAPL MSFT NVDA --timeframes 1h --session overnight --limit 3

# Resume/fill only missing 8h + 1d IBKR bars and write progress artifacts
uv run python tools/download_ibkr_stock_overnight.py \
  --universe nasdaq100 --timeframes 8h 1d --since 2020-01-01 --port 4001
```

Notes:
- Connect TWS or IB Gateway first, then point `--host` / `--port` / `--client-id`
  at that session.
- The tool requests `useRTH=0`. With `--session overnight`, it keeps only bars
  fully outside `09:30-16:00 America/New_York`.
- Intraday files are written under
  `price-data/stocks/ibkr/<universe>/<SYMBOL>/<SYMBOL>-<session>-<TF>.parquet`
  via `utils.catalog.stock_file_path()`.
- Existing stock parquet files are scanned first; the downloader only requests
  prepend history, internal gaps, or trailing missing bars unless `--full` is set.
- Progress is checkpointed to `tmp/ibkr_stock_download_progress.json` and
  append-only resume events go to `tmp/ibkr_stock_download_resume.jsonl`.
- Daily YF entries stay intact — `update_stock_timeframe_entry()` writes IBKR's
  data into its own `timeframe_dates[tf]["IBKR"]` node, never touching YF's
  sibling node for the same timeframe (see schema rule above).
- Watch out for IBKR's historical-data pacing limits (not the general 50 msg/s
  API limit) if you increase throughput: sustained aggressive request rates —
  either very short `--sleep-seconds` on one connection, or multiple parallel
  connections — have caused both a burst pacing-violation lockout and a
  multi-hour dead-connection hang in practice. Prefer a single connection at a
  moderate `--sleep-seconds` (0.3-1.0) over parallelizing across client-ids.

## Agent Skills

| Skill | File | Description |
|-------|------|-------------|
| crypto_download | `.agent/crypto_download.md` | Full workflow: check gaps → download → verify |

## Utilities (importable)

| Module | Key exports |
|--------|-------------|
| `utils/data.py` | `load_assets()`, `load_ohlcv()`, `asset_path()` (handles both flat and nested-by-provider `timeframe_dates` shapes) |
| `utils/catalog.py` | `load_crypto_catalog()`, `save_crypto_catalog()`, `read_parquet_range()`, `refresh_catalog_dates()`, `load_stock_catalog()`, `save_stock_catalog()`, `stock_file_path()`, `update_stock_timeframe_entry()`, `refresh_stock_catalog_dates()`, `normalize_ohlcv()`, `FREQTRADE_BIN`, `TF_TO_MS` |
| `utils/gaps.py` | `detect_gaps()`, `audit_exchange()`, `SymbolAudit` (crypto-only) |

`tools/migrate_price_data_layout.py` is the one-off script that performed the
2026-07 provider-first migration (phases A-G, each idempotent + `--dry-run`).
Kept for reference/re-run if a similar restructuring is ever needed again.

## Adding a new tool

Place executable scripts in `tools/`. They should use the project venv:
```python
#!/usr/bin/env -S uv run python
```
Or run with `uv run python tools/my_tool.py`. Add an entry to the Tools table above.

## Adding a new agent skill

Place skill definitions, prompts, and agent configs in `.agent/`. Add an entry
to the Agent Skills table above and document the workflow in the skill file.
