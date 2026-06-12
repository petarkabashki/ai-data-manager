# Agents

This workspace manages price data across asset classes (equities, crypto, indexes,
commodities, macro) and providers (YF, Binance, Bitget, Bybit, KuCoin).

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
│   ├── assets.yaml          # Master asset registry
│   ├── stocks/              # Equities (parquet, by index subdir)
│   ├── cryptos/             # Crypto OHLCV by exchange
│   ├── indexes/             # Market indexes (CSV/parquet)
│   ├── commodities/         # Commodities (parquet)
│   ├── macro/               # Macro series (parquet)
│   └── *.yaml               # Per-asset-class metadata catalogs
├── pyproject.toml
└── agents.md        # This file
```

## Data conventions

- Metadata catalogs: YAML keyed by symbol, with `provider`, `categories`,
  `timeframes`, `timeframe_dates` (start/end/file_path per timeframe).
- File formats: `.parquet` for crypto; `.parquet` or `.csv` for indexes/stocks.
- File paths in YAML are relative to the repo root (e.g. `price-data/indexes/DJI.csv`).
- Asset classes: `Index`, `Crypto`, `Stock`, `Commodity`, `Macro`.
- Providers: `YF` (Yahoo Finance), `Binance`, `Bitget`, `Bybit`, `KuCoin`.

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

| Class      | Location                  | Format   | Notes                              |
|------------|---------------------------|----------|------------------------------------|
| Indexes    | price-data/indexes/       | CSV+pqt  | DJI, GSPC, IXIC, NDX, RUT, VIX…  |
| Stocks     | price-data/stocks/        | parquet  | SP500, NASDAQ, Russell2000 subdirs |
| Crypto     | price-data/cryptos/<exch> | parquet  | Binance (~395), Bitget, Bybit, KuCoin |
| Commodities| price-data/commodities/   | parquet  | GC=F (Gold)                        |
| Macro      | price-data/macro/         | parquet  | WTB3MS (T-bill)                    |

## Tools

| Tool | Purpose |
|------|---------|
| `tools/query.py` | Query available assets, timeframes, date ranges, staleness |
| `tools/check_gaps.py` | Audit data for internal gaps and trailing staleness |
| `tools/download_crypto.py` | Download via freqtrade, merge parquet, update YAML metadata |
| `tools/convert_crypto.py` | Bulk normalize all crypto parquet to canonical schema + JSON→parquet |

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

### download_crypto.py

```bash
# Append new data for existing pairs
uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d 4h

# Download a new symbol from scratch
uv run python tools/download_crypto.py --exchange binance --pairs SOL/USDT --timeframes 1d 4h 1h --full

# Prepend historical data
uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT --timeframes 1d --prepend --since 2017-01-01
```

## Agent Skills

| Skill | File | Description |
|-------|------|-------------|
| crypto_download | `.agent/crypto_download.md` | Full workflow: check gaps → download → verify |

## Utilities (importable)

| Module | Key exports |
|--------|-------------|
| `utils/data.py` | `load_assets()`, `load_ohlcv()`, `asset_path()` |
| `utils/catalog.py` | `load_crypto_catalog()`, `save_crypto_catalog()`, `read_parquet_range()`, `refresh_catalog_dates()`, `FREQTRADE_BIN`, `TF_TO_MS` |
| `utils/gaps.py` | `detect_gaps()`, `audit_exchange()`, `SymbolAudit` |

## Adding a new tool

Place executable scripts in `tools/`. They should use the project venv:
```python
#!/usr/bin/env -S uv run python
```
Or run with `uv run python tools/my_tool.py`. Add an entry to the Tools table above.

## Adding a new agent skill

Place skill definitions, prompts, and agent configs in `.agent/`. Add an entry
to the Agent Skills table above and document the workflow in the skill file.
