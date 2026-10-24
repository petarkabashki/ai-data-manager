# Architecture

## Overview

This repo has three layers:

1. **`tools/`** — standalone CLI scripts, one process per invocation, no
   shared state beyond the files they read/write. Two of them are auditors:
   `check_schema.py` checks a file's columns, `check_catalog.py` checks what
   the catalog claims about that file.
2. **`utils/`** — importable modules with no CLI of their own. They own the
   canonical schema, the catalog read/write logic, and gap detection.
3. **`price-data/`** — a symlink to `../price-data/`, the actual data root:
   parquet files plus YAML catalogs that index them.

Everything downstream (notebooks, trading research, agent skills) reads
`price-data/` through `utils/data.py` or `utils/catalog.py` — never by
guessing paths directly, since the path shape differs by asset class (see
below).

## Data flow

```mermaid
sequenceDiagram
    participant P as Provider (YF/IBKR/CEX/CoinGecko)
    participant T as tools/download_*.py
    participant N as normalize_ohlcv()
    participant F as price-data/*.parquet
    participant C as price-data/*.yaml (catalog)
    participant Q as tools/query.py / utils.data

    T->>P: request bars (REST / freqtrade / ibapi)
    P-->>T: raw OHLCV (provider-specific column names)
    T->>N: normalize_ohlcv(raw_df)
    N-->>T: canonical df (timestamp int64 ms, OHLCV float64)
    T->>F: merge + dedup on timestamp, write parquet
    T->>C: update_*_timeframe_entry() → start/end/file_path
    Q->>C: load catalog
    Q->>F: read_parquet_range() for live start/end
    Q-->>Q: print table / return DataFrame
```

Every download tool converges on the same normalization step — raw provider
output never touches disk directly — and on the same writer,
`utils.catalog.merge_ohlcv_parquet()`, which normalizes both sides, dedups on
`timestamp` (newest wins) and writes atomically via a temp file.
`tools/check_schema.py` exists specifically to catch a tool that skipped
normalization.

## Repository layout

```
ai-data-manager/
├── .agent/
│   └── skills/crypto_download/    # agent skill: gap-check → download → verify
├── docs/                         # this documentation set
├── tools/
│   ├── status.py                 # read: one cross-class health roll-up
│   ├── query.py                  # read: series, timeframes, staleness (all classes)
│   ├── check_gaps.py             # read: crypto gap/staleness audit
│   ├── check_schema.py           # read: canonical-schema conformance audit
│   ├── check_catalog.py          # read/fix: catalog completeness + disk drift
│   ├── download_crypto.py        # write: freqtrade → parquet + catalog
│   ├── convert_crypto.py         # write: bulk-normalize legacy files
│   ├── download_yf.py            # write: Yahoo → parquet + catalog
│   ├── download_market_caps.py   # write: CoinGecko snapshots/history
│   ├── download_ibkr_stock_overnight.py  # write: IBKR stock bars → parquet + catalog
│   ├── download_ibkr_index.py    # write: IBKR index bars → parquet + catalog
│   ├── retry_ibkr_failed_sequentially.py # write: re-run IBKR failures one at a time
│   └── migrate_price_data_layout.py      # one-off: 2026-07 provider-first migration
├── tests/                        # pytest suite (no network, no price-data/)
├── utils/
│   ├── calendars.py              # trading sessions: which bars should exist
│   ├── config.py                 # env-resolved paths, provider endpoints
│   ├── catalog.py                # canonical schema, atomic parquet IO, catalog CRUD, paths
│   ├── data.py                   # load_assets(), asset_path(), load_ohlcv()
│   ├── gaps.py                   # detect_gaps(), audit_catalog_series()
│   ├── ibkr.py                   # ibapi wrapper, range planner, chunking, throttling
│   ├── market_caps.py            # CoinGecko client, universe resolution
│   ├── planning.py               # provider-neutral download range planner
│   ├── yf.py                     # Yahoo tickers, intervals, limits, fetching
│   ├── report.py                 # shared table rendering
│   └── semantics.py              # what the numbers mean, per catalog node
├── price-data/ -> ../price-data/
│   ├── assets.yaml                        # master index (include: [...])
│   ├── {sp500,nasdaq100,russell2000,delisted_stocks}.yaml
│   ├── crypto_{binance,bitget,bybit,kucoin}.yaml
│   ├── crypto_futures_{binance,bybit,kucoin}.yaml
│   ├── {indexes,commodities,macro}.yaml
│   ├── stocks/{yf,ibkr}/<universe>/<SYMBOL>/<SYMBOL>[-<session>]-<TF>.parquet
│   ├── cryptos/<exchange>/<symbol>/<symbol>-<TF>.parquet
│   ├── crypto-futures/<exchange>/<symbol>/<symbol>-<TF>.parquet
│   ├── indexes/yf/<SYMBOL>/<SYMBOL>-1d.parquet
│   ├── commodities/yf/<SYMBOL>/<SYMBOL>-1d.parquet
│   ├── macro/yf/<SYMBOL>/<SYMBOL>-1d.parquet
│   └── market-cap/coingecko/{exports,history,reference}/
├── CLAUDE.md
├── agents.md
├── ROADMAP.md
├── .env.example
└── pyproject.toml
```

## Provider-first path rule

Every asset class follows exactly one path shape, provider always first
under the class directory:

```
<asset-class>/<provider>[/<universe>]/<symbol>/<symbol>[-<session>]-<timeframe>.parquet
```

```mermaid
flowchart TD
    A["asset-class<br/>(stocks / cryptos / crypto-futures / indexes / commodities / macro)"]
    A --> B["provider<br/>(yf / ibkr / binance / bitget / bybit / kucoin)"]
    B --> C{"universe?<br/>(stocks only)"}
    C -->|yes| D["sp500 / nasdaq100 /<br/>russell2000 / delisted_stocks"]
    C -->|no| E[symbol]
    D --> E
    E --> F["SYMBOL[-session]-TIMEFRAME.parquet"]
```

- `universe` exists only for stocks.
- `session` (`all` / `overnight`) exists only for IBKR intraday stock data.
- Never write one provider's data into another provider's path, and never
  let one provider's catalog write clobber another provider's registration
  for the same `(symbol, timeframe)` — see the catalog shapes below.

## Catalog resolution

`assets.yaml` is not itself the full registry — it's an include list that
`utils.data.load_assets()` merges at read time:

```mermaid
flowchart LR
    AY["assets.yaml<br/>include: [...]"] --> M[load_assets merges all]
    SP[sp500.yaml] --> M
    ND[nasdaq100.yaml] --> M
    RS[russell2000.yaml] --> M
    DL[delisted_stocks.yaml] --> M
    IX[indexes.yaml] --> M
    CB[crypto_binance.yaml] --> M
    CBG[crypto_bitget.yaml] --> M
    CK[crypto_kucoin.yaml] --> M
    CBY[crypto_bybit.yaml] --> M
    CFB[crypto_futures_*.yaml] --> M
    CM[commodities.yaml] --> M
    MC[macro.yaml] --> M
    M --> R["single in-memory dict<br/>keyed by symbol"]
```

`market-cap/` is deliberately **not** included — it holds snapshot/ranking
exports, not per-symbol OHLCV series, so it doesn't fit the symbol-keyed
registry shape.

## Two catalog shapes

Stocks are the one asset class where two providers (YF, IBKR) can register
the *same* `(symbol, timeframe)`. Every other class has exactly one provider
per timeframe. The YAML shape reflects that:

```mermaid
flowchart TB
    subgraph Flat["Flat shape — crypto, indexes, commodities, macro"]
        F1["timeframe_dates:<br/>  1d: {start, end, file_path}"]
    end
    subgraph Nested["Nested-by-provider — stocks only"]
        N1["timeframe_dates:<br/>  1d:<br/>    YF:   {start, end, file_path}<br/>    IBKR: {start, end, file_path, session}<br/>  1h:<br/>    IBKR: {start, end, file_path, session}"]
    end
```

`utils.catalog.update_stock_timeframe_entry()` writes into the nested shape
and can never clobber a sibling provider's node, because they're different
dict keys. `utils.data.asset_path()` transparently handles both shapes so
callers don't need to know which one a given asset class uses.

## Module dependency graph

```mermaid
flowchart BT
    config[utils/config.py]
    catalog[utils/catalog.py]
    data[utils/data.py]
    gaps[utils/gaps.py]
    ibkr[utils/ibkr.py]
    market_caps[utils/market_caps.py]
    report[utils/report.py]

    semantics[utils/semantics.py]
    calendars[utils/calendars.py]

    catalog --> config
    catalog --> semantics
    data --> config
    gaps --> catalog
    gaps --> calendars
    ibkr --> catalog

    query[tools/query.py] --> catalog
    query --> data
    check_gaps[tools/check_gaps.py] --> gaps
    check_gaps --> catalog
    check_schema[tools/check_schema.py] --> catalog
    check_catalog[tools/check_catalog.py] --> catalog
    check_catalog --> semantics
    check_catalog --> report
    download_crypto[tools/download_crypto.py] --> catalog
    convert_crypto[tools/convert_crypto.py] --> catalog
    download_ibkr[tools/download_ibkr_stock_overnight.py] --> catalog
    download_ibkr --> gaps
    download_ibkr --> calendars
    download_ibkr --> ibkr
    download_ibkr_index[tools/download_ibkr_index.py] --> catalog
    download_ibkr_index --> ibkr
    query --> report
    check_gaps --> report
    retry_ibkr[tools/retry_ibkr_failed_sequentially.py] -.subprocess.-> download_ibkr
    download_market_caps[tools/download_market_caps.py] --> market_caps
    planning[utils/planning.py]
    yfmod[utils/yf.py]
    ibkr --> planning
    yfmod --> catalog
    download_yf[tools/download_yf.py] --> yfmod
    download_yf --> planning
    download_yf --> catalog
    download_yf --> semantics
```

`utils/market_caps.py` deliberately has no dependency on `utils/catalog.py`
— market-cap data isn't part of the symbol catalog (see above).
