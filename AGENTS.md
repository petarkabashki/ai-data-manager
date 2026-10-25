# Agents

This workspace manages price data across asset classes (equities, crypto, indexes,
commodities, macro) and providers (YF, IBKR, Binance, Bitget, Bybit, KuCoin).

## Environment

- **Python**: `uv run python` or activate `.venv/bin/activate`
- **Data root**: `price-data/` (symlink → `../price-data/`), overridable with
  `PRICE_DATA_ROOT`
- **Asset metadata**: `price-data/assets.yaml` (master index with includes)
- **Configuration**: every machine-specific value (data root, `FREQTRADE_BIN`,
  IBKR host/port/client-id, scratch root) resolves through `utils/config.py`
  from the environment. See `.env.example`. Never hardcode an absolute path.
- **Tests**: `uv run pytest` — no network, no `price-data/` required.

## Layout

```
ai-data-manager/
├── .agent/          # Agent skills: skills/<name>/SKILL.md with frontmatter
├── tools/           # Standalone executable tools (CLI scripts)
├── tests/           # pytest suite (no network, no price-data/ needed)
├── utils/           # Shared Python utilities (importable modules)
├── price-data/      # Symlink → ../price-data/
│   ├── assets.yaml          # Master asset registry (include: list of per-class YAMLs)
│   ├── stocks/
│   │   ├── yf/<universe>/<SYMBOL>/<SYMBOL>-1d.parquet
│   │   └── ibkr/<universe>/<SYMBOL>/<SYMBOL>-<session>-<TF>.parquet
│   ├── cryptos/<exchange>/<symbol>/<symbol>-<TF>.parquet
│   ├── crypto-futures/<exchange>/<symbol>/<symbol>-<TF>.parquet   # + <symbol>-funding.parquet
│   ├── futures/<provider>/<CONTRACT>/<CONTRACT>-<TF>.parquet   # ESU26, CLZ26 …
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
  The node key is `PROVIDER` for the default session and `PROVIDER:session`
  otherwise (`IBKR:overnight`), because `session` is part of the *filename* —
  without it in the key, downloading `--session all` and then
  `--session overnight` leaves two parquet files and one catalog node. Build
  keys with `utils.catalog.provider_node_key()`, never by hand.
- **Index catalogs** (`indexes.yaml`) use the same nested shape, for the same
  reason: YF owns every index's `1d`, and `download_ibkr_index.py` can request
  `1d` too. A pre-existing flat node is migrated to the nested shape on first
  write, so older YAML keeps working.
- **Crypto, crypto-futures, commodities and macro** keep the simpler flat
  shape — `timeframe_dates[tf]` is one node directly
  (`{start_date, end_date, file_path}`). For crypto the exchange is already
  part of the catalog key and the path, so there is no contention to guard
  against; don't add nesting there.
- File formats: `.parquet` everywhere (indexes' old dual CSV/parquet files
  were merged into one canonical parquet per symbol during the 2026-07
  provider-first migration).
- File paths in YAML are relative to the repo root.
- Asset classes: `Index`, `Crypto`, `Stock`, `Commodity`, `Macro`.
- Providers: `YF` (Yahoo Finance), `IBKR` (Interactive Brokers), `Binance`,
  `Bitget`, `Bybit`, `KuCoin`. Our symbol is the storage key everywhere; each
  provider's own spelling is translated at its boundary only
  (`utils.yf.yf_ticker()`, `utils.ibkr.INDEX_CONTRACT_MAP`,
  `utils.catalog.pair_to_symbol()`).
- Every OHLCV-shaped asset class — crypto, crypto-futures, stocks (YF and
  IBKR), indexes, and commodities — is stored on disk in the canonical
  schema below (`timestamp` int64 ms UTC + lowercase OHLCV float64). Raw YF
  exports still arrive as `Date` (datetime64) + `Open/High/Low/Close/
  [Adj Close]/Volume`; `normalize_ohlcv()` converts that shape on ingestion
  before anything is written. Macro's `WTB3MS` is the one exception (a plain
  rate series with `Value` in a DatetimeIndex, not OHLCV — don't push it
  through `normalize_ohlcv()`/`read_parquet_range()`).
- Perpetual **funding rates** sit next to a contract's bars as
  `crypto-futures/<exchange>/<SYMBOL>/<SYMBOL>-funding.parquet` (`timestamp`
  int64 ms at the settlement, rounded to the hour; `funding_rate`, a fraction
  per settlement, positive = longs pay shorts; `mark_price`, Binance only; Binance, Bybit and KuCoin are covered).
  Also not OHLCV: the catalog node is `timeframe_dates.funding` on the
  contract's `crypto_futures_<exchange>.yaml` entry, `schema: funding_rate`,
  and `funding` is not in the entry's `timeframes` list or its bounds.
  `tools/download_funding.py` owns them (they were py-qwant's
  `scripts/sync_funding_rates.py` until 2026-10).

## Trading calendars (RULE — never judge a gap by the 24/7 clock)

Crypto trades continuously, so any missing bar is a gap. Nothing else does.
Judging a daily S&P 500 series by the 24/7 rule reports every weekend and every
market holiday as a hole — on a complete six-year series, 327 gaps and 683
candles that were never supposed to exist. Worse, the IBKR overnight planner
turned each one into a historical request IBKR could never satisfy, on every
run.

`utils/calendars.py` supplies the rule. `detect_gaps(path, timeframe, calendar)`
reports a gap only where the calendar says bars were expected:

| Calendar | Sessions |
|---|---|
| `continuous` | always open — crypto and crypto-futures |
| `us_equity_rth` | 09:30–16:00 ET (13:00 on early closes) — the default for stocks and indexes |
| `us_equity_extended` | 04:00–20:00 ET — the default for commodities |
| `us_equity_overnight` | pre-market 04:00–09:30 and post-market close–20:00, matching what `--session overnight` stores |
| `cme_futures` | Globex: Sun 18:00 ET → Fri 17:00 ET, daily 17:00–18:00 ET halt — futures |

Holiday and early-close rules are computed, not tabulated (plus a small set of
ad-hoc closures like Hurricane Sandy), so there's no calendar package to
install and no table to maintain. `calendar_for(asset_class, session)` picks
the right one; `--calendar` overrides it on the CLI.

Daily bars are expected iff their UTC date is a trading day — which is exactly
how both YF and IBKR stamp daily bars. Intraday expectation is an *overlap*
test against the session window, not a reconstruction of the provider's bar
grid: this suppresses gaps a closure explains rather than predicting bars that
may not exist. The trade-off is deliberate and one-directional — a false gap
triggers an unsatisfiable download, a missed one shows up as staleness.

## Data semantics (RULE — every node carries these)

The canonical schema guarantees every OHLCV file has the same columns and the
same timestamp units. It cannot guarantee that `close` means the same thing in
two of them — Yahoo's `Adj Close` is dropped at ingestion, so YF closes are
unadjusted, while IBKR's `--what-to-show ADJUSTED_LAST` returns adjusted ones.
Same column, same dtype, different number.

So every `timeframe_dates` node records four fields, stamped by the catalog
writers from `utils/semantics.py`:

| Field | Values | Means |
|---|---|---|
| `schema` | `ohlcv`, `rate_series` | Whether the canonical schema applies at all. `macro/WTB3MS` is `rate_series` — that exclusion used to be convention, now it's data |
| `adjustment` | `raw`, `adjusted`, `not_applicable` | Corporate-action adjustment. Crypto and indexes are `not_applicable` |
| `volume_basis` | `base_asset`, `contracts`, `shares`, `none` | What a unit of `volume` counts. `none` means the column is not meaningful (indexes) |
| `bar_label` | `open`, `close` | Whether `timestamp` marks the bar's start or end. Every current provider stamps the open |

Never hand-write these — pass `semantics=` to a writer to override, otherwise
`semantics_for(asset_class, provider, what_to_show)` supplies the default.
`tools/check_catalog.py` verifies they're present and valid, and `--fix`
backfills nodes written before this existed.

Two defaults are **stated assumptions**, documented in `utils/semantics.py`
and worth verifying against exchange docs before you size anything on them:
Binance USDⓈ-M and Bybit linear klines report volume in base asset, KuCoin
futures in contracts.

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

To write or merge, always use `write_parquet_atomic()` / `merge_ohlcv_parquet()`
from `utils.catalog` — never `df.to_parquet(dst)` onto a live file. A merge has
already read the destination's rows into memory, so a crash mid-write would
destroy the only copy.

To bulk-convert legacy files or newly introduced non-canonical files:
```bash
uv run python tools/convert_crypto.py [--exchange X] [--dry-run]
```

## Asset class coverage

| Class      | Location                        | Format  | Notes                                    |
|------------|----------------------------------|---------|-------------------------------------------|
| Indexes    | price-data/indexes/{yf,ibkr}/     | parquet | DJI, GSPC, IXIC, NDX, RUT, VIX… (IBKR: intraday, US indices only) |
| Stocks     | price-data/stocks/{yf,ibkr}/     | parquet | sp500, nasdaq100, russell2000, delisted_stocks |
| Crypto     | price-data/cryptos/<exch>/       | parquet | Binance, Bitget, Bybit, KuCoin             |
| Crypto-futures | price-data/crypto-futures/<exch>/ | parquet | Binance, Bybit, KuCoin                 |
| Commodities| price-data/commodities/yf/       | parquet | GC=F (Gold)                                |
| Macro      | price-data/macro/yf/             | parquet | WTB3MS (T-bill; not OHLCV-shaped)          |
| Market-cap | price-data/market-cap/coingecko/ | parquet | Not in assets.yaml — snapshot/history exports, not per-symbol OHLCV |

## Tools

| Tool | Purpose |
|------|---------|
| `tools/status.py` | One roll-up: coverage, staleness and (with `--deep`) file/metadata health across every class |
| `tools/query.py` | Query available series — any asset class, any provider — with date ranges and staleness |
| `tools/check_gaps.py` | Audit any catalog for internal gaps and staleness, against its trading calendar |
| `tools/check_schema.py` | Verify every OHLCV-classed parquet file matches the canonical schema |
| `tools/check_catalog.py` | Verify every catalog node has valid semantics and matches its file; `--fix` backfills |
| `tools/check_splits.py` | Find un-back-adjusted split seams inside stored files — history at the old share count spliced to bars at the new one |
| `tools/fix_daily_stamps.py` | Restamp a provider's daily/weekly/monthly bars to 00:00 UTC of their date and drop duplicate dates, with backups |
| `tools/download_crypto.py` | Download via freqtrade, merge parquet, update YAML metadata |
| `tools/download_funding.py` | Fetch perpetual funding-rate history (Binance, Bybit, KuCoin) next to each contract's bars |
| `tools/convert_crypto.py` | Bulk normalize all crypto parquet to canonical schema + JSON→parquet |
| `tools/download_market_caps.py` | Fetch CoinGecko historical market-cap data and export snapshot/top-N universes |
| `tools/download_ibkr_stock_overnight.py` | Download IBKR US stock bars for `sp500` / `nasdaq100`, with optional overnight-only filtering |
| `tools/download_ibkr_index.py` | Download IBKR intraday index bars (e.g. 1h) for US indices, merged alongside YF's daily data |
| `tools/retry_ibkr_failed_sequentially.py` | Re-run the symbols an IBKR stock download failed on, one timeframe at a time |
| `tools/download_yf.py` | Download Yahoo bars for stock universes, indexes and commodities |
| `tools/import_futures.py` | Import a bulk per-contract futures archive from any vendor into `futures/<provider>/` |
| `tools/download_deribit_options.py` | Download Deribit crypto option history (the only venue serving expired options) into `options/<provider>/` |
| `tools/pdm_server.py` | The price data manager server: serves catalogs, series and bars over ZeroMQ, queues download jobs, broadcasts new and live bars — see [`docs/server.md`](docs/server.md) |
| `tools/pdm_client.py` | CLI client for the PDM server — query, fetch bars, coverage, approved refreshes, manage schedules and live feeds, submit/watch jobs, subscribe to broadcasts; every command takes `--json` |
| `tools/pdm_mcp.py` | MCP server (stdio) exposing the PDM server to agents as native tools — read-only tools separate from writes; registered in `.mcp.json` |

### status.py

```bash
uv run python tools/status.py               # coverage + staleness, catalogs only
uv run python tools/status.py --deep        # + files, schema, semantics
uv run python tools/status.py --json        # for an agent
```

The roll-up across `check_gaps.py`, `check_schema.py` and `check_catalog.py`.
Fast by default — YAML loads, no parquet reads; `--deep` opens every file and
exits non-zero on any problem, so it works as a cron/CI health check.

### query.py

```bash
uv run python tools/query.py --catalog crypto_binance --timeframe 1d
uv run python tools/query.py --asset-class Stock --symbol AAPL
uv run python tools/query.py --provider IBKR --timeframe 1h
uv run python tools/query.py --stale-days 30          # only stale series
uv run python tools/query.py --list-catalogs
uv run python tools/query.py --list-symbols --asset-class Index
uv run python tools/query.py --asset-class Crypto --json
```

Covers every asset class, not just crypto. Provider matching is
case-insensitive. Dates come from the catalog by default; `--rescan` reads the
parquet files instead — authoritative but it opens every one, so it's opt-in
rather than the default it used to be.

### check_gaps.py

```bash
uv run python tools/check_gaps.py --exchange binance --timeframe 1d
uv run python tools/check_gaps.py --all                   # all crypto exchanges, 1d
uv run python tools/check_gaps.py --exchange binance --internal  # show gap details
uv run python tools/check_gaps.py --exchange binance --stale-days 7

# Non-crypto, each with its own calendar:
uv run python tools/check_gaps.py --catalog sp500 --timeframe 1d
uv run python tools/check_gaps.py --catalog indexes --internal
uv run python tools/check_gaps.py --all-catalogs --json
uv run python tools/check_gaps.py --catalog sp500 --calendar us_equity_extended
```

`--exchange`/`--all` keep the crypto-shaped path (and `--trading-mode
futures`); `--catalog`/`--all-catalogs` audit any catalog in the registry. The
calendar is derived from the asset class and the node's stored session unless
`--calendar` overrides it, and the summary prints which calendar was used.

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

### check_catalog.py

```bash
uv run python tools/check_catalog.py                      # everything
uv run python tools/check_catalog.py --catalog indexes     # one catalog
uv run python tools/check_catalog.py --json                # for agents
uv run python tools/check_catalog.py --fix                 # backfill + migrate
```

The layer above `check_schema.py`: that one checks a file's columns, this one
checks what the catalog claims about it. Flags nodes with missing or invalid
semantics, missing files, dates that disagree with the file, flat nodes in a
nested catalog, and parquet files on disk no catalog references. `--fix` only
ever *adds* information — it never deletes a node or touches a parquet file,
and it refuses to rewrite a semantic value that is present but invalid.

### download_crypto.py

```bash
# Append new data for existing pairs
uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d 4h

# Download a new symbol from scratch
uv run python tools/download_crypto.py --exchange binance --pairs SOL/USDT --timeframes 1d 4h 1h --full

# Prepend historical data
uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT --timeframes 1d --prepend --since 2017-01-01
```

### download_funding.py

```bash
# Append new settlements for every contract in the catalog
uv run python tools/download_funding.py --exchange binance

# A few contracts; plan only
uv run python tools/download_funding.py --exchange bybit --symbols BTC_USDT ETH_USDT --dry-run
```

### fix_daily_stamps.py

```bash
# What would change; then the real run (through PDM, in the equity lane)
uv run python tools/fix_daily_stamps.py --provider yf --dry-run
uv run python tools/pdm_client.py submit fix_daily_stamps -- --provider yf --json
```

### download_yf.py

```bash
# Refresh a stock universe's daily bars
uv run python tools/download_yf.py --universe sp500 --timeframes 1d

# A few symbols from scratch
uv run python tools/download_yf.py --universe sp500 --symbols AAPL MSFT \
    --timeframes 1d --full --since 2000-01-01

# Indexes and commodities
uv run python tools/download_yf.py --asset-class Index --timeframes 1d
uv run python tools/download_yf.py --asset-class Commodity --timeframes 1d

# Adjusted closes instead of as-traded
uv run python tools/download_yf.py --universe sp500 --timeframes 1d --adjusted
```

Notes:
- **Our symbols are not Yahoo's.** Indexes take a `^` prefix (`GSPC` → `^GSPC`)
  and share classes a dash (`BRK.B` → `BRK-B`). `utils.yf.yf_ticker()` owns the
  mapping — never build a ticker by hand. Getting it wrong doesn't raise:
  Yahoo returns an empty frame for an unknown ticker, so an empty result means
  "nothing came back", never "already current".
- **Yahoo has no 4h or 8h bar.** Those timeframes are rejected rather than
  silently substituted; use IBKR for intraday equity bars.
- **Intraday history is capped** — roughly 730 days of 60m bars, 7 days of 1m —
  and Yahoo truncates silently rather than erroring, so `clamp_start()` pulls
  the request forward and says so.
- `--adjusted` sets `auto_adjust`, and the choice is written to the node's
  `adjustment` field either way. The default is as-traded, matching what is
  already on disk.
- The catalog is saved after each series, so a long run stays resumable.
- Macro (`WTB3MS`): `--asset-class Macro --timeframes 1d`. It is a rate series, not OHLCV.
  The file is stored from Yahoo `^IRX` (`utils/yf.MACRO_TICKERS`), the close becomes `Value`,
  and `merge_rate_series()` keeps the `Value`-on-`Date` layout. Daily only; `--adjusted` is refused.

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
- `--use-rth` controls the RTH flag. The default, `auto`, requests
  regular-hours-only bars for `1d`/`1w` and extended-hours bars for intraday
  timeframes; `always`/`never` override it. (This tool always behaved this way;
  the docs previously claimed `useRTH=0` unconditionally.)
- With `--session overnight`, it additionally keeps only bars fully outside
  `09:30-16:00 America/New_York`, and the session is recorded in both the
  filename and the catalog node key.
- Intraday files are written under
  `price-data/stocks/ibkr/<universe>/<SYMBOL>/<SYMBOL>-<session>-<TF>.parquet`
  via `utils.catalog.stock_file_path()`.
- Existing stock parquet files are scanned first; the downloader only requests
  prepend history, internal gaps, or trailing missing bars unless `--full` is set.
- Progress is checkpointed to
  `tmp/ibkr_stock_progress_<session>_<timeframes>.json` with append-only resume
  events in `tmp/ibkr_stock_resume_<session>_<timeframes>.jsonl`, so concurrent
  or sequential runs don't overwrite each other — and so
  `retry_ibkr_failed_sequentially.py` can find them.
- Daily YF entries stay intact — `update_stock_timeframe_entry()` writes IBKR's
  data into its own `timeframe_dates[tf]["IBKR"]` node, never touching YF's
  sibling node for the same timeframe (see schema rule above).
- Watch out for IBKR's historical-data pacing limits (not the general 50 msg/s
  API limit) if you increase throughput: sustained aggressive request rates —
  either very short `--sleep-seconds` on one connection, or multiple parallel
  connections — have caused both a burst pacing-violation lockout and a
  multi-hour dead-connection hang in practice. Prefer a single connection at a
  moderate `--sleep-seconds` (0.3-1.0) over parallelizing across client-ids.

### download_ibkr_index.py

```bash
# 1h bars for all mapped US indices (GSPC, DJI, IXIC, NDX, RUT, VIX)
uv run python tools/download_ibkr_index.py --timeframes 1h --since 2024-01-01

# Smoke test on a couple of symbols
uv run python tools/download_ibkr_index.py --symbols GSPC NDX --timeframes 1h --limit 2
```

Notes:
- Only resolves the US indices in `utils.ibkr.INDEX_CONTRACT_MAP` (`GSPC`→SPX/CBOE,
  `DJI`→INDU/CME, `IXIC`→COMP/NASDAQ, `NDX`→NDX/NASDAQ, `RUT`→RUT/RUSSELL,
  `VIX`→VIX/CBOE) via IBKR `IND` contracts. Non-US indices (STOXX50E, FTSE,
  GDAXI, N225, HSI) aren't mapped — add an entry to `INDEX_CONTRACT_MAP` if
  you need one, along with its correct exchange/currency.
- Writes to `price-data/indexes/ibkr/<SYMBOL>/<SYMBOL>-<TF>.parquet` via
  `utils.catalog.index_file_path()`, and registers its own node under
  `timeframe_dates[tf]["IBKR"]` via `update_index_timeframe_entry()` — so
  requesting a timeframe YF already covers adds a sibling node instead of
  replacing YF's (see schema rule above).
- Indices have no real trade volume; `--what-to-show` defaults to `TRADES`
  (works in practice for IBKR's index feeds) with `MIDPOINT` as a fallback
  if a symbol returns no data.
- Connect TWS or IB Gateway first, same as `download_ibkr_stock_overnight.py`.

## Network service (PDM)

The data lives on one host (see [`docs/server.md`](docs/server.md)) and is
served to every other machine by `tools/pdm_server.py`. Clients — people,
agents, strategies — never mount `price-data/`; they ask the server.

- **Agents on another machine**: `uv run python tools/pdm_client.py --json
  <command>` is the network twin of the read-only tools — `series` ≈
  `query.py`, `status` = `status.py --json`, `bars` returns the canonical
  frame. Writes go through `submit`, which queues one of the existing tools.
- **Agents with MCP**: `tools/pdm_mcp.py` (registered in `.mcp.json`) gives
  the same as native tools. Reads (`pdm_series`, `pdm_bars`, `pdm_coverage`,
  `pdm_refresh_preview` …) are separate tools from writes (`pdm_refresh_run`,
  `pdm_schedule_*`, `pdm_live_*`), so the harness's permission prompt is the
  approval step. Always preview a refresh before asking to run it.
- **Refreshes**: the `refresh` method takes a typed plan (target, timeframes,
  symbols, since, full) and a required `dry_run`; a real run is re-checked by
  a dry run, one at a time, killed after 30 minutes. Shape: docstring of
  `pdm/refresh.py`.
- **Schedules and live feeds** are managed at runtime (`schedule_*`,
  `live_add` / `live_remove`) and written back to `pdm.yaml`. `coverage
  <symbol>` answers "what is stored, what refreshes it, what streams it".
- **Audits without the token**: the `audit` method (CLI `audit`, MCP
  `pdm_audit`) runs `status`, `query` and the `check_*` tools on the host and
  returns their JSON. The options are typed, and nothing in them can write.
- **HTTP**: a read-only gateway on port 5580 (`/v1/<method>`) serves the same
  reads, audits and refresh previews to clients that can't use ZMQ. It never
  writes.
- `bars` replies are capped at 100,000 rows and refused in words beyond that.
  Always pass `start`/`end` or `limit`.
- **Python**: `from pdm.client import PriceDataClient`.
- The server only ever runs tools from the whitelist in `pdm/jobs.py`, as an
  argv list, never a shell string. Tools that write the same catalog share a
  lane and run one at a time (YF and IBKR both write `sp500.yaml`).
- Live bars and quotes (`live/...`, `quote/...`, py-qwant's `prices`) are
  broadcast only, never written to disk. Stored history is still the product
  of the download tools.

## Agent Skills

Skills live in `.agent/skills/<name>/SKILL.md` with YAML frontmatter (`name`,
`description`) — the same layout as the sibling `py-qwant` repo. The
frontmatter is what makes them **discoverable**: a harness can scan the
directory instead of relying on someone having read this table. The content is
harness-neutral; point a harness at it rather than keeping copies that drift:

```bash
ln -s ../.agent/skills .claude/skills
ln -s ../.agent/skills .codex/skills
```

| Skill | File | Description |
|-------|------|-------------|
| crypto_download | `.agent/skills/crypto_download/SKILL.md` | Check gaps → download via freqtrade → merge → verify the catalog |

## Utilities (importable)

| Module | Key exports |
|--------|-------------|
| `utils/data.py` | `load_assets()`, `load_ohlcv()`, `asset_path()` (handles both flat and nested-by-provider `timeframe_dates` shapes) |
| `utils/config.py` | `DATA_ROOT`, `TMP_ROOT`, `FREQTRADE_BIN`, `IBKR_HOST/PORT/CLIENT_ID`, `relative_to_repo()`, `resolve_from_repo()` |
| `utils/catalog.py` | `normalize_ohlcv()`, `empty_ohlcv_frame()`, `write_parquet_atomic()`, `merge_ohlcv_parquet()`, `read_parquet_range()`, `provider_node_key()`, `recompute_entry_bounds()`, `load_*_catalog()`/`save_*_catalog()`, `*_file_path()`, `update_*_timeframe_entry()`, `refresh_*_catalog_dates()`, `TF_TO_MS` |
| `utils/gaps.py` | `detect_gaps(path, tf, calendar)`, `audit_catalog_series()` (any catalog), `audit_exchange()` (crypto), `SymbolAudit` |
| `utils/ibkr.py` | `IbkrHistoricalClient`, `plan_download_ranges()`, `download_ranges()`, `DownloadRange`, `chunk_days_for_timeframe()`, `filter_full_bars_outside_rth()` |
| `utils/report.py` | `render_table()`, `print_table()`, `allow_broken_pipe()` |
| `utils/market_caps.py` | `CoinGeckoClient`, `resolve_coin_ids()`, `fetch_or_load_history()`, `rank_top_n()` |
| `utils/semantics.py` | `Semantics`, `semantics_for()`, `validate_node_semantics()`, `apply_semantics()` |
| `utils/calendars.py` | `CONTINUOUS`, `CME_FUTURES`, `UsEquityCalendar`, `CmeFuturesCalendar`, `calendar_for()`, `calendar_by_name()`, `us_market_holidays()` |
| `utils/planning.py` | `DownloadRange`, `plan_download_ranges()`, `merge_ranges()`, `daterange_chunks()` — provider-neutral |
| `utils/futures.py` | `FuturesContract`, `contract_for()`, `contract_symbol()`, `parse_contract_symbol()`, `futures_file_path()`, `futures_catalog_path()`, `ROOTS`, `MONTH_CODES` — provider-neutral contract identity |
| `utils/options.py` | `OptionContract`, `contract_for()`, `option_symbol()`, `parse_option_symbol()`, `parse_deribit_name()`, `option_file_path()`, `update_option_entry()`, `UNDERLYINGS` — provider-neutral option identity |
| `utils/deribit.py` | `DeribitClient`, `DeribitError`, `InstrumentInfo`, `TIMEFRAME_TO_RESOLUTION` — public REST client; pages the 5001-bar cap and trims post-expiry padding |
| `utils/yf.py` | `yf_ticker()`, `yf_interval()`, `clamp_start()`, `frame_from_yahoo()`, `fetch_ohlcv()`, `YahooFetcher` |

The `pdm/` package (server side of the network service) is not under `utils/`
because nothing in the tools imports it: `protocol.py` (wire format, topics),
`server.py`, `client.py` (`PriceDataClient`), `jobs.py` (lanes + whitelist),
`scheduler.py`, `watch.py` (new-bar detection), `index.py`, `live.py` (ccxt
polling).

`tools/migrate_price_data_layout.py` is the one-off script that performed the
2026-07 provider-first migration (phases A-G, each idempotent + `--dry-run`).
Kept for reference/re-run if a similar restructuring is ever needed again.

## Adding a new tool

1. Place the executable script in `tools/`, shebang `#!/usr/bin/env -S uv run python`.
2. Resolve machine-specific values through `utils/config.py`; never hardcode a path.
3. Route every write through `merge_ohlcv_parquet()` / `write_parquet_atomic()`.
4. Add a row to the Tools table above **and** a section in
   [`docs/cli-reference.md`](docs/cli-reference.md).
5. Add a test under `tests/` — the suite needs no network and no `price-data/`,
   so planning logic has no excuse to be untested.

`tests/test_docs_consistency.py` fails if a tool is missing from either the
table or the CLI reference. That test exists because the tool table was once
duplicated across four files and had already drifted.

## Adding a new agent skill

1. Create `.agent/skills/<name>/SKILL.md` starting with frontmatter:

   ```yaml
   ---
   name: <name>
   description: What it does and when to reach for it — a harness matches
     against this, so name the triggers explicitly.
   ---
   ```

2. Body: the tool chain, standard invocations, the rules that apply to every
   write, and the gotchas. Concrete commands beat prose.
3. Add a row to the table above.

`tests/test_docs_consistency.py` fails if a skill lacks frontmatter or is
missing from that table.
