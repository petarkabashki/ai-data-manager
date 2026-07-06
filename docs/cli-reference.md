# CLI reference

All tools live in `tools/` and run via `uv run python tools/<tool>.py`. All
are safe to run from the repo root; they insert the repo root onto
`sys.path` themselves.

Tools marked **read-only** never write to `price-data/`. Tools marked
**write** create or modify parquet/YAML — run the write tools' `--dry-run`
first when one is offered.

| Tool | Mode | Purpose |
|---|---|---|
| [`query.py`](#querypy) | read-only | Query assets, timeframes, date ranges, staleness |
| [`check_gaps.py`](#check_gapspy) | read-only | Audit crypto data for internal gaps + trailing staleness |
| [`check_schema.py`](#check_schemapy) | read-only | Verify every OHLCV parquet matches the canonical schema |
| [`download_crypto.py`](#download_cryptopy) | write | Download via freqtrade, merge parquet, update catalog |
| [`convert_crypto.py`](#convert_cryptopy) | write | Bulk-normalize crypto parquet/JSON to canonical schema |
| [`download_market_caps.py`](#download_market_capspy) | write | Fetch CoinGecko market-cap history + snapshot/top-N exports |
| [`download_ibkr_stock_overnight.py`](#download_ibkr_stock_overnightpy) | write | Download IBKR US-stock bars for sp500/nasdaq100 |
| [`retry_ibkr_failed_sequentially.py`](#retry_ibkr_failed_sequentiallypy) | write | Re-run failed IBKR symbols one connection at a time |
| [`migrate_price_data_layout.py`](#migrate_price_data_layoutpy) | write (one-off) | 2026-07 provider-first layout migration, phases A–G |

---

## `query.py`

```
uv run python tools/query.py [--exchange EX] [--symbol SYM] [--timeframe TF]
                              [--stale-days N] [--list-exchanges] [--list-symbols]
```

| Flag | Meaning |
|---|---|
| `-e, --exchange` | Filter to one crypto exchange (`binance`, `bitget`, `bybit`, `kucoin`) |
| `-s, --symbol` | Filter to one symbol, e.g. `BTC_USDT` |
| `-t, --timeframe` | Filter to one timeframe, e.g. `1d`, `4h` |
| `--stale-days N` | Only show entries stale by ≥ N days |
| `--list-exchanges` | Print exchange → symbol-count table and exit |
| `--list-symbols` | Print only matching symbol names, one per line |

```bash
uv run python tools/query.py --exchange binance --timeframe 1d
uv run python tools/query.py --exchange binance --symbol BTC_USDT
uv run python tools/query.py --stale-days 30
uv run python tools/query.py --list-exchanges
uv run python tools/query.py --list-symbols --exchange binance --timeframe 1d
```

Currently crypto-only (reads `load_crypto_catalog`) — it doesn't yet cover
stocks/indexes/commodities/macro.

---

## `check_gaps.py`

```
uv run python tools/check_gaps.py (--exchange EX | --all) [--symbol SYM]
                                   [--timeframe TF | --timeframes TF...]
                                   [--internal] [--stale-days N]
```

| Flag | Meaning |
|---|---|
| `-e, --exchange` | Exchange to audit |
| `--all` | Audit all exchanges, `1d` only, unless `--timeframe(s)` given |
| `-s, --symbol` | Single symbol |
| `-t, --timeframe` | Single timeframe |
| `--timeframes` | Multiple timeframes |
| `--internal` | Print internal-gap details (default: just a count) |
| `--stale-days N` | Minimum staleness to report (default: 1) |

```bash
uv run python tools/check_gaps.py --exchange binance --timeframe 1d
uv run python tools/check_gaps.py --exchange binance --symbol BTC_USDT
uv run python tools/check_gaps.py --exchange binance --timeframe 4h --stale-days 7
uv run python tools/check_gaps.py --all
uv run python tools/check_gaps.py --exchange binance --internal
```

Report sections: **ERRORS** (missing/unreadable files), **STALE DATA**
(sorted by staleness), **INTERNAL GAPS** (count, or detail with
`--internal`). Crypto-only — driven by `utils.gaps.audit_exchange()`.
`1Mo` candles are skipped (variable interval, gap math doesn't apply).

---

## `check_schema.py`

```
uv run python tools/check_schema.py [--path DIR]
```

| Flag | Meaning |
|---|---|
| `--path` | Limit to one subdirectory, e.g. `price-data/commodities`, instead of scanning all OHLCV classes |

```bash
uv run python tools/check_schema.py
uv run python tools/check_schema.py --path price-data/commodities
```

Scans `cryptos/`, `crypto-futures/`, `stocks/`, `indexes/`, `commodities/`
(macro and market-cap are intentionally excluded — different, documented
schemas). Read-only; exits non-zero and lists every mismatch if any file's
columns/dtypes don't exactly match `CANONICAL_COLS`. Run this after adding a
new tool or provider to catch anything that wrote raw output straight to
disk without `normalize_ohlcv()`.

---

## `download_crypto.py`

```
uv run python tools/download_crypto.py --exchange EX (--all-pairs | --pairs P...)
                                        --timeframes TF... [--full] [--prepend] [--since DATE]
```

```bash
# Append new data for existing pairs
uv run python tools/download_crypto.py --exchange binance --all-pairs --timeframes 1d 4h

# Download a new symbol from scratch
uv run python tools/download_crypto.py --exchange binance --pairs SOL/USDT \
  --timeframes 1d 4h 1h --full

# Prepend historical data
uv run python tools/download_crypto.py --exchange binance --pairs BTC/USDT \
  --timeframes 1d --prepend --since 2017-01-01
```

Wraps freqtrade's public (unauthenticated) OHLCV download, merges into
existing parquet with `normalize_ohlcv()` + dedup-on-timestamp, and updates
the exchange's YAML catalog. See [`docs/workflows.md`](workflows.md#crypto-refresh)
for the full refresh workflow and [`.agent/crypto_download.md`](../.agent/crypto_download.md)
for the agent-skill version.

---

## `convert_crypto.py`

```
uv run python tools/convert_crypto.py [--exchange EX] [--symbol SYM]
                                       [--dry-run] [--workers N] [--skip-metadata]
```

| Flag | Meaning |
|---|---|
| `-e, --exchange` | Limit to one exchange |
| `-s, --symbol` | Limit to one symbol |
| `--dry-run` | Show what would happen, no writes |
| `--workers` | Parallel worker threads (default: 8) |
| `--skip-metadata` | Skip the final YAML start/end-date refresh |

```bash
uv run python tools/convert_crypto.py --dry-run
uv run python tools/convert_crypto.py --exchange binance --dry-run
uv run python tools/convert_crypto.py
uv run python tools/convert_crypto.py --exchange binance
```

Four operations, in order: (1) legacy OHLCV JSON → parquet, merged with any
existing parquet, JSON deleted; (2) delete stray `download_report.json`
files; (3) normalize any non-canonical parquet in place (skips truly empty
files); (4) refresh YAML start/end dates from the actual files. Always
dry-run first on a new exchange.

---

## `download_market_caps.py`

```
uv run python tools/download_market_caps.py
    (--coin-ids ID... | --coin-ids-file F | --symbols SYM... | --symbols-file F
     | --all-active | --all-coins)
    (--date D | --dates D... | --dates-file F | --start D --end D)
    [--frequency daily|weekly|monthly] [--top-n N]
    [--exclude-ids ID...] [--exclude-symbols SYM...] [--exclude-stablecoins]
    [--vs-currency CCY] [--workers N] [--refresh-coin-list] [--refresh-history]
    [--output-format parquet|csv] [--history-output PATH] [--snapshot-output PATH]
    [--history-only]
```

| Group | Flags |
|---|---|
| Universe | `--coin-ids`, `--coin-ids-file`, `--symbols`, `--symbols-file`, `--all-active` (active coins only), `--all-coins` (active + inactive) |
| Dates | `--date` (repeatable), `--dates`, `--dates-file`, or `--start`/`--end` + `--frequency` |
| Ranking | `--top-n`, `--exclude-ids`, `--exclude-symbols`, `--exclude-stablecoins` |
| I/O | `--vs-currency` (default `usd`), `--workers` (default 4), `--refresh-coin-list`, `--refresh-history`, `--output-format`, `--history-output`, `--snapshot-output`, `--history-only` |

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

# Explicit CoinGecko ids when symbols are ambiguous
uv run python tools/download_market_caps.py \
  --coin-ids solana cardano chainlink \
  --dates 2025-09-01 2025-12-01 2026-03-01
```

Notes:
- Without `COINGECKO_PRO_API_KEY`, the public API limits historical chart
  requests to roughly the most recent 365 days — `--workers` is forced to 1
  in that mode to reduce rate-limit stalls.
- `--top-n` without `--all-active`/`--all-coins` only ranks within the
  explicitly supplied ids/symbols — the tool prints a warning.
- Outputs land under `price-data/market-cap/coingecko/` and are **not**
  registered in `assets.yaml` (see [`docs/data-model.md`](data-model.md)).

---

## `download_ibkr_stock_overnight.py`

```
uv run python tools/download_ibkr_stock_overnight.py --universe UNIV... [UNIV...]
    [--symbols SYM...] [--timeframes TF...] [--since DATE] [--full]
    [--host H] [--port P] [--client-id N] [--chunk-days N] [--overlap-days N]
    [--what-to-show TRADES|ADJUSTED_LAST] [--session all|overnight]
    [--sleep-seconds S] [--request-timeout S] [--max-chunk-retries N]
    [--limit N] [--resume-log PATH] [--progress-file PATH] [--dry-run]
```

| Flag | Meaning |
|---|---|
| `--universe` | `sp500` and/or `nasdaq100` — required |
| `--symbols` | Explicit subset (must exist in the universe catalog) |
| `--timeframes` | `1h 4h 8h 1d 1w`, default `1h 4h` |
| `--since` | Start date for new symbols / full refresh |
| `--full` | Ignore existing files, rebuild from `--since` (requires `--since`) |
| `--host` / `--port` / `--client-id` | TWS/Gateway connection (defaults `127.0.0.1:7497`, client id `73`) |
| `--chunk-days` | Historical-request chunk size; default adapts per timeframe (`1h`→365d, `4h`/`8h`→730d, `1d`→365d, `1w`→3650d) |
| `--overlap-days` | Overlap window when appending (default 5) |
| `--what-to-show` | `TRADES` (default) or `ADJUSTED_LAST` |
| `--session` | `all` (default) or `overnight` — keep only bars fully outside `09:30–16:00 America/New_York` |
| `--sleep-seconds` | Delay between requests (default 0.35) |
| `--request-timeout` | Per-request timeout before treating the chunk as failed (default 60s) |
| `--max-chunk-retries` | Retries for timeout/HMDS-disconnect errors before skipping (default 2) |
| `--limit` | Cap symbols per universe — smoke tests |
| `--resume-log` / `--progress-file` | Override the JSONL/JSON checkpoint paths |
| `--dry-run` | Plan without writing files or catalogs |

```bash
# S&P 500 intraday bars from IBKR
uv run python tools/download_ibkr_stock_overnight.py \
  --universe sp500 --timeframes 1h 4h --since 2024-01-01

# Nasdaq 100 overnight-only smoke test on a few symbols
uv run python tools/download_ibkr_stock_overnight.py \
  --universe nasdaq100 --symbols AAPL MSFT NVDA --timeframes 1h \
  --session overnight --limit 3

# Resume/fill only missing 8h + 1d bars
uv run python tools/download_ibkr_stock_overnight.py \
  --universe nasdaq100 --timeframes 8h 1d --since 2020-01-01 --port 4001
```

Requires TWS or IB Gateway running first, pointed at by `--host`/`--port`.
Existing files are scanned before any request is planned — it only asks for
prepend history, internal gaps, or trailing missing bars unless `--full` is
set. Progress checkpoints to `tmp/ibkr_stock_download_progress.json`;
append-only resume events go to `tmp/ibkr_stock_download_resume.jsonl`.

**Pacing warning**: sustained aggressive request rates — either a very
short `--sleep-seconds` on one connection, or multiple parallel connections
— have caused both a burst pacing-violation lockout and a multi-hour
dead-connection hang in practice. Prefer one connection at a moderate
`--sleep-seconds` (0.3–1.0) over parallelizing across client-ids.

---

## `retry_ibkr_failed_sequentially.py`

```
uv run python tools/retry_ibkr_failed_sequentially.py
    [--port P] [--since DATE] [--base-client-id N] [--sleep-seconds S]
    [--request-timeout S] [--max-chunk-retries N] [--timeframes 4h|1h ...]
```

| Flag | Meaning |
|---|---|
| `--port` | IBKR port (default 4001) |
| `--since` | Start date for retries (default `2020-01-01`) |
| `--base-client-id` | First client id used; incremented per timeframe (default 90) |
| `--sleep-seconds` | Delay between requests (default 1.0) |
| `--request-timeout` | Per-request timeout (default 25s) |
| `--max-chunk-retries` | Retries per chunk (default 2) |
| `--timeframes` | Which failed-timeframe sets to retry, in order (default `4h 1h`) |

```bash
uv run python tools/retry_ibkr_failed_sequentially.py --port 4001 --timeframes 4h 1h
```

Reads `tmp/ibkr_stock_download_progress_{4h,1h}.json`'s `failures` list for
the `nasdaq100` universe, then re-invokes
`download_ibkr_stock_overnight.py` as a subprocess once per timeframe,
sequentially — never in parallel — writing to its own
`tmp/ibkr_retry_<tf>_{progress.json,resume.jsonl}`. Stops on the first
non-zero exit code.

---

## `migrate_price_data_layout.py`

```
uv run python tools/migrate_price_data_layout.py --phase {A,B,C,D,E,F,G} [--dry-run]
```

One-off script that performed the 2026-07 provider-first restructuring.
Each phase is idempotent and dry-runnable; kept for reference or re-run if a
similar restructuring is ever needed:

| Phase | What it did |
|---|---|
| A | Dedupe `BF.B`/`BRK.B`, remove dead ETF entries, snapshot baseline |
| B | Move 27 stray YF stock files into `delisted_stocks.yaml` / `stocks/yf/delisted_stocks/` |
| C | Bulk YF stock migration for sp500/nasdaq100/russell2000 |
| D | Rewrite IBKR stock catalog entries into the nested-by-provider schema |
| E | Move indexes to provider-first parquet paths + rebuild `indexes.yaml` |
| F | Move commodities/macro to provider-first paths (schema unchanged) |
| G | Fix `assets.yaml`'s include list + clean up stray inline entries |

```bash
uv run python tools/migrate_price_data_layout.py --phase A --dry-run
uv run python tools/migrate_price_data_layout.py --phase A
```

Run phases strictly in order A→G; each assumes the previous phase's output.
