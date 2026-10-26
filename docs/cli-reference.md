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
| [`check_splits.py`](#check_splitspy) | read-only | Find un-back-adjusted split seams inside stored price files |
| [`fix_daily_stamps.py`](#fix_daily_stampspy) | write | Restamp daily+ bars to 00:00 UTC of their date; drop duplicate dates |
| [`download_crypto.py`](#download_cryptopy) | write | Download via freqtrade, merge parquet, update catalog |
| [`download_funding.py`](#download_fundingpy) | write | Fetch perpetual funding-rate history next to each contract's bars |
| [`convert_crypto.py`](#convert_cryptopy) | write | Bulk-normalize crypto parquet/JSON to canonical schema |
| [`download_market_caps.py`](#download_market_capspy) | write | Fetch CoinGecko market-cap history + snapshot/top-N exports |
| [`download_ibkr_stock_overnight.py`](#download_ibkr_stock_overnightpy) | write | Download IBKR US-stock bars for sp500/nasdaq100 |
| [`retry_ibkr_failed_sequentially.py`](#retry_ibkr_failed_sequentiallypy) | write | Re-run failed IBKR symbols one connection at a time |
| [`pdm_server.py`](#pdm_serverpy) | server | Serve price-data over ZeroMQ; run jobs; broadcast bars |
| [`pdm_client.py`](#pdm_clientpy) | client | Query/fetch/submit/watch against a PDM server |
| [`migrate_price_data_layout.py`](#migrate_price_data_layoutpy) | write (one-off) | 2026-07 provider-first layout migration, phases A–G |

---

## `status.py`

```
uv run python tools/status.py [--catalog NAME ...] [--stale-days N] [--deep] [--json]
```

| Flag | Meaning |
|---|---|
| `--catalog` | Limit to named catalogs |
| `--stale-days` | N or more session days overdue counts as stale (default 2); see below |
| `--deep` | Also check files exist, match the canonical schema, and carry semantics |
| `--json` | Machine-readable output |

**Stale** means session days overdue (`utils.planning.days_behind`). A series is
overdue once the bar after its last one has closed and it still lacks it. The
count runs from then: trading days on an equity calendar, every day on
crypto's. A current series is 0, one whose newest bar closed since the last
scheduled run is 1, and a daily series missing its last N bars is N. The next
bar comes from the series' own last one, so a 3-day or weekly series isn't
stale while its next bar is still forming, whatever its venue's grid.
`query.py`, PDM's `series` and the stale alert use the same count.
**Retired** series (a crypto pair the venue delisted, marked by
`download_crypto.py --universe`) are counted apart and never stale.

The roll-up over the three audits. Shallow by default — a few YAML loads, no
parquet reads — so it's cheap enough to run constantly. `--deep` opens every
file and exits non-zero if anything is wrong, which makes it usable as a cron
or CI health check. For detail on a problem it surfaces, go to the specific
audit: `check_catalog.py` for metadata, `check_schema.py` for file shape,
`check_gaps.py` for holes.

```bash
uv run python tools/status.py
uv run python tools/status.py --deep --json
```

---

## `query.py`

```
uv run python tools/query.py
    [--catalog NAME ...] [--asset-class CLASS ...] [--symbol S | --symbols S ...]
    [--timeframe TF | --timeframes TF ...] [--provider P] [--stale-days N]
    [--rescan] [--list-catalogs] [--list-symbols] [--json] [--limit N]
```

| Flag | Meaning |
|---|---|
| `--catalog` / `--asset-class` | Narrow by catalog name or by `Crypto`, `Crypto Futures`, `Stock`, `Index`, `Commodity`, `Macro` |
| `--symbol(s)` / `--timeframe(s)` | The usual filters |
| `--provider` | `YF`, `IBKR`, `binance`… case-insensitive, so callers needn't know that directories are lowercase and catalog nodes upper |
| `--stale-days` | Only series N or more session days overdue (as in `status.py`) |
| `--rescan` | Read dates from the parquet files rather than the catalog — authoritative, but opens every file |
| `--list-catalogs` | Catalogs with symbol and series counts |
| `--list-symbols` | Matching symbol names, deduped, one per line |
| `--json` | Machine-readable; unlike the table it is not truncated by `--limit` |

Covers every asset class. It previously read crypto only — despite the name,
the docstring, and importing `load_assets()` without ever calling it — and
re-read every parquet file on every run.

```bash
uv run python tools/query.py --asset-class Stock --symbol AAPL
uv run python tools/query.py --provider IBKR --timeframe 1h --json
uv run python tools/query.py --list-catalogs
```

Each row carries the node's `session`, `adjustment` and `volume_basis`, so an
agent can tell adjusted from as-traded without opening anything.

---

## `check_gaps.py`

```
uv run python tools/check_gaps.py
    [--exchange EX | --all | --catalog NAME ... | --all-catalogs]
    [--symbol S | --symbols S ...] [--timeframe TF | --timeframes TF ...]
    [--internal] [--stale-days N] [--trading-mode spot|futures]
    [--calendar continuous|us_equity_rth|us_equity_extended|us_equity_overnight]
    [--json]
```

| Flag | Meaning |
|---|---|
| `--exchange` / `--all` | Crypto path: one exchange, or all four. Supports `--trading-mode futures` |
| `--catalog` / `--all-catalogs` | Any catalog by name (`sp500`, `indexes`, `commodities`, `crypto_binance`…) |
| `--timeframe(s)` | Restrict to these timeframes. `--all` defaults to `1d` |
| `--internal` | Print each internal gap, not just the count |
| `--stale-days` | Minimum trailing staleness to report (default 1) |
| `--calendar` | Override the calendar instead of deriving it from the asset class |
| `--json` | Machine-readable output, including the calendar each series was judged against |

**Gaps are judged against a trading calendar.** Crypto uses `continuous`;
stocks and indexes default to `us_equity_rth`, commodities to
`us_equity_extended`, and a node stored with `session: overnight` to
`us_equity_overnight`. Without this, a complete six-year daily equity series
reported 327 gaps and 683 missing candles — all of them weekends and holidays.

```bash
uv run python tools/check_gaps.py --exchange binance --timeframe 1d
uv run python tools/check_gaps.py --catalog sp500 --timeframe 1d --internal
uv run python tools/check_gaps.py --all-catalogs --json
uv run python tools/check_gaps.py --catalog sp500 --calendar us_equity_extended
```

Intraday expectation is an overlap test against the session window rather than
a reconstruction of the provider's bar grid, so intraday candle counts are
approximate and a genuinely missing bar inside a closure window won't be
reported. That direction is deliberate: a false gap becomes a download request
that can never succeed, a missed one shows up as staleness.

---

## `check_schema.py`

Add `--json` for machine-readable output: `{"checked": N, "mismatches": [{"path", "issue"}]}`.
Exits non-zero if any file mismatches, in either output mode. A `-1d`/`-1w`/`-1Mo`
file also mismatches when a bar is not at 00:00 UTC or a date repeats
(`issue` names the counts; `fix_daily_stamps.py` repairs it).

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

## `check_catalog.py`

```
uv run python tools/check_catalog.py
    [--catalog NAME ...] [--fix] [--skip-dates] [--skip-orphans] [--json]
```

| Flag | Meaning |
|---|---|
| `--catalog` | Limit to named catalogs (`indexes`, `sp500`, `crypto_binance`, `macro`, …). Suppresses the orphan scan |
| `--fix` | Backfill missing semantics, migrate flat nodes, refresh drifted dates, adopt orphan crypto files |
| `--skip-dates` | Don't open parquet files to verify dates — much faster on 5,000 symbols |
| `--skip-orphans` | Don't walk the data tree looking for unreferenced files |
| `--json` | Machine-readable output |

Issue kinds:

| Kind | Meaning | `--fix`? |
|---|---|---|
| `semantics` | A node is missing `schema`/`adjustment`/`volume_basis`/`bar_label`, or has a value outside its vocabulary | Backfills *missing* fields only |
| `legacy_shape` | A stocks/indexes timeframe still holds a flat node, so another provider's write would replace it | Migrates to nested-by-provider |
| `date_drift` | Recorded `start_date`/`end_date` disagree with the file's first/last bar | Refreshes from the file |
| `file_missing` | `file_path` points at nothing | No — investigate |
| `no_file_path` | The node records no path at all | No |
| `no_provider` | Provider can't be determined from node, key or entry | No |
| `orphan_file` | A parquet under an OHLCV root that no catalog references | Crypto and crypto-futures: adopted (the path names the catalog, symbol and timeframe; JSON `orphans_adopted`). Others: no |

```bash
uv run python tools/check_catalog.py --json
uv run python tools/check_catalog.py --catalog indexes --fix
uv run python tools/check_catalog.py --skip-dates          # quick structural pass
```

Exits non-zero if any issue remains unfixed, or if orphan files were found.
`--fix` only ever adds information: it never deletes a node, never touches a
parquet file, and never overwrites a semantic value that is present but
invalid — a wrong value is someone's decision and gets reported, not silently
corrected.

---

## `check_splits.py`

```
uv run python tools/check_splits.py [--path DIR] [--tolerance F] [--json]
```

| Flag | Meaning |
|---|---|
| `--path` | Directory to scan (repeatable). Defaults to `price-data/stocks` — only equities have corporate actions |
| `--tolerance` | How exactly a move must match a simple fraction to be labelled a split factor (default `0.003`) |
| `--json` | Emit findings as JSON instead of a table |

```bash
uv run python tools/check_splits.py
uv run python tools/check_splits.py --path price-data/stocks/yf/sp500
```

An incremental download appends bars to an existing file. When the symbol
splits, the provider restates its whole history at the new share count, but
appending does not restate what is already on disk — so the file holds
as-traded prices up to the last full download and split-adjusted prices after
it, joined by a step that never traded. To a backtest that step is a crash.

Read-only; exits non-zero if anything is found. Repair means re-downloading the
symbol in full: appending cannot fix a seam whose history is stated at the
wrong share count.

Output leads with a per-date count, which is the actual signal. A real crash
moves many symbols by *similar* factors in one direction (March 2020 clusters
at 0.53–0.62x); a bad update run moves them by wildly different factors on a
date with no market event. On this repo that is `2024-08-07`, where 17 files
break by anywhere from 0.038x to 0.48x — `nasdaq100/ORLY` sits at exactly 15x
the provider's price before it and 1.0x after, its 15:1 split having happened
in June 2025, long after the history was written.

## `download_crypto.py`

```
uv run python tools/download_crypto.py --exchange EX (--universe | --all-pairs | --pairs P...)
                                        --timeframes TF... [--trading-mode {spot,futures}]
                                        [--full] [--prepend] [--since DATE] [--dry-run]
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

# Every USDT perpetual bitget lists, new ones from their listing
uv run python tools/download_crypto.py --exchange bitget --trading-mode futures \
  --universe --timeframes 1h 4h 6h 12h 1d 3d 1w 1Mo
```

| Flag | Meaning |
|---|---|
| `--universe` | Every spot pair (any quote) or perpetual (any settlement, linear or inverse) the venue lists now, minus leveraged tokens, bitget's innovation zone and demo contracts. Catalog pairs it no longer lists get `retired: <date>` and are skipped from then on |
| `--all-pairs` | The catalog's pairs, except retired ones |
| `--since` | Start for series with no data (and for `--full`). Default: the pair's listing |
| `--full` | Ignore stored data and refetch from `--since` or the listing |

kucoin and bitget futures, bitget spot 3d/1w/1Mo, and every perpetual not
settled in USDT go through ccxt (`CCXT_ROUTES`, `freqtrade_can`) because
freqtrade can't page or trade them; everything else through
freqtrade, `FT_BATCH_PAIRS` pairs per run, each batch merged before the next.

Wraps freqtrade's public (unauthenticated) OHLCV download, merges into
existing parquet with `normalize_ohlcv()` + dedup-on-timestamp, and updates
the exchange's YAML catalog. See [`docs/workflows.md`](workflows.md#crypto-refresh)
for the full refresh workflow and [`.agent/skills/crypto_download/SKILL.md`](../.agent/skills/crypto_download/SKILL.md)
for the agent-skill version.

---

## `download_funding.py`

```
uv run python tools/download_funding.py --exchange {binance,bybit,kucoin} [--symbols SYM...]
                                         [--since DATE] [--full] [--dry-run] [--json]
```

```bash
# Append new settlements for every contract in crypto_futures_binance.yaml
uv run python tools/download_funding.py --exchange binance

# Two contracts; show where each fetch would start
uv run python tools/download_funding.py --exchange bybit --symbols BTC_USDT ETH/USDT --dry-run
```

| Flag | Meaning |
|---|---|
| `--exchange` | `binance`, `bybit` or `kucoin` (public endpoints, no key) |
| `--symbols` | Contracts to fetch (`BTC_USDT` or `BTC/USDT`); must be in the catalog. Default: all but retired. Non-USDT contracts (`BTC_USDC`, inverse `BTC_USD`) use the venue's own id from ccxt |
| `--since` | Start date when there is no file yet (or with `--full`) |
| `--full` | Ignore the file and refetch from `--since` or the first bar; new rows win |
| `--dry-run` | Plan only: no network, no writes |
| `--json` | One JSON document on stdout (exchange, dry_run, plan[], results[], failures[], updated) |

Writes `crypto-futures/<exchange>/<SYMBOL>/<SYMBOL>-funding.parquet`
(`timestamp`, `funding_rate`, `mark_price`) and records it as
`timeframe_dates.funding` (`schema: funding_rate`) on the contract's catalog
entry. Resumes after the last settlement on disk. Exits 1 only when every
contract failed; partial failures are listed. As a server refresh:
`{"provider": "funding", "target": {"exchange": "binance"}, "dry_run": true}`.

---

## `fix_daily_stamps.py`

```
uv run python tools/fix_daily_stamps.py --provider P [--backup-dir DIR] [--dry-run] [--json]
```

```bash
uv run python tools/fix_daily_stamps.py --provider bitget --dry-run
uv run python tools/pdm_client.py submit fix_daily_stamps -- --provider bitget --json
```

| Flag | Meaning |
|---|---|
| `--provider` | `yf`, `ibkr`, `binance`, `bybit`, `kucoin`, `bitget` or `deribit`: every `*-1d`/`-1w`/`-1Mo` parquet under `price-data/*/<provider>/` |
| `--backup-dir` | Where originals are copied before rewriting. Default `<data root>.stamp-backup-<today>`, beside the data root; inside it is refused |
| `--dry-run` | Report only: no backups, no writes |
| `--json` | One JSON document on stdout (provider, dry_run, backup_dir, scanned, changed, rows_dropped, files[], skipped[], failures[]) |

Date-labelled bars carry `timestamp` = 00:00 UTC of their calendar date.
Each row is labelled by rounding to the nearest UTC midnight, which maps a
venue's local midnight (04:00/05:00 UTC in New York, the previous UTC day in
Frankfurt or Tokyo) to its own date. Then one row per date is kept: a
venue-midnight row from download_yf beats a 00:00 row from the old import.
Files already in convention are not touched, so reruns change nothing. Dates
don't move, so catalogs aren't rewritten. Run it through PDM: it runs in the
provider's lane (`equity`, `crypto` or `options`), so it can't race that
provider's downloader. Exits 1 if any file failed.

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

## `download_yf.py`

```
uv run python tools/download_yf.py
    (--universe sp500|nasdaq100|russell2000|delisted_stocks | --asset-class Index|Commodity|Macro)
    [--symbols S ...] [--timeframes TF ...] [--since DATE] [--full]
    [--overlap-days N] [--adjusted] [--sleep-seconds S] [--limit N] [--dry-run]
```

| Flag | Meaning |
|---|---|
| `--universe` / `--asset-class` | What to refresh; exactly one is required |
| `--symbols` | Subset, which must already exist in that catalog |
| `--timeframes` | Default `1d`. Yahoo has `1m 5m 15m 30m 1h 1d 1w 1Mo` — no 4h or 8h |
| `--since` / `--full` | `--full` rebuilds from `--since`; otherwise only prepend and tail windows are requested |
| `--adjusted` | Store split/dividend-adjusted closes (`auto_adjust`). Default is as-traded |
| `--dry-run` | Print the plan, fetch nothing, write nothing |

```bash
uv run python tools/download_yf.py --universe sp500 --timeframes 1d
uv run python tools/download_yf.py --asset-class Index --timeframes 1d
uv run python tools/download_yf.py --universe sp500 --symbols AAPL --full --since 2000-01-01
```

Three Yahoo-specific behaviours worth knowing:

- **An unknown ticker returns an empty frame, not an error.** The tool treats
  empty as "nothing returned" and writes nothing — no empty parquet, no catalog
  node. Symbol spelling is translated by `utils.yf.yf_ticker()` (`GSPC` →
  `^GSPC`, `BRK.B` → `BRK-B`); the stored path always keeps *our* spelling.
- **Intraday history is capped** (~730 days of 60m, 7 days of 1m) and truncates
  silently, so the start is clamped before the request.
- **Adjustment is recorded, not assumed.** Either way the node gets
  `adjustment: raw|adjusted`, so `close` is interpretable later.

The catalog is written after every series, so interrupting a long run loses at
most one symbol's registration. Exits non-zero if any series failed.

Macro (`WTB3MS`) is refreshed with `--asset-class Macro --timeframes 1d`. It is a rate series,
not OHLCV: the file is stored from Yahoo `^IRX` (`utils/yf.MACRO_TICKERS`), the close becomes `Value`,
and `merge_rate_series()` keeps the `Value`-on-`Date` layout. Daily only, and `--adjusted` is refused.

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
    [--what-to-show TRADES|ADJUSTED_LAST] [--use-rth auto|always|never]
    [--session all|overnight]
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
| `--use-rth` | `auto` (default: RTH for 1d/1w, extended for intraday), `always`, or `never` |
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

## `download_ibkr_index.py`

```
uv run python tools/download_ibkr_index.py
    [--symbols SYM ...] [--timeframes 1h|4h|8h|1d|1w ...]
    [--since DATE] [--full] [--host H] [--port P] [--client-id N]
    [--chunk-days N] [--overlap-days N] [--what-to-show TRADES|MIDPOINT]
    [--sleep-seconds S] [--request-timeout S] [--max-chunk-retries N]
    [--limit N] [--dry-run]
```

| Flag | Meaning |
|---|---|
| `--symbols` | Index symbols to download (default: every symbol in `INDEX_CONTRACT_MAP`) |
| `--timeframes` | IBKR bar sizes to request and store (default `1h`) |
| `--since` / `--full` | Start date for new symbols; `--full` rebuilds from `--since` |
| `--host` / `--port` / `--client-id` | TWS/Gateway connection (env: `IBKR_HOST`, `IBKR_PORT`, `IBKR_CLIENT_ID` + 1) |
| `--what-to-show` | `TRADES` (default) or `MIDPOINT` as a fallback if TRADES returns nothing |
| `--overlap-days` | Re-request window when appending (default 5) |
| `--limit` | Cap symbols, for smoke tests |

```bash
# 1h bars for every mapped US index
uv run python tools/download_ibkr_index.py --timeframes 1h --since 2024-01-01

# Smoke test
uv run python tools/download_ibkr_index.py --symbols GSPC NDX --timeframes 1h --limit 2
```

Only the US indices in `utils.ibkr.INDEX_CONTRACT_MAP` resolve (`GSPC`→SPX/CBOE,
`DJI`→INDU/CME, `IXIC`→COMP/NASDAQ, `NDX`→NDX/NASDAQ, `RUT`→RUT/RUSSELL,
`VIX`→VIX/CBOE). Non-US indices need an entry added with the right
exchange/currency. Writes to
`price-data/indexes/ibkr/<SYMBOL>/<SYMBOL>-<TF>.parquet` and registers an
`IBKR` node under `timeframe_dates[tf]`, alongside YF's rather than over it.
Indices have no real trade volume. Exits non-zero if any symbol failed.

---

## `retry_ibkr_failed_sequentially.py`

```
uv run python tools/retry_ibkr_failed_sequentially.py
    [--universe sp500|nasdaq100] [--timeframes TF ...] [--session all|overnight]
    [--port P] [--since DATE] [--base-client-id N] [--sleep-seconds S]
    [--request-timeout S] [--max-chunk-retries N] [--dry-run]
```

| Flag | Meaning |
|---|---|
| `--universe` | Universe whose failures to retry (default `nasdaq100`) |
| `--timeframes` | Retry these timeframes' failures, in order (default `4h 1h`) |
| `--session` | Session of the original run — selects which progress files to read |
| `--port` | IBKR port (env: `IBKR_PORT`) |
| `--base-client-id` | First client id; incremented per timeframe (env: `IBKR_CLIENT_ID` + 20) |
| `--since` | Start date for retries (default `2020-01-01`) |
| `--sleep-seconds` | Delay between requests (default 1.0) |
| `--request-timeout` | Per-request timeout (default 25s) |
| `--dry-run` | Print the subprocess commands without running them |

```bash
uv run python tools/retry_ibkr_failed_sequentially.py \
  --universe nasdaq100 --timeframes 4h 1h --port 4001
```

Reads the `failures` list from `tmp/ibkr_stock_progress_<session>_<tfs>.json`
— the files `download_ibkr_stock_overnight.py` actually writes — then
re-invokes it as a subprocess once per timeframe, sequentially, never in
parallel, writing to its own `tmp/ibkr_retry_<tf>_{progress.json,resume.jsonl}`.
Stops on the first non-zero exit code.

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

---

## `import_futures.py`

```
uv run python tools/import_futures.py
    --source DIR --provider NAME --timeframe TF --pattern REGEX
    [--glob PAT] [--tz ZONE] [--decade YYYY] [--roots ROOT ...] [--apply]
```

| Flag | Meaning |
|---|---|
| `--source` | Directory of per-contract vendor files (searched recursively) |
| `--provider` | Provider name — becomes the directory under `futures/` |
| `--timeframe` | The timeframe these files hold, e.g. `1h` |
| `--pattern` | Regex over the filename capturing `symbol`, or `root` + (`code`\|`month`) + (`year`\|`yy`) |
| `--tz` | Timezone of naive vendor timestamps, e.g. `America/Chicago` |
| `--decade` | Decade for ambiguous 1–2 digit years; refuses to guess without it |
| `--roots` | Only import these roots |
| `--apply` | Actually write — the default is a dry run |

```bash
# Check the filename mapping before writing anything
uv run python tools/import_futures.py --source ~/archive/cme --provider csidata \
    --timeframe 1h --pattern '(?P<root>[A-Z]{2,3})_(?P<year>\d{4})(?P<month>\d{2})'

# Then import, localising the vendor's exchange-local timestamps
uv run python tools/import_futures.py --source ~/archive/cme --provider csidata \
    --timeframe 1h --tz America/Chicago --apply \
    --pattern '(?P<root>[A-Z]{2,3})_(?P<year>\d{4})(?P<month>\d{2})'
```

IBKR only retains expired futures for about a year, so deep history has to be
imported. Files land in `price-data/futures/<provider>/<SYMBOL>/<SYMBOL>-<TF>.parquet`
and register a provider node under `timeframe_dates[tf]` in `futures.yaml`,
beside IBKR's rather than over it.

The canonical symbol keys on the **delivery** month, not the expiry date:
December crude is `CLZ25` even though it stops trading in November. Contracts
whose last trade date is unknown get one derived from the final bar in the file.

## `download_deribit_options.py`

```
uv run python tools/download_deribit_options.py
    [--underlying BTC|ETH] [--timeframes TF ...]
    (--expiries DATE ... | --quarterlies | --monthlies | --active)
    [--since YYYY-MM-DD] [--until YYYY-MM-DD]
    [--strike-grid MIN:MAX:STEP | --strikes K ...] [--rights C P]
    [--limit N] [--keep-untraded] [--sleep-seconds S] [--dry-run]
```

| Flag | Meaning |
|---|---|
| `--underlying` | Option underlying; `BTC` or `ETH` (add more in `utils/options.py`) |
| `--timeframes` | Timeframes to store; `1d` by default, `1h` and finer are paged automatically |
| `--expiries` | Explicit expiry dates, e.g. `2024-12-27` |
| `--quarterlies` / `--monthlies` | Generate last-Friday expiries; needs `--since` |
| `--active` | Use the instruments currently on the board instead of probing |
| `--strike-grid` | Strikes to probe, as `MIN:MAX:STEP` |
| `--strikes` | Explicit strikes instead of a grid |
| `--rights` | `C`, `P`, or both (default) |
| `--keep-untraded` | Keep zero-volume bars; by default they are dropped |
| `--dry-run` | Discover and report without downloading or writing |

```bash
# One known expiry, daily bars
uv run python tools/download_deribit_options.py \
    --expiries 2024-12-27 --strike-grid 20000:140000:5000

# Every quarterly back to 2019 — check the discovery cost first
uv run python tools/download_deribit_options.py \
    --quarterlies --since 2019-01-01 --strike-grid 5000:150000:5000 --dry-run

# The live board, hourly
uv run python tools/download_deribit_options.py --active --timeframes 1h
```

Deribit is the only venue that serves bars for **expired** options. OKX, Gate
and Bybit list thousands of option markets but expose OHLCV for the live board
only, so they are usable going forward and useless for history. Measured depth
on Deribit reaches at least March 2019, daily and intraday.

There is no bulk listing of expired instruments — `get_instruments(expired=true)`
retains about a day — so discovery constructs candidate names and validates each
with `get_instrument`, which answers for contracts that expired years ago.
A probe costs roughly 39ms, so grid width is the thing to watch: expiries ×
strikes × rights is the request count before a single bar is downloaded.

Three properties of the data that the tool handles and a hand-rolled script
would not:

- **Post-expiry padding.** The chart endpoint pads an expired contract forward
  to today with flat zero-volume bars — a 2019 contract returns 2731 daily bars
  of which 191 are real. The request window is clamped to the contract's life
  and untraded bars are dropped unless `--keep-untraded` is passed.
- **The 5001-bar cap**, anchored to the *end* of the window. A single request
  for a year of hourly bars silently returns only the last 5001 hours. Requests
  are paged backwards from expiry.
- **Premiums are not in USD.** A BTC option quoting `0.1575` means 0.1575 BTC.
  `options.yaml` records `quote_currency` and `contract_size` on each entry;
  the stored prices are left exactly as traded.

Files land in `price-data/options/<provider>/<UNDERLYING>/<SYMBOL>/<SYMBOL>-<TF>.parquet`
with the canonical symbol `BTC-20241227-60000-C` — an ISO date so a directory
listing sorts chronologically, which Deribit's own `27DEC24` does not.

## `pdm_server.py`

```
uv run python tools/pdm_server.py [--bind HOST] [--req-port N] [--pub-port N]
    [--config pdm.yaml] [--scan-seconds S] [--broadcast-max-rows N] [--check] [-v]
```

| Flag | Meaning |
|---|---|
| `--bind` | Interface to bind; default `PDM_BIND_HOST` (`0.0.0.0`). Use the Tailscale IP to stay on the tailnet |
| `--req-port` / `--pub-port` | Request/reply and broadcast ports; default 5555 / 5556 |
| `--http-port` | Read-only HTTP gateway (reads, audits, refresh previews; no token); default `PDM_HTTP_PORT` (5580), 0 disables it |
| `--config` | Schedules and live subscriptions; default `PDM_CONFIG` (`pdm.yaml`). See `pdm.example.yaml`. The server rewrites it when clients change schedules or live feeds (comments are not kept) |
| `--scan-seconds` | How often to diff the tree for writes made outside the server (default 60) |
| `--broadcast-max-rows` | Cap on rows per bar broadcast; bigger updates carry `truncated: true` |
| `--check` | Validate the config, print the schedules' next runs, and exit |

Runs as the `pdm` systemd user unit on the data host — see
[`server.md`](server.md) for the protocol, topics and deployment.

## `pdm_client.py`

```
uv run python tools/pdm_client.py [--host H] [--req-port N] [--pub-port N]
    [--token T] [--json] COMMAND ...
```

| Command | Does |
|---|---|
| `ping` | Server facts: index/watch readiness, lanes, auth |
| `catalogs` | Every catalog with symbol and series counts |
| `series [--catalog --asset-class --symbol --name --timeframe --provider --session --stale-days --fields]` | Coverage rows, like `query.py`; `--name apple` matches common names by leading words, `--fields` projects and dedupes |
| `bars (filters \| --path P) [--start --end --limit --csv F --parquet F]` | One series as the canonical frame; filters must match exactly one series |
| `status [--catalog --stale-days]` | `status.py --json`, run on the server |
| `submit [--wait] TOOL -- ARGS...` | Queue a whitelisted tool run (needs `PDM_TOKEN` if the server sets one) |
| `jobs [--state --limit]` / `job ID [--log N]` / `cancel ID` | Job queue |
| `alerts [--limit N]` / `alert-test` | Recent alerts (failed jobs, series that stopped updating); `alert-test` (token) sends one through `PDM_NOTIFY_CMD` |
| `audit TOOL [option=value ...]` | Read-only check on the host, no token: `status`, `query`, `check_gaps`, `check_schema`, `check_catalog` (never `--fix`), `check_splits`. Options are the tool's flags with `_` for `-`; lists comma-separated |
| `coverage SYMBOL` | Stored series, the schedules that refresh it, and its live feeds |
| `refresh (--universe U \| --asset-class C \| --exchange E [--trading-mode M]) -t TF... [-s SYM...] [--since D] [--full] [--run [--wait]]` | Approved one-shot refresh; a dry-run preview unless `--run`. One at a time, 30-min cap |
| `schedules [--symbol S] [--catalog C]` | Schedules with next/last run and what they cover |
| `schedule-add NAME (--at HH:MM [--days mon-fri] \| --every 6h) (--refresh JSON \| --tool T -- ARGS) [--paused]` | Create or replace a schedule |
| `schedule-pause` / `schedule-resume` / `schedule-run` / `schedule-rm NAME` | Manage one schedule |
| `live [--symbol S] [--exchange E]` | Live subscriptions with per-symbol transport, update count, age |
| `live-add EXCHANGE SYM... [--market spot\|swap] [--kind bars\|quotes] [--timeframe TF] [--throttle-ms N] [--transport auto\|poll]` | Start live feeds |
| `live-rm EXCHANGE [SYM...] [--market --kind --timeframe]` | Stop live feeds; no symbols = the whole subscription |
| `watch [PREFIX ...]` | Print broadcasts, e.g. `bar/cryptos/binance/`, `live/`, `quote/`, `prices`, `job/` |
| `topics` / `reload` | Topic scheme; rebuild the series index |

```bash
uv run python tools/pdm_client.py --host <host-tailscale-ip> series --symbol AAPL
uv run python tools/pdm_client.py bars --symbol AAPL --catalog sp500 --timeframe 1d --start 2026-01-01
uv run python tools/pdm_client.py submit --wait download_yf -- --universe sp500 --timeframes 1d
uv run python tools/pdm_client.py watch bar/stocks/yf/sp500/AAPL/ live/binance/
uv run python tools/pdm_client.py coverage AAPL
uv run python tools/pdm_client.py refresh --universe sp500 -t 1d -s AAPL MSFT
uv run python tools/pdm_client.py live-add binance SOL/USDT --market swap --kind quotes
```

Exit code 2 with `error: ...` on stderr for a refused or failed request.

## `pdm_mcp.py`

```
uv run python tools/pdm_mcp.py [--list]
```

MCP server over stdio, launched by the agent harness (`.mcp.json` registers it
for this repo; elsewhere: `claude mcp add pdm -- uv run --project <repo>
python <repo>/tools/pdm_mcp.py`). Connects to `PDM_SERVER` like the CLI.
`--list` prints the tools and whether each is read-only.

| Read-only | Writes (keep on "ask") |
|---|---|
| `pdm_ping`, `pdm_catalogs`, `pdm_series`, `pdm_bars` (≤ 500 rows), `pdm_status`, `pdm_audit`, `pdm_coverage`, `pdm_schedules`, `pdm_live`, `pdm_jobs`, `pdm_job`, `pdm_refresh_preview` | `pdm_refresh_run`, `pdm_cancel`, `pdm_schedule_put`, `pdm_schedule_set` (pause/resume/run), `pdm_schedule_remove`, `pdm_live_add`, `pdm_live_remove` |

