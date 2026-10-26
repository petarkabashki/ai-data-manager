# Roadmap

Sequenced work plan derived from the 2026-09 repo review. Epics are ordered by
blast radius: correctness bugs that can destroy data first, then structural
debt that makes every later change more expensive, then new capability.

Each epic states **why it exists**, **what's in scope**, and **how you know
it's done**. Nothing here changes the two load-bearing rules in
[`AGENTS.md`](AGENTS.md) — the provider-first path rule and the canonical OHLCV
schema. Every epic is constrained by them.

| Epic | Theme | Status |
|---|---|---|
| [E1](#e1--catalog-integrity-and-durable-writes) | Catalog integrity & durable writes | done |
| [E2](#e2--collapse-duplicated-logic-into-utils) | Collapse duplicated logic into `utils/` | done |
| [E3](#e3--a-test-suite-for-the-normalization-core) | Test suite for the normalization core | done |
| [E4](#e4--configuration-and-portability) | Configuration & portability | done |
| [E5](#e5--calendar-aware-gap-detection) | Calendar-aware gap detection | done |
| [E6](#e6--yahoo-finance-ingestion) | Yahoo Finance ingestion | done |
| [E7](#e7--machine-readable-tool-surface) | Machine-readable tool surface | done |
| [E8](#e8--one-source-of-truth-for-docs-and-skills) | One source of truth for docs & skills | done |
| [E9](#e9--freshness-every-catalog-on-a-schedule) | Freshness: every catalog on a schedule | in progress |
| [E10](#e10--off-host-backups) | Off-host backups | done |
| [E11](#e11--the-data-host-deploys-from-git) | The data host deploys from git | done |
| [E12](#e12--data-quality-backlog) | Data-quality backlog | in progress |
| [E13](#e13--consumers-on-the-provider-first-layout) | Consumers on the provider-first layout | in progress |
| [E14](#e14--funding-on-every-perpetual-venue) | Funding on every perpetual venue | done |

Anything marked done has tests that fail against the previous behaviour.

---

## E1 — Catalog integrity and durable writes

**Why.** Three defects can lose data that has already been downloaded, which
makes them strictly more urgent than anything else on this list.

**Scope.**

1. **Index catalogs nest by provider.** `download_ibkr_index.py` accepts
   `--timeframes 1d`, but `update_index_timeframe_entry()` wrote a flat
   `timeframe_dates[tf]` node — so an IBKR daily download silently replaced
   YF's node for the same timeframe and orphaned YF's parquet. Indexes now use
   the same nested-by-provider shape as stocks, and the writer reads either
   shape so existing flat YAML keeps working.
2. **Stock nodes key on `(provider, session)`.** `session` is part of the
   filename but was not part of the catalog key, so downloading `--session all`
   and then `--session overnight` for one symbol/timeframe left two parquet
   files and one catalog node. The node key is now `IBKR` for the default
   session and `IBKR:overnight` for a non-default one.
3. **Every parquet write is atomic.** Catalog YAML already wrote via
   `.tmp` + `replace()`; the merge path wrote straight onto the destination
   after reading the old rows into memory, so a crash mid-write destroyed the
   only copy. All writes now go through one `write_parquet_atomic()`.
4. **`check_schema.py --path` no longer crashes** when it finds a mismatch
   (`relative_to(REPO_ROOT)` on a relative path raised `ValueError` — only on
   the error branch, i.e. exactly when you needed the output).

**Done when.** `update_index_timeframe_entry()` and
`update_stock_timeframe_entry()` are covered by collision tests that fail on
the old behaviour; no `to_parquet()` call in the repo targets a live file
path; `check_schema.py --path price-data/cryptos` prints mismatches instead of
raising.

**Migration note.** Existing `indexes.yaml` is flat. The writer upgrades a
flat node to the nested shape on first write, and `utils.data.asset_path()`
already reads both. `uv run python tools/check_catalog.py --fix` converts the
rest in bulk — run it once and the mixed-shape period is over.

---

## E2 — Collapse duplicated logic into `utils/`

**Why.** The same logic existed in three or four places, and the copies had
already drifted. Every future fix had to be applied N times or it silently
wasn't.

**Scope.**

- One `merge_ohlcv_parquet()` in `utils/catalog.py`, replacing
  `download_crypto.merge_parquet()`, `ibkr.merge_canonical_parquet()` and the
  inline merge in `convert_crypto.json_to_parquet()`.
- One `empty_ohlcv_frame()` replacing five hand-written literals, two of which
  omitted dtypes and produced object-typed columns.
- The IBKR range planner, chunk loop, duration string, chunk-day defaults and
  `DownloadRange` moved from the two download tools into `utils/ibkr.py`.
  `download_ibkr_index.py` and `download_ibkr_stock_overnight.py` had ~150
  near-identical lines between them.
- One `render_table()` in `utils/report.py`, replacing the copies in
  `query.py` and `check_gaps.py`.
- Dead code removed: the duplicated `log_file` block and double
  `import datetime as _dt` in `download_crypto.py`, plus unused imports in
  seven modules.

**Done when.** `grep -c "to_parquet" tools/ utils/` shows writes only in the
atomic helper and the market-cap exporter; the two IBKR tools share a planner;
the test suite passes unchanged.

---

## E3 — A test suite for the normalization core

**Why.** `normalize_ohlcv()` is the single point every byte of price data
passes through, and it had zero tests. The catalog writers guard against
provider collisions purely by convention.

**Scope.** `tests/`, pure-python, no network and no `price-data/`:

- every `normalize_ohlcv()` input shape, including the tz-aware and
  non-nanosecond datetime cases the docstring calls out
- merge round-trips: dedup, sort, prepend, and the atomic-write path
- catalog writers: the E1 collision cases, and top-level date recomputation
- `detect_gaps()`: internal gaps, trailing candles, variable timeframes
- path builders and the freqtrade naming translation

**Done when.** `uv run pytest` is green from a clean checkout with no
`price-data/` symlink present.

---

## E4 — Configuration and portability

**Why.** The repo hardcoded one machine's absolute paths
(`/home/grenada/work/ai-quant/.venv/bin/freqtrade`, twelve `/home/grenada/...`
entries in `.claude/settings.json`), which means it only runs for one user on
one box — and an agent reading those paths learns something untrue about the
repo.

**Scope.**

- `utils/config.py` resolving `PRICE_DATA_ROOT`, `FREQTRADE_BIN`,
  `IBKR_HOST`/`IBKR_PORT`/`IBKR_CLIENT_ID` and `TMP_ROOT` from the
  environment, with the current values as documented defaults.
- `.env.example` listing every knob.
- `.claude/settings.json` rewritten with repo-relative globs and a small set
  of patterns instead of a dozen accreted one-off `Bash(...)` grants.
- Freqtrade logs to `TMP_ROOT` (repo `tmp/`) instead of `/tmp`, matching the
  IBKR tools.
- A `[build-system]` and `[tool.pytest.ini_options]` block in `pyproject.toml`.

**Done when.** `PRICE_DATA_ROOT=/somewhere/else uv run pytest` passes and no
`/home/` string remains in tracked source. Both hold.

**Not done, and deliberately so.** The `sys.path.insert(...)` preamble is still
in all eleven tool scripts. Removing it depends on the project actually being
installed into the venv, and adding `[build-system]` changes how uv treats this
repo — from a virtual project to a built-and-installed package — which also
makes `uv.lock` stale. That needs one `uv sync` on a machine with the real
environment to confirm before the preamble comes out. If `uv sync` objects,
the safe revert is to delete the `[build-system]` block; nothing else depends
on it, and `[tool.pytest.ini_options].pythonpath` is what makes the tests run
either way.

---

## E5 — Calendar-aware gap detection

**Why.** `detect_gaps()` assumes a 24/7 series. It is correct for crypto and
wrong for everything else: every weekend and market holiday reads as an
internal gap. `download_ibkr_stock_overnight.py` feeds those phantom gaps
straight into its range planner in `--session overnight` mode, so it
re-requests ranges IBKR will never fill, on every run, forever.

**Scope.**

- `utils/calendars.py`: `continuous`, `us_equity_rth`, `us_equity_extended` and
  `us_equity_overnight`. NYSE holiday and early-close rules are computed rather
  than tabulated — plus a small ad-hoc closure set — so there is no calendar
  package to install and no table to maintain.
- `detect_gaps(path, timeframe, calendar)` reports a gap only where the
  calendar expected bars. The default stays `CONTINUOUS`, so crypto behaviour
  is bit-for-bit unchanged.
- The IBKR overnight planner passes the matching equity calendar, so it stops
  generating windows IBKR can never fill.
- `check_gaps.py` audits any catalog (`--catalog`, `--all-catalogs`), with
  `--calendar` to override and `--json` for agents. `audit_catalog_series()`
  in `utils/gaps.py` is the shape-agnostic engine behind it.

**Done when.** Both hold: a complete daily equity series reports zero internal
gaps under `us_equity_rth`, and the same file reports 327 under the old 24/7
rule — the second assertion is in the suite specifically so the fix can't
silently regress.

**Design note.** Intraday expectation is an *overlap* test against the session
window, not a reconstruction of the provider's bar grid — those differ by
provider and request type, and guessing them would produce confident nonsense.
So this suppresses gaps a closure explains rather than predicting every bar.
A genuinely missing bar inside a closure window won't be reported; that
direction is deliberate, since a false gap becomes an unsatisfiable download
while a missed one is visible as staleness.

**Not covered.** Non-US venues. Every listed symbol here is US-traded, so
`UsEquityCalendar` is sufficient today; a London or Tokyo listing would need
its own sessions and holiday rules.

---

## E6 — Yahoo Finance ingestion

**Why.** YF is the largest provider in the repo — 2,557 stock symbols, all 11
indexes, commodities and macro — and there is **no tool that downloads it**.
No `yfinance` dependency, no `download_yf_*.py`. The data can only have been
produced out-of-band, so the largest dataset here has no reproducible refresh
path. `normalize_ohlcv()` already has the `Date`+`Open` branch and
`stock_file_path("YF", ...)` already exists; only the downloader is missing.

**Scope — done.** `tools/download_yf.py` covers stocks by universe, indexes and
commodities, on the shared planner (now extracted to `utils/planning.py` so it
no longer lives inside an IBKR module) and the shared merge. `utils/yf.py` owns
the three things that actually go wrong with Yahoo:

- **Ticker spelling.** Ours is not Yahoo's: indexes take a `^` prefix, share
  classes a dash (`BRK.B` → `BRK-B`). Getting it wrong doesn't raise — Yahoo
  answers an unknown ticker with an empty frame — so the mapping is explicit
  and tested, and an empty result is treated as "nothing returned", never
  "already current".
- **Missing bar sizes.** Yahoo has no 4h or 8h bar. Requesting one is rejected
  rather than silently substituted.
- **Silent truncation.** Intraday history is capped (~730 days of 60m, 7 of
  1m) and Yahoo shortens the frame rather than erroring, so the start is
  clamped before the request.

`commodities.yaml` and `macro.yaml` CRUD landed with the semantics work.

**Semantics — done.** `close` meant different things per provider and nothing
recorded which. Every catalog node now carries four fields from
`utils/semantics.py` — `schema`, `adjustment`, `volume_basis`, `bar_label` —
stamped by all five writers from per-provider defaults, with
`tools/check_catalog.py --fix` to backfill nodes written earlier. `macro`'s
`rate_series` marking turns `check_schema.py`'s hand-maintained exclusion into
data. Two volume defaults are recorded as stated assumptions (Binance/Bybit
linear = base asset, KuCoin futures = contracts) rather than left implicit.

**Adjustment is now a choice, not an accident.** `--adjusted` sets
`auto_adjust` and the node records `adjustment: adjusted`; the default stays
as-traded, matching what is already on disk. Whether to *re-ingest* the
existing YF history as adjusted is a decision left open — both are now
expressible and both are labelled.

**Not verified: the network leg.** This environment's proxy blocks Yahoo, so
`YahooFetcher.fetch()` — the one function that makes an HTTP call — has never
run against the live API. Everything around it is tested against a fake
fetcher that records its requests: ticker translation, interval mapping,
clamping, the yfinance frame shape (index named `Date` vs `Datetime`,
tz-aware intraday, multi-indexed columns), planning, merging, catalog writes
and the semantics stamped on the node. The first real run should be
`--limit 2 --dry-run`, then `--limit 2`, before a full universe.

**Not covered: macro.** `WTB3MS` is a rate series, not OHLCV, with its own
schema and arguably its own source. It needs a separate path.

**Done when.** `download_yf.py --universe sp500 --timeframes 1d` reproduces
the existing files byte-for-byte modulo the new adjustment field.

---

## E7 — Machine-readable tool surface

**Why.** This is the real harness-neutrality lever. Today every audit tool
prints an ASCII table an agent has to scrape, and three asset classes can't be
audited at all. The more the tools describe themselves, the less any of this
depends on prose an agent may or may not have read.

**Scope.**

- ~~`--json` on `query.py`, `check_gaps.py`, `check_schema.py`,
  `check_catalog.py` and `status.py`~~ **done** — every audit tool now emits
  JSON, and all of them exit cleanly when their output is piped into something
  that stops reading (`| head` used to end in a BrokenPipeError traceback that
  reads like a tool failure).
- ~~`query.py` covering all asset classes~~ **done**. It had imported
  `load_assets` without ever calling it, and its docstring advertised an
  `--asset-class` flag that didn't exist; both are now real. Adds
  `--provider` (case-insensitive) and surfaces each node's session and
  semantics.
- ~~Stop re-reading every parquet file by default~~ **done** — `--rescan` is
  opt-in.
- ~~`tools/status.py --json`~~ **done**: coverage, staleness, provider and
  timeframe breakdown, and with `--deep` file existence, schema conformance and
  semantic completeness. Non-zero exit on `--deep` problems, so it works as a
  health check.
- ~~`tools/check_catalog.py`~~ **done**: catalog/disk drift, semantic
  completeness, the flat→nested migration, and orphan detection, with `--json`
  and `--fix`.
- ~~`refresh_*_catalog_dates()` for the classes that lack one~~ **done**:
  shape-agnostic `refresh_entry_dates()` plus index and futures wrappers;
  `check_catalog.py --fix` is the cross-catalog entry point.
- `commodities.yaml` / `macro.yaml` CRUD — **done** (`load_simple_catalog`,
  `update_simple_timeframe_entry`); they previously had no writer at all.
- ~~Fix `retry_ibkr_failed_sequentially.py`~~ **done** (E1-era): it read
  progress filenames the downloader never wrote.

**Done when.** An agent can answer "what's stale, what's broken, what's
missing" with one command and no table parsing. `status.py --deep --json` is
that command.

---

## E8 — One source of truth for docs and skills

**Why.** The tool table is documented in four places (`README.md`,
`AGENTS.md`, `docs/cli-reference.md`, `docs/agentic-usage.md`) and has already
drifted: `download_ibkr_index.py` appears in exactly one of them, and the
crypto symbol counts in `.agent/crypto_download.md` disagree with the ones in
`README.md`. Separately, `.agent/crypto_download.md` has no frontmatter, so no
harness can discover it — it is findable only by reading a table in
`AGENTS.md`.

**Scope.**

- `AGENTS.md` → `AGENTS.md` (the cross-agent convention), with `CLAUDE.md`
  reduced to the venv rule plus a pointer. Today `CLAUDE.md` restates the
  layout, data root and metadata files, which is exactly the duplication that
  drifts.
- Generate the tool table from the tools' own `--help` so it cannot drift;
  `docs/` keeps the prose and diagrams and stops restating the table.
- `.agent/*.md` → `.agent/skills/<name>/SKILL.md` with `name` + `description`
  frontmatter — neutral content, discoverable by any harness, with
  `.claude/skills` symlinked for Claude Code.
- Reconcile the symbol counts against a single generated source.
- Fix the standing doc/code contradiction: `AGENTS.md` and `workflows.md` both
  say the IBKR stock tool "requests `useRTH=0`" unconditionally, but the code
  hardcodes `use_rth = timeframe == "1d"`, so daily IBKR bars are RTH-only.
  Decide which is intended, then make the other match.
- Delete `stuff.txt`; de-duplicate `price-data/` in `.gitignore`.

**Done when.** Adding a tool requires editing one file, and `AGENTS.md` and
the code agree on `useRTH`.

---

## E9 — Freshness: every catalog on a schedule

**Why.** The data server owns the writes now, but only macro and funding ran
on a timer. Every other catalog went stale the moment nobody remembered it:
bybit and bitget spot had fallen behind because nothing ever scheduled them.

**Scope.**

- Daily schedules for every downloadable catalog, staggered so jobs that
  share a lane don't pile up. Yahoo runs after the US close on weekdays:
  sp500, nasdaq100, russell2000, indexes, commodities (22:30–23:30 UTC).
  Crypto spot runs on binance, kucoin, bybit and bitget (00:15–01:45 UTC),
  then futures on binance, bybit and kucoin (02:15–03:05 UTC). **Done.**
- A one-off catch-up run for each, through the same lanes. **Submitted.**
- Intraday crypto (binance 1m–30m for 436 pairs, bybit for 16 pairs, kucoin's
  single 15m series). **Owner decision**: what to schedule, and how often.
- IBKR catalogs. **Blocked**: there is no gateway on the data host.
- Alerts (`pdm/alerts.py`), so a schedule that breaks gets noticed. A failed
  or timed-out job raises an alert. A failing schedule alerts once, and again
  when it recovers. A check every 6h flags series that were fresh and have
  fallen 4+ days behind. Alerts go to `tmp/pdm_alerts.jsonl`, `server/alert`,
  `pdm_client alerts`, and optionally `PDM_NOTIFY_CMD`. **Done 2026-10-25**;
  the notify channel is the owner's choice.

**Done when.** `status.py` reports no daily catalog more than one trading day
old, except IBKR, without anyone running a download by hand.

## E10 — Off-host backups

**Why.** The data host holds the only writable copy. The local copy is now
a read-only mount of it, so it is not a backup.

**Scope.**

- `deploy/pdm-backup.{sh,service,timer}`: a pull-based rsync at 04:30 every
  day. Each run writes a dated snapshot, hard-linked against the last one.
  An interrupted run resumes the next day. A snapshot with under 80% of the
  previous one's files is never finalized. See
  [docs/server.md](docs/server.md#backups). **Done.** A run pulls the data
  first and the catalogs last, so a catalog is never older than its files.
- Restore procedure: stop `pdm`, rsync the snapshot back, run
  `check_catalog.py --fix` and `check_schema.py`, start `pdm`. See
  [docs/server.md](docs/server.md#backups). `check_catalog --fix` now adopts
  crypto files that no catalog lists, and `download_crypto` saves its catalog
  every 60s, so a snapshot taken mid-download restores cleanly. **Done.**
- Retention (`PDM_BACKUP_KEEP`, unset = keep everything) and whether a third
  copy goes off-site. **Owner decision.**

**Done when.** Met 2026-10-26: a restore of the newest snapshot into a scratch
tree on the host. The copy took 27 min (30 GB, 27,067 files) with 0 checksum
diffs. The repair refreshed 5,608 dates, backfilled 4,242 nodes and adopted
3,465 orphan files. Nothing was left over that live doesn't also have.

## E11 — The data host deploys from git

**Why.** The host checkout had drifted from `main` and was updated by
copying files over, so nobody could say which code was running.

**Scope.** The host now tracks `origin/main` over https. To deploy: `git pull
--ff-only`, then `uv sync --frozen`, then restart `pdm` when no jobs are
running. The host's local edits were stashed as `stash@{0}`. They are older
subsets of committed files, so the stash can be dropped on the owner's word.

The job queue is durable since 2026-10-25 (`$TMP_ROOT/pdm_jobs.json`), so a
deploy can restart `pdm` without waiting for the queue to drain.

**Done when.** Met: the host's HEAD is a commit on `main`.

## E12 — Data-quality backlog

**Scope.**

- 17 Yahoo files broke at the 2024-08-07 split seam. **Fixed 2026-10-25**:
  re-downloaded with `--full --since 2000-01-01`, then the stale rows were
  dropped (see the next item). `check_splits.py` now flags only real moves.
- Yahoo daily files had duplicate dates: imported history was stamped
  00:00 UTC, while `download_yf` wrote the exchange's local midnight, and the
  merge deduplicated on the exact timestamp. **Fixed 2026-10-25.** Owner's
  decision: date-labelled bars (1d/1w/1Mo) are stamped 00:00 UTC of their
  date. `download_yf` now writes that, and the merge (`daily=True`) collapses
  any two stamps of one date. `tools/fix_daily_stamps.py` rewrote 2,488
  files, dropping 12,187 duplicate rows (the owner deleted the backup).
- The stamp rule now holds for every provider. `merge_ohlcv_parquet`
  restamps any `-1d`/`-1w`/`-1Mo` file on write, `check_schema.py` flags a bar
  off 00:00 UTC or a repeated date, and `fix_daily_stamps.py` takes any
  provider. **Fixed 2026-10-25**: 56 bitget monthly files (stamped 16:00 UTC,
  midnight UTC+8) and 2 Deribit dailies (08:00 UTC). A full sweep of 28,095
  files is clean (the owner deleted the backup of the originals).
- Live catalogs were repaired 2026-10-26 with `check_catalog --fix`:
  2,125 crypto_binance and 10 crypto_futures_kucoin nodes backfilled with
  their semantics, and 7 crypto_binance date ranges refreshed. The audit is
  now clean apart from 6 stock files that no catalog lists: EA (ibkr
  nasdaq100 all-1d/1h/4h/8h, yf sp500 1d) and SATS (yf russell2000 1d).
  **Owner decision**: catalog them or remove them.
- 22 bitget pairs have no `1w` series, and a probe (VET/USDT) returned 0 rows.
  **Owner decision**: drop `1w` for bitget, or find another source.
- Yahoo adjustment differs per file: sp500 has 429 raw and 69 adjusted,
  nasdaq100 93/15, russell2000 93/1823, delisted 33/0. **Owner decision**:
  pick one convention and re-download the rest.
- Not gaps: two expired options series (expected), and `Adj Close`, which
  the canonical schema drops anyway.

**Done when.** `check_splits.py` is clean, and each provider uses one
adjustment convention.

## E13 — Consumers on the provider-first layout

**Scope.**

- bqn_quant: the loaders and their tests now use provider-first paths.
  **Unverified**: its `bqn-extensions` submodule is empty, so the tests can't
  run.
- ai-quant: 18 files still use the old paths. **Owner decision**: fix it or
  archive it.
- py-qwant: `qwant/data/sync.py` no longer did anything. **Deleted** in
  py-qwant.

**Done when.** No active repo reads a path the old layout used.

## E14 — Funding on every perpetual venue

**Scope.** `download_funding.py` now covers kucoin as well as binance and
bybit. KuCoin calls bitcoin `XBT`, so BTC_USDT is fetched as `XBTUSDTM`. KuCoin
also reports errors as HTTP 200 with an error code in the body, and these
count as failures. Deployed 2026-10-25. The first full download fetched
all 74 contracts with no failures: 309,824 settlements from 2020-03-30. The
`funding-kucoin` schedule refreshes them every 8h.

bitget has no funding source.

**Done when.** Each of the three venues has funding on disk with a schedule
behind it.
