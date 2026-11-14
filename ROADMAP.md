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
| [E15](#e15--the-full-crypto-universe) | The full crypto universe | in progress |
| [E16](#e16--row-level-backup-deltas) | Row-level backup deltas | in progress |
| [E17](#e17--trustworthy-breadth) | Trustworthy breadth | proposed |

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
- Stale means session days overdue (`utils.planning.days_behind`), counted
  from when the bar after a series' last one closed: trading days on equity
  calendars, every day on crypto. Before, it was days since `end_date`, so
  `status` called every equity series stale on a Monday, and 3-day and
  weekly bars alerted while their next bar was still forming. `status`,
  `query`, PDM's `series` and the stale alert share it, default 2 (a missed
  run). A check tool exiting 1 with a report no longer alerts as a failed
  job. **Done 2026-10-26.**
- Monthly crypto bars had not updated since 2024 on any exchange.
  freqtrade writes `1M` as `-1Mo.parquet`, and `download_crypto` looked for
  `-1M`. **Fixed 2026-10-26**; the next scheduled run catches them up.
- `every:` schedules counted from server start, so binance and bybit
  funding (every 8h) went 16h without a run on a day of four deploys, and an
  `at` slot that passed during a restart was skipped. The scheduler now keeps
  each schedule's last run in `$TMP_ROOT/pdm_schedules.json` and resumes from
  it. **Fixed 2026-10-26.**
- What is still stale under the new count (2026-10-26): IBKR, at
  2026-09-16 (blocked, above); bitget `3d`/`1w`, which mostly ended in 2024
  (E12); 73 russell2000 Yahoo dailies, mostly dropped or delisted symbols; and
  crypto pairs that stopped trading, which make up most of the crypto count.
  The `--universe` runs mark those `retired` (E15), and they leave the count.

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
- Retention: `PDM_BACKUP_KEEP=3` on the hub since 2026-10-27 (owner). Whole-file
  hard links make each snapshot cost ~8.5 GB, so a longer history waits on
  E16. Whether a third copy goes off-site: **owner decision.**

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
  **Cause found 2026-10-26**: bitget serves 100 bars a page on `3d`/`1w`,
  freqtrade assumes more and stops after one page (JST `1w` ended
  2023-11-20), and a window that starts before a pair's listing comes back
  empty, which the old 2020 fallback hit for every newer pair. bitget spot
  `3d`/`1w`/`1Mo` now go through ccxt from each pair's listing (E15).
- Yahoo adjustment differs per file: sp500 has 429 raw and 69 adjusted,
  nasdaq100 93/15, russell2000 93/1823, delisted 33/0. **Decided 2026-11-13:
  RAW.** It matches IBKR, needs the fewest re-downloads and keeps true price
  levels. The adjusted files were re-downloaded raw by `redownload_yf`
  (runs yf-redownload-20261113T132948Z and the follow-up for the 231 files
  adjusted only from 2024-08-07), backed up and row-recorded; nightly YF
  stays raw and refuses a restated tail.
  **The only exceptions to RAW** (owner, 2026-11-13): sp500/WBD,
  nasdaq100/WBD, sp500/PSKY, sp500/KVUE, delisted_stocks/AVB and
  delisted_stocks/EQR stay dividend-adjusted and are labelled `adjusted`.
  Yahoo serves no raw history before 2021 for WBD or PSKY: DISCA, DISCK,
  VIAC and CBS return nothing, and PARA starts 2021-02-12. For KVUE, AVB
  and EQR, Yahoo now serves a single bar.
  download_yf skips a file stored under the other adjustment, so the
  nightly can't append raw bars to them.
  Labels record the request, not the content: `tools/check_adjustment.py`
  measures each YF node against IBKR TRADES (else other universes) and lists
  mislabelled nodes (sp500 AAPL and NVDA are `raw` but dividend-adjusted) and
  IBKR files with unadjusted split jumps (XOM 2001, PG 2004). Its counts are
  the input to this decision.
- Kucoin spot may hold reused tickers spliced into one series. Across the
  hole starting 2024-08-15/16 the exchange has no bars, but the close jumps
  about 316× on ARX (0.000927 to 0.29304) and about 233× on AI (0.0002209 to
  0.05151); LIT moves about 3×. A different token on the same ticker would
  fit, and so would a redenomination: token identity is unchecked (found
  2026-11-06). If so, returns across the join are wrong and no fill can fix
  them. The other 11 of kucoin's holes of 100+ candles need the same check.
  **Owner decision**: whether to split these series.
- Kucoin BRISE/USDT is a live pair we aren't updating: kucoin serves bars to
  2026-11-01, and our file stops 2024-08-15. Cause unknown. The other 2
  stopped kucoin symbols are unchecked; don't mark any of them `retired`.
- Stock universe dedupe conflicts. **CRWD closed 2026-11-14**: the stray
  `ibkr/nasdaq100/CRWD` copy (1d 1h 4h 8h, 4x pre-split scale before
  2026-06-15) was dropped on the owner's yes, via `dedupe_stock_universes.py
  --drop-conflicts`, run `dedupe-drop-20261114-crwd` (revision record under
  `<data root>/revisions/`). nasdaq100 now points at `ibkr/sp500/CRWD`, and
  `stock_home_universe` keeps downloads there. **HON (0.954x) is still open.**
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
- flux-quant (added 2026-11-13): `flux/src/words/indicators.rs`
  `resolve_symbol_path` still read `stocks/<SYM>.parquet` and
  `indexes/<SYM>.parquet`, so every stock and index load failed. Crypto still
  resolved. flux-quant's Architect is moving it to look up `file_path`
  (catalog YAML, or PDM `series`) instead of building paths. Its
  `kalman_hmm_nasdaq_100` study expected `stocks/nasdaq/<T>.parquet` and
  `indexes/NDX.parquet`.

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

## E15 — The full crypto universe

**Scope.** Every spot pair, in any quote currency, and every perpetual,
linear or inverse, on binance, kucoin, bybit and bitget. `1h` and up, from
listing, kept current by the nightly schedules. Approved by the owner
2026-10-26, non-USDT pairs included. Dated futures are out of scope.

- `download_crypto --universe` lists what the venue trades now. Leveraged
  tokens and bitget's innovation zone are left out: ~3,000 on-chain tokens,
  against 502 pairs on its main board. Venue counts on 2026-10-26:

  | Venue | Spot listed / ours / new | Perps listed / ours / new |
  |---|---|---|
  | binance | 514 / 449 / 199 | 747 / 146 / 603 |
  | kucoin | 810 / 908 / 422 | 697 / 74 / 626 |
  | bybit | 388 / 94 / 302 | 808 / 71 / 739 |
  | bitget | 502 / 796 / 280 | 857 / 0 / 857 |

- Catalog pairs a venue no longer lists are marked `retired` with a date and a
  reason, not deleted. That is 134 on binance spot, 520 on kucoin, 8 on
  bybit, 575 on bitget, and 7 perps. `status` counts them apart, the stale
  alert ignores them, and downloads skip them.
- New series start at the pair's listing. bitget and kucoin return nothing
  for a window that begins before then.
- The new catalog `crypto_futures_bitget` goes through ccxt: requests at 1d
  and above are capped at 90 days, and bitget fills a window from its end.
  Funding for bitget remains open (E14).
- Non-USDT markets add ~1,180 spot pairs and ~190 perpetuals. Spot quotes
  include TRY, USDC, BTC, EUR, USD1 and FDUSD; perpetuals settle in USDC,
  USD1, or the coin itself (inverse). They are downloaded the same way. Perps
  that freqtrade can't take go through ccxt. Funding resolves each contract's
  venue id (bybit's USDC BTC perp is `BTCPERP`), and skips retired contracts.
- Estimate: ~5,400 new pairs, ~170M bars, ~5 GB of parquet (29 bytes a bar
  measured). That is roughly 20–25 h on the one crypto lane, dominated by
  kucoin and bitget futures (200-bar pages). Nightly refreshes grow to
  ~6,700 pairs.
- Started 2026-10-26 04:21 UTC: the seven crypto schedules now run
  `--universe`, joined by `crypto-futures-bitget-daily` (03:30), and all
  eight ran at once as the backfill. The night's old-code bitget spot run was
  cancelled first; it would have taken ~15 h. That cancel left freqtrade
  running as an orphan: jobs now lead their own process group, and cancel,
  stop and timeout signal the whole group.
- Bitget spot has about 1.18M missing candles across 1d/12h/6h/4h/1h (found
  2026-11-06), mostly 300-candle holes. The bars are still on the exchange.
  Merged, not deployed: spot moves to the ccxt route, and
  `tools/fill_crypto_holes.py` fills holes only, checking each window at the
  exchange and recording real absences in `exchange_empty.yaml`. Bitget's
  history endpoint omits a bar that closes after `until`, so a bounded walk
  asks for one bar past the hole. 3d files hold two interleaved grids from
  2024-05-17.
- Trial 2026-11-07 (`--limit 20` each on 1h-1d and 3d/1w): 28 holes filled,
  1,025 candles, 0 failed. A8's refilled 1d matches its 1h across 842 days
  (median difference 0; largest open 2%, volume 25%, not traced). All 12 holes recorded empty are
  10SET (8 of them) and AVA or 10SET at 3d/1w. Recorded empty doesn't always
  mean nothing traded: bitget no longer serves 10SET before about 2026-03 or
  AVA 3d at 2024-02, including bars we already hold, while AAVE's same window
  is served. So 10SET's four 300-candle holes are our paging loss and can't be
  refilled from bitget. After the full fill, separate these from true absences
  in the record (`first served bar` per series), and check 10SET for ticker
  reuse: 0.8463 before its 2024-05 to 2026-03 hole, 0.01 after (E12, unproven).
- 26 bitget spot pairs have other timeframes but no 1d file (ZEC, KITE, MON
  and others), 24 active and 2 retired (SAUCE, PACK). The fill reads holes
  in existing files, so it won't create them. The first `--universe` run on
  the ccxt route did (2026-11-07, job 578fc925cdab, 4,393 files updated, no
  errors in its log): 1d count 1,133 of 1,135, only SAUCE and PACK absent.
  All 24 start the same day as their 1h, and why the freqtrade route skipped
  them is unknown.
- Some series start late. From the catalogs (2026-11-07), series at 1h to 1d
  starting more than 2 days after the earliest such series of their pair,
  retired pairs included. 3d, 1w and 1Mo are left out: they're stamped at a
  period start, which makes a comparison with them meaningless.
  Bitget spot, 407 pairs: 1d 254/1133, 1h 176, 4h 175, 6h 61, 12h 52. Kucoin
  perps, 250 pairs: 1h 250/719, 2h 216, 4h 141. Binance spot, 66 pairs: about
  63 each at 1h, 2h, 4h, 8h and 1d, starting 2020-01-01 where 6h and 12h go
  earlier. Kucoin spot, 21 pairs: 18 at 1d, fewer below. Bybit spot and
  binance, bitget and bybit perps: none.
  Kucoin perps probed live 2026-11-07 (24 active series): the venue appears to
  stop serving at a date per timeframe, about 2024-08-05 at 1h, 2024-04-07 at
  2h and 2023-08-11 at 4h. Series starting there got nothing before their
  first bar: 4h 8 of 8, 2h 5 of 5. By catalog start, 532 of the 607 late series
  sit at those dates and 75 start after them (1h 35, 2h 40). The sample's
  newer-listed pairs, ALLO, AAOI, LIGHT and VRT, are still served before their
  first bar, so those look fillable. Some 1h series starting up to 11 days past
  the date (ARB, MANTA, SOL, FIL) also had bars before their first bar.
  Binance spot's 2020-01-01 starts are the first backfill's `--since`, carried
  in the catalog entry's `start_date` and copied to every later timeframe (the
  step 3 fix stops the copying; it reaches no existing file). The front repair
  can reach these 48 active spot pairs: ANKR, ARPA, ATOM, BAND, BAT, BCH, BNB,
  BTC, CELR, CHZ, CVC, DASH, DOGE, DUSK, ENJ, ETC, FET, FTT, HBAR, HOT, IOST,
  IOTA, IOTX, KAVA, LINK, LTC, MTL, NEO, ONE, ONG, ONT, QTUM, RLC, RVN, STX,
  TFUEL, THETA, TRX, TUSD, USDC, VET, WIN, XLM, XRP, XTZ, ZEC, ZIL, ZRX; and
  3 binance perps: BCH, BTC, ETH (USDT perps listed 2019-09).
  Bitget 1d clusters on 2023-07-24, 2022-03-11 and 2026-09-27. None of the
  24 new files are among them, so these are older files: nightlies resume from
  the last stored bar, and neither the nightly nor the hole fill looks before
  the first bar. `--prepend` doesn't help on the ccxt route, which ignores it.
  Only GRASS 1d is confirmed served at the exchange. Candidate repair:
  `fill_crypto_holes` treats the span from the pair's earliest series to a
  file's first bar as a hole, so answers land on the same record. Not built.
- Full bitget spot fill ran 2026-11-11 13:32-15:03 UTC (job 8bf641537916,
  rc 0): 4,531 holes filled, 950,379 candles added, 162 windows of 155,076
  candles recorded empty, 0 failed. Two shapes dominated: a shared 1d gap
  2024-10-28 to 2026-09-26 (699 bars; OP_USDC, ZKL, ZRO and others, their
  old counts match a front plus the 45 bars since 2026-09-27 exactly), and
  single interior bars at venue-wide instants (1h 2024-03-21 16:00 and
  2026-09-02 16:00, 4h 2023-11-17 16:00, 6h and 12h 2023-09-10 00:00, 6h
  2025-12-10 00:00: each added to 5 of the 6 sample pairs), so not the tail
  bar a nightly would have fetched. The 28 series that start 2026-09-27 had
  no bars before that gap and stay late starts: the fill doesn't reach fronts.
  Record split by the sibling rule (a window counts as traded when another
  timeframe of the pair holds a bar inside it), whole record incl. the trial:
  174 windows, 177,163 candles, 11 pairs; 169 windows and 177,158 candles
  traded, 5 single 4h bars nothing traded. Of the traded candles, 1h 138,817,
  4h 31,404, 1d 4,688, 3d 1,573, 1w 681. The rule overstates "no longer
  served" for single bars at the venue-wide instants above (1h 2024-03-21
  16:00 on 7 pairs, 3d 2024-02-08 on 7, 4h 2023-11-17 16:00 on 5): a day bar
  exists but the venue has no 1h bar, which is an outage, not lost history.
  The split isn't built; this is a one-off read.
- Shared-gap scan 2026-11-11 (host files, gaps over 30 days at 1h to 1w, all
  seven non-bitget-spot venues; bitget perps have none): no venue-wide gap on the freqtrade
  route. Binance, bybit spot and binance, bybit perps: 0 shared; what exists
  is per-pair trading halts on every timeframe at once (binance 15 USDC pairs
  2022-09 to 2023-03, 12 kucoin spot pairs from 2024-08 incl. the E12 ARX, AI
  and LIT suspects, single pairs otherwise). Kucoin perps (ccxt route): 217
  gaps in 66 pairs, about 535,000 bars, mostly 4h (66 gaps, 223k), 8h (67,
  162k), 1d (59, 22k) and 1h (16, 125k), on scattered dates, some multi-year
  (BTC 8h from 2021-09, SHIB 8h from 2021-06). A kucoin perps hole fill
  (`fill_crypto_holes --exchange kucoin --trading-mode futures`, untried on
  that venue) goes before its 15m backfill; 1h windows before 2024-08 are
  past the venue's cutoff and will land on the record. Plan 2026-11-11:
  172,090 holes, 790,022 candles, 174,573 requests, of which 171,793 holes
  are under 24 candles (mostly 1h, 1 to 3 bars: kucoin writes no candle for
  a period with no trades, so most are expected to be nothing-traded). With
  `--min-candles 24`: 297 holes, 519,276 candles, 2,780 requests. Order: the
  big holes first (trial `--limit 20`, then full), then a `--limit 200`
  sample of the small ones to measure the empty ratio before deciding
  whether 172k requests for 270k candles are worth it.
  Trial 2026-11-11 16:26 UTC (job f51793492984, `--limit 20`): 20 holes
  filled, 47,108 candles, 0 empty, 0 failed, 247 requests in 158 s. AAVE and
  ADA 4h after it: no duplicate, gap or off-grid bar; 1d against 4h closes
  and highs agree (median 0, p99 0.2%). Found while checking: kucoin perps
  1d volume disagrees with the 4h sum on 2023-08-11 to 2025-03-15 (ADA 334
  of 1,188 days, median ratio 230; AAVE 327 days, median 0.6), all
  pre-existing bars, prices unaffected. Settled the same day: not a basis
  mix but the wrong venue. Kucoin perps files hold blocks of bars identical
  to binance perps in close and volume, 1,500 bars a block (binance's page
  size), and the binance venue confirms them as its own (ADA 2024-06-03:
  binance 362,292,555, kucoin 1,187,886 contracts). Host scan of the 4,619
  kucoin perps files with a binance twin, bars equal in close and volume:
  4h 29 of 724 series (43,500 bars, 28 of them 2023-08-26 to 2024-05-02), 8h
  29 (43,500, 2020-11 to 2022-08 in three blocks), 12h 26 (38,644, 2020-10 to
  2023-06), 1d 29 (41,156; 12 pairs 2021-02-05 to 2025-03-15, 7 to
  2025-10-01, 3 to 2026-04-30), 1w 28 of 719 (3,640, all 2023-11-06 to
  2026-04-27), 2h 26 (9,733; 24 pairs 2026-07-02 to 2026-07-26), 1h 5
  (6,237). About 186,000 bars on the 29 majors both venues list (AAVE, ADA,
  ALGO, ATOM, AVAX, BCH, BTC, DOGE, SOL and others). No copy after
  2026-07-26, so the route since 2026-10-26 writes kucoin bars; how the
  blocks got in is not established. Prices differ from kucoin's by well
  under 1%, volume by 10 to 300 times, so price features barely notice and
  volume features are wrong. The trial's filled bars are kucoin's own (4h
  after 2023-08-11, where kucoin serves them). Remedy, owner's call since
  it deletes bars: drop the copied blocks, then one hole fill refetches from
  kucoin what it serves (1d and 1w far back; 4h from 2023-08, 8h and 12h
  from where the probe shows) and the rest becomes honest holes. Until then
  the trust record marks these ranges as another venue's bars. The full
  kucoin perps fill waits for that decision, so it runs once.
  Twin scan of every venue pair 2026-11-11 (host, 44,867 files, 28,791
  symbol-timeframe groups, blocks of 24+ bars equal in close and volume):
  binance perps is the source of every large block, and three venue probes
  confirm the direction (binance perps files equal the binance venue; bybit's
  own ADA 1h bar for 2026-07-12 12:00 differs from the stored one, which is
  binance's; bitget spot's stored ADA 1d 2025-06-03 and 6h 2025-01-10 are
  binance's to the digit). Series and bars holding binance perps blocks:
  - kucoin perps: as above, ~186,000 bars.
  - bitget spot: 6h 22 series 32,846 bars and 12h 22 series 16,434 bars
    (both 2024-08-23 to 2025-08-31), 1d 23 series 11,394 (2025-03-16 to
    2026-07-25), 3d 26 series 6,757 (2024-05-18 to 2026-07-22), 1w 23
    series 1,089 (2025-10-27 to 2026-07-06). About 68,500 bars.
  - bybit perps: one event, 2026-07-02 to 07-24: 2h 28 series 7,532 bars,
    6h 28 series 2,520, 12h 28 series 1,260, and 2026-07-11 to 07-18 at 1h
    29 series 5,452 and 4h 29 series 1,363. About 18,100 bars.
  - kucoin spot: 1w FET, FIL, STG (302 bars), STG 8h 1,494 and 1h 360.
  - binance spot: WBTC 3d 266 and 1w 113, STG 12h 88. Small.
  - STG_USDT carries binance perps bars in every venue's file for 2026-07-11
    to 07-26, and WBTC, FET and FIL at 1w on spot venues.
  Two events stand out: 2024-08-23 to 2025-08-31 (bitget spot 6h and 12h,
  kucoin perps 12h for ICP and QNT) and 2026-07-02 to 07-26 (bybit perps at
  every timeframe, kucoin perps 2h, bitget spot 1d/3d/1w ends, STG
  everywhere). The second is after the data moved to the host and before
  the ccxt route; the job that did it is not identified. Binance perps and
  binance spot files are clean as targets. Deleting all of it is the same
  owner decision as the kucoin blocks; then each venue's hole fill refetches
  what its venue serves. The 15m backfill on a venue starts only after its
  copied blocks are gone, binance first since it's clean.
  Root cause, looked for 2026-11-11 and not found: PDM job logs on the host
  start 2026-10-24, so July has none; the only commit between 2026-06-12 and
  2026-10-24 is 1ca8f6b "wip" (2026-07-06), whose downloader used a fresh
  temp dir per run and `crypto_futures_file_path(exchange, ...)` for every
  write, with kucoin the only ccxt futures venue; the layout migration of
  that commit touches no crypto; the laptop shell history holds no crypto
  download in July (an external `ai-quant` script in April-May wrote binance
  spot only). So the writer of the 2026-07-02 to 07-26 blocks is unknown and
  could recur; the twin check below is the guard.
  **Status 2026-11-14:** the twin repair is complete above 24 bars
  (2026-11-13, check_twins 0 blocks). Sub-threshold residue was found
  2026-11-14 and is being removed: kucoin spot 1w, 116 Monday-grid copies
  of binance perps (2026-06-29 to 07-20) plus 98 UXLINK rows resampled from
  our own 1d (pair delisted at kucoin); bybit perps 1d 232 and 1w 90;
  bitget spot 1w 42. The lowered sweep (every venue pair, 15m-1w) also
  found binance perps bars in binance spot 3d (22 files, 204 bars,
  2026-06-28 to 07-22), binance spot 15m (9 files, 99 bars,
  2024-08-07 21:00-23:45 UTC, the YF seam's date too: probable
  bulk/incremental boundary, unproven), kucoin spot ADA 15m (1,500 bars,
  2025-02), bybit spot LTC 15m (4) and
  STG 1w in binance spot and bybit perps; 0 blocks at 1h-12h except three
  scattered chance singles. check_twins now calls 3 equal bars in a row a
  block at every timeframe (24 scattered as a backstop) and scans 15m; the
  merge refuses crypto 1w rows off the venue's weekly grid.
  Delete tool requirements (py-qwant via Product, 2026-11-11): the dry run
  prints exact ranges per venue, series and timeframe; the real run writes
  every deleted and later refetched row to the revision record with old and
  new values and a content hash per row, beside the catalogs if the step 2
  log isn't built yet. Their exposure is bybit perps.
  Delete tool, built 2026-11-12 (uncommitted): `tools/delete_twin_bars.py`,
  crypto lane, dry run by default; `--apply` backs up outside the data root,
  writes `revisions/<run id>.parquet` (old OHLCV + sha256 per deleted bar)
  and `.yaml`; `--log-refill` adds `refetched`/`not_served` rows. Per venue:
  dry run, apply, `fill_crypto_holes --apply`, `--log-refill`,
  `check_twins --record`, run id to py-qwant. bybit perps dry run: 142
  spans, 18,127 rows, 29 symbols, all equal; py-qwant no objection
  (2026-11-12). Order: bybit perps first. Other dry runs (2026-11-12):
  kucoin perps 172 spans, 186,407 rows, 29 symbols (1h 6,234, 2h 9,733,
  4h 43,500, 8h 43,500, 12h 38,644, 1d 41,156, 1w 3,640); bitget spot 118
  spans, 68,970 rows, 26 symbols; kucoin spot 5 spans, 2,156 rows (FET,
  FIL, STG 1w; STG 1h, 8h); binance spot and bybit spot none; bitget perps
  not run (stopped for laptop memory). Every row to delete equals binance
  perps. Inside a span only rows at the source's timestamps go: kucoin
  weekly bars open Thursday and bitget 3d bars sit between binance's, and
  both are interleaved with the copies (199 and 1,100 native rows kept).
  kucoin perps copies reach back to 2020-07, before kucoin serves 1h-4h
  history (from 2023-08 to 2024-08), so most become `not_served` and those
  files will start later.
  A second source: binance *spot* bars copied into bitget spot (WBTC 1w
  113, 3d 266) and kucoin spot (WBTC 1w 101, STG 12h 88), the host scan's
  "binance-spot vs" rows (pairs are listed alphabetically, so binance comes
  first; binance's own files are clean). Run those venues twice, once per
  source: `--source binance-perps` and `--source binance-spot`. Before
  `check_twins --record`, the remaining list must be empty or each block
  explained; never record an unexplained block.
  **Rule (product, 2026-11-12):** bars a venue doesn't serve stay recorded
  gaps (`not_served`, exchange-empty), never filled from another venue. No
  fill, fallback or merge may write one venue's bars into another's file.
  Twin check, built 2026-11-11 (uncommitted): `tools/check_twins.py`, audit
  lane, read-only. It groups spot and perps files by symbol and timeframe,
  joins every venue pair on timestamp and reports blocks of 24+ bars equal
  in close and volume (volume 0 never counts). Blocks in `twin_blocks.yaml`
  at the data root are known; the job exits 1, so alerts, only when a block
  is new or has grown past its recorded bar count. `--record` rewrites that
  file from a whole scan. Six tests in `tests/test_check_twins.py`. Deploy
  steps: pull, restart pdm, run `check_twins --record` once on the host to
  acknowledge the backlog above, then add the schedule `check-twins-daily`
  (`check_twins --json`, 09:45 UTC daily: the crypto lane ran until ~08:00
  UTC on 2026-11-11 and the audit lane runs in parallel, so an earlier scan
  reads files mid-write; move it later if 15m lengthens the window). The
  scheduler has no "after job X" chaining. The host refused the schedule
  before deploy because its PDM does not know the tool yet. After a venue's
  copied blocks are deleted and refilled, `--record` again so the known list
  shrinks to what remains; otherwise a block reappearing in a deleted range
  would read as known and not alert.
- 16 binance holes matched the exchange and belong on the exchange-empty
  record, as do kucoin's ARX, AI and LIT windows (see E12).
- Candidate, not chosen and outside the done-when below: retired contracts lose
  their tail. `mark_retired` retires a pair the night it leaves TRADING with no
  final fetch, so `status` calling retired data complete is false (TON, STG and
  VELODROME end days before the exchange's last bars). `download_funding` skips
  retired contracts too. Possible work: a final fetch on retirement, a one-off
  tail refill, and a truer meaning for retired. Why kucoin's retired series end
  2023-02 to 2025-06 is unexplained.
- **Closed 2026-11-14:** delisted Binance (and bybit) USDT perps for a
  point-in-time universe. flux-quant gave a final no (owner decision, their
  msg_02fd4af1) and py-qwant only ever said "partly"; not scoped, not built.
  The retired-tail item above is separate and stays a candidate.

**Done when.** Each venue's catalog matches its listed universe, every new
series reaches back to its listing, `status` shows them current, and
`check_gaps` finds no internal gaps except recorded exchange-empty windows,
with history the venue no longer serves counted separately.

## E16 — Row-level backup deltas

**Why.** rsync's hard links work on whole files. Each nightly update adds a
bar or so to most parquets, so most files are copied whole again. The
2026-10-27 snapshot added 8.5 GB (5.3 GB over the network) for a few MB of
new rows. `PDM_BACKUP_KEEP=3` holds the disk at ~60 GB, but that buys only
three restore points. restic gains nothing: the median file is 126 KB, below
its 512 KB minimum chunk. Measured: 0% as files are written today, ~4% with
4096-row groups.

**Off-the-shelf, rejected.** TimescaleDB, QuestDB, ClickHouse, Delta Lake and
ArcticDB all back up row by row, or close to it. But each one replaces the
parquet files that forthkit, py-qwant, the tools and the catalogs read
directly, to fix a problem that exists only in the backup. Year partitions
were also rejected: they change the reader-facing layout, and the current
year's file is still copied whole every night (~1 GB for crypto).

**Scope.** Approved by the owner 2026-10-27; built the same day.

- `tools/backup_delta.py export` runs on the host as the `backup-delta` PDM
  schedule (07:15 UTC, in its own `backup` lane, so a backfill never holds it
  back). `.deltas/manifest.parquet` stores, for each file: size and mtime (an
  unchanged file is not read), row count, a hash of all rows but the last 64,
  and per-row hashes of those 64.
  - Old rows intact: only the rows from the first that differs go to
    `.deltas/<stamp>/data/<path>` (`append`). The cut is recorded as the
    `timestamp` of the last row kept, so a revised last bar is fine, and so is
    a base taken between two exports.
  - Older rows changed (split re-download, stamp repair), no strictly
    increasing int64 `timestamp`, a new file, a catalog: whole (`full`).
    Deletions are listed, and `paths.txt.gz` lists every file.
  - The delta dir is written as `<stamp>.tmp` and renamed, then the manifest
    is replaced. A crash between the two makes the next export diff against
    the same parent again, and restoring both is harmless. The first export
    records the manifest only. The host keeps 14 days of deltas.
- `.deltas/` is at the top of `price-data/`, so `pdm-ro` reads it with no new
  key. The watcher and the `check_*` tools walk the asset-class directories
  only.
- **The hub.** `pdm-backup.sh` with `PDM_BACKUP_BASE_EVERY` pulls the new
  deltas every run, and takes a full snapshot (a base, stamped
  `.backup-started`) only when `hub-plan` asks for one: none yet, the newest
  is that old, or the deltas don't chain. `hub-prune` drops deltas no kept
  base needs. Monthly, not weekly as first proposed: over a week nearly every
  active file changes, so a weekly base would cost almost a full copy each
  time. The hub timer moves to 09:00 local, after the export.
- `backup_delta.py restore --backup-dir D --dest OUT [--until STAMP]`: it
  hard-links the base, replays the deltas from two days before the base
  began, and checks every touched file against its recorded row hash and the
  file list against the last delta's. Then E10's steps apply (`check_catalog
  --fix`, `check_schema`).
- Tests (`tests/test_backup_delta.py`, `tests/test_pdm_backup.py`): appends
  with a revised last bar, rewritten history, new, deleted and non-parquet
  files, a touched-but-identical file, any of seven nights, a base between
  exports, an export that died before its manifest, a gap in the chain, a base
  older than the chain, both prunes, worker processes, and the script end to
  end in delta mode.

**Deployed 2026-10-27.** The first export read all 68,794 files in 244 s
(4 workers, niced) and wrote a 36 MB manifest. A live rehearsal on the host
used a hard-linked scratch base (`~pdm/work/e16-test/hub/base`, begun
19:48:25 UTC). The second export, 90 s later and mid-backfill, read 849
changed files in 4.7 s: all new series, so all `full`, 92 MB. Restoring base
+ both deltas took 7.6 s, and all 69,642 files matched the manifest's hashes
(0 mismatches). PDM restarted with the `backup` lane; the `backup-delta`
schedule runs at 07:15 UTC. The hub timer is off, at the owner's word, until
E16 is done. The scratch base stays for the 7-night test.

**Four nights in (2026-10-31).** The 07:15 exports ran 10-28 to 10-31, now
mostly appends (10-31: 49,012 append, 421 full). They took 374–650 MB a night,
though, because each append was its own parquet: ~4 KB of footer for ~7 rows.
Restore was slow too: one core, every delta rewriting every file it touched
(base + 6 deltas: 25+ min). d79b8f5 packs the new rows into one Arrow IPC file
per column layout (format 2), and restores each file once through all its
deltas, across worker processes. On a hard-linked copy of live data, 18,182
appends packed into 5.9 MB. Base + 7 deltas (both formats) restored 55,863
touched files in 240 s with 4 workers, and all 77,162 files matched the
manifest. Format 2 starts with the 11-01 export.

**Done when.** A restore of base + 7 deltas into a scratch tree matches the
live tree row for row (restored files are rewritten, so bytes differ), and
the hub keeps 30 days of restore points in less disk than `KEEP=3` uses
today.

## E17 — Trustworthy breadth

Owner goal, 2026-11-07: "as much trustworthy data across providers and asset
classes as possible". Nothing here is built or downloaded. Trust comes first:
a new source is only worth holding if we can say how far it can be believed.

**Gate.** Steps 3 to 5 wait for steps 1 and 2, with one exception: Yahoo chain
snapshots (step 3) start once the layout in step 4 is agreed, because every
missed day is lost for good.

**1. Trust debts.** The open items, not new work:
- bitget spot fill (E15) and the 24 pairs with no 1d file;
- final fetch on retirement, then a one-off tail refill (E15 candidate). First
  confirm how long each venue keeps serving bars after a delisting;
- `status` stops calling retired series complete;
- PDM stalls: find the cause. The fill and every later step run through PDM;
- E16 restore check and the hub timer. Until that passes, step 4 has no backup
  to lean on;
- the russell2000 nightly's rc 1: decide whether per-symbol failures fail a job.

**2. One trust record per series.** Consumers ask how far a series can be
trusted. `status` can't answer: it reads catalogs, not parquet, so it doesn't
see holes.
- A derived file written by a new audit-lane tool, not catalog fields. Catalogs
  have writers enough, and this can always be rebuilt.
- Facts, not a single score: provenance (provider, route, last fetched),
  coverage (start against listing, end against now or delisting), holes less
  windows where nothing traded (history the venue no longer serves still counts
  as a hole), and cross-provider agreement where a second
  source exists. A verdict can sit on top, but the facts stay visible.
- Cross-provider agreement respects the schema: compare like with like
  (`adjustment`, `bar_label`, `volume_basis`), or it reports false conflicts.
- Known trading halts are a fact of their own, not holes: a gap on every
  timeframe of a pair at once (binance's 15 USDC pairs 2022-09 to 2023-03,
  12 kucoin spot pairs from 2024-08, single pairs such as LUNA_TRY) is a halt
  the record names with its dates, so a consumer can tell a halt from a loss,
  and no fill asks the venue for it.
- Revisions: every merge that changes a stored bar's value logs it (series,
  bar, old and new values, run). Nightly resumes overwrite their overlap
  silently today (ccxt route the last 2 bars, freqtrade about a day), so how
  often venues revise bars, and past what margin history is stable, is
  unknown. Each entry counts rows added, changed and deleted, so repairs that
  drop rows (the YF stale-row cleanup) show too. Writers that change stored
  values, all new-wins:
  - `merge_ohlcv_parquet`, every OHLCV download: per bar.
  - `merge_funding` (funding, mark) and `merge_rate_series` (rates): per row,
    the same hook.
  - Whole-file rewrites (`fix_daily_stamps`, `convert_crypto`, the layout
    migration, market caps in `utils/market_caps.py` and
    `download_market_caps`): one row per file per run, with the counts.
  Adjusted series (YF, IBKR adjusted) are rewritten on every split or dividend
  by design, so they log one row per series and event: date range touched,
  bars changed, and the min and max of old/new. A constant ratio is an
  adjustment; a ratio that varies is logged per bar and flagged, because the
  YF split-seam bug was a rewrite that looked like an adjustment.
  Append-only, beside the catalogs so the backup carries it. Build this first
  in step 2, because its value is the history it accumulates.

**3. Free additions, cheapest first.** Size each one with a trial month before
its full run, and put the measured size in this section.
- 15m crypto on all 8 catalogs (owner, 2026-11-11: "1h and 15min crypto
  historical data across the exchanges on spot and futures"; the 1h half is
  step 1's late-start repair). Held today: binance spot 436 files (1.4 GB,
  one-off backfill to 2026-10-25), bybit spot 16; no schedule has 15m, so the
  "sub-1h stall" was never a stall. Sized from the 1h starts of the 7,764
  active pairs at 39 bytes a bar (BTC 15m, 238,877 rows in 9.4 MB): about
  522M bars, 20 GB. Per venue, bars and requests at the route's page size:
  binance spot 129M/129k, kucoin spot 105M/70k, binance perps 56M/56k, bybit
  perps 55M/55k, bybit spot 49M/49k, bitget spot 49M/245k, bitget perps
  46M/230k, kucoin perps at most 33M/165k. The three ccxt venues page 200 bars
  and cost 5 times their size; at the bitget nightly's ~4 requests/s each is
  ~16 h, so they run as daytime chunks over several days. The freqtrade
  venues run 2-7 h each.
  How far back BTC 15m is served (probed 2026-11-11, one pair, one day per
  year): binance spot 2018, binance and bitget spot and perps from 2019-09,
  bybit spot and perps between 2020 and 2022, kucoin spot 2018, kucoin perps
  between 2024 and 2025. Each venue's cutoff is measured on more pairs before
  its run and written here.
  Order: binance perps, binance spot, bybit spot and perps, bitget spot and
  perps, kucoin spot and perps. Each venue is one `--universe --timeframes
  15m` run in the crypto lane, cancelled at 23:30 if still running and
  resumed the next day (both routes keep finished series, freqtrade in
  batches of 50 pairs). Nightly: 15m is added to each venue's schedule after
  its backfill; if the window then passes 09:30 UTC, 15m moves to a second
  lane with the venues offset so none runs in both lanes at once.
  Blocker found by Product: a new series for an existing pair starts at the
  catalog entry's `start_date`, which for pairs from the first backfill is
  its `--since 2020-01-01` (BTC binance perps listed 2019-09; the 63 binance
  spot late starts in step 1 are the same floor). `listing_starts` asks the
  venue only for pairs not in the catalog. Fix before the first run: for a
  pair missing a timeframe, take the earlier of the entry's start and the
  venue's listing (published in the market, else the first monthly bar, one
  request a pair).
- Deribit trades through `history.deribit.com`, with its expired listing. This
  replaces the name guessing in `utils/deribit.py`. `options` lane.
- Daily Yahoo chain snapshots for sp500 + nasdaq100, in a new `chains` lane.
  Delayed and unofficial, and the trust record says so. Runs on the US market
  clock, not a fixed UTC time, or daylight saving moves it off the close.
- Binance `bookDepth`, BTCUSDT, 2023-01-01 on.
- Hyperliquid perps by ccxt and funding from 2023-05. REST candles stop near
  5,000 bars, so history only builds up if the nightly runs from day one.

**4. Tick and book storage.** The parquet schema and `merge_ohlcv_parquet` are
OHLCV only, and stay that way.
- Layout: a new `ticks/<provider>/<symbol>/<kind>/` root, provider first, one
  immutable file per UTC day. `kind` is `trades`, `book_snapshot`, `book_delta`
  or `chain_snapshot`, each with its own checked schema. Files are written once
  and never merged; a correction replaces a whole day.
- Timestamps: integer microseconds, exchange time and receive time apart, so
  capture lag can be measured.
- Catalog: one `ticks_<provider>` entry per symbol and kind, with a per-day
  manifest (rows, hash, first and last timestamp, known gaps).
- Capture runs on Optiplex as its own systemd unit, outside PDM. A PDM restart
  already interrupts the live feed, which capture can't afford. It keeps raw
  payloads until the day's parquet is verified, and records every sequence gap
  and reconnect, because captured gaps can't be refilled.
- Cold store: hpz4 `/mnt/storage`, a single 1.8TB rotational SATA disk, NTFS
  through ntfs-3g, mounted `nofail` (checked 2026-11-07). SMART passes with 0 bad sectors, but no
  self-test has ever run and 64 old command aborts are logged.
  If it fails to mount the path is an empty directory on the 238 GB system SSD,
  so writers must check a sentinel file, not just the path. Reformatting to
  ext4 is an owner decision: 565 MB of `qwant-evidence` is on it.
- What may live only on hpz4: history a provider can serve again. What may not:
  captured books, chain snapshots, catalogs and manifests. Those keep a second
  copy, so that copy's disk, not the 1.9 TB, sets how much capture we can run.
- PDM reads only the manifests. It doesn't serve ticks from hpz4 until a
  consumer needs that.

**5. Paid sources.** Owner decision after trials, at confirmed prices: ThetaData
or Massive for option quotes, Databento for stock depth, Tardis for crypto book
history. A trial is judged against step 2: agreement with what we hold.

**Capacity.**
- The crypto lane isn't overloaded, it's stacked: 11-06's eight nightlies took
  about 9 h end to end (00:15 to 09:20), and the 03:57 funding run started at
  09:22. The rest of the day is free. Catalogs are per venue, so splitting the
  lane by venue is the fix if the delay matters; check that each venue's writers
  share no file first. Don't add Hyperliquid to it until then.
- IBKR: no option work on the account while the Russell 2000 workers run. They
  share one pacing limit and a Gateway on the laptop. Whether the account has
  OPRA, and what IBKR serves for expired options, are untested.

**Open with the owner.** OPRA subscription; full book updates or snapshots; AWS
account for the Hyperliquid archive; a disk for the second copy; reformatting
hpz4.

**Acceptance by step (Product).** Judged on the held data and what a consumer
sees, never on a job's exit code alone.

Step 1:
- Bitget spot: a plan-only run after the fill finds no holes outside recorded
  exchange-empty windows. `1d` holds 1,133 of 1,135 symbols, and the other two
  are named with the reason. Exchange-empty is split into nothing traded and
  history the venue no longer serves, because the second is our loss: those
  windows stay visible as holes in the trust record, and the count is written
  here.
- Crypto history start: each series at 1h to 1d starts within 2 days of the
  earliest 1h-to-1d series of its pair, and each 3d, 1w and 1Mo series starts
  within one of its own bars of that date, or has a named reason such as a
  relisting or history no longer served. Weekly and monthly bars are stamped at
  a period start, so they can't set the reference. Late counts on 2026-11-07
  are in E15, per venue.
- How long each of the four venues serves bars after a delisting is measured
  and written here, before the final fetch is built.
- Tails: on Binance, TON reaches 2026-06-23 08:00, STG 2026-09-24 08:00 and
  VELODROME 2026-10-28 08:00 at 8h, and every other held timeframe ends at the
  same point to within one of its bars. Across all venues, no retired perp has
  funding past its klines without a named reason.
- Final fetch: the next real delisting on each venue ends within one bar of the
  last bar the exchange serves, with no manual step.
- `status` shows retired series apart from complete ones, and a retired series
  still missing its tail is visible there.
- PDM stalls: the cause is named, and for 7 days, nightlies included, a probe
  every 5 minutes never fails twice in a row. The probe is a ping plus one
  `status` and one `pdm_job` read, because ping answered at 06:35 on 11-06
  while `pdm_job` and `pdm_alerts` did not.
- Backups: a restore of a named file set matches by row count and hash, the
  result is recorded, and the owner has turned the hub timer back on.
- Russell 2000 nightly: each of the 47 failures is classed as delisted or other,
  and the job result and alert follow the decision. A nightly can't stay red
  every day and still mean something.

Step 2:
- Records and catalog series match one to one, difference 0, checked per run.
- A consumer gets one series' facts and the record's age in a single PDM call,
  and a record older than its data says so.
- Known cases come out right: bitget spot holes (30 pre-fill files with 474
  holes and 131,349 candles are kept in `tmp/e17-prefill-sample`, on the laptop
  only, with `holes.json`), the STG 1h end before its 8h end, the 16 Binance exchange-empty
  holes not counted as holes, and the kucoin ARX and AI suspects flagged.
- No second source reads "no second source", never as agreement. Each comparison
  names what it compared. YF adjusted against raw on the same stock is not a
  conflict.
- Any verdict links to its facts, and no verdict is shown without them.
- Revisions: a test merges an overlap with one changed bar and finds exactly
  that bar logged with both values, and an unchanged overlap logs nothing. An
  adjusted rewrite with a constant factor logs one row with its range and
  ratio; one with a varying factor logs per bar and is flagged. A repair that
  drops rows logs the count deleted. Funding, rate and whole-file writers each
  have a test that a change is logged. After 30 nights the record states, per venue,
  how many bars were revised and the oldest bar age revised.

Step 3 (all sources: size, rows, requests and run time from the trial month are
written here before the full run):
- 15m crypto: every non-retired pair in the 8 catalogs has a 15m series from
  the earliest bar the venue serves; where that is later than the pair's 1h
  start, the venue's cutoff is measured once and written here. 15m is
  refreshed nightly like 1h: after the window its last bar is at most 1 day
  old, and the window ends by 09:30 UTC. The series pass `check_gaps` and
  `check_schema` and count in `status`; windows the venue doesn't serve go on
  the exchange-empty record with step 1's split. A backfill day delays no
  nightly by more than an hour. Retired pairs only if the venue still serves
  them cheaply. No new 15m series starts at the 2020-01-01 floor when the
  venue serves earlier bars (BTC binance perps 15m starts 2019-09).
- Deribit: bars built from trades match the 4 held 1h and 1d files for the
  BTC 2024-12-27 60000 call and put, with any difference explained. First trade
  dates are recorded for BTC, ETH and the USDC contracts, which are untested.
- Yahoo chains: every trading day stores a snapshot for each universe symbol
  with listed options. Misses are named and alert within one day. The capture
  stays after the close across a clock change and on half days.
- Binance `bookDepth`: every day since 2023-01-01 is held or named as missing.
- Hyperliquid: funding reaches 2023-05-12 and the nightly bars have no gap since
  day one. Daily bars from before the venue traded are dropped or labelled.

Step 4:
- Restart test: stop capture, restart it, and the gap appears in the manifest
  with its start and end. No gap goes unrecorded.
- A book rebuilt from snapshots and deltas matches a live snapshot at sampled
  checkpoints.
- Capture lag per day is readable from exchange and receive time.
- Unmount test: with hpz4 absent, writers refuse, nothing lands on the system
  SSD, and an alert fires.
- A captured day's raw payload is deleted only after its hash matches on both
  copies.
- A consumer can list days and gaps through PDM with hpz4 offline.

Step 5:
- Before any trial: written price, storage and use terms, including whether the
  use counts as professional. OPRA's non-professional rate covers personal,
  non-business use only.
- The trial reports agreement with held data per step 2, on the same symbols
  and days, and the owner's decision is recorded here.

**Done when.** Every series has a trust record that consumers can read through
PDM, `status` no longer counts retired series as complete, each step 3 source
is on a schedule with its measured size recorded here, and a capture restart
leaves a recorded gap, not a silent one.
