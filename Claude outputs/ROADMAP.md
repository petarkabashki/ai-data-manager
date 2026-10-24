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
| [E5](#e5--calendar-aware-gap-detection) | Calendar-aware gap detection | planned |
| [E6](#e6--yahoo-finance-ingestion) | Yahoo Finance ingestion | planned |
| [E7](#e7--machine-readable-tool-surface) | Machine-readable tool surface | planned |
| [E8](#e8--one-source-of-truth-for-docs-and-skills) | One source of truth for docs & skills | planned |

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
already reads both. `tools/check_catalog.py --fix` (E7) will convert the rest
in bulk; until then, mixed shapes are expected and supported.

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
- Packaging so `sys.path.insert(...)` disappears from nine tool scripts.

**Done when.** `PRICE_DATA_ROOT=/somewhere/else uv run pytest` passes and no
`/home/` string remains in tracked source.

---

## E5 — Calendar-aware gap detection

**Why.** `detect_gaps()` assumes a 24/7 series. It is correct for crypto and
wrong for everything else: every weekend and market holiday reads as an
internal gap. `download_ibkr_stock_overnight.py` feeds those phantom gaps
straight into its range planner in `--session overnight` mode, so it
re-requests ranges IBKR will never fill, on every run, forever.

**Scope.** A session calendar per asset class (24/7, US equity RTH, US equity
extended), threaded through `detect_gaps()` and the IBKR planner; a
`--calendar` override; extending `check_gaps.py` past crypto once gaps mean
something for stocks and indexes.

**Done when.** A daily S&P 500 series with no missing trading days reports
zero internal gaps, and the overnight planner stops re-requesting weekends.

**Depends on** E3 (needs the gap tests to refactor against).

---

## E6 — Yahoo Finance ingestion

**Why.** YF is the largest provider in the repo — 2,557 stock symbols, all 11
indexes, commodities and macro — and there is **no tool that downloads it**.
No `yfinance` dependency, no `download_yf_*.py`. The data can only have been
produced out-of-band, so the largest dataset here has no reproducible refresh
path. `normalize_ohlcv()` already has the `Date`+`Open` branch and
`stock_file_path("YF", ...)` already exists; only the downloader is missing.

**Scope.** `tools/download_yf.py` covering stocks (by universe), indexes,
commodities and macro, using the shared planner from E2 and the shared merge;
loader/saver helpers for `commodities.yaml` and `macro.yaml`, which are the
two catalogs with no CRUD in `utils/catalog.py` at all.

**Also in scope: decide what `close` means.** YF's `Adj Close` is dropped at
ingestion and IBKR's `ADJUSTED_LAST` is off by default, so `close` currently
means different things per provider for the same symbol and nothing records
which. This epic picks a policy, records the adjustment mode in the catalog
node, and documents it in `docs/data-model.md`.

**Done when.** `download_yf.py --universe sp500 --timeframes 1d` reproduces
the existing files byte-for-byte modulo the new adjustment field.

---

## E7 — Machine-readable tool surface

**Why.** This is the real harness-neutrality lever. Today every audit tool
prints an ASCII table an agent has to scrape, and three asset classes can't be
audited at all. The more the tools describe themselves, the less any of this
depends on prose an agent may or may not have read.

**Scope.**

- `--json` on `query.py`, `check_gaps.py` and `check_schema.py`.
- `query.py` covering all asset classes, not only crypto. It currently imports
  `load_assets` and never calls it, and its docstring advertises an
  `--asset-class` flag that does not exist.
- Stop re-reading every parquet file for live ranges by default (unusable
  across 5,019 symbols) — make it `--rescan`.
- `tools/status.py --json`: one cross-class health call (coverage, staleness,
  schema conformance, catalog drift) that replaces most of the "how do I drive
  this repo" prose.
- `tools/check_catalog.py`: detect and `--fix` catalog/disk drift, including
  the flat→nested index migration from E1 and orphaned `file_path` entries.
- `refresh_*_catalog_dates()` for the classes that lack one (futures, index,
  commodity, macro); `refresh_stock_catalog_dates()` exists but nothing calls
  it.
- Fix `retry_ibkr_failed_sequentially.py`, which reads
  `tmp/ibkr_stock_download_progress_{4h,1h}.json` — filenames the downloader
  never writes — and hardcodes `nasdaq100` and a two-timeframe choice list.

**Done when.** An agent can answer "what's stale, what's broken, what's
missing" with one command and no table parsing.

---

## E8 — One source of truth for docs and skills

**Why.** The tool table is documented in four places (`README.md`,
`agents.md`, `docs/cli-reference.md`, `docs/agentic-usage.md`) and has already
drifted: `download_ibkr_index.py` appears in exactly one of them, and the
crypto symbol counts in `.agent/crypto_download.md` disagree with the ones in
`README.md`. Separately, `.agent/crypto_download.md` has no frontmatter, so no
harness can discover it — it is findable only by reading a table in
`agents.md`.

**Scope.**

- `agents.md` → `AGENTS.md` (the cross-agent convention), with `CLAUDE.md`
  reduced to the venv rule plus a pointer. Today `CLAUDE.md` restates the
  layout, data root and metadata files, which is exactly the duplication that
  drifts.
- Generate the tool table from the tools' own `--help` so it cannot drift;
  `docs/` keeps the prose and diagrams and stops restating the table.
- `.agent/*.md` → `skills/<name>/SKILL.md` with `name` + `description`
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
