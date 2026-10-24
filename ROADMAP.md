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
| [E6](#e6--yahoo-finance-ingestion) | Yahoo Finance ingestion | done (network leg unverified) |
| [E7](#e7--machine-readable-tool-surface) | Machine-readable tool surface | done |
| [E8](#e8--one-source-of-truth-for-docs-and-skills) | One source of truth for docs & skills | done |

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
