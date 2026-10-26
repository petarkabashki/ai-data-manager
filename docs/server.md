# PDM server — price data over the network

`tools/pdm_server.py` serves `price-data/` from the data host to any client on
the network. It does four jobs:

1. **Answers requests**: catalogs, series coverage and bars, over a ZeroMQ
   ROUTER socket (port 5555).
2. **Runs downloads** as queued jobs that call the existing tools, either on
   request or on a schedule.
3. **Broadcasts new bars** on a PUB socket (port 5556) whenever any job or
   tool writes them to disk, plus job events and live exchange bars.
4. **Keeps the data in one place.** Clients never mount `price-data/`.

```
              tailnet (100.x)                           data host
 ┌──────────────┐  DEALER ──req/rep──▶ ROUTER :5555 ┌──────────────────────┐
 │ agent / app  │                                    │ pdm_server.py        │
 │ PriceData-   │  SUB ◀──broadcast── PUB :5556      │  ├ series index      │
 │ Client / CLI │                                    │  ├ job lanes ─▶ tools/*.py ─▶ price-data/
 └──────────────┘                                    │  ├ scheduler (pdm.yaml)
                                                     │  ├ watcher (diffs disk → bar/…)
                                                     │  └ live (ccxt → live/…)
                                                     └──────────────────────┘
```

## Deployment

| | |
|---|---|
| Host | `<host>`, Tailscale `<host-tailscale-ip>` — the real values are kept out of this public repo; set `PDM_SERVER` in your local `.env` |
| Repo | `~/work/ai-data-manager` (git checkout) |
| Data | `~/work/price-data` (`price-data` symlink in the repo, as everywhere) |
| Service | systemd user unit `pdm.service` (`deploy/pdm.service`), lingering so it runs without a login |
| Config | `~/work/ai-data-manager/.env` (`PDM_BIND_HOST` = the Tailscale IP) and `pdm.yaml` |
| Logs | `journalctl --user -u pdm -f`; per-job logs in `tmp/pdm-jobs/` |

The server binds the Tailscale address only, so it is reachable from your
tailnet and nowhere else. Tailscale handles authentication and encryption.
`PDM_TOKEN` adds a shared secret for `submit`/`cancel`/`reload` if more than
one person is on the tailnet.

```bash
# on the data host
systemctl --user status pdm
systemctl --user restart pdm            # after a git pull or a pdm.yaml edit; queued jobs survive
uv run python tools/pdm_server.py --check
```

## Clients

```bash
# any tailnet machine with a checkout (PDM_SERVER=<host-tailscale-ip> in .env)
uv run python tools/pdm_client.py ping
uv run python tools/pdm_client.py --json series --symbol AAPL
uv run python tools/pdm_client.py bars --symbol BTC_USDT --catalog crypto_binance -t 1h --start 2026-10-01
uv run python tools/pdm_client.py submit --wait download_yf -- --universe sp500 --timeframes 1d
uv run python tools/pdm_client.py watch bar/ live/ quote/
uv run python tools/pdm_client.py coverage AAPL
uv run python tools/pdm_client.py schedules --symbol AAPL
uv run python tools/pdm_client.py live
```

```python
from pdm.client import PriceDataClient, with_datetime_index

with PriceDataClient("<host-tailscale-ip>") as pdm:
    df = pdm.bars(symbol="AAPL", catalog="sp500", timeframe="1d", start="2020-01-01")
    for topic, header, frame in pdm.subscribe("bar/cryptos/binance/", "live/"):
        ...
```

A client needs only `pyzmq`, `pyarrow`, `pandas` and `msgpack`. `pdm/client.py` and
`pdm/protocol.py` are the whole client, so they can be copied into another
project.

### Agents

An agent on another machine uses the CLI with `--json`. It's the network twin
of the read-only tools, so the habits in `AGENTS.md` carry over:

| Local (data host) | Over the network |
|---|---|
| `tools/query.py --json --symbol X` | `tools/pdm_client.py --json series --symbol X` |
| `tools/query.py --json --list-catalogs` | `tools/pdm_client.py --json catalogs` |
| `tools/status.py --json` | `tools/pdm_client.py --json status` |
| `load_ohlcv(path)` | `tools/pdm_client.py --json bars …` / `PriceDataClient.bars()` |
| `tools/download_yf.py …` | `tools/pdm_client.py submit --wait download_yf -- …` |

The server runs only tools on the `pdm/jobs.py` whitelist, as an argv list
(never a shell), so a client can't run anything else on the data host.

Agents with MCP use `tools/pdm_mcp.py` instead (registered in `.mcp.json`):
the same calls as native tools, reads separate from writes so the harness's
permission prompt is the approval step. See `cli-reference.md`.

### Audits (no token)

`audit` runs one of the checking tools on the host with `--json` and returns
its report: `status`, `query`, `check_gaps`, `check_schema`, `check_catalog`,
`check_splits`. The caller passes typed options (the tool's flags with `_` for
`-`), never argv, and the option table in `pdm/audit.py` has nothing that
writes. `check_catalog --fix` can't be expressed, and paths must lie under the
data root. Each audit runs synchronously, capped at 600s, with at most 2 at once.
A non-zero `exit` with `ok: true` means the check found problems.

```bash
uv run python tools/pdm_client.py audit check_gaps catalog=sp500 timeframe=1d
uv run python tools/pdm_client.py audit query list_catalogs
```

### HTTP gateway (read-only)

For clients that can't use ZMQ, such as sandboxed agents whose only egress is
HTTP, the server also listens on `http://<bind>:5580/v1/` (`PDM_HTTP_PORT`, 0 =
off). It calls the same code as the ZMQ path, but routes only reads, `audit`
and `refresh` with `dry_run: true`. It never takes a token, and everything
else gets 403.

```bash
curl 'http://<host-tailscale-ip>:5580/v1/series?symbol=AAPL&timeframe=1d'
curl 'http://<host-tailscale-ip>:5580/v1/bars?symbol=AAPL&catalog=sp500&timeframe=1d&provider=YF&start=2026-01-01'
curl -o aapl.arrow 'http://<host-tailscale-ip>:5580/v1/bars?...&format=arrow'   # Arrow IPC stream
curl -d '{"tool":"check_gaps","options":{"catalog":["sp500"],"timeframe":"1d"}}' http://<host-tailscale-ip>:5580/v1/audit
curl -d '{"dry_run":true,"target":{"universe":"sp500"},"timeframes":["1d"],"symbols":["AAPL"]}' http://<host-tailscale-ip>:5580/v1/refresh
```

**HTTPS**: `tailscale serve` on the data host fronts the gateway with a
Let's Encrypt certificate for its MagicDNS name, tailnet-only (Serve, not
Funnel). It persists across reboots, and Tailscale renews the certificate:

    https://<host>.<tailnet>.ts.net/v1/<method>  →  http://<host-tailscale-ip>:5580

Set up once, on the data host:

    sudo tailscale set --operator=$USER      # once, so serve doesn't need root
    tailscale serve --bg --https=443 http://<host-tailscale-ip>:5580
    tailscale serve status                   # check; `tailscale serve --https=443 off` removes it

`GET` takes query params (repeat a key, or comma-separate `symbols` and
`timeframes`, for a list); `POST` takes a typed JSON body. Replies are
`{"ok": true, "result": ...}`. JSON `bars` adds `data: [[ts, o, h, l, c, v], ...]`;
with `format=arrow` the body is the frame and `X-PDM-Result` holds the
metadata. Errors: 400 (refused, with the reason), 403 (not routed over HTTP),
404, 503 (busy or still loading). Reference: docstring of `pdm/http_gateway.py`.

### Refreshes

`refresh` is the narrow, approvable alternative to `submit`. The caller states
*what* (provider, target, timeframes, symbols, since, full) and a **required**
`dry_run`; the server builds the argv itself.

- `dry_run: true` runs the tool with `--dry-run` and returns its plan
  (`download_yf --json`: per symbol and timeframe, the ranges it would fetch).
  Nothing is written: a dry run never constructs a fetcher or saves a catalog.
- `dry_run: false` re-runs that dry run, refuses the request if the tool
  refuses (unknown symbol, bad flag), else queues it: at most one refresh at
  a time, killed after 30 minutes (`timed_out`). The job's `result` is the
  real run's JSON document.

Targets: a stock `universe`, an `asset_class` (`Index`, `Commodity`, or
`Macro`, which is daily only), or a crypto `exchange`. The macro T-bill rate
`WTB3MS` refreshes from Yahoo `^IRX`:
`{"dry_run": false, "target": {"asset_class": "Macro"}, "timeframes": ["1d"]}`.
Perpetual funding rates (Binance, Bybit) are `provider: "funding"`, with no
timeframes: `{"dry_run": false, "provider": "funding", "target": {"exchange": "bybit"}}`
(`pdm_client.py refresh --funding bybit`). They then read like any series:
`series --timeframe funding`, and `bars ... timeframe=funding`.

Full request/reply shape: the docstring of `pdm/refresh.py`.

### Schedules, live feeds, coverage

Both are managed while the server runs, and every change is written back to
`pdm.yaml` (comments aren't kept, so `pdm.example.yaml` holds the docs):

| Method | |
|---|---|
| `schedules [symbol] [catalog]` | List, optionally only those covering a symbol (resolved through catalog membership) or catalog |
| `schedule_put {schedule}` | Create or replace by name; a `refresh:` plan or `tool` + `args` |
| `schedule_pause` / `schedule_resume` / `schedule_run` / `schedule_remove` | By name; `run` fires now, even while paused |
| `live [symbol] [exchange]` | Subscriptions with per-symbol topic, transport, update count, age, errors |
| `live_add` / `live_remove` | Add/remove symbols; `live_remove` without symbols drops the subscription |
| `coverage symbol` | Stored series + schedules covering it + live feeds |

### Reading the files directly: a read-only mount

Projects that read parquet through a `price-data -> ../price-data` symlink
keep working without code changes if a client machine mounts the host's data
read-only at `~/work/price-data`. The template is `deploy/pdm-data.service`
(sshfs, a systemd user unit). It is skipped, not failed, unless
`~/work/price-data` exists and is empty, so it can never hide a local copy:

    mv ~/work/price-data ~/work/price-data.retired-<date> && mkdir ~/work/price-data
    systemctl --user enable --now pdm-data

Reads run at sshfs speed over Tailscale: about 0.2 s for a daily series,
about 11 s for the 120 MB BTC 1m file on a cold cache, and fast once cached.
Writes fail with "Read-only file system". Data changes go through PDM
refreshes on the host. Anything that wrote into the tree from a client must
move: py-qwant's `scripts/sync_funding_rates.py`, which writes
`crypto-futures/<venue>/<SYM>/<SYM>-funding.parquet`, is the one live case.

### Backups

The host holds the only copy of `price-data`, so a client machine pulls
nightly, off-host snapshots. `deploy/pdm-backup.sh` runs `rsync -a --delete`
over ssh (BatchMode) from `$PDM_SERVER:work/price-data/` into
`$PDM_BACKUP_DIR/<YYYY-MM-DD>/` (default `~/backups/pdm-price-data`), with
`--link-dest` on the previous snapshot, so unchanged files are hard links and
each snapshot costs only what changed. It writes `<date>.partial` first
(a failed or interrupted run leaves it, and the next run, on any day, adopts
it and resumes), renames on success, and repoints a `latest` symlink. A
snapshot with under 80% of the previous one's files is never finalized: an
empty or truncated source can't become `latest` or rotate good snapshots out.
`*.tmp` is excluded; a second run on the same day is a no-op. The catalogs
(top-level `*.yaml`) are copied in a second pass after the data, so a
snapshot's catalog is never older than its files. `PDM_SERVER`
and the variables below come from the environment or the repo `.env`
(unquoted or quoted values both work).

Install the systemd user units (`deploy/pdm-backup.service`, idle I/O and CPU
class, and `deploy/pdm-backup.timer`, daily 04:30 local, `Persistent=true`):

    cp deploy/pdm-backup.{service,timer} ~/.config/systemd/user/
    systemctl --user daemon-reload
    systemctl --user enable --now pdm-backup.timer
    systemctl --user start --no-block pdm-backup.service   # first snapshot now

The first run copies everything (28 GB over Tailscale takes a while); follow
it with `journalctl --user -u pdm-backup -f`.

Retention is opt-in: `PDM_BACKUP_KEEP=N` keeps the newest N snapshots and
removes older ones after a successful run. Unset (the default) keeps every
snapshot and deletes nothing, so disk use grows with churn until the owner
chooses a number.

**Restoring.** A snapshot is a plain copy of `price-data`, but one taken
while a download was merging can hold files whose catalog nodes weren't
saved yet, and dates the catalog hadn't caught up with. Step 3 repairs both.

1. On the host, stop the server so nothing writes: `systemctl --user stop pdm`.
2. From the client, copy the snapshot back. `--delete` drops anything on the
   host newer than the snapshot, so to keep newer files, restore into a
   sibling directory first and compare:

       rsync -a --delete -e "ssh -o BatchMode=yes" \
           ~/backups/pdm-price-data/latest/ <data host>:work/price-data/

3. On the host, repair the catalogs and check the files:

       uv run python tools/check_catalog.py --fix
       uv run python tools/check_schema.py

   `--fix` refreshes drifted dates, backfills semantics, and adopts orphan
   crypto files (the path names their catalog). If it still exits 1, it
   lists what it couldn't adopt.
4. `systemctl --user start pdm`. The next scheduled runs fetch whatever the
   snapshot is missing, since every tool plans from what is on disk.

Rehearsed 2026-10-25 into a scratch directory, with `PRICE_DATA_ROOT`
pointing the tools at it. Copying 30 GB (27,067 files) back over Tailscale
took 27 min, and a checksum pass found 0 differences. That snapshot was taken
mid-download: step 3 refreshed 5,608 dates, backfilled 4,242 nodes and
adopted 3,465 orphan files.

## Protocol

Every message is a multipart message: a JSON control frame, plus optionally an
Arrow IPC (zstd) frame that holds a table. Bars stay in the canonical schema
on the wire: `timestamp` int64 ms UTC plus OHLCV float64. See
`pdm/protocol.py`.

Request methods: `ping`, `catalogs`, `series`, `bars`, `status`, `submit`,
`jobs`, `job`, `cancel`, `refresh`, `coverage`, `schedules`, `schedule_put`,
`schedule_remove`, `schedule_pause`, `schedule_resume`, `schedule_run`,
`live`, `live_add`, `live_remove`, `reload`, `topics` (docstring of
`pdm/server.py`). With `PDM_TOKEN` set, every method that changes state needs
the token; reads and `refresh` with `dry_run: true` stay open, so an agent
that holds no secret can still preview (at most 2 dry runs at once). The
server reads `PDM_TOKEN` from its `.env`. A client reads `PDM_TOKEN` from the
environment, else `PDM_TOKEN_FILE` (default `~/.config/ai-data-manager/token`,
mode 0600). Keep it out of the checkout: sandboxed agents that can read the
repo must not be able to read the token.

`series` rows carry the catalog's `common_name`. Filter by it with `name`:
case-insensitive, matching the name's leading whole words, so "apple" and
"apple inc" match "Apple Inc." but "app" and "inc" don't. `fields` keeps only
the listed keys and collapses rows that become identical:
`series?name=alphabet&fields=symbol,catalog,common_name` is one row per asset
(GOOG and GOOGL in sp500 and nasdaq100). The names are whatever the catalogs
hold: crypto pairs read "Bitcoin/Tether", and some entries carry only the
ticker (commodities' `GC=F`), so keep an alias table for spoken names.

`bars` replies are capped at 100,000 rows. A request over the cap is
refused with the matching row count, never silently truncated. Narrow
`start`/`end`, or pass `limit` (the last N rows).

`bars` must match **exactly one** series, or it's refused with the
candidates listed. `AAPL 1d` is in both `sp500` and `nasdaq100`, and those two
YF files are not the same data (one adjusted, one raw). A silent first match
would hand back the wrong numbers.

## Broadcast topics

| Topic | When |
|---|---|
| `bar/<asset-dir>/<provider>[/<universe>]/<SYMBOL>/[<session>-]<tf>` | New rows landed on disk; frame holds just those rows |
| `live/<exchange>/<SYMBOL>/<tf>` | Live bars: every newly closed bar once (`closed: true`), the forming bar on change (`closed: false`) |
| `quote/<exchange>/<market>/<SYMBOL>` | Live best bid/ask, JSON only: `symbol timestamp last bid ask bid_size ask_size` |
| `prices` | With `qwant_compat: true`: py-qwant's feed, byte-compatible — 2 frames, msgpack `{symbol, timestamp, last, bid, ask}` |
| `job/<state>/<tool>` | Job queued / running / succeeded / failed / cancelled / timed_out / interrupted |
| `server/started`, `server/index`, `server/heartbeat` | Lifecycle, index rebuilt, every 5s with each live feed's age |
| `server/alert` | Something needs a look: `kind` job, stale or recovered, one-line `text` (see Alerts) |

Live feeds use websockets through ccxt.pro (`watch_ohlcv`; `watch_bids_asks`
— Binance's bookTicker — or `watch_ticker` for quotes) on binance, bitget,
bybit and kucoin, spot or swap; REST polling when an exchange lacks the
stream or `transport: poll` is set. Disconnects back off 1s → 60s. To move
py-qwant onto this feed, point its SUB socket at `tcp://<data host>:5556`
instead of its local `qwant_prices.ipc`; topic and payload are unchanged.
A subscriber that stops seeing `server/heartbeat` knows the link is down, not
the market quiet.

Bar topics mirror the storage path, so a prefix subscription selects a class
(`bar/cryptos/`), a provider (`bar/stocks/ibkr/`), a universe
(`bar/stocks/yf/sp500/`) or one series (`bar/stocks/yf/sp500/AAPL/1d`).
Topics use `/` and never `.`, because `BRK.B` would otherwise match the prefix
`…/BRK`. End a prefix with `/` to select exactly one node.

The watcher finds new bars by diffing file stats, not by trusting the job, so
a manual tool run on the host gets broadcast too (within `--scan-seconds`).
An update over `--broadcast-max-rows` (such as a new full-history series)
sends the newest rows with `truncated: true`. Fetch the rest with `bars`.

PUB/SUB doesn't buffer for absent subscribers. A client that was offline
calls `bars --start <last seen>` to catch up, then subscribes again.

## Alerts

The server raises an alert when:

- a job ends `failed` or `timed_out`. A scheduled job alerts once; while its
  schedule keeps failing it stays quiet, and the next success sends
  `recovered`. One-off jobs (submits, refreshes) alert every time.
- series that were fresh at the previous check fall `PDM_STALE_ALERT_DAYS`
  (default 4) or more behind their catalog `end_date`. The check runs at start
  and every 6h. A series that stopped updating long ago never alerts, only one
  that stops now, and it sends `recovered` when it catches up.

Each alert is appended to `tmp/pdm_alerts.jsonl` and published on
`server/alert`. `pdm_client alerts` lists the newest ones (also over HTTP).
To get them on a phone or desktop, set `PDM_NOTIFY_CMD` in the host's `.env`.
It is run without a shell; the one-line text goes on stdin and the full alert
JSON in `$PDM_ALERT`:

```bash
PDM_NOTIFY_CMD="curl -fsS -d @- https://ntfy.sh/<your-private-topic>"
```

`pdm_client alert-test` (token) sends a test alert through it.

## Concurrency rules

Jobs run in lanes, serial within each lane:

| Lane | Tools | Why together |
|---|---|---|
| `equity` | download_yf, download_ibkr_*, retry_ibkr_failed_sequentially, check_catalog, fix_daily_stamps | They load, edit and save the same stock/index catalogs |
| `crypto` | download_crypto, download_funding, convert_crypto | crypto catalogs |
| `options` / `marketcap` / `futures` | one tool each | disjoint catalogs |
| `audit` | status, query, check_gaps, check_schema, check_splits | read-only, so never queued behind a download |

The lanes only serialize jobs that go **through the server**. A tool started
by hand on the host while a server job writes the same catalog can still lose
an update. Prefer `pdm_client.py submit` on the data host too.

**Restarts don't lose the queue.** Every job state change rewrites
`$TMP_ROOT/pdm_jobs.json` with the queued and running jobs. On start the
server queues them again, keeping their ids. A job that was running goes back
to the front of its lane with `requeued` incremented and starts over. The
tools resume from what is already on disk, so starting over only re-fetches
the tail. A clean stop marks that job `interrupted`, not `failed`. The 50
newest finished jobs are saved too, results included, so a client polling an
id across a restart still gets its outcome. Older ones survive only as logs
in `$TMP_ROOT/pdm-jobs/`. A deploy can therefore restart `pdm` at any time. The only cost is the
running job's progress since its last write.
