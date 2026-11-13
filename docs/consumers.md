# Reading price-data from another project

This checklist is for a repo that reads price-data but never writes it, such
as py-qwant or flux-quant. Work through it in order. Each step says what is
true once it is done.

The worked example is flux-quant (Rust), which loads stocks, indexes and crypto
through `resolve_symbol_path` in `flux/src/words/indicators.rs`.

## 1. Learn the layout contract

Pull the agent-library skill `price-data-layout` (market-data collection). It
is language-neutral and covers the following:

- the path rule
- the two catalog shapes
- the series lookup
- provider choice
- per-file YF adjustment

The one rule that matters most: **resolve `file_path` from the catalog node or
from `/v1/series`, and never build a path.**

The full reference is [data-model.md](data-model.md), with the schema in
[AGENTS.md](../AGENTS.md).

Done when your loader takes (catalog, symbol, timeframe, provider), finds the
node, and opens its `file_path`. It contains no string formatting of
symbol-to-path.

flux-quant today builds `stocks/<SYM>.parquet`, `indexes/<SYM>.parquet` and
`cryptos/binance/<SYM>/<SYM>-1d.parquet`:

- The stock and index paths don't exist, so those lookups find nothing.
- The crypto path happens to match today, but it will break when a venue or
  timeframe moves.

The fix is to replace those joins with a node lookup:

Catalogs sit at the data root (`price-data/<catalog>.yaml`). There is no
`catalogs/` directory.

In the non-binance crypto catalogs, the YAML key carries a venue suffix
(`BTC_USDT_bybit`), while the entry's `name`, its directory and the series
index all use `BTC_USDT`. So match on `name`.

```text
resolve(catalog, symbol, tf, provider?):
    cat   = yaml(data_root / f"{catalog}.yaml")                  # e.g. sp500, indexes, crypto_bybit
    entry = the value v at key k in cat with (v.name or k) == symbol   # key may be BTC_USDT_bybit; name is BTC_USDT
    node  = entry.timeframe_dates[tf]
    if "file_path" not in node:                                  # nested shape (stocks, indexes)
        node = node[provider or "YF"]                            # match keys case-insensitively
    check node.schema == "ohlcv" and node.adjustment is what you expect
    return data_root / node.file_path.split("/", 1)[1]           # file_path starts with "price-data/"
```

Index symbols have no caret: `GSPC`, not `^GSPC`.

## 2. Choose an access path

| Path | When | What you need |
|---|---|---|
| **Read-only mount** (default) | Code that opens parquet files on a fleet host | The `pdm-ro` key on that host, plus the `pdm-data` user unit |
| **HTTP gateway** | Sandboxes and agents whose only egress is HTTP; any language without a parquet reader | Tailnet access to `http://<pdm-host>:5580/v1/`. No token. |
| **Arrow `bars`** | Fast columnar reads without a mount | Same gateway, `format=arrow` |

### The read-only mount (default)

The data host serves price-data to the `pdm-ro` account. It is read-only, it
can see price-data only, and it has a forced `sftp-server -R`. A client mounts
it at `~/work/price-data` with sshfs.

A project keeps its `price-data -> ../price-data` symlink and needs no code
change for the path itself.

**Keys are per host, and the owner grants them.** Each fleet host that runs
your code needs its own mount: gpu26, hpz4, optiplex, and the hub. On each one:

1. The owner adds that host's key to `pdm-ro` on the data host. Key lines are
   root-owned and limited by `from=`. You can't do this yourself. Ask the owner
   and name the host.
2. The owner adds the `pdm-mount` ssh alias to that host's `~/.ssh/config`.
   It uses `IdentitiesOnly yes`, and the real address stays out of every repo.
3. Install the user unit from `deploy/pdm-data.service`, then:

   ```bash
   mv ~/work/price-data ~/work/price-data.retired-<date>   # only if a local copy exists
   mkdir ~/work/price-data                                 # the unit mounts only onto an empty dir
   systemctl --user enable --now pdm-data
   ```

Done when `ls ~/work/price-data/sp500.yaml` works on every host that runs your
code, and writing there fails with "Read-only file system".

Speed: about 0.2 s for a daily series, and about 11 s for a 120 MB 1m file from
a cold cache. Details are in
[server.md, read-only mount](server.md#reading-the-files-directly-a-read-only-mount).

Don't put agent accounts (such as `agfleet`) into the `pdm-data` group to give
them access. Group membership skips the forced read-only command. Use a
`pdm-ro` mount instead.

### Fleet hosts: the agfleet variant

On gpu26, hpz4 and optiplex, code runs as `agfleet`. flux-quant set these
mounts up first, on 2026-11-13, and the owner granted them. The setup differs
from the hub template in three ways:

1. **The key is local to the host.** Each host's `agfleet` generated its own
   key in `~/.config/pdm-mount/`, because `agfleet`'s `~/.ssh` is root-owned.
   The private key never leaves the host.
2. **There is a unit drop-in.** It adds `-F ~/.config/pdm-mount/ssh_config` to
   the sshfs command in `deploy/pdm-data.service`. That ssh config holds the
   `pdm-mount` alias, the key and `IdentitiesOnly yes`.
3. **The owner appends one grant line per host** to
   `/home/pdm-ro/.ssh/authorized_keys` on the data host:

   ```text
   from="<host tailnet ip>",restrict,command="/usr/lib/openssh/sftp-server -R -d /home/pdm/work/price-data" <agfleet's public key>
   ```

   This is the owner's root step. The script is flux-quant's
   `docs/product/fleet-price-data-grant.sh`. It only appends, and it skips a
   key that is already present.

The data host's own setup script, `deploy/pdm-move-prepare.sh`, rewrites only
the hub's key lines and keeps every other line. Re-running it leaves these
grants in place.

To add another host, generate a key there, then ask the owner to add one more
grant line. Never copy a key from another host.

### The HTTP gateway and Arrow

```bash
curl 'http://<pdm-host>:5580/v1/series?symbol=GSPC&timeframe=1d'           # which nodes exist, with file_path
curl 'http://<pdm-host>:5580/v1/bars?symbol=AAPL&catalog=sp500&timeframe=1d&provider=YF&start=2020-01-01'
curl -o btc.arrow 'http://<pdm-host>:5580/v1/bars?symbol=BTC_USDT&catalog=crypto_binance&timeframe=1h&format=arrow'
```

For Rust, `arrow-ipc`'s `StreamReader` reads the `format=arrow` body as it is.
The metadata comes in the `X-PDM-Result` header.

There is also HTTPS at `https://<host>.<tailnet>.ts.net/v1/`, tailnet only.

The gateway refuses every write. The reference is
[server.md, HTTP gateway](server.md#http-gateway-read-only).

Done when one `series` call and one `bars` call succeed from the place your
code runs.

## 3. Pull the skills

In your repo's `.agents/library.yaml`, opt into `market-data` and exclude the
downloaders, because consumers don't download:

```yaml
version: 1
# reads price-data only; data changes go through PDM (ai-data-manager)
collections: [market-data]
exclude:
  skills: [stock-data-downloading, crypto-data-downloading, commodity-data-downloading, macro-data-downloading]
```

Then run `agent-library link --apply` to deliver the skills to every harness.
flux-quant already has this file. It also adds its `quant` collection.

Use `price-data-layout` as the layout source. `data-loading` is Python-only, and
its examples assume a `quant/` tree with `quant/scripts/check_data_status.py`,
so it is not a layout reference.

## 4. Report breakage, don't patch it

Send a message to the ai-data-manager **Product** session (Omnigent,
`sys_session_message`) when you find any of these:

- a series is missing
- a price is wrong
- a gap is unexpected
- an `end_date` is stale
- a catalog node has no `file_path`
- the mount or gateway is down

Include:

- catalog, symbol, timeframe, provider (and session, if any)
- the node's `file_path`
- what you expected and what you saw: rows or timestamps, and the command you ran

To check first without asking, use the gateway's audits. For example:

```bash
curl -d '{"tool":"check_gaps","options":{"catalog":["sp500"],"timeframe":"1d"}}' http://<pdm-host>:5580/v1/audit
```

Do not fill a hole from another venue or provider, and don't hard-code a
workaround path. Bars a venue didn't serve are deliberate gaps.

## Checklist

- [ ] The loader resolves `file_path` from the catalog or `/v1/series`. No path
      is built from a symbol.
- [ ] The loader checks `schema` and `adjustment` on every node it opens.
- [ ] The access path works on every host that runs the code: a mount (key
      granted per host by the owner) or the gateway.
- [ ] `.agents/library.yaml` pulls `market-data` without the downloaders, and
      `agent-library link` has been run.
- [ ] The team knows to report breakage to ai-data-manager Product.
