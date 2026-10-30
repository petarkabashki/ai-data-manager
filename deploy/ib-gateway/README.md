# IB Gateway on the PDM host

Headless IB Gateway in Docker so `tools/download_ibkr_*.py` have an API to talk to.

```bash
# on the host, as a user that can run docker
mkdir -p ~/ib-gateway && cd ~/ib-gateway
cp <repo>/deploy/ib-gateway/compose.yaml .
cp <repo>/deploy/ib-gateway/ib-gateway.env.example ib-gateway.env
chmod 600 ib-gateway.env && $EDITOR ib-gateway.env     # username / password
docker compose up -d
docker logs -f ib-gateway                              # approve the 2FA prompt in IBKR Mobile
```

Point the tools at it (`.env`): `IBKR_HOST=127.0.0.1`, `IBKR_PORT=4001` (live) or `4002` (paper).

Smoke test, then the real run:

```bash
uv run python tools/download_ibkr_index.py --symbols GSPC NDX --timeframes 1h --since 2024-01-01 --port 4001 --limit 2 --dry-run
```

Notes
- IBKR forces a fresh login about weekly (Sunday) and the container restarts nightly at 23:45 UTC;
  either can need a 2FA approval on the phone. `TWOFA_TIMEOUT_ACTION=restart` re-prompts.
- `READ_ONLY_API=yes` means no order can be placed through this gateway.
- Ports bind to loopback only. Do not expose 4001/4002 on the network.
- Env var names follow the `gnzsnz/ib-gateway` image; check its README if a setting seems ignored.
