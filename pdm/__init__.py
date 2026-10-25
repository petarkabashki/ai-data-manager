"""Price data manager (PDM) — serves `price-data/` over the network.

Two ZeroMQ sockets:

  * ROUTER (`PDM_REQ_PORT`, default 5555) — request/reply. Clients connect a
    DEALER and ask for catalogs, series coverage, bars, or to queue a download.
  * PUB (`PDM_PUB_PORT`, default 5556) — broadcast. New bars written by any job,
    job lifecycle events, and live exchange polls. Topics mirror the
    provider-first path rule, so a ZeroMQ prefix subscription selects a class,
    a provider, a universe or one series.

`pdm.client.PriceDataClient` is the Python client; `tools/pdm_client.py` is the
CLI over it. Wire format: `pdm.protocol`.
"""
