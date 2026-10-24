"""The three Deribit chart-endpoint behaviours that would corrupt an import.

No network: a stub session answers with the shapes the live API returns,
including the string-typed numbers it really sends.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from utils.deribit import MAX_TICKS, DeribitClient, DeribitError, InstrumentInfo

DAY_MS = 86_400_000
HOUR_MS = 3_600_000


def ms(when: str) -> int:
    return int(datetime.fromisoformat(when).replace(tzinfo=timezone.utc).timestamp() * 1000)


class FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self.payload


class FakeSession:
    """Records every request and answers from a handler."""

    def __init__(self, handler) -> None:
        self.handler = handler
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        payload = self.handler(url, dict(params or {}))
        # The live API wraps everything in a JSON-RPC envelope; handlers here
        # return the bare result, so wrap it unless they meant to return an error.
        return FakeResponse(payload if "error" in payload else {"result": payload})

    def close(self) -> None:
        pass


def client_with(handler) -> tuple[DeribitClient, FakeSession]:
    client = DeribitClient(sleep_seconds=0)
    session = FakeSession(handler)
    client._session = session  # type: ignore[assignment]
    return client, session


def bar_series(start_ms: int, count: int, step_ms: int, volume: float) -> dict:
    ticks = [start_ms + i * step_ms for i in range(count)]
    # Deribit sends every number as a string.
    return {
        "status": "ok",
        "ticks": [str(t) for t in ticks],
        "open": ["1.0"] * count, "high": ["1.0"] * count,
        "low": ["1.0"] * count, "close": ["1.0"] * count,
        "volume": [str(volume)] * count,
    }


CONTRACT = InstrumentInfo(
    name="BTC-27DEC24-60000-C",
    listed=date(2024, 12, 20),
    expiry=date(2024, 12, 27),
    tick_size=0.0001,
    contract_size=1.0,
)


# --- padding past expiry ------------------------------------------------------

def test_flat_bars_printed_after_expiry_are_not_stored():
    """Deribit pads an expired contract forward to today with zero-volume
    bars. A 2019 contract comes back with 2731 daily bars of which 191 are
    real; storing the other 2540 invents seven years of price history."""
    traded = bar_series(ms("2024-12-20"), 7, DAY_MS, volume=5.0)
    padding = bar_series(ms("2024-12-28"), 600, DAY_MS, volume=0.0)
    merged = {k: (traded[k] + padding[k] if isinstance(traded[k], list) else traded[k])
              for k in traded}

    client, _ = client_with(lambda url, params: merged)
    bars = client.fetch_bars(CONTRACT, "1d")

    assert len(bars) == 7
    assert bars["timestamp"].max() <= ms("2024-12-27")


def test_the_request_window_never_reaches_past_expiry():
    """Clamping the window is what makes the padding unreachable in the first
    place; dropping zero-volume bars is the second line of defence."""
    client, session = client_with(
        lambda url, params: bar_series(ms("2024-12-20"), 7, DAY_MS, 5.0)
    )
    client.fetch_bars(CONTRACT, "1d")

    ends = [int(params["end_timestamp"]) for _, params in session.calls]
    assert max(ends) <= ms("2024-12-27") + DAY_MS


def test_the_request_window_never_starts_before_listing():
    """Deribit refuses a start earlier than the instrument existed."""
    client, session = client_with(
        lambda url, params: bar_series(ms("2024-12-20"), 7, DAY_MS, 5.0)
    )
    client.fetch_bars(CONTRACT, "1d")

    starts = [int(params["start_timestamp"]) for _, params in session.calls]
    assert min(starts) >= ms("2024-12-20")


def test_untraded_bars_can_be_kept_when_asked_for():
    traded = bar_series(ms("2024-12-20"), 3, DAY_MS, 5.0)
    quiet = bar_series(ms("2024-12-23"), 2, DAY_MS, 0.0)
    merged = {k: (traded[k] + quiet[k] if isinstance(traded[k], list) else traded[k])
              for k in traded}

    client, _ = client_with(lambda url, params: merged)
    assert len(client.fetch_bars(CONTRACT, "1d")) == 3
    assert len(client.fetch_bars(CONTRACT, "1d", keep_untraded=True)) == 5


# --- the 5001-bar cap ---------------------------------------------------------

def test_a_long_intraday_span_is_paged_rather_than_truncated():
    """The endpoint caps at ~5001 ticks and anchors them to `end_timestamp`,
    so a single request for a year of hourly bars silently returns only the
    last 5001 hours."""
    long_contract = InstrumentInfo(
        name="BTC-27DEC24-60000-C",
        listed=date(2023, 12, 28), expiry=date(2024, 12, 27),
        tick_size=0.0001, contract_size=1.0,
    )

    def handler(url, params):
        start, end = int(params["start_timestamp"]), int(params["end_timestamp"])
        count = max(0, min(MAX_TICKS, (end - start) // HOUR_MS))
        return bar_series(start, count, HOUR_MS, volume=1.0)

    client, session = client_with(handler)
    bars = client.fetch_bars(long_contract, "1h")

    assert len(session.calls) > 1, "a year of hourly bars must be paged"
    assert len(bars) > MAX_TICKS
    assert bars["timestamp"].is_monotonic_increasing
    assert not bars["timestamp"].duplicated().any()


def test_an_unsupported_timeframe_is_refused_by_name():
    client, _ = client_with(lambda url, params: {})
    with pytest.raises(ValueError, match="Unsupported timeframe"):
        client.fetch_bars(CONTRACT, "4h")


def test_deribit_resolutions_are_minutes_not_our_timeframe_names():
    """`resolution='1h'` is not something Deribit accepts; `'60'` is."""
    client, session = client_with(
        lambda url, params: bar_series(ms("2024-12-20"), 2, HOUR_MS, 1.0)
    )
    client.fetch_bars(CONTRACT, "1h")
    assert session.calls[0][1]["resolution"] == "60"


# --- discovery ----------------------------------------------------------------

def test_an_instrument_that_never_existed_reads_as_absent_not_as_failure():
    """Grid discovery depends on telling "no such strike" from "request
    failed"; if they collapse together the sweep either stops or invents."""
    client, _ = client_with(
        lambda url, params: {"error": {"code": -32602, "message": "Invalid params"}}
    )
    assert client.get_instrument("BTC-27DEC24-60123-C") is None


def test_a_real_error_is_not_mistaken_for_an_absent_instrument():
    client, _ = client_with(
        lambda url, params: {"error": {"code": 10028, "message": "too many requests"}}
    )
    with pytest.raises(DeribitError) as caught:
        client.get_instrument("BTC-27DEC24-60000-C")
    assert caught.value.code == 10028
    assert not caught.value.is_unknown_instrument


def test_an_expired_instrument_still_reports_when_it_was_listed():
    """This is what makes deep history reachable: `get_instruments` retains
    expired listings for about a day, but `get_instrument` answers for one
    that expired years ago."""
    client, _ = client_with(lambda url, params: {
        "instrument_name": "BTC-27DEC19-8000-C",
        "creation_timestamp": str(ms("2019-03-27")),
        "expiration_timestamp": str(ms("2019-12-27")),
        "tick_size": "0.0005", "contract_size": "1.0",
    })
    info = client.get_instrument("BTC-27DEC19-8000-C")
    assert info is not None
    assert info.listed == date(2019, 3, 27)
    assert info.expiry == date(2019, 12, 27)
    assert info.tick_size == 0.0005


def test_a_json_rpc_error_is_never_retried():
    """Retrying a nonexistent instrument multiplies a discovery sweep's cost
    by `max_retries` for exactly the candidates that don't exist."""
    client, session = client_with(
        lambda url, params: {"error": {"code": -32602, "message": "Invalid params"}}
    )
    client.get_instrument("BTC-27DEC24-60123-C")
    assert len(session.calls) == 1
