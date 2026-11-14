"""check_twins: blocks of bars identical across venues are a provenance fault, not agreement."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from tools import check_twins

HOUR = 3_600_000


def bars(n: int, seed: float, start_ms: int = 0, step_ms: int = HOUR) -> pd.DataFrame:
    """n distinct traded bars whose values depend on `seed`."""
    return pd.DataFrame({
        "timestamp": pd.Series([start_ms + i * step_ms for i in range(n)], dtype="int64"),
        "open": 1.0, "high": 2.0, "low": 0.5,
        "close": [seed + i * 0.01 for i in range(n)],
        "volume": [seed * 10 + i for i in range(n)],
    })


def write(root: Path, subdir: str, venue: str, symbol: str, tf: str, frame: pd.DataFrame) -> Path:
    path = root / subdir / venue / symbol / f"{symbol}-{tf}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("PRICE_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(check_twins, "DATA_ROOT", tmp_path)
    return tmp_path


def run(monkeypatch, capsys, *argv) -> tuple[int, str]:
    monkeypatch.setattr(sys, "argv", ["check_twins.py", *argv])
    code = check_twins.main()
    return code, capsys.readouterr().out


def test_distinct_venues_are_not_twins(root):
    write(root, "crypto-futures", "binance", "BTC_USDT", "1h", bars(100, 1.0))
    write(root, "crypto-futures", "kucoin", "BTC_USDT", "1h", bars(100, 2.0))
    blocks, files = check_twins.scan(root, ["1h"])
    assert files == 2
    assert blocks == []


def test_a_block_copied_from_another_venue_is_found_with_its_extent(root):
    binance = bars(200, 1.0)
    kucoin = bars(200, 2.0)
    kucoin.loc[50:99, ["close", "volume"]] = binance.loc[50:99, ["close", "volume"]].to_numpy()
    write(root, "crypto-futures", "binance", "BTC_USDT", "1h", binance)
    write(root, "crypto-futures", "kucoin", "BTC_USDT", "1h", kucoin)
    # Spot files of the same symbol are compared too, across classes.
    spot = bars(200, 3.0)
    spot.loc[0:29, ["close", "volume"]] = binance.loc[0:29, ["close", "volume"]].to_numpy()
    write(root, "cryptos", "bitget", "BTC_USDT", "1h", spot)

    blocks, _ = check_twins.scan(root, ["1h"])
    by_pair = {(b.a, b.b): b for b in blocks}
    assert set(by_pair) == {("binance-perps", "kucoin-perps"), ("binance-perps", "bitget-spot")}
    kb = by_pair[("binance-perps", "kucoin-perps")]
    assert (kb.bars, kb.first, kb.last) == (50, "1970-01-03", "1970-01-05")
    assert by_pair[("binance-perps", "bitget-spot")].bars == 30


def test_scattered_or_untraded_equal_bars_do_not_count(root):
    a = bars(100, 1.0)
    b = bars(100, 2.0)
    b.loc[0:45:2, ["close", "volume"]] = a.loc[0:45:2, ["close", "volume"]].to_numpy()  # 23, none adjacent
    # 40 more bars equal but with zero volume on both sides: a flat, untraded stretch.
    a.loc[50:89, ["close", "volume"]] = [5.0, 0.0]
    b.loc[50:89, ["close", "volume"]] = [5.0, 0.0]
    write(root, "cryptos", "binance", "X_USDT", "1h", a)
    write(root, "cryptos", "bybit", "X_USDT", "1h", b)
    assert check_twins.scan(root, ["1h"])[0] == []
    assert check_twins.scan(root, ["1h"], min_bars=23)[0][0].bars == 23
    assert check_twins.scan(root, ["1h"], min_run=1)[0][0].run == 1


def test_known_blocks_are_reported_apart_and_do_not_fail_until_they_grow(root, monkeypatch, capsys):
    binance = bars(200, 1.0)
    kucoin = bars(200, 2.0)
    kucoin.loc[0:49, ["close", "volume"]] = binance.loc[0:49, ["close", "volume"]].to_numpy()
    write(root, "crypto-futures", "binance", "ETH_USDT", "4h", binance)
    path = write(root, "crypto-futures", "kucoin", "ETH_USDT", "4h", kucoin)

    code, out = run(monkeypatch, capsys, "--json")
    assert code == 1
    assert [b["bars"] for b in json.loads(out)["new"]] == [50]

    code, out = run(monkeypatch, capsys, "--record")
    assert code == 0
    assert "Recorded 1 block" in out
    assert check_twins.load_known(root / check_twins.RECORD_NAME) == {
        ("ETH_USDT", "4h", "binance-perps", "kucoin-perps"): 50
    }

    code, out = run(monkeypatch, capsys)
    assert code == 0
    assert "Known twin blocks (1, 50 bars" in out and "NEW" not in out

    kucoin.loc[0:79, ["close", "volume"]] = binance.loc[0:79, ["close", "volume"]].to_numpy()
    kucoin.to_parquet(path, index=False)
    code, out = run(monkeypatch, capsys, "--json")
    assert code == 1
    report = json.loads(out)
    assert [b["bars"] for b in report["new"]] == [80] and report["known"] == []


def test_a_clean_root_exits_zero_and_says_so(root, monkeypatch, capsys):
    write(root, "cryptos", "binance", "BTC_USDT", "1h", bars(50, 1.0))
    write(root, "cryptos", "kucoin", "BTC_USDT", "1h", bars(50, 2.0))
    code, out = run(monkeypatch, capsys)
    assert code == 0
    assert "No twin blocks." in out


def test_record_refuses_a_partial_scan(root, monkeypatch, capsys):
    with pytest.raises(SystemExit):
        run(monkeypatch, capsys, "--record", "--symbol", "BTC_USDT")


def test_three_copied_bars_in_a_row_are_a_block_at_any_timeframe(root):
    week = 7 * 86_400_000
    binance = bars(30, 1.0, step_ms=week)
    kucoin = bars(30, 2.0, step_ms=week)
    kucoin.loc[10:12, ["close", "volume"]] = binance.loc[10:12, ["close", "volume"]].to_numpy()
    write(root, "crypto-futures", "binance", "AAVE_USDT", "1w", binance)
    write(root, "cryptos", "kucoin", "AAVE_USDT", "1w", kucoin)
    write(root, "crypto-futures", "binance", "AAVE_USDT", "1h", binance.assign(timestamp=binance.timestamp // 168))
    write(root, "cryptos", "kucoin", "AAVE_USDT", "1h", kucoin.assign(timestamp=kucoin.timestamp // 168))
    blocks, _ = check_twins.scan(root, ["1h", "1w"])
    assert sorted((b.timeframe, b.bars, b.run) for b in blocks) == [("1h", 3, 3), ("1w", 3, 3)]
    kucoin.loc[11, "volume"] += 1  # bars 10 and 12 still equal, but not in a row
    write(root, "cryptos", "kucoin", "AAVE_USDT", "1w", kucoin)
    assert check_twins.scan(root, ["1w"])[0] == []
