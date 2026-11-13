"""delete_twin_bars: copied spans come out whole, backed up and logged row by row."""

from __future__ import annotations

import pandas as pd
import pytest

from tools import delete_twin_bars as dtb

HOUR = 3_600_000


def bars(n: int, seed: float) -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp": pd.Series([i * HOUR for i in range(n)], dtype="int64"),
        "open": seed, "high": seed + 1, "low": seed - 1,
        "close": [seed + i * 0.01 for i in range(n)],
        "volume": [seed * 10 + i for i in range(n)],
    })


def write(root, subdir, venue, symbol, tf, frame):
    path = root / subdir / venue / symbol / f"{symbol}-{tf}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


@pytest.fixture
def root(tmp_path, monkeypatch):
    data = tmp_path / "price-data"
    data.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(data))
    monkeypatch.setattr(dtb, "DATA_ROOT", data)
    monkeypatch.setattr(dtb, "refresh_dates_for_files", lambda paths: [])
    return data


@pytest.fixture
def copied(root):
    """bybit perps BTC 1h: rows 100-159 copied from binance perps, row 130 revised by binance."""
    binance = bars(300, 1.0)
    bybit = bars(300, 2.0)
    bybit.loc[100:159, ["close", "volume"]] = binance.loc[100:159, ["close", "volume"]].to_numpy()
    binance.loc[130, "close"] += 0.5  # the source revised one bar after the copy
    write(root, "crypto-futures", "binance", "BTC_USDT", "1h", binance)
    path = write(root, "crypto-futures", "bybit", "BTC_USDT", "1h", bybit)
    # 10 equal bars elsewhere: below --min-bars, so kept.
    other = bars(300, 3.0)
    other.loc[0:9, ["close", "volume"]] = binance.loc[0:9, ["close", "volume"]].to_numpy()
    write(root, "crypto-futures", "bybit", "ETH_USDT", "1h", other)
    write(root, "crypto-futures", "binance", "ETH_USDT", "1h", binance)
    return path, bybit


def test_the_dry_run_finds_the_span_with_exact_bounds_and_writes_nothing(root, copied, capsys):
    path, before = copied
    assert dtb.main(["--exchange", "bybit", "--trading-mode", "futures"]) == 0
    out = capsys.readouterr().out
    assert "1970-01-05 04:00" in out and "1970-01-07 15:00" in out  # rows 100 and 159
    assert "Total: 1 spans, 60 rows" in out
    pd.testing.assert_frame_equal(pd.read_parquet(path), before)
    assert not (root / "revisions").exists()


def test_a_revised_bar_inside_the_span_goes_with_it(root, copied):
    (span,) = dtb.plan(root, "bybit-perps", "binance-perps", ["1h"])
    assert (span.rows, span.matched) == (60, 59)


def test_apply_backs_up_deletes_and_records_each_row_with_its_hash(root, copied, tmp_path, capsys):
    path, before = copied
    backup = tmp_path / "backup"
    assert dtb.main(["--exchange", "bybit", "--trading-mode", "futures", "--apply",
                     "--backup-dir", str(backup)]) == 0
    after = pd.read_parquet(path)
    assert len(after) == 240
    assert not after["timestamp"].between(100 * HOUR, 159 * HOUR).any()
    pd.testing.assert_frame_equal(pd.read_parquet(backup / path.relative_to(root)), before)
    (record_file,) = (root / "revisions").glob("*.parquet")
    record = pd.read_parquet(record_file)
    assert len(record) == 60 and set(record["action"]) == {"deleted"}
    first = record.iloc[0]
    assert first["timestamp"] == 100 * HOUR and first["close"] == before.loc[100, "close"]
    assert first["row_hash"] == dtb.row_hash(before.loc[100])
    assert (root / "revisions" / f"{record_file.stem}.yaml").exists()


def test_log_refill_records_new_values_and_bars_the_venue_does_not_serve(root, copied, tmp_path):
    path, before = copied
    dtb.main(["--exchange", "bybit", "--trading-mode", "futures", "--apply",
              "--backup-dir", str(tmp_path / "b")])
    run_id = next((root / "revisions").glob("*.parquet")).stem
    refill = bars(300, 9.0).loc[100:149]  # the venue serves 50 of the 60 bars
    merged = pd.concat([pd.read_parquet(path), refill]).sort_values("timestamp").reset_index(drop=True)
    merged.to_parquet(path, index=False)

    result = dtb.log_refill(root, run_id)
    assert (result["deleted"], result["refetched"], result["not_served"]) == (60, 50, 10)
    record = pd.read_parquet(root / "revisions" / f"{run_id}.parquet")
    new = record[(record["action"] == "refetched") & (record["timestamp"] == 100 * HOUR)].iloc[0]
    assert new["close"] == refill.loc[100, "close"]
    assert new["row_hash"] == dtb.row_hash(refill.loc[100])
    with pytest.raises(ValueError):
        dtb.log_refill(root, run_id)


def test_the_backup_dir_must_be_outside_the_data_root(root, copied):
    with pytest.raises(SystemExit):
        dtb.main(["--exchange", "bybit", "--trading-mode", "futures", "--apply",
                  "--backup-dir", str(root / "bk")])


def test_the_venues_own_bars_on_another_anchor_stay(root):
    """kucoin weekly bars open on Thursday, binance's on Monday: only Mondays were copied."""
    week, day = 7 * 86_400_000, 86_400_000
    monday0 = 4 * day  # 1970-01-05 was a Monday
    binance = bars(60, 1.0)
    binance["timestamp"] = [monday0 + i * week for i in range(60)]
    own = bars(60, 2.0)
    own["timestamp"] = [i * week for i in range(60)]  # 1970-01-01 was a Thursday
    copied = binance.iloc[10:40]
    kucoin = pd.concat([own, copied]).sort_values("timestamp").reset_index(drop=True)
    write(root, "crypto-futures", "binance", "FET_USDT", "1w", binance)
    path = write(root, "cryptos", "kucoin", "FET_USDT", "1w", kucoin)

    (span,) = dtb.plan(root, "kucoin-spot", "binance-perps", ["1w"])
    assert (span.rows, span.matched) == (30, 30)
    dtb.apply(root, [span], "run", root.parent / "bk", {})
    after = pd.read_parquet(path)
    assert len(after) == 60
    assert after["timestamp"].isin(own["timestamp"]).all()


def test_the_hash_changes_with_any_value():
    row = bars(1, 1.0).iloc[0]
    other = row.copy()
    other["volume"] += 1e-9
    assert dtb.row_hash(row) != dtb.row_hash(other)


def test_off_anchor_bars_go_with_a_backup_and_record(root):
    """Two 3d bars a day off bitget's grid make the fill reject bitget's own bars."""
    day = 86_400_000
    own = bars(40, 1.0)
    own["timestamp"] = [i * 3 * day for i in range(40)]
    strays = bars(2, 5.0)
    strays["timestamp"] = [10 * 3 * day + day, 30 * 3 * day + day]
    frame = pd.concat([own, strays]).sort_values("timestamp").reset_index(drop=True)
    path = write(root, "cryptos", "bitget", "AAVE_USDT", "3d", frame)

    spans = dtb.plan_off_anchor(root, "bitget-spot", ["3d"])
    assert [s.first for s in spans] == strays["timestamp"].tolist()
    summary = dtb.apply(root, spans, "run", root.parent / "bk", {})
    assert summary["rows_deleted"] == 2
    after = pd.read_parquet(path)
    assert after["timestamp"].tolist() == own["timestamp"].tolist()
    assert len(pd.read_parquet(root.parent / "bk" / path.relative_to(root))) == 42
    record = pd.read_parquet(root / "revisions" / "run.parquet")
    assert record["timestamp"].tolist() == strays["timestamp"].tolist()


def test_no_anchor_majority_deletes_nothing():
    ts = pd.Series([0, 86_400_000, 3 * 86_400_000, 4 * 86_400_000])
    assert not dtb.off_anchor_rows(ts, 3 * 86_400_000).any()
