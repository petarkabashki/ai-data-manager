"""Date-labelled bars: 00:00 UTC of the calendar date, one row per date.

Yahoo stamps a daily bar at the exchange's local midnight; the repo stamps
00:00 UTC of that date. The downloader restamps, the merge collapses two
stamps of one date, and fix_daily_stamps repairs files written before both."""

from __future__ import annotations

import importlib
import json
import sys

import pandas as pd
import pytest

from utils.config import REPO_ROOT

DAY = 86_400_000
D0 = int(pd.Timestamp("2026-05-04", tz="UTC").value // 1_000_000)     # a Monday
H = 3_600_000


def _bars(stamps, close):
    return pd.DataFrame({"timestamp": pd.Series(stamps, dtype="int64"), "open": close, "high": close,
                         "low": close, "close": close, "volume": 1.0})


def test_date_label_maps_each_venue_midnight_to_its_own_date():
    from utils.catalog import date_label_ms

    stamps = pd.Series([D0, D0 + 4 * H, D0 + 5 * H,            # 00:00, New York EDT, EST
                        D0 - 2 * H, D0 - H, D0 - 9 * H])        # Frankfurt CEST/CET, Tokyo: previous UTC day
    assert (date_label_ms(stamps) == D0).all()


def test_yahoo_daily_bars_are_restamped_to_the_exchange_local_date():
    from utils.yf import frame_from_yahoo

    def raw(tz):
        idx = pd.DatetimeIndex([pd.Timestamp("2026-05-04", tz=tz), pd.Timestamp("2026-05-05", tz=tz)], name="Date")
        return pd.DataFrame({"Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0, "Volume": 1}, index=idx)

    for tz in ("America/New_York", "Europe/Berlin", "Asia/Tokyo"):
        assert frame_from_yahoo(raw(tz), "1d")["timestamp"].tolist() == [D0, D0 + DAY], tz
    intraday = frame_from_yahoo(raw("America/New_York"), "1h")
    assert intraday["timestamp"].tolist() == [D0 + 4 * H, D0 + DAY + 4 * H]   # real instants stay


def test_daily_merge_keeps_one_row_per_date(tmp_path):
    from utils.catalog import merge_ohlcv_parquet

    dst = tmp_path / "X-1d.parquet"
    _bars([D0 - DAY, D0], 1.0).to_parquet(dst, index=False)                  # old import, 00:00 UTC
    before, after = merge_ohlcv_parquet(_bars([D0 + 4 * H, D0 + DAY + 4 * H], 2.0), dst, daily=True)
    out = pd.read_parquet(dst)
    assert (before, after) == (2, 3)
    assert out["timestamp"].tolist() == [D0 - DAY, D0, D0 + DAY]
    assert out["close"].tolist() == [1.0, 2.0, 2.0]                          # new data wins its date


@pytest.fixture
def tool(tmp_path, monkeypatch):
    root = tmp_path / "price-data"
    root.mkdir()
    monkeypatch.setenv("PRICE_DATA_ROOT", str(root))
    for name in [m for m in sys.modules if m.startswith("utils.") or m == "fix_daily_stamps"]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.syspath_prepend(str(REPO_ROOT / "tools"))
    return root, importlib.import_module("fix_daily_stamps")


def test_fix_restamps_dedupes_backs_up_and_is_idempotent(tool, tmp_path, capsys):
    root, fix = tool
    mixed = root / "stocks/yf/sp500/AAPL/AAPL-1d.parquet"
    mixed.parent.mkdir(parents=True)
    # Old import through 05-05 at 00:00; the downloader from 05-04 at New York midnight.
    _bars([D0 - DAY, D0, D0 + DAY, D0 + 4 * H, D0 + DAY + 4 * H, D0 + 2 * DAY + 4 * H],
          [1.0, 1.0, 1.0, 2.0, 2.0, 2.0]).to_parquet(mixed, index=False)
    europe = root / "indexes/yf/GDAXI/GDAXI-1d.parquet"
    europe.parent.mkdir(parents=True)
    _bars([D0 - 2 * H, D0 + DAY - 2 * H], 3.0).to_parquet(europe, index=False)
    clean = root / "commodities/yf/GC=F/GC=F-1d.parquet"
    clean.parent.mkdir(parents=True)
    _bars([D0, D0 + DAY], 4.0).to_parquet(clean, index=False)
    hourly = root / "stocks/yf/sp500/AAPL/AAPL-1h.parquet"
    _bars([D0 + 13 * H], 5.0).to_parquet(hourly, index=False)
    macro = root / "macro/yf/WTB3MS/WTB3MS-1d.parquet"                      # Date/Value layout
    macro.parent.mkdir(parents=True)
    pd.DataFrame({"Value": [5.0]}, index=pd.DatetimeIndex(["2026-05-04"], name="Date")).to_parquet(macro)
    backups = tmp_path / "backups"

    assert fix.main(["--provider", "yf", "--dry-run", "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert (doc["scanned"], doc["changed"], doc["rows_dropped"]) == (4, 2, 2)
    assert doc["skipped"] == ["price-data/macro/yf/WTB3MS/WTB3MS-1d.parquet"] and doc["failures"] == []
    assert len(pd.read_parquet(mixed)) == 6                                   # dry run wrote nothing

    assert fix.main(["--provider", "yf", "--backup-dir", str(backups), "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["changed"] == 2 and doc["failures"] == []
    out = pd.read_parquet(mixed)
    assert out["timestamp"].tolist() == [D0 - DAY, D0, D0 + DAY, D0 + 2 * DAY]
    assert out["close"].tolist() == [1.0, 2.0, 2.0, 2.0]                      # the downloader's rows win
    assert pd.read_parquet(europe)["timestamp"].tolist() == [D0, D0 + DAY]    # Frankfurt: next UTC day
    assert len(pd.read_parquet(backups / "stocks/yf/sp500/AAPL/AAPL-1d.parquet")) == 6
    assert not (backups / "commodities").exists()                             # untouched, not copied
    assert pd.read_parquet(hourly)["timestamp"].tolist() == [D0 + 13 * H]     # intraday out of scope

    assert fix.main(["--provider", "yf", "--backup-dir", str(backups), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["changed"] == 0


def test_fix_refuses_a_backup_dir_inside_the_data_root(tool):
    root, fix = tool
    with pytest.raises(SystemExit) as exc:
        fix.main(["--provider", "yf", "--backup-dir", str(root / "bak")])
    assert exc.value.code == 2


def test_merge_restamps_any_date_labelled_file_by_its_name(tmp_path):
    from utils.catalog import merge_ohlcv_parquet

    monthly = tmp_path / "BNB_USDT-1Mo.parquet"                              # bitget: 16:00 UTC = 00:00 UTC+8
    merge_ohlcv_parquet(_bars([D0 - 8 * H], 1.0), monthly)
    assert pd.read_parquet(monthly)["timestamp"].tolist() == [D0]
    hourly = tmp_path / "BNB_USDT-1h.parquet"
    merge_ohlcv_parquet(_bars([D0 - 8 * H], 1.0), hourly)
    assert pd.read_parquet(hourly)["timestamp"].tolist() == [D0 - 8 * H]     # real instants stay


def test_check_schema_flags_daily_stamps_off_midnight_or_repeated(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(REPO_ROOT / "tools"))
    from check_schema import check_file

    def write(name, stamps):
        path = tmp_path / name
        _bars(stamps, 1.0).to_parquet(path, index=False)
        return check_file(path)

    assert write("A-1d.parquet", [D0, D0 + DAY]) is None
    assert write("B-1d.parquet", [D0 + 8 * H, D0 + DAY + 8 * H]).startswith("2 bar(s) not at 00:00 UTC")
    assert "1 repeated date(s)" in write("C-1w.parquet", [D0, D0 + 4 * H])
    assert write("D-1h.parquet", [D0 + 8 * H]) is None                       # intraday out of scope


def test_fix_reaches_every_provider(tool):
    root, fix = tool
    path = root / "options/deribit/BTC/X-C/X-C-1d.parquet"
    path.parent.mkdir(parents=True)
    _bars([D0 + 8 * H, D0 + DAY + 8 * H], 1.0).to_parquet(path, index=False)
    assert fix.main(["--provider", "deribit", "--backup-dir", str(root.parent / "bak")]) == 0
    assert pd.read_parquet(path)["timestamp"].tolist() == [D0, D0 + DAY]
