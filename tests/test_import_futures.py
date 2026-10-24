"""Filename→contract mapping and vendor-quirk handling in the futures importer."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pandas as pd
import pytest

_spec = importlib.util.spec_from_file_location(
    "import_futures", Path(__file__).resolve().parent.parent / "tools" / "import_futures.py"
)
assert _spec and _spec.loader
import_futures = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(import_futures)

contract_from_match = import_futures.contract_from_match
PatternError = import_futures.PatternError
read_bars = import_futures.read_bars

CSI = re.compile(r"(?P<root>[A-Z]{2,3})_(?P<year>\d{4})(?P<month>\d{2})")
FIRSTRATE = re.compile(r"(?P<root>[A-Z]{2,3})_(?P<year>\d{4})(?P<code>[FGHJKMNQUVXZ])")
CANONICAL = re.compile(r"(?P<symbol>[A-Z]{2,3}[FGHJKMNQUVXZ]\d{2})")


def match(pattern: re.Pattern, name: str) -> re.Match:
    m = pattern.search(name)
    assert m, f"{pattern.pattern} did not match {name}"
    return m


# --- filename mapping ---------------------------------------------------------

def test_numeric_month_is_read_as_the_delivery_month():
    assert contract_from_match(match(CSI, "CL_201512.parquet"), decade=None) == ("CL", 2015, 12)


def test_month_code_is_read_as_the_delivery_month():
    assert contract_from_match(match(FIRSTRATE, "ES_2016H_1hour.csv"), decade=None) == ("ES", 2016, 3)


def test_an_already_canonical_symbol_is_taken_as_is():
    assert contract_from_match(match(CANONICAL, "ESU26.ohlcv-1h.csv"), decade=None) == ("ES", 2026, 9)


def test_december_crude_keys_on_delivery_not_expiry():
    """CLZ15 delivers in December and stops trading in November.

    Two vendors, one labelling by delivery month and one by expiry, must not
    produce two different directories for the same contract.
    """
    from utils.futures import contract_for, contract_from_symbol

    root, year, month = contract_from_match(match(CSI, "CL_201512.csv"), decade=None)
    assert contract_for(root, year, month).symbol == "CLZ15"
    assert contract_from_symbol("CLZ15").contract_month == "2015-12"


def test_an_ambiguous_short_year_is_refused_rather_than_guessed():
    """A wrong decade silently files a 2016 contract as 2026."""
    short = re.compile(r"(?P<root>[A-Z]{2,3})_(?P<year>\d{1})(?P<code>[FGHJKMNQUVXZ])")
    m = match(short, "ES_6H.csv")
    with pytest.raises(PatternError, match="decade"):
        contract_from_match(m, decade=None)
    assert contract_from_match(m, decade=2020) == ("ES", 2026, 3)


def test_a_pattern_that_captures_nothing_useful_is_rejected():
    bare = re.compile(r"(?P<root>[A-Z]{2,3})")
    with pytest.raises(PatternError):
        contract_from_match(match(bare, "ES.csv"), decade=None)


# --- vendor quirks ------------------------------------------------------------

def _write(tmp_path: Path, header: str, when: str) -> Path:
    path = tmp_path / "ES_2016H.csv"
    path.write_text(f"{header},Open,High,Low,Close,Volume\n{when},1.0,2.0,0.5,1.5,10\n")
    return path


def test_a_spaced_header_is_still_a_timestamp(tmp_path):
    """Vendors write "Date Time"; folding whitespace is what finds it."""
    bars = read_bars(_write(tmp_path, "Date Time", "2016-01-05 09:30:00"), None)
    assert len(bars) == 1
    assert list(bars.columns) == ["timestamp", "open", "high", "low", "close", "volume"]


def test_naive_vendor_timestamps_are_localised_not_assumed_utc(tmp_path):
    """An unlocalised exchange clock shifts every bar by a whole session."""
    path = _write(tmp_path, "Date Time", "2016-01-05 00:00:00")
    as_utc = read_bars(path, None)
    as_chicago = read_bars(path, "America/Chicago")
    assert pd.to_datetime(as_utc.timestamp.iloc[0], unit="ms").hour == 0
    # CST is UTC-6, so local midnight is 06:00 UTC.
    assert pd.to_datetime(as_chicago.timestamp.iloc[0], unit="ms").hour == 6


def test_a_file_with_no_time_column_names_what_it_found(tmp_path):
    path = tmp_path / "ES_2016H.csv"
    path.write_text("Open,High,Low,Close,Volume\n1.0,2.0,0.5,1.5,10\n")
    with pytest.raises(ValueError, match="no recognisable time column"):
        read_bars(path, None)
