from datetime import datetime, timedelta, timezone

from utils.ibkr import DownloadRange, clip_ranges_to_earliest

UTC = timezone.utc


def _r(a, b):
    return DownloadRange(datetime(*a, tzinfo=UTC), datetime(*b, tzinfo=UTC), "missing_file")


def test_no_earliest_leaves_ranges_alone():
    ranges = [_r((2000, 1, 1), (2026, 1, 1))]
    assert clip_ranges_to_earliest(ranges, None) is ranges


def test_start_is_moved_up_to_earliest_with_a_day_of_margin():
    earliest = datetime(2021, 6, 10, 13, 30, tzinfo=UTC)
    out = clip_ranges_to_earliest([_r((2000, 1, 1), (2026, 1, 1))], earliest)
    assert len(out) == 1
    assert out[0].start == earliest - timedelta(days=1)
    assert out[0].end == datetime(2026, 1, 1, tzinfo=UTC)
    assert out[0].reason == "missing_file"


def test_range_wholly_before_earliest_is_dropped():
    earliest = datetime(2021, 6, 10, tzinfo=UTC)
    prepend = _r((2000, 1, 1), (2014, 10, 9))
    tail = _r((2026, 10, 25), (2026, 11, 1))
    assert clip_ranges_to_earliest([prepend, tail], earliest) == [tail]


def test_range_after_earliest_is_untouched():
    earliest = datetime(2005, 1, 1, tzinfo=UTC)
    r = _r((2010, 1, 1), (2011, 1, 1))
    assert clip_ranges_to_earliest([r], earliest) == [r]
