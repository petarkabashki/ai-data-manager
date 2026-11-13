"""`data_flags`: what is known to be wrong in a stored file, on its catalog node.

A finding the owner has ruled on, but whose bars stay as they are (for now),
is written on every catalog node that points at the file:

    1d:
      YF:
        file_path: price-data/stocks/yf/russell2000/VTOL/VTOL-1d.parquet
        adjustment: raw
        data_flags:
        - kind: break              # the series steps on `date`; bars before it are on another scale
          date: '2020-06-12'
          reason: Yahoo serves an unadjusted 1.55 jump; its split calendar says 0.5
          ref: Product msg_d3d90042
          recorded: '2026-11-13'
      IBKR:
        file_path: price-data/stocks/ibkr/sp500/APO/APO-all-1d.parquet
        data_flags:
        - kind: bad                # bars from..to are wrong; repair pending
          from: '2020-01-09'
          to: '2021-12-31'
          ...

Kinds:

- `bad`: the bars `from`..`to` are known wrong and wait on a repair.
- `uncertain`: the price scale of `from`..`to` is not established (SSP before
  2008-07-15: YF and IBKR differ by 20-30x and neither is confirmed).
- `break`: the series steps on `date` and nothing in the file adjusts for it.
- `deleted`: the bars `from`..`to` were removed by `delete_stock_bars.py`
  (run `run_id`) and stay a gap: `deleted_bars.yaml` keeps every writer from
  putting them back.

A reader who needs clean history drops `bad`, `uncertain` and `deleted`
ranges and treats a `break` as a seam. `check_adjustment.py` does exactly that:
flagged ranges are left out of every comparison, and a split jump on a `break`
date reads `known_split_jump`. Dates are inclusive and in UTC.
"""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

FIELD = "data_flags"
RANGE_KINDS = ("bad", "uncertain", "deleted")
DATE_KINDS = ("break",)
KINDS = RANGE_KINDS + DATE_KINDS


def _key(flag: dict) -> tuple:
    return (flag["kind"], str(flag.get("from")), str(flag.get("to")), str(flag.get("date")))


def make_flag(kind: str, *, start: str | None = None, end: str | None = None, date: str | None = None,
              reason: str, ref: str | None, recorded: str, **extra) -> dict:
    """A validated flag. Ranges need `start` <= `end`; a break needs `date`."""
    if kind in RANGE_KINDS:
        if not (start and end) or date:
            raise ValueError(f"a {kind} flag takes from and to, not a date")
        if pd.Timestamp(start) > pd.Timestamp(end):
            raise ValueError(f"from {start} is after to {end}")
        flag = {"kind": kind, "from": str(start), "to": str(end)}
    elif kind in DATE_KINDS:
        if not date or start or end:
            raise ValueError(f"a {kind} flag takes a date, not from/to")
        flag = {"kind": kind, "date": str(date)}
    else:
        raise ValueError(f"unknown flag kind {kind!r}; one of {', '.join(KINDS)}")
    flag["reason"] = reason
    if ref:
        flag["ref"] = ref
    flag["recorded"] = recorded
    flag.update({k: v for k, v in extra.items() if v is not None})
    return flag


def set_flag(node: dict, flag: dict) -> bool:
    """Add `flag` to the node, replacing one of the same kind and dates. True if the node changed."""
    flags = [f for f in node.get(FIELD) or [] if _key(f) != _key(flag)]
    before = node.get(FIELD) or []
    flags.append(flag)
    flags.sort(key=lambda f: (str(f.get("from") or f.get("date")), f["kind"]))
    if flags == before:
        return False
    node[FIELD] = flags
    return True


def clear_flag(node: dict, flag: dict) -> bool:
    """Remove the flag of the same kind and dates. True if the node changed."""
    before = node.get(FIELD) or []
    flags = [f for f in before if _key(f) != _key(flag)]
    if len(flags) == len(before):
        return False
    if flags:
        node[FIELD] = flags
    else:
        node.pop(FIELD, None)
    return True


def masked_ranges(flags: Iterable[dict] | None) -> list[tuple[str, str]]:
    """(from, to) of every bad, uncertain or deleted range."""
    return [(str(f["from"]), str(f["to"])) for f in flags or () if f.get("kind") in RANGE_KINDS]


def break_dates(flags: Iterable[dict] | None) -> set[str]:
    return {str(f["date"]) for f in flags or () if f.get("kind") in DATE_KINDS}


def drop_masked(series: pd.Series, ranges: Iterable[tuple[str, str]]) -> pd.Series:
    """`series` (indexed by date) without the days inside `ranges`."""
    keep = pd.Series(True, index=series.index)
    for start, end in ranges:
        keep &= ~((series.index >= pd.Timestamp(start)) & (series.index <= pd.Timestamp(end)))
    return series[keep.to_numpy()]
