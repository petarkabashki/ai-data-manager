"""In-memory index of every catalogued series, for answering requests.

Built from `catalog_registry()` — the same source `tools/query.py` reads — so
the server and the CLI can never disagree about what exists. Loading every
catalog takes a second or two, so the index is built once and rebuilt after a
job that may have written to a catalog, not per request.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from pathlib import Path

from utils.catalog import catalog_registry, iter_timeframe_nodes, resolve_from_repo
from utils.planning import days_stale

_WORD = re.compile(r"[a-z0-9]+")


def name_words(text: str | None) -> tuple[str, ...]:
    """Lower-cased alphanumeric words: "Apple Inc." → ("apple", "inc")."""
    return tuple(_WORD.findall(text.lower())) if text else ()


def name_matches(query: tuple[str, ...], name: tuple[str, ...]) -> bool:
    """`query` names the asset if its words are the leading whole words of the
    common name: "apple" and "apple inc" match "Apple Inc.", "app" and "inc" don't."""
    return bool(query) and name[:len(query)] == query


@dataclass(slots=True)
class SeriesIndex:
    rows: list[dict] = field(default_factory=list)
    catalogs: list[dict] = field(default_factory=list)
    _words: dict[str, tuple[str, ...]] = field(default_factory=dict)   # common_name → words
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def reload(self) -> int:
        rows: list[dict] = []
        catalogs: list[dict] = []
        for spec in catalog_registry():
            data = spec.load()
            n_series = 0
            for key, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                name = entry.get("name", key)
                for timeframe, node_key, node in iter_timeframe_nodes(entry):
                    provider = (
                        node.get("provider")
                        or (node_key.split(":")[0] if node_key else None)
                        or entry.get("provider")
                    )
                    rows.append(
                        {
                            "catalog": spec.name,
                            "asset_class": spec.asset_class,
                            "symbol": name,
                            "common_name": entry.get("common_name"),
                            "timeframe": timeframe,
                            "provider": provider,
                            "session": node.get("session"),
                            "start_date": node.get("start_date"),
                            "end_date": node.get("end_date"),
                            "adjustment": node.get("adjustment"),
                            "volume_basis": node.get("volume_basis"),
                            "file_path": node.get("file_path"),
                        }
                    )
                    n_series += 1
            catalogs.append(
                {
                    "catalog": spec.name,
                    "asset_class": spec.asset_class,
                    "symbols": sum(1 for v in data.values() if isinstance(v, dict)),
                    "series": n_series,
                }
            )
        words = {r["common_name"]: name_words(r["common_name"]) for r in rows if r["common_name"]}
        with self._lock:
            self.rows, self.catalogs, self._words = rows, catalogs, words
        return len(rows)

    def query(
        self,
        *,
        catalog: str | list[str] | None = None,
        asset_class: str | list[str] | None = None,
        symbol: str | list[str] | None = None,
        name: str | None = None,
        timeframe: str | list[str] | None = None,
        provider: str | None = None,
        session: str | None = None,
        stale_days: int | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        def as_set(v):
            if v is None:
                return None
            return {v} if isinstance(v, str) else set(v)

        catalogs, classes = as_set(catalog), as_set(asset_class)
        symbols, timeframes = as_set(symbol), as_set(timeframe)
        if name is not None and not isinstance(name, str):
            raise ValueError("name must be a string")
        wanted = name_words(name) if name is not None else None
        if wanted == ():
            raise ValueError("name needs at least one letter or digit")
        out = []
        with self._lock:
            rows, words = self.rows, self._words
        for row in rows:
            if catalogs and row["catalog"] not in catalogs:
                continue
            if classes and row["asset_class"] not in classes:
                continue
            if symbols and row["symbol"] not in symbols:
                continue
            if timeframes and row["timeframe"] not in timeframes:
                continue
            if wanted and not name_matches(wanted, words.get(row["common_name"], ())):
                continue
            if provider and (row["provider"] or "").lower() != provider.lower():
                continue
            if session and (row["session"] or "all") != session:
                continue
            stale = days_stale(row["end_date"])
            if stale_days is not None and (stale is None or stale < stale_days):
                continue
            out.append({**row, "stale_days": stale})
            if limit and len(out) >= limit:
                break
        return out

    def resolve(self, **filters) -> tuple[dict, Path]:
        """Exactly one series for a bars request, or a ValueError that says why.

        Ambiguity is an error rather than a silent first-match: `AAPL 1d` is in
        both `sp500` and `nasdaq100`, and the two YF files are *not* the same
        data (one adjusted, one raw), so guessing would hand a client the wrong
        numbers without telling it.
        """
        matches = [r for r in self.query(**filters) if r.get("file_path")]
        if not matches:
            raise ValueError(f"no series matches {filters}")
        if len(matches) > 1:
            options = [
                {k: m[k] for k in ("catalog", "provider", "session", "adjustment")}
                for m in matches[:10]
            ]
            raise ValueError(
                f"{len(matches)} series match {filters}; narrow with catalog/provider/session: {options}"
            )
        row = matches[0]
        return row, resolve_from_repo(row["file_path"])
