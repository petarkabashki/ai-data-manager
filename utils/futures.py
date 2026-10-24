"""Futures contract identity, layout and catalog shape.

This module exists because a futures contract has to mean the same thing no
matter who sold us the data. IBKR names the September 2026 E-mini by its last
trade date (`lastTradeDateOrContractMonth=20260918`); CME DataMine and
Databento use `ESU6` / `ES.U26` / `ESU2026`. If the on-disk symbol were
whatever a provider happens to call it, the same contract would land in two
unrelated directories and nothing would join them.

So the repo picks its own canonical symbol — `ESU26` — and every provider
adapter maps its naming onto that. `expiry` is the authoritative field and the
symbol is a label: month letters repeat every decade (`U26` vs `U36`) and roots
collide across venues, so the real key is (root, exchange, expiry).

Layout follows the provider-first path rule unchanged, which is what lets a
second provider arrive without a migration:

    futures/<provider>/<symbol>/<symbol>-<timeframe>.parquet

    futures/ibkr/ESU26/ESU26-1h.parquet
    futures/databento/ESU26/ESU26-1h.parquet

The catalog nests by provider *within* each timeframe, mirroring the stock
catalogs rather than the flat crypto ones. See `AGENTS.md` — a flat node is
only safe when one provider can ever own a (symbol, timeframe), which was true
for crypto because the exchange is already in the catalog key. It is not true
here, and `indexes.yaml` is the cautionary tale: a catalog whose shape didn't
separate providers let an IBKR download inherit the entry's `provider: YF` and
register a mislabelled duplicate pointing at an IBKR file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from utils.config import DATA_ROOT

#: CME month codes. The standard set, used by every futures venue.
MONTH_CODES: dict[int, str] = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

CODE_TO_MONTH: dict[str, int] = {code: month for month, code in MONTH_CODES.items()}

#: Roots we know how to resolve, with the venue and contract multiplier.
#: `multiplier` is stored in the catalog so a model can size a position without
#: a second lookup; it is *not* used to scale prices, which stay as traded.
ROOTS: dict[str, dict[str, object]] = {
    "ES": {"exchange": "CME", "currency": "USD", "multiplier": 50, "name": "E-mini S&P 500"},
    "NQ": {"exchange": "CME", "currency": "USD", "multiplier": 20, "name": "E-mini Nasdaq-100"},
    "RTY": {"exchange": "CME", "currency": "USD", "multiplier": 50, "name": "E-mini Russell 2000"},
    "YM": {"exchange": "CBOT", "currency": "USD", "multiplier": 5, "name": "E-mini Dow"},
    "CL": {"exchange": "NYMEX", "currency": "USD", "multiplier": 1000, "name": "Crude Oil WTI"},
    "GC": {"exchange": "COMEX", "currency": "USD", "multiplier": 100, "name": "Gold"},
    "SI": {"exchange": "COMEX", "currency": "USD", "multiplier": 5000, "name": "Silver"},
    "ZN": {"exchange": "CBOT", "currency": "USD", "multiplier": 1000, "name": "10-Year T-Note"},
}

_SYMBOL_RE = re.compile(r"^(?P<root>[A-Z0-9]{1,4}?)(?P<code>[FGHJKMNQUVXZ])(?P<yy>\d{2})$")


class UnknownRootError(KeyError):
    """Raised for a root that isn't in `ROOTS`."""


@dataclass(frozen=True, slots=True)
class FuturesContract:
    """One deliverable contract, identified independently of any provider.

    The identity is (root, exchange, year, month) where year/month are the
    *delivery* month — the one the symbol's letter code names. `expiry` (the
    last trade date) is extra information, not the key, and is often unknown
    for an imported archive that only labels files by contract month.
    """

    root: str
    exchange: str
    year: int
    month: int
    expiry: date | None = None
    currency: str = "USD"
    multiplier: int | None = None

    def __post_init__(self) -> None:
        if self.month not in MONTH_CODES:
            raise ValueError(f"month must be 1-12, got {self.month}")

    @property
    def symbol(self) -> str:
        return contract_symbol(self.root, self.year, self.month)

    @property
    def contract_month(self) -> str:
        """The delivery month as `YYYY-MM` — what the symbol's code names."""
        return f"{self.year:04d}-{self.month:02d}"

    def catalog_entry(self) -> dict:
        """The non-timeframe half of this contract's catalog entry."""
        return {
            "name": self.symbol,
            "root": self.root,
            "exchange": self.exchange,
            "currency": self.currency,
            "multiplier": self.multiplier,
            "contract_month": self.contract_month,
            "expiry": self.expiry.isoformat() if self.expiry else None,
            "categories": ["Futures"],
        }


def contract_symbol(root: str, year: int, month: int) -> str:
    """Canonical symbol: root + delivery-month code + 2-digit year.

    `month` is the delivery month, which is what every venue's symbol names.
    It is deliberately *not* the expiry month: crude oil's December contract
    is `CLZ`, and it stops trading in November. Deriving the code from the
    expiry date would file the same contract under two different symbols
    depending on which provider supplied it, which is the one thing this
    module exists to prevent.
    """
    if month not in MONTH_CODES:
        raise ValueError(f"month must be 1-12, got {month}")
    return f"{root.upper()}{MONTH_CODES[month]}{year % 100:02d}"


def parse_contract_symbol(symbol: str, *, pivot_year: int | None = None) -> tuple[str, int, int]:
    """Inverse of `contract_symbol`: returns (root, year, delivery_month).

    The two-digit year is resolved against a sliding window centred on
    `pivot_year` (default: the current year), so `U26` reads as 2026 rather
    than 1926 and still works when the archive outlives this decade.
    """
    match = _SYMBOL_RE.match(symbol.upper())
    if not match:
        raise ValueError(f"Not a canonical futures symbol: {symbol!r}")

    if pivot_year is None:
        pivot_year = date.today().year

    root = match.group("root")
    month = CODE_TO_MONTH[match.group("code")]
    yy = int(match.group("yy"))

    century = pivot_year - (pivot_year % 100)
    year = century + yy
    # Keep the resolved year within (pivot - 50, pivot + 50].
    if year - pivot_year > 50:
        year -= 100
    elif pivot_year - year > 50:
        year += 100
    return root, year, month


def contract_for(
    root: str,
    year: int,
    month: int,
    expiry: date | None = None,
) -> FuturesContract:
    """Build a contract from a known root, filling venue metadata from `ROOTS`."""
    key = root.upper()
    try:
        meta = ROOTS[key]
    except KeyError as exc:
        raise UnknownRootError(
            f"Unknown futures root {root!r}; add it to utils.futures.ROOTS"
        ) from exc
    return FuturesContract(
        root=key,
        exchange=str(meta["exchange"]),
        year=year,
        month=month,
        expiry=expiry,
        currency=str(meta["currency"]),
        multiplier=int(meta["multiplier"]),  # type: ignore[arg-type]
    )


def contract_from_symbol(symbol: str, expiry: date | None = None) -> FuturesContract:
    """Build a contract from a canonical symbol like `ESU26`."""
    root, year, month = parse_contract_symbol(symbol)
    return contract_for(root, year, month, expiry=expiry)


def futures_file_path(provider: str, symbol: str, timeframe: str) -> Path:
    """Canonical path for a futures parquet file.

    Provider-first, exactly like every other asset class, so a second provider
    is a sibling directory rather than a migration.
    """
    return DATA_ROOT / "futures" / provider.lower() / symbol / f"{symbol}-{timeframe}.parquet"


def futures_catalog_path() -> Path:
    """The futures catalog. One file for all roots; `root` groups within it."""
    return DATA_ROOT / "futures.yaml"


# --- catalog ------------------------------------------------------------------


def load_futures_catalog() -> dict:
    """The futures catalog, or an empty one if nothing has been written yet."""
    path = futures_catalog_path()
    if not path.exists():
        return {}
    import yaml

    return yaml.safe_load(path.read_text()) or {}


def save_futures_catalog(data: dict) -> None:
    from utils.catalog import _dump_yaml

    _dump_yaml(futures_catalog_path(), data)


def update_futures_entry(
    catalog: dict,
    contract: FuturesContract,
    timeframe: str,
    provider: str,
    file_path: Path,
    start_date: str,
    end_date: str,
    semantics: "Semantics | None" = None,
) -> None:
    """Register one (contract, timeframe, provider) series in the catalog.

    Nests by provider *within* the timeframe, like the stock catalogs, so an
    imported vendor archive and the IBKR feed for the same contract coexist
    instead of one overwriting the other. That is the whole reason a deep
    history and a live feed can share this tree.
    """
    from utils.catalog import (
        _register_timeframe,
        _set_provider_node,
        provider_node_key,
        recompute_entry_bounds,
        relative_to_repo,
    )
    from utils.semantics import apply_semantics, semantics_for

    entry = catalog.setdefault(contract.symbol, contract.catalog_entry())
    # A contract seen first from a month-only vendor gets its expiry filled in
    # later by whichever provider actually knows the last trade date.
    if contract.expiry and not entry.get("expiry"):
        entry["expiry"] = contract.expiry.isoformat()
    entry.setdefault("timeframes", [])
    entry.setdefault("timeframe_dates", {})
    entry.setdefault("hide_from_frontend", False)
    entry.setdefault("description", f"Historical data for {contract.symbol}")

    _register_timeframe(entry, timeframe)

    node = {
        "start_date": start_date,
        "end_date": end_date,
        "file_path": relative_to_repo(file_path),
        "provider": provider,
    }
    apply_semantics(node, semantics or semantics_for("Futures", provider))

    _set_provider_node(entry, timeframe, provider_node_key(provider), node)
    recompute_entry_bounds(entry)
