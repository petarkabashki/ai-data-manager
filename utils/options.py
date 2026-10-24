"""Option contract identity, layout and catalog shape.

The futures module exists because one contract must mean the same thing no
matter who sold us the data. Options have the same problem twice over: the
identity is four things — underlying, expiry, strike, right — and every venue
spells it differently. Deribit writes `BTC-27DEC24-60000-C`, OKX writes
`BTC-USD-241227-60000-C`, Gate writes `BTC_USDT-20241227-60000-C`. Sorting a
directory of Deribit names puts August before December; a wrong parse files a
put under a call.

So the repo picks its own canonical symbol, with an ISO date that sorts:

    BTC-20241227-60000-C
    <underlying>-<YYYYMMDD expiry>-<strike>-<C|P>

and every provider adapter maps its naming onto that. Unlike futures, expiry
here *is* part of the key: an option's expiry is fixed at listing and is the
only thing separating two otherwise identical strikes.

Layout follows the provider-first path rule unchanged, with the underlying in
the universe slot the way `sp500` is for stocks:

    options/<provider>/<underlying>/<symbol>/<symbol>-<timeframe>.parquet

    options/deribit/BTC/BTC-20241227-60000-C/BTC-20241227-60000-C-1d.parquet

The catalog nests by provider *within* each timeframe, like the stock and
futures catalogs rather than the flat crypto ones — see `AGENTS.md`. A flat
node is only safe when one provider can ever own a (symbol, timeframe), and
that is not true here: the same BTC option trades on Deribit and OKX.

Two things about option data that the OHLCV schema cannot express, and which
are therefore recorded on the catalog *entry*:

  * **Prices are not in USD.** A Deribit BTC option quotes in BTC — a close of
    0.1575 means 0.1575 BTC, not $0.1575. `quote_currency` says which.
  * **A contract is not one unit of underlying.** `contract_size` scales a
    position; it is stored, never applied to the stored prices, which stay
    exactly as traded.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from utils.config import DATA_ROOT

if TYPE_CHECKING:
    from utils.semantics import Semantics

#: Underlyings we know how to resolve, with venue conventions.
#: `quote_currency` is what the option premium is denominated in, which for a
#: coin-margined venue is the coin itself rather than a fiat currency.
UNDERLYINGS: dict[str, dict[str, object]] = {
    "BTC": {"exchange": "Deribit", "quote_currency": "BTC", "contract_size": 1.0,
            "name": "Bitcoin"},
    "ETH": {"exchange": "Deribit", "quote_currency": "ETH", "contract_size": 1.0,
            "name": "Ether"},
}

RIGHTS: dict[str, str] = {"C": "call", "P": "put"}

#: Deribit spells expiries `27DEC24`, and days are *not* zero-padded (`2OCT26`).
_DERIBIT_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                   "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

_SYMBOL_RE = re.compile(
    r"^(?P<underlying>[A-Z0-9]{2,6})"
    r"-(?P<expiry>\d{8})"
    r"-(?P<strike>\d+(?:\.\d+)?)"
    r"-(?P<right>[CP])$"
)

_DERIBIT_RE = re.compile(
    r"^(?P<underlying>[A-Z0-9]{2,6})"
    r"-(?P<day>\d{1,2})(?P<month>[A-Z]{3})(?P<yy>\d{2})"
    r"-(?P<strike>\d+(?:\.\d+)?)"
    r"-(?P<right>[CP])$"
)


class UnknownUnderlyingError(KeyError):
    """Raised for an underlying that isn't in `UNDERLYINGS`."""


def format_strike(strike: float) -> str:
    """Strike as it appears in a symbol: no trailing `.0`, no exponent.

    A strike of 60000.0 must render `60000` and not `60000.0`, because the two
    would be different directories for one contract.
    """
    if strike != strike or strike in (float("inf"), float("-inf")):
        raise ValueError(f"strike must be a finite number, got {strike!r}")
    if float(strike).is_integer():
        return str(int(strike))
    return f"{strike:f}".rstrip("0").rstrip(".")


@dataclass(frozen=True, slots=True)
class OptionContract:
    """One option series, identified independently of any provider.

    The identity is (underlying, expiry, strike, right). Everything else —
    exchange, quote currency, contract size — is metadata attached to it.
    """

    underlying: str
    expiry: date
    strike: float
    right: str
    exchange: str = ""
    quote_currency: str = ""
    contract_size: float = 1.0
    #: When the venue first listed this series. Deribit reports it even for
    #: contracts that expired years ago, and it is the true start of history —
    #: a request for bars before it is refused.
    listed: date | None = None

    def __post_init__(self) -> None:
        if self.right not in RIGHTS:
            raise ValueError(f"right must be one of {sorted(RIGHTS)}, got {self.right!r}")
        if self.strike <= 0:
            raise ValueError(f"strike must be positive, got {self.strike}")

    @property
    def symbol(self) -> str:
        return option_symbol(self.underlying, self.expiry, self.strike, self.right)

    @property
    def deribit_name(self) -> str:
        """This contract as Deribit names it, e.g. `BTC-27DEC24-60000-C`."""
        month = _DERIBIT_MONTHS[self.expiry.month - 1]
        return (f"{self.underlying}-{self.expiry.day}{month}{self.expiry.year % 100:02d}"
                f"-{format_strike(self.strike)}-{self.right}")

    def catalog_entry(self) -> dict:
        """The non-timeframe half of this contract's catalog entry."""
        return {
            "name": self.symbol,
            "underlying": self.underlying,
            "exchange": self.exchange,
            "expiry": self.expiry.isoformat(),
            "strike": self.strike,
            "right": RIGHTS[self.right],
            # Not USD. See the module docstring: a BTC option's premium is
            # quoted in BTC, and reading it as dollars is off by ~5 orders of
            # magnitude.
            "quote_currency": self.quote_currency,
            "contract_size": self.contract_size,
            "listed": self.listed.isoformat() if self.listed else None,
            "categories": ["Crypto Options"],
        }


def option_symbol(underlying: str, expiry: date, strike: float, right: str) -> str:
    """Canonical symbol: underlying, ISO expiry, strike, right.

    The date is `YYYYMMDD` rather than the venues' `27DEC24` so that a
    lexicographic sort of a directory listing is also a chronological one.
    """
    if right not in RIGHTS:
        raise ValueError(f"right must be one of {sorted(RIGHTS)}, got {right!r}")
    return f"{underlying.upper()}-{expiry:%Y%m%d}-{format_strike(strike)}-{right}"


def parse_option_symbol(symbol: str) -> tuple[str, date, float, str]:
    """Inverse of `option_symbol`: returns (underlying, expiry, strike, right)."""
    match = _SYMBOL_RE.match(symbol.upper())
    if not match:
        raise ValueError(f"Not a canonical option symbol: {symbol!r}")
    raw = match.group("expiry")
    expiry = date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    return (match.group("underlying"), expiry,
            float(match.group("strike")), match.group("right"))


def parse_deribit_name(name: str, *, pivot_year: int | None = None) -> tuple[str, date, float, str]:
    """Parse a Deribit instrument name like `BTC-27DEC24-60000-C`.

    The two-digit year resolves against a window centred on `pivot_year` the
    same way futures symbols do, so an archive stays readable next decade.
    """
    match = _DERIBIT_RE.match(name.upper())
    if not match:
        raise ValueError(f"Not a Deribit option name: {name!r}")

    try:
        month = _DERIBIT_MONTHS.index(match.group("month")) + 1
    except ValueError as exc:
        raise ValueError(f"{match.group('month')!r} is not a month in {name!r}") from exc

    if pivot_year is None:
        pivot_year = date.today().year
    year = pivot_year - (pivot_year % 100) + int(match.group("yy"))
    if year - pivot_year > 50:
        year -= 100
    elif pivot_year - year > 50:
        year += 100

    return (match.group("underlying"), date(year, month, int(match.group("day"))),
            float(match.group("strike")), match.group("right"))


def contract_for(
    underlying: str,
    expiry: date,
    strike: float,
    right: str,
    *,
    listed: date | None = None,
    contract_size: float | None = None,
) -> OptionContract:
    """Build a contract from a known underlying, filling venue metadata."""
    key = underlying.upper()
    try:
        meta = UNDERLYINGS[key]
    except KeyError as exc:
        raise UnknownUnderlyingError(
            f"Unknown option underlying {underlying!r}; add it to utils.options.UNDERLYINGS"
        ) from exc
    return OptionContract(
        underlying=key,
        expiry=expiry,
        strike=strike,
        right=right.upper(),
        exchange=str(meta["exchange"]),
        quote_currency=str(meta["quote_currency"]),
        contract_size=(contract_size if contract_size is not None
                       else float(meta["contract_size"])),  # type: ignore[arg-type]
        listed=listed,
    )


def contract_from_deribit_name(
    name: str,
    *,
    listed: date | None = None,
    contract_size: float | None = None,
) -> OptionContract:
    """Build a contract from a Deribit instrument name."""
    underlying, expiry, strike, right = parse_deribit_name(name)
    return contract_for(underlying, expiry, strike, right,
                        listed=listed, contract_size=contract_size)


def option_file_path(provider: str, underlying: str, symbol: str, timeframe: str) -> Path:
    """Canonical path for an option parquet file.

    Provider-first, with the underlying in the universe slot, so a second
    provider is a sibling directory rather than a migration.
    """
    return (DATA_ROOT / "options" / provider.lower() / underlying.upper()
            / symbol / f"{symbol}-{timeframe}.parquet")


def options_catalog_path() -> Path:
    """The options catalog. One file for all underlyings."""
    return DATA_ROOT / "options.yaml"


# --- catalog ------------------------------------------------------------------


def load_options_catalog() -> dict:
    """The options catalog, or an empty one if nothing has been written yet."""
    path = options_catalog_path()
    if not path.exists():
        return {}
    import yaml

    return yaml.safe_load(path.read_text()) or {}


def save_options_catalog(data: dict) -> None:
    from utils.catalog import _dump_yaml

    _dump_yaml(options_catalog_path(), data)


def update_option_entry(
    catalog: dict,
    contract: OptionContract,
    timeframe: str,
    provider: str,
    file_path: Path,
    start_date: str,
    end_date: str,
    semantics: "Semantics | None" = None,
) -> None:
    """Register one (contract, timeframe, provider) series in the catalog.

    Nests by provider *within* the timeframe, like the stock and futures
    catalogs, so the same option sourced from two venues coexists instead of
    one overwriting the other.
    """
    from utils.catalog import (
        _register_timeframe,
        _set_provider_node,
        provider_node_key,
        recompute_entry_bounds,
    )
    from utils.config import relative_to_repo
    from utils.semantics import apply_semantics, semantics_for

    entry = catalog.setdefault(contract.symbol, contract.catalog_entry())
    # A contract first seen from a venue that doesn't report its listing date
    # gets it filled in later by one that does.
    if contract.listed and not entry.get("listed"):
        entry["listed"] = contract.listed.isoformat()
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
    apply_semantics(node, semantics or semantics_for("Crypto Options", provider))

    _set_provider_node(entry, timeframe, provider_node_key(provider), node)
    recompute_entry_bounds(entry)
