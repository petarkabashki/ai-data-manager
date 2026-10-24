"""What the numbers in a series *mean*, recorded per catalog node.

`normalize_ohlcv()` guarantees every OHLCV file has the same columns and the
same timestamp units. It cannot guarantee that `close` means the same thing in
two of them, and it doesn't: `download_yf.py --adjusted` requests Yahoo's
back-adjusted closes and stores them in the same `close` column as an
unadjusted run, just as IBKR's `--what-to-show ADJUSTED_LAST` does against
`TRADES`. Same column, same dtype, different number — and nothing in the file
records which one it is.

This is not hypothetical: the stored `stocks/yf` tree is mixed per symbol.
`sp500/AAPL` matches Yahoo's adjusted curve while `nasdaq100/AAPL`, `sp500/KO`
and `sp500/XOM` match the unadjusted one bit-for-bit, because they were
downloaded by runs that passed the flag differently.

This module turns those conventions from tribal knowledge into four fields
stamped on every catalog node at write time:

    schema        what kind of series this is at all
    adjustment    whether prices are corporate-action adjusted
    volume_basis  what a unit of `volume` counts
    bar_label     whether `timestamp` marks the bar's open or its close

`tools/check_catalog.py` verifies they're present and valid, and can backfill
existing catalogs from the provider defaults below.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

# --- vocabularies -------------------------------------------------------------

#: Kind of series. Only `ohlcv` is subject to the canonical schema, which is
#: why `tools/check_schema.py` skips macro — that exclusion used to be
#: convention, and this field is what makes it data.
SCHEMAS = frozenset({"ohlcv", "rate_series"})

#: `raw` — prices as traded, no dividend back-adjustment.
#: `adjusted` — back-adjusted for splits and dividends.
#: `not_applicable` — instrument has no corporate actions (crypto, indexes).
#:
#: KNOWN IMPRECISION: `raw` covers two different things, because splits and
#: dividends are adjusted independently and this vocabulary has no room to say
#: so. Yahoo's unadjusted `Close` is already split-adjusted; IBKR `TRADES` is
#: not. Measured on the stored tree, `stocks/ibkr/sp500/{XOM,PG,CVX,JNJ}`
#: closes are exactly 2x Yahoo's for the same days — the 2:1 splits those names
#: ran between 2001 and 2004, which IBKR never back-adjusted. Both files are
#: `raw` here. That is fine for returns, which are scale-free, and wrong for
#: anything quoting a historical price or sizing in shares; add a
#: `split_adjusted` field before relying on it for the latter.
ADJUSTMENTS = frozenset({"raw", "adjusted", "not_applicable"})

#: What one unit of `volume` counts.
VOLUME_BASES = frozenset({"base_asset", "contracts", "shares", "none"})

#: Whether `timestamp` marks the start or the end of the bar's interval.
BAR_LABELS = frozenset({"open", "close"})

SEMANTIC_FIELDS: tuple[str, ...] = ("schema", "adjustment", "volume_basis", "bar_label")

_VOCABULARIES: dict[str, frozenset[str]] = {
    "schema": SCHEMAS,
    "adjustment": ADJUSTMENTS,
    "volume_basis": VOLUME_BASES,
    "bar_label": BAR_LABELS,
}


@dataclass(frozen=True, slots=True)
class Semantics:
    schema: str = "ohlcv"
    adjustment: str = "raw"
    volume_basis: str = "base_asset"
    bar_label: str = "open"

    def __post_init__(self) -> None:
        for field, allowed in _VOCABULARIES.items():
            value = getattr(self, field)
            if value not in allowed:
                raise ValueError(
                    f"{field}={value!r} is not one of {sorted(allowed)}"
                )

    def as_node_fields(self) -> dict[str, str]:
        return asdict(self)


# --- provider defaults --------------------------------------------------------
#
# Keyed by asset class, then by provider. `None` as a provider key is the
# fallback for that class.
#
# ASSUMPTIONS worth verifying against provider documentation before relying on
# them for sizing or PnL, flagged here rather than left implicit:
#   * Binance USDⓈ-M and Bybit linear perpetual klines report volume in base
#     asset units; KuCoin futures reports it in contracts (lots). If that is
#     wrong for a pair, it is wrong by a contract multiplier, not a rounding
#     error.
#   * IBKR index feeds carry no real trade volume; whatever arrives is stored
#     as-is, so `volume_basis: none` means "do not use this column".
#   * YF index and commodity volume is likewise not a clean share count.

_DEFAULTS: dict[str, dict[str | None, Semantics]] = {
    "Crypto": {
        None: Semantics("ohlcv", "not_applicable", "base_asset", "open"),
    },
    "Crypto Futures": {
        "kucoin": Semantics("ohlcv", "not_applicable", "contracts", "open"),
        None: Semantics("ohlcv", "not_applicable", "base_asset", "open"),
    },
    "Stock": {
        # Matches `download_yf.py`'s default (`auto_adjust=False`). A run that
        # passed `--adjusted` stamps "adjusted" explicitly and overrides this;
        # this default is only ever the seed for a node that recorded nothing,
        # so do not trust it over a measurement of the file itself.
        "YF": Semantics("ohlcv", "raw", "shares", "open"),
        "IBKR": Semantics("ohlcv", "raw", "shares", "open"),
        None: Semantics("ohlcv", "raw", "shares", "open"),
    },
    "Index": {
        None: Semantics("ohlcv", "not_applicable", "none", "open"),
    },
    "Commodity": {
        None: Semantics("ohlcv", "raw", "contracts", "open"),
    },
    # Individual futures contracts, stored raw and never back-adjusted. A
    # continuous series is a *derived* view built from these on demand: you can
    # always compute an adjusted series from raw contracts, and never the
    # reverse. Storing raw also keeps the append-only merge valid, because a
    # back-adjustment would rewrite every historical bar on each roll.
    "Futures": {
        None: Semantics("ohlcv", "raw", "contracts", "open"),
    },
    # Option premiums, stored exactly as quoted. Two warnings that the four
    # fields here cannot carry, so they live on the catalog entry instead (see
    # `utils/options.py`): the premium is denominated in the settlement coin
    # rather than USD, and `volume` counts contracts whose size is a separate
    # field. `not_applicable` for adjustment because an option series has no
    # corporate actions — it simply expires.
    "Crypto Options": {
        None: Semantics("ohlcv", "not_applicable", "contracts", "open"),
    },
    "Macro": {
        None: Semantics("rate_series", "not_applicable", "none", "open"),
    },
}

#: IBKR `--what-to-show` values that return back-adjusted prices.
_ADJUSTED_WHAT_TO_SHOW = frozenset({"ADJUSTED_LAST"})


def semantics_for(
    asset_class: str,
    provider: str | None = None,
    what_to_show: str | None = None,
) -> Semantics:
    """Default semantics for a (asset class, provider) pair.

    `what_to_show` is the IBKR request type; passing `ADJUSTED_LAST` flips
    `adjustment` to `adjusted`. Any explicit value a caller has beats this —
    these are defaults for writers that don't know better, and the seed for
    `check_catalog.py --fix` when backfilling nodes written before this
    existed.
    """
    try:
        by_provider = _DEFAULTS[asset_class]
    except KeyError as exc:
        raise ValueError(
            f"Unknown asset class {asset_class!r}; expected one of {sorted(_DEFAULTS)}"
        ) from exc

    base = by_provider.get(provider) or by_provider.get(
        (provider or "").lower()
    ) or by_provider[None]

    if what_to_show and what_to_show.upper() in _ADJUSTED_WHAT_TO_SHOW:
        return Semantics(base.schema, "adjusted", base.volume_basis, base.bar_label)
    return base


def asset_class_from_categories(categories: list[str] | None) -> str | None:
    """Best-effort asset class from a catalog entry's `categories` list."""
    for category in categories or []:
        if category in _DEFAULTS:
            return category
    return None


def validate_node_semantics(node: dict) -> list[str]:
    """Problems with a node's semantic fields: missing, or outside its vocabulary."""
    problems: list[str] = []
    for field in SEMANTIC_FIELDS:
        if field not in node:
            problems.append(f"missing {field}")
        elif node[field] not in _VOCABULARIES[field]:
            problems.append(f"{field}={node[field]!r} not in {sorted(_VOCABULARIES[field])}")
    return problems


def apply_semantics(node: dict, semantics: Semantics) -> None:
    """Stamp semantics onto a node, leaving any explicit existing value alone."""
    for field, value in semantics.as_node_fields().items():
        node.setdefault(field, value)
