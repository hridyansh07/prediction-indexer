"""Closed, immutable fee inputs. Decimal strings enter only through Fixed.parse."""

from __future__ import annotations

import hashlib
import json
import re
import types
from dataclasses import dataclass, fields, is_dataclass
from enum import Enum
from fractions import Fraction
from functools import lru_cache
from typing import get_args, get_origin, get_type_hints


@lru_cache(maxsize=128)
def _hints(cls):
    return get_type_hints(cls)


def _matches(value, annotation):
    origin = get_origin(annotation)
    if origin is types.UnionType:
        return any(_matches(value, part) for part in get_args(annotation))
    if origin is tuple:
        args = get_args(annotation)
        return type(value) is tuple and all(_matches(v, args[0]) for v in value)
    return type(value) is annotation


class Closed:
    __slots__ = ()

    def __post_init__(self):
        for name, annotation in _hints(type(self)).items():
            value = getattr(self, name)
            if not _matches(value, annotation):
                raise TypeError(f"{type(self).__name__}.{name}: invalid type")
            if type(value) is str and len(value) > 4096:
                raise ValueError("string exceeds 4096 characters")
            if type(value) is int and abs(value) > 10**36:
                raise ValueError("integer exceeds domain bound")

    @property
    def identity(self) -> str:
        return hashlib.sha256(b"fee-sdk-v1\0" + canonical(self)).hexdigest()


def tree(value):
    if isinstance(value, Enum):
        return {"enum": type(value).__name__, "value": value.value}
    if is_dataclass(value):
        return {
            "type": type(value).__name__,
            **{f.name: tree(getattr(value, f.name)) for f in fields(value)},
        }
    if type(value) is tuple:
        return [tree(v) for v in value]
    if value is None or type(value) in (str, int, bool):
        return value
    raise TypeError("not a canonical fee value")


def canonical(value) -> bytes:
    return (
        json.dumps(
            tree(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True
        )
        + "\n"
    ).encode()


@dataclass(frozen=True, slots=True)
class Fixed(Closed):
    atoms: int
    scale: int

    def __post_init__(self):
        Closed.__post_init__(self)
        if not 0 <= self.scale <= 18 or self.atoms < 0:
            raise ValueError("nonnegative fixed point with scale 0..18 required")

    @classmethod
    def parse(cls, value: str):
        # No exponent notation, NaN, infinity, float coercion or ambient context.
        if type(value) is not str or not re.fullmatch(
            r"[0-9]{1,36}(?:\.[0-9]{1,18})?", value
        ):
            raise ValueError("bounded unsigned decimal string required")
        whole, dot, fraction = value.partition(".")
        return cls(int(whole + fraction), len(fraction) if dot else 0)

    @property
    def value(self) -> Fraction:
        return Fraction(self.atoms, 10**self.scale)


@dataclass(frozen=True, slots=True)
class Quantity(Fixed):
    pass


@dataclass(frozen=True, slots=True)
class QuotePrice(Fixed):
    pass


@dataclass(frozen=True, slots=True)
class Probability(Fixed):
    def __post_init__(self):
        Fixed.__post_init__(self)
        if self.value > 1:
            raise ValueError("probability exceeds one")


@dataclass(frozen=True, slots=True)
class Rate(Fixed):
    pass


@dataclass(frozen=True, slots=True)
class Multiplier(Fixed):
    pass


class AssetKind(Enum):
    USD = "USD"
    USDC = "USDC"
    PUSD = "pUSD"
    OUTCOME = "outcome"


@dataclass(frozen=True, slots=True)
class Asset(Closed):
    kind: AssetKind
    chain: str
    token: str

    def __post_init__(self):
        Closed.__post_init__(self)
        if not self.chain or not self.token:
            raise ValueError("asset requires chain/ledger and token identity")


@dataclass(frozen=True, slots=True)
class AssetAmount(Closed):
    asset: Asset
    amount: Fixed


@dataclass(frozen=True, slots=True)
class Notional(Closed):
    asset: Asset
    amount: Fixed


@dataclass(frozen=True, slots=True)
class AssetDelta(Closed):
    asset: Asset
    atoms: int
    scale: int

    def __post_init__(self):
        Closed.__post_init__(self)
        if not 0 <= self.scale <= 18:
            raise ValueError("delta scale outside 0..18")

    @property
    def value(self):
        return Fraction(self.atoms, 10**self.scale)


class Venue(Enum):
    POLYMARKET = "polymarket"
    KALSHI = "kalshi"
    LIMITLESS = "limitless"


class Product(Enum):
    CLOB = "prediction_clob"
    AMM = "prediction_amm"


class Side(Enum):
    BUY = "buy"
    SELL = "sell"


class Role(Enum):
    MAKER = "maker"
    TAKER = "taker"
    UNKNOWN = "unknown"


class AccountClass(Enum):
    DIRECT = "direct"
    NON_DIRECT = "non_direct"
    UNKNOWN = "unknown"


class BuilderStatus(Enum):
    ABSENT = "known_absent"
    KNOWN = "known"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class Builder(Closed):
    status: BuilderStatus
    builder_id: str | None = None

    def __post_init__(self):
        Closed.__post_init__(self)
        if (self.status is BuilderStatus.KNOWN) != bool(self.builder_id):
            raise ValueError("only a known builder has an ID")


@dataclass(frozen=True, slots=True)
class Context(Closed):
    venue: Venue
    product: Product
    market: str
    event: str | None
    series: str | None
    category: str | None
    instrument: str
    orientation: str
    account: str
    subaccount: str
    account_class: AccountClass
    builder: Builder

    def __post_init__(self):
        Closed.__post_init__(self)
        if not all(
            (
                self.market,
                self.instrument,
                self.orientation,
                self.account,
                self.subaccount,
            )
        ):
            raise ValueError(
                "explicit instrument, orientation and account context required"
            )


@dataclass(frozen=True, slots=True)
class InstrumentEconomics(Closed):
    quote: Asset
    outcome: Asset
    payout: AssetAmount
    quote_scale: int
    quantity_scale: int
    price_scale: int

    def __post_init__(self):
        Closed.__post_init__(self)
        if (
            self.outcome.kind is not AssetKind.OUTCOME
            or self.quote.kind is AssetKind.OUTCOME
        ):
            raise ValueError("quote/outcome asset mismatch")
        if self.payout.amount.value <= 0 or any(
            not 0 <= s <= 18
            for s in (self.quote_scale, self.quantity_scale, self.price_scale)
        ):
            raise ValueError("invalid economics dimensions")


@dataclass(frozen=True, slots=True)
class HypotheticalFill(Closed):
    fill_id: str
    context: Context
    economics: InstrumentEconomics
    side: Side
    price: QuotePrice
    probability: Probability
    quantity: Quantity
    gross_notional: Notional
    event_time: int
    role: Role
    order_key: str
    fill_index: int
    new_order: bool
    decision_reference: str | None = None

    def __post_init__(self):
        Closed.__post_init__(self)
        if (
            not self.fill_id
            or not self.order_key
            or self.quantity.value <= 0
            or self.event_time < 0
            or self.fill_index < 0
        ):
            raise ValueError("invalid fill identity, time, index or quantity")
        if self.new_order and self.fill_index != 0:
            raise ValueError("new order must start at fill index zero")
        e = self.economics
        if (
            self.gross_notional.asset != e.quote
            or self.gross_notional.amount.value
            != self.price.value * self.quantity.value
        ):
            raise ValueError("gross notional mismatch")
        for value, scale in (
            (self.price.value, e.price_scale),
            (self.quantity.value, e.quantity_scale),
            (self.gross_notional.amount.value, e.quote_scale),
        ):
            if (value * 10**scale).denominator != 1:
                raise ValueError("input not representable at instrument scale")


class Component(Enum):
    PLATFORM = "platform"
    BUILDER = "builder"
    ROUNDING = "rounding"
    ACCOUNT = "account"


class Basis(Enum):
    GROSS = "gross"
    BASE_TAKER = "base_taker"
    MAKER = "maker"
    ACCOUNT_ADJUSTED = "account_adjusted"


class Evidence(Enum):
    ACTUAL_OBSERVED = "actual_observed"
    EXACT_MODEL = "exact_model"
    ESTIMATE = "estimate"
    CONDITIONAL_BOUND = "conditional_conservative_bound"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class FeeCharge(Closed):
    component: Component
    amount: AssetAmount
    evidence: Evidence


@dataclass(frozen=True, slots=True)
class Rebate(Closed):
    component: Component
    amount: AssetAmount
    evidence: Evidence


def fixed(value: Fraction, scale: int) -> Fixed:
    if type(scale) is not int or not 0 <= scale <= 18 or type(value) is not Fraction:
        raise ValueError("exact rational and bounded integer scale required")
    scaled = value * 10**scale
    if scaled.denominator != 1:
        raise ValueError("inexact fixed-point conversion")
    return Fixed(scaled.numerator, scale)


def ceil_grid(value: Fraction, scale: int) -> Fixed:
    if type(scale) is not int or not 0 <= scale <= 18 or type(value) is not Fraction:
        raise ValueError("exact rational and bounded integer scale required")
    scaled = value * 10**scale
    return Fixed(-(-scaled.numerator // scaled.denominator), scale)
