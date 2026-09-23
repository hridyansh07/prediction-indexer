"""Pinned schedules and version-one component precedence; no I/O or rate defaults."""

from __future__ import annotations

import re
import threading
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from enum import Enum

from .domain import (
    Asset,
    Closed,
    Component,
    Context,
    InstrumentEconomics,
    Multiplier,
    Product,
    Rate,
    Venue,
)


class Rounding(Enum):
    UNKNOWN = "unknown"
    CEIL = "ceil"
    EXACT = "exact"
    PM_CEIL5_SCENARIO = "pm_declared_fill_ceil5_scenario"


@dataclass(frozen=True, slots=True)
class Polymarket(Closed):
    rate: Rate
    exponent: int
    taker_only: bool
    rounding: Rounding = Rounding.PM_CEIL5_SCENARIO

    def __post_init__(self):
        Closed.__post_init__(self)
        if not 0 <= self.exponent <= 16:
            raise ValueError("supported integer exponent range is 0..16")


class KalshiKind(Enum):
    QUADRATIC = "quadratic"
    MAKER = "quadratic_with_maker_fees"
    FLAT = "flat"
    COMBO = "quadratic_with_combo_maker_fees"


@dataclass(frozen=True, slots=True)
class Kalshi(Closed):
    # None explicitly clears an EVENT override to the current SERIES field.
    kind: KalshiKind | None
    multiplier: Multiplier | None


@dataclass(frozen=True, slots=True)
class LimitlessClob(Closed):
    maker_free: bool


class TradeBasis(Enum):
    NOTIONAL = "gross_quote_notional"
    QUANTITY = "gross_contract_quantity"


@dataclass(frozen=True, slots=True)
class LimitlessAmm(Closed):
    rate: Rate
    basis: TradeBasis
    rounding: Rounding


@dataclass(frozen=True, slots=True)
class BuilderFee(Closed):
    taker_bps: int
    maker_bps: int
    rounding: Rounding

    def __post_init__(self):
        Closed.__post_init__(self)
        if not 0 <= self.taker_bps <= 100 or not 0 <= self.maker_bps <= 50:
            raise ValueError("builder rate exceeds documented caps")


@dataclass(frozen=True, slots=True)
class AccountRebate(Closed):
    rate: Rate
    rounding: Rounding

    def __post_init__(self):
        Closed.__post_init__(self)
        if self.rate.value > 1:
            raise ValueError("rebate fraction exceeds one")


@dataclass(frozen=True, slots=True)
class ZeroFee(Closed):
    reason: str


@dataclass(frozen=True, slots=True)
class Unsupported(Closed):
    reason: str


Model = (
    Polymarket
    | Kalshi
    | LimitlessClob
    | LimitlessAmm
    | BuilderFee
    | AccountRebate
    | ZeroFee
    | Unsupported
)


@dataclass(frozen=True, slots=True)
class Source(Closed):
    url: str
    sha256: str
    byte_length: int
    retrieved_at: int

    def __post_init__(self):
        Closed.__post_init__(self)
        if not self.url.startswith("https://") or not re.fullmatch(
            "[0-9a-f]{64}", self.sha256
        ):
            raise ValueError("HTTPS source URL and lowercase SHA256 required")
        if not 0 <= self.byte_length <= 16 * 1024 * 1024 or self.retrieved_at < 0:
            raise ValueError("source size/time outside bounds")


@dataclass(frozen=True, slots=True)
class Scope(Closed):
    venue: Venue
    product: Product
    market: str | None = None
    event: str | None = None
    series: str | None = None
    category: str | None = None
    account: str | None = None
    subaccount: str | None = None
    builder: str | None = None
    instrument: str | None = None
    orientation: str | None = None

    @property
    def rank(self):
        for rank, name in ((4, "market"), (3, "event"), (2, "series"), (1, "category")):
            if getattr(self, name) is not None:
                return rank
        return 0

    def matches(self, context: Context):
        if self.venue != context.venue or self.product != context.product:
            return False
        for name in (
            "market",
            "event",
            "series",
            "category",
            "account",
            "subaccount",
            "instrument",
            "orientation",
        ):
            value = getattr(self, name)
            if value is not None and value != getattr(context, name):
                return False
        return self.builder is None or self.builder == context.builder.builder_id


@dataclass(frozen=True, slots=True)
class Schedule(Closed):
    component: Component
    scope: Scope
    economics: InstrumentEconomics
    fee_asset: Asset
    fee_scale: int
    model: Model
    sources: tuple[Source, ...]
    effective_from: int | None
    effective_to: int | None
    effective_evidence: str | None
    extractor_version: str
    model_version: str
    supersedes: tuple[str, ...] = ()

    def __post_init__(self):
        Closed.__post_init__(self)
        if (
            not self.sources
            or len(self.sources) > 32
            or not self.extractor_version
            or not self.model_version
        ):
            raise ValueError("source and version identities required")
        if not 0 <= self.fee_scale <= 18:
            raise ValueError("fee scale outside range")
        if self.effective_from is None:
            if self.effective_to is not None or self.effective_evidence is not None:
                raise ValueError("unknown effective interval must be explicit")
        elif (
            self.effective_from < 0
            or not self.effective_evidence
            or (
                self.effective_to is not None
                and self.effective_to <= self.effective_from
            )
        ):
            raise ValueError("invalid half-open effective interval")
        if self.component is Component.ROUNDING:
            raise ValueError("rounding belongs to the platform model")
        if isinstance(self.model, BuilderFee) and (
            self.component is not Component.BUILDER
            or self.scope.builder is None
            or self.scope.venue is not Venue.POLYMARKET
        ):
            raise ValueError(
                "builder model requires scoped Polymarket builder component"
            )
        if isinstance(self.model, AccountRebate) and (
            self.component is not Component.ACCOUNT
            or self.scope.account is None
            or self.scope.subaccount is None
            or self.scope.venue is not Venue.POLYMARKET
        ):
            raise ValueError("account rebate requires scoped Polymarket account")
        venue_models = {
            Polymarket: Venue.POLYMARKET,
            Kalshi: Venue.KALSHI,
            LimitlessClob: Venue.LIMITLESS,
            LimitlessAmm: Venue.LIMITLESS,
        }
        if type(self.model) in venue_models:
            product = (
                Product.AMM if isinstance(self.model, LimitlessAmm) else Product.CLOB
            )
            if (
                self.component is not Component.PLATFORM
                or self.scope.venue is not venue_models[type(self.model)]
                or self.scope.product is not product
            ):
                raise ValueError("venue/product/component model mismatch")
        if (
            isinstance(self.model, Kalshi)
            and (self.model.kind is None or self.model.multiplier is None)
            and self.scope.rank != 3
        ):
            raise ValueError("only event overrides may clear Kalshi fields")
        if len(set(self.supersedes)) != len(self.supersedes) or any(
            not re.fullmatch("[0-9a-f]{64}", s) for s in self.supersedes
        ):
            raise ValueError("invalid supersedes identities")

    @property
    def known_at(self):
        return max(s.retrieved_at for s in self.sources)


@dataclass(frozen=True, slots=True)
class Catalog(Closed):
    schedules: tuple[Schedule, ...]
    precedence_version: str = "component-scope-v1"

    def __post_init__(self):
        Closed.__post_init__(self)
        if (
            self.precedence_version != "component-scope-v1"
            or len(self.schedules) > 10000
        ):
            raise ValueError("unsupported precedence or oversized catalog")
        ids = tuple(s.identity for s in self.schedules)
        if ids != tuple(sorted(set(ids))):
            raise ValueError("catalog schedules must be unique and identity-sorted")

    @classmethod
    def build(cls, schedules):
        return cls(tuple(sorted(schedules, key=lambda s: s.identity)))


class UnknownReason(Enum):
    MISSING = "missing_schedule"
    HISTORY = "unknown_effective_history"
    CONFLICT = "equal_priority_conflict"
    UNSUPPORTED = "unsupported_model"
    BUILDER = "unknown_builder"
    ACCOUNT = "unknown_account_class"
    STATE = "incomplete_order_state"
    ROUNDING = "unknown_rounding"
    DIMENSIONS = "unsupported_dimensions"


@dataclass(frozen=True, slots=True)
class UnknownSchedule(Closed):
    component: Component
    reason: UnknownReason
    candidates: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Selection(Closed):
    schedule: Schedule
    model: Model
    dependencies: tuple[Schedule, ...]


@dataclass(frozen=True, slots=True)
class ResolvedScheduleSet(Closed):
    context: Context
    event_time: int
    knowledge_cutoff: int | None
    catalog_identity: str
    platform: Selection | UnknownSchedule
    builder: Selection | UnknownSchedule
    account: Selection | UnknownSchedule
    next_boundary: int | None
    reference_time: int | None = None
    snapshot_identity: str | None = None


class _ResolverMemo:
    """Derived per-resolver state; excluded from identity, equality and pickles."""

    __slots__ = ("lock", "identities", "index", "selections")

    def __init__(self):
        self.lock = threading.Lock()
        self.identities = None
        self.index = None
        self.selections = OrderedDict()

    def __reduce__(self):
        return (_ResolverMemo, ())


@dataclass(frozen=True, slots=True)
class Resolver(Closed):
    catalog: Catalog
    reference_time: int | None = None
    _memo: _ResolverMemo = field(
        default_factory=_ResolverMemo, init=False, repr=False, compare=False
    )

    def __post_init__(self):
        Closed.__post_init__(self)
        if self.reference_time is not None and self.reference_time < 0:
            raise ValueError("nonnegative reference time required")

    def resolve(
        self, context: Context, event_time: int, knowledge_cutoff: int | None = None
    ) -> ResolvedScheduleSet:
        if (
            type(context) is not Context
            or type(event_time) is not int
            or event_time < 0
        ):
            raise ValueError("invalid resolution context/time")
        if knowledge_cutoff is not None and (
            type(knowledge_cutoff) is not int or not 0 <= knowledge_cutoff <= event_time
        ):
            raise ValueError(
                "as-known cutoff must not exceed event time; None means retrospective"
            )
        if self.reference_time is not None:
            if knowledge_cutoff is not None:
                raise ValueError("current snapshot does not accept a knowledge cutoff")
            return replace(
                self._select(context, self.reference_time, None), event_time=event_time
            )
        return self._select(context, event_time, knowledge_cutoff)

    def _select(self, context, event_time, knowledge_cutoff):
        # Bounded memoization reuses reference-time selections across fill times.
        # Keys hold only small selection inputs, never the catalog, so a lookup
        # does not rehash it; eviction only recomputes the identical selection.
        memo, key = self._memo, (context, event_time, knowledge_cutoff)
        with memo.lock:
            selected = memo.selections.get(key)
            if selected is not None:
                memo.selections.move_to_end(key)
                return selected
        selected = self._compute(context, event_time, knowledge_cutoff)
        with memo.lock:
            memo.selections[key] = selected
            if len(memo.selections) > 1024:
                memo.selections.popitem(last=False)
        return selected

    def _compute(self, context, event_time, knowledge_cutoff):
        # Unknown-start evidence is caller-asserted current only in snapshot mode.
        # Identities and the venue/product index are idempotent, so a racing
        # first computation is harmless; each is derived once per resolver.
        memo = self._memo
        if memo.identities is None:
            memo.identities = (
                self.catalog.identity,
                self.identity if self.reference_time is not None else None,
            )
        if memo.index is None:
            index = {}
            for s in self.catalog.schedules:
                index.setdefault((s.scope.venue, s.scope.product), []).append(s)
            memo.index = {k: tuple(v) for k, v in index.items()}
        catalog_identity, snapshot_identity = memo.identities
        # Scope.matches requires venue/product first; the index keeps catalog order.
        candidates = tuple(
            s
            for s in memo.index.get((context.venue, context.product), ())
            if s.scope.matches(context)
            and (knowledge_cutoff is None or s.known_at <= knowledge_cutoff)
        )
        boundaries = tuple(
            t
            for s in candidates
            for t in (s.effective_from, s.effective_to)
            if t is not None and t > event_time
        )

        def choose(component, pool):
            pool = tuple(s for s in pool if s.component is component)
            active = tuple(
                s
                for s in pool
                if (s.effective_from is None and self.reference_time is not None)
                or (
                    s.effective_from is not None
                    and s.effective_from <= event_time
                    and (s.effective_to is None or event_time < s.effective_to)
                )
            )
            # Unknown applicability cannot silently lose to a broad known default.
            uncertain = tuple(
                s
                for s in pool
                if s.effective_from is None and self.reference_time is None
            )
            rank = max((s.scope.rank for s in active), default=-1)
            if any(s.scope.rank >= rank for s in uncertain):
                return UnknownSchedule(
                    component,
                    UnknownReason.HISTORY,
                    tuple(sorted(s.identity for s in uncertain)),
                )
            active = tuple(s for s in active if s.scope.rank == rank)
            superseded = {identity for s in active for identity in s.supersedes}
            active = tuple(s for s in active if s.identity not in superseded)
            if len(active) != 1:
                return UnknownSchedule(
                    component,
                    UnknownReason.CONFLICT if active else UnknownReason.MISSING,
                    tuple(sorted(s.identity for s in (active or pool))),
                )
            s = active[0]
            return Selection(s, s.model, (s,))

        platform = choose(Component.PLATFORM, candidates)
        if isinstance(platform, Selection) and isinstance(platform.model, Kalshi):
            m = platform.model
            if m.kind is None or m.multiplier is None:
                series = choose(
                    Component.PLATFORM,
                    tuple(s for s in candidates if s.scope.rank == 2),
                )
                if not isinstance(series, Selection) or not isinstance(
                    series.model, Kalshi
                ):
                    platform = UnknownSchedule(
                        Component.PLATFORM, UnknownReason.MISSING
                    )
                elif (
                    series.schedule.economics != platform.schedule.economics
                    or series.schedule.fee_asset != platform.schedule.fee_asset
                    or series.schedule.fee_scale != platform.schedule.fee_scale
                ):
                    platform = UnknownSchedule(
                        Component.PLATFORM, UnknownReason.DIMENSIONS
                    )
                else:
                    platform = replace(
                        platform,
                        model=Kalshi(
                            m.kind if m.kind is not None else series.model.kind,
                            m.multiplier
                            if m.multiplier is not None
                            else series.model.multiplier,
                        ),
                        dependencies=(platform.schedule, series.schedule),
                    )
        return ResolvedScheduleSet(
            context,
            event_time,
            knowledge_cutoff,
            catalog_identity,
            platform,
            choose(Component.BUILDER, candidates),
            choose(Component.ACCOUNT, candidates),
            min(boundaries, default=None),
            self.reference_time,
            snapshot_identity,
        )
