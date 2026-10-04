"""Closed value types exchanged between economic strategies and the SDK."""

from __future__ import annotations

from dataclasses import dataclass, field

SIDES = ("bid", "ask")
# Closed SDK measurement statuses; ``DEPTH_SUFFICIENT`` is the only evaluated one.
UNUSABLE, ONE_SIDED, DEPTH_LIMITED, EVALUATED = (
    "UNUSABLE", "ONE_SIDED", "DEPTH_LIMITED", "DEPTH_SUFFICIENT")
SDK_STATUSES = (UNUSABLE, ONE_SIDED, DEPTH_LIMITED, EVALUATED)
# Admission statuses never reach ``evaluate``. NO_MATCH is control-only.
ADMISSIONS = ("NOT_CAPTURED", "UNSUPPORTED_SHAPE", "UNSUPPORTED_SCALE", "NO_MATCH")
DETAILS = ("intervals", "episodes", "slices")
TRANSFORMS = ("kalshi_complement_ask",)
CONTROLS = ("cyclic_neighbor", "time_shift")
REAL, CONTROL = "real", "control"


@dataclass(frozen=True, slots=True)
class BookRequirement:
    """Static per-book needs; the SDK takes the union over every basket."""

    sides: tuple[str, ...]
    sizes_contracts: tuple[int, ...]
    consumed: bool = True
    transforms: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Requirements:
    books: dict
    profile: object = None


@dataclass(frozen=True, slots=True)
class Basket:
    """One measured key (a basket at one direction and size).

    ``descriptor`` is strategy-owned JSON whose ``legs`` list corresponds to
    ``legs`` by index; the entity identity is its digest. ``order`` is the
    strategy part of the close-order key. ``control_leg`` names the leg a
    control substitutes, or ``None`` when the basket takes no controls;
    ``peer_group`` and ``peer_order`` drive the cyclic-neighbour peers.
    ``inputs`` optionally declares, per leg, every ``(source, size)`` the
    strategy reads: a side, a transform name, or ``("best", None)`` for the
    best quotes. With it the SDK reuses a context-free observation while those
    inputs are unchanged; without it every affected entity is re-evaluated.
    """

    descriptor: dict
    legs: tuple
    order: tuple
    admission: str | None = None
    admission_reasons: tuple[str, ...] = ()
    control_leg: int | None = None
    peer_group: object = None
    peer_order: object = None
    inputs: tuple | None = None


@dataclass(frozen=True, slots=True)
class Control:
    kind: str
    shift_ns: int | None = None


@dataclass(frozen=True, slots=True)
class Observation:
    """A strategy's pure result for one entity at one committed time.

    ``fields`` holds the strategy's declared measurement fields in declared
    order. ``predicates`` names the active episode kinds and must equal the
    declared class map. ``payload`` is written at episode/slice open;
    ``quotes`` (per-leg consumed ``(price, displayed)`` tuples) defines slice
    identity; ``skew_legs`` selects legs for skew, default all.
    ``context_free`` asserts the result used only the declared inputs (not the
    time, sequence or scope), which makes it reusable while they are unchanged.
    """

    status: str
    reasons: tuple[str, ...] = ()
    value_class: str | None = None
    fields: tuple = ()
    predicates: frozenset = frozenset()
    payload: dict | None = None
    quotes: tuple | None = None
    skew_legs: tuple[int, ...] | None = None
    context_free: bool = False


@dataclass(frozen=True, slots=True)
class Context:
    """Evaluation context: the staged time, cut sequence, scope, experiment."""

    time: int
    sequence: int
    scope: int
    experiment_sha256: str


@dataclass(frozen=True)
class Experiment:
    """Everything the SDK needs from a configured strategy, fixed before callbacks.

    ``layout`` 1 is the legacy complement V1 file layout (mixed measurement and
    slice files, ``placebo_episodes.ndjson``); 2 separates real and control rows.
    """

    strategy: str
    policy: dict
    policy_sha256: str
    experiment_sha256: str
    tiers_ns: tuple[str, ...]
    skew_edges_ns: tuple[int, ...]
    kinds: tuple[str, ...]
    episode_classes: dict
    value_classes: tuple[str, ...]
    diagnostic_statuses: tuple[str, ...]
    measurement_fields: tuple[str, ...]
    unevaluated_fields: tuple
    maxima: tuple[str, ...]
    slice_invariant: tuple[str, ...]
    detail: dict = field(default_factory=lambda: {REAL: "slices", CONTROL: "slices"})
    controls: tuple[Control, ...] = ()
    layout: int = 2
    ring_entries: int = 0
    static_reservation: int = 0
    profile: object = None
