"""Count-based retained-state bounds, replacing recursive size accounting.

Every retained structure is charged by a closed-form function of its declared
shape (books, sides, sizes, levels, entities, strings). The constants are
upper bounds on CPython's per-object costs; a unit test compares each formula
against a real recursive ``sys.getsizeof`` traversal of representative
maximal states. Exceeding a limit fails the attempt; nothing is truncated.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field

from replay.fees.lazy import LazyAssessmentIds
from replay.streams.protocol import freeze, require
from replay.strategy_sdk import plain

MAX_ROWS = 2_000_000
MAX_BYTES = 512 * 1024 * 1024
MAX_LINE = 64 * 1024
MAX_METADATA = 8 * 1024 * 1024
# Layout-2 entities.json repeats every scope's descriptors, so long events need
# more than MAX_METADATA. Reason tables and summaries keep MAX_METADATA.
MAX_ENTITY_TABLE = 64 * 1024 * 1024
MAX_STATE = 512 * 1024 * 1024
MAX_CONSUMED_LEVELS = 1024
MAX_SKEW_POINTS = 100_000
MAX_PENDING_PROBES = 100_000

# Per-object upper bounds (64-bit CPython 3.11+), with allocator slack.
INT = 48            # an int below 2**120
SMALL = 80          # a small fixed-size object (bool/None share singletons)
PAIR = 64 + 2 * INT  # a (price, quantity) tuple of two ints
STR = 80            # string header; characters are charged separately
CONTAINER = 120     # empty list/tuple/dict/set header
SLOT = 48           # one container slot, including dict/set hash-table growth
FILL = 120 + 3 * INT + 2 * CONTAINER  # Fill object with its scalars and tuples
VIEW = 400          # BookView with its dicts, excluding fills
OBSERVATION = 400   # Observation, its tuples and the staged (value, skew) pair
ENTITY = 600        # entity record and its map/reverse-map slots
MEASUREMENT = 400   # open measurement tuple and its start time
EPISODE = 1600      # open-episode record, tiers and qualified-time slots
RING_ENTRY = 160    # one (time, view) ring slot, excluding the view
LIVE_FILL = 160     # a live fill record (spec §13), excluding its JSON and kill prices
FILL_STATE = 240    # one entity's open fill state: start, state and ladder epochs


def json_cost(value):
    """Closed-form cost of a small JSON-shaped value (strings, ints, lists, dicts)."""
    kind = type(value)
    if kind is str:
        return STR + 4 * len(value)
    if kind is int:
        return INT + value.bit_length() // 8
    if kind is dict:
        return CONTAINER + sum(SLOT + json_cost(k) + json_cost(v) for k, v in value.items())
    if kind is list or kind is tuple:
        return CONTAINER + sum(SLOT + json_cost(v) for v in value)
    if kind is bool or value is None:
        return SMALL
    if isinstance(value, LazyAssessmentIds):
        # Captured immutable order inputs plus the worst-case eager debug IDs
        # and materialized IDs. Never traverse the shared engine/catalog or
        # materialize a handle just to account its retained state.
        return (512 + json_cost(value.request.retained_inputs)
                + 2 * (CONTAINER + len(value) * (SLOT + STR + 4 * 64)))
    if type(value) is FrozenJSON:
        return value.cost
    if isinstance(value, Mapping):
        return CONTAINER + sum(SLOT + json_cost(k) + json_cost(v) for k, v in value.items())
    return SMALL


@dataclass(frozen=True, slots=True, eq=False)
class FrozenJSON(Mapping):
    """Owned immutable metadata with the original occurrence-based JSON cost.

    This saves traversal, never reservation: every reference costs exactly as
    the original tree. Entity metadata fits the existing descriptor reservation.
    """

    value: object
    cost: int = field(init=False)

    def __post_init__(self):
        owned = freeze(plain(self.value))
        object.__setattr__(self, "value", owned)
        object.__setattr__(self, "cost", json_cost(owned))

    def __getitem__(self, key):
        return self.value[key]

    def __iter__(self):
        return iter(self.value)

    def __len__(self):
        return len(self.value)


def fills_cost(sizes, levels):
    """Fills for ``sizes`` across sides/transforms that hold ``levels`` pairs."""
    return sizes * (FILL + SLOT) + levels * (PAIR + SLOT)


def fingerprint_cost(fingerprint):
    """A memoized flat input fingerprint: fills, best quotes, validity, reasons."""
    cost = CONTAINER
    for part in fingerprint:
        cost += SLOT
        if hasattr(part, "consumed"):
            cost += FILL + (len(part.taken) + len(part.consumed)) * (PAIR + SLOT)
        elif type(part) is tuple:
            cost += PAIR
        elif type(part) is dict:
            cost += json_cost(part)
        elif type(part) is str:
            cost += STR + 4 * len(part)
    return cost


def view_cost(view, sizes):
    """A view; ``levels`` counts fill pairs and, in fill mode, retained ladder pairs."""
    sides = len(view.fills) + len(view.transformed)
    return (VIEW + sides * CONTAINER + fills_cost(sides * sizes, view.levels)
            + (json_cost(view.reason) if view.reason is not None else 0) + 2 * PAIR
            + len(view.ladders) * (CONTAINER + SLOT)
            + (age_timestamps_cost(len(view.best_changes)) if view.best_changes else 0))


def age_timestamps_cost(ladders):
    """One last book update and one best-change integer per declared ladder.

    Exit ladders count. Map containers/keys are charged separately from the
    exact ``1 + L`` timestamp reservation; L is at most four.
    """
    return ((1 + ladders) * INT + CONTAINER
            + ladders * (SLOT + STR + 4 * len("kalshi_complement_ask")))


def age_commit_cost(ladders):
    """Same-time comparison baseline, including replacement allocation overlap.

    The baseline owns only each declared ladder's best pair and timestamp,
    never its full depth. Reserve the previous and replacement baseline while
    a stage is committing so temporary dictionary copies remain bounded too.
    """
    return 2 * (CONTAINER + 2 * SLOT + 2 * CONTAINER + ladders * (
        2 * SLOT + 2 * (STR + 4 * len("kalshi_complement_ask")) + PAIR + INT))


def live_fill_cost(priced):
    """A live fill: its episode ``fill`` object and per-leg kill prices."""
    return (LIVE_FILL + json_cost(priced.json) + CONTAINER
            + len(priced.kill) * (SLOT + INT))


def fill_state_cost(legs):
    return FILL_STATE + CONTAINER + legs * (SLOT + INT)


def probe_cost(entity, legs, levels):
    """One pending scenario, including copied decision, fills and arrival exit depth.

    Reserve both object state and canonical row strings before adding a probe;
    arithmetic is bounded separately to 80 decimal digits per output scalar.
    """
    return (4096 + json_cost(entity.descriptor) + json_cost(entity.legs)
            + legs * (4096 + levels * (8 * (PAIR + SLOT) + 8 * (STR + 4 * 80))))


def quotes_cost(quotes):
    if quotes is None:
        return 0
    return CONTAINER + sum(CONTAINER + SLOT + len(leg) * (PAIR + SLOT) for leg in quotes)


def observation_cost(observation):
    cost = OBSERVATION + sum(STR + 4 * len(reason) + SLOT for reason in observation.reasons)
    cost += sum(json_cost(value) + SLOT for value in observation.fields)
    if observation.payload is not None:
        cost += json_cost(observation.payload)
    return cost + quotes_cost(observation.quotes)


def episode_cost(observation, tiers):
    # Opening and current slice payload/quotes are both retained.
    return EPISODE + tiers * (SLOT + INT) + 2 * (
        json_cost(observation.payload) + quotes_cost(observation.quotes))


class StateBudget:
    """Running total of retained detached bytes, checked before retention."""

    def __init__(self, limit=MAX_STATE):
        self.limit = limit
        self.used = 0

    def replace(self, old, new, message="detached state budget"):
        require(self.used - old + new <= self.limit, message)
        self.used += new - old

    def charge(self, amount, message="detached state budget"):
        self.replace(0, amount, message)

    def release(self, amount):
        self.used -= amount
