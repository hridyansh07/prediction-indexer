"""Fill checks: one priced fill per episode, with per-leg kill prices.

SDK spec §13. In fill mode the strategy's pure ``evaluate`` stays the cheap
trigger: it decides from best prices whether an opportunity exists. When the
trigger kind is active and the entity has no live fill, the runtime prices the
strategy's *governing* sizings with ``walk_basket`` against the ladders the
views already hold at that committed instant. When every governing sizing is
tradeable a fill opens: its *recording* sizings are priced once too, kill
prices are computed per leg, and nothing is ever re-walked. The fill ends at
the first committed update where a leg's best walked price reaches its
effective kill price, or when the trigger turns false. A bought leg walks asks
and is killed when its best ask is at or above its kill price (the lowest over
governing sizings); a sold leg walks bids and is killed when its best bid is at
or below it (the highest over governing sizings).

This module holds the closed policy, the static per-entity spec check, the
runtime pricing and the episode's ``fill`` object. The independent reader in
``aggregate_reader`` re-verifies that object without calling anything here
except the closed-policy parser.
"""

from __future__ import annotations

import re
from fractions import Fraction

from replay.economic_fills import kill_price, walk_basket
from replay.economic_sdk import bounds
from replay.economic_sdk.types import (BUY_SOURCES, SELL_SOURCES, FillPolicy, FillSizing,
                                       FillSpec)
from replay.streams.protocol import obj, require

FILL_LIVE, FILL_DEPTH_SHORT, FILL_VALUE_UNKNOWN, FILL_NONPOSITIVE = (
    "FILL_LIVE", "FILL_DEPTH_SHORT", "FILL_VALUE_UNKNOWN", "FILL_NONPOSITIVE")
# Closed partition of trigger-positive time (denominator ``fill_ns`` keys).
FILL_STATES = (FILL_LIVE, FILL_DEPTH_SHORT, FILL_VALUE_UNKNOWN, FILL_NONPOSITIVE)
# When governing sizings fail for different reasons, the first of these wins.
NO_FILL_PRECEDENCE = (FILL_DEPTH_SHORT, FILL_VALUE_UNKNOWN, FILL_NONPOSITIVE)
KILL_PRICE = "KILL_PRICE"
GOVERNS, RECORDS = "governs", "records"
MODES = ("target", "edge")
# A leg buys from an ascending ask ladder or sells into a descending bid
# ladder, natively or through a transform that projects to that side.
FILL_SOURCES = BUY_SOURCES + SELL_SOURCES
MAX_SIZINGS = 8
_DECIMAL = re.compile(r"(0|[1-9][0-9]*)(\.[0-9]*[1-9])?")
_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")


def contracts(text):
    """Exact positive contract count from a canonical decimal string."""
    require(type(text) is str and _DECIMAL.fullmatch(text) is not None,
            "fill contracts must be canonical decimal strings")
    value = Fraction(text)
    require(value > 0, "fill contracts must be positive")
    return value


def _sizing(value, step):
    require(type(value) is dict and "name" in value and "role" in value, "fill sizing")
    modes = [field for field in ("target_contracts", "edge") if field in value]
    require(len(modes) == 1, "a fill sizing has exactly one mode")
    (field,) = modes
    value = obj(value, "name role " + field)
    require(type(value["name"]) is str and _NAME.fullmatch(value["name"]) is not None,
            "fill sizing name")
    require(value["role"] in (GOVERNS, RECORDS), "fill sizing role")
    if field == "target_contracts":
        ratio = contracts(value[field]) / step
        require(ratio.denominator == 1, "fill target must be a whole number of steps")
        return FillSizing(value["name"], value["role"], "target", target_contracts=value[field],
                          target_steps=int(ratio))
    require(value[field] is True, "fill edge sizing must be true")
    return FillSizing(value["name"], value["role"], "edge")


def fill_policy(value, kind):
    """Parse the closed fill policy; ``kind`` is the strategy's trigger kind.

    ``{version: 1, step_contracts, max_levels, sizings: [..]}``; each sizing is
    ``{name, role}`` plus exactly one of ``target_contracts`` (a whole number of
    steps) or ``edge: true``. Names are unique, there are at most eight
    sizings, and at least one governs.
    """
    value = obj(value, "version step_contracts max_levels sizings")
    require(type(value["version"]) is int and value["version"] == 1, "fill policy version")
    step = contracts(value["step_contracts"])
    levels = value["max_levels"]
    require(type(levels) is int and 1 <= levels <= bounds.MAX_CONSUMED_LEVELS, "fill max_levels")
    sizings = value["sizings"]
    require(type(sizings) is list and 1 <= len(sizings) <= MAX_SIZINGS, "fill sizings")
    sizings = tuple(_sizing(sizing, step) for sizing in sizings)
    require(len({sizing.name for sizing in sizings}) == len(sizings), "fill sizing names must be unique")
    require(any(sizing.role == GOVERNS for sizing in sizings), "a fill sizing must govern")
    require(type(kind) is str and kind, "fill trigger kind")
    return FillPolicy(kind, value["step_contracts"], levels, sizings)


def step_atoms(policy, quantity_scale, ratio=1):
    """Atoms one basket step takes from a leg at ``quantity_scale`` and ``ratio``."""
    atoms = contracts(policy.step_contracts) * ratio * 10 ** int(quantity_scale)
    require(atoms.denominator == 1 and atoms > 0,
            "fill step is not a whole number of quantity atoms")
    return int(atoms)


def check_experiment(experiment):
    """Fill mode constraints shared by the runtime and the reader."""
    policy = experiment.fills
    if policy is None:
        return
    require(type(policy) is FillPolicy, "fill policy")
    require(type(experiment.layout) is int and experiment.layout in (2, 3),
            "fill checks require aggregate output layout")
    require(not experiment.controls, "controls are not supported with fill checks (deferred)")
    require(policy.kind in experiment.kinds, "fill trigger kind must be a declared episode kind")


def check_spec(spec, entity):
    require(type(spec) is FillSpec and type(spec.sources) is tuple and type(spec.units) is tuple
            and len(spec.sources) == len(spec.units) == len(entity.legs), "fill spec shape")
    require(all(source in FILL_SOURCES for source in spec.sources), "fill ladder source")
    require(all(type(unit) is int and unit > 0 for unit in spec.units), "fill units")


def sells(sources):
    """Per leg: ``True`` when it sells (walks bids), from its fill source."""
    return tuple(source in SELL_SOURCES for source in sources)


def reached(best, kill, sell):
    """Whether best walked price ``best`` has reached kill price ``kill``."""
    return kill is not None and (best <= kill if sell else best >= kill)


def tighter(current, kill, sell):
    """The effective kill price of two: the first one a move away from the fill reaches."""
    if current is None:
        return kill
    if kill is None:
        return current
    return max(current, kill) if sell else min(current, kill)


# Bounds one leg's ``end_books`` entry; a longer book reason kind fails the attempt.
MAX_END_REASON = 128
END_BOOK_LINE = 192 + 4 * MAX_END_REASON


def end_books(sources, views):
    """Each leg's book as the fill ended: why a fill stopped, without the book.

    Per leg: the view's ``validity`` and its reason ``kind`` (a disconnected,
    gapped or reset book is not ``usable``), whether the book is self-crossed,
    and the best level of the walked ladder (``None`` when that side is empty
    or the book is not usable).
    """
    rows = []
    for source, view in zip(sources, views):
        kind = view.reason.get("kind") if type(view.reason) is dict else None
        require(kind is None or (type(kind) is str and len(kind) <= MAX_END_REASON),
                "fill end book reason")
        ladder = view.ladders.get(source) if view.validity == "usable" else None
        rows.append({"validity": view.validity, "reason": kind, "crossed": view.crossed,
                     "best": _level(ladder[0]) if ladder else None})
    return rows


class Priced:
    """One pricing at one committed instant: the fill state and, when live, the fill."""

    __slots__ = ("state", "kill", "json")

    def __init__(self, state, kill, json):
        self.state, self.kill, self.json = state, kill, json


def _level(level):
    return None if level is None else [str(level[0]), str(level[1])]


def _pairs(levels):
    return [[str(price), str(quantity)] for price, quantity in levels]


def _optional(value):
    return None if value is None else str(value)


def walk_sizing(sizing, ladders, units, value, max_levels):
    """The one ``walk_basket`` call that prices ``sizing``."""
    if sizing.mode == "target":
        (result,) = walk_basket(ladders, units, targets=(sizing.target_steps,), value=value,
                                max_levels=max_levels)
    else:
        (result,) = walk_basket(ladders, units, value=value, edge=True, max_levels=max_levels)
    return result


def assess(sizing, result, value, maxima, sell):
    """``(kill prices or None, no-fill reason or None)`` for one priced sizing.

    A target is short unless the full target is served; an edge walk is short
    when depth (not value) left it at zero steps. A sizing that took steps is
    *positive* when its value is a known positive integer; only positive
    sizings get kill prices. A sizing is tradeable (reason ``None``) when it is
    not short, it is positive, and no leg's best walked price has already
    reached its kill price. ``sell[i]`` says leg ``i`` sells.
    """
    short = (result.steps < sizing.target_steps if sizing.mode == "target"
             else not result.steps and result.stop != "edge")
    if short and sizing.role == GOVERNS:
        return None, FILL_DEPTH_SHORT  # no fill opens: its kill prices are never written
    kills = None
    if result.steps and result.value is not None and result.value > 0:
        kills = tuple(kill_price(value, result.steps, result.legs, leg, maxima[leg], sell[leg])
                      for leg in range(len(result.legs)))
    if short:
        return kills, FILL_DEPTH_SHORT
    if result.steps and result.value is None:
        return kills, FILL_VALUE_UNKNOWN
    if kills is None or any(reached(before[0], kill, side)
                            for kill, before, side in zip(kills, result.before, sell)):
        return kills, FILL_NONPOSITIVE
    return kills, None


def _row(sizing, result, kills, reason):
    return {
        "name": sizing.name, "role": sizing.role, "mode": sizing.mode,
        "steps": str(result.steps), "stop": result.stop, "value": _optional(result.value),
        "legs": [{"atoms": str(leg.filled_atoms), "cost": str(leg.cost),
                  "taken": _pairs(leg.taken), "consumed": _pairs(leg.consumed)}
                 for leg in result.legs],
        "before": [_level(level) for level in result.before],
        "after": [_level(level) for level in result.after],
        "impact_ppm": [_optional(impact) for impact in result.impact_ppm],
        "beyond": [_pairs(levels) for levels in result.beyond],
        "tradeable": reason is None,
        "kill_prices": None if kills is None else [_optional(kill) for kill in kills],
    }


def price(policy, spec, ladders, maxima, value):
    """Price the governing sizings; when all are tradeable, open with the records too.

    The fill is live only when every governing sizing is tradeable; otherwise
    the time takes the first no-fill reason of ``NO_FILL_PRECEDENCE`` that any
    governing sizing has, and recording sizings are not priced. A live fill's
    effective kill price per leg is the tightest over governing sizings: the
    lowest for a bought leg, the highest for a sold one (``None`` when none of
    them can be killed on that leg). Recording sizings are priced
    once, written whatever their value, and carry kill prices only when
    positive; they never open, end or block a fill.
    """
    sell = sells(spec.sources)
    governing, reasons = {}, set()
    for index, sizing in enumerate(policy.sizings):
        if sizing.role == GOVERNS:
            result = walk_sizing(sizing, ladders, spec.units, value, policy.max_levels)
            kills, reason = assess(sizing, result, value, maxima, sell)
            governing[index] = (result, kills, reason)
            if reason is not None:
                reasons.add(reason)
    if reasons:
        return Priced(next(r for r in NO_FILL_PRECEDENCE if r in reasons), None, None)
    effective = [None] * len(ladders)
    rows = []
    for index, sizing in enumerate(policy.sizings):
        if index in governing:
            result, kills, reason = governing[index]
            effective = [tighter(current, kill, side)
                         for current, kill, side in zip(effective, kills, sell)]
        else:
            result = walk_sizing(sizing, ladders, spec.units, value, policy.max_levels)
            kills, reason = assess(sizing, result, value, maxima, sell)
        rows.append(_row(sizing, result, kills, reason))
    kill = tuple(effective)
    return Priced(FILL_LIVE, kill, {
        "sources": list(spec.sources), "units": [str(unit) for unit in spec.units],
        "results": rows, "kill_prices": [_optional(k) for k in kill],
        "kill_leg": None, "kill_best": None, "end_books": None})


def crossed(kill, sources, views):
    """First leg whose best walked price has reached its kill price, with that level."""
    for leg, (limit, source, view) in enumerate(zip(kill, sources, views)):
        if limit is not None:
            ladder = view.ladders[source]
            if ladder and reached(ladder[0][0], limit, source in SELL_SOURCES):
                return leg, ladder[0]
    return None
