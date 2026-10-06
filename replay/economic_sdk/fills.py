"""Fill checks: one priced basket fill per episode, with per-leg kill prices.

SDK spec §13. In fill mode the strategy's pure ``evaluate`` stays the cheap
trigger: it decides from best prices whether an opportunity exists. When the
trigger kind is active and the entity has no live fill, the runtime prices one
fill with ``walk_basket`` against the ladders the views already hold at that
committed instant, computes one kill price per leg, and never re-walks it. The
fill ends at the first committed update where a leg's best walked price is at
or above its kill price, or when the trigger turns false.

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
from replay.economic_sdk.types import TRANSFORMS, FillPolicy, FillSpec
from replay.streams.protocol import obj, require

FILL_LIVE, FILL_NONPOSITIVE, FILL_VALUE_UNKNOWN = (
    "FILL_LIVE", "FILL_NONPOSITIVE", "FILL_VALUE_UNKNOWN")
# Closed partition of trigger-positive time (denominator ``fill_ns`` keys).
FILL_STATES = (FILL_LIVE, FILL_NONPOSITIVE, FILL_VALUE_UNKNOWN)
KILL_PRICE = "KILL_PRICE"
# V1 fills buy: an ascending ask ladder, or a transform that projects to asks.
FILL_SOURCES = ("ask",) + TRANSFORMS
MAX_TARGETS = 8
_DECIMAL = re.compile(r"(0|[1-9][0-9]*)(\.[0-9]*[1-9])?")


def contracts(text):
    """Exact positive contract count from a canonical decimal string."""
    require(type(text) is str and _DECIMAL.fullmatch(text) is not None,
            "fill contracts must be canonical decimal strings")
    value = Fraction(text)
    require(value > 0, "fill contracts must be positive")
    return value


def fill_policy(value, kind):
    """Parse the closed fill policy; ``kind`` is the strategy's trigger kind.

    ``{version: 1, targets_contracts: [..], edge, step_contracts, max_levels}``.
    Every target must be a whole number of steps; targets are sorted and
    unique by size; at least one target or the edge walk is required.
    """
    value = obj(value, "version targets_contracts edge step_contracts max_levels")
    require(type(value["version"]) is int and value["version"] == 1, "fill policy version")
    step = contracts(value["step_contracts"])
    targets = value["targets_contracts"]
    require(type(targets) is list and len(targets) <= MAX_TARGETS, "fill targets")
    steps = []
    for target in targets:
        ratio = contracts(target) / step
        require(ratio.denominator == 1, "fill target must be a whole number of steps")
        steps.append(int(ratio))
    require(steps == sorted(set(steps)), "fill targets must be sorted and unique")
    require(type(value["edge"]) is bool and (steps or value["edge"]),
            "fill policy needs a target or the edge walk")
    levels = value["max_levels"]
    require(type(levels) is int and 1 <= levels <= bounds.MAX_CONSUMED_LEVELS, "fill max_levels")
    require(type(kind) is str and kind, "fill trigger kind")
    return FillPolicy(kind, tuple(targets), tuple(steps), value["edge"], value["step_contracts"],
                      levels)


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
    require(experiment.layout == 2, "fill checks require output layout 2")
    require(not experiment.controls, "controls are not supported with fill checks (deferred)")
    require(policy.kind in experiment.kinds, "fill trigger kind must be a declared episode kind")


def check_spec(spec, entity):
    require(type(spec) is FillSpec and type(spec.sources) is tuple and type(spec.units) is tuple
            and len(spec.sources) == len(spec.units) == len(entity.legs), "fill spec shape")
    require(all(source in FILL_SOURCES for source in spec.sources), "fill ladder source")
    require(all(type(unit) is int and unit > 0 for unit in spec.units), "fill units")


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


def price(policy, spec, ladders, maxima, value):
    """Price every configured sizing once, on ``ladders``, and derive kill prices.

    A sizing is *positive* when it took steps and its value is a known
    positive integer; its kill prices are then computed per leg. It is
    *tradeable* when, in addition, no leg's best walked price is already at or
    above its kill price. The fill is live when any sizing is tradeable; its
    effective kill price per leg is the lowest over tradeable sizings (``None``
    when no tradeable sizing can be killed on that leg). Otherwise the time is
    ``FILL_VALUE_UNKNOWN`` when any sizing with steps has an unknown value, or
    ``FILL_NONPOSITIVE``.
    """
    results = walk_basket(ladders, spec.units, targets=policy.target_steps, value=value,
                          edge=policy.edge, max_levels=policy.max_levels)
    modes = [("target", str(steps)) for steps in policy.target_steps]
    if policy.edge:
        modes.append(("edge", None))
    effective = [None] * len(ladders)
    live = unknown = False
    rows = []
    for (mode, target), result in zip(modes, results):
        if result.steps and result.value is None:
            unknown = True
        kills, tradeable = None, False
        if result.steps and result.value is not None and result.value > 0:
            kills = tuple(kill_price(value, result.steps, result.legs, leg, maxima[leg])
                          for leg in range(len(ladders)))
            tradeable = all(kill is None or before[0] < kill
                            for kill, before in zip(kills, result.before))
        if tradeable:
            live = True
            effective = [kill if current is None else current if kill is None else min(current, kill)
                         for current, kill in zip(effective, kills)]
        rows.append({
            "mode": mode, "target_steps": target, "steps": str(result.steps), "stop": result.stop,
            "value": _optional(result.value),
            "legs": [{"atoms": str(leg.filled_atoms), "cost": str(leg.cost),
                      "taken": _pairs(leg.taken), "consumed": _pairs(leg.consumed)}
                     for leg in result.legs],
            "before": [_level(level) for level in result.before],
            "after": [_level(level) for level in result.after],
            "impact_ppm": [_optional(impact) for impact in result.impact_ppm],
            "tradeable": tradeable,
            "kill_prices": None if kills is None else [_optional(kill) for kill in kills],
        })
    if not live:
        return Priced(FILL_VALUE_UNKNOWN if unknown else FILL_NONPOSITIVE, None, None)
    kill = tuple(effective)
    return Priced(FILL_LIVE, kill, {
        "sources": list(spec.sources), "units": [str(unit) for unit in spec.units],
        "results": rows, "kill_prices": [_optional(k) for k in kill],
        "kill_leg": None, "kill_best": None})


def crossed(kill, sources, views):
    """First leg whose best walked price is at or above its kill price, with that level."""
    for leg, (limit, source, view) in enumerate(zip(kill, sources, views)):
        if limit is not None:
            ladder = view.ladders[source]
            if ladder and ladder[0][0] >= limit:
                return leg, ladder[0]
    return None
