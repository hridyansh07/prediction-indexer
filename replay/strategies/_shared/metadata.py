"""Constants owned by a resolved entity, shared by trigger and reader fill hooks.

No prices, selected terms, fee results or observation context live here. The
existing ENTITY + json_cost(descriptor) reservation covers the compact records
and their owned frozen basket; shared economics remain configuration-owned.
"""

from dataclasses import dataclass
from fractions import Fraction
from typing import NamedTuple

from replay.economic_sdk.bounds import FrozenJSON


class FeeLeg(NamedTuple):
    market_id: str
    key: tuple
    price_scale: int
    quantity_scale: int
    side: str


@dataclass(frozen=True, slots=True)
class NativeMetadata:
    dependencies: tuple
    size: int
    acquired: tuple
    kalshi: tuple
    economics: tuple
    quote_assets: tuple
    outcome_assets: tuple
    fee_legs: tuple
    missing_economics: tuple
    missing_valuation: tuple
    mixed_quotes: bool
    payout: str
    minimum_step: object
    basket: FrozenJSON
    gross_payoffs: tuple | None


def native_metadata(strategy, entity):
    # Economics/valuation can be replaced by callers. Only reuse derived
    # constants while their actual inputs still agree; never cache a fee result.
    from replay.strategies.cross_venue_arbitrage.contract import UNIT, asset_row
    from replay.strategies.same_venue_complement.contract import reason

    acquired = (entity.metadata.acquired if entity.metadata is not None else
                tuple((leg["instrument"], leg["orientation"]) for leg in entity.descriptor["legs"]))
    economics = tuple(strategy.bridge.economics(key) for key in acquired)
    quotes = (entity.metadata.quote_assets if entity.metadata is not None
              and entity.metadata.economics == economics else
              tuple(None if e is None else asset_row(e.quote) for e in economics))
    valuation = getattr(strategy, "valuation", None)
    unvalued_legs = tuple(i for i, quote in enumerate(quotes) if quote is not None
                         and (valuation is None or quote not in valuation["assets"]))
    fills = strategy.experiment.fills
    dependencies = (economics, unvalued_legs, strategy.threshold,
                    None if fills is None else fills.step_contracts)
    if entity.metadata is None or entity.metadata.dependencies != dependencies:
        d = entity.descriptor
        size = int(d["size_contracts"])
        outcomes = tuple(None if e is None else asset_row(e.outcome) for e in economics)
        missing = tuple(reason({"leg": i, "kind": "missing_economics_or_asset"})
                        for i, e in enumerate(economics) if e is None)
        legs = tuple(None if e is None else FeeLeg(
            leg["market_id"], key, e.price_scale, e.quantity_scale, "BUY")
            for key, leg, e in zip(acquired, d["legs"], economics))
        minimum = (None if fills is None else
                   Fraction(fills.step_contracts) * strategy.threshold * 10**18)
        gross = None
        if "implication" in d:
            from replay.strategies._shared.implication_cover.contract import payoff
            gross = payoff(d["implication"], (size * UNIT, size * UNIT))
        entity.metadata = NativeMetadata(
            dependencies, size, acquired, tuple(key[0].startswith("kalshi:") for key in acquired),
            economics, quotes, outcomes, legs, missing, unvalued_legs,
            len({e.quote for e in economics if e is not None}) != 1,
            str(size * UNIT), minimum, FrozenJSON(d),
            None if gross is None else (tuple(str(v) for v in gross), str(max(gross)),
                                        str(max(gross) - min(gross))))
    return entity.metadata


@dataclass(frozen=True, slots=True)
class ComplementMetadata:
    size: int
    venue: str
    direction: str
    gross_scale: int
    unit: int
    fee_legs: tuple
    basket: FrozenJSON


def complement_metadata(strategy, entity):
    if entity.metadata is None:
        d = entity.descriptor
        size, venue, direction = int(d["size_contracts"]), d["venue"], d["direction"]
        plans = tuple(strategy.plans[key] for key in entity.legs)
        scale = int(plans[0]["price_scale"]) + int(plans[0]["quantity_scale"])
        projected = venue == "kalshi"
        legs = tuple(FeeLeg(
            leg["market_id"],
            (leg["instrument"], "complement" if leg["orientation"] == "outcome" else "outcome")
                   if projected else (leg["instrument"], leg["orientation"]),
            int(plan["price_scale"]), int(plan["quantity_scale"]),
            "SELL" if direction == "short" else "BUY")
            for leg, plan in zip(d["legs"], plans))
        entity.metadata = ComplementMetadata(size, venue, direction, scale, size * 10**scale,
                                              legs, FrozenJSON(d))
    return entity.metadata
