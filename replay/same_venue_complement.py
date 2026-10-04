"""Same-venue complement on the economic strategy SDK.

The strategy supplies only its book requirements, its structural baskets and a
pure ``evaluate``; the SDK owns time, staging, episodes, slices, controls,
bounded output and the reader. Policy version 1 reproduces the committed V1
experiment byte for byte; version 2 is the SDK-default experiment.
"""

from __future__ import annotations

from replay.complement_contract import SELF_CROSSED, Inputs
from replay.complement_output import ComplementReader, validate_content
from replay.economic_sdk import (
    DEPTH_LIMITED, EVALUATED, ONE_SIDED, UNUSABLE, BookRequirement, Observation, Requirements,
    factory,
)
from replay.preparation import encoded
from replay.streams.protocol import require

_UNEVALUATED = ("NOT_APPLICABLE", None)
_SKIPPED = {"gap_net": None, "fee_status": "SKIPPED", "assessments": [[], []],
            "reasons": [], "assumptions": [], "evidence": []}
_GROSS, _GROSS_NET = frozenset({"gross"}), frozenset({"gross", "net"})


class SameVenueComplement(ComplementReader):
    name = "same_venue_complement"

    def __init__(self, config):
        self.inputs = Inputs(config)
        ComplementReader.__init__(self, self.inputs.policy, self.inputs.experiment_sha256)
        self.snapshot = self.inputs.snapshot
        self.plans = {(p["instrument"], p["orientation"]): p for p in self.snapshot["plans"]}
        self.bridge = self.inputs.bridge
        self.sizes = tuple(int(size) for size in self.policy["sizes_contracts"])
        self.threshold = int(self.policy["minimum_net_gap_per_contract_e18"])
        self.v2 = self.policy["version"] == 2
        self.snapshot_sha256 = self.inputs.prepared.sha256

    # -- binding -----------------------------------------------------------------
    def bind(self, initial):
        self.inputs.prepared.bind(initial)
        # Binding is the final consumer of PreparedInput's decoded snapshot.
        self.inputs.prepared.snapshot = None

    @property
    def bound(self):
        return self.inputs.prepared.bound

    # -- declarations ------------------------------------------------------------
    def requirements(self, snapshot, policy):
        books = {}
        for key, plan in self.plans.items():
            if plan["venue"] == "kalshi":
                books[key] = BookRequirement(("bid",), self.sizes, True, ("kalshi_complement_ask",))
            else:
                books[key] = BookRequirement(("bid", "ask"), self.sizes)
        return Requirements(books, self.experiment.profile)

    # -- evaluation --------------------------------------------------------------
    def _reason(self, reason):
        if reason is None:
            return "null"
        # V1 formatted the encoded bytes object itself; V2 writes the JSON text.
        return encoded(reason).decode() if self.v2 else f"{encoded(reason)}"

    def evaluate(self, entity, views, context):
        desc = entity.descriptor
        size = int(desc["size_contracts"])
        venue, direction = desc["venue"], desc["direction"]
        if any(view.validity != "usable" for view in views):
            return Observation(UNUSABLE, tuple(
                f"leg:{i}:{view.validity}:{self._reason(view.reason)}"
                for i, view in enumerate(views)), fields=_UNEVALUATED, context_free=True)
        if venue == "limitless":
            view = views[0]
            bid, ask = view.fills["bid"][size], view.fills["ask"][size]
            if not view.bid_present or not view.ask_present:
                return Observation(ONE_SIDED, fields=_UNEVALUATED, context_free=True)
            if bid.depth_limited or ask.depth_limited:
                return Observation(DEPTH_LIMITED, (str(min(bid.filled_atoms, ask.filled_atoms)),),
                                   fields=_UNEVALUATED, context_free=True)
            status = "LOCKED" if bid.cost == ask.cost else ("CROSSED" if bid.cost > ask.cost else "NOT_CROSSED")
            return Observation(status, fields=_UNEVALUATED, context_free=True)
        if self.v2:
            crossed = tuple(f"leg:{i}" for i, view in enumerate(views)
                            if view.best_bid is not None and view.best_ask is not None
                            and view.best_bid[0] > view.best_ask[0])
            if crossed:
                return Observation(SELF_CROSSED, crossed, fields=_UNEVALUATED, context_free=True)
        projected = venue == "kalshi"
        if projected:
            side = "bid"
            fills = [view.transformed["kalshi_complement_ask"][size] for view in views]
        else:
            side = "ask" if direction == "long" else "bid"
            fills = [view.fills[side][size] for view in views]
        if any(not view.present(side) for view in views):
            return Observation(ONE_SIDED, fields=_UNEVALUATED, context_free=True)
        if any(fill.depth_limited for fill in fills):
            return Observation(DEPTH_LIMITED, (str(min(f.filled_atoms for f in fills)),),
                               fields=_UNEVALUATED, context_free=True)
        plan = self.plans[entity.legs[0]]
        gross_scale = int(plan["price_scale"]) + int(plan["quantity_scale"])
        unit = size * 10 ** gross_scale
        # Projected BUY costs are P-p, so U-cost is exactly bid_1+bid_2-U.
        gross = (unit - sum(f.cost for f in fills) if direction in ("long", "both_bids")
                 else sum(f.cost for f in fills) - unit)
        if gross <= 0:
            return Observation(EVALUATED, (), "GROSS_NONPOSITIVE", ("SKIPPED", None),
                               context_free=True)
        fee_direction = "SELL" if direction == "short" else "BUY"
        legs = tuple({"market_id": leg["market_id"],
                      "key": _opposite(leg) if projected else (leg["instrument"], leg["orientation"]),
                      "fill": fill, "price_scale": int(self.plans[key]["price_scale"]),
                      "quantity_scale": int(self.plans[key]["quantity_scale"]), "side": fee_direction}
                     for leg, key, fill in zip(desc["legs"], entity.legs, fills))
        assessment = self.bridge.assess(experiment=self.experiment.experiment_sha256,
                                        scope=context.scope, basket=desc, direction=fee_direction,
                                        size=size, time=context.time, sequence=context.sequence,
                                        legs=legs)
        net = assessment["gap_net"]
        net_positive = net is not None and net > 0 and net >= size * self.threshold
        if assessment["fee_status"] == "UNKNOWN":
            value = "FEE_UNKNOWN"
        else:
            value = "NET_POSITIVE" if net_positive else "NET_NONPOSITIVE"
        payload = {"gap_gross": str(gross), "gap_net": None if net is None else str(net),
                   "gross_scale": gross_scale, "net_scale": 18,
                   "fee_status": assessment["fee_status"], "assessments": assessment["assessments"],
                   "assumptions": assessment["assumptions"], "evidence": assessment["evidence"]}
        return Observation(EVALUATED, tuple(assessment["reasons"]), value,
                           (assessment["fee_status"], None),
                           _GROSS_NET if net_positive else _GROSS, payload,
                           tuple(fill.consumed for fill in fills))

    # -- completion --------------------------------------------------------------
    def manifest(self, files, instantaneous):
        return {"version": self.policy["version"], "strategy": self.experiment.strategy,
                "snapshot_sha256": self.inputs.prepared.sha256, "policy": self.policy,
                "policy_sha256": self.inputs.policy_sha256,
                "experiment_sha256": self.experiment.experiment_sha256,
                "fee_config": self.bridge.semantic_config,
                "fee_engine_identity": self.bridge.engine_identity, "files": files,
                "instantaneous_positive": instantaneous,
                "payout_assumption": "unit_complement_assumption_not_resolution_proof"}

    def validate(self, directory, snapshot, manifest):
        require(self.bound, "missing initial")
        return validate_content(directory, snapshot, manifest)


def _opposite(leg):
    orientation = "complement" if leg["orientation"] == "outcome" else "outcome"
    return (leg["instrument"], orientation)


build = factory(SameVenueComplement)
