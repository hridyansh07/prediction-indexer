"""All-BUY complete sets across one venue's markets, on the shared economic SDK."""

from replay.strategies.same_venue_complement.contract import reason
from replay.strategies.cross_venue_arbitrage.contract import asset_row, assess_net, native_amount, net_of
from replay.economic_sdk import (BookRequirement, Requirements, Observation, factory,
                                 EVALUATED, UNUSABLE, ONE_SIDED, DEPTH_LIMITED)
from .contract import ACCOUNT, SCALE, SETTLEMENT, TRIGGER_SIZE, UNIT, Inputs
from .output import MultiMarketReader, validate_content


class SameVenueMultiMarket(MultiMarketReader):
    name = "same_venue_multi_market"

    def __init__(self, config):
        self.inputs = Inputs(config)
        self.snapshot = self.inputs.snapshot
        self.snapshot_sha256 = self.inputs.prepared.sha256
        super().__init__(self.inputs.policy, self.inputs.identity, self.inputs.bridge.semantic_config,
                         self.snapshot, self.inputs.bridge)

    def bind(self, initial):
        self.inputs.prepared.bind(initial)
        self.inputs.prepared.snapshot = None

    @property
    def bound(self):
        return self.inputs.prepared.bound

    def requirements(self, snapshot, policy):
        # The trigger reads one contract; fill checks walk each leg's full ask ladder.
        size = (int(TRIGGER_SIZE),)
        return Requirements({key: (
            BookRequirement(("bid",), size, True, ("kalshi_complement_ask",), ("kalshi_complement_ask",))
            if plan["venue"] == "kalshi" else BookRequirement(("bid", "ask"), size, ladders=("ask",)))
            for key, plan in self.plans.items()}, self.experiment.profile)

    def evaluate(self, entity, views, context):
        """The trigger: one complete set bought at the best asks, net of exact fees."""
        d = entity.descriptor
        size = int(d["size_contracts"])
        acquired = tuple((leg["instrument"], leg["orientation"]) for leg in d["legs"])
        if any(v.validity != "usable" for v in views):
            return Observation(UNUSABLE, tuple(reason({"leg": i, "validity": v.validity,
                               "kind": v.reason["kind"] if v.reason else "not_initialized"})
                               for i, v in enumerate(views) if v.validity != "usable"), context_free=True)
        crossed = tuple(reason({"leg": i, "kind": "self_crossed"}) for i, v in enumerate(views) if v.crossed)
        if crossed:
            return Observation("SELF_CROSSED_LEG", crossed, context_free=True)
        kalshi = tuple(k[0].startswith("kalshi:") for k in acquired)
        if any(not v.present("bid" if k else "ask") for k, v in zip(kalshi, views)):
            return Observation(ONE_SIDED, context_free=True)
        fills = tuple(v.transformed["kalshi_complement_ask"][size] if k else v.fills["ask"][size]
                      for k, v in zip(kalshi, views))
        if any(f.depth_limited for f in fills):
            return Observation(DEPTH_LIMITED, context_free=True)
        economics = tuple(self.bridge.economics(k) for k in acquired)
        missing = tuple(reason({"leg": i, "kind": "missing_economics_or_asset"})
                        for i, e in enumerate(economics) if e is None)
        if missing:
            return Observation("ECONOMICS_UNKNOWN", missing, context_free=True)
        if len({e.quote for e in economics}) != 1:
            return Observation("ECONOMICS_UNKNOWN", (reason({"kind": "mixed_quote_assets"}),),
                               context_free=True)
        rows, fee_legs = [], []
        for key, leg, fill, e in zip(acquired, d["legs"], fills, economics):
            rows.append({"outcome_asset": asset_row(e.outcome),
                         "cost_e36": str(native_amount(fill.cost, e.price_scale + e.quantity_scale)),
                         "quote_delta_e36": None, "received_e36": None, "charges": [], "assessment_ids": []})
            fee_legs.append({"market_id": leg["market_id"], "key": key, "fill": fill,
                             "price_scale": e.price_scale, "quantity_scale": e.quantity_scale, "side": "BUY"})
        gross = size * UNIT - sum(int(row["cost_e36"]) for row in rows)
        payload = {"gap_gross": str(gross), "gap_net": None, "gross_scale": SCALE, "net_scale": SCALE,
                   "payout_floor_gross_e36": str(size * UNIT), "payout_floor_net_e36": None,
                   "native_legs": rows, "quote_asset": asset_row(economics[0].quote),
                   "fee_status": "SKIPPED", "assumptions": [], "evidence": [],
                   "settlement_model": SETTLEMENT, "outcomes_provider": d["outcomes_provider"]}
        quotes = tuple(f.consumed for f in fills)
        if gross <= 0:
            return Observation(EVALUATED, value_class="GROSS_NONPOSITIVE", payload=payload,
                               quotes=quotes, context_free=True)
        legs, unknown, assumptions, evidence = assess_net(
            self.bridge, experiment=context.experiment_sha256, scope=context.scope, basket=d,
            size=size, time=context.time, sequence=context.sequence, fee_legs=fee_legs, account=ACCOUNT)
        for row, leg in zip(rows, legs):
            row["assessment_ids"], row["charges"] = leg["assessment_ids"], leg["charges"]
            if leg["cash"] is not None:
                row["quote_delta_e36"], row["received_e36"] = str(leg["cash"]), str(leg["received"])
        net = None
        if not unknown:
            payload["payout_floor_net_e36"] = str(min(leg["received"] for leg in legs))
            net = net_of(legs)
            payload["gap_net"] = str(net)
        payload.update(fee_status="UNKNOWN" if unknown else "KNOWN",
                       assumptions=sorted(assumptions), evidence=sorted(evidence))
        eligible = net is not None and net > 0 and net >= size * self.threshold * 10 ** 18
        value_class = "FEE_UNKNOWN" if unknown else "NET_POSITIVE" if eligible else "NET_NONPOSITIVE"
        return Observation(EVALUATED, tuple(reason({"kind": "fee", "detail": r}) for r in sorted(set(unknown))),
                           value_class, predicates=frozenset({"gross", "net"} if eligible else {"gross"}),
                           payload=payload, quotes=quotes)

    def manifest(self, files, instantaneous):
        return {"version": 1, "strategy": self.experiment.strategy,
                "snapshot_sha256": self.inputs.prepared.sha256, "policy": self.policy,
                "policy_sha256": self.experiment.policy_sha256,
                "experiment_sha256": self.experiment.experiment_sha256,
                "fee_config": self.bridge.semantic_config, "fee_engine_identity": self.bridge.engine_identity,
                "files": files, "instantaneous_positive": instantaneous,
                "settlement_model": SETTLEMENT,
                "outcomes_provider": self.snapshot.get("outcomes", {}).get("provider")}

    def validate(self, directory, snapshot, manifest):
        from replay.streams.protocol import require
        require(self.bound, "missing initial")
        return validate_content(directory, snapshot, manifest, self.bridge)


build = factory(SameVenueMultiMarket)
