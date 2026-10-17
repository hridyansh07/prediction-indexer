"""Two-leg taker complete-set census on the shared economic SDK."""

from .contract import (Inputs, SCALE, UNIT, SETTLEMENT, assess_net, native_amount,
                                         net_of, sizes)
from .output import CrossVenueReader, validate_content
from replay.strategies._shared.metadata import native_metadata
from replay.strategies.same_venue_complement.contract import reason
from replay.economic_sdk.storage import stored_manifest
from replay.economic_sdk import (BookRequirement, Requirements, Observation, factory,
                                 EVALUATED, UNUSABLE, ONE_SIDED, DEPTH_LIMITED)


class CrossVenueArbitrage(CrossVenueReader):
    name = "cross_venue_arbitrage"

    def __init__(self, config):
        self.inputs = Inputs(config)
        self.snapshot = self.inputs.snapshot
        self.snapshot_sha256 = self.inputs.prepared.sha256
        super().__init__(self.inputs.policy, self.inputs.identity, self.inputs.valuation,
                         self.inputs.bridge.semantic_config, self.snapshot, self.inputs.bridge)
        self.sizes = tuple(int(s) for s in sizes(self.policy))

    def bind(self, initial):
        self.inputs.prepared.bind(initial)
        self.inputs.prepared.snapshot = None

    @property
    def bound(self):
        return self.inputs.prepared.bound

    def requirements(self, snapshot, policy):
        # Fill checks walk each leg's full ask ladder (Kalshi: the projected bids).
        fills = self.experiment.fills is not None
        probes = self.probe_decision_evidence
        return Requirements({key: (
            BookRequirement(("bid", "ask") if probes else ("bid",), self.sizes, True,
                            ("kalshi_complement_ask", "kalshi_complement_bid") if probes else ("kalshi_complement_ask",),
                            ("kalshi_complement_ask", "kalshi_complement_bid") if probes else
                            ("kalshi_complement_ask",) if fills else ())
            if plan["venue"] == "kalshi" else
            BookRequirement(("bid", "ask"), self.sizes, ladders=("ask", "bid") if probes else ("ask",) if fills else ()))
            for key, plan in self.plans.items()}, self.experiment.profile)

    def evaluate(self, entity, views, context):
        d = entity.descriptor
        metadata = entity.metadata
        size = metadata.size if metadata is not None else int(d["size_contracts"])
        if any(v.validity != "usable" for v in views):
            return Observation(UNUSABLE, tuple(reason({"leg": i, "validity": v.validity,
                              "kind": v.reason["kind"] if v.reason else "not_initialized"})
                              for i, v in enumerate(views) if v.validity != "usable"), context_free=True)
        crossed = tuple(reason({"leg": i, "kind": "self_crossed"})
                        for i, v in enumerate(views) if v.crossed)
        if crossed:
            return Observation("SELF_CROSSED_LEG", crossed, context_free=True)
        kalshi = (metadata.kalshi if metadata is not None else
                  tuple(leg["instrument"].startswith("kalshi:") for leg in d["legs"]))
        fills = tuple(v.transformed["kalshi_complement_ask"][size]
                      if k else v.fills["ask"][size] for k, v in zip(kalshi, views))
        if any(not v.present("bid" if k else "ask") for k, v in zip(kalshi, views)):
            return Observation(ONE_SIDED, context_free=True)
        if any(f.depth_limited for f in fills):
            return Observation(DEPTH_LIMITED, context_free=True)
        metadata = native_metadata(self, entity)
        economics = metadata.economics
        if metadata.missing_economics:
            return Observation("ECONOMICS_UNKNOWN", metadata.missing_economics, context_free=True)
        if metadata.missing_valuation:
            return Observation("VALUATION_UNKNOWN", tuple(reason({
                "leg": i, "kind": "missing_valuation", "asset": metadata.quote_assets[i]})
                for i in metadata.missing_valuation), context_free=True)
        rows, fee_legs = [], []
        for quote, outcome, leg, fill, e in zip(metadata.quote_assets, metadata.outcome_assets,
                                               metadata.fee_legs, fills, economics):
            cost = native_amount(fill.cost, e.price_scale + e.quantity_scale)
            rows.append({"quote_asset": dict(quote), "outcome_asset": dict(outcome),
                         "cost_e36": str(cost), "quote_delta_e36": None,
                         "received_e36": None, "charges": [], "assessment_ids": []})
            fee_legs.append(dict(leg._asdict(), fill=fill))
        gross = size * UNIT - sum(int(row["cost_e36"]) for row in rows)
        payload = {"gap_gross": str(gross), "gap_net": None,
                   "gross_scale": SCALE, "net_scale": SCALE,
                   "payout_floor_gross_e36": metadata.payout, "payout_floor_net_e36": None,
                   "native_legs": rows, "fee_status": "SKIPPED", "assumptions": [], "evidence": [],
                   "settlement_model": SETTLEMENT, "outcomes_provider": d["outcomes_provider"],
                   "valuation": self.valuation}
        quotes = tuple(f.consumed for f in fills)
        if gross <= 0:
            return Observation(EVALUATED, value_class="GROSS_NONPOSITIVE", payload=payload,
                               quotes=quotes, context_free=True)
        legs, unknown, assumptions, evidence = assess_net(
            self.bridge, experiment=context.experiment_sha256, scope=context.scope, basket=metadata.basket,
            size=size, time=context.time, sequence=context.sequence, fee_legs=fee_legs)
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
        vc = "FEE_UNKNOWN" if unknown else "NET_POSITIVE" if eligible else "NET_NONPOSITIVE"
        return Observation(EVALUATED, tuple(reason({"kind": "fee", "detail": r}) for r in sorted(set(unknown))),
                           vc, predicates=frozenset({"gross", "net"} if eligible else {"gross"}),
                           payload=payload, quotes=quotes)

    def manifest(self, files, instantaneous):
        return stored_manifest({"version": 2, "strategy": self.experiment.strategy,
                "snapshot_sha256": self.inputs.prepared.sha256, "policy": self.policy,
                "policy_sha256": self.experiment.policy_sha256,
                "experiment_sha256": self.experiment.experiment_sha256,
                "fee_config": self.bridge.semantic_config, "fee_engine_identity": self.bridge.engine_identity,
                "files": files, "instantaneous_positive": instantaneous,
                "settlement_model": SETTLEMENT,
                "outcomes_provider": self.snapshot.get("outcomes", {}).get("provider"),
                "valuation": self.valuation}, self.inputs.storage, 3)

    def validate(self, directory, snapshot, manifest):
        from replay.streams.protocol import require
        require(self.bound, "missing initial")
        return validate_content(directory, snapshot, manifest, self.bridge)


build = factory(CrossVenueArbitrage)
