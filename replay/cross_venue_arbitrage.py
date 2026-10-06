"""Two-leg taker complete-set census on the shared economic SDK."""

from replay.cross_venue_contract import Inputs, SCALE, UNIT, asset_row, SETTLEMENT
from replay.cross_venue_output import CrossVenueReader, validate_content
from replay.complement_contract import reason
from replay.complement_fees import FeeEconomicsUnavailable
from replay.economic_sdk import (BookRequirement, Requirements, Observation, factory,
                                 EVALUATED, UNUSABLE, ONE_SIDED, DEPTH_LIMITED)


def native_amount(atoms, scale):
    return atoms * 10 ** (SCALE - scale)


class CrossVenueArbitrage(CrossVenueReader):
    name = "cross_venue_arbitrage"

    def __init__(self, config):
        self.inputs = Inputs(config)
        self.snapshot = self.inputs.snapshot
        self.snapshot_sha256 = self.inputs.prepared.sha256
        super().__init__(self.inputs.policy, self.inputs.identity, self.inputs.valuation,
                         self.inputs.bridge.semantic_config, self.snapshot)
        self.bridge = self.inputs.bridge
        self.sizes = tuple(int(s) for s in self.policy["sizes_contracts"])

    def bind(self, initial):
        self.inputs.prepared.bind(initial)
        self.inputs.prepared.snapshot = None

    @property
    def bound(self):
        return self.inputs.prepared.bound

    def requirements(self, snapshot, policy):
        return Requirements({key: (
            BookRequirement(("bid",), self.sizes, True, ("kalshi_complement_ask",))
            if plan["venue"] == "kalshi" else BookRequirement(("bid", "ask"), self.sizes))
            for key, plan in self.plans.items()}, self.experiment.profile)

    def evaluate(self, entity, views, context):
        d = entity.descriptor
        size = int(d["size_contracts"])
        acquired = tuple((leg["instrument"], leg["orientation"]) for leg in d["legs"])
        if any(v.validity != "usable" for v in views):
            return Observation(UNUSABLE, tuple(reason({"leg": i, "validity": v.validity,
                              "kind": v.reason["kind"] if v.reason else "not_initialized"})
                              for i, v in enumerate(views) if v.validity != "usable"), context_free=True)
        crossed = tuple(reason({"leg": i, "kind": "self_crossed"})
                        for i, v in enumerate(views) if v.crossed)
        if crossed:
            return Observation("SELF_CROSSED_LEG", crossed, context_free=True)
        fills = tuple(v.transformed["kalshi_complement_ask"][size]
                      if k[0].startswith("kalshi:") else v.fills["ask"][size]
                      for k, v in zip(acquired, views))
        if any(not v.present("bid" if k[0].startswith("kalshi:") else "ask")
               for k, v in zip(acquired, views)):
            return Observation(ONE_SIDED, context_free=True)
        if any(f.depth_limited for f in fills):
            return Observation(DEPTH_LIMITED, context_free=True)
        economics = tuple(self.bridge.economics(k) for k in acquired)
        missing = tuple(reason({"leg": i, "kind": "missing_economics_or_asset"})
                        for i, e in enumerate(economics) if e is None)
        if missing:
            return Observation("ECONOMICS_UNKNOWN", missing, context_free=True)
        valued = set() if self.valuation is None else {
            tuple(sorted(row.items())) for row in self.valuation["assets"]}
        missing = tuple(reason({"leg": i, "kind": "missing_valuation", "asset": asset_row(e.quote)})
                        for i, e in enumerate(economics)
                        if tuple(sorted(asset_row(e.quote).items())) not in valued)
        if missing:
            return Observation("VALUATION_UNKNOWN", missing, context_free=True)
        rows, fee_legs = [], []
        for key, leg, fill, e in zip(acquired, d["legs"], fills, economics):
            cost = native_amount(fill.cost, e.price_scale + e.quantity_scale)
            rows.append({"quote_asset": asset_row(e.quote), "outcome_asset": asset_row(e.outcome),
                         "cost_e36": str(cost), "quote_delta_e36": None,
                         "received_e36": None, "charges": [], "assessment_ids": []})
            fee_legs.append({"market_id": leg["market_id"], "key": key, "fill": fill,
                             "price_scale": e.price_scale, "quantity_scale": e.quantity_scale, "side": "BUY"})
        gross = size * UNIT - sum(int(row["cost_e36"]) for row in rows)
        payload = {"gap_gross": str(gross), "gap_net": None,
                   "gross_scale": SCALE, "net_scale": SCALE,
                   "payout_floor_gross_e36": str(size * UNIT), "payout_floor_net_e36": None,
                   "native_legs": rows, "fee_status": "SKIPPED", "assumptions": [], "evidence": [],
                   "settlement_model": SETTLEMENT, "outcomes_provider": d["outcomes_provider"],
                   "valuation": self.valuation}
        quotes = tuple(f.consumed for f in fills)
        if gross <= 0:
            return Observation(EVALUATED, value_class="GROSS_NONPOSITIVE", payload=payload,
                               quotes=quotes, context_free=True)
        try:
            orders, missing = self.bridge.assess_orders(
                experiment=context.experiment_sha256, scope=context.scope, basket=d,
                direction="BUY", size=size, time=context.time, sequence=context.sequence,
                legs=tuple(fee_legs), account="cross_venue_arbitrage_v1")
        except FeeEconomicsUnavailable as error:
            orders, missing = (None,) * len(fee_legs), ("fee_economics_unavailable:" + str(error),)
        unknown, assumptions, evidence = list(missing), {"PER_LEVEL_DECLARED_PARTITION_ESTIMATE"}, set()
        for i, order in enumerate(orders):
            if order is None:
                continue
            e, results = order
            rows[i]["assessment_ids"] = [r.identity for r in results]
            cash, received = 0, 0
            known = True
            for r in results:
                assumptions.update(r.assumptions)
                evidence.add(r.evidence.value)
                unknown.extend(f"fee_sdk:{u.component.value}:{u.reason.value}" for u in r.unknowns)
                for charge in r.charges:
                    rows[i]["charges"].append({"asset": asset_row(charge.amount.asset),
                        "amount_e36": str(native_amount(charge.amount.amount.atoms, charge.amount.amount.scale)),
                        "component": charge.component.value})
                if r.net_deltas is None:
                    known = False
                    unknown.append("fee_sdk:missing_net_deltas")
                    continue
                for delta in r.net_deltas:
                    amount = native_amount(delta.atoms, delta.scale)
                    if delta.asset == e.quote:
                        cash += amount
                    elif delta.asset == e.outcome:
                        received += amount
                    else:
                        known = False
                        unknown.append("unexpected_asset_delta")
            if known:
                rows[i]["quote_delta_e36"], rows[i]["received_e36"] = str(cash), str(received)
        net = None
        if not unknown:
            floor = min(int(row["received_e36"]) for row in rows)
            payload["payout_floor_net_e36"] = str(floor)
            net = floor + sum(int(row["quote_delta_e36"]) for row in rows)
            payload["gap_net"] = str(net)
        payload.update(fee_status="UNKNOWN" if unknown else "KNOWN",
                       assumptions=sorted(assumptions), evidence=sorted(evidence))
        eligible = net is not None and net > 0 and net >= size * self.threshold * 10 ** 18
        vc = "FEE_UNKNOWN" if unknown else "NET_POSITIVE" if eligible else "NET_NONPOSITIVE"
        return Observation(EVALUATED, tuple(reason({"kind": "fee", "detail": r}) for r in sorted(set(unknown))),
                           vc, predicates=frozenset({"gross", "net"} if eligible else {"gross"}),
                           payload=payload, quotes=quotes)

    def manifest(self, files, instantaneous):
        return {"version": 2, "strategy": self.experiment.strategy,
                "snapshot_sha256": self.inputs.prepared.sha256, "policy": self.policy,
                "policy_sha256": self.experiment.policy_sha256,
                "experiment_sha256": self.experiment.experiment_sha256,
                "fee_config": self.bridge.semantic_config, "fee_engine_identity": self.bridge.engine_identity,
                "files": files, "instantaneous_positive": instantaneous,
                "settlement_model": SETTLEMENT,
                "outcomes_provider": self.snapshot.get("outcomes", {}).get("provider"),
                "valuation": self.valuation}

    def validate(self, directory, snapshot, manifest):
        from replay.streams.protocol import require
        require(self.bound, "missing initial")
        return validate_content(directory, snapshot, manifest)


build = factory(CrossVenueArbitrage)
