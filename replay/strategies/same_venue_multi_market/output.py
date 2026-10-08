"""Independent same-venue multi-market reader; never imports the runtime evaluator."""

from fractions import Fraction
from pathlib import Path

from replay.strategies import canonical_reference

from replay.strategies.same_venue_complement.output import _aggregate_rows
from replay.strategies.cross_venue_arbitrage.contract import asset_row, asset_value, assess_net, economics_by_key, net_of
from replay.economic_sdk import FillSpec, Strategy, aggregate_reader
from replay.economic_sdk.bounds import MAX_METADATA, MAX_STATE
from replay.economic_sdk.fills import step_atoms
from replay.economic_sdk.output import manifest_layout
from replay.economic_sdk.reader import read_json, signed
from replay.fees import Policy
from replay.fees.domain import AccountClass, Component, Evidence, Venue
from replay.preparation import digest, encoded, load_snapshot, sha
from .contract import (
    ACCOUNT, SCALE, SETTLEMENT, STRATEGY, UNIT, Inputs, baskets, experiment, experiment_identity,
    policy_config,
)
from replay.streams.protocol import freeze, obj, require, uint

PAYLOAD = ("gap_gross gap_net gross_scale net_scale payout_floor_gross_e36 payout_floor_net_e36 "
           "native_legs quote_asset fee_status assumptions evidence settlement_model outcomes_provider")


class MultiMarketReader(Strategy):
    native_scales = True

    def __init__(self, policy, identity, fees, snapshot, bridge=None):
        self.policy = policy_config(policy)
        self.experiment = experiment(self.policy, identity)
        # Fill values price fees through the bridge in the runtime and the reader alike.
        require(bridge is not None, "same-venue multi-market fill checks require the fee catalog")
        require(bridge.semantic_config == fees, "fee bridge/manifest fee configuration")
        self.bridge = bridge
        self.threshold = int(self.policy["minimum_net_gap_per_contract_e18"])
        self.plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
        require(type(fees["assets"]) is dict, "fee assets")
        for venue, row in fees["assets"].items():
            Venue(venue)
            require(asset_value(row).kind.value != "outcome", "quote asset cannot be outcome")
        Policy(limitless_buy_bps=fees["limitless_buy_bps"], limitless_sell_bps=fees["limitless_sell_bps"],
               kalshi_member_class=AccountClass[fees["kalshi_member_class"]])
        uint(fees["reference_ns"])
        self.economics = economics_by_key(fees)
        self.provider = snapshot.get("outcomes", {}).get("provider")

    def baskets(self, snapshot, policy, scope_index):
        return baskets(snapshot, policy, scope_index)

    def extra_files(self):
        if self.experiment.profile is None:
            return ()
        from replay.economic_sdk.profile import FILES
        return FILES

    # -- fill checks -------------------------------------------------------------
    def fill_spec(self, entity):
        """Each leg buys from its ask ladder; Kalshi's is the projected opposite bids."""
        fills = self.experiment.fills
        return FillSpec(tuple("kalshi_complement_ask" if key[0].startswith("kalshi:") else "ask"
                              for key in entity.legs),
                        tuple(step_atoms(fills, self.plans[key]["quantity_scale"]) for key in entity.legs))

    def fill_value(self, entity, steps, legs, time):
        """Exact net of ``steps`` sets above the configured minimum, at scale 36.

        Fees are the trigger's single-order assessment per leg, at the fill's
        open time. ``None`` when any fee or economics input is unknown.
        """
        d = entity.descriptor
        acquired = tuple((leg["instrument"], leg["orientation"]) for leg in d["legs"])
        economics = tuple(self.bridge.economics(key) for key in acquired)
        if any(e is None for e in economics) or len({e.quote for e in economics}) != 1:
            return None
        fee_legs = tuple({"market_id": leg["market_id"], "key": key, "fill": fill,
                          "price_scale": e.price_scale, "quantity_scale": e.quantity_scale, "side": "BUY"}
                         for leg, key, fill, e in zip(d["legs"], acquired, legs, economics))
        native, unknown, _, _ = assess_net(
            self.bridge, experiment=self.experiment.experiment_sha256, scope=0, basket=d,
            size=steps, time=time, sequence=0, fee_legs=fee_legs, account=ACCOUNT)
        if unknown:
            return None
        minimum = steps * Fraction(self.experiment.fills.step_contracts) * self.threshold * 10 ** 18
        require(minimum.denominator == 1, "minimum net gap is not exact at the fill step")
        return net_of(native) - int(minimum)

    # -- episode payloads ----------------------------------------------------------
    def open_facts(self, value, entity, kind):
        obj(value, PAYLOAD)
        require(type(value["gross_scale"]) is int and value["gross_scale"] == SCALE
                and type(value["net_scale"]) is int and value["net_scale"] == SCALE, "research amount scale")
        require(value["settlement_model"] == SETTLEMENT and value["outcomes_provider"] == self.provider,
                "normal-resolution mask labels")
        for field in ("assumptions", "evidence"):
            require(type(value[field]) is list and all(type(x) is str for x in value[field])
                    and value[field] == sorted(set(value[field])), "fee labels")
        for evidence in value["evidence"]:
            Evidence(evidence)
        legs = entity.descriptor["legs"]
        size = int(entity.descriptor["size_contracts"])
        gross = signed(value["gap_gross"])
        net = None if value["gap_net"] is None else signed(value["gap_net"])
        require(gross > 0 and value["fee_status"] in {"KNOWN", "UNKNOWN"}, "positive fee-assessed payload")
        require(signed(value["payout_floor_gross_e36"]) == size * UNIT, "gross payout floor")
        require(type(value["native_legs"]) is list and len(value["native_legs"]) == len(legs), "native legs")
        keys = [(leg["instrument"], leg["orientation"]) for leg in legs]
        require(all(key in self.economics for key in keys), "native economics binding")
        quotes = {encoded(asset_row(self.economics[key].quote)) for key in keys}
        require(len(quotes) == 1 and encoded(value["quote_asset"]) in quotes, "one native quote asset")
        costs, cash, receipts = [], [], []
        for leg, desc in zip(value["native_legs"], legs):
            obj(leg, "outcome_asset cost_e36 quote_delta_e36 received_e36 charges assessment_ids")
            e = self.economics[desc["instrument"], desc["orientation"]]
            require(leg["outcome_asset"] == asset_row(e.outcome), "native asset binding")
            cost = signed(leg["cost_e36"])
            require(cost >= 0, "negative acquisition cost")
            costs.append(cost)
            require(type(leg["assessment_ids"]) is list, "assessment identities")
            for identity in leg["assessment_ids"]:
                sha(identity)
            require(type(leg["charges"]) is list, "native charges")
            quote_fee, token_fee = 0, 0
            for charge in leg["charges"]:
                obj(charge, "asset amount_e36 component")
                asset_value(charge["asset"])
                Component(charge["component"])
                amount = signed(charge["amount_e36"])
                require(amount >= 0, "nonnegative fee")
                if charge["asset"] == value["quote_asset"]:
                    quote_fee += amount
                elif charge["asset"] == leg["outcome_asset"]:
                    token_fee += amount
                else:
                    require(value["fee_status"] == "UNKNOWN", "unexpected fee asset")
            require((leg["quote_delta_e36"] is None) == (leg["received_e36"] is None), "partial native net")
            if leg["quote_delta_e36"] is not None:
                c, r = signed(leg["quote_delta_e36"]), signed(leg["received_e36"])
                require(c == -cost - quote_fee and r == size * UNIT - token_fee and r >= 0,
                        "native fee/cashflow arithmetic")
                cash.append(c)
                receipts.append(r)
            if value["fee_status"] == "KNOWN":
                require(leg["quote_delta_e36"] is not None and bool(leg["assessment_ids"]),
                        "known native assessment")
        require(gross == size * UNIT - sum(costs), "gross native cashflow arithmetic")
        if value["fee_status"] == "KNOWN":
            # Exactly one leg pays in each outcome, so the guaranteed payout is the minimum receipt.
            require(len(receipts) == len(legs) and signed(value["payout_floor_net_e36"]) == min(receipts),
                    "net outcome payout floor")
            require(net == sum(cash) + min(receipts) and net <= gross, "net native cashflow arithmetic")
        else:
            require(net is None and value["payout_floor_net_e36"] is None, "unknown net payout")
        require(kind != "net" or net is not None and net > 0
                and net >= size * self.threshold * 10 ** 18, "net payload eligibility")
        return gross, net

    def check_open(self, values, facts, entity, kind, opening_class, where):
        gross, net = facts
        eligible = (net is not None and net > 0
                    and net >= int(entity.descriptor["size_contracts"]) * self.threshold * 10 ** 18)
        expected = "FEE_UNKNOWN" if net is None else "NET_POSITIVE" if eligible else "NET_NONPOSITIVE"
        require(opening_class == expected, "fee class/net eligibility")
        require(gross > 0 and (kind != "net" or eligible), "episode predicate")

    def check_quotes(self, consumed, values, entity):
        legs = entity.descriptor["legs"]
        require(type(consumed) is list and len(consumed) == len(legs), "consumed legs")
        size = int(entity.descriptor["size_contracts"])
        for quotes, desc, native, source in zip(consumed, legs, values["native_legs"], entity.legs):
            plan = self.plans[source]
            ps, qs = int(plan["price_scale"]), int(plan["quantity_scale"])
            remaining, previous, cost = size * 10 ** qs, None, 0
            require(type(quotes) is list, "consumed quotes")
            for level in quotes:
                require(type(level) is list and len(level) == 2, "consumed level")
                price, quantity = uint(level[0]), uint(level[1])
                require(remaining > 0 and quantity > 0 and price <= 10 ** ps, "consumed depth/price")
                require(previous is None or price > previous, "BUY price order")
                taken = min(remaining, quantity)
                cost += taken * price
                remaining -= taken
                previous = price
            require(remaining == 0, "insufficient consumed depth")
            require(signed(native["cost_e36"]) == cost * 10 ** (SCALE - ps - qs), "consumed/native cost arithmetic")

    def summary_key(self, entity):
        d = entity.descriptor
        return d["venue"], d["basket_kind"], d["direction"] + "@" + d["set_id"], d["size_contracts"], None

    def summarize_aggregate(self, aggregates, manifest, snapshot):
        rows = _aggregate_rows(aggregates, aggregates.groups[""], self.policy["headline_latency_ns"],
                               size_order=lambda size: 0 if size is None else int(size))
        for row in rows:
            row["direction"], row["set_id"] = row["direction"].split("@")
        event_id = snapshot.get("outcomes", {}).get("document", {}).get("event_id")
        result = {"version": 1, "strategy": STRATEGY, "bundle_id": snapshot["config"]["bundle_id"],
                  "event_id": event_id, "scope_count": len(snapshot["scopes"]),
                  "requested_ns": str(int(snapshot["config"]["end_ns"]) - int(snapshot["config"]["start_ns"])),
                  "time_unit": "basket_direction_nanoseconds",
                  "snapshot_sha256": manifest["snapshot_sha256"], "policy_sha256": manifest["policy_sha256"],
                  "experiment_sha256": manifest["experiment_sha256"],
                  "settlement_model": SETTLEMENT, "outcomes_provider": self.provider,
                  "history_complete": snapshot["history_complete"], "vendor_completeness": "NOT_PROVEN",
                  "instantaneous_positive": manifest["instantaneous_positive"], "reasons": aggregates.reasons,
                  "rows": rows,
                  "qualifiers": ["DETECTED_NOT_EXECUTED", "RETROSPECTIVE_DISPLAYED_DEPTH_SURVIVAL",
                                 "NO_SETTLEMENT_COMPATIBILITY_PROOF", "NO_MERGE_OR_REDEMPTION_ASSUMED",
                                 "PINNED_FEE_ESTIMATE"]}
        if self.experiment.profile is not None:
            result["profile"] = self.profile_summary
        require(len(encoded(result)) <= MAX_METADATA and aggregates.budget.used + len(encoded(result)) <= MAX_STATE,
                "summary budget")
        return result


def check_manifest(value, snapshot, complete):
    fields = ("version strategy snapshot_sha256 policy policy_sha256 experiment_sha256 fee_config "
              "fee_engine_identity files instantaneous_positive settlement_model outcomes_provider")
    manifest_layout(value)
    if "layout" in value:
        fields += " layout"
    obj(value, fields + (" summary_sha256" if complete else ""))
    require(type(value["version"]) is int and value["version"] == 1 and value["strategy"] == STRATEGY,
            "manifest version/strategy")
    policy_config(value["policy"])
    require(value["settlement_model"] == SETTLEMENT
            and value["outcomes_provider"] == snapshot.get("outcomes", {}).get("provider"),
            "manifest settlement/provider")
    for field in ("snapshot_sha256", "policy_sha256", "experiment_sha256", "fee_engine_identity"):
        sha(value[field])
    if complete:
        sha(value["summary_sha256"])
    obj(value["fee_config"], "catalog_identity reference_ns limitless_buy_bps limitless_sell_bps "
                            "kalshi_member_class assets instrument_bindings")
    require(value["policy_sha256"] == digest(value["policy"]), "policy identity")
    require(value["experiment_sha256"] == experiment_identity(value["snapshot_sha256"], value["policy"],
                                                              value["fee_config"]), "experiment identity")
    require(type(value["instantaneous_positive"]) is dict, "instantaneous positive map")
    for key, amount in value["instantaneous_positive"].items():
        sha(key)
        require(type(amount) is int and 0 <= amount <= 2 ** 64 - 1, "instantaneous positive count")


def validate_content(directory, snapshot, manifest, bridge):
    """Read one output; fill values need the configured fee ``bridge``."""
    check_manifest(manifest, snapshot, "summary_sha256" in manifest)
    require(bridge is not None and bridge.engine_identity == manifest["fee_engine_identity"],
            "fee engine identity")
    strategy = MultiMarketReader(manifest["policy"], manifest["experiment_sha256"], manifest["fee_config"],
                                 snapshot, bridge)
    if strategy.experiment.profile is not None:
        from replay.economic_sdk.profile_reader import validate_profile
        strategy.profile_summary = validate_profile(Path(directory), snapshot, manifest["files"],
                                                    strategy.experiment.profile,
                                                    manifest["experiment_sha256"], manifest["snapshot_sha256"])
    return aggregate_reader.validate(directory, snapshot, manifest, strategy)


def read_provisional(directory, snapshot_directory, *, expected_sha256, bridge):
    root = Path(directory)
    snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = read_json(root / "manifest.json")
    check_manifest(manifest, snapshot, True)
    require(manifest["snapshot_sha256"] == expected_sha256, "snapshot binding")
    receipt = obj(read_json(root / "content_receipt.json"),
                  "version semantic_sha256 run_id attempt_id group identity terminal")
    require(type(receipt["version"]) is int and receipt["version"] == 1, "receipt version")
    sha(receipt["identity"])
    require(receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    for field in ("run_id", "attempt_id", "group"):
        require(type(receipt[field]) is str and 0 < len(receipt[field]) <= 128, "receipt identifiers")
    require(type(receipt["terminal"]) is int and receipt["terminal"] >= 2, "terminal sequence")
    summary = validate_content(root, snapshot, manifest, bridge)
    require(encoded(summary) == encoded(read_json(root / "summary.json"))
            and digest(summary) == manifest["summary_sha256"], "summary identity/schema")
    return {"receipt": receipt, "manifest": manifest, "summary": summary}


def read_completed(run_directory, group):
    from replay.supervisor import read, read_success, initial
    root = Path(run_directory)
    require((root / "run.json").is_file() and (root / "SUCCESS.json").is_file(),
            "completed same-venue multi-market result requires supervisor SUCCESS")
    success = read_success(root)
    config = read(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(canonical_reference(spec["factory"]) == "replay.strategies.same_venue_multi_market:build", "strategy factory binding")
    inputs = Inputs(spec["config"])
    inputs.prepared.bind(freeze(initial(config)))
    result = read_provisional(root / success["outputs"][group], spec["config"]["snapshot_directory"],
                              expected_sha256=spec["config"]["snapshot_sha256"], bridge=inputs.bridge)
    require(result["manifest"]["experiment_sha256"] == inputs.identity
            and result["manifest"]["fee_engine_identity"] == inputs.bridge.engine_identity, "configured identity")
    require({k: result["receipt"][k] for k in ("identity", "attempt_id", "group", "run_id", "terminal")} ==
            {"identity": success["identity"], "attempt_id": success["attempt"], "group": group,
             "run_id": config["transport"]["run_id"], "terminal": success["terminal"]},
            "supervisor/content binding")
    return result
