"""Independent implication proof, payoff and completed-output verification."""

from pathlib import Path

from replay.strategies import canonical_reference

from replay.strategies.cross_venue_arbitrage.contract import UNIT, SETTLEMENT, policy_config, valuation_config
from replay.strategies.cross_venue_arbitrage.output import CrossVenueReader
from replay.economic_sdk import aggregate_reader
from replay.economic_sdk.output import manifest_layout
from replay.economic_sdk.reader import read_json, signed
from replay.strategies._shared.implication_cover.contract import Inputs, STRATEGIES, FACTORIES, baskets, experiment, experiment_identity, payoff
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.streams.protocol import freeze, obj, require

PAYOFF_FIELDS = ("payout_middle_gross_e36", "payout_middle_net_e36",
                 "payout_middle_extra_gross_e36", "payout_middle_extra_net_e36",
                 "payoffs_gross_e36", "payoffs_net_e36")
_BASE_FIELDS = ("gap_gross gap_net gross_scale net_scale payout_floor_gross_e36 "
                "payout_floor_net_e36 native_legs fee_status assumptions evidence "
                "settlement_model outcomes_provider valuation")


class ImplicationReader(CrossVenueReader):
    """Reuse two-leg BUY accounting checks; add the nonconstant payout witness."""

    def __init__(self, policy, identity, valuation, fees, snapshot, mode, bridge=None):
        # Explicit base call also serves the runtime's shared evaluator mixin.
        CrossVenueReader.__init__(self, policy, identity, valuation, fees, snapshot, bridge)
        self.mode, self.snapshot = mode, snapshot
        self.experiment = experiment(self.policy, identity, mode)

    def baskets(self, snapshot, policy, scope_index):
        return baskets(snapshot, policy, scope_index, self.mode)

    def open_facts(self, value, entity, kind):
        obj(value, _BASE_FIELDS + " " + " ".join(PAYOFF_FIELDS))
        facts = CrossVenueReader.open_facts(self, {k: v for k, v in value.items() if k not in PAYOFF_FIELDS},
                                           entity, kind)
        size, proof = int(entity.descriptor["size_contracts"]), entity.descriptor["implication"]
        require(set(proof["antecedent_keys"]) < set(proof["consequent_keys"]), "strict implication proof")
        expected = payoff(proof, (size * UNIT, size * UNIT))
        require(type(value["payoffs_gross_e36"]) is list
                and value["payoffs_gross_e36"] == [str(v) for v in expected], "gross outcome payoffs")
        require(min(expected) == signed(value["payout_floor_gross_e36"]), "gross state floor")
        require(signed(value["payout_middle_gross_e36"]) == max(expected) == 2 * size * UNIT,
                "gross middle payout")
        require(signed(value["payout_middle_extra_gross_e36"]) == max(expected) - min(expected),
                "gross middle extra")
        if value["fee_status"] == "KNOWN":
            received = [signed(leg["received_e36"]) for leg in value["native_legs"]]
            expected = payoff(proof, received)
            require(type(value["payoffs_net_e36"]) is list
                    and value["payoffs_net_e36"] == [str(v) for v in expected], "net outcome payoffs")
            require(signed(value["payout_floor_net_e36"]) == min(expected), "net state floor")
            require(signed(value["payout_middle_net_e36"]) == max(expected), "net middle payout")
            require(signed(value["payout_middle_extra_net_e36"]) == max(expected) - min(expected), "net middle extra")
        else:
            require(all(value[field] is None for field in
                        ("payoffs_net_e36", "payout_middle_net_e36", "payout_middle_extra_net_e36")),
                    "unknown net middle payoff")
        return facts

    def summarize_aggregate(self, aggregates, manifest, snapshot):
        result = super().summarize_aggregate(aggregates, manifest, snapshot)
        result.update(version=1, strategy=STRATEGIES[self.mode], venue_mode=self.mode)
        result["qualifiers"].extend(["STATIC_STRICT_IMPLICATION", "NO_PROBABILITY_MODEL",
                                     "NO_IMMEDIATE_REDEMPTION_ASSUMPTION"])
        return result


def check_manifest(value, snapshot, complete, mode):
    fields = ("version strategy venue_mode snapshot_sha256 policy policy_sha256 experiment_sha256 "
              "fee_config fee_engine_identity files instantaneous_positive settlement_model outcomes_provider valuation")
    manifest_layout(value)
    if "layout" in value:
        fields += " layout"
    obj(value, fields + (" summary_sha256" if complete else ""))
    require(mode in STRATEGIES and value["venue_mode"] == mode and value["strategy"] == STRATEGIES[mode],
            "implication strategy/mode binding")
    require(type(value["version"]) is int and value["version"] == 1, "implication output version")
    policy_config(value["policy"]); valuation_config(value["valuation"])
    require(value["settlement_model"] == SETTLEMENT and
            value["outcomes_provider"] == snapshot.get("outcomes", {}).get("provider"), "manifest settlement/provider")
    for field in ("snapshot_sha256", "policy_sha256", "experiment_sha256", "fee_engine_identity"):
        sha(value[field])
    if complete:
        sha(value["summary_sha256"])
    obj(value["fee_config"], "catalog_identity reference_ns limitless_buy_bps limitless_sell_bps "
                            "kalshi_member_class assets instrument_bindings")
    require(value["policy_sha256"] == digest(value["policy"]), "policy identity")
    require(value["experiment_sha256"] == experiment_identity(value["snapshot_sha256"], value["policy"],
            value["fee_config"], value["valuation"], mode), "experiment identity")
    require(type(value["instantaneous_positive"]) is dict, "instantaneous positive map")
    for key, amount in value["instantaneous_positive"].items():
        sha(key)
        require(type(amount) is int and 0 <= amount <= 2 ** 64 - 1, "instantaneous positive count")


def validate_content(directory, snapshot, manifest, bridge=None, *, state_bytes=128 * 1024**2):
    mode = manifest.get("venue_mode")
    check_manifest(manifest, snapshot, "summary_sha256" in manifest, mode)
    if bridge is not None:
        require(bridge.engine_identity == manifest["fee_engine_identity"], "fee engine identity")
    reader = ImplicationReader(manifest["policy"], manifest["experiment_sha256"], manifest["valuation"],
                               manifest["fee_config"], snapshot, mode, bridge)
    if reader.experiment.profile is not None:
        from replay.economic_sdk.profile_reader import validate_profile
        reader.profile_summary = validate_profile(Path(directory), snapshot, manifest["files"], reader.experiment.profile,
                                                  manifest["experiment_sha256"], manifest["snapshot_sha256"], state_bytes=state_bytes)
    return aggregate_reader.validate(directory, snapshot, manifest, reader, state_bytes=state_bytes)


def read_provisional(directory, snapshot_directory, *, expected_sha256, mode, bridge=None, state_bytes=128 * 1024**2):
    root = Path(directory)
    snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = read_json(root / "manifest.json")
    check_manifest(manifest, snapshot, True, mode)
    require(manifest["snapshot_sha256"] == expected_sha256, "snapshot binding")
    receipt = obj(read_json(root / "content_receipt.json"), "version semantic_sha256 run_id attempt_id group identity terminal")
    require(type(receipt["version"]) is int and receipt["version"] == 1, "receipt version")
    sha(receipt["identity"])
    require(receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    for field in ("run_id", "attempt_id", "group"):
        require(type(receipt[field]) is str and 0 < len(receipt[field]) <= 128, "receipt identifiers")
    require(type(receipt["terminal"]) is int and receipt["terminal"] >= 2, "terminal sequence")
    summary = validate_content(root, snapshot, manifest, bridge, state_bytes=state_bytes)
    require(encoded(summary) == encoded(read_json(root / "summary.json")) and
            digest(summary) == manifest["summary_sha256"], "summary identity/schema")
    return {"receipt": receipt, "manifest": manifest, "summary": summary}


def read_completed(run_directory, group, *, mode):
    from replay.supervisor import read, read_success, initial
    root = Path(run_directory)
    require((root / "run.json").is_file() and (root / "SUCCESS.json").is_file(),
            "completed implication result requires supervisor SUCCESS")
    require(mode in STRATEGIES, "implication venue mode")
    success, config = read_success(root), read(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(canonical_reference(spec["factory"]) == FACTORIES[mode], "strategy factory binding")
    inputs = Inputs(spec["config"], mode)
    inputs.prepared.bind(freeze(initial(config)))
    result = read_provisional(root / success["outputs"][group], spec["config"]["snapshot_directory"],
                              expected_sha256=spec["config"]["snapshot_sha256"], mode=mode, bridge=inputs.bridge,
                              state_bytes=config["limits"].get("state_bytes", 128 * 1024**2))
    require(result["manifest"]["experiment_sha256"] == inputs.identity and
            result["manifest"]["fee_engine_identity"] == inputs.bridge.engine_identity, "configured identity")
    require({k: result["receipt"][k] for k in ("identity", "attempt_id", "group", "run_id", "terminal")} ==
            {"identity": success["identity"], "attempt_id": success["attempt"], "group": group,
             "run_id": config["transport"]["run_id"], "terminal": success["terminal"]}, "supervisor/content binding")
    return result
