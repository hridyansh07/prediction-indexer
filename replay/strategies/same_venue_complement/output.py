"""Independent reader for same-venue-complement output.

This module does not import the strategy runtime. The generic SDK reader checks
partitions, episodes, slices and recomputes aggregates; the hooks below add the
complement wire format, fee-class consistency and the re-walk of consumed
quotes, and derive the summary and verdicts from the recomputed facts.
"""

from pathlib import Path

from replay.strategies import canonical_reference

from .contract import (
    FIELDS, LIMITLESS_STATUSES, PAYOUT, SELF_CROSSED, STRATEGY, Inputs, baskets,
    control_descriptor, control_label, experiment, experiment_identity, policy_config,
)
from replay.economic_sdk import Strategy, aggregate_reader, reader
from replay.economic_sdk.bounds import MAX_METADATA, MAX_STATE
from replay.economic_sdk.output import Layout, manifest_layout
from replay.economic_sdk.reader import quantiles as _quantiles  # noqa: F401  (stable test surface)
from replay.economic_sdk.reader import read_json as _json
from replay.economic_sdk.reader import signed as _signed
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.streams.protocol import freeze, obj, require, uint
from replay.supervisor import initial, read_success
from replay.supervisor import read as read_run

FEE_STATUSES = {"SKIPPED", "KNOWN", "UNKNOWN", "NOT_APPLICABLE"}
NET_CLASSES = {"NET_POSITIVE", "NET_NONPOSITIVE"}
# A positive confined to skew buckets at or above this lower edge is labelled
# SKEW_ARTIFACT_LIKELY (complement spec §5); it still counts toward the verdict.
SKEW_ARTIFACT_EDGE_NS = 1_000_000_000


def check_manifest(value, complete=True):
    fields = ("version strategy snapshot_sha256 policy policy_sha256 experiment_sha256 fee_config "
              "fee_engine_identity files instantaneous_positive payout_assumption")
    manifest_layout(value, legacy=value.get("version") == 1)
    if "layout" in value:
        fields += " layout"
    if complete:
        fields += " summary_sha256"
    obj(value, fields)
    policy_config(value["policy"])
    require(type(value["version"]) is int and value["version"] == value["policy"]["version"])
    require(value["strategy"] == STRATEGY and value["payout_assumption"] == PAYOUT)
    for field in ("snapshot_sha256", "policy_sha256", "experiment_sha256", "fee_engine_identity"):
        sha(value[field])
    if complete:
        sha(value["summary_sha256"])
    require(digest(value["policy"]) == value["policy_sha256"], "policy identity")
    fee_fields = {"catalog_identity", "reference_ns", "limitless_buy_bps",
                  "limitless_sell_bps", "kalshi_member_class", "assets",
                  "instrument_bindings"}
    require(type(value["fee_config"]) is dict and set(value["fee_config"]) == fee_fields,
            "closed semantic fee configuration")
    require(experiment_identity(value["snapshot_sha256"], value["policy"], value["fee_config"])
            == value["experiment_sha256"], "experiment identity")
    require(type(value["instantaneous_positive"]) is dict)
    for key, count in value["instantaneous_positive"].items():
        sha(key)
        require(type(count) is int and 0 <= count <= 2**64 - 1)


class ComplementReader(Strategy):
    """Policy-only view of the complement: baskets, controls and reader hooks."""

    def __init__(self, policy, experiment_sha256):
        self.policy = policy_config(policy)
        self.experiment = experiment(self.policy, experiment_sha256)
        self.plans = {}

    @classmethod
    def for_manifest(cls, manifest, snapshot):
        instance = cls.__new__(cls)
        ComplementReader.__init__(instance, manifest["policy"], manifest["experiment_sha256"])
        instance.plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
        return instance

    def baskets(self, snapshot, policy, scope_index):
        return baskets(snapshot, policy, scope_index)

    def control_descriptor(self, basket, control, replacement, admission):
        return control_descriptor(self.policy, basket, control, replacement, admission)

    def extra_files(self):
        if self.experiment.profile is None:
            return ()
        from replay.economic_sdk.profile import FILES
        return FILES

    # -- row hooks -------------------------------------------------------------
    def check_measurement(self, row, entity):
        require(row["fee_status"] in FEE_STATUSES)
        require(row["diagnostic"] is None or type(row["diagnostic"]) is str)
        status, vc = row["status"], row["value_class"]
        if vc == "GROSS_NONPOSITIVE":
            require(row["fee_status"] == "SKIPPED")
        if vc in NET_CLASSES:
            require(row["fee_status"] == "KNOWN")
        if vc == "FEE_UNKNOWN":
            require(row["fee_status"] == "UNKNOWN")
        if entity.descriptor["venue"] == "limitless":
            require(status in {"NOT_CAPTURED", "UNSUPPORTED_SHAPE", "UNSUPPORTED_SCALE",
                               "UNUSABLE", "ONE_SIDED", "DEPTH_LIMITED", *LIMITLESS_STATUSES}
                    and vc is None and row["fee_status"] == "NOT_APPLICABLE",
                    "Limitless diagnostic measurement")
        else:
            require(status not in LIMITLESS_STATUSES, "diagnostic status on economic basket")
        if status == SELF_CROSSED:
            require(vc is None and row["fee_status"] == "NOT_APPLICABLE", "self-crossed measurement")

    def open_facts(self, value, entity, kind):
        descriptor = entity.descriptor
        require(descriptor["venue"] != "limitless", "Limitless economic episode")
        obj(value, "gap_gross gap_net gross_scale net_scale fee_status assessments assumptions evidence")
        gross = _signed(value["gap_gross"])
        net = None if value["gap_net"] is None else _signed(value["gap_net"])
        require(type(value["gross_scale"]) is int and 0 <= value["gross_scale"] <= 36)
        require(type(value["net_scale"]) is int and value["net_scale"] == 18)
        require(value["fee_status"] in FEE_STATUSES)
        require(type(value["assessments"]) is list and len(value["assessments"]) == 2)
        require(all(type(x) is list and all(type(identity) is str for identity in x)
                    for x in value["assessments"]))
        for leg in value["assessments"]:
            for identity in leg:
                sha(identity)
        require(all(type(x) is str for name in ("assumptions", "evidence") for x in value[name]))
        scales = {int(self.plans[(leg["instrument"], leg["orientation"])]["price_scale"])
                  + int(self.plans[(leg["instrument"], leg["orientation"])]["quantity_scale"])
                  for leg in descriptor["legs"]}
        require(len(scales) == 1 and value["gross_scale"] in scales, "gross scale/plan mismatch")
        if value["fee_status"] == "KNOWN":
            require(net is not None and all(value["assessments"]), "known fee assessment")
        elif value["fee_status"] == "UNKNOWN":
            require(net is None, "unknown fee has net value")
        else:
            require(net is None and not any(value["assessments"]), "unassessed fee values")
        return gross, net

    def check_open(self, values, facts, entity, kind, opening_class, where):
        gross, net = facts
        threshold = int(entity.descriptor["size_contracts"]) * int(
            self.policy["minimum_net_gap_per_contract_e18"])
        fee_class = ((opening_class == "FEE_UNKNOWN") == (values["fee_status"] == "UNKNOWN")
                     and (opening_class in NET_CLASSES) == (values["fee_status"] == "KNOWN"))
        if where == "episode":
            require(gross > 0, "nonpositive episode open")
            require(fee_class, "episode open fee class mismatch")
            if kind == "net":
                require(opening_class == "NET_POSITIVE" and net is not None and net > 0
                        and net >= threshold, "noneligible net open")
            return
        require(gross > 0 and (kind != "net" or net is not None and net > 0))
        if kind == "net":
            require(opening_class == "NET_POSITIVE" and net >= threshold, "slice net eligibility")
        fact_class = ("FEE_UNKNOWN" if values["fee_status"] == "UNKNOWN" else
                      "NET_POSITIVE" if net is not None and net > 0 and net >= threshold else
                      "NET_NONPOSITIVE")
        require(opening_class == fact_class, "slice/measurement eligibility mismatch")
        require(fee_class, "open fee class mismatch")

    def check_quotes(self, consumed, values, entity):
        """Re-walk full displayed consumed levels to the ticket: order, depth, scales, gross."""
        descriptor = entity.descriptor
        require(type(consumed) is list and len(consumed) == 2)
        for leg in consumed:
            require(type(leg) is list)
            for level in leg:
                require(type(level) is list and len(level) == 2, "consumed level")
                uint(level[0], 2**64 - 1)
                require(uint(level[1], 2**64 - 1) > 0, "consumed quantity")
        costs = []
        quantity = int(descriptor["size_contracts"])
        for index, leg in enumerate(consumed):
            plan = self.plans[(descriptor["legs"][index]["instrument"], descriptor["legs"][index]["orientation"])]
            remaining = quantity * 10 ** int(plan["quantity_scale"])
            cost, previous = 0, None
            descending = descriptor["direction"] == "short"
            for price_text, amount_text in leg:
                require(remaining > 0, "extra unconsumed level")
                price, amount = uint(price_text, 2**64 - 1), uint(amount_text, 2**64 - 1)
                require(price <= 10 ** int(plan["price_scale"]), "consumed price scale")
                require(previous is None or (price < previous if descending else price > previous),
                        "consumed price order")
                previous = price
                taken = min(remaining, amount)
                cost += price * taken
                remaining -= taken
            require(remaining == 0, "consumed depth insufficient")
            costs.append(cost)
        first = self.plans[(descriptor["legs"][0]["instrument"], descriptor["legs"][0]["orientation"])]
        unit = quantity * 10 ** (int(first["price_scale"]) + int(first["quantity_scale"]))
        calculated = (unit - sum(costs) if descriptor["direction"] in {"long", "both_bids"}
                      else sum(costs) - unit)
        require(_signed(values["gap_gross"]) == calculated, "consumed/gross arithmetic")

    # -- summary ---------------------------------------------------------------
    def summary_key(self, entity):
        d = entity.descriptor
        return (d["venue"], d["basket_kind"], d["direction"], d["size_contracts"], control_label(d))

    def summarize(self, aggregates, manifest, snapshot):
        """Policy 1 (layout 1) summary: rows per skew bucket, placebo beside real."""
        policy = self.policy
        headline, latency = policy["headline_size_contracts"], policy["headline_latency_ns"]
        rows = []
        for key, g in sorted(aggregates.groups.items(),
                             key=lambda x: (x[0][0], x[0][1], x[0][2], int(x[0][3]), x[0][4], x[0][5])):
            venue, bkind, direction, size, placebo, skew = key
            d = g.durations
            rows.append({
                "venue": venue, "basket_kind": bkind, "direction": direction, "size_contracts": size,
                "placebo": placebo, "skew_bucket": skew, "policy_sha256": manifest["policy_sha256"],
                "durations_ns": {k: str(v) for k, v in sorted(d.items())},
                "evaluated_ns": str(sum(d.get(k, 0) for k in _EVALUATED_LABELS)),
                "gross_positive_ns": str(d.get("net_positive_ns", 0) + d.get("net_nonpositive_ns", 0)
                                         + d.get("fee_unknown_ns", 0)),
                "net_assessed_ns": str(d.get("net_positive_ns", 0) + d.get("net_nonpositive_ns", 0)),
                "episode_count": g.episode_count, "slice_count": g.slice_count,
                "q_ns": {k: str(v) for k, v in g.q["net"].items()},
                "gross_q_ns": {k: str(v) for k, v in g.q["gross"].items()},
                "episode_lifetime_quantiles_ns": {kind: aggregates.quantiles(g.episode_values[kind])
                                                  for kind in ("gross", "net")},
                "slice_survival_quantiles_ns": {kind: aggregates.quantiles(g.slice_values[kind])
                                                for kind in ("gross", "net")},
                "censored_episodes": g.censored_episodes, "censored_slices": g.censored_slices})
        verdicts = []
        venues = sorted({e.descriptor["venue"] for e in aggregates.entities.values()} - {"limitless"})
        for venue in venues:
            relevant = [r for r in rows if r["venue"] == venue and r["size_contracts"] == headline
                        and not r["placebo"]]
            E = sum(sum(int(v) for k, v in r["durations_ns"].items() if k in _EVALUATED_LABELS)
                    for r in relevant)
            Q = sum(int(r["q_ns"][latency]) for r in relevant)
            X = sum(int(r["durations_ns"].get("fee_unknown_ns", "0")) for r in relevant)
            verdicts.append(_verdict(policy, venue, E, Q, X))
        exclusions = sorted({(e.descriptor["venue"], e.descriptor["market_id"], e.descriptor["admission"])
                             for e in aggregates.entities.values() if e.descriptor["admission"] is not None})
        summary = {"version": 1, "snapshot_sha256": manifest["snapshot_sha256"],
                   "policy_sha256": manifest["policy_sha256"],
                   "experiment_sha256": manifest["experiment_sha256"],
                   "instantaneous_positive": manifest["instantaneous_positive"], "rows": rows,
                   "verdicts": verdicts,
                   "member_exclusions": [{"venue": v, "market_id": m, "status": s} for v, m, s in exclusions],
                   "history_complete": snapshot["history_complete"], "vendor_completeness": "NOT_PROVEN",
                   "qualifiers": QUALIFIERS}
        require(len(encoded(summary)) <= MAX_METADATA, "summary budget")
        require(aggregates.budget.used + len(encoded(summary)) <= MAX_STATE, "reader state budget")
        return summary

    def summarize_aggregate(self, aggregates, manifest, snapshot):
        """Policy 2 (layout 2) summary from denominators and episodes.

        Rows are per (venue, basket kind, direction, size), with no skew
        dimension; skew appears only as attributed positive time. Controls get
        their own summaries and never feed a verdict.
        """
        policy = self.policy
        headline, latency = policy["headline_size_contracts"], policy["headline_latency_ns"]
        rows = _aggregate_rows(aggregates, aggregates.groups[""], latency)
        verdicts = []
        venues = sorted({e.descriptor["venue"] for e in aggregates.entities[""].values()} - {"limitless"})
        for venue in venues:
            relevant = [r for r in rows if r["venue"] == venue and r["size_contracts"] == headline]
            E = sum(int(r["evaluated_ns"]) for r in relevant)
            Q = sum(int(r["q_ns"][latency]) for r in relevant)
            # Only unknown-fee time inside gross slices that reach the headline
            # latency can hide a qualifying opportunity.
            X = sum(int(r["fee_unknown_in_qualifying_gross_slices_ns"]) for r in relevant)
            row = _verdict(policy, venue, E, Q, X)
            row["fee_unknown_total_ns"] = str(sum(int(r["class_ns"].get("FEE_UNKNOWN", "0"))
                                                  for r in relevant))
            positive = {}
            for r in relevant:
                for b, v in r["gross_slice_ns_by_skew"].items():
                    positive[b] = positive.get(b, 0) + int(v)
            row["labels"] = ["SKEW_ARTIFACT_LIKELY"] if _skew_artifact(positive, policy) else []
            verdicts.append(row)
        exclusions = sorted({(e.descriptor["venue"], e.descriptor["market_id"], e.descriptor["admission"])
                             for e in aggregates.entities[""].values() if e.descriptor["admission"] is not None})
        summary = {"version": 2, "snapshot_sha256": manifest["snapshot_sha256"],
                   "policy_sha256": manifest["policy_sha256"],
                   "experiment_sha256": manifest["experiment_sha256"],
                   "instantaneous_positive": manifest["instantaneous_positive"],
                   "reasons": aggregates.reasons, "rows": rows, "verdicts": verdicts,
                   "member_exclusions": [{"venue": v, "market_id": m, "status": s} for v, m, s in exclusions],
                   "history_complete": snapshot["history_complete"], "vendor_completeness": "NOT_PROVEN",
                   "qualifiers": QUALIFIERS}
        controls = {}
        for group, facts in sorted(aggregates.groups.items()):
            if group:
                name = group.split("/")[1]
                controls[name] = {"control": name, "experiment_sha256": manifest["experiment_sha256"],
                                  "rows": _aggregate_rows(aggregates, facts, latency)}
        if controls:
            summary["controls"] = controls
        if self.experiment.profile is not None:
            summary["profile"] = self.profile_summary
        require(len(encoded(summary)) <= MAX_METADATA, "summary budget")
        require(aggregates.budget.used + len(encoded(summary)) <= MAX_STATE, "reader state budget")
        return summary


_EVALUATED_LABELS = ("gross_nonpositive_ns", "net_positive_ns", "net_nonpositive_ns", "fee_unknown_ns")
QUALIFIERS = ["DETECTED_NOT_EXECUTED", "RETROSPECTIVE_DISPLAYED_DEPTH_SURVIVAL",
              "PINNED_FEE_ESTIMATE", "NOT_RESOLUTION_PROOF"]


def _verdict(policy, venue, E, Q, X):
    if E < int(policy["verdict"]["minimum_evaluated_ns"]):
        verdict, reason = "INCONCLUSIVE_FIXTURE", "INSUFFICIENT_EVALUATED_TIME"
    elif Q * 1_000_000 > int(policy["verdict"]["maximum_positive_time_fraction_ppm"]) * E:
        verdict, reason = "INTRA_INSTRUMENT_GAPS_PRESENT_INVESTIGATE", None
    elif X:
        verdict, reason = "INCONCLUSIVE_FIXTURE", "UNRESOLVED_POSITIVE_GROSS"
    else:
        verdict, reason = "INTRA_INSTRUMENT_GAPS_ABSENT_IN_FIXTURE", None
    return {"venue": venue, "verdict": verdict, "reason": reason, "evaluated_ns": str(E),
            "qualified_ns": str(Q), "fee_unknown_ns": str(X),
            "basis": "PINNED_FEE_MODEL_AND_DISPLAYED_DEPTH_POLICY"}


def _aggregate_rows(aggregates, facts, latency, *, size_order=int):
    rows = []
    for key, g in sorted(facts.items(), key=lambda x: (x[0][0], x[0][1], x[0][2], size_order(x[0][3]))):
        venue, bkind, direction, size, _ = key
        c = g.class_ns
        row = {"venue": venue, "basket_kind": bkind, "direction": direction, "size_contracts": size,
               "status_ns": {k: str(v) for k, v in sorted(g.status_ns.items())},
               "class_ns": {k: str(v) for k, v in sorted(c.items())},
               "evaluated_ns": str(g.status_ns.get("DEPTH_SUFFICIENT", 0)),
               "gross_positive_ns": str(sum(c.get(k, 0) for k in ("NET_POSITIVE", "NET_NONPOSITIVE",
                                                                  "FEE_UNKNOWN"))),
               "net_assessed_ns": str(c.get("NET_POSITIVE", 0) + c.get("NET_NONPOSITIVE", 0))}
        if g.episodes:
            row |= {
                "episode_count": g.episode_count, "slice_count": g.slice_count,
                "q_ns": {t: str(v) for t, v in g.q["net"].items()},
                "gross_q_ns": {t: str(v) for t, v in g.q["gross"].items()},
                "q_by_skew_ns": {kind: {t: {b: str(v) for b, v in sorted(m.items())}
                                        for t, m in g.q_by_skew[kind].items()} for kind in ("gross", "net")},
                "episode_lifetime_quantiles_ns": {kind: aggregates.quantiles(g.lifetimes[kind])
                                                  for kind in ("gross", "net")},
                "slice_survival_quantiles_ns": {kind: aggregates.quantiles(g.survival[kind])
                                                for kind in ("gross", "net")},
                "censored_episodes": g.censored_episodes, "censored_slices": g.censored_slices,
                "fee_unknown_in_qualifying_gross_slices_ns":
                    str(g.qualifying["gross"][latency].get("FEE_UNKNOWN", 0)),
                "gross_slice_ns_by_skew": {b: str(v) for b, v in sorted(g.slice_ns_by_skew["gross"].items())}}
        if g.fill_ns:
            # Fill checks only (SDK spec §13): trigger-positive time by fill state.
            row["fill_ns"] = {k: str(v) for k, v in sorted(g.fill_ns.items())}
            row["fill_ends"] = dict(sorted(g.fill_ends.items()))
        rows.append(row)
    return rows


def _skew_artifact(positive, policy):
    """Positive time exists and lies entirely in skew buckets whose lower edge is >= 1 s."""
    edges = [0] + [int(e) for e in policy["leg_skew_buckets_ns"]]
    positive = {b: v for b, v in positive.items() if v}
    return bool(positive) and all(edges[int(b)] >= SKEW_ARTIFACT_EDGE_NS for b in positive)


def validate_content(directory, snapshot, manifest, *, state_bytes=128 * 1024**2):
    """Validate semantic files and return the independently derived summary."""
    check_manifest(manifest, "summary_sha256" in manifest)
    strategy = ComplementReader.for_manifest(manifest, snapshot)
    if strategy.experiment.profile is not None:
        from replay.economic_sdk.profile_reader import validate_profile
        strategy.profile_summary = validate_profile(Path(directory), snapshot, manifest["files"],
                                                    strategy.experiment.profile,
                                                    manifest["experiment_sha256"],
                                                    manifest["snapshot_sha256"])
    if strategy.experiment.layout == 1:
        return reader.validate(directory, snapshot, manifest, strategy, state_bytes=state_bytes)
    return aggregate_reader.validate(directory, snapshot, manifest, strategy, state_bytes=state_bytes)


def read_provisional(directory, snapshot_directory, *, expected_sha256, state_bytes=128 * 1024**2):
    root = Path(directory)
    snapshot = load_snapshot(snapshot_directory, expected_sha256=expected_sha256)
    manifest = _json(root / "manifest.json")
    check_manifest(manifest, True)
    require(manifest["snapshot_sha256"] == expected_sha256, "snapshot binding")
    receipt = obj(_json(root / "content_receipt.json"),
                  "version semantic_sha256 run_id attempt_id group identity terminal")
    require(type(receipt["version"]) is int and receipt["version"] == 1)
    for field in ("semantic_sha256", "identity"):
        sha(receipt[field])
    require(receipt["semantic_sha256"] == digest(manifest), "semantic identity")
    for field in ("run_id", "attempt_id", "group"):
        require(type(receipt[field]) is str and 0 < len(receipt[field]) <= 128)
    require(type(receipt["terminal"]) is int and receipt["terminal"] >= 2)
    summary = validate_content(root, snapshot, manifest, state_bytes=state_bytes)
    require(encoded(_json(root / "summary.json")) == encoded(summary)
            and manifest["summary_sha256"] == digest(summary), "summary identity/schema")
    return {"receipt": receipt, "manifest": manifest, "summary": summary}


def read_completed(run_directory, group):
    root = Path(run_directory)
    require((root / "run.json").is_file() and (root / "SUCCESS.json").is_file(),
            "completed complement requires supervisor SUCCESS")
    success = read_success(root)
    config = read_run(root / "run.json")
    require(group in success["outputs"], "unknown strategy group")
    spec = config["strategies"][group]
    require(canonical_reference(spec["factory"]) == "replay.strategies.same_venue_complement:build", "complement factory binding")
    inputs = Inputs(spec["config"])
    inputs.prepared.bind(freeze(initial(config)))
    result = read_provisional(root / success["outputs"][group], spec["config"]["snapshot_directory"],
                              expected_sha256=spec["config"]["snapshot_sha256"],
                              state_bytes=config["limits"].get("state_bytes", 128 * 1024**2))
    require(result["manifest"]["experiment_sha256"] == inputs.experiment_sha256
            and result["manifest"]["fee_engine_identity"] == inputs.bridge.engine_identity,
            "configured identity")
    receipt = result["receipt"]
    require({k: receipt[k] for k in ("identity", "attempt_id", "group", "run_id", "terminal")}
            == {"identity": success["identity"], "attempt_id": success["attempt"], "group": group,
                "run_id": config["transport"]["run_id"], "terminal": success["terminal"]},
            "supervisor/content binding")
    return result


__all__ = ["ComplementReader", "Layout", "FIELDS", "SELF_CROSSED", "read_completed",
           "read_provisional", "validate_content"]
