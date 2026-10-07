"""Closed configuration, identities and baskets for the same-venue complement.

Policy version 1 is the frozen V1 experiment (output layout 1): a complete
interval partition with skew buckets, the cyclic-neighbour placebo beside real
rows, and full slices. Its wire contract is recorded in
SAME_VENUE_COMPLEMENT_V1.md §9 and is pinned byte for byte by
``replay/tests/fixtures/complement_v1_golden.json``.

Policy version 2 is the SDK default (output layout 2, ECONOMIC_STRATEGY_SDK_V1.md
§5): episodes are the result, time per key is aggregated into denominators,
reasons are structured, controls and the full interval audit are opt-in.
Policy 2 fields beyond policy 1: ``controls`` (list, default empty),
``controls_episodes``, ``controls_slices``, ``audit_intervals`` (booleans),
``time_shift_ring_entries`` and ``profile`` (null or a profile policy).
"""

from replay.economic_sdk.bounds import MAX_METADATA
from replay.economic_sdk.types import Basket, Control, Experiment
from replay.preparation import digest, encoded
from replay.strategy_sdk import plain
from replay.streams.protocol import obj, require, uint

STRATEGY = "same_venue_complement_v1"
PAYOUT = "unit_complement_assumption_not_resolution_proof"
FIELDS = ("fee_status", "diagnostic")
VALUE_CLASSES = ("GROSS_NONPOSITIVE", "NET_POSITIVE", "NET_NONPOSITIVE", "FEE_UNKNOWN")
EPISODE_CLASSES = {"gross": frozenset({"NET_POSITIVE", "NET_NONPOSITIVE", "FEE_UNKNOWN"}),
                   "net": frozenset({"NET_POSITIVE"})}
LIMITLESS_STATUSES = ("LOCKED", "CROSSED", "NOT_CROSSED")
SELF_CROSSED = "SELF_CROSSED_LEG"
# Fee-engine resolver memoization is bounded at 1,024 entries; reserve a
# complete maximum-line-sized graph per entry.
RESOLVER_RESERVATION = 1024 * 64 * 1024
_V1_FIELDS = ("version sizes_contracts headline_size_contracts latency_tiers_ns headline_latency_ns "
              "minimum_net_gap_per_contract_e18 leg_skew_buckets_ns verdict")
_V2_FIELDS = _V1_FIELDS + (" controls controls_episodes controls_slices audit_intervals "
                           "time_shift_ring_entries profile")


def policy_config(value):
    value = plain(value)
    require(type(value) is dict and value.get("version") in (1, 2), "policy version")
    obj(value, _V1_FIELDS if value["version"] == 1 else _V2_FIELDS)
    for name, cap, positive in (("sizes_contracts", 16, True), ("latency_tiers_ns", 8, True),
                                ("leg_skew_buckets_ns", 16, False)):
        entries = value[name]
        require(type(entries) is list and 1 <= len(entries) <= cap, "policy list budget")
        numbers = [uint(v) for v in entries]
        require(numbers == sorted(set(numbers)) and (not positive or numbers[0] > 0),
                "policy list order/positive")
    for headline, collection in (("headline_size_contracts", "sizes_contracts"),
                                 ("headline_latency_ns", "latency_tiers_ns")):
        require(value[headline] in value[collection], "headline membership")
    uint(value["minimum_net_gap_per_contract_e18"], 10**36)
    verdict = obj(value["verdict"], "maximum_positive_time_fraction_ppm minimum_evaluated_ns")
    uint(verdict["maximum_positive_time_fraction_ppm"], 1_000_000)
    require(uint(verdict["minimum_evaluated_ns"]) > 0, "minimum evaluated duration")
    if value["version"] == 2:
        for flag in ("controls_episodes", "controls_slices", "audit_intervals"):
            require(type(value[flag]) is bool, "policy flag")
        require(value["controls_episodes"] or not value["controls_slices"],
                "control slices require control episodes")
        controls = value["controls"]
        require(type(controls) is list and len(controls) <= 2, "controls")
        kinds = []
        for control in controls:
            require(type(control) is dict, "control")
            kinds.append(control.get("kind"))
            if control.get("kind") == "time_shift":
                obj(control, "kind shift_ns")
                shifts = control["shift_ns"]
                require(type(shifts) is list and 1 <= len(shifts) <= 4, "time_shift shifts")
                numbers = [uint(v) for v in shifts]
                require(numbers == sorted(set(numbers)) and numbers[0] > 0, "time_shift order/positive")
            else:
                obj(control, "kind")
                require(control["kind"] == "cyclic_neighbor", "control kind")
        require(len(set(kinds)) == len(kinds), "duplicate control")
        ring = uint(value["time_shift_ring_entries"], 10_000_000)
        require(ring > 0 or "time_shift" not in kinds, "time_shift ring entries")
        if value["profile"] is not None:
            from replay.economic_sdk.profile import profile_policy
            profile_policy(value["profile"])
    return value


def reason(value):
    """A structured reason: canonical JSON object text (policy 2)."""
    return encoded(value).decode()


def experiment_identity(snapshot_sha, policy, fee_config):
    return digest({"strategy": STRATEGY, "bridge_version": 1, "policy": policy,
                   "fees": fee_config, "snapshot_sha256": snapshot_sha})


def experiment(policy, experiment_sha256):
    """SDK experiment description, derived from the policy alone."""
    flags = {}
    if policy["version"] == 1:
        controls, layout, ring, profile = (Control("cyclic_neighbor"),), 1, 0, None
        fields, unevaluated = FIELDS, ("NOT_APPLICABLE", None)
    else:
        flags = {name: policy[name] for name in ("audit_intervals", "controls_episodes", "controls_slices")}
        fields, unevaluated = (), ()
        controls = []
        for control in policy["controls"]:
            if control["kind"] == "time_shift":
                controls.extend(Control("time_shift", int(s)) for s in control["shift_ns"])
            else:
                controls.append(Control(control["kind"]))
        controls, layout = tuple(controls), 2
        ring, profile = int(policy["time_shift_ring_entries"]), policy["profile"]
    return Experiment(
        strategy=STRATEGY, policy=policy, policy_sha256=digest(policy),
        experiment_sha256=experiment_sha256, tiers_ns=tuple(policy["latency_tiers_ns"]),
        skew_edges_ns=tuple(int(e) for e in policy["leg_skew_buckets_ns"]),
        kinds=("gross", "net"), episode_classes=EPISODE_CLASSES, value_classes=VALUE_CLASSES,
        diagnostic_statuses=LIMITLESS_STATUSES + ((SELF_CROSSED,) if policy["version"] == 2 else ()),
        measurement_fields=fields, unevaluated_fields=unevaluated,
        maxima=("gap_gross", "gap_net"), slice_invariant=("gap_gross",),
        controls=controls, layout=layout, ring_entries=ring,
        static_reservation=RESOLVER_RESERVATION, profile=profile, **flags)


class Inputs:
    def __init__(self, config):
        from replay.strategies._shared.fee_bridge import FeeBridge
        from replay.strategy_sdk import PreparedInput

        config = obj(plain(config), "version snapshot_directory snapshot_sha256 fees policy")
        require(len(encoded(config)) <= MAX_METADATA, "configuration budget")
        self.config = config
        self.prepared = PreparedInput({k: config[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
        self.snapshot = plain(self.prepared.snapshot)
        self.policy = policy_config(config["policy"])
        self.bridge = FeeBridge(config["fees"], self.snapshot["plans"])
        self.policy_sha256 = digest(self.policy)
        self.experiment_sha256 = experiment_identity(self.prepared.sha256, self.policy, self.bridge.semantic_config)


_KINDS = {"polymarket": "PM_TOKEN_PAIR", "kalshi": "KALSHI_YES_NO", "limitless": "LIMITLESS_SELF_CROSS"}
_DIRECTIONS = {"polymarket": ("long", "short"), "kalshi": ("both_bids",), "limitless": ("self_cross",)}


def baskets(snapshot, policy, scope_index):
    """Structural pairs per scope member, without prose inference."""
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    result = []
    for member in snapshot["scopes"][scope_index]["members"]:
        market = member["market_id"]
        venue = market.split(":", 1)[0]
        books = sorted(member["books"], key=lambda b: (b["instrument"], b["orientation"]))
        legs = [{"market_id": market, **plain(b)} for b in books]
        valid = (
            venue == "polymarket" and len(books) == 2 and all(b["instrument"].startswith("polymarket:") and b["orientation"] == "outcome" for b in books)
            or venue == "kalshi" and len(books) == 2 and all(b["instrument"] == market for b in books) and {b["orientation"] for b in books} == {"outcome", "complement"}
            or venue == "limitless" and len(books) == 1 and books[0]["instrument"].startswith("limitless:") and books[0]["orientation"] == "outcome"
        )
        keys = tuple((b["instrument"], b["orientation"]) for b in books)
        status, reasons = None, ()
        if not member["capture_selected"]:
            status = "NOT_CAPTURED"
        elif not valid:
            status = "UNSUPPORTED_SHAPE"
            reasons = ((str(len(books)),) if policy["version"] == 1
                       else (reason({"kind": "book_count", "value": len(books)}),))
        elif len({(plans[k]["price_scale"], plans[k]["quantity_scale"]) for k in keys}) != 1:
            status = "UNSUPPORTED_SCALE"
        paired = venue in ("polymarket", "kalshi")
        for direction in _DIRECTIONS.get(venue, ("unsupported",)):
            for size in policy["sizes_contracts"]:
                inputs = _inputs(venue, direction, int(size), len(keys), policy["version"])
                descriptor = {"venue": venue, "basket_kind": _KINDS.get(venue, "UNSUPPORTED"),
                              "market_id": market, "legs": legs, "admission": status,
                              "direction": direction, "size_contracts": size}
                if policy["version"] == 1:
                    descriptor |= {"placebo": False, "replaced_leg": None}
                result.append(Basket(descriptor, keys, (venue, market, direction, int(size)),
                                     status, reasons, 1 if paired else None,
                                     (venue, direction, size), market, inputs))
    return tuple(result)


def _inputs(venue, direction, size, legs, version):
    """Every view input ``evaluate`` reads per leg, for SDK observation reuse."""
    if venue == "limitless":
        sources = (("bid", size), ("ask", size))
    elif venue == "kalshi":
        sources = (("kalshi_complement_ask", size), ("bid", size))
    elif venue == "polymarket":
        sources = (("ask" if direction == "long" else "bid", size),)
    else:
        return None
    if version == 2 and venue != "limitless":
        sources += (("crossed", None),)
    return (sources,) * legs


def control_descriptor(policy, basket, control, replacement, admission):
    descriptor = dict(basket.descriptor)
    descriptor["admission"] = admission
    if replacement is not None:
        index = basket.control_leg
        legs = list(descriptor["legs"])
        replaced, legs[index] = legs[index], replacement
        descriptor["legs"] = legs
    else:
        replaced = None
    if policy["version"] == 1:
        require(control.kind == "cyclic_neighbor", "V1 control")
        descriptor["placebo"], descriptor["replaced_leg"] = True, replaced
    else:
        label = {"kind": control.kind, "leg": basket.control_leg}
        if control.kind == "time_shift":
            label["shift_ns"] = str(control.shift_ns)
        elif replaced is not None:
            label["replaced_leg"] = replaced
        descriptor["control"] = label
    return descriptor


def control_label(descriptor):
    if "placebo" in descriptor:
        return descriptor["placebo"]
    control = descriptor.get("control")
    if control is None:
        return None
    return control["kind"] + (":" + control["shift_ns"] if control["kind"] == "time_shift" else "")
