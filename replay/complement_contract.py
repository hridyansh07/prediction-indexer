"""Closed configuration and identities for same-venue complement V1.

Output wire contract (all fields required, unknown fields rejected):
common = version(1), experiment_sha256, scope(int), entity(sha), start_ns, end_ns.
measurement = common + status, reasons(list), skew_bucket(int|null), value_class
    (str|null), fee_status(SKIPPED|KNOWN|UNKNOWN|NOT_APPLICABLE), diagnostic(str|null).
episode = common + episode_id, kind(gross|net), basket(layout descriptor),
    end_reason, censored(bool), gap_lifetime_ns, opening_slice_survival_ns,
    viable_tiers(list of ns strings), qualified_ns(map tier->ns),
    open_values, max_gap_gross, max_gap_net, opening_skew_bucket.
open_values = gap_gross(signed str), gap_net(signed str|null), gross_scale(int),
    net_scale(18), fee_status, assessments(list of per-leg lists),
    assumptions(list[str]), evidence(list[str]).
slice = common + episode_id, kind, end_reason, censored, consumed(list of two
    lists of [price str, displayed quantity str]), survival_ns, viable_tiers,
    open_values, opening_skew_bucket.
Manifest = version,strategy,snapshot_sha256,policy,policy_sha256,experiment_sha256,
    fee_config(no directory),fee_engine_identity,files(map basename->identity),
    instantaneous_positive(map entity->int),payout_assumption,
    [summary_sha256 only after validation].
Episode IDs hash [scope, entity, kind, start_ns]. Rows close-sort with row_order.
"""

from replay.complement_fees import FeeBridge
from replay.preparation import digest, encoded
from replay.strategy_sdk import PreparedInput, plain
from replay.streams.protocol import obj, require, uint

STRATEGY = "same_venue_complement_v1"
MAX_ROWS = 2_000_000
MAX_BYTES = 512 * 1024 * 1024
MAX_LINE = 64 * 1024
MAX_METADATA = 8 * 1024 * 1024
MAX_STATE = 128 * 1024 * 1024
FILES = ("measurements.ndjson", "episodes.ndjson", "placebo_episodes.ndjson", "slices.ndjson")
PAYOUT = "unit_complement_assumption_not_resolution_proof"
POLICY_FIELDS = "version sizes_contracts headline_size_contracts latency_tiers_ns headline_latency_ns minimum_net_gap_per_contract_e18 leg_skew_buckets_ns verdict"


def policy_config(value):
    value = obj(plain(value), POLICY_FIELDS)
    require(type(value["version"]) is int and value["version"] == 1, "policy version")
    for name, cap, positive in (("sizes_contracts", 16, True), ("latency_tiers_ns", 8, True), ("leg_skew_buckets_ns", 16, False)):
        entries = value[name]
        require(type(entries) is list and 1 <= len(entries) <= cap, "policy list budget")
        numbers = [uint(v) for v in entries]
        require(numbers == sorted(set(numbers)) and (not positive or numbers[0] > 0), "policy list order/positive")
    for headline, collection in (("headline_size_contracts", "sizes_contracts"), ("headline_latency_ns", "latency_tiers_ns")):
        require(value[headline] in value[collection], "headline membership")
    uint(value["minimum_net_gap_per_contract_e18"], 10**36)
    verdict = obj(value["verdict"], "maximum_positive_time_fraction_ppm minimum_evaluated_ns")
    uint(verdict["maximum_positive_time_fraction_ppm"], 1_000_000)
    require(uint(verdict["minimum_evaluated_ns"]) > 0, "minimum evaluated duration")
    return value


def experiment_identity(snapshot_sha, policy, fee_config):
    return digest({"strategy": STRATEGY, "bridge_version": 1, "policy": policy,
                   "fees": fee_config, "snapshot_sha256": snapshot_sha})


class Inputs:
    def __init__(self, config):
        config = obj(plain(config), "version snapshot_directory snapshot_sha256 fees policy")
        require(len(encoded(config)) <= MAX_METADATA, "configuration budget")
        self.config = config
        self.prepared = PreparedInput({k: config[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
        self.snapshot = plain(self.prepared.snapshot)
        self.policy = policy_config(config["policy"])
        self.bridge = FeeBridge(config["fees"], self.snapshot["plans"])
        self.policy_sha256 = digest(self.policy)
        self.experiment_sha256 = experiment_identity(self.prepared.sha256, self.policy, self.bridge.semantic_config)


def layouts(snapshot, policy, scope_index):
    """Resolve structural pairs and their actual placebo legs without prose inference."""
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    members = snapshot["scopes"][scope_index]["members"]
    baskets = []
    for member in members:
        market = member["market_id"]
        venue = market.split(":", 1)[0]
        books = sorted(member["books"], key=lambda b: (b["instrument"], b["orientation"]))
        legs = [{"market_id": market, **plain(b)} for b in books]
        kind = {"polymarket": "PM_TOKEN_PAIR", "kalshi": "KALSHI_YES_NO", "limitless": "LIMITLESS_SELF_CROSS"}.get(venue, "UNSUPPORTED")
        valid = (
            venue == "polymarket" and len(books) == 2 and all(b["instrument"].startswith("polymarket:") and b["orientation"] == "outcome" for b in books)
            or venue == "kalshi" and len(books) == 2 and all(b["instrument"] == market for b in books) and {b["orientation"] for b in books} == {"outcome", "complement"}
            or venue == "limitless" and len(books) == 1 and books[0]["instrument"].startswith("limitless:") and books[0]["orientation"] == "outcome"
        )
        status = None
        if not member["capture_selected"]:
            status = "NOT_CAPTURED"
        elif not valid:
            status = "UNSUPPORTED_SHAPE"
        elif len({(plans[b["instrument"], b["orientation"]]["price_scale"], plans[b["instrument"], b["orientation"]]["quantity_scale"]) for b in books}) != 1:
            status = "UNSUPPORTED_SCALE"
        baskets.append({"venue": venue, "basket_kind": kind, "market_id": market, "legs": legs, "admission": status})
    supported = {}
    for b in baskets:
        if b["admission"] is None and b["venue"] in ("kalshi", "polymarket"):
            supported.setdefault(b["venue"], []).append(b)
    for bs in supported.values():
        bs.sort(key=lambda b: b["market_id"])
    result = {}
    for b in baskets:
        directions = {"polymarket": ("long", "short"), "kalshi": ("both_bids",), "limitless": ("self_cross",)}.get(b["venue"], ("unsupported",))
        for direction in directions:
            for size in policy["sizes_contracts"]:
                for placebo in ((False, True) if b["venue"] in ("polymarket", "kalshi") else (False,)):
                    descriptor = {**b, "direction": direction, "size_contracts": size, "placebo": placebo, "replaced_leg": None}
                    if placebo and b["admission"] is None:
                        peers = supported[b["venue"]]
                        if len(peers) < 2:
                            descriptor["admission"] = "NO_MATCH"
                        else:
                            other = peers[(peers.index(b) + 1) % len(peers)]
                            descriptor["replaced_leg"] = b["legs"][1]
                            descriptor["legs"] = [b["legs"][0], other["legs"][1]]
                            leg_plans = [plans[x["instrument"], x["orientation"]] for x in descriptor["legs"]]
                            if len({(p["price_scale"], p["quantity_scale"]) for p in leg_plans}) != 1:
                                descriptor["admission"] = "UNSUPPORTED_SCALE"
                    result[digest(descriptor)] = descriptor
    return result


def episode_id(scope, entity, kind, start):
    return digest([scope, entity, kind, str(start)])


def row_order(row, layout):
    d = layout[row["entity"]]
    return (int(row["end_ns"]), row["scope"], d["venue"], d["market_id"], d["direction"],
            int(d["size_contracts"]), d["placebo"], row.get("kind", ""), int(row["start_ns"]))


def reached(duration, policy):
    return [tier for tier in policy["latency_tiers_ns"] if duration >= int(tier)]
