"""Closed configuration and pinned complete-set routes across one venue's markets.

SAME_VENUE_MULTI_MARKET_V1.md. A basket is a set of two to four books on one
venue, from at least two different markets, whose normal-resolution masks
partition one EXHAUSTIVE outcome space. Every leg is bought; a complete set
pays exactly one unit in every outcome.
"""

from replay.strategies.same_venue_complement.contract import (EPISODE_CLASSES, RESOLVER_RESERVATION, VALUE_CLASSES,
                                        reason)
from replay.strategies.cross_venue_arbitrage.contract import SCALE, UNIT, source_key  # noqa: F401  (shared helpers)
from replay.economic_sdk.fills import fill_policy
from replay.economic_sdk.outcomes import outcome_scope
from replay.economic_sdk.types import Basket, Experiment
from replay.preparation import digest, encoded
from replay.strategy_sdk import plain, PreparedInput
from replay.streams.protocol import obj, require, uint

STRATEGY = "same_venue_multi_market_v1"
BASKET_KIND = "SAME_VENUE_MULTI_MARKET_SET"
SETTLEMENT = "normal_resolution_only"
ACCOUNT = "same_venue_multi_market_v1"
VENUES = ("kalshi", "limitless", "polymarket")
DIAGNOSTICS = ("SELF_CROSSED_LEG", "ECONOMICS_UNKNOWN")
FILL_KIND = "net"
TRIGGER_SIZE = "1"
MAX_LEGS = 4
MAX_SETS = 4096
_FIELDS = ("version fills max_legs latency_tiers_ns headline_latency_ns "
           "minimum_net_gap_per_contract_e18 leg_skew_buckets_ns audit_intervals profile")


def _numbers(value, cap, positive):
    require(type(value) is list and 1 <= len(value) <= cap, "policy list budget")
    numbers = [uint(v) for v in value]
    require(numbers == sorted(set(numbers)) and (not positive or numbers[0] > 0),
            "policy list order/positive")
    return numbers


def policy_config(value):
    """Closed policy version 1; fill checks are the only mode."""
    value = obj(plain(value), _FIELDS)
    require(type(value["version"]) is int and value["version"] == 1, "policy version")
    fill_policy(value["fills"], FILL_KIND)
    require(type(value["max_legs"]) is int and 2 <= value["max_legs"] <= MAX_LEGS, "max_legs")
    _numbers(value["latency_tiers_ns"], 8, True)
    require(value["headline_latency_ns"] in value["latency_tiers_ns"], "headline membership")
    _numbers(value["leg_skew_buckets_ns"], 16, False)
    uint(value["minimum_net_gap_per_contract_e18"], 10**36)
    require(type(value["audit_intervals"]) is bool, "policy flag")
    if value["profile"] is not None:
        from replay.economic_sdk.profile import profile_policy
        profile_policy(value["profile"])
    return value


def experiment_identity(snapshot_sha, policy, fees):
    return digest({"strategy": STRATEGY, "entity_contract_version": 1, "native_scales": True,
                   "settlement_model": SETTLEMENT, "snapshot_sha256": snapshot_sha,
                   "policy": policy, "fees": fees})


def experiment(policy, identity):
    return Experiment(
        strategy=STRATEGY, policy=policy, policy_sha256=digest(policy),
        experiment_sha256=identity, tiers_ns=tuple(policy["latency_tiers_ns"]),
        skew_edges_ns=tuple(int(e) for e in policy["leg_skew_buckets_ns"]),
        kinds=("gross", "net"), episode_classes=EPISODE_CLASSES, value_classes=VALUE_CLASSES,
        diagnostic_statuses=DIAGNOSTICS, measurement_fields=(), unevaluated_fields=(),
        maxima=("gap_gross", "gap_net"), slice_invariant=("gap_gross",), layout=2,
        audit_intervals=policy["audit_intervals"], static_reservation=RESOLVER_RESERVATION,
        profile=policy["profile"], fills=fill_policy(policy["fills"], FILL_KIND))


class Inputs:
    def __init__(self, config):
        from replay.strategies._shared.fee_bridge import FeeBridge
        config = obj(plain(config), "version snapshot_directory snapshot_sha256 fees policy")
        require(type(config["version"]) is int and config["version"] == 1, "config version")
        require(len(encoded(config)) <= 8 * 1024 * 1024, "configuration budget")
        self.prepared = PreparedInput({k: config[k] for k in ("version", "snapshot_directory", "snapshot_sha256")})
        self.snapshot = plain(self.prepared.snapshot)
        self.policy = policy_config(config["policy"])
        self.bridge = FeeBridge(config["fees"], self.snapshot["plans"])
        self.identity = experiment_identity(self.prepared.sha256, self.policy, self.bridge.semantic_config)


def baskets(snapshot, policy, scope_index):
    """Every admitted multi-market complete set, and a visible row for each exclusion.

    Rows: one ``NOT_CAPTURED`` per uncaptured member; per venue, one
    ``UNSUPPORTED_SHAPE`` per book that cannot be a leg (not ``MASKED``, or its
    ask source is not planned), then every complete set of two to ``max_legs``
    eligible books from at least two markets. A venue with captured books but no
    such set gets one ``no_multi_market_set`` row; outcomes unavailable gives one
    row per venue. Sets inside a single market are the same-instrument
    complement's domain and are not baskets here.
    """
    scope = snapshot["scopes"][scope_index]
    masks = outcome_scope(snapshot, scope_index)
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    provider = snapshot.get("outcomes", {}).get("provider")
    result = []

    def emit(venue, legs, admission, why, *, label=None):
        acquired = tuple((leg["instrument"], leg["orientation"]) for leg in legs)
        sources = tuple(source_key(k) for k in acquired)
        set_id = digest(legs if label is None else label)
        descriptor = {
            "venue": venue, "basket_kind": BASKET_KIND, "set_id": set_id,
            "market_id": label if label is not None else set_id, "legs": legs,
            "mask_legs": [{"status": masks.status(k)[0], "reason": masks.status(k)[1],
                           "space_shape_id": masks.leg(k).shape_id if masks.leg(k) else None,
                           "claim_id": masks.leg(k).claim_id if masks.leg(k) else None,
                           "negated": masks.leg(k).negated if masks.leg(k) else None} for k in acquired],
            "ask_sources": [{"instrument": s[0], "orientation": s[1],
                             "kind": "projected_opposite_bid" if k[0].startswith("kalshi:") else "native_ask"}
                            for s, k in zip(sources, acquired)],
            "admission": admission, "direction": "buy",
            "size_contracts": None if admission is not None else TRIGGER_SIZE,
            "settlement_model": SETTLEMENT, "outcomes_provider": provider,
        }
        inputs = None if admission is not None else tuple(
            (("kalshi_complement_ask", 1), ("bid", 1)) if k[0].startswith("kalshi:")
            else (("ask", 1), ("crossed", None)) for k in acquired)
        result.append(Basket(descriptor, sources, (venue, set_id, "buy", 0 if admission else 1),
                             admission, tuple(reason(r) for r in why), None, (venue,), set_id, inputs))

    by_venue = {}
    for member in scope["members"]:
        if not member["capture_selected"] or not member["books"]:
            emit(member["market_id"].split(":", 1)[0], [], "NOT_CAPTURED",
                 [{"kind": "not_captured", "market_id": member["market_id"]}], label=member["market_id"])
            continue
        for book in member["books"]:
            row = {"market_id": member["market_id"], **plain(book)}
            by_venue.setdefault(row["instrument"].split(":", 1)[0], []).append(row)
    for venue in sorted(by_venue):
        rows = sorted(by_venue[venue], key=lambda r: (r["instrument"], r["orientation"]))
        if venue not in VENUES:
            emit(venue, [], "UNSUPPORTED_SHAPE", [{"kind": "unsupported_venue"}], label=venue)
            continue
        if not masks.available:
            emit(venue, [], "UNSUPPORTED_SHAPE", [{"kind": "outcomes_unavailable"}], label=venue)
            continue
        eligible = {}
        for row in rows:
            key = row["instrument"], row["orientation"]
            status, detail = masks.status(key)
            if status != "MASKED":
                emit(venue, [row], "UNSUPPORTED_SHAPE", [{"kind": status, "detail": detail}])
            elif source_key(key) not in plans:
                emit(venue, [row], "UNSUPPORTED_SHAPE", [{"kind": "ask_source_not_planned"}])
            else:
                eligible[key] = row
        sets = [s for s in masks.complete_sets(tuple(eligible), max_legs=policy["max_legs"], limit=MAX_SETS)
                if len(s) >= 2 and len({eligible[k]["market_id"] for k in s}) >= 2]
        for keys in sets:
            emit(venue, [eligible[k] for k in keys], None, [])
        if not sets:
            emit(venue, [], "UNSUPPORTED_SHAPE", [{"kind": "no_multi_market_set"}], label=venue)
    return tuple(result)
