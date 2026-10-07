"""Static mask proofs and bounded routes for both implication-cover variants."""

from dataclasses import replace
from itertools import combinations, product

from replay.complement_contract import reason
from replay.cross_venue_contract import (
    Inputs as BuyInputs, SETTLEMENT, experiment as buy_experiment, sizes, source_key,
)
from replay.economic_sdk import Basket
from replay.economic_sdk.outcomes import outcome_scope
from replay.preparation import digest
from replay.strategy_sdk import plain
from replay.streams.protocol import require

STRATEGIES = {"same_venue": "same_venue_implication_cover_v1",
              "cross_venue": "cross_venue_implication_cover_v1"}
FACTORIES = {mode: "replay." + strategy.removesuffix("_v1") + ":build"
             for mode, strategy in STRATEGIES.items()}
MAX_PAIRS = 4096


def experiment_identity(snapshot_sha, policy, fees, valuation, mode):
    require(mode in STRATEGIES, "implication venue mode")
    return digest({"strategy": STRATEGIES[mode], "venue_mode": mode,
                   "bridge_version": 1, "entity_contract_version": 1, "native_scales": True,
                   "settlement_model": SETTLEMENT, "snapshot_sha256": snapshot_sha,
                   "policy": policy, "fees": fees, "valuation": valuation})


def experiment(policy, identity, mode):
    require(mode in STRATEGIES, "implication venue mode")
    return replace(buy_experiment(policy, identity), strategy=STRATEGIES[mode])


class Inputs(BuyInputs):
    def __init__(self, config, mode):
        super().__init__(config)
        self.identity = experiment_identity(self.prepared.sha256, self.policy,
                                            self.bridge.semantic_config, self.valuation, mode)


def payoff(proof, quantities):
    """Exact acquired-token payouts, aligned to the proof's outcome keys."""
    return [sum(quantity * pays for quantity, pays in zip(quantities, state))
            for state in proof["leg_payoffs"]]


def baskets(snapshot, policy, scope_index, mode):
    require(mode in STRATEGIES, "implication venue mode")
    scope, masks = snapshot["scopes"][scope_index], outcome_scope(snapshot, scope_index)
    plans = {(p["instrument"], p["orientation"]) for p in snapshot["plans"]}
    books, result = [], []

    def emit(legs, admission, reasons, proof=None, missing_market=None):
        keys = tuple((leg["instrument"], leg["orientation"]) for leg in legs)
        sources = tuple(source_key(key) for key in keys)
        venue = "+".join(sorted({key[0].split(":", 1)[0] for key in keys}))
        if not venue:
            venue = missing_market.split(":", 1)[0] if missing_market else "unavailable"
        route = digest({"mode": mode, "legs": legs, "missing_market": missing_market})
        for size in ([None] if admission else sizes(policy)):
            descriptor = {"venue": venue, "venue_mode": mode, "basket_kind": "IMPLICATION_COVER",
                "market_id": missing_market or route, "route_id": route, "direction": "buy",
                "size_contracts": size, "legs": legs, "admission": admission, "implication": proof,
                "mask_legs": [{"status": masks.status(k)[0], "reason": masks.status(k)[1],
                    "space_shape_id": masks.leg(k).shape_id if masks.leg(k) else None,
                    "claim_id": masks.leg(k).claim_id if masks.leg(k) else None,
                    "negated": masks.leg(k).negated if masks.leg(k) else None} for k in keys],
                "ask_sources": [{"instrument": k[0], "orientation": k[1],
                    "kind": "projected_opposite_bid" if a[0].startswith("kalshi:") else "native_ask"}
                    for k, a in zip(sources, keys)],
                "settlement_model": SETTLEMENT,
                "outcomes_provider": snapshot.get("outcomes", {}).get("provider")}
            inputs = None if admission else tuple(
                (("kalshi_complement_ask", int(size)), ("bid", int(size)))
                if k[0].startswith("kalshi:") else (("ask", int(size)), ("crossed", None)) for k in keys)
            result.append(Basket(descriptor, sources, (venue, route, "buy", 0 if size is None else int(size)),
                admission, tuple(reason(r) for r in reasons), 1 if len(legs) == 2 else None,
                (venue, size), route, inputs))

    for member in scope["members"]:
        if not member["capture_selected"] or not member["books"]:
            emit([], "NOT_CAPTURED", [{"kind": "not_captured", "market_id": member["market_id"]}],
                 missing_market=member["market_id"])
        for book in member["books"]:
            books.append(({"market_id": member["market_id"], **plain(book)}, member["capture_selected"]))
    books.sort(key=lambda row: (row[0]["instrument"], row[0]["orientation"]))
    by_venue = {}
    for row in books:
        by_venue.setdefault(row[0]["instrument"].split(":", 1)[0], []).append(row)
    if mode == "same_venue":
        count = sum(len(rows) * (len(rows) - 1) // 2 for rows in by_venue.values())
        require(count <= MAX_PAIRS, "implication candidate pair limit exceeded")
        candidates = (pair for venue in sorted(by_venue) for pair in combinations(by_venue[venue], 2))
    else:
        groups = list(combinations(sorted(by_venue), 2))
        count = sum(len(by_venue[a]) * len(by_venue[b]) for a, b in groups)
        require(count <= MAX_PAIRS, "implication candidate pair limit exceeded")
        candidates = (pair for a, b in groups for pair in product(by_venue[a], by_venue[b]))
    for (a, captured_a), (b, captured_b) in sorted(candidates,
            key=lambda pair: tuple((r[0]["instrument"], r[0]["orientation"]) for r in pair)):
        keys = tuple((leg["instrument"], leg["orientation"]) for leg in (a, b))
        why, proof = [], None
        admission = "UNSUPPORTED_SHAPE"
        if not captured_a or not captured_b:
            admission, why = "NOT_CAPTURED", [{"kind": "not_captured"}]
        elif not masks.available:
            why = [{"kind": "outcomes_unavailable"}]
        elif any(masks.status(k)[0] != "MASKED" for k in keys):
            why = [{"leg": i, "kind": masks.status(k)[0], "detail": masks.status(k)[1]}
                   for i, k in enumerate(keys) if masks.status(k)[0] != "MASKED"]
        elif a["market_id"] == b["market_id"]:
            why = [{"kind": "same_market"}]
        else:
            left, right = (masks.leg(k) for k in keys)
            space = masks.spaces[left.shape_id]
            if left.shape_id != right.shape_id:
                why = [{"kind": "mixed_space"}]
            elif space.coverage != "EXHAUSTIVE":
                why = [{"kind": "incomplete_space"}]
            elif left.keys | right.keys != frozenset(space.keys):
                why = [{"kind": "gap"}]
            elif not left.keys & right.keys:
                why = [{"kind": "identity_cover"}]
            elif any(source_key(k) not in plans for k in keys):
                why = [{"kind": "ask_source_not_planned"}]
            elif any(k[0].split(":", 1)[0] not in {"kalshi", "polymarket", "limitless"} for k in keys):
                why = [{"kind": "unsupported_venue"}]
            else:
                # Leg 0 pays B; leg 1 pays not A. Full coverage plus a nonempty
                # intersection proves A is a STRICT subset of B. Complementary
                # identity routes belong to the existing complete-set strategy.
                proof = {"space_shape_id": space.shape_id, "outcome_keys": list(space.keys),
                    "antecedent_keys": sorted(frozenset(space.keys) - right.keys),
                    "consequent_keys": sorted(left.keys), "middle_keys": sorted(left.keys & right.keys),
                    "leg_payoffs": [[int(k in left.keys), int(k in right.keys)] for k in space.keys],
                    "payout_per_contract": list(masks.payoff(keys)[space.shape_id])}
                admission = None
        emit([a, b], admission, why, proof)
    if not result:
        emit([], "UNSUPPORTED_SHAPE", [{"kind": "no_" + mode + "_pair"}])
    return tuple(result)
