"""Volatility V1 declarative mask tables and resource limits; no time-series arithmetic."""

from replay.economic_sdk import bounds
from replay.economic_sdk.outcomes import OutcomeScope
from replay.streams.protocol import require

MAX_PAIRS = 4096  # across all scopes, not a truncated discovery result
MAX_PENDING = 1_000_000
MAX_BUCKETS = 100_000
PENDING_COST = 512


def accumulator_cost(policy):
    """Conservative cost of a bucket/total, including all horizon and jump maps."""
    return 2048 + len(policy["horizons_ms"]) * (
        1024 + sum(256 + 8 * len(str(j)) for j in policy["jump_atoms"]))


def pair_tables(snapshot, books, *, enabled=True, limit=MAX_PAIRS):
    """Scope-pinned different-market MASKED complements in canonical book/side order.

    Sides name the bought native orientation. A Kalshi written Yes row represents
    its Yes ask and its No ask (unit minus Yes bid); the No mask is used only when
    its native key is actually scoped. Unavailable masks remain candidate evidence.
    """
    if not enabled:
        return [], []
    pairs, unavailable = [], []
    for scope_index, scope in enumerate(snapshot["scopes"]):
        native = {(b["instrument"], b["orientation"]) for m in scope["members"] for b in m["books"]}
        outcomes = OutcomeScope(snapshot, scope_index)
        candidates = []
        for index, book in enumerate(books):
            key = book["instrument"], book["orientation"]
            if key not in native:
                continue
            sides = [key[1]]
            other = "complement" if key[1] == "outcome" else "outcome"
            if book["venue"] == "kalshi" and (key[0], other) in native:
                sides.append(other)
            for side in sorted(sides):
                leg_key = key[0], side
                leg = outcomes.leg(leg_key)
                status, reason = outcomes.status(leg_key)
                if leg is None or outcomes.spaces[leg.shape_id].coverage != "EXHAUSTIVE":
                    unavailable.append({"scope": scope_index, "book": index, "side": side,
                                        "status": status if leg is None else "INCOMPLETE_COVERAGE",
                                        "reason": reason})
                    continue
                mask = {"space_shape_id": leg.shape_id, "claim_id": leg.claim_id, "negated": leg.negated}
                candidates.append((index, side, book["market_id"], leg.shape_id, leg.keys, mask))
        candidates.sort(key=lambda c: c[:2])
        by_mask = {}
        for candidate in candidates:
            by_mask.setdefault((candidate[3], candidate[4]), []).append(candidate)
        for a in candidates:
            complement = frozenset(outcomes.spaces[a[3]].keys) - a[4]
            for b in by_mask.get((a[3], complement), ()):
                if a[:2] >= b[:2] or a[2] == b[2]:
                    continue
                require(len(pairs) < limit, "volatility gap pair budget")
                pairs.append({"scope": scope_index, "pair": len(pairs),
                              "book_a": a[0], "side_a": a[1], "book_b": b[0], "side_b": b[1],
                              "market_a": a[2], "market_b": b[2], "mask_a": a[5], "mask_b": b[5]})
    require(bounds.json_cost(pairs) + bounds.json_cost(unavailable) <= bounds.MAX_METADATA,
            "volatility mask table budget")
    return pairs, unavailable
