"""Read-time normal-resolution masks from the existing catalogue projection."""

from analysis.claims import (
    CLAIM_ALGEBRA_VERSION,
    CLAIM_IDENTITY_VERSION,
    claim_id,
    space_shape_id,
)
from targeter.v2.relationships import _meaningful_labels, _spaces, market_scope_masks
from universe.claims.claim_projection import rebuild_bundle


def _tokens(market, claims):
    subscriptions = market.subscription_ids
    if len(subscriptions) != len(set(subscriptions)):
        return []
    if (
        market.venue in {"kalshi", "limitless"}
        and len(subscriptions) == len(claims) == 1
    ):
        return [
            {
                "subscription_id": subscriptions[0],
                "claim_key": "claim=0",
                "negated": False,
            }
        ]
    if market.venue == "polymarket":
        if len(_meaningful_labels(market)) == len(subscriptions) == len(claims) >= 2:
            return [
                {"subscription_id": token, "claim_key": f"claim={i}", "negated": False}
                for i, token in enumerate(subscriptions)
            ]
        labels = [label.casefold() for label in market.outcome_labels]
        if (
            len(subscriptions) == 2
            and set(labels) == {"yes", "no"}
            and len(labels) == 2
            and len(claims) == 1
        ):
            return [
                {
                    "subscription_id": token,
                    "claim_key": "claim=0",
                    "negated": label == "no",
                }
                for token, label in zip(subscriptions, labels)
            ]
    return []


def bundle_document(event, venue_events, venue_markets, *, recorded_claim_matches):
    """No I/O or selection exclusions; the caller owns bounded, consistent reads."""
    from universe.store import DetailTooLarge

    bundle = rebuild_bundle(event, venue_events, venue_markets)
    spaces, diagnostics = _spaces(bundle) if bundle is not None else ((), ())
    if len(spaces) > 8 or any(len(space.keys) > 1024 for space in spaces):
        raise DetailTooLarge("bundle outcomes exceeds the space limit")
    shape_spaces = {space_shape_id(s): s for s in spaces}
    compiled = {
        shape: {
            m.target_id: (masks, reason)
            for m, masks, reason in market_scope_masks(bundle, space)
        }
        for shape, space in shape_spaces.items()
    }
    claims, markets = {}, []
    canonical_markets = {m.target_id: m for m in bundle.markets} if bundle else {}
    for row in venue_markets:
        mid = row["venue"] + ":" + row["venue_market_id"]
        market = canonical_markets.get(mid)
        status, reason, refs, tokens = "NO_SPACE", None, [], []
        if bundle is not None:
            candidates = [
                (shape, *results[mid])
                for shape, results in compiled.items()
                if results[mid][1] != "DIFFERENT_SCOPE"
            ]
            if row["status"].casefold() in {"void", "voided", "cancelled", "canceled"}:
                status = "VOID_UNSUPPORTED"
            elif not candidates:
                expected_series = row["scope"] == "series" or (
                    bundle.game
                    and row["market_type"]
                    in {"map_winner", "total_maps", "map_handicap"}
                )
                status = (
                    "NO_SPACE" if expected_series and diagnostics else "DIFFERENT_SCOPE"
                )
            else:
                shape, masks, rejection = candidates[0]
                status = "MASKED"
                if rejection is not None:
                    status, reason = "REJECTED", rejection
                elif not masks:
                    status = "UNSUPPORTED"
                elif any(not mask.derivable for mask in masks):
                    status = next(mask.status for mask in masks if not mask.derivable)
                elif any(
                    not mask.outcome_keys
                    or mask.outcome_keys == shape_spaces[shape].keys
                    for mask in masks
                ):
                    status = "TAUTOLOGY"
                else:
                    refs = sorted(
                        [
                            {
                                "claim_key": mask.market_key.split("#", 1)[1],
                                "claim_id": claim_id(mask.outcome_keys, shape),
                            }
                            for mask in masks
                        ],
                        key=lambda r: r["claim_key"],
                    )
                    if any(
                        not recorded_claim_matches(
                            row["venue"],
                            row["venue_market_id"],
                            ref["claim_key"],
                            ref["claim_id"],
                        )
                        for ref in refs
                    ):
                        status, reason = "CLAIM_CONFLICT", "recorded_claim_mismatch"
                    else:
                        tokens = _tokens(market, refs)
                        if not tokens:
                            status, reason = "UNSUPPORTED", "token_alignment"
                    if status == "MASKED":
                        for mask in masks:
                            cid = claim_id(mask.outcome_keys, shape)
                            claims[cid] = {
                                "claim_id": cid,
                                "space_shape_id": shape,
                                "outcome_keys": sorted(mask.outcome_keys),
                            }
                            if len(claims) > 4096:
                                raise DetailTooLarge(
                                    "bundle outcomes exceeds the claim limit"
                                )
        markets.append(
            {
                "market_id": mid,
                "venue": row["venue"],
                "market_type": row["market_type"],
                "market_status": row["status"],
                "subscription_ids": list(row["subscription_ids"]),
                "outcome_labels": list(row["outcome_labels"]),
                "mask_status": status,
                "reason": reason,
                "claims": refs if status == "MASKED" else [],
                "tokens": tokens if status == "MASKED" else [],
            }
        )
    return {
        "version": 1,
        "bundle_id": event["source_bundle_id"],
        "event_id": event["event_id"],
        "identities": {
            "claim_identity_version": CLAIM_IDENTITY_VERSION,
            "claim_algebra_version": CLAIM_ALGEBRA_VERSION,
        },
        "status": "complete" if bundle else "unreconstructed",
        "diagnostics": sorted(diagnostics),
        "participants": list(event["participants"]),
        "spaces": [
            {
                "space_shape_id": shape,
                "scope": space.scope,
                "coverage": space.coverage,
                "best_of": space.metadata.get("best_of")
                if space.scope == "series"
                else None,
                "outcome_keys": sorted(space.keys),
            }
            for shape, space in sorted(shape_spaces.items())
        ],
        "claims": [claims[c] for c in sorted(claims)],
        "markets": sorted(markets, key=lambda m: m["market_id"]),
    }
