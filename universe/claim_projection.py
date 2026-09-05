"""Claims for one run, recomputed from its market projection.

Relations are a property of claims, not of market pairs: `analysis.masks.
relationship` reads two outcome subsets and no other field. So Universe stores
which claim each market expresses and how claims relate, and never a row per
market pair per run.

The projection document already carries every input the mask engine reads --
`venue_markets` has title, parameters, outcome labels and subscription ids;
`venue_events` has the series format; `events` has the participants -- so claims
are recomputed here from the run's own projection, with no database read and no
targeter change. That is what lets the whole archive re-project.

Recomputation is the risk: a bundle rebuilt here must compile the same masks the
targeter compiled at report time. `verify_claims` is the standing check, and it
is deliberately asymmetric. A relation the claims *invent* is a guessed
cross-venue equivalence and rejects the run. A relation the claims *miss* is a
visible false negative, which `AGENTS.md` prefers to a guess, so it is counted
and surfaced rather than raised -- an unreachable claim costs coverage, not
correctness.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from analysis.claims import (
    CLAIM_ALGEBRA_VERSION,
    CLAIM_IDENTITY_VERSION,
    UNINFORMATIVE_RELATIONS,
    implied_market_relations,
)
from targeter.v2.models import (
    ActivationEvidence,
    CanonicalEvent,
    CanonicalMarket,
    ClassificationEvidence,
    EventBundle,
    parse_timestamp,
)
from targeter.v2.relationships import derive_bundle_claims
from universe.market_projection import MarketProjectionError


_RECONSTRUCTED_CLASSIFICATION = (
    ClassificationEvidence(
        "universe-claim-reconstruction", "reconstructed", "reconstructed"
    ),
)


def project_claims(projection: Mapping[str, Any]) -> dict[str, Any]:
    """Return the claim rows one run's projection implies.

    Raises ``MarketProjectionError`` when the recomputed claims assert a
    cross-venue relation the report did not record.
    """
    events = {row["event_id"]: row for row in projection["events"]}
    venue_events: dict[str, list[Mapping[str, Any]]] = {}
    for row in projection["venue_events"]:
        venue_events.setdefault(row["event_id"], []).append(row)
    venue_markets: dict[str, list[Mapping[str, Any]]] = {}
    for row in projection["venue_markets"]:
        venue_markets.setdefault(row["event_id"], []).append(row)

    claims: dict[str, dict[str, Any]] = {}
    relations: dict[tuple[str, str, str], dict[str, Any]] = {}
    market_claims: dict[tuple[str, str, str], dict[str, Any]] = {}
    implied: set[tuple[str, str, str]] = set()

    for event_id in sorted(events):
        bundle = _bundle(events[event_id], venue_events.get(event_id, ()),
                         venue_markets.get(event_id, ()))
        if bundle is None:
            continue
        groups = derive_bundle_claims(bundle)
        implied |= implied_market_relations(groups)
        for group in groups:
            for claim in group.claims:
                claims.setdefault(
                    claim.claim_id,
                    {
                        "claim_id": claim.claim_id,
                        "space_shape_id": claim.space_shape_id,
                        "scope": claim.scope,
                        "coverage": group.coverage,
                        "outcome_key_count": len(claim.outcome_keys),
                        "claim_identity_version": CLAIM_IDENTITY_VERSION,
                    },
                )
                for mask in claim.members:
                    venue, venue_market_id, claim_key = _split(mask.market_key)
                    market_claims.setdefault(
                        (venue, venue_market_id, claim_key),
                        {
                            "venue": venue,
                            "venue_market_id": venue_market_id,
                            "claim_key": claim_key,
                            "claim_id": claim.claim_id,
                            "event_id": event_id,
                        },
                    )
            for relation in group.relations:
                key = (
                    relation.space_shape_id,
                    relation.left_claim_id,
                    relation.right_claim_id,
                )
                relations.setdefault(
                    key,
                    {
                        "space_shape_id": relation.space_shape_id,
                        "left_claim_id": relation.left_claim_id,
                        "right_claim_id": relation.right_claim_id,
                        "relation_type": relation.relation_type,
                        "algebra_version": CLAIM_ALGEBRA_VERSION,
                    },
                )

    shortfall = verify_claims(implied, projection["relations"])
    return {
        "claims": [claims[key] for key in sorted(claims)],
        "claim_relations": [relations[key] for key in sorted(relations)],
        "market_claims": [market_claims[key] for key in sorted(market_claims)],
        "relation_shortfall": shortfall,
    }


def verify_claims(
    implied: set[tuple[str, str, str]],
    recorded: Sequence[Mapping[str, Any]],
) -> int:
    """Compare recomputed claims against the relations the report recorded.

    Returns the number of recorded relations the claims did not reproduce.
    Raises when the claims assert one the report does not have.
    """
    expected: set[tuple[str, str, str]] = set()
    for row in recorded:
        if row["relation_type"] in UNINFORMATIVE_RELATIONS:
            continue
        members = row["members"]
        if len(members) != 2:
            continue
        if members[0]["venue"] == members[1]["venue"]:
            continue
        keys = [_mask_key(member) for member in members]
        kind = row["relation_type"]
        ordered = sorted(keys)
        # Directed relations are written from the member the report named
        # first; flip when sorting reverses that pair, so both sides of the
        # comparison speak the same convention.
        if ordered[0] != keys[0] and kind in {"IMPLICATION", "REVERSE_IMPLICATION"}:
            kind = "REVERSE_IMPLICATION" if kind == "IMPLICATION" else "IMPLICATION"
        expected.add((*ordered, kind))

    invented = implied - expected
    if invented:
        sample = sorted(invented)[:3]
        raise MarketProjectionError(
            f"recomputed claims assert {len(invented)} cross-venue relations the "
            f"report does not record, for example {sample}"
        )
    return len(expected - implied)


def _mask_key(member: Mapping[str, Any]) -> str:
    """The mask key a projected relation member names."""
    base = f"{member['venue']}:{member['venue_market_id']}"
    claim_key = member.get("claim_key") or ""
    return f"{base}#{claim_key}" if claim_key else base


def _split(market_key: str) -> tuple[str, str, str]:
    base, marker, claim = str(market_key).partition("#")
    venue, separator, native = base.partition(":")
    if not separator or not venue or not native:
        raise MarketProjectionError(f"claim member {market_key} is invalid")
    return venue, native, claim if marker else ""


def _bundle(
    event: Mapping[str, Any],
    venue_events: Sequence[Mapping[str, Any]],
    venue_markets: Sequence[Mapping[str, Any]],
) -> EventBundle | None:
    """Rebuild the bundle the targeter compiled, from projection rows alone.

    Returns None when the rows cannot form one, which yields no claims for that
    event rather than a guess.
    """
    participants = tuple(event["participants"])
    if len(participants) != 2 or not venue_events or not venue_markets:
        return None
    activation_at = parse_timestamp(event["activation_at"])
    try:
        events = tuple(
            CanonicalEvent(
                venue=row["venue"],
                venue_event_id=row["venue_event_id"],
                sport=event["sport"],
                league=row.get("league"),
                title=row["title"],
                # venue_events carries no participants of its own; the umbrella
                # event's are what `_side_label` resolves a side against, and it
                # maps straight back to the bundle's own list.
                participants=participants,
                activation_at=activation_at,
                status=row["status"],
                source_ref=row["source_ref"],
                format=row.get("format"),
                fragment_type=row.get("fragment_type"),
                game=event.get("game"),
                topology=event.get("topology"),
                # `CanonicalEvent` requires classification and activation
                # evidence for structured esports events. That is the targeter's
                # provenance for *how* it classified, which the projection does
                # not carry and which no mask resolver reads. These stand-ins
                # satisfy the constructor and are named so they can never be
                # mistaken for real evidence; they are never persisted.
                game_evidence=_RECONSTRUCTED_CLASSIFICATION,
                activation_evidence=(
                    ActivationEvidence(
                        activation_at, "structured", "reconstructed", True
                    ),
                ),
            )
            for row in venue_events
        )
        markets = tuple(
            CanonicalMarket(
                venue=row["venue"],
                venue_market_id=row["venue_market_id"],
                venue_event_id=row["venue_event_id"],
                canonical_class=row["canonical_class"],
                market_type=row["market_type"],
                scope=row["scope"],
                title=row["title"],
                parameters=dict(row["parameters"]),
                subscription_ids=tuple(row["subscription_ids"]),
                outcome_labels=tuple(row["outcome_labels"]),
                status=row["status"],
                accepting_orders=bool(row["accepting_orders"]),
                source_ref=row["source_ref"],
            )
            for row in venue_markets
        )
        return EventBundle(
            bundle_id=event["source_bundle_id"],
            sport=event["sport"],
            participants=participants,
            participant_keys=tuple(event["participant_keys"]),
            activation_at=activation_at,
            events=events,
            markets=markets,
            confidence="structured",
            game=event.get("game"),
            topology=event.get("topology"),
        )
    except (ValueError, KeyError, TypeError):
        # A bundle that cannot be rebuilt contributes no claims. The shortfall
        # count in `verify_claims` makes that visible rather than silent.
        return None
