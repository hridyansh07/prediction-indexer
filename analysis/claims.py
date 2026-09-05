"""Claim identity and the relation algebra derived from an outcome space.

A market's semantic content, for relationship purposes, is the subset of Omega
it resolves YES on. Two markets naming the same subset are the same *claim*
however their venues word them, and the relationship between two markets is a
function of their two subsets and nothing else — ``analysis.masks.relationship``
reads no other field.

Outcome keys are participant-independent: ``build_series_space`` keys outcomes
``seq:{sequence}`` and ``build_score_space`` keys them ``score:{h}-{a}``, so a
best-of-5 space carries the same twenty keys whoever is playing. A claim is
therefore identified globally by its key set, and the algebra over claims can be
computed once per space shape instead of once per market pair per run.

``Mask.resolver`` is a readable label, not an identity: ``maps_over`` at 2.5 and
at 3.5 share it while denoting different subsets whose relation is IMPLICATION.
Identity is always the key set.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import combinations
from typing import Any

from analysis.masks import IDENTITY, Mask, relationship
from analysis.outcome_space import OutcomeSpace


# Bumped when claim identity changes meaning. Stored alongside derived rows so a
# database built under an older rule is detectable rather than silently mixed.
CLAIM_IDENTITY_VERSION = 1

# Bumped when the pairwise classification changes. Independent of identity: the
# same claims can be re-related without their identities moving.
CLAIM_ALGEBRA_VERSION = 1

# Relations that carry no discovery evidence. OVERLAP is the catch-all `else`
# branch of `relationship()` rather than a finding, and the targeter's own
# scorer excludes it (`targeter/v2/selection.py`).
UNINFORMATIVE_RELATIONS = frozenset({"OVERLAP"})

# Relations whose meaning depends on which member is which. The projection
# records the distinction in `relation_members.role`; symmetric types collapse
# every member to role `member`.
DIRECTED_RELATIONS = frozenset({"IMPLICATION", "REVERSE_IMPLICATION"})


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        .encode("utf-8")
    ).hexdigest()


def claim_id(outcome_keys: Iterable[str]) -> str:
    """Identify a claim by the outcome subset it resolves YES on.

    Participant-independent, so the same claim in two different events of the
    same shape yields the same id. Two claims in *different* shapes draw from
    different key universes and so never collide.
    """
    keys = sorted(str(key) for key in outcome_keys)
    if not keys:
        raise ValueError("a claim must name at least one outcome")
    return _digest({"version": CLAIM_IDENTITY_VERSION, "outcome_keys": keys})


def space_shape_id(space: OutcomeSpace) -> str:
    """Identify an outcome space by its structure, not its participants.

    The key vocabulary is exactly what claims are drawn from and what the
    algebra is valid over, so it *is* the shape. Deriving the id from the keys
    rather than from ``best_of``/cap metadata keeps this correct for any future
    space builder without a per-sport table.
    """
    return _digest(
        {
            "version": CLAIM_IDENTITY_VERSION,
            "scope": space.scope,
            "outcome_keys": sorted(space.keys),
        }
    )


@dataclass(frozen=True)
class Claim:
    """One distinct outcome subset, with the masks that expressed it."""

    claim_id: str
    space_shape_id: str
    scope: str
    outcome_keys: frozenset[str]
    members: tuple[Mask, ...]

    @property
    def venues(self) -> frozenset[str]:
        return frozenset(mask.venue for mask in self.members)

    @property
    def cross_venue(self) -> bool:
        """Whether this claim is listed at more than one venue.

        A cross-venue claim is the equivalence the system exists to find; under
        the pairwise model the same fact appeared as IDENTITY edges between
        every pair of its members.
        """
        return len(self.venues) > 1


@dataclass(frozen=True)
class ClaimRelation:
    """How two claims of one space shape relate. Independent of any event."""

    space_shape_id: str
    left_claim_id: str
    right_claim_id: str
    relation_type: str

    @property
    def informative(self) -> bool:
        return self.relation_type not in UNINFORMATIVE_RELATIONS


def usable_masks(masks: Iterable[Mask], space: OutcomeSpace) -> tuple[Mask, ...]:
    """The masks that may participate in the algebra.

    Mirrors the filter in ``derive_bundle_relationships``: derivable, and neither
    empty nor the whole space, since a tautology relates to everything.
    """
    total = len(space.outcomes)
    return tuple(
        mask
        for mask in masks
        if mask.derivable and 0 < len(mask.outcome_keys) < total
    )


def derive_claims(masks: Iterable[Mask], space: OutcomeSpace) -> tuple[Claim, ...]:
    """Collapse masks to distinct claims, ordered by id for determinism."""
    shape = space_shape_id(space)
    grouped: dict[str, list[Mask]] = {}
    keys: dict[str, frozenset[str]] = {}
    for mask in usable_masks(masks, space):
        identifier = claim_id(mask.outcome_keys)
        grouped.setdefault(identifier, []).append(mask)
        keys[identifier] = mask.outcome_keys
    return tuple(
        Claim(
            claim_id=identifier,
            space_shape_id=shape,
            scope=space.scope,
            outcome_keys=keys[identifier],
            members=tuple(
                sorted(members, key=lambda mask: (mask.venue, mask.market_key))
            ),
        )
        for identifier, members in sorted(grouped.items())
    )


def derive_claim_algebra(
    claims: Sequence[Claim],
    *,
    informative_only: bool = True,
) -> tuple[ClaimRelation, ...]:
    """Relate every pair of claims in one space shape.

    This is the whole relation model: quadratic in *claims*, which is bounded by
    the space, rather than in market instances, which is not. It depends on no
    event, run, or venue, so its result is reusable wherever that shape occurs.
    """
    shapes = {claim.space_shape_id for claim in claims}
    if len(shapes) > 1:
        raise ValueError("claims from different space shapes are not comparable")
    out: list[ClaimRelation] = []
    for left, right in combinations(sorted(claims, key=lambda c: c.claim_id), 2):
        kind = relationship(_probe(left), _probe(right))
        if kind is None:
            continue
        if informative_only and kind in UNINFORMATIVE_RELATIONS:
            continue
        out.append(
            ClaimRelation(
                space_shape_id=left.space_shape_id,
                left_claim_id=left.claim_id,
                right_claim_id=right.claim_id,
                relation_type=kind,
            )
        )
    return tuple(out)


def _probe(claim: Claim) -> Mask:
    """A minimal mask carrying only what ``relationship`` reads."""
    return Mask(
        market_key=claim.claim_id,
        venue="",
        market_type="",
        scope=claim.scope,
        status="DERIVABLE",
        outcome_keys=claim.outcome_keys,
        resolver="claim",
    )


# ---------------------------------------------------------------------------
# Independent derivation, for cross-checking the model against stored evidence
# ---------------------------------------------------------------------------


def classes_from_identity_edges(
    edges: Iterable[tuple[str, str]],
    *,
    members: Iterable[str] = (),
) -> tuple[frozenset[str], ...]:
    """Partition mask keys by the IDENTITY edges recorded for a bundle.

    ``relationship`` returns IDENTITY exactly when two key sets are equal, so the
    IDENTITY subgraph is a disjoint union of complete cliques and its connected
    components are the claim classes. This reconstructs the partition from an
    already-recorded edge list, without recompiling any mask — an independent
    check on ``derive_claims``.
    """
    parent: dict[str, str] = {}

    def find(node: str) -> str:
        parent.setdefault(node, node)
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def union(left: str, right: str) -> None:
        a, b = find(left), find(right)
        if a != b:
            parent[b] = a

    for node in members:
        find(node)
    for left, right in edges:
        union(left, right)

    grouped: dict[str, set[str]] = {}
    for node in parent:
        grouped.setdefault(find(node), set()).add(node)
    return tuple(sorted((frozenset(group) for group in grouped.values()), key=sorted))


def identity_clique_defects(
    components: Sequence[frozenset[str]],
    edges: Iterable[tuple[str, str]],
) -> tuple[str, ...]:
    """Report components whose IDENTITY edges do not form a complete clique.

    A component of size k must carry exactly k(k-1)/2 IDENTITY edges. A shortfall
    means the recorded edge list is not the transitive closure it is assumed to
    be, and the partition cannot be trusted.
    """
    seen: dict[frozenset[str], int] = {}
    for left, right in edges:
        seen[frozenset((left, right))] = seen.get(frozenset((left, right)), 0) + 1
    defects: list[str] = []
    for component in components:
        size = len(component)
        if size < 2:
            continue
        expected = size * (size - 1) // 2
        found = sum(
            1 for pair in combinations(sorted(component), 2) if frozenset(pair) in seen
        )
        if found != expected:
            defects.append(
                f"component of {size} masks carries {found} IDENTITY edges, expected {expected}"
            )
    return tuple(defects)


def compare_partitions(
    derived: Sequence[Claim],
    recorded: Sequence[frozenset[str]],
) -> tuple[str, ...]:
    """Diff claim classes computed from key sets against those from edges.

    The two derivations share no code path, so any disagreement is a real defect
    in one of them. Returns human-readable differences, empty when they agree.
    """
    left = {frozenset(mask.market_key for mask in claim.members) for claim in derived}
    right = {frozenset(group) for group in recorded}
    problems: list[str] = []
    for group in sorted(left - right, key=sorted):
        problems.append(f"only in key-set derivation: {sorted(group)}")
    for group in sorted(right - left, key=sorted):
        problems.append(f"only in identity-edge derivation: {sorted(group)}")
    return tuple(problems)


def relation_members_to_edges(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[tuple[str, str], ...]:
    """Build an IDENTITY edge list from stored ``relation_members`` rows.

    Rows must already be restricted to IDENTITY relations. Members are keyed the
    way the projection writes them — ``venue:venue_market_id#claim_key`` — so the
    partition lands at claim granularity rather than market granularity.
    """
    grouped: dict[Any, list[str]] = {}
    for row in rows:
        member = f"{row['venue']}:{row['venue_market_id']}"
        claim_key = row.get("claim_key") or ""
        if claim_key:
            member = f"{member}#{claim_key}"
        grouped.setdefault(row["relation_id"], []).append(member)
    edges: list[tuple[str, str]] = []
    for members in grouped.values():
        for left, right in combinations(sorted(members), 2):
            edges.append((left, right))
    return tuple(edges)


def normalize_relation(
    relation_type: str,
    member_claims: Iterable[tuple[str, str]],
) -> tuple[str, ...]:
    """Reduce a recorded relation to one descriptor per fact.

    ``member_claims`` pairs each member's ``relation_members.role`` with the
    claim it belongs to.

    IMPLICATION and REVERSE_IMPLICATION are the same statement written from
    opposite ends: ``relationship`` returns IMPLICATION when the left member's
    subset is contained in the right's, and REVERSE_IMPLICATION when it is the
    other way round. So a bundle that records ``p -> q`` and another that records
    ``q <- p`` are agreeing, not contradicting, and comparing the raw labels over
    an unordered pair reports a conflict that is not there.

    Normalizing to ``(antecedent, consequent)`` removes that false conflict while
    keeping a real one visible: two claims genuinely disagreeing about which
    contains which still produce two different descriptors.
    """
    pairs = tuple(member_claims)
    if relation_type in DIRECTED_RELATIONS:
        by_role = {role: claim for role, claim in pairs}
        if set(by_role) != {"left", "right"}:
            raise ValueError(
                f"{relation_type} needs one left and one right member, got {sorted(by_role)}"
            )
        if relation_type == "IMPLICATION":
            antecedent, consequent = by_role["left"], by_role["right"]
        else:
            antecedent, consequent = by_role["right"], by_role["left"]
        return ("IMPLICATION", antecedent, consequent)
    return (relation_type, *sorted({claim for _role, claim in pairs}))


def relation_agreement_defects(
    relations: Iterable[tuple[str, Sequence[tuple[str, str]]]],
) -> tuple[str, ...]:
    """Report claim pairs whose recorded relations do not agree.

    A relation is a function of the two claims' outcome subsets, so every
    relation recorded between one pair of claims must reduce to one descriptor.
    More than one falsifies the claim model for that pair.
    """
    seen: dict[frozenset[str], set[tuple[str, ...]]] = {}
    for relation_type, member_claims in relations:
        claims = {claim for _role, claim in member_claims}
        if len(claims) != 2:
            continue
        seen.setdefault(frozenset(claims), set()).add(
            normalize_relation(relation_type, member_claims)
        )
    defects: list[str] = []
    for pair, descriptors in seen.items():
        if len(descriptors) > 1:
            defects.append(
                f"claims {sorted(pair)} carry disagreeing relations "
                f"{sorted(descriptors)}"
            )
    return tuple(sorted(defects))
