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

from analysis.masks import (
    IDENTITY,
    IMPLICATION,
    REVERSE_IMPLICATION,
    Mask,
    relationship,
)
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
        # The pair is ordered by claim_id, which says nothing about implication
        # direction, so roughly half of all implications arrive reversed. Store
        # one direction only: swapping the operands says the same thing and
        # keeps (antecedent, consequent) the sole convention downstream.
        antecedent, consequent = left, right
        if kind == REVERSE_IMPLICATION:
            antecedent, consequent = right, left
            kind = IMPLICATION
        out.append(
            ClaimRelation(
                space_shape_id=antecedent.space_shape_id,
                left_claim_id=antecedent.claim_id,
                right_claim_id=consequent.claim_id,
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


def implied_market_relations(
    groups: Iterable[Any],
) -> set[tuple[str, str, str]]:
    """Rebuild market-pair relations from claims alone.

    Two markets in one claim are IDENTITY; two markets in different claims take
    their claims' relation. Only cross-venue pairs are returned, since those are
    the ones the selection scorer reads and the only ones worth asserting on.

    ``groups`` are ``BundleClaims``-shaped: each carries ``claims`` and
    ``relations``. Keys are mask keys, ``venue:venue_market_id#claim_key``.

    This is the check that keeps a recomputed claim honest: if Universe rebuilds
    a bundle slightly differently from the targeter, the relations implied here
    stop matching the ones the report recorded.
    """
    out: set[tuple[str, str, str]] = set()
    for group in groups:
        by_id = {claim.claim_id: claim for claim in group.claims}
        for claim in group.claims:
            for index, left in enumerate(claim.members):
                for right in claim.members[index + 1:]:
                    if left.venue != right.venue:
                        out.add((*sorted((left.market_key, right.market_key)), IDENTITY))
        for relation in group.relations:
            left_claim = by_id[relation.left_claim_id]
            right_claim = by_id[relation.right_claim_id]
            for left in left_claim.members:
                for right in right_claim.members:
                    if left.venue == right.venue:
                        continue
                    ordered = sorted((left.market_key, right.market_key))
                    kind = relation.relation_type
                    # The recorded edge is written from whichever member came
                    # first; flip the direction when sorting reverses the pair.
                    if ordered[0] != left.market_key and kind in {
                        "IMPLICATION", "REVERSE_IMPLICATION"
                    }:
                        kind = (
                            "REVERSE_IMPLICATION" if kind == "IMPLICATION" else "IMPLICATION"
                        )
                    out.add((*ordered, kind))
    return out
