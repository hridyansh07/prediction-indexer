from __future__ import annotations

import unittest

from analysis.claims import (
    CLAIM_ALGEBRA_VERSION,
    CLAIM_IDENTITY_VERSION,
    claim_id,
    derive_claim_algebra,
    derive_claims,
    space_shape_id,
    usable_masks,
)
from analysis.masks import (
    IDENTITY,
    IMPLICATION,
    MUTUAL_EXCLUSION,
    Mask,
    compile_mask,
    relationship,
)
from analysis.outcome_space import build_series_space


def _space(best_of: int = 5, home: str = "H", away: str = "A"):
    return build_series_space("evt", best_of=best_of, home=home, away=away)


def _mask(space, **market) -> Mask:
    return compile_mask({"venue": "kalshi", "target_id": "t", **market}, space)


def _named(space, key: str, venue: str, **market) -> Mask:
    compiled = _mask(space, **market)
    return Mask(
        market_key=key,
        venue=venue,
        market_type=compiled.market_type,
        scope=compiled.scope,
        status=compiled.status,
        outcome_keys=compiled.outcome_keys,
        resolver=compiled.resolver,
    )


class ClaimIdentity(unittest.TestCase):
    def test_identity_is_the_outcome_subset(self) -> None:
        self.assertEqual(claim_id(["b", "a"]), claim_id(["a", "b"]))
        self.assertNotEqual(claim_id(["a"]), claim_id(["a", "b"]))

    def test_a_claim_must_name_an_outcome(self) -> None:
        with self.assertRaises(ValueError):
            claim_id([])

    def test_claims_are_participant_independent(self) -> None:
        """The property the whole model rests on: a claim is global.

        Outcome keys are ``seq:{sequence}``, so a best-of-5 space carries the
        same keys whoever is playing. The same claim in two different events of
        the same shape must therefore land on one id.
        """
        one, two = _space(home="PARI", away="TYL"), _space(home="Liquid", away="G2")
        self.assertEqual(space_shape_id(one), space_shape_id(two))
        left = _mask(one, market_type="series_moneyline", outcome_label="PARI")
        right = _mask(two, market_type="series_moneyline", outcome_label="Liquid")
        self.assertEqual(claim_id(left.outcome_keys), claim_id(right.outcome_keys))

    def test_shapes_differ_between_series_formats(self) -> None:
        self.assertNotEqual(space_shape_id(_space(3)), space_shape_id(_space(5)))

    def test_resolver_is_a_label_not_an_identity(self) -> None:
        """``maps_over`` at two lines shares a resolver but not a claim."""
        space = _space()
        over_25 = _mask(space, market_type="total_maps",
                        yes_label="Over 2.5", group_item_title="Over 2.5")
        over_35 = _mask(space, market_type="total_maps",
                        yes_label="Over 3.5", group_item_title="Over 3.5")
        self.assertEqual(over_25.resolver, over_35.resolver)
        self.assertNotEqual(claim_id(over_25.outcome_keys), claim_id(over_35.outcome_keys))
        self.assertEqual(relationship(over_35, over_25), IMPLICATION)


class ClaimDerivation(unittest.TestCase):
    def test_venues_naming_one_claim_collapse_to_one_class(self) -> None:
        space = _space()
        masks = [
            _named(space, "kalshi:k1#claim=0", "kalshi",
                   market_type="series_moneyline", outcome_label="H"),
            _named(space, "polymarket:p1#claim=0", "polymarket",
                   market_type="series_moneyline", group_item_title="H to win"),
            _named(space, "limitless:l1#claim=0", "limitless",
                   market_type="series_moneyline", outcome_label="H"),
        ]
        claims = derive_claims(masks, space)
        self.assertEqual(len(claims), 1)
        self.assertTrue(claims[0].cross_venue)
        self.assertEqual(len(claims[0].members), 3)

    def test_tautologies_and_empty_masks_are_excluded(self) -> None:
        space = _space()
        everything = Mask(market_key="m", venue="v", market_type="t", scope=space.scope,
                          status="DERIVABLE", outcome_keys=space.keys, resolver="all")
        nothing = Mask(market_key="n", venue="v", market_type="t", scope=space.scope,
                       status="DERIVABLE", outcome_keys=frozenset(), resolver="none")
        self.assertEqual(usable_masks([everything, nothing], space), ())

    def test_derivation_is_deterministic(self) -> None:
        space = _space()
        masks = [
            _named(space, "kalshi:k1#claim=0", "kalshi",
                   market_type="series_moneyline", outcome_label="H"),
            _named(space, "kalshi:k2#claim=0", "kalshi",
                   market_type="map_winner", outcome_label="H", map_index=1),
        ]
        self.assertEqual(
            [claim.claim_id for claim in derive_claims(masks, space)],
            [claim.claim_id for claim in derive_claims(list(reversed(masks)), space)],
        )


class ClaimAlgebra(unittest.TestCase):
    @staticmethod
    def _build(space, best_of: int, prefix: str, venue: str):
        """Compile the same claim vocabulary against whatever space is given.

        Sides are named with the space's own participants, so the vocabulary is
        identical in meaning across events even though the labels differ.
        """
        masks = []
        for side in (space.metadata["home"], space.metadata["away"]):
            masks.append(_named(space, f"{prefix}:ml-{side}#claim=0", venue,
                                market_type="series_moneyline", outcome_label=side))
            for index in range(1, best_of + 1):
                masks.append(_named(space, f"{prefix}:map{index}-{side}#claim=0", venue,
                                    market_type="map_winner",
                                    outcome_label=side, map_index=index))
        return derive_claims(masks, space)

    def _claims(self, best_of: int = 5):
        space = _space(best_of)
        return space, self._build(space, best_of, "v", "kalshi")

    def test_algebra_is_quadratic_in_claims_not_markets(self) -> None:
        space, claims = self._claims()
        relations = derive_claim_algebra(claims, informative_only=False)
        self.assertEqual(len(relations), len(claims) * (len(claims) - 1) // 2)

    def test_informative_filter_drops_overlap(self) -> None:
        _space_, claims = self._claims()
        every = derive_claim_algebra(claims, informative_only=False)
        signal = derive_claim_algebra(claims, informative_only=True)
        self.assertLess(len(signal), len(every))
        self.assertNotIn("OVERLAP", {item.relation_type for item in signal})
        self.assertTrue(all(item.informative for item in signal))

    def test_algebra_is_reusable_across_events_of_one_shape(self) -> None:
        """The saving that makes the table global rather than per event.

        Two unrelated fixtures, different teams and different venues, yield the
        same claim ids and therefore the same algebra.
        """
        _one, claims_one = self._claims()
        space_two = _space(home="Liquid", away="G2")
        claims_two = self._build(space_two, 5, "w", "polymarket")
        self.assertEqual(
            {claim.claim_id for claim in claims_one},
            {claim.claim_id for claim in claims_two},
        )
        self.assertEqual(
            {(r.left_claim_id, r.right_claim_id, r.relation_type)
             for r in derive_claim_algebra(claims_one)},
            {(r.left_claim_id, r.right_claim_id, r.relation_type)
             for r in derive_claim_algebra(claims_two)},
        )

    def test_opposing_sides_are_mutually_exclusive(self) -> None:
        space = _space()
        home = _named(space, "v:h#claim=0", "kalshi",
                      market_type="series_moneyline", outcome_label="H")
        away = _named(space, "v:a#claim=0", "kalshi",
                      market_type="series_moneyline", outcome_label="A")
        relations = derive_claim_algebra(derive_claims([home, away], space))
        self.assertEqual([item.relation_type for item in relations], [MUTUAL_EXCLUSION])

    def test_mixed_shapes_are_rejected(self) -> None:
        _a, claims_three = self._claims(3)
        _b, claims_five = self._claims(5)
        with self.assertRaises(ValueError):
            derive_claim_algebra([*claims_three, *claims_five])


class Versioning(unittest.TestCase):
    def test_versions_are_recorded(self) -> None:
        self.assertIsInstance(CLAIM_IDENTITY_VERSION, int)
        self.assertIsInstance(CLAIM_ALGEBRA_VERSION, int)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Falsification: the claim algebra must reproduce the pairwise model
# ---------------------------------------------------------------------------

from dataclasses import replace  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from pathlib import Path  # noqa: E402

from analysis.claims import UNINFORMATIVE_RELATIONS  # noqa: E402
from targeter.v2.domain import (  # noqa: E402
    ActivationEvidence,
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    ClassificationEvidence,
)
from targeter.v2.matching import match_events  # noqa: E402
from analysis.claims import implied_market_relations  # noqa: E402
from targeter.v2.relationships import (  # noqa: E402
    derive_bundle_claims,
    derive_bundle_relationships,
)

_AT = datetime(2026, 8, 3, 15, tzinfo=timezone.utc)


def _snapshots(game: str = "dota_2", best_of: str = "3"):
    """A two-venue esports bundle, shaped like the adapter contract fixtures."""
    def evidence(venue):
        return ClassificationEvidence(f"{game}:{venue}:game:test", "test", game)

    events, markets = [], []
    for venue in ("polymarket", "limitless"):
        identifier = f"{game}-{venue}"
        events.append(CanonicalEvent(
            venue, identifier, "esports", None, "Alpha vs Beta", ("Alpha", "Beta"),
            _AT, "open", f"/{identifier}", best_of, "group", game, "best_of_series",
            (evidence(venue),), (ActivationEvidence(_AT, "structured", "start", True),),
        ))
        # Venues publish two legitimate token shapes, and a mask is only
        # compiled from a whole one: Polymarket lists a token per outcome,
        # Limitless lists a single affirmative token naming its side out of
        # band. A single token carrying two distinct labels is a partial
        # condition and is deliberately refused.
        if venue == "polymarket":
            tokens, labels = (identifier + "-a", identifier + "-b"), ("Alpha", "Beta")
        else:
            tokens, labels = (identifier + "-token",), ("Yes", "No")
        markets.append(CanonicalMarket(
            venue, identifier + "-m", identifier, "esports.series_moneyline",
            "series_moneyline", "series", "Match Winner", {"side": "home"},
            tokens, labels, "open", True,
            volume_total_usd=20_000, source_ref=f"/{identifier}-m",
            classification_evidence=ClassificationEvidence(
                f"{game}:{venue}:esports.series_moneyline:test", "test", "match_winner"),
        ))
    return tuple(
        CatalogSnapshot(
            venue,
            tuple(event for event in events if event.venue == venue),
            tuple(market for market in markets if market.venue == venue),
        )
        for venue in ("polymarket", "limitless")
    )


def _with_map_markets(snapshots, game: str = "dota_2"):
    """Add per-map winner markets, so the bundle carries a real claim lattice."""
    out = []
    for snapshot in snapshots:
        venue = snapshot.venue
        identifier = f"{game}-{venue}"
        if venue == "polymarket":
            def tokens(index):
                return (f"{identifier}-map{index}-a", f"{identifier}-map{index}-b")
            labels = ("Alpha", "Beta")
        else:
            def tokens(index):
                return (f"{identifier}-map{index}-token",)
            labels = ("Yes", "No")
        extra = tuple(
            replace(
                snapshot.markets[0],
                venue_market_id=f"{identifier}-map{index}",
                canonical_class="esports.map_winner",
                market_type="map_winner",
                scope="map",
                title=f"Map {index} Winner",
                parameters={"side": "home", "map_index": index},
                subscription_ids=tokens(index),
                outcome_labels=labels,
                source_ref=f"/{identifier}-map{index}",
            )
            for index in (1, 2, 3)
        )
        out.append(replace(snapshot, markets=snapshot.markets + extra))
    return tuple(out)


def _bundle(snapshots):
    bundles, _rejected = match_events(snapshots, tolerance_seconds=900, minimum_venues=2)
    assert bundles, "fixture produced no bundle"
    return bundles[0]


def _pairwise_signal(analysis) -> set[tuple[str, str, str]]:
    """Cross-venue, non-OVERLAP relations — what the selection scorer reads."""
    return {
        (*sorted((item.left, item.right)), item.relationship)
        for item in analysis.relationships
        if item.cross_venue and item.relationship not in UNINFORMATIVE_RELATIONS
    }


def _reconstructed_signal(spaces) -> set[tuple[str, str, str]]:
    """The production reconstruction, exercised against real bundles."""
    return implied_market_relations(spaces)


class ClaimAlgebraReproducesPairwiseModel(unittest.TestCase):
    """Phase 1 gate: the model must lose no relation the scorer acts on."""

    def _check(self, snapshots) -> int:
        bundle = _bundle(snapshots)
        recorded = _pairwise_signal(derive_bundle_relationships(bundle))
        rebuilt = _reconstructed_signal(derive_bundle_claims(bundle))
        self.assertEqual(
            recorded - rebuilt, set(), "claims lost a relation the pairwise model found"
        )
        self.assertEqual(
            rebuilt - recorded, set(), "claims invented a relation the pairwise model lacks"
        )
        return len(recorded)

    def test_series_moneyline_bundle(self) -> None:
        self.assertGreater(self._check(_snapshots()), 0)

    def test_bundle_with_map_markets(self) -> None:
        self.assertGreater(self._check(_with_map_markets(_snapshots())), 0)

    def test_best_of_five_bundle(self) -> None:
        self.assertGreater(
            self._check(_with_map_markets(_snapshots(best_of="5"))), 0
        )

    def test_claims_collapse_cross_venue_duplication(self) -> None:
        """The compression: markets fall to fewer claims than pairwise edges."""
        bundle = _bundle(_with_map_markets(_snapshots()))
        spaces = derive_bundle_claims(bundle)
        claims = sum(len(group.claims) for group in spaces)
        pairwise = len(derive_bundle_relationships(bundle).relationships)
        self.assertGreater(pairwise, claims)
        self.assertTrue(any(claim.cross_venue for group in spaces for claim in group.claims))

    def test_algebra_carries_no_uninformative_relations(self) -> None:
        bundle = _bundle(_with_map_markets(_snapshots()))
        kinds = {
            relation.relation_type
            for group in derive_bundle_claims(bundle)
            for relation in group.relations
        }
        self.assertFalse(kinds & UNINFORMATIVE_RELATIONS)
