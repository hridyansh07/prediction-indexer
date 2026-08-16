from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from analysis.durable_http import RetryingJsonClient
from targeter.v2.domain import ActivationEvidence, CanonicalEvent, CanonicalMarket, CatalogSnapshot, ClassificationEvidence
from targeter.v2.adapters import PolymarketSportsAdapter, durable_client
from targeter.v2.parsing.esports import parse_best_of_values, parse_participants
from targeter.v2.matching import MatchRejection, match_events
from targeter.v2.registry import MarketClassRegistry, StrategyError, load_strategy
from targeter.v2.relationships import derive_bundle_relationships

ROOT = Path(__file__).resolve().parents[1]
STRATEGY = ROOT / "configs" / "targeter_v2.json"
AT = datetime(2026, 8, 3, 15, tzinfo=timezone.utc)


class EsportsRegistryAndParserTests(unittest.TestCase):
    def test_v4_ships_only_families_with_two_reviewed_venues(self):
        strategy = load_strategy(STRATEGY)
        self.assertEqual(strategy.version, 4)
        families = {family.id: family for family in strategy.game_families}
        self.assertEqual(set(families), {"league_of_legends", "counter_strike_2", "dota_2", "valorant"})
        # Selection needs two venues, so a family with no reviewed products on a
        # venue can never reach a bundle; none may ship in that state.
        for identifier, family in families.items():
            venues = [venue for venue, products in family.venue_products.items() if products]
            self.assertGreaterEqual(len(venues), 2, identifier)
        self.assertEqual(len({tag for family in families.values() for tag in family.polymarket_game_tags}), 4)

    def test_game_tags_are_globally_unique(self):
        document = json.loads(STRATEGY.read_text())
        document["game_families"][1]["polymarket_game_tags"] = document["game_families"][0]["polymarket_game_tags"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "strategy.json"
            path.write_text(json.dumps(document))
            with self.assertRaises(StrategyError):
                load_strategy(path)

    def test_parser_matrix_preserves_even_format_and_only_reviewed_suffixes(self):
        cases = {
            "Counter-Strike: Alpha vs Beta (BO2) - Cup": (("Alpha", "Beta"), (2,)),
            "Dota 2: Alpha vs. Beta (Best of 5)": (("Alpha", "Beta"), (5,)),
            "League of Legends: Alpha @ Beta: Map 10": (("Alpha", "Beta"), ()),
            "Valorant: Alpha versus Beta-Team": (("Alpha", "Beta-Team"), ()),
        }
        for title, expected in cases.items():
            aliases = (title.split(":", 1)[0],)
            self.assertEqual(parse_participants(title, aliases), expected[0])
            self.assertEqual(parse_best_of_values(title), expected[1])
        self.assertEqual(parse_participants("Dota 2: Gaming Map vs Game Kings", ("dota 2",)), ("Gaming Map", "Game Kings"))

    def test_parser_rejects_bare_v_and_strips_combined_suffixes(self):
        self.assertIsNone(parse_participants("Dota 2: Alpha v Beta", ("dota 2",)))
        self.assertIsNone(parse_participants("Unreviewed: Alpha vs Beta", ("dota 2",)))
        self.assertEqual(
            parse_participants(
                "Dota 2: Alpha-Team vs Beta-Team (BO3) - Cup: Map 1",
                ("dota 2",),
            ),
            ("Alpha-Team", "Beta-Team"),
        )
        self.assertEqual(
            parse_participants(
                "LoL: Alpha-Team vs Beta-Team - More Markets",
                ("lol", "league of legends"),
            ),
            ("Alpha-Team", "Beta-Team"),
        )

    def test_live_client_retries_partial_catalogue_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            client = durable_client(Path(directory))
            self.assertIsInstance(client, RetryingJsonClient)
            self.assertTrue(client.compress_responses)


class _Client:
    network_requests = 0
    cache_hits = 0

    def __init__(self, events):
        self.events = events

    def get_json(self, _base, _path, *, params=None, headers=None):
        self.network_requests += 1
        return {
            "events": self.events if params["tag_slug"] == "esports" else [],
            "next_cursor": "",
        }


class EsportsAdapterContractTests(unittest.TestCase):
    def setUp(self):
        self.registry = MarketClassRegistry(load_strategy(STRATEGY))

    @staticmethod
    def _event(identifier, prefix, game_tag, description=""):
        return {
            "id": identifier,
            "title": f"{prefix}: Alpha vs Beta (BO3)",
            "description": description,
            "eventStartTime": AT.isoformat(),
            "closed": False,
            "tags": [{"slug": "esports"}, {"slug": game_tag}],
            "markets": [{
                "id": identifier + "-winner", "groupItemTitle": "Match Winner",
                "clobTokenIds": '["a","b"]', "outcomes": '["Alpha","Beta"]',
                "active": True, "acceptingOrders": True, "volumeNum": 20000,
            }],
        }

    def test_all_new_games_normalize_as_game_specific_anchors(self):
        cases = (
            ("counter_strike_2", "Counter-Strike", "counter-strike-2"),
            ("dota_2", "Dota 2", "dota-2"),
            ("valorant", "Valorant", "valorant"),
        )
        for game, prefix, tag in cases:
            snapshot = PolymarketSportsAdapter(self.registry).discover(
                _Client([self._event(game, prefix, tag)]), now=AT
            )
            self.assertEqual((len(snapshot.events), len(snapshot.markets)), (1, 1), game)
            self.assertEqual(snapshot.events[0].game, game)
            self.assertEqual(snapshot.markets[0].canonical_class, "esports.series_moneyline")
            other = _pair(game)[1]
            bundles, rejected = match_events((snapshot, other), tolerance_seconds=900, minimum_venues=2)
            self.assertEqual((len(bundles), rejected), (1, ()), game)
            self.assertTrue(derive_bundle_relationships(bundles[0]).relationships, game)

    def test_polymarket_tag_prefix_disagreement_is_complete_conflict_matrix(self):
        cases = (
            self._event("wrong-tag", "Dota 2", "valorant"),
            self._event("missing-game-tag", "Dota 2", "esports"),
            self._event("missing-prefix", "Unknown Game", "dota-2"),
        )
        for event in cases:
            snapshot = PolymarketSportsAdapter(self.registry).discover(_Client([event]), now=AT)
            self.assertFalse(snapshot.complete)
            self.assertFalse(snapshot.events)
            self.assertEqual(snapshot.classification_diagnostics[0].code, "game_classification_conflict")

    def test_conflicting_format_excludes_event_with_exact_diagnostic(self):
        event = self._event("format-conflict", "Dota 2", "dota-2", "Best of 5")
        snapshot = PolymarketSportsAdapter(self.registry).discover(_Client([event]), now=AT)
        self.assertFalse(snapshot.events)
        diagnostic = snapshot.classification_diagnostics[0]
        self.assertEqual(diagnostic.code, "intra_event_format_conflict")
        self.assertEqual(diagnostic.details, {"format_observed": [3, 5]})

def _pair(game: str, best_of: str = "3"):
    evidence = lambda venue: ClassificationEvidence(f"{game}:{venue}:game:test", "test", game)
    events, markets = [], []
    for venue in ("polymarket", "limitless"):
        identifier = f"{game}-{venue}"
        events.append(CanonicalEvent(venue, identifier, "esports", None, "Alpha vs Beta", ("Alpha", "Beta"), AT, "open", f"/{identifier}", best_of, "group", game, "best_of_series", (evidence(venue),), (ActivationEvidence(AT, "structured", "start", True),)))
        markets.append(CanonicalMarket(venue, identifier + "-m", identifier, "esports.series_moneyline", "series_moneyline", "series", "Match Winner", {"side": "home"}, (identifier + "-token",), ("Alpha", "Beta"), "open", True, volume_total_usd=20_000, source_ref=f"/{identifier}-m", classification_evidence=ClassificationEvidence(f"{game}:{venue}:esports.series_moneyline:test", "test", "match_winner")))
    return tuple(CatalogSnapshot(v, tuple(e for e in events if e.venue == v), tuple(m for m in markets if m.venue == v)) for v in ("polymarket", "limitless"))


class EsportsMatchingAndFormatTests(unittest.TestCase):
    def test_game_rejection_serializes_format_at_top_level(self):
        rejection = MatchRejection(
            "esports", ("alpha", "beta"), ("kalshi:a", "polymarket:b"),
            "competition_format_mismatch", game="dota_2", topology="best_of_series",
            details={"format_observed": [5, 3]},
        )
        record = rejection.as_record()
        self.assertEqual(record["format_observed"], [3, 5])
        self.assertIsNone(record["best_of"])
        self.assertEqual(record["format_status"], "conflicting")
        self.assertEqual(record["outcome_space_status"], "not_built_format_conflict")

    def test_partial_series_outcomes_compile_no_claims(self):
        snapshots = list(_pair("dota_2"))
        bad = replace(
            snapshots[0].markets[0],
            subscription_ids=("only-one",),
            outcome_labels=("Alpha", "Beta"),
        )
        snapshots[0] = replace(snapshots[0], markets=(bad,))
        bundles, _ = match_events(snapshots, tolerance_seconds=900, minimum_venues=2)
        analysis = derive_bundle_relationships(bundles[0])
        self.assertFalse(any(mask.market_key.startswith(bad.target_id) for mask in analysis.masks))

    def test_kalshi_single_ticker_affirmative_claim_matches_polymarket_condition(self):
        polymarket, limitless = _pair("dota_2")
        polymarket = replace(
            polymarket,
            markets=(
                replace(
                    polymarket.markets[0],
                    subscription_ids=("alpha-token", "beta-token"),
                ),
            ),
        )
        kalshi_event = replace(
            limitless.events[0],
            venue="kalshi",
            venue_event_id="dota_2-kalshi",
        )
        kalshi_market = replace(
            limitless.markets[0],
            venue="kalshi",
            venue_event_id=kalshi_event.venue_event_id,
            venue_market_id="dota_2-kalshi-alpha",
            subscription_ids=("KXDOTA2GAME-ALPHA",),
            # Kalshi publishes one binary YES ticker per participant and may
            # repeat the affirmative subtitle in both normalized label slots.
            outcome_labels=("Alpha", "Alpha"),
            parameters={"side": "home"},
        )
        kalshi = CatalogSnapshot("kalshi", (kalshi_event,), (kalshi_market,))

        bundles, rejected = match_events(
            (polymarket, kalshi), tolerance_seconds=900, minimum_venues=2
        )
        self.assertEqual((len(bundles), rejected), (1, ()))
        analysis = derive_bundle_relationships(bundles[0])

        self.assertTrue(
            any(mask.market_key.startswith(kalshi_market.target_id) for mask in analysis.masks)
        )
        self.assertTrue(
            any(
                relationship.cross_venue and relationship.relationship == "IDENTITY"
                for relationship in analysis.relationships
            )
        )

    def test_all_new_games_match_independently(self):
        for game in ("counter_strike_2", "dota_2", "valorant"):
            bundles, rejected = match_events(_pair(game), tolerance_seconds=900, minimum_venues=2)
            self.assertEqual((len(bundles), bundles[0].game), (1, game))
            self.assertFalse(rejected)

    def test_cross_game_partitioning(self):
        catalogs = _pair("dota_2")[:1] + _pair("counter_strike_2")[1:]
        bundles, rejected = match_events(catalogs, tolerance_seconds=900, minimum_venues=2)
        self.assertFalse(bundles)
        self.assertEqual({item.reason for item in rejected}, {"fewer_than_minimum_venues"})

    def test_bo2_bundle_is_safe_and_reportable(self):
        bundles, _ = match_events(_pair("dota_2", "2"), tolerance_seconds=900, minimum_venues=2)
        analysis = derive_bundle_relationships(bundles[0])
        self.assertFalse(analysis.spaces)
        self.assertIn("unsupported_series_format", analysis.diagnostics)


if __name__ == "__main__":
    unittest.main()
