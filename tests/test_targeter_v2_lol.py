from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from targeter.v2.adapters import (
    KalshiSportsAdapter,
    LimitlessSportsAdapter,
    PolymarketSportsAdapter,
)
from targeter.v2.domain import (
    ActivationEvidence,
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    ClassificationEvidence,
)
from targeter.v2.matching import match_events
from targeter.v2.parsing.esports import parse_participants
from targeter.v2.registry import MarketClassRegistry, load_strategy


ROOT = Path(__file__).resolve().parents[1]
STRATEGY_PATH = ROOT / "configs" / "targeter_v2.json"
NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
MATCH_START = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
KALSHI_PRIMARY = datetime(2026, 8, 3, 19, 0, tzinfo=timezone.utc)


class FakeClient:
    def __init__(self, handler):
        self.handler = handler
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.network_requests = 0
        self.cache_hits = 0

    def get_json(self, base_url, path, *, params=None, headers=None):
        self.network_requests += 1
        arguments = dict(params or {})
        self.calls.append((base_url, path, arguments))
        return self.handler(base_url, path, arguments)


def _evidence(
    instant: datetime,
    *,
    field: str,
    primary: bool = True,
    source_kind: str = "structured",
    parser_id: str | None = None,
) -> ActivationEvidence:
    return ActivationEvidence(
        instant=instant,
        source_kind=source_kind,
        source_field=field,
        primary=primary,
        parser_id=parser_id,
    )


def _event(
    venue: str,
    identifier: str,
    *,
    primary: datetime = MATCH_START,
    extra_evidence: tuple[ActivationEvidence, ...] = (),
    market_class: str = "esports.series_moneyline",
    format: str | None = "3",
) -> tuple[CanonicalEvent, CanonicalMarket]:
    event = CanonicalEvent(
        venue=venue,
        venue_event_id=identifier,
        sport="esports",
        league="lcs",
        title="Shifters vs Natus Vincere (BO3)",
        participants=("Shifters", "Natus Vincere"),
        activation_at=primary,
        status="open",
        source_ref=f"/{identifier}",
        format=format,
        fragment_type="group",
        game="league_of_legends",
        topology="best_of_series",
        game_evidence=(
            ClassificationEvidence(
                f"league_of_legends:{venue}:game:test",
                "test",
                "league of legends",
            ),
        ),
        activation_evidence=(
            _evidence(primary, field="primary"),
            *extra_evidence,
        ),
    )
    market_type = {
        "esports.series_moneyline": "series_moneyline",
        "esports.map_winner": "map_winner",
    }[market_class]
    market = CanonicalMarket(
        venue=venue,
        venue_market_id=f"{identifier}-market",
        venue_event_id=identifier,
        canonical_class=market_class,
        market_type=market_type,
        scope="series" if market_type == "series_moneyline" else "map",
        title="Match Winner" if market_type == "series_moneyline" else "Map 1 Winner",
        parameters={"map_index": 1} if market_type == "map_winner" else {"side": "home"},
        subscription_ids=(f"{identifier}-yes",),
        outcome_labels=("Shifters", "Natus Vincere"),
        status="open",
        accepting_orders=True,
        created_at=NOW - timedelta(days=1),
        volume_total_usd=30_000 if venue != "kalshi" else None,
        source_ref=f"/{identifier}-market",
        classification_evidence=ClassificationEvidence(
            f"league_of_legends:{venue}:{market_class}:test",
            "test",
            market_class,
        ),
    )
    return event, market


class LeagueOfLegendsParserAndRegistryTests(unittest.TestCase):
    def test_default_strategy_loads_structured_lol_family(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)
        registry = MarketClassRegistry(strategy)

        family = registry.game_family("league_of_legends")
        self.assertIsNotNone(family)
        self.assertEqual(family.topology, "best_of_series")
        self.assertEqual(
            registry.definition("esports.series_moneyline").market_type,
            "series_moneyline",
        )

    def test_bounded_parser_removes_only_reviewed_lol_metadata(self) -> None:
        # The shared esports grammar is the only participant parser; the game
        # contributes nothing but its configured aliases.
        family = MarketClassRegistry(load_strategy(STRATEGY_PATH)).game_family(
            "league_of_legends"
        )
        aliases = family.venue_game_aliases["polymarket"]
        self.assertEqual(
            parse_participants(
                "LoL: Shifters vs. Natus Vincere (BO3) - LEC Group D", aliases
            ),
            ("Shifters", "Natus Vincere"),
        )
        self.assertEqual(
            parse_participants("Shifters vs Natus Vincere: Map 1", aliases),
            ("Shifters", "Natus Vincere"),
        )
        self.assertEqual(
            parse_participants(
                "LoL: Shifters vs Natus Vincere - More Markets", aliases
            ),
            ("Shifters", "Natus Vincere"),
        )
        self.assertEqual(
            parse_participants("Giants Gaming vs Maple", aliases),
            ("Giants Gaming", "Maple"),
        )


class LeagueOfLegendsAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = MarketClassRegistry(load_strategy(STRATEGY_PATH))

    def test_kalshi_series_ticker_classifies_anchor_and_retains_rule_time(self) -> None:
        def handler(_base, path, _params):
            if path == "/series":
                return {
                    "series": [
                        {
                            "category": "Sports",
                            "ticker": "KXLOLGAME",
                            "title": "League of Legends Game",
                            "tags": ["Esports"],
                        }
                    ]
                }
            self.assertEqual(path, "/events")
            return {
                "events": [
                    {
                        "event_ticker": "KXLOLGAME-26AUG031100SHFTNAVI",
                        "series_ticker": "KXLOLGAME",
                        "title": "Shifters vs Natus Vincere",
                        "strike_date": KALSHI_PRIMARY.isoformat(),
                        "markets": [
                            {
                                "ticker": "KXLOLGAME-SHFT",
                                "title": "Will Shifters win?",
                                "yes_sub_title": "Shifters",
                                "no_sub_title": "Natus Vincere",
                                "status": "active",
                                "rules_primary": (
                                    "If Shifters wins the match originally scheduled for "
                                    "Aug 3, 2026 at 11:00 AM EDT, then this resolves Yes. "
                                    "The market refers to the match originally scheduled "
                                    "for Aug 3, 2026 at 11:00 AM EDT."
                                ),
                                "created_time": (NOW - timedelta(days=1)).isoformat(),
                                "volume_fp": "100000",
                            }
                        ],
                    }
                ],
                "cursor": "",
            }

        snapshot = KalshiSportsAdapter(self.registry).discover(
            FakeClient(handler), now=NOW
        )

        self.assertTrue(snapshot.complete)
        self.assertEqual(len(snapshot.events), 1)
        event = snapshot.events[0]
        self.assertEqual(event.game, "league_of_legends")
        self.assertEqual(event.participants, ("Shifters", "Natus Vincere"))
        self.assertEqual(
            {item.instant for item in event.activation_evidence},
            {MATCH_START, KALSHI_PRIMARY},
        )
        self.assertEqual(snapshot.markets[0].canonical_class, "esports.series_moneyline")
        self.assertEqual(snapshot.markets[0].parameters["side"], "home")
        self.assertIsNotNone(snapshot.markets[0].classification_evidence)

    def test_polymarket_esports_tag_and_title_prefix_classify_anchor(self) -> None:
        def handler(_base, path, params):
            self.assertEqual(path, "/events/keyset")
            if params["tag_slug"] != "esports":
                return {"events": [], "next_cursor": ""}
            return {
                "events": [
                    {
                        "id": "poly-lol",
                        "title": "LoL: Shifters vs Natus Vincere (BO3)",
                        "eventStartTime": MATCH_START.isoformat(),
                        "endDate": (MATCH_START + timedelta(hours=3)).isoformat(),
                        "closed": False,
                        "tags": [{"slug": "esports"}, {"slug": "league-of-legends"}],
                        "markets": [
                            {
                                "id": "poly-match-winner",
                                "groupItemTitle": "Match Winner",
                                "question": "Who will win the match?",
                                "clobTokenIds": '["shifters-token","navi-token"]',
                                "outcomes": '["Shifters","Natus Vincere"]',
                                "active": True,
                                "closed": False,
                                "acceptingOrders": True,
                                "createdAt": (NOW - timedelta(days=1)).isoformat(),
                                "volumeNum": 98_853.25,
                            }
                        ],
                    }
                ],
                "next_cursor": "",
            }

        snapshot = PolymarketSportsAdapter(self.registry).discover(
            FakeClient(handler), now=NOW
        )

        self.assertEqual(len(snapshot.events), 1)
        self.assertEqual(snapshot.events[0].participants, ("Shifters", "Natus Vincere"))
        self.assertEqual(snapshot.events[0].game, "league_of_legends")
        self.assertEqual(snapshot.markets[0].canonical_class, "esports.series_moneyline")
        self.assertEqual(snapshot.markets[0].volume_total_usd, 98_853.25)

    def test_limitless_structured_metadata_classifies_anchor(self) -> None:
        client = FakeClient(
            lambda _base, path, params: {
                "data": [
                    {
                        "id": 100,
                        "title": "Shifters vs Natus Vincere",
                        "slug": "shifters-navi",
                        "automationType": "sports",
                        "expirationTimestamp": int(
                            (MATCH_START + timedelta(hours=3)).timestamp()
                        ),
                        "status": "open",
                        "metadata": {
                            "eventId": "limitless-lol-event",
                            "homeTeam": "Shifters",
                            "awayTeam": "Natus Vincere",
                            "esportTitle": "League of Legends",
                            "marketType": "match_winner",
                            "startMatchTimestampInUTC": MATCH_START.isoformat(),
                            "numberOfGames": 3,
                        },
                        "markets": [
                            {
                                "id": 101,
                                "slug": "shifters-navi-winner",
                                "title": "Match Winner",
                                "tradeType": "clob",
                                "status": "open",
                                "createdAt": (NOW - timedelta(days=1)).isoformat(),
                                "volumeFormatted": "35000.50",
                                "outcomeTokens": ["Shifters", "Natus Vincere"],
                            }
                        ],
                    }
                ],
                "totalMarketsCount": 1,
            }
        )

        snapshot = LimitlessSportsAdapter(self.registry).discover(client, now=NOW)

        self.assertEqual(len(snapshot.events), 1)
        self.assertEqual(snapshot.events[0].venue_event_id, "limitless-lol-event")
        self.assertEqual(snapshot.events[0].game, "league_of_legends")
        self.assertEqual(snapshot.markets[0].canonical_class, "esports.series_moneyline")
        self.assertEqual(snapshot.markets[0].volume_total_usd, 35_000.5)
        category_paths = {
            f"/markets/active/{identifier}"
            for identifier in self.registry.strategy.limitless_category_ids
        }
        self.assertTrue(
            all(
                call[2].get("automationType") == "sports"
                if call[1] == "/markets/active"
                else call[1] in category_paths
                for call in client.calls
            )
        )


class LeagueOfLegendsMatchingTests(unittest.TestCase):
    def test_reviewed_secondary_time_attaches_conflicting_kalshi_anchor(self) -> None:
        poly_event, poly_market = _event("polymarket", "poly")
        limitless_event, limitless_market = _event("limitless", "limitless")
        kalshi_event, kalshi_market = _event(
            "kalshi",
            "kalshi",
            primary=KALSHI_PRIMARY,
            extra_evidence=(
                _evidence(
                    MATCH_START,
                    field="rules_primary",
                    primary=False,
                    source_kind="rule_template",
                    parser_id="kalshi_lol_originally_scheduled_v1",
                ),
            ),
        )
        catalogs = (
            CatalogSnapshot("polymarket", (poly_event,), (poly_market,)),
            CatalogSnapshot("limitless", (limitless_event,), (limitless_market,)),
            CatalogSnapshot("kalshi", (kalshi_event,), (kalshi_market,)),
        )

        bundles, rejected = match_events(
            catalogs, tolerance_seconds=900, minimum_venues=2
        )

        self.assertEqual(len(bundles), 1)
        self.assertEqual(bundles[0].activation_at, MATCH_START)
        self.assertEqual(set(bundles[0].venues), {"kalshi", "polymarket", "limitless"})
        self.assertTrue(
            any(item["code"] == "activation_primary_conflict" for item in bundles[0].activation_conflicts)
        )
        self.assertFalse(any(item.reason == "fewer_than_minimum_venues" for item in rejected))

    def test_two_agreeing_venues_survive_a_third_activation_conflict(self) -> None:
        poly_event, poly_market = _event("polymarket", "poly")
        limitless_event, limitless_market = _event("limitless", "limitless")
        kalshi_event, kalshi_market = _event(
            "kalshi", "kalshi", primary=KALSHI_PRIMARY
        )

        bundles, rejected = match_events(
            (
                CatalogSnapshot("polymarket", (poly_event,), (poly_market,)),
                CatalogSnapshot("limitless", (limitless_event,), (limitless_market,)),
                CatalogSnapshot("kalshi", (kalshi_event,), (kalshi_market,)),
            ),
            tolerance_seconds=900,
            minimum_venues=2,
        )

        self.assertEqual(len(bundles), 1)
        self.assertEqual(set(bundles[0].venues), {"polymarket", "limitless"})
        self.assertIn("activation_time_conflict", {item.reason for item in rejected})

    def test_far_same_day_sibling_does_not_attach_to_anchor_bundle(self) -> None:
        poly_event, poly_market = _event("polymarket", "poly")
        limitless_event, limitless_market = _event("limitless", "limitless")
        sibling_event, sibling_market = _event(
            "kalshi",
            "map-fragment",
            primary=MATCH_START + timedelta(hours=8),
            market_class="esports.map_winner",
        )

        bundles, rejected = match_events(
            (
                CatalogSnapshot("polymarket", (poly_event,), (poly_market,)),
                CatalogSnapshot("limitless", (limitless_event,), (limitless_market,)),
                CatalogSnapshot("kalshi", (sibling_event,), (sibling_market,)),
            ),
            tolerance_seconds=900,
            minimum_venues=2,
        )

        self.assertEqual(len(bundles), 1)
        self.assertNotIn("kalshi", bundles[0].venues)
        self.assertIn("sibling_no_anchor", {item.reason for item in rejected})


if __name__ == "__main__":
    unittest.main()
