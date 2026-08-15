from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

from targeter.targets import TargetsError
from targeter.v2.adapters import (
    KalshiSportsAdapter,
    LimitlessSportsAdapter,
    PolymarketSportsAdapter,
)
from targeter.v2.domain import (
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    SUPPORTED_VENUES,
    canonical_participant,
    isoformat,
)
from targeter.v2.continuity import (
    ContinuityBundle,
    ContinuityError,
    ContinuityTarget,
    TerminalProbe,
)
from targeter.v2.matching import match_events
from targeter.v2.registry import MarketClassRegistry, StrategyError, load_strategy
from targeter.v2.rules import assess_rules, normal_path_contradictions, template_for
from targeter.v2.run import (
    ShadowRun,
    _continuity_for_run,
    main as shadow_main,
    parse_args,
    run_shadow,
)
from targeter.v2.run_archive import read_run_report
from targeter.v2.selection import select_targets


ROOT = Path(__file__).resolve().parents[1]
STRATEGY_PATH = ROOT / "configs" / "targeter_v2.json"
NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
START = NOW + timedelta(minutes=50)


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


def event(
    venue: str,
    identifier: str,
    *,
    participants=("Arsenal", "Chelsea"),
    activation=START,
    format=None,
) -> CanonicalEvent:
    return CanonicalEvent(
        venue=venue,
        venue_event_id=identifier,
        sport="soccer" if format is None else "esports",
        league="premier league" if format is None else "dota 2",
        title=f"{participants[0]} vs {participants[1]}",
        participants=participants,
        activation_at=activation,
        status="open",
        source_ref=f"/{identifier}",
        format=format,
    )


def market(
    venue: str,
    identifier: str,
    event_id: str,
    *,
    side="home",
    created_at=NOW - timedelta(days=1),
    rules="Resolves Yes if Arsenal wins in regulation time only.",
    volume_total_usd=20_000,
    raw=None,
) -> CanonicalMarket:
    # Shaped like a real venue record: the fields `replay.catalog._instrument`
    # reads, plus the volume fields that move on every trade and must stay out
    # of the projection.
    if raw is None:
        raw = {
            "id": identifier,
            "clobTokenIds": f'["{identifier}-subscription", "{identifier}-no"]',
            "outcomes": '["Yes", "No"]',
            "orderMinSize": "5",
            "endDate": "2026-08-04T00:00:00Z",
            "description": rules,
            "feesEnabled": False,
            "createdAt": isoformat(created_at),
            "volume24hr": 100,
            "liquidity": 200,
        }
    return CanonicalMarket(
        raw=raw,
        venue=venue,
        venue_market_id=identifier,
        venue_event_id=event_id,
        canonical_class="soccer.moneyline_3way",
        market_type="moneyline_3way",
        scope="regulation_fulltime",
        title="Arsenal to win",
        parameters={"side": side},
        subscription_ids=(f"{identifier}-subscription",),
        outcome_labels=("Yes", "No"),
        status="open",
        accepting_orders=True,
        rules_text=rules,
        created_at=created_at,
        volume_24h=100,
        volume_total=500,
        volume_total_usd=volume_total_usd,
        liquidity=200,
        source_ref=f"/{identifier}",
    )


def snapshot(venue: str, event_id: str, market_id: str, **kwargs) -> CatalogSnapshot:
    item = event(venue, event_id, participants=kwargs.pop("participants", ("Arsenal", "Chelsea")))
    return CatalogSnapshot(venue, (item,), (market(venue, market_id, event_id, **kwargs),))


class StrategyAndMatchingTests(unittest.TestCase):
    def test_plain_football_is_not_assumed_to_mean_soccer(self) -> None:
        from targeter.v2.parsing.text import sport_from_labels

        self.assertIsNone(sport_from_labels(["Football"], "NFL Game Winner"))
        self.assertEqual(sport_from_labels(["Football Matches"]), "soccer")

    def test_traditional_parser_drops_tournament_metadata_but_not_game_prefixes(self) -> None:
        from targeter.v2.parsing.traditional import parse_participants

        self.assertEqual(
            parse_participants("UEL, Benfica vs Hearts - Stake Pulse Beat I Group D"),
            ("Benfica", "Hearts"),
        )
        # Esports titles are the game-family path's business. This parser must
        # not recognize a game prefix, because doing so would let an event the
        # registry refused to classify re-enter through title grammar.
        self.assertEqual(
            parse_participants(
                "Counter-Strike: fnatic vs Lilmix (BO3) - Stake Pulse Beat I Group D"
            ),
            ("Counter-Strike: fnatic", "Lilmix (BO3)"),
        )

    def test_registry_is_config_driven_and_recognises_current_shapes(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)
        registry = MarketClassRegistry(strategy)
        self.assertEqual(
            registry.classify(
                "polymarket", "soccer", {"event_title": "Arsenal vs. Chelsea"}
            ).id,
            "soccer.moneyline_3way",
        )
        self.assertIsNone(
            registry.classify(
                "kalshi", "esports", {"series_title": "Dota 2 Map Winner"}
            ),
            "configured esports games must bypass the old generic title regex path",
        )

    def test_strategy_rejects_unknown_top_level_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            document = json.loads(STRATEGY_PATH.read_text(encoding="utf-8"))
            document["typo"] = True
            path = Path(directory) / "strategy.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaises(StrategyError):
                load_strategy(path)

    def test_strategy_rejects_an_alias_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            document = json.loads(STRATEGY_PATH.read_text(encoding="utf-8"))
            document["participant_aliases"] = {
                "Heart of Midlothian": ["Hearts"],
                "Queen of the South": ["Hearts"],
            }
            path = Path(directory) / "strategy.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(StrategyError, "more than one"):
                load_strategy(path)

    def test_strategy_rejects_a_non_positive_moneyline_volume_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            document = json.loads(STRATEGY_PATH.read_text(encoding="utf-8"))
            document["selection"]["minimum_combined_moneyline_volume_usd"] = 0
            path = Path(directory) / "strategy.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(StrategyError, "positive finite number"):
                load_strategy(path)

    def test_domain_rejects_cross_venue_catalog_records(self) -> None:
        with self.assertRaisesRegex(ValueError, "catalog venue"):
            CatalogSnapshot(
                "kalshi",
                (event("polymarket", "p"),),
                (),
            )

    def test_reviewed_participant_alias_applies_to_matching_and_side_resolution(self) -> None:
        strategy = replace(
            load_strategy(STRATEGY_PATH),
            participant_aliases={
                "hearts": "heart of midlothian",
                "heart of midlothian": "heart of midlothian",
            },
        )
        kalshi = snapshot(
            "kalshi", "k-hearts", "km-hearts",
            participants=("Benfica", "Hearts"), side="away",
        )
        poly = snapshot(
            "polymarket", "p-hearts", "pm-hearts",
            participants=("Benfica", "Heart of Midlothian"), side="away",
        )
        result = select_targets((kalshi, poly), strategy=strategy, now=NOW)
        self.assertEqual(len(result.selected), 1)
        self.assertEqual(
            result.selected[0].bundle.participant_key_map,
            {"hearts": "heart of midlothian"},
        )

    def test_alias_that_collapses_an_event_is_rejected(self) -> None:
        bundles, rejected = match_events(
            (snapshot("kalshi", "k", "km"),),
            tolerance_seconds=900,
            minimum_venues=2,
            participant_aliases={"arsenal": "chelsea", "chelsea": "chelsea"},
        )
        self.assertEqual(bundles, ())
        self.assertEqual(rejected[0].reason, "participant_alias_collision")

    def test_matches_reversed_participants_and_all_fragments(self) -> None:
        kalshi_event = event("kalshi", "k")
        poly_base = event(
            "polymarket", "p1", participants=("Chelsea FC", "Arsenal"), activation=START + timedelta(minutes=5)
        )
        poly_more = event(
            "polymarket", "p2", participants=("Chelsea", "Arsenal FC"), activation=START + timedelta(minutes=5)
        )
        catalogs = (
            CatalogSnapshot("kalshi", (kalshi_event,), (market("kalshi", "km", "k"),)),
            CatalogSnapshot(
                "polymarket",
                (poly_base, poly_more),
                (
                    market("polymarket", "pm1", "p1", side="away"),
                    market("polymarket", "pm2", "p2", side="away"),
                ),
            ),
        )
        bundles, rejected = match_events(catalogs, tolerance_seconds=900, minimum_venues=2)
        self.assertEqual(rejected, ())
        self.assertEqual(len(bundles), 1)
        self.assertEqual(len(bundles[0].events), 3)
        self.assertEqual(len(bundles[0].markets), 3)
        self.assertEqual(bundles[0].confidence, "HIGH")

    def test_non_latin_participant_names_do_not_collapse_to_the_same_key(self) -> None:
        self.assertEqual(
            canonical_participant("Ventforet Kōfu"),
            canonical_participant("Ventforet Kofu"),
        )
        self.assertNotEqual(
            canonical_participant("Team 北京"),
            canonical_participant("Team 上海"),
        )
        bundles, rejected = match_events(
            (
                CatalogSnapshot(
                    "kalshi",
                    (event("kalshi", "beijing", participants=("Team 北京", "Alpha")),),
                    (),
                ),
                CatalogSnapshot(
                    "polymarket",
                    (event("polymarket", "shanghai", participants=("Team 上海", "Alpha")),),
                    (),
                ),
            ),
            tolerance_seconds=900,
            minimum_venues=2,
        )
        self.assertEqual(bundles, ())
        self.assertEqual(len(rejected), 2)

    def test_format_mismatch_is_not_auto_matched(self) -> None:
        first = event("kalshi", "k", format="3")
        second = event("polymarket", "p", format="5")
        bundles, rejected = match_events(
            (CatalogSnapshot("kalshi", (first,), ()), CatalogSnapshot("polymarket", (second,), ())),
            tolerance_seconds=900,
            minimum_venues=2,
        )
        self.assertEqual(bundles, ())
        self.assertEqual(rejected[0].reason, "competition_format_mismatch")


class AdapterContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.strategy = load_strategy(STRATEGY_PATH)
        self.registry = MarketClassRegistry(self.strategy)

    def test_terminal_probes_use_only_definitive_venue_signals(self) -> None:
        kalshi = KalshiSportsAdapter(self.registry)
        polymarket = PolymarketSportsAdapter(self.registry)
        limitless = LimitlessSportsAdapter(self.registry)

        kalshi_result = kalshi.probe_terminal(
            FakeClient(
                lambda *_args: {
                    "markets": [
                        {
                            "ticker": "K-OPEN",
                            "status": "active",
                            "close_time": "2026-08-04T00:00:00Z",
                            "expiration_time": "2026-08-04T00:00:00Z",
                        },
                        {
                            "ticker": "K-DONE",
                            "status": "finalized",
                            "close_time": "2026-08-03T12:00:00Z",
                            "expiration_time": "2026-08-05T12:00:00Z",
                        },
                    ]
                }
            ),
            ("K-OPEN", "K-DONE"),
        )
        self.assertEqual(kalshi_result["K-OPEN"].state, "open")
        self.assertEqual(kalshi_result["K-DONE"].state, "terminal")

        polymarket_result = polymarket.probe_terminal(
            FakeClient(
                lambda _base, path, _params: {
                    "id": path.rsplit("/", 1)[-1],
                    "active": True,
                    "acceptingOrders": path.endswith("open"),
                }
            ),
            ("open", "closed"),
        )
        self.assertEqual(polymarket_result["open"].state, "open")
        self.assertEqual(polymarket_result["closed"].state, "terminal")

        limitless_result = limitless.probe_terminal(
            FakeClient(
                lambda _base, path, _params: {
                    "slug": path.rsplit("/", 1)[-1],
                    "tradeType": "clob",
                    "expired": path.endswith("done"),
                    "status": "RESOLVED" if path.endswith("done") else "FUNDED",
                }
            ),
            {"l-open": "/markets/open", "l-done": "/markets/done"},
        )
        self.assertEqual(limitless_result["l-open"].state, "open")
        self.assertEqual(limitless_result["l-done"].state, "terminal")

    def test_failed_or_malformed_terminal_probe_is_unknown(self) -> None:
        def failed(*_args):
            raise RuntimeError("404")

        kalshi = KalshiSportsAdapter(self.registry)
        polymarket = PolymarketSportsAdapter(self.registry)
        limitless = LimitlessSportsAdapter(self.registry)
        self.assertEqual(
            kalshi.probe_terminal(FakeClient(failed), ("missing",))["missing"].state,
            "unknown",
        )
        self.assertEqual(
            kalshi.probe_terminal(
                FakeClient(
                    lambda *_args: {
                        "markets": [
                            {
                                "ticker": "K-ACTIVE",
                                "status": "active",
                                "close_time": "2026-08-03T12:00:00Z",
                                "expected_expiration_time": "2026-08-05T12:00:00Z",
                            }
                        ]
                    }
                ),
                ("K-ACTIVE",),
            )["K-ACTIVE"].state,
            "open",
            "expected_expiration_time must not stand in for expiration_time",
        )
        self.assertEqual(
            polymarket.probe_terminal(FakeClient(failed), ("missing",))["missing"].state,
            "unknown",
        )
        self.assertEqual(
            limitless.probe_terminal(
                FakeClient(lambda *_args: {"tradeType": "clob"}),
                {"limitless:bad": "/markets/bad"},
            )["limitless:bad"].state,
            "unknown",
        )

    def test_kalshi_paginates_series_events_and_normalizes_a_small_shape(self) -> None:
        def handler(_base, path, params):
            if path == "/series":
                return {
                    "series": [{
                        "category": "Sports", "ticker": "KXSOCCERML",
                        "title": "Soccer Match Winner", "tags": ["Soccer", "Premier League"],
                    }]
                }
            self.assertEqual(path, "/events")
            self.assertTrue(params["with_nested_markets"])
            return {
                "events": [{
                    "event_ticker": "K-ARS-CHE", "series_ticker": "KXSOCCERML",
                    "title": "Arsenal vs Chelsea",
                    "strike_date": START.isoformat(),
                    "markets": [{
                        "ticker": "K-ARS", "title": "Will Arsenal win?",
                        "yes_sub_title": "Arsenal", "no_sub_title": "Not Arsenal",
                        "status": "active", "rules_primary": "Arsenal must win in regulation time only.",
                        "created_time": (NOW - timedelta(days=2)).isoformat(),
                        # The real catalogue shape: a contract count and a
                        # price. Kalshi publishes no dollar volume, and both
                        # ``notional_value_dollars`` and ``liquidity_dollars``
                        # are constants that describe nothing about this market.
                        "volume_fp": "1000000.00", "last_price_dollars": "0.6400",
                        "notional_value_dollars": "1.0000",
                        "liquidity_dollars": "0.0000",
                    }],
                }],
                "cursor": "",
            }

        result = KalshiSportsAdapter(self.registry).discover(FakeClient(handler), now=NOW)
        self.assertEqual(len(result.events), 1)
        self.assertEqual(result.markets[0].parameters["side"], "home")
        self.assertEqual(result.markets[0].canonical_class, "soccer.moneyline_3way")
        self.assertEqual(result.markets[0].volume_total, 1_000_000)
        # Contracts x price, not the $1-per-contract settlement notional.
        self.assertEqual(result.markets[0].volume_total_usd, 640_000.0)
        self.assertIsNone(result.markets[0].liquidity)

    def test_kalshi_current_game_and_colon_suffixed_spread_shapes_are_supported(self) -> None:
        def handler(_base, path, _params):
            if path == "/series":
                return {
                    "series": [
                        {
                            "category": "Sports", "ticker": "KXUCLGAME",
                            "title": "UEFA Champions League Game", "tags": ["Soccer"],
                        },
                        {
                            "category": "Sports", "ticker": "KXUCLSPREAD",
                            "title": "UEFA Champions League Spread", "tags": ["Soccer"],
                        },
                    ]
                }
            self.assertEqual(path, "/events")
            return {
                "events": [
                    {
                        "event_ticker": "KXUCLGAME-26AUG04MJASLO",
                        "series_ticker": "KXUCLGAME",
                        "title": "Mjallby vs Slovan Bratislava",
                        "strike_date": START.isoformat(),
                        "markets": [{
                            "ticker": "KXUCLGAME-MJA", "title": "Will Mjallby win?",
                            "yes_sub_title": "Mjallby", "status": "active",
                        }],
                    },
                    {
                        "event_ticker": "KXUCLSPREAD-26AUG04MJASLO",
                        "series_ticker": "KXUCLSPREAD",
                        "title": "Mjallby vs Slovan Bratislava: Spread",
                        "strike_date": START.isoformat(),
                        "markets": [{
                            "ticker": "KXUCLSPREAD-MJA15", "title": "Mjallby (-1.5)",
                            "yes_sub_title": "Mjallby -1.5", "status": "active",
                        }],
                    },
                ],
                "cursor": "",
            }

        result = KalshiSportsAdapter(self.registry).discover(FakeClient(handler), now=NOW)
        self.assertEqual(
            {event.participants for event in result.events},
            {("Mjallby", "Slovan Bratislava")},
        )
        self.assertEqual(
            {market.canonical_class for market in result.markets},
            {"soccer.moneyline_3way", "soccer.spread"},
        )

    def test_kalshi_non_fulltime_soccer_series_are_not_registered_as_fulltime(self) -> None:
        for title in (
            "Champions League First Half Winner",
            "Champions League First Half Spread",
            "Champions League Correct Score after Extra Time",
        ):
            with self.subTest(title=title):
                self.assertIsNone(
                    self.registry.classify(
                        "kalshi", "soccer", {"series_title": title}
                    )
                )

    def test_polymarket_uses_keyset_shape_and_token_ids(self) -> None:
        def handler(_base, path, params):
            self.assertEqual(path, "/events/keyset")
            if params["tag_slug"] == "esports":
                return {"events": [], "next_cursor": ""}
            return {
                "events": [{
                    "id": "p-event", "title": "Arsenal vs Chelsea", "endDate": START.isoformat(),
                    "closed": False, "tags": [{"slug": "soccer"}, {"slug": "premier-league"}],
                    "markets": [{
                        "id": "p-market", "groupItemTitle": "Arsenal",
                        "question": "Will Arsenal win?", "clobTokenIds": '["yes-token","no-token"]',
                        "outcomes": '["Yes","No"]', "active": True, "closed": False,
                        "acceptingOrders": True, "createdAt": (NOW - timedelta(days=2)).isoformat(),
                        "volumeNum": 13_500.5,
                    }],
                }],
                "next_cursor": "",
            }

        result = PolymarketSportsAdapter(self.registry).discover(FakeClient(handler), now=NOW)
        self.assertEqual(len(result.markets), 1)
        self.assertEqual(result.markets[0].subscription_ids, ("yes-token", "no-token"))
        self.assertEqual(result.markets[0].parameters["side"], "home")
        self.assertEqual(result.markets[0].volume_total_usd, 13_500.5)

    def test_polymarket_prefers_event_start_and_defensively_enforces_horizon(self) -> None:
        def handler(_base, _path, params):
            if params["tag_slug"] == "esports":
                return {"events": [], "next_cursor": ""}
            return {
                "events": [
                    {
                        "id": "inside", "title": "Arsenal vs Chelsea",
                        "startTime": START.isoformat(),
                        "endDate": (START + timedelta(hours=3)).isoformat(),
                        "tags": [{"slug": "soccer"}],
                        "markets": [{
                            "id": "inside-market", "groupItemTitle": "Arsenal",
                            "clobTokenIds": '["yes","no"]', "outcomes": '["Yes","No"]',
                            "active": True, "acceptingOrders": True,
                        }],
                    },
                    {
                        "id": "outside", "title": "Liverpool vs Everton",
                        "startTime": (NOW + timedelta(days=5)).isoformat(),
                        "endDate": (NOW + timedelta(days=5)).isoformat(),
                        "tags": [{"slug": "soccer"}],
                        "markets": [{
                            "id": "outside-market", "groupItemTitle": "Liverpool",
                            "clobTokenIds": '["a","b"]', "outcomes": '["Yes","No"]',
                            "active": True, "acceptingOrders": True,
                        }],
                    },
                ],
                "next_cursor": "",
            }

        result = PolymarketSportsAdapter(self.registry).discover(FakeClient(handler), now=NOW)
        self.assertEqual([event.venue_event_id for event in result.events], ["inside"])
        self.assertEqual(result.events[0].activation_at, START)

    def test_limitless_attaches_a_prop_by_canonical_participants(self) -> None:
        expiration = int((START + timedelta(hours=2)).timestamp())

        def handler(_base, path, params):
            # Discovery now reads the Sports query plus each configured
            # category; this fixture only exercises the Sports source.
            if path != "/markets/active":
                self.assertTrue(path.startswith("/markets/active/"))
                return {"data": [], "totalMarketsCount": 0}
            return {
                "data": [
                    {
                        "id": 10, "title": "Arsenal FC vs Chelsea", "slug": "ars-che",
                        "expirationTimestamp": expiration, "status": "open",
                        "metadata": {
                            "homeTeam": "Arsenal FC", "awayTeam": "Chelsea", "sportType": "Soccer",
                            "leagueKey": "premier-league", "marketType": "match_winner",
                            "startMatchTimestampInUTC": START.isoformat(),
                        },
                        "markets": [{
                            "id": 11, "slug": "ars-win", "title": "Arsenal FC",
                            "tradeType": "clob", "status": "open", "createdAt": (NOW - timedelta(days=1)).isoformat(),
                            "volumeFormatted": "14000.75",
                        }],
                    },
                    {
                        "id": 12, "slug": "both-score", "title": "Arsenal and Chelsea both to score?",
                        "expirationTimestamp": expiration, "tradeType": "clob", "status": "open",
                        "description": "Both teams must score in regulation time only.",
                        "categories": ["Soccer"],
                    },
                ],
                "totalMarketsCount": 2,
            }

        result = LimitlessSportsAdapter(self.registry).discover(FakeClient(handler), now=NOW)
        self.assertEqual({market.canonical_class for market in result.markets}, {
            "soccer.moneyline_3way", "soccer.both_teams_to_score",
        })
        self.assertEqual({market.venue_event_id for market in result.markets}, {"10"})
        moneyline = next(
            market for market in result.markets if market.market_type == "moneyline_3way"
        )
        self.assertEqual(moneyline.volume_total_usd, 14_000.75)

    def test_repeated_polymarket_cursor_fails_instead_of_looping(self) -> None:
        client = FakeClient(lambda *_args: {"events": [], "next_cursor": "same"})
        with self.assertRaisesRegex(ValueError, "repeated cursor"):
            PolymarketSportsAdapter(self.registry).discover(client, now=NOW)

    def test_polymarket_terminal_cursor_is_not_requested_again(self) -> None:
        client = FakeClient(lambda *_args: {"events": [], "next_cursor": "LTE="})
        result = PolymarketSportsAdapter(self.registry).discover(client, now=NOW)
        self.assertEqual(result.requests, 2, "one terminal page per configured tag")

    def test_limitless_repeated_page_fails_closed(self) -> None:
        payload = {
            "data": [{"id": 1, "slug": "same"}],
            "totalMarketsCount": 50,
        }
        with self.assertRaisesRegex(ValueError, "repeated page"):
            LimitlessSportsAdapter(self.registry, page_size=1).discover(
                FakeClient(lambda *_args: payload),
                now=NOW,
            )

    def test_limitless_reconciles_a_live_total_change_by_stable_id(self) -> None:
        calls = 0

        def handler(_base, path, params):
            nonlocal calls
            if path != "/markets/active":
                return {"data": [], "totalMarketsCount": 0}
            calls += 1
            page = params["page"]
            second_pass = calls > 2
            if page == 1:
                return {
                    "data": [
                        {"id": identifier, "slug": f"market-{identifier}"}
                        for identifier in range(1, 26)
                    ],
                    "totalMarketsCount": 49 if second_pass else 50,
                }
            return {
                "data": [
                    {"id": identifier, "slug": f"market-{identifier}"}
                    for identifier in range(26, 50)
                ],
                "totalMarketsCount": 49,
            }

        result = LimitlessSportsAdapter(self.registry).discover(
            FakeClient(handler),
            now=NOW,
        )

        self.assertTrue(result.complete)
        # Four for the reconciled Sports source, one terminal page for each
        # configured esports category.
        self.assertEqual(
            result.requests, 4 + len(self.registry.strategy.limitless_category_ids)
        )
        self.assertTrue(
            any("totalMarketsCount changed" in item for item in result.diagnostics)
        )
        self.assertTrue(
            any("reconciled" in item for item in result.diagnostics)
        )

    def test_limitless_rejects_premature_pagination_exhaustion(self) -> None:
        with self.assertRaisesRegex(ValueError, "ended before reported total"):
            LimitlessSportsAdapter(self.registry).discover(
                FakeClient(lambda *_args: {"data": [], "totalMarketsCount": 5}),
                now=NOW,
            )

    def test_polymarket_does_not_relabel_half_time_as_full_time_moneyline(self) -> None:
        def handler(_base, _path, params):
            if params["tag_slug"] == "esports":
                return {"events": [], "next_cursor": ""}
            return {
                "events": [{
                    "id": "half", "title": "Arsenal vs Chelsea - Halftime Result",
                    "endDate": START.isoformat(), "closed": False,
                    "tags": [{"slug": "soccer"}],
                    "markets": [{
                        "id": "half-arsenal", "groupItemTitle": "Arsenal",
                        "clobTokenIds": '["yes","no"]', "outcomes": '["Yes","No"]',
                        "active": True, "acceptingOrders": True,
                    }],
                }],
                "next_cursor": "",
            }

        result = PolymarketSportsAdapter(self.registry).discover(FakeClient(handler), now=NOW)
        self.assertEqual(result.events, ())
        self.assertEqual(result.markets, ())


class RulesAndSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.strategy = load_strategy(STRATEGY_PATH)

    def test_template_id_ignores_event_variables_but_not_language(self) -> None:
        first_event = event("kalshi", "one")
        second_event = CanonicalEvent(
            venue="kalshi", venue_event_id="two", sport="soccer", league="premier league",
            title="Liverpool vs Everton", participants=("Liverpool", "Everton"), activation_at=START,
            status="open", source_ref="/two",
        )
        first_market = market(
            "kalshi", "one-market", "one",
            rules="Arsenal wins if Arsenal has more goals than Chelsea on August 3, 2026 at 13:00 UTC.",
        )
        second_market = replace(
            first_market,
            venue_market_id="two-market", venue_event_id="two", title="Liverpool to win",
            rules_text="Liverpool wins if Liverpool has more goals than Everton on August 4, 2026 at 13:00 UTC.",
        )
        one = template_for(first_market, first_event, known_template_ids=frozenset())
        two = template_for(second_market, second_event, known_template_ids=frozenset())
        self.assertEqual(one.template_id, two.template_id)
        changed = template_for(
            replace(second_market, rules_text=second_market.rules_text + " Extra time counts."),
            second_event,
            known_template_ids=frozenset(),
        )
        self.assertNotEqual(one.template_id, changed.template_id)

    def test_exceptional_rule_drift_is_reported_but_non_blocking(self) -> None:
        k_event = event("kalshi", "k")
        bundle, _ = match_events(
            (
                CatalogSnapshot(
                    "kalshi", (k_event,),
                    (
                        market("kalshi", "one", "k", rules="Arsenal wins in regulation. If abandoned, void."),
                        market("kalshi", "two", "k", rules="Arsenal wins in regulation. If abandoned, waits 7 days."),
                    ),
                ),
                snapshot("polymarket", "p", "pm"),
            ),
            tolerance_seconds=900,
            minimum_venues=2,
        )
        assessment = assess_rules(bundle[0])
        self.assertEqual(len(assessment.drift), 1)
        self.assertFalse(assessment.drift[0]["blocking"])
        self.assertEqual(assessment.contradictions, {})

    def test_known_normal_scope_contradiction_is_blocking(self) -> None:
        conflicting = market(
            "kalshi", "m", "e", rules="The result includes extra time and penalty shootouts."
        )
        self.assertIn(
            "rules_include_extra_time_but_class_is_regulation",
            normal_path_contradictions(conflicting),
        )
        explicitly_excluded = replace(
            conflicting,
            rules_text="Regulation result, not including extra time or penalties.",
        )
        self.assertEqual(normal_path_contradictions(explicitly_excluded), ())

    def test_extra_time_exclusion_does_not_hide_penalty_inclusion(self) -> None:
        for rules in (
            "The result does not include extra time, but the penalty shootout will count.",
            "Regulation time only, but the penalty shootout will count.",
        ):
            with self.subTest(rules=rules):
                conflicting = market(
                    "kalshi", "mixed-scope", "e", rules=rules
                )
                self.assertIn(
                    "rules_include_extra_time_but_class_is_regulation",
                    normal_path_contradictions(conflicting),
                )

    def test_first_half_rule_language_is_not_accepted_as_fulltime(self) -> None:
        conflicting = market(
            "kalshi",
            "first-half",
            "e",
            rules="Resolves to the winner of the first half after 45 minutes.",
        )
        self.assertIn(
            "rules_are_first_half_but_class_is_fulltime",
            normal_path_contradictions(conflicting),
        )

    def test_reversed_event_order_remaps_correct_score_into_bundle_order(self) -> None:
        from targeter.v2.relationships import derive_bundle_relationships

        kalshi_event = event("kalshi", "k-score")
        polymarket_event = event(
            "polymarket", "p-score", participants=("Chelsea", "Arsenal")
        )
        kalshi_score = replace(
            market("kalshi", "k-1-0", "k-score"),
            canonical_class="soccer.correct_score",
            market_type="correct_score",
            title="1-0",
            parameters={"home_goals": 1, "away_goals": 0},
        )
        polymarket_score = replace(
            market("polymarket", "p-1-0", "p-score"),
            canonical_class="soccer.correct_score",
            market_type="correct_score",
            title="1-0",
            parameters={"home_goals": 1, "away_goals": 0},
        )
        bundles, _ = match_events(
            (
                CatalogSnapshot("kalshi", (kalshi_event,), (kalshi_score,)),
                CatalogSnapshot("polymarket", (polymarket_event,), (polymarket_score,)),
            ),
            tolerance_seconds=900,
            minimum_venues=2,
        )
        analysis = derive_bundle_relationships(bundles[0])
        masks = {mask.market_key: mask.outcome_keys for mask in analysis.masks}
        self.assertEqual(masks["kalshi:k-1-0#claim=0"], {"score:1-0"})
        self.assertEqual(masks["polymarket:p-1-0#claim=0"], {"score:0-1"})
        self.assertEqual(
            [item.relationship for item in analysis.relationships if item.cross_venue],
            ["MUTUAL_EXCLUSION"],
        )

    def test_positive_handicap_is_not_misread_as_win_by_that_margin(self) -> None:
        kalshi = snapshot("kalshi", "k", "km")
        poly_event = event("polymarket", "p")
        underdog = CanonicalMarket(
            venue="polymarket", venue_market_id="spread", venue_event_id="p",
            canonical_class="soccer.spread", market_type="spread", scope="regulation_fulltime",
            title="Chelsea (+1.5)",
            parameters={"side": "away", "line": 1.5, "line_style": "handicap"},
            subscription_ids=("spread-token",), outcome_labels=("Yes", "No"),
            status="open", accepting_orders=True, created_at=NOW - timedelta(days=1),
        )
        # Pair it with the equivalent Kalshi-style affirmative claim: Arsenal
        # does not win by more than 1.5. The explicit handicap resolver must
        # cover draws and one-goal Arsenal wins, not only Chelsea wins by 2+.
        from targeter.v2.matching import match_events
        from targeter.v2.relationships import derive_bundle_relationships

        bundles, _ = match_events(
            (kalshi, CatalogSnapshot("polymarket", (poly_event,), (underdog,))),
            tolerance_seconds=900,
            minimum_venues=2,
        )
        analysis = derive_bundle_relationships(bundles[0])
        spread_mask = next(mask for mask in analysis.masks if mask.market_key.startswith("polymarket:spread"))
        self.assertIn("score:1-0", spread_mask.outcome_keys)
        self.assertIn("score:0-0", spread_mask.outcome_keys)
        self.assertNotIn("score:2-0", spread_mask.outcome_keys)

    def test_two_token_polymarket_spread_compiles_complementary_claims(self) -> None:
        from targeter.v2.relationships import derive_bundle_relationships

        polymarket_event = event("polymarket", "p-spread")
        spread = CanonicalMarket(
            venue="polymarket",
            venue_market_id="spread-pair",
            venue_event_id="p-spread",
            canonical_class="soccer.spread",
            market_type="spread",
            scope="regulation_fulltime",
            title="Arsenal (-1.5)",
            parameters={"side": "home", "line": -1.5, "line_style": "handicap"},
            subscription_ids=("arsenal-token", "chelsea-token"),
            outcome_labels=("Arsenal", "Chelsea"),
            status="open",
            accepting_orders=True,
            created_at=NOW - timedelta(days=1),
        )
        bundles, _ = match_events(
            (
                snapshot("kalshi", "k-spread", "k-moneyline"),
                CatalogSnapshot("polymarket", (polymarket_event,), (spread,)),
            ),
            tolerance_seconds=900,
            minimum_venues=2,
        )
        analysis = derive_bundle_relationships(bundles[0])
        spread_masks = [
            mask
            for mask in analysis.masks
            if mask.market_key.startswith("polymarket:spread-pair")
        ]
        self.assertEqual(len(spread_masks), 2)
        self.assertEqual(spread_masks[0].outcome_keys & spread_masks[1].outcome_keys, set())
        self.assertEqual(
            spread_masks[0].outcome_keys | spread_masks[1].outcome_keys,
            analysis.spaces[0].keys,
        )

    def test_map_handicap_uses_token_order_when_title_uses_an_alias(self) -> None:
        from targeter.v2.domain import EventBundle
        from targeter.v2.relationships import derive_bundle_relationships

        poly_event = event(
            "polymarket", "p-map", participants=("fnatic", "Lilmix"), format="3"
        )
        handicap = CanonicalMarket(
            venue="polymarket", venue_market_id="map-handicap", venue_event_id="p-map",
            canonical_class="esports.map_handicap", market_type="map_handicap", scope="series",
            title="Map Handicap: FNC (-1.5) vs Lilmix (+1.5)",
            parameters={"line": -1.5, "line_style": "handicap"},
            subscription_ids=("fnatic-token", "lilmix-token"),
            outcome_labels=("fnatic", "Lilmix"), status="open", accepting_orders=True,
        )
        bundle = EventBundle(
            bundle_id="bundle-map", sport="esports", participants=("fnatic", "Lilmix"),
            participant_keys=("fnatic", "lilmix"), activation_at=START,
            events=(poly_event,), markets=(handicap,), confidence="HIGH",
        )
        analysis = derive_bundle_relationships(bundle)
        self.assertEqual([mask.status for mask in analysis.masks], ["DERIVABLE", "DERIVABLE"])
        self.assertEqual(
            analysis.masks[0].outcome_keys | analysis.masks[1].outcome_keys,
            analysis.spaces[0].keys,
        )

    def test_two_venue_identity_is_selected_at_the_t_minus_one_hour_gate(self) -> None:
        result = select_targets(
            (snapshot("kalshi", "k", "km"), snapshot("polymarket", "p", "pm")),
            strategy=self.strategy,
            now=NOW,
        )
        self.assertEqual(len(result.selected), 1)
        relationship = result.selected[0].relationships.relationships[0]
        self.assertEqual(relationship.relationship, "IDENTITY")
        self.assertTrue(relationship.cross_venue)
        self.assertEqual(relationship.coverage, "INCOMPLETE_COVERAGE")
        self.assertFalse(result.as_record()["selection"]["publication_performed"])

    def test_event_is_rejected_below_normalized_moneyline_volume_threshold(self) -> None:
        result = select_targets(
            (
                snapshot("kalshi", "k-low", "km-low", volume_total_usd=10_000),
                snapshot("polymarket", "p-low", "pm-low", volume_total_usd=10_000),
            ),
            strategy=self.strategy,
            now=NOW,
        )

        self.assertEqual(result.selected, ())
        candidate = result.candidates[0]
        self.assertIn(
            "combined_moneyline_volume_usd_below_minimum",
            candidate.rejection_reasons,
        )
        record = candidate.as_record()
        self.assertEqual(record["event_status"], "REJECTED")
        self.assertEqual(record["admission"]["combined_moneyline_volume_usd"], 20_000)
        self.assertEqual(record["admission"]["minimum_moneyline_volume_usd"], 25_000)

    def test_contract_count_is_not_treated_as_dollar_volume(self) -> None:
        kalshi = snapshot(
            "kalshi", "k-contracts", "km-contracts", volume_total_usd=None
        )
        kalshi_market = replace(kalshi.markets[0], volume_total=1_000_000)
        polymarket = snapshot(
            "polymarket", "p-dollars", "pm-dollars", volume_total_usd=20_000
        )

        result = select_targets(
            (
                CatalogSnapshot("kalshi", kalshi.events, (kalshi_market,)),
                polymarket,
            ),
            strategy=self.strategy,
            now=NOW,
        )

        self.assertEqual(result.selected, ())
        admission = result.candidates[0].as_record()["admission"]
        self.assertEqual(admission["combined_moneyline_volume_usd"], 20_000)
        self.assertEqual(admission["moneyline_volume_usd_by_venue"]["kalshi"], 0)
        self.assertEqual(
            admission["moneyline_volume_usd_coverage"]["kalshi"],
            {"known_markets": 0, "unknown_markets": 1},
        )

    def test_bad_sibling_is_excluded_without_vetoing_the_event(self) -> None:
        polymarket = snapshot("polymarket", "p", "pm")
        bad_sibling = replace(
            market("polymarket", "p-first-half-total", "p"),
            canonical_class="soccer.total_goals",
            market_type="total_goals",
            title="Over 2.5 goals",
            parameters={"line": 2.5, "direction": "over"},
            rules_text="Resolves from the first-half result after 45 minutes.",
        )

        result = select_targets(
            (
                snapshot("kalshi", "k", "km"),
                CatalogSnapshot(
                    "polymarket",
                    polymarket.events,
                    (*polymarket.markets, bad_sibling),
                ),
            ),
            strategy=self.strategy,
            now=NOW,
        )

        self.assertEqual(len(result.selected), 1)
        candidate = result.selected[0]
        self.assertNotIn(bad_sibling, candidate.eligible_markets)
        self.assertEqual(candidate.rejection_reasons, ())
        self.assertEqual(
            candidate.market_exclusions[bad_sibling.target_id],
            ("rules_are_first_half_but_class_is_fulltime",),
        )
        self.assertEqual(candidate.as_record()["event_status"], "ELIGIBLE")

    def test_bundle_is_not_selected_too_early(self) -> None:
        early = START - timedelta(hours=4)
        catalogs = (
            CatalogSnapshot("kalshi", (replace(event("kalshi", "k"), activation_at=early),), (market("kalshi", "km", "k"),)),
            CatalogSnapshot("polymarket", (replace(event("polymarket", "p"), activation_at=early),), (market("polymarket", "pm", "p"),)),
        )
        result = select_targets(catalogs, strategy=self.strategy, now=NOW - timedelta(hours=8))
        self.assertEqual(result.selected, ())
        self.assertIn("before_capture_lookahead", result.candidates[0].rejection_reasons)

    def test_three_venue_bundle_ranks_before_two_venue_bundle(self) -> None:
        three_start = START
        two_start = START + timedelta(minutes=20)
        three = (
            snapshot("kalshi", "k3", "km3"),
            snapshot("polymarket", "p3", "pm3"),
            snapshot("limitless", "l3", "lm3"),
        )
        two = (
            CatalogSnapshot("kalshi", (replace(event("kalshi", "k2"), activation_at=two_start, participants=("Liverpool", "Everton"), title="Liverpool vs Everton"),), (replace(market("kalshi", "km2", "k2"), title="Liverpool to win"),)),
            CatalogSnapshot("polymarket", (replace(event("polymarket", "p2"), activation_at=two_start, participants=("Liverpool", "Everton"), title="Liverpool vs Everton"),), (replace(market("polymarket", "pm2", "p2"), title="Liverpool to win"),)),
        )
        merged = []
        for venue in ("kalshi", "polymarket", "limitless"):
            venue_events = tuple(item for snap in (*three, *two) if snap.venue == venue for item in snap.events)
            venue_markets = tuple(item for snap in (*three, *two) if snap.venue == venue for item in snap.markets)
            if venue_events:
                merged.append(CatalogSnapshot(venue, venue_events, venue_markets))
        result = select_targets(merged, strategy=self.strategy, now=NOW)
        self.assertEqual(len(result.selected[0].bundle.venues), 3)

    def test_three_venue_priority_cannot_be_overwhelmed_by_listing_count(self) -> None:
        two_start = START + timedelta(minutes=20)
        catalogs = []
        for venue in ("kalshi", "polymarket", "limitless"):
            three_event = event(venue, f"{venue}-three")
            venue_events = [three_event]
            venue_markets = [market(venue, f"{venue}-three-market", three_event.venue_event_id)]
            if venue != "limitless":
                two_event = event(
                    venue,
                    f"{venue}-two",
                    participants=("Liverpool", "Everton"),
                    activation=two_start,
                )
                venue_events.append(two_event)
                venue_markets.extend(
                    replace(
                        market(venue, f"{venue}-two-{index}", two_event.venue_event_id),
                        title="Liverpool to win",
                    )
                    for index in range(10)
                )
            catalogs.append(
                CatalogSnapshot(venue, tuple(venue_events), tuple(venue_markets))
            )

        result = select_targets(
            catalogs,
            strategy=replace(self.strategy, maximum_bundles=1),
            now=NOW,
        )
        self.assertEqual(len(result.selected), 1)
        self.assertEqual(len(result.selected[0].bundle.venues), 3)

    def test_new_markets_do_not_participate_until_mature(self) -> None:
        fresh = NOW - timedelta(minutes=5)
        result = select_targets(
            (
                snapshot("kalshi", "k", "km", created_at=fresh),
                snapshot("polymarket", "p", "pm", created_at=fresh),
            ),
            strategy=self.strategy,
            now=NOW,
        )
        self.assertEqual(result.selected, ())
        self.assertIn("fewer_than_minimum_eligible_venues", result.candidates[0].rejection_reasons)

    def test_plain_overlap_is_reported_but_does_not_activate_capture(self) -> None:
        poly_event = event("polymarket", "p-total")
        total = CanonicalMarket(
            venue="polymarket", venue_market_id="total", venue_event_id="p-total",
            canonical_class="soccer.total_goals", market_type="total_goals",
            scope="regulation_fulltime", title="Over 2.5",
            parameters={"line": 2.5, "direction": "over"},
            subscription_ids=("total-token",), outcome_labels=("Yes", "No"),
            status="open", accepting_orders=True, created_at=NOW - timedelta(days=1),
        )
        result = select_targets(
            (
                snapshot("kalshi", "k-moneyline", "km-moneyline"),
                CatalogSnapshot("polymarket", (poly_event,), (total,)),
            ),
            strategy=self.strategy,
            now=NOW,
        )
        self.assertEqual(result.selected, ())
        self.assertIn(
            "no_cross_venue_structural_relationship",
            result.candidates[0].rejection_reasons,
        )
        self.assertEqual(
            {item.relationship for item in result.candidates[0].relationships.relationships},
            {"OVERLAP"},
        )

    def test_budget_is_applied_to_whole_bundles(self) -> None:
        constrained = replace(
            self.strategy,
            target_budgets={"kalshi": 1, "polymarket": 1, "limitless": 1},
        )
        poly = snapshot("polymarket", "p", "pm")
        poly_market = replace(poly.markets[0], subscription_ids=("p-yes", "p-no"))
        result = select_targets(
            (snapshot("kalshi", "k", "km"), CatalogSnapshot("polymarket", poly.events, (poly_market,))),
            strategy=constrained,
            now=NOW,
        )
        self.assertEqual(result.selected, ())
        self.assertEqual(result.budget_used, {"kalshi": 0, "polymarket": 0, "limitless": 0})

    def test_retained_bundle_survives_one_terminal_leg_and_higher_ranked_newcomer(self) -> None:
        retained = ContinuityBundle(
            base_run_id="20260803T110000.000000Z",
            bundle_id="prior",
            activation_at=NOW,
            score=1.0,
            targets=(
                ContinuityTarget(
                    target_id="kalshi:old-k",
                    venue="kalshi",
                    venue_market_id="old-k",
                    canonical_class="soccer.moneyline_3way",
                    subscription_ids=("old-k-sub",),
                    activation_at=NOW,
                    capture_start_at=NOW - timedelta(hours=1),
                    source_ref="/markets/old-k",
                    probe=TerminalProbe("terminal", "status_finalized"),
                ),
                ContinuityTarget(
                    target_id="polymarket:old-p",
                    venue="polymarket",
                    venue_market_id="old-p",
                    canonical_class="soccer.moneyline_3way",
                    subscription_ids=("old-p-sub",),
                    activation_at=NOW,
                    capture_start_at=NOW - timedelta(hours=1),
                    source_ref="/markets/old-p",
                    probe=TerminalProbe("open", "accepting_orders"),
                ),
            ),
        )
        constrained = replace(
            self.strategy,
            target_budgets={"kalshi": 1, "polymarket": 1, "limitless": 1},
        )
        result = select_targets(
            (snapshot("kalshi", "k", "km"), snapshot("polymarket", "p", "pm")),
            strategy=constrained,
            now=NOW,
            continuity_bundles=(retained,),
        )

        self.assertEqual(result.as_record()["selection"]["bundle_ids"], ["prior"])
        self.assertEqual(result.targets["kalshi"][0]["target_id"], "kalshi:old-k")
        self.assertEqual(
            next(iter(result.allocation_rejections.values())),
            "displaced_by_continuity_hold",
        )

    def test_held_bundle_does_not_replace_its_committed_subscription_ids(self) -> None:
        catalogs = (
            snapshot("kalshi", "k", "km"),
            snapshot("polymarket", "p", "pm"),
        )
        fresh = select_targets(catalogs, strategy=self.strategy, now=NOW)
        candidate = fresh.selected[0]
        prior = ContinuityBundle(
            base_run_id="20260803T110000.000000Z",
            bundle_id=candidate.bundle.bundle_id,
            activation_at=candidate.bundle.activation_at,
            score=candidate.score,
            targets=tuple(
                ContinuityTarget(
                    target_id=market.target_id,
                    venue=market.venue,
                    venue_market_id=market.venue_market_id,
                    canonical_class=market.canonical_class,
                    subscription_ids=(f"committed-{market.venue}",),
                    activation_at=candidate.bundle.activation_at,
                    capture_start_at=candidate.capture_start_at,
                    source_ref=market.source_ref,
                    probe=TerminalProbe("open", "accepting_orders"),
                )
                for market in candidate.eligible_markets
            ),
        )

        held = select_targets(
            catalogs,
            strategy=self.strategy,
            now=NOW,
            continuity_bundles=(prior,),
        )

        self.assertEqual(
            {
                target["subscription_ids"][0]
                for venue_targets in held.targets.values()
                for target in venue_targets
            },
            {"committed-kalshi", "committed-polymarket"},
        )

    def test_past_post_start_retention_rejects_only_fresh_admission(self) -> None:
        catalogs = (
            snapshot("kalshi", "k", "km"),
            snapshot("polymarket", "p", "pm"),
        )
        fresh = select_targets(catalogs, strategy=self.strategy, now=NOW)
        candidate = fresh.selected[0]
        prior = ContinuityBundle(
            base_run_id="20260803T110000.000000Z",
            bundle_id=candidate.bundle.bundle_id,
            activation_at=candidate.bundle.activation_at,
            score=candidate.score,
            targets=tuple(
                ContinuityTarget(
                    target_id=market.target_id,
                    venue=market.venue,
                    venue_market_id=market.venue_market_id,
                    canonical_class=market.canonical_class,
                    subscription_ids=market.subscription_ids,
                    activation_at=candidate.bundle.activation_at,
                    capture_start_at=candidate.capture_start_at,
                    source_ref=market.source_ref,
                    probe=TerminalProbe("open", "accepting_orders"),
                )
                for market in candidate.eligible_markets
            ),
        )

        later = candidate.bundle.activation_at + timedelta(hours=7)
        result = select_targets(
            catalogs,
            strategy=self.strategy,
            now=later,
            continuity_bundles=(prior,),
        )

        current = next(
            item for item in result.candidates if item.bundle.bundle_id == prior.bundle_id
        )
        self.assertIn("past_post_start_retention", current.rejection_reasons)
        self.assertEqual(result.as_record()["selection"]["bundle_ids"], [prior.bundle_id])
        self.assertEqual(result.continuity_dispositions[prior.bundle_id], "retained")

    def test_retained_bundle_retires_only_when_all_legs_terminal_or_clamped(self) -> None:
        def continuity(*states: str, activation: datetime = NOW) -> ContinuityBundle:
            return ContinuityBundle(
                base_run_id="20260803T110000.000000Z",
                bundle_id="prior",
                activation_at=activation,
                score=1.0,
                targets=tuple(
                    ContinuityTarget(
                        target_id=f"{venue}:old",
                        venue=venue,
                        venue_market_id="old",
                        canonical_class="soccer.moneyline_3way",
                        subscription_ids=(f"{venue}-sub",),
                        activation_at=activation,
                        capture_start_at=activation - timedelta(hours=1),
                        source_ref="/markets/old",
                        probe=TerminalProbe(state, state),
                    )
                    for venue, state in zip(("kalshi", "polymarket"), states, strict=True)
                ),
            )

        partial = select_targets(
            (), strategy=self.strategy, now=NOW + timedelta(hours=1),
            continuity_bundles=(continuity("terminal", "unknown"),),
        )
        terminal = select_targets(
            (), strategy=self.strategy, now=NOW + timedelta(hours=1),
            continuity_bundles=(continuity("terminal", "terminal"),),
        )
        clamped = select_targets(
            (), strategy=self.strategy, now=NOW,
            continuity_bundles=(continuity("open", "unknown", activation=NOW - timedelta(hours=9)),),
        )

        self.assertEqual(partial.as_record()["selection"]["bundle_ids"], ["prior"])
        self.assertEqual(terminal.as_record()["selection"]["bundle_ids"], [])
        self.assertEqual(clamped.as_record()["selection"]["bundle_ids"], [])

    def test_protected_budget_trims_the_lowest_score_bundle_atomically(self) -> None:
        def retained(bundle_id: str, score: float, suffix: str) -> ContinuityBundle:
            return ContinuityBundle(
                base_run_id="20260803T110000.000000Z",
                bundle_id=bundle_id,
                activation_at=NOW,
                score=score,
                targets=tuple(
                    ContinuityTarget(
                        target_id=f"{venue}:{suffix}",
                        venue=venue,
                        venue_market_id=suffix,
                        canonical_class="soccer.moneyline_3way",
                        subscription_ids=(f"{venue}-{suffix}",),
                        activation_at=NOW,
                        capture_start_at=NOW - timedelta(hours=1),
                        source_ref=f"/markets/{suffix}",
                        probe=TerminalProbe("unknown", "probe_failed"),
                    )
                    for venue in ("kalshi", "polymarket")
                ),
            )

        result = select_targets(
            (),
            strategy=replace(
                self.strategy,
                target_budgets={"kalshi": 1, "polymarket": 1, "limitless": 1},
            ),
            now=NOW,
            continuity_bundles=(
                retained("higher", 20.0, "high"),
                retained("lower", 10.0, "low"),
            ),
        )

        self.assertEqual(result.as_record()["selection"]["bundle_ids"], ["higher"])
        self.assertEqual(
            result.allocation_rejections["lower"], "continuity_budget_trimmed"
        )

    def test_unrelated_protected_venue_does_not_relabel_budget_exhaustion(self) -> None:
        first_participants = ("Arsenal", "Chelsea")
        second_participants = ("Liverpool", "Everton")
        catalogs = tuple(
            CatalogSnapshot(
                venue,
                (
                    event(venue, f"{venue}-first", participants=first_participants),
                    event(
                        venue,
                        f"{venue}-second",
                        participants=second_participants,
                        activation=START + timedelta(minutes=5),
                    ),
                ),
                (
                    market(venue, f"{venue}-first-market", f"{venue}-first"),
                    market(venue, f"{venue}-second-market", f"{venue}-second"),
                ),
            )
            for venue in ("kalshi", "polymarket")
        )
        limitless_hold = ContinuityBundle(
            base_run_id="20260803T110000.000000Z",
            bundle_id="limitless-only-hold",
            activation_at=START,
            score=1.0,
            targets=(
                ContinuityTarget(
                    target_id="limitless:held",
                    venue="limitless",
                    venue_market_id="held",
                    canonical_class="soccer.moneyline_3way",
                    subscription_ids=("limitless-held",),
                    activation_at=START,
                    capture_start_at=NOW,
                    source_ref="/markets/held",
                    probe=TerminalProbe("open", "funded_not_expired"),
                ),
            ),
        )

        result = select_targets(
            catalogs,
            strategy=replace(
                self.strategy,
                target_budgets={"kalshi": 1, "polymarket": 10, "limitless": 1},
            ),
            now=NOW,
            continuity_bundles=(limitless_hold,),
        )

        rejected = {
            bundle_id: reason
            for bundle_id, reason in result.allocation_rejections.items()
            if bundle_id != limitless_hold.bundle_id
        }
        self.assertEqual(list(rejected.values()), ["target_budget_exceeded"])


class ShadowRunTests(unittest.TestCase):
    def test_old_corrupt_generation_still_writes_a_discovery_report(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)

        class Adapter:
            def __init__(self, venue: str) -> None:
                self.venue = venue

            def discover(self, _client, *, now):
                return CatalogSnapshot(self.venue, (), ())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            live_root = root / "live"
            pointer = live_root / "targeter-v2" / "current.json"
            pointer.parent.mkdir(parents=True)
            pointer.write_text(
                json.dumps({"run_id": "20260803T070000.000000Z"}),
                encoding="utf-8",
            )
            with patch(
                "targeter.v2.run.load_continuity_bundles",
                side_effect=TargetsError("committed target identity mismatch"),
            ):
                result = run_shadow(
                    strategy=strategy,
                    output_root=root / "runs",
                    cache_root=root / "cache",
                    live_root=live_root,
                    now=NOW,
                    adapters=tuple(Adapter(venue) for venue in SUPPORTED_VENUES),
                    client=object(),
                    artifact_format="ndjson",
                )

            report = read_run_report(result.directory)
            self.assertTrue(report["input_complete"])
            self.assertEqual(
                report["continuity_diagnostics"],
                ["continuity_degraded_after_timeout: committed target identity mismatch"],
            )
            self.assertEqual(
                report["continuity_degraded_base_run_id"],
                "20260803T070000.000000Z",
            )

    def test_unreadable_recent_continuity_fails_closed_but_degrades_after_timeout(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)

        def attempt(run_age: timedelta, error: Exception):
            with tempfile.TemporaryDirectory() as directory:
                live_root = Path(directory)
                pointer = live_root / "targeter-v2" / "current.json"
                pointer.parent.mkdir(parents=True)
                run_time = NOW - run_age
                run_id = run_time.strftime("%Y%m%dT%H%M%S.000000Z")
                pointer.write_text(json.dumps({"run_id": run_id}), encoding="utf-8")
                with patch(
                    "targeter.v2.run.load_continuity_bundles",
                    side_effect=error,
                ):
                    return _continuity_for_run(
                        live_root=live_root,
                        adapters=(),
                        client=object(),
                        strategy=strategy,
                        now=NOW,
                    )

        with self.assertRaisesRegex(ContinuityError, "missing continuity metadata"):
            attempt(timedelta(hours=3), ContinuityError("missing continuity metadata"))
        bundles, diagnostics, degraded_base_run_id = attempt(
            timedelta(hours=5), ContinuityError("missing continuity metadata")
        )
        self.assertEqual(bundles, ())
        self.assertEqual(
            diagnostics,
            ["continuity_degraded_after_timeout: missing continuity metadata"],
        )
        self.assertEqual(degraded_base_run_id, "20260803T070000.000000Z")

        corrupt_bundles, corrupt_diagnostics, corrupt_base_run_id = attempt(
            timedelta(hours=5), TargetsError("committed target identity mismatch")
        )
        self.assertEqual(corrupt_bundles, ())
        self.assertEqual(
            corrupt_diagnostics,
            ["continuity_degraded_after_timeout: committed target identity mismatch"],
        )
        self.assertEqual(corrupt_base_run_id, "20260803T070000.000000Z")

    def test_raw_response_cache_control_is_explicit_and_incompatible_with_reuse(self) -> None:
        arguments = parse_args(["--no-response-cache"])
        self.assertTrue(arguments.no_response_cache)
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(["--reuse-cache", "--no-response-cache"])

    def test_zstd_is_the_default_artifact_format_with_an_ndjson_override(self) -> None:
        self.assertEqual(parse_args([]).artifact_format, "zstd")
        self.assertEqual(
            parse_args(["--artifact-format", "ndjson"]).artifact_format,
            "ndjson",
        )

    def test_writes_a_local_auditable_run_without_live_publication(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)

        class Adapter:
            def __init__(self, value):
                self.value = value
                self.venue = value.venue

            def discover(self, _client, *, now):
                return self.value

        with tempfile.TemporaryDirectory() as directory:
            result = run_shadow(
                strategy=strategy,
                output_root=Path(directory) / "out",
                cache_root=Path(directory) / "cache",
                now=NOW,
                adapters=(
                    Adapter(snapshot("kalshi", "k", "km")),
                    Adapter(snapshot("polymarket", "p", "pm")),
                ),
                client=object(),
            )
            report = read_run_report(result.directory)
            self.assertEqual(report["mode"], "shadow")
            self.assertFalse(report["selection"]["publication_performed"])
            self.assertEqual(report["selection"]["bundle_count"], 1)
            self.assertFalse(
                report["input_complete"],
                "omitting one supported venue is a diagnostic probe, not a complete production input",
            )
            self.assertEqual(report["artifact_format"], "zstd")
            self.assertTrue((result.directory / "catalog_kalshi_markets.ndjson.zst").exists())
            self.assertTrue((result.directory / "rule_templates.ndjson.zst").exists())
            self.assertTrue((result.directory / "selection_report.json.zst").exists())
            self.assertTrue((result.directory / "selection_report.meta.json").exists())
            self.assertFalse((result.directory / "catalog_kalshi_markets.ndjson").exists())
            self.assertFalse((result.directory / "selection_report.json").exists())

    def test_shadow_can_emit_plain_ndjson_for_local_inspection(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)

        class Adapter:
            venue = "kalshi"

            def discover(self, _client, *, now):
                return snapshot("kalshi", "k", "km")

        with tempfile.TemporaryDirectory() as directory:
            result = run_shadow(
                strategy=strategy,
                output_root=Path(directory) / "out",
                cache_root=Path(directory) / "cache",
                now=NOW,
                adapters=(Adapter(),),
                client=object(),
                artifact_format="ndjson",
            )

            report = read_run_report(result.directory)
            self.assertEqual(report["artifact_format"], "ndjson")
            self.assertTrue((result.directory / "catalog_kalshi_markets.ndjson").exists())
            self.assertTrue((result.directory / "rule_templates.ndjson").exists())
            self.assertTrue((result.directory / "selection_report.json").exists())
            self.assertFalse((result.directory / "catalog_kalshi_markets.ndjson.zst").exists())
            self.assertFalse((result.directory / "selection_report.meta.json").exists())

    def test_artifact_directory_fsync_failure_publishes_no_selection_report(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)

        class Adapter:
            venue = "kalshi"

            def discover(self, _client, *, now):
                return snapshot("kalshi", "k", "km")

        with tempfile.TemporaryDirectory() as directory, patch(
            "analysis.storage.fsync_directory_strict",
            side_effect=OSError("injected directory fsync failure"),
        ):
            output_root = Path(directory) / "out"
            with self.assertRaisesRegex(OSError, "directory fsync failure"):
                run_shadow(
                    strategy=strategy,
                    output_root=output_root,
                    cache_root=Path(directory) / "cache",
                    now=NOW,
                    adapters=(Adapter(),),
                    client=object(),
                )
            run_directory = output_root / "20260803T120000.000000Z"
            self.assertFalse((run_directory / "selection_report.meta.json").exists())

    def test_adapter_failure_is_preserved_in_the_report(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)

        class Broken:
            venue = "kalshi"

            def discover(self, _client, *, now):
                raise RuntimeError("catalog unavailable")

        with tempfile.TemporaryDirectory() as directory:
            result = run_shadow(
                strategy=strategy,
                output_root=Path(directory) / "out",
                cache_root=Path(directory) / "cache",
                now=NOW,
                adapters=(Broken(),),
                client=object(),
            )
            report = read_run_report(result.directory)
            self.assertFalse(report["input_complete"])
            self.assertIn("catalog unavailable", report["discovery_failures"]["kalshi"])

    def test_cli_returns_nonzero_for_a_preserved_incomplete_run(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)
        selection = select_targets((), strategy=strategy, now=NOW)
        shadow = ShadowRun(
            run_id="run",
            directory=Path("/tmp/not-written-by-mock"),
            selection=selection,
            discovery_failures={"kalshi": "RuntimeError: unavailable"},
            input_complete=False,
        )
        with patch("targeter.v2.run.load_strategy", return_value=strategy), patch(
            "targeter.v2.run.run_shadow", return_value=shadow
        ):
            with tempfile.TemporaryDirectory() as directory:
                self.assertEqual(
                    shadow_main(
                        [
                            "--strategy",
                            "ignored.json",
                            "--output-root",
                            directory,
                        ]
                    ),
                    1,
                )


class TargetRecordArtifactTests(unittest.TestCase):
    class _Adapter:
        def __init__(self, catalog):
            self.venue, self.catalog = catalog.venue, catalog

        def discover(self, _client, *, now):
            return self.catalog

    def _run(self, *catalogs, directory):
        return run_shadow(
            strategy=load_strategy(STRATEGY_PATH),
            output_root=Path(directory) / "out",
            cache_root=Path(directory) / "cache",
            now=NOW,
            adapters=tuple(self._Adapter(item) for item in catalogs),
            client=object(),
            artifact_format="ndjson",
        )

    @staticmethod
    def _rows(run_directory: Path, venue: str) -> list[dict]:
        path = run_directory / f"target_records_{venue}.ndjson"
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def test_the_venue_record_survives_the_run_verbatim(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self._run(
                snapshot("kalshi", "k", "km"),
                snapshot("polymarket", "p", "pm"),
                CatalogSnapshot("limitless", (), ()),
                directory=directory,
            )
            rows = self._rows(result.directory, "polymarket")
            self.assertEqual(len(rows), 1)
            record = rows[0]["record"]
            # The fields the reader consumes.
            self.assertIn("clobTokenIds", record)
            self.assertIn("endDate", record)
            # And the ones it does not. Trimming to the projection would save a
            # few kilobytes and cost every field nobody has thought of yet,
            # which is the mistake this artifact exists to undo.
            self.assertIn("volume24hr", record)
            self.assertIn("liquidity", record)
            self.assertEqual(rows[0]["provenance"], "captured")
            self.assertEqual(rows[0]["run_id"], result.directory.name)

    def test_only_subscribed_markets_get_a_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self._run(
                snapshot("kalshi", "k", "km"),
                snapshot("polymarket", "p", "pm"),
                CatalogSnapshot("limitless", (), ()),
                directory=directory,
            )
            published = {
                target["target_id"]
                for targets in result.selection.targets.values()
                for target in targets
            }
            recorded = {
                row["target_id"]
                for venue in SUPPORTED_VENUES
                for row in self._rows(result.directory, venue)
            }
            self.assertEqual(recorded, published)

    def test_a_venue_that_subscribed_nothing_still_writes_its_artifact(self) -> None:
        # An empty artifact is positive evidence that nothing was subscribed. A
        # missing file is indistinguishable from a writer that died, which is
        # the same distinction `every_segment_is_sealed` draws for the tape.
        with tempfile.TemporaryDirectory() as directory:
            result = self._run(
                snapshot("kalshi", "k", "km"),
                snapshot("polymarket", "p", "pm"),
                CatalogSnapshot("limitless", (), ()),
                directory=directory,
            )
            path = result.directory / "target_records_limitless.ndjson"
            self.assertTrue(path.exists())
            self.assertEqual(self._rows(result.directory, "limitless"), [])

    def test_a_venue_the_reader_cannot_read_gets_no_projection(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = self._run(
                snapshot("kalshi", "k", "km"),
                snapshot("polymarket", "p", "pm"),
                CatalogSnapshot("limitless", (), ()),
                directory=directory,
            )
            kalshi = self._rows(result.directory, "kalshi")[0]
            self.assertIsNone(kalshi["projection_id"])
            self.assertIsNone(kalshi["projection_sha256"])
            polymarket = self._rows(result.directory, "polymarket")[0]
            self.assertEqual(polymarket["projection_id"], "polymarket.v1")
            self.assertIsNotNone(polymarket["projection_sha256"])

    def test_a_subscribed_market_with_no_record_is_reported_not_skipped(self) -> None:
        # A subscribed asset whose record never arrived is exactly what makes a
        # leg unanalysable, so it must not be discoverable only by its absence.
        with tempfile.TemporaryDirectory() as directory:
            stripped = snapshot("polymarket", "p", "pm")
            stripped = CatalogSnapshot(
                "polymarket",
                stripped.events,
                tuple(
                    replace(market, raw={}) for market in stripped.markets
                ),
            )
            result = self._run(
                snapshot("kalshi", "k", "km"),
                stripped,
                CatalogSnapshot("limitless", (), ()),
                directory=directory,
            )
            self.assertEqual(self._rows(result.directory, "polymarket"), [])
            report = read_run_report(result.directory)
            self.assertEqual(
                report["target_record_diagnostics"]["polymarket"],
                ["polymarket:pm: catalogue market carries no raw record"],
            )


if __name__ == "__main__":
    unittest.main()
