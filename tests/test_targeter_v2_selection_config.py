"""Selection config V1: scope, market caps and ranking (docs/specs/TARGETER_SELECTION_CONFIG_V1.md).

Offline, small hand-authored shapes. Esports fixtures carry only the fields the
selection pipeline reads.
"""

from __future__ import annotations

import itertools
import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from archive.storage import INDEPENDENT, LocalObjectStore
from targeter.targets import load_targets
from targeter.v2 import registry
from targeter.v2.domain import (
    ActivationEvidence,
    CanonicalEvent,
    CanonicalMarket,
    CatalogSnapshot,
    ClassificationEvidence,
)
from targeter.v2.publication import audit_current_publication, publish_run
from targeter.v2.registry import StrategyError, load_strategy
from targeter.v2.run import run_shadow
from targeter.v2.run_archive import archive_run
from targeter.v2.selection import _capped_markets, select_targets
from tests.test_targeter_v2 import snapshot as soccer_snapshot

ROOT = Path(__file__).resolve().parents[1]
STRATEGY_PATH = ROOT / "configs" / "targeter_v2.json"
EXAMPLE_PATH = ROOT / "configs" / "examples" / "targeter_v2_cs2_single_event.json"
SCHEMA_PATH = ROOT / "configs" / "targeter_v2.schema.json"
NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
START = NOW + timedelta(minutes=50)

CS2 = "counter_strike_2"
DOTA = "dota_2"
ML = "esports.series_moneyline"
MAP = "esports.map_winner"


def esports(
    tag: str,
    *,
    game: str = CS2,
    start: datetime = START,
    ml_volume: float | None = 30_000,
    maps: tuple[int, ...] = (),
    venues: tuple[str, ...] = ("polymarket", "limitless"),
    best_of: str = "3",
) -> dict[str, tuple[CanonicalEvent, list[CanonicalMarket]]]:
    """One bundle's worth of esports events and markets, per venue."""
    names = (f"Alpha {tag}", f"Beta {tag}")
    out = {}
    for venue in venues:
        event_id = f"{tag}-{venue}"
        event = CanonicalEvent(
            venue, event_id, "esports", None, f"{names[0]} vs {names[1]} (BO{best_of})",
            names, start, "open", f"/{event_id}", best_of, "group", game, "best_of_series",
            (ClassificationEvidence(f"{game}:{venue}:game:t", "t", game),),
            (ActivationEvidence(start, "structured", "start", True),),
        )
        specs = [("series_moneyline", ML, "series", {"side": "home"}, ml_volume)] + [
            ("map_winner", MAP, "map", {"map_index": index}, None) for index in maps
        ]
        markets = []
        for market_type, canonical_class, scope, parameters, volume in specs:
            market_id = f"{event_id}-{market_type}-{parameters.get('map_index', '')}"
            markets.append(
                CanonicalMarket(
                    venue, market_id, event_id, canonical_class, market_type, scope, "t",
                    parameters, (market_id + "-a", market_id + "-b"), names, "open", True,
                    created_at=NOW - timedelta(days=1), volume_total_usd=volume,
                    volume_24h=100, source_ref=f"/{market_id}",
                    classification_evidence=ClassificationEvidence(
                        f"{game}:{venue}:{canonical_class}:t", "t", canonical_class
                    ),
                )
            )
        out[venue] = (event, markets)
    return out


def catalogs(*groups: dict[str, Any]) -> tuple[CatalogSnapshot, ...]:
    events: dict[str, list] = {}
    markets: dict[str, list] = {}
    for group in groups:
        for venue, (event, venue_markets) in group.items():
            events.setdefault(venue, []).append(event)
            markets.setdefault(venue, []).extend(venue_markets)
    return tuple(
        CatalogSnapshot(venue, tuple(events[venue]), tuple(markets[venue]))
        for venue in sorted(events)
    )


def strategy_from(
    mutate: Callable[[dict[str, Any]], None] | None = None,
    *,
    path: Path = STRATEGY_PATH,
) -> registry.Strategy:
    document = json.loads(path.read_text(encoding="utf-8"))
    if mutate:
        mutate(document)
    with tempfile.TemporaryDirectory() as directory:
        target = Path(directory) / "strategy.json"
        target.write_text(json.dumps(document), encoding="utf-8")
        return load_strategy(target)


def v1(
    *,
    scope: dict | None = None,
    ranking: dict | None = None,
    per_bundle: int | None = None,
    total: int | None = None,
    maximum_bundles: int | None = None,
) -> registry.Strategy:
    def mutate(document: dict[str, Any]) -> None:
        if scope is not None:
            document["scope"] = scope
        if ranking is not None:
            document["ranking"] = ranking
        if per_bundle is not None:
            document["selection"]["maximum_markets_per_bundle"] = per_bundle
        if total is not None:
            document["selection"]["maximum_markets"] = total
        if maximum_bundles is not None:
            document["selection"]["maximum_bundles"] = maximum_bundles

    return strategy_from(mutate)


def keys(*pairs: tuple[str, str]) -> dict[str, Any]:
    return {"keys": [{"key": key, "order": order} for key, order in pairs]}


def candidate_for(result, tag: str):
    return next(item for item in result.candidates if f"Alpha {tag}" in item.bundle.participants)


class LoaderTests(unittest.TestCase):
    def assertRejected(self, mutate: Callable[[dict[str, Any]], None], fragment: str = "") -> None:
        with self.assertRaises(StrategyError) as caught:
            strategy_from(mutate)
        self.assertIn(fragment, str(caught.exception))

    def test_every_field_is_accepted(self) -> None:
        strategy = strategy_from(
            lambda d: d.update(
                scope={"sports": ["esports"], "games": [CS2], "market_classes": [ML, MAP]},
                ranking={
                    **keys(("moneyline_volume_usd", "desc"), ("activation_time", "asc")),
                    "priorities": {
                        "default": 0,
                        "game": {CS2: 10},
                        "market_class": {ML: 5},
                        "venue_set": {"kalshi+polymarket": 2, "kalshi+limitless+polymarket": 3},
                    },
                },
            )
            or d["selection"].update(maximum_markets_per_bundle=6, maximum_markets=12)
        )
        self.assertEqual(strategy.scope.sports, frozenset({"esports"}))
        self.assertEqual(strategy.scope.games, frozenset({CS2}))
        self.assertEqual(strategy.scope.market_classes, frozenset({ML, MAP}))
        self.assertEqual((strategy.maximum_markets_per_bundle, strategy.maximum_markets), (6, 12))
        self.assertEqual(strategy.ranking.priorities.venue_set["kalshi+polymarket"], 2)
        self.assertTrue(strategy.uses_selection_config)

    def test_shipped_strategy_configures_nothing(self) -> None:
        strategy = load_strategy(STRATEGY_PATH)
        self.assertIsNone(strategy.scope)
        self.assertIsNone(strategy.ranking)
        self.assertIsNone(strategy.maximum_markets_per_bundle)
        self.assertIsNone(strategy.maximum_markets)
        self.assertFalse(strategy.uses_selection_config)

    def test_scope_rejections(self) -> None:
        cases = {
            "not an object": [],
            "empty": {},
            "unknown key": {"sports": ["esports"], "leagues": ["x"]},
            "empty sports": {"sports": []},
            "sports not a list": {"sports": "esports"},
            "sports non-string": {"sports": [1]},
            "sports duplicate": {"sports": ["esports", "esports"]},
            "sport not in top-level sports": {"sports": ["tennis"]},
            "unknown game": {"games": ["chess"]},
            "duplicate game": {"games": [CS2, CS2]},
            "unknown class": {"market_classes": ["esports.nope"]},
            "duplicate class": {"market_classes": [ML, ML]},
            "empty classes": {"market_classes": []},
        }
        for name, scope in cases.items():
            with self.subTest(name):
                self.assertRejected(lambda d, s=scope: d.update(scope=s), "scope")

    def test_cap_rejections(self) -> None:
        for key in ("maximum_markets_per_bundle", "maximum_markets"):
            for value in (0, -1, 1.5, "3", True, None):
                with self.subTest(key=key, value=value):
                    self.assertRejected(
                        lambda d, k=key, v=value: d["selection"].update({k: v}),
                        f"{key} must be a positive integer",
                    )

    def test_ranking_rejections(self) -> None:
        good = keys(("activity", "desc"))
        cases = {
            "not an object": [],
            "unknown key": {**good, "extra": 1},
            "missing keys": {},
            "empty keys": {"keys": []},
            "keys not a list": {"keys": {"key": "activity", "order": "asc"}},
            "item not an object": {"keys": ["activity"]},
            "item unknown field": {"keys": [{"key": "activity", "order": "asc", "x": 1}]},
            "unknown ranking key": keys(("popularity", "asc")),
            "missing order": {"keys": [{"key": "activity"}]},
            "bad order": keys(("activity", "up")),
            "duplicate key": keys(("activity", "asc"), ("activity", "desc")),
            "priority key without priorities": keys(("game_priority", "desc")),
            "priorities not an object": {**good, "priorities": []},
            "priorities unknown field": {**good, "priorities": {"default": 0, "x": {}}},
            "priorities without default": {**good, "priorities": {"game": {}}},
            "default not an integer": {**good, "priorities": {"default": 1.5}},
            "default is a bool": {**good, "priorities": {"default": True}},
            "unknown game": {**good, "priorities": {"default": 0, "game": {"chess": 1}}},
            "unknown class": {**good, "priorities": {"default": 0, "market_class": {"x.y": 1}}},
            "non-integer value": {**good, "priorities": {"default": 0, "game": {CS2: "1"}}},
            "group not an object": {**good, "priorities": {"default": 0, "venue_set": []}},
        }
        for venue_set in (
            "kalshi",
            "polymarket+kalshi",
            "kalshi+kalshi",
            "kalshi+venue",
            "kalshi+polymarket+",
            "kalshi,polymarket",
            "kalshi+limitless+polymarket+kalshi",
        ):
            cases[f"bad venue set {venue_set!r}"] = {
                **good, "priorities": {"default": 0, "venue_set": {venue_set: 1}}
            }
        for name, ranking in cases.items():
            with self.subTest(name):
                self.assertRejected(lambda d, r=ranking: d.update(ranking=r), "ranking")

    def test_priority_values_may_be_negative_or_zero(self) -> None:
        strategy = strategy_from(
            lambda d: d.update(
                ranking={**keys(("game_priority", "desc")), "priorities": {"default": -1, "game": {CS2: 0}}}
            )
        )
        self.assertEqual(strategy.ranking.priorities.default, -1)

    def test_existing_validation_order_is_unchanged(self) -> None:
        # An unknown top-level field is still reported before any V1 problem.
        with self.assertRaises(StrategyError) as caught:
            strategy_from(lambda d: d.update(bogus=1, scope=[], ranking=[]))
        self.assertEqual(str(caught.exception), "unknown strategy fields: bogus")
        with self.assertRaises(StrategyError) as caught:
            strategy_from(lambda d: d["selection"].update(bogus=1))
        self.assertEqual(str(caught.exception), "unknown selection fields: bogus")
        # Existing value errors still win over a malformed V1 field.
        def broken(d: dict[str, Any]) -> None:
            d["version"] = 0
            d["scope"] = []

        with self.assertRaises(StrategyError) as caught:
            strategy_from(broken)
        self.assertEqual(str(caught.exception), "version must be a positive integer")


class CompatibilityTests(unittest.TestCase):
    def fixture_catalogs(self):
        return catalogs(esports("a", maps=(1, 2)), esports("b", game=DOTA)) + (
            soccer_snapshot("kalshi", "k", "km"),
            soccer_snapshot("polymarket", "p", "pm"),
        )

    def merged(self):
        by_venue: dict[str, list[CatalogSnapshot]] = {}
        for snap in self.fixture_catalogs():
            by_venue.setdefault(snap.venue, []).append(snap)
        return tuple(
            CatalogSnapshot(
                venue,
                tuple(e for s in parts for e in s.events),
                tuple(m for s in parts for m in s.markets),
            )
            for venue, parts in sorted(by_venue.items())
        )

    def test_unconfigured_report_has_no_new_keys(self) -> None:
        result = select_targets(self.merged(), strategy=load_strategy(STRATEGY_PATH), now=NOW)
        self.assertGreaterEqual(len(result.selected), 3)
        record = result.as_record()
        self.assertEqual(record["report_version"], 3)
        self.assertEqual(
            set(record["selection"]),
            {
                "bundle_ids", "bundle_count", "budget_used", "targets",
                "allocation_rejections", "publication_performed",
            },
        )
        for candidate in record["candidates"]:
            self.assertNotIn("rank_values", candidate)
        self.assertNotIn("configuration", json.dumps(record["selection"]))
        self.assertNotIn("rank_values", json.dumps(record))

    def test_default_order_spelled_out_selects_exactly_like_no_ranking(self) -> None:
        base = load_strategy(STRATEGY_PATH)
        explicit = v1(
            ranking=keys(
                ("venue_coverage", "desc"),
                ("cross_venue_relationships", "desc"),
                ("market_class_breadth", "desc"),
                ("activity", "desc"),
                ("activation_time", "asc"),
            )
        )
        plain = select_targets(self.merged(), strategy=base, now=NOW)
        spelled = select_targets(self.merged(), strategy=explicit, now=NOW)
        self.assertEqual(
            [item.bundle.bundle_id for item in plain.selected],
            [item.bundle.bundle_id for item in spelled.selected],
        )
        self.assertEqual(plain.as_record()["selection"]["targets"], spelled.as_record()["selection"]["targets"])


class ScopeTests(unittest.TestCase):
    def test_sport_outside_scope_rejects_the_bundle_visibly(self) -> None:
        result = select_targets(
            catalogs(esports("a")) + (soccer_snapshot("kalshi", "k", "km"), soccer_snapshot("polymarket", "p", "pm")),
            strategy=v1(scope={"sports": ["esports"]}),
            now=NOW,
        )
        soccer = next(item for item in result.candidates if item.bundle.sport == "soccer")
        self.assertFalse(soccer.eligible)
        self.assertEqual(soccer.rejection_reasons, ("sport_outside_scope",))
        self.assertEqual(soccer.as_record()["event_status"], "REJECTED")
        self.assertEqual([item.bundle.sport for item in result.selected], ["esports"])

    def test_reason_is_appended_after_existing_reasons(self) -> None:
        result = select_targets(
            catalogs(esports("a", ml_volume=1_000)),
            strategy=v1(scope={"sports": ["soccer"]}),
            now=NOW,
        )
        self.assertEqual(
            result.candidates[0].rejection_reasons,
            ("combined_moneyline_volume_usd_below_minimum", "sport_outside_scope"),
        )

    def test_game_outside_scope_applies_to_esports_only(self) -> None:
        result = select_targets(
            catalogs(esports("a", game=CS2), esports("b", game=DOTA))
            + (soccer_snapshot("kalshi", "k", "km"), soccer_snapshot("polymarket", "p", "pm")),
            strategy=v1(scope={"games": [CS2]}),
            now=NOW,
        )
        by_sport_game = {(c.bundle.sport, c.bundle.game): c for c in result.candidates}
        self.assertTrue(by_sport_game[("esports", CS2)].eligible)
        self.assertEqual(by_sport_game[("esports", DOTA)].rejection_reasons, ("game_outside_scope",))
        self.assertTrue(by_sport_game[("soccer", None)].eligible)

    def test_class_scope_excludes_markets_visibly_and_keeps_the_volume_gate(self) -> None:
        group = esports("a", maps=(1,), ml_volume=30_000)
        result = select_targets(
            catalogs(group), strategy=v1(scope={"market_classes": [MAP]}), now=NOW
        )
        candidate = result.candidates[0]
        # Moneylines are out of scope, but they still carry the volume gate.
        self.assertTrue(candidate.eligible, candidate.rejection_reasons)
        self.assertEqual(candidate.combined_moneyline_volume_usd, 60_000)
        self.assertEqual(
            sorted(market.market_type for market in candidate.eligible_markets),
            ["map_winner", "map_winner"],
        )
        exclusions = dict(candidate.market_exclusions)
        for venue in ("limitless", "polymarket"):
            self.assertEqual(
                exclusions[f"{venue}:a-{venue}-series_moneyline-"], ("market_class_outside_scope",)
            )
        self.assertIn(
            "market_class_outside_scope",
            json.dumps(candidate.as_record()["market_exclusions"]),
        )

    def test_class_scope_does_not_lower_a_failing_volume_gate(self) -> None:
        result = select_targets(
            catalogs(esports("a", maps=(1,), ml_volume=10_000)),
            strategy=v1(scope={"market_classes": [MAP]}),
            now=NOW,
        )
        candidate = result.candidates[0]
        self.assertFalse(candidate.eligible)
        self.assertEqual(candidate.combined_moneyline_volume_usd, 20_000)
        self.assertEqual(candidate.rejection_reasons, ("combined_moneyline_volume_usd_below_minimum",))

    def test_only_in_scope_markets_form_relationships(self) -> None:
        result = select_targets(
            catalogs(esports("a", maps=(1, 2))),
            strategy=v1(scope={"market_classes": [ML]}),
            now=NOW,
        )
        candidate = result.candidates[0]
        self.assertTrue(candidate.eligible)
        self.assertEqual({m.market_type for m in candidate.eligible_markets}, {"series_moneyline"})
        for relationship in candidate.relationships.relationships:
            self.assertNotIn("map_winner", relationship.left + relationship.right)

    def test_scope_leaving_no_cross_venue_pair_rejects_the_event(self) -> None:
        result = select_targets(
            catalogs(esports("a", maps=(1,), venues=("polymarket", "limitless", "kalshi"))),
            strategy=v1(scope={"market_classes": [MAP]}),
            now=NOW,
        )
        self.assertTrue(result.candidates[0].eligible)
        # Same event, but a scope naming a class none of its markets has.
        none = select_targets(
            catalogs(esports("a")), strategy=v1(scope={"market_classes": [MAP]}), now=NOW
        )
        self.assertFalse(none.candidates[0].eligible)
        self.assertIn("no_cross_venue_structural_relationship", none.candidates[0].rejection_reasons)


class PerBundleCapTests(unittest.TestCase):
    def six(self, **kwargs):
        return catalogs(esports("a", maps=(1, 2), **kwargs))

    def test_anchors_are_kept_first(self) -> None:
        result = select_targets(self.six(), strategy=v1(per_bundle=2), now=NOW)
        candidate = result.candidates[0]
        self.assertTrue(candidate.eligible)
        self.assertEqual(
            sorted(market.market_type for market in candidate.eligible_markets),
            ["series_moneyline", "series_moneyline"],
        )
        dropped = [t for t, why in candidate.market_exclusions.items() if "market_cap_per_bundle" in why]
        self.assertEqual(len(dropped), 4)
        record = candidate.as_record()
        self.assertEqual(
            record["eligible_market_ids"],
            [m.target_id for m in candidate.eligible_markets],
        )
        self.assertEqual(len(record["eligible_market_ids"]), 2)
        # Selected targets equal the candidate's kept markets.
        targets = {t["target_id"] for items in result.targets.values() for t in items}
        self.assertEqual(targets, set(record["eligible_market_ids"]))

    def test_anchor_beats_class_priority(self) -> None:
        strategy = v1(
            per_bundle=2,
            ranking={**keys(("activity", "desc")), "priorities": {"default": 0, "market_class": {MAP: 100}}},
        )
        candidate = select_targets(self.six(), strategy=strategy, now=NOW).candidates[0]
        self.assertEqual({m.market_type for m in candidate.eligible_markets}, {"series_moneyline"})

    def test_pruning_drops_a_market_left_without_a_partner(self) -> None:
        # Cap 3 keeps both moneylines plus the lexically first map market, which
        # has no kept cross-venue partner and is pruned.
        candidate = select_targets(self.six(), strategy=v1(per_bundle=3), now=NOW).candidates[0]
        self.assertEqual(
            sorted(m.target_id for m in candidate.eligible_markets),
            ["limitless:a-limitless-series_moneyline-", "polymarket:a-polymarket-series_moneyline-"],
        )
        self.assertEqual(
            candidate.market_exclusions["limitless:a-limitless-map_winner-1"],
            ("market_cap_per_bundle",),
        )

    def test_cap_that_leaves_one_venue_rejects_the_event(self) -> None:
        candidate = select_targets(self.six(), strategy=v1(per_bundle=1), now=NOW).candidates[0]
        self.assertFalse(candidate.eligible)
        self.assertEqual(
            candidate.rejection_reasons,
            ("fewer_than_minimum_eligible_venues", "no_cross_venue_structural_relationship"),
        )
        self.assertEqual(candidate.eligible_markets, ())

    def test_cap_at_or_above_the_market_count_changes_nothing(self) -> None:
        plain = select_targets(self.six(), strategy=load_strategy(STRATEGY_PATH), now=NOW).candidates[0]
        capped = select_targets(self.six(), strategy=v1(per_bundle=6), now=NOW).candidates[0]
        self.assertEqual(
            {k: v for k, v in plain.as_record().items()},
            {k: v for k, v in capped.as_record().items() if k != "rank_values"},
        )

    def test_gates_are_evaluated_on_the_kept_set_not_the_volume_set(self) -> None:
        # The cap never lowers the volume gate: moneyline volume is measured first.
        candidate = select_targets(self.six(), strategy=v1(per_bundle=2), now=NOW).candidates[0]
        self.assertEqual(candidate.combined_moneyline_volume_usd, 60_000)

    def test_ordering_unit(self) -> None:
        def market(name: str, canonical_class: str, market_type: str, volume: float):
            return SimpleNamespace(
                target_id=name,
                canonical_class=canonical_class,
                market_type=market_type,
                volume_24h=volume,
                volume_total=None,
                liquidity=None,
            )

        def pair(left: str, right: str):
            return SimpleNamespace(left=left + "#claim=0", right=right + "#claim=1")

        markets = (
            market("anchor-a", ML, "series_moneyline", 1),
            market("anchor-b", ML, "series_moneyline", 1),
            market("low-hot", "esports.map_handicap", "map_handicap", 9_000),
            market("high-cold", MAP, "map_winner", 1),
            market("high-cold-2", MAP, "map_winner", 1),
        )
        everyone = [pair(a.target_id, b.target_id) for a, b in itertools.combinations(markets, 2)]
        ranking = {**keys(("activity", "desc")), "priorities": {"default": 0, "market_class": {MAP: 7}}}
        with_priority = v1(ranking=ranking)
        self.assertEqual(
            _capped_markets(markets, everyone, 3, with_priority),
            {"anchor-a", "anchor-b", "high-cold"},  # class priority, then target_id
        )
        # Without priorities the hotter market wins (activity descending).
        self.assertEqual(
            _capped_markets(markets, everyone, 3, load_strategy(STRATEGY_PATH)),
            {"anchor-a", "anchor-b", "low-hot"},
        )
        # Pruning: a kept market without a partner among the kept set is dropped.
        sparse = [pair("anchor-a", "anchor-b"), pair("anchor-a", "low-hot")]
        self.assertEqual(
            _capped_markets(markets, sparse, 3, load_strategy(STRATEGY_PATH)),
            {"anchor-a", "anchor-b", "low-hot"},
        )
        self.assertEqual(
            _capped_markets(markets, [pair("anchor-a", "anchor-b")], 3, load_strategy(STRATEGY_PATH)),
            {"anchor-a", "anchor-b"},
        )


class TotalCapTests(unittest.TestCase):
    def groups(self):
        # "big" has more relationships, so it ranks first by default.
        return catalogs(esports("big", maps=(1, 2)), esports("small"))

    def test_a_bundle_over_the_budget_is_skipped_and_a_smaller_one_still_fits(self) -> None:
        result = select_targets(self.groups(), strategy=v1(total=4), now=NOW)
        self.assertEqual([c.bundle.participants[0] for c in result.selected], ["Alpha small"])
        big = next(c for c in result.candidates if c.bundle.participants[0] == "Alpha big")
        self.assertTrue(big.eligible)
        self.assertEqual(
            result.allocation_rejections, {big.bundle.bundle_id: "maximum_markets_reached"}
        )
        self.assertEqual(sum(len(items) for items in result.targets.values()), 2)

    def test_markets_already_allocated_count_towards_the_budget(self) -> None:
        result = select_targets(self.groups(), strategy=v1(total=7), now=NOW)
        self.assertEqual([c.bundle.participants[0] for c in result.selected], ["Alpha big"])
        small = next(c for c in result.candidates if c.bundle.participants[0] == "Alpha small")
        self.assertEqual(
            result.allocation_rejections, {small.bundle.bundle_id: "maximum_markets_reached"}
        )

    def test_a_budget_that_holds_everything_changes_nothing(self) -> None:
        plain = select_targets(self.groups(), strategy=load_strategy(STRATEGY_PATH), now=NOW)
        roomy = select_targets(self.groups(), strategy=v1(total=8), now=NOW)
        self.assertEqual(
            [c.bundle.bundle_id for c in plain.selected], [c.bundle.bundle_id for c in roomy.selected]
        )
        self.assertEqual(roomy.allocation_rejections, {})

    def test_the_cap_does_not_trim_a_bundle_it_skips_whole(self) -> None:
        result = select_targets(self.groups(), strategy=v1(total=1), now=NOW)
        self.assertEqual(result.selected, ())
        self.assertEqual(sum(len(items) for items in result.targets.values()), 0)


class RankingTests(unittest.TestCase):
    PRIORITIES = {
        "default": 0,
        "game": {CS2: 10, DOTA: 5},
        "market_class": {ML: 1, MAP: 4},
        "venue_set": {"limitless+polymarket": 2, "kalshi+limitless+polymarket": 3},
    }

    def groups(self):
        return catalogs(
            esports("a", game=CS2, maps=(1,), ml_volume=30_000, start=START),
            esports("b", game=DOTA, ml_volume=20_000, start=START + timedelta(minutes=10)),
            esports(
                "c", game=CS2, maps=(1, 2), ml_volume=35_000, start=START + timedelta(minutes=5),
                venues=("polymarket", "limitless", "kalshi"),
            ),
        )

    def run_with(self, *pairs: tuple[str, str]):
        strategy = v1(ranking={**keys(*pairs), "priorities": self.PRIORITIES})
        return select_targets(self.groups(), strategy=strategy, now=NOW)

    def test_rank_values_are_reported_per_configured_key(self) -> None:
        names = (
            "venue_coverage", "cross_venue_relationships", "market_class_breadth", "activity",
            "moneyline_volume_usd", "activation_time", "game_priority",
            "market_class_priority_max", "market_class_priority_sum", "venue_set_priority",
        )
        result = self.run_with(*[(name, "desc") for name in names])
        values = {
            c.bundle.participants[0]: c.as_record()["rank_values"] for c in result.candidates
        }
        self.assertEqual(set(values["Alpha a"]), set(names))
        self.assertEqual(values["Alpha a"]["moneyline_volume_usd"], 60_000)
        self.assertEqual(values["Alpha b"]["moneyline_volume_usd"], 40_000)
        self.assertEqual(values["Alpha c"]["moneyline_volume_usd"], 105_000)
        self.assertEqual(values["Alpha a"]["venue_coverage"], 2_000)
        self.assertEqual(values["Alpha c"]["venue_coverage"], 3_000)
        self.assertEqual(values["Alpha a"]["activation_time"], START.isoformat().replace("+00:00", "Z"))
        self.assertEqual(
            [values[k]["game_priority"] for k in ("Alpha a", "Alpha b", "Alpha c")], [10, 5, 10]
        )
        self.assertEqual(
            [values[k]["market_class_priority_max"] for k in ("Alpha a", "Alpha b", "Alpha c")],
            [4, 1, 4],
        )
        self.assertEqual(
            [values[k]["market_class_priority_sum"] for k in ("Alpha a", "Alpha b", "Alpha c")],
            [5, 1, 5],
        )
        self.assertEqual(
            [values[k]["venue_set_priority"] for k in ("Alpha a", "Alpha b", "Alpha c")], [2, 2, 3]
        )

    def test_every_key_orders_both_ways_with_an_implicit_bundle_id_tie_break(self) -> None:
        names = (
            "venue_coverage", "cross_venue_relationships", "market_class_breadth", "activity",
            "moneyline_volume_usd", "activation_time", "game_priority",
            "market_class_priority_max", "market_class_priority_sum", "venue_set_priority",
        )
        for name in names:
            for order in ("asc", "desc"):
                result = self.run_with((name, order))
                reported = {
                    c.bundle.bundle_id: c.as_record()["rank_values"][name] for c in result.candidates
                }
                sign = 1 if order == "asc" else -1
                expected = sorted(
                    reported,
                    key=lambda bundle_id: (
                        sign * (
                            datetime.fromisoformat(reported[bundle_id].replace("Z", "+00:00")).timestamp()
                            if name == "activation_time"
                            else reported[bundle_id]
                        ),
                        bundle_id,
                    ),
                )
                got = [c.bundle.bundle_id for c in result.selected]
                self.assertEqual(got, expected, f"{name} {order}")
        # A key with distinct values reverses; a tied key does not (bundle_id decides).
        self.assertNotEqual(
            [c.bundle.bundle_id for c in self.run_with(("moneyline_volume_usd", "asc")).selected],
            [c.bundle.bundle_id for c in self.run_with(("moneyline_volume_usd", "desc")).selected],
        )

    def test_later_keys_break_ties_of_earlier_ones(self) -> None:
        # a and c tie on game_priority (10); activation_time asc puts a first, desc puts c first.
        names = lambda order: [  # noqa: E731
            c.bundle.participants[0]
            for c in self.run_with(("game_priority", "desc"), ("activation_time", order)).selected
        ]
        self.assertEqual(names("asc"), ["Alpha a", "Alpha c", "Alpha b"])
        self.assertEqual(names("desc"), ["Alpha c", "Alpha a", "Alpha b"])

    def test_five_events_select_a_different_single_event_under_volume_first(self) -> None:
        # Volume gate is satisfied by all; the richest bundle is not the most liquid one.
        group = lambda: catalogs(  # noqa: E731
            esports("e1", maps=(1, 2, 3), ml_volume=15_000),
            esports("e2", maps=(1, 2), ml_volume=20_000),
            esports("e3", maps=(1,), ml_volume=40_000),
            esports("e4", ml_volume=50_000),
            esports("e5", ml_volume=45_000, maps=(1,)),
        )
        default = select_targets(
            group(), strategy=v1(maximum_bundles=1), now=NOW
        )
        volume_first = select_targets(
            group(),
            strategy=v1(maximum_bundles=1, ranking=keys(("moneyline_volume_usd", "desc"))),
            now=NOW,
        )
        self.assertEqual([c.bundle.participants[0] for c in default.selected], ["Alpha e1"])
        self.assertEqual([c.bundle.participants[0] for c in volume_first.selected], ["Alpha e4"])
        self.assertEqual(
            sum(1 for c in volume_first.candidates if c.eligible), 5
        )
        # Everything else was displaced only by the bundle limit.
        self.assertEqual(
            set(volume_first.allocation_rejections.values()), {"maximum_bundles_reached"}
        )

    def test_shipped_example_selects_the_most_liquid_cs2_event(self) -> None:
        strategy = load_strategy(EXAMPLE_PATH)
        groups = catalogs(
            esports("e1", maps=(1, 2), ml_volume=20_000),
            esports("e2", ml_volume=60_000),
            esports("e3", game=DOTA, ml_volume=90_000),
            esports("e4", maps=(1,), ml_volume=40_000),
        )
        result = select_targets(groups, strategy=strategy, now=NOW)
        self.assertEqual([c.bundle.participants[0] for c in result.selected], ["Alpha e2"])
        dota = next(c for c in result.candidates if c.bundle.game == DOTA)
        self.assertEqual(dota.rejection_reasons, ("game_outside_scope",))


class ReportTests(unittest.TestCase):
    def test_configuration_and_rank_values_appear_only_with_v1_fields(self) -> None:
        groups = catalogs(esports("a", maps=(1,)), esports("low", ml_volume=1_000))
        plain = select_targets(groups, strategy=load_strategy(STRATEGY_PATH), now=NOW).as_record()
        self.assertNotIn("configuration", plain["selection"])
        self.assertTrue(all("rank_values" not in c for c in plain["candidates"]))

        record = select_targets(groups, strategy=v1(per_bundle=2), now=NOW).as_record()
        self.assertEqual(record["report_version"], 3)
        configuration = record["selection"]["configuration"]
        self.assertEqual(configuration["scope"], None)
        self.assertEqual(configuration["maximum_markets_per_bundle"], 2)
        self.assertIsNone(configuration["maximum_markets"])
        self.assertEqual(
            configuration["ranking"]["keys"][-1], {"key": "bundle_id", "order": "asc"}
        )
        self.assertEqual(
            [item["key"] for item in configuration["ranking"]["keys"]],
            [
                "venue_coverage", "cross_venue_relationships", "market_class_breadth",
                "activity", "activation_time", "bundle_id",
            ],
        )
        self.assertIsNone(configuration["ranking"]["priorities"])
        by_name = {c["participants"][0]: c for c in record["candidates"]}
        self.assertIn("rank_values", by_name["Alpha a"])
        self.assertNotIn("rank_values", by_name["Alpha low"])  # ineligible
        json.dumps(record)  # serializable

    def test_configuration_echoes_the_resolved_strategy(self) -> None:
        strategy = load_strategy(EXAMPLE_PATH)
        configuration = select_targets((), strategy=strategy, now=NOW).as_record()["selection"][
            "configuration"
        ]
        self.assertEqual(configuration["scope"], {"sports": ["esports"], "games": [CS2], "market_classes": None})
        self.assertEqual(configuration["maximum_markets"], 6)
        self.assertEqual(configuration["ranking"]["keys"][0], {"key": "moneyline_volume_usd", "order": "desc"})
        self.assertEqual(configuration["ranking"]["priorities"]["game"], {CS2: 10})


class PublicationAcceptsCappedSelectionTests(unittest.TestCase):
    def test_capped_selection_publishes_and_audits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy = v1(per_bundle=2, total=2, scope={"games": [CS2]})
            groups = catalogs(esports("a", maps=(1, 2)))

            class Adapter:
                def __init__(self, catalog: CatalogSnapshot) -> None:
                    self.venue = catalog.venue
                    self.catalog = catalog

                def discover(self, _client, *, now):
                    return self.catalog

                def probe_terminal(self, _client, targets):
                    from targeter.v2.continuity import TerminalProbe

                    return {key: TerminalProbe("open", "test_open") for key in targets}

            adapters = [Adapter(c) for c in groups] + [Adapter(CatalogSnapshot("kalshi", (), ()))]
            outcome = run_shadow(
                strategy=strategy,
                output_root=root / "runs",
                cache_root=root / "cache",
                live_root=root / "live",
                now=NOW,
                adapters=adapters,
                client=object(),
                artifact_format="zstd",
            )
            report_name = next(
                p for p in outcome.directory.iterdir() if p.name.startswith("selection_report")
            )
            self.assertTrue(report_name.exists())
            store = LocalObjectStore(root / "store", store_id="selection-config", durability=INDEPENDENT)
            receipt = archive_run(outcome.directory, store, now=NOW)
            publish_run(
                outcome.directory, receipt, store, live_root=root / "live", strategy=strategy, now=NOW
            )
            pointer = root / "live" / "targeter-v2" / "current.json"
            self.assertEqual(len(load_targets(pointer, venue="polymarket").asset_ids()), 2)
            self.assertEqual(len(load_targets(pointer, venue="limitless").asset_ids()), 2)
            audited = audit_current_publication(
                live_root=root / "live", output_root=root / "runs", store=store, strategy=strategy
            )
            self.assertEqual(audited.run_id, outcome.directory.name)
            self.assertEqual(audited.venue_counts, {"kalshi": 0, "limitless": 2, "polymarket": 2})


class SchemaSyncTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        cls.defs = cls.schema["$defs"]

    def properties(self, node: dict[str, Any]) -> frozenset[str]:
        self.assertIs(node.get("additionalProperties"), False)
        return frozenset(node["properties"])

    def test_object_property_sets_match_the_loader(self) -> None:
        ranking = self.defs["ranking"]
        pairs = {
            "top level": (self.schema, registry.TOP_LEVEL_KEYS),
            "selection": (self.defs["selection"], registry.SELECTION_KEYS),
            "scope": (self.defs["scope"], registry.SCOPE_KEYS),
            "ranking": (ranking, registry.RANKING_KEYS),
            "ranking.keys item": (ranking["properties"]["keys"]["items"], registry.RANKING_KEY_ITEM_KEYS),
            "priorities": (ranking["properties"]["priorities"], registry.PRIORITIES_KEYS),
            "market class": (self.defs["market_class"], registry.MARKET_CLASS_KEYS),
            "game family": (self.defs["game_family"], registry.GAME_FAMILY_KEYS),
            "product mapping": (self.defs["product_mapping"], registry.PRODUCT_MAPPING_KEYS),
        }
        for name, (node, accepted) in pairs.items():
            with self.subTest(name):
                self.assertEqual(self.properties(node), accepted)

    def test_enumerations_match_the_loader(self) -> None:
        item = self.defs["ranking"]["properties"]["keys"]["items"]["properties"]
        self.assertEqual(tuple(item["key"]["enum"]), registry.RANKING_KEY_NAMES)
        self.assertEqual(item["order"]["enum"], ["asc", "desc"])
        venues = ("kalshi", "limitless", "polymarket")
        combos = {
            "+".join(group)
            for size in (2, 3)
            for group in itertools.combinations(venues, size)
        }
        venue_set = self.defs["ranking"]["properties"]["priorities"]["properties"]["venue_set"]
        self.assertEqual(set(venue_set["propertyNames"]["enum"]), combos)
        self.assertEqual(set(self.defs["venue"]["enum"]), set(venues))
        aliases = self.defs["game_family"]["properties"]["venue_game_aliases"]
        self.assertEqual(frozenset(aliases["properties"]), frozenset(venues))
        self.assertEqual(frozenset(self.defs["game_family"]["properties"]["venue_products"]["required"]), frozenset(venues))

    def test_required_fields_are_required_by_the_loader(self) -> None:
        for field in self.schema["required"]:
            with self.subTest(top=field):
                with self.assertRaises(StrategyError):
                    strategy_from(lambda d, f=field: d.pop(f))
        for field in self.defs["selection"]["required"]:
            with self.subTest(selection=field):
                with self.assertRaises(StrategyError):
                    strategy_from(lambda d, f=field: d["selection"].pop(f))
        for venue in self.defs["game_family"]["properties"]["venue_products"]["required"]:
            with self.subTest(venue_products=venue):
                with self.assertRaises(StrategyError):
                    strategy_from(lambda d, v=venue: d["game_families"][0]["venue_products"].pop(v))

    def test_example_config_loads_and_matches_its_description(self) -> None:
        strategy = load_strategy(EXAMPLE_PATH)
        shipped = load_strategy(STRATEGY_PATH)
        self.assertEqual(strategy.scope.sports, frozenset({"esports"}))
        self.assertEqual(strategy.scope.games, frozenset({CS2}))
        self.assertEqual(strategy.maximum_bundles, 1)
        self.assertIsNotNone(strategy.maximum_markets)
        self.assertEqual(strategy.ranking.keys[0].key, "moneyline_volume_usd")
        self.assertIsNotNone(strategy.ranking.priorities)
        # Admission gates match the shipped strategy.
        for field in (
            "minimum_venues", "preferred_venues", "pre_event_seconds",
            "minimum_combined_moneyline_volume_usd", "target_budgets", "classes",
            "game_families",
        ):
            self.assertEqual(getattr(strategy, field), getattr(shipped, field), field)

    def test_example_is_a_full_copy_plus_the_v1_fields(self) -> None:
        shipped = json.loads(STRATEGY_PATH.read_text(encoding="utf-8"))
        example = json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
        added = {"scope", "ranking"}
        self.assertEqual(set(example) - set(shipped), added)
        for key in set(shipped) - {"note", "selection"}:
            self.assertEqual(example[key], shipped[key], key)


if __name__ == "__main__":
    unittest.main()
