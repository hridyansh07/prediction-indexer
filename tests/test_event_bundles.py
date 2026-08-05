from __future__ import annotations

import unittest

from analysis.event_bundles import build_event_bundles


class EventBundleTests(unittest.TestCase):
    def test_kalshi_game_children_are_grouped_as_moneyline(self) -> None:
        events = [
            {
                "venue": "kalshi",
                "event_id": "KXDOTA2GAME-26JUL181030PARITY",
                "title": "PARIVISION vs Team Yandex",
                "mutually_exclusive": True,
                "market_count": 2,
            }
        ]
        markets = [
            {
                "venue": "kalshi",
                "event_id": "KXDOTA2GAME-26JUL181030PARITY",
                "market_id": "KXDOTA2GAME-26JUL181030PARITY-PARI",
                "oddpool_market_id": "KXDOTA2GAME-26JUL181030PARITY-PARI",
                "yes_label": "PARIVISION",
            },
            {
                "venue": "kalshi",
                "event_id": "KXDOTA2GAME-26JUL181030PARITY",
                "market_id": "KXDOTA2GAME-26JUL181030PARITY-TY",
                "oddpool_market_id": "KXDOTA2GAME-26JUL181030PARITY-TY",
                "yes_label": "Team Yandex",
            },
        ]

        bundles, orphans = build_event_bundles(events, markets)

        self.assertEqual(orphans, [])
        self.assertEqual(len(bundles), 1)
        self.assertEqual(
            bundles[0]["structure"],
            "kalshi_mutually_exclusive_market_group",
        )
        self.assertEqual(
            bundles[0]["partition_status"],
            "venue_declared_mutually_exclusive",
        )
        self.assertEqual(
            [outcome["label"] for outcome in bundles[0]["outcomes"]],
            ["PARIVISION", "Team Yandex"],
        )

    def test_polymarket_binary_conditions_are_nested_under_event(self) -> None:
        events = [
            {
                "venue": "polymarket",
                "event_id": "630352",
                "title": "EWC League of Legends Winner",
                "market_count": 2,
            }
        ]
        markets = [
            {
                "venue": "polymarket",
                "event_id": "630352",
                "market_id": "1",
                "condition_id": "0x1",
                "group_item_title": "Gen.G",
                "token_ids": ["yes-1", "no-1"],
                "warnings": [],
            },
            {
                "venue": "polymarket",
                "event_id": "630352",
                "market_id": "2",
                "condition_id": "0x2",
                "group_item_title": "T1",
                "token_ids": ["yes-2", "no-2"],
                "warnings": [],
            },
        ]

        bundles, orphans = build_event_bundles(events, markets)

        self.assertEqual(orphans, [])
        self.assertEqual(
            bundles[0]["structure"],
            "polymarket_event_with_binary_conditions",
        )
        self.assertEqual(
            bundles[0]["partition_status"],
            "candidate_requires_rules_verification",
        )
        self.assertEqual(len(bundles[0]["outcomes"]), 2)


if __name__ == "__main__":
    unittest.main()

