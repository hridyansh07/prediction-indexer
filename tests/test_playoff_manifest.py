from __future__ import annotations

import unittest

from analysis.playoff_manifest import build_playoff_manifest


class PlayoffManifestTests(unittest.TestCase):
    def test_only_match_winner_is_selected_and_both_kalshi_sides_are_targets(self) -> None:
        kalshi_events = [
            {
                "event_id": "KXDOTA2GAME-EXAMPLE",
                "title": "PARIVISION vs. Team Yandex",
                "mutually_exclusive": True,
            }
        ]
        kalshi_markets = [
            {
                "event_id": "KXDOTA2GAME-EXAMPLE",
                "market_id": "KXDOTA2GAME-EXAMPLE-PARI",
                "oddpool_market_id": "KXDOTA2GAME-EXAMPLE-PARI",
                "yes_label": "PARIVISION",
                "open_time": "2026-07-16T00:00:00Z",
                "close_time": "2026-07-18T00:00:00Z",
                "rules_hash": "a",
            },
            {
                "event_id": "KXDOTA2GAME-EXAMPLE",
                "market_id": "KXDOTA2GAME-EXAMPLE-TY",
                "oddpool_market_id": "KXDOTA2GAME-EXAMPLE-TY",
                "yes_label": "Team Yandex",
                "open_time": "2026-07-16T00:00:00Z",
                "close_time": "2026-07-18T00:00:00Z",
                "rules_hash": "b",
            },
        ]
        polymarket_events = [
            {
                "event_id": "710578",
                "slug": "dota2-ty-pari-2026-07-18",
                "title": (
                    "Dota 2: Team Yandex vs PARIVISION (BO3) - "
                    "Esports World Cup Playoffs"
                ),
            }
        ]
        polymarket_markets = [
            {
                "event_id": "710578",
                "event_slug": "dota2-ty-pari-2026-07-18",
                "market_slug": "dota2-ty-pari-2026-07-18",
                "group_item_title": "Match Winner",
                "market_id": "2947646",
                "condition_id": "0xwinner",
                "oddpool_market_id": "0xwinner",
                "outcome_tokens": [],
            },
            {
                "event_id": "710578",
                "event_slug": "dota2-ty-pari-2026-07-18",
                "market_slug": "dota2-ty-pari-2026-07-18-game1",
                "group_item_title": "Game 1 Winner",
                "market_id": "2947641",
                "condition_id": "0xprop",
                "oddpool_market_id": "0xprop",
            },
        ]

        manifest = build_playoff_manifest(
            kalshi_events,
            kalshi_markets,
            polymarket_events,
            polymarket_markets,
        )

        self.assertEqual(manifest["match_count"], 1)
        self.assertEqual(manifest["history_target_count"], 3)
        poly_targets = [
            row for row in manifest["history_targets"] if row["venue"] == "polymarket"
        ]
        self.assertEqual([row["market_id"] for row in poly_targets], ["0xwinner"])


if __name__ == "__main__":
    unittest.main()
