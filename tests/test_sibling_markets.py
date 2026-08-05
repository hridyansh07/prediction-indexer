from __future__ import annotations

import unittest

from analysis.sibling_markets import (
    build_sibling_manifest,
    classify_kalshi_market_type,
    classify_polymarket_market_type,
)


class KalshiClassificationTests(unittest.TestCase):
    def test_known_series_map_to_structural_types(self) -> None:
        self.assertEqual(classify_kalshi_market_type("KXWCSCORE"), "correct_score")
        self.assertEqual(classify_kalshi_market_type("KXWCGAME"), "moneyline_3way")
        self.assertEqual(classify_kalshi_market_type("KXDOTA2MAP"), "map_winner")
        self.assertEqual(
            classify_kalshi_market_type("KXDOTA2GAME"), "series_moneyline"
        )

    def test_unknown_and_empty_series_fall_back_to_other(self) -> None:
        self.assertEqual(classify_kalshi_market_type("KXWCGOAL"), "other")
        self.assertEqual(classify_kalshi_market_type(None), "other")


class PolymarketClassificationTests(unittest.TestCase):
    def test_dota_sibling_labels(self) -> None:
        self.assertEqual(classify_polymarket_market_type("Match Winner"), "series_moneyline")
        self.assertEqual(classify_polymarket_market_type("Game 1 Winner"), "map_winner")
        self.assertEqual(classify_polymarket_market_type("Game 4 Winner"), "map_winner")
        self.assertEqual(classify_polymarket_market_type("O/U 2.5 Games"), "total_maps")
        self.assertEqual(classify_polymarket_market_type("O/U 3.5 Games"), "total_maps")
        self.assertEqual(
            classify_polymarket_market_type("Game Handicap: TY (-1.5)"), "map_handicap"
        )

    def test_novelty_markets_are_excluded(self) -> None:
        for label in (
            "Any Player Rampage",
            "Both Teams Beat Roshan",
            "Total Kills Over/Under 50.5",
            "First Blood in Game 1?",
            "Ends in Daytime",
        ):
            self.assertEqual(classify_polymarket_market_type(label), "other", label)

    def test_soccer_three_way_uses_fixture_outcomes(self) -> None:
        outcomes = ("Spain", "Argentina")
        self.assertEqual(
            classify_polymarket_market_type("Spain", moneyline_outcomes=outcomes),
            "moneyline_3way",
        )
        self.assertEqual(
            classify_polymarket_market_type(
                "Draw (Spain vs. Argentina)", moneyline_outcomes=outcomes
            ),
            "moneyline_3way",
        )
        self.assertEqual(
            classify_polymarket_market_type("Brazil", moneyline_outcomes=outcomes),
            "other",
        )


class ManifestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.kalshi = [
            {
                "market_id": "KXWCSCORE-26JUL19ESPARG-ESP1ARG0",
                "series_id": "KXWCSCORE",
                "event_key": "wc-2026-07-19-esp-arg",
                "market_type": "correct_score",
                "open_time": "2026-07-15T21:35:00Z",
                "close_time": "2026-07-19T21:19:51Z",
            },
            {
                "market_id": "KXWCGOAL-26JUL19ESPARG-MESSI",
                "series_id": "KXWCGOAL",
                "event_key": "wc-2026-07-19-esp-arg",
                "market_type": "other",
                "open_time": "2026-07-15T21:35:00Z",
                "close_time": "2026-07-19T21:19:51Z",
            },
        ]
        self.polymarket = [
            {
                "condition_id": "0xabc",
                "event_key": "wc-2026-07-19-esp-arg",
                "market_type": "moneyline_3way",
                "outcome_tokens": [
                    {"asset_id": "1", "outcome": "Yes"},
                    {"asset_id": "2", "outcome": "No"},
                ],
                "start_time": "2026-07-16T02:57:42Z",
                "end_time": "2026-07-19T19:00:00Z",
            }
        ]

    def test_targets_carry_the_fields_each_puller_needs(self) -> None:
        manifest = build_sibling_manifest(
            dataset_name="wc_knockout_2026",
            kalshi_markets=self.kalshi,
            polymarket_markets=self.polymarket,
        )
        kalshi_target = next(
            t for t in manifest["history_targets"] if t["venue"] == "kalshi"
        )
        # kalshi_history reads these exact keys.
        for key in ("ticker", "series_ticker", "open_time", "close_time"):
            self.assertIn(key, kalshi_target)

        polymarket_target = next(
            t for t in manifest["history_targets"] if t["venue"] == "polymarket"
        )
        # polymarket_history reads these exact keys.
        for key in ("market_id", "outcome_tokens", "start_time", "end_time"):
            self.assertIn(key, polymarket_target)

    def test_market_type_filter_drops_excluded_types(self) -> None:
        manifest = build_sibling_manifest(
            dataset_name="wc_knockout_2026",
            kalshi_markets=self.kalshi,
            polymarket_markets=self.polymarket,
            include_market_types=("correct_score", "moneyline_3way"),
        )
        tickers = [t.get("ticker") for t in manifest["history_targets"]]
        self.assertNotIn("KXWCGOAL-26JUL19ESPARG-MESSI", tickers)
        self.assertEqual(manifest["history_target_count"], 2)

    def test_events_group_market_types_by_venue(self) -> None:
        manifest = build_sibling_manifest(
            dataset_name="wc_knockout_2026",
            kalshi_markets=self.kalshi,
            polymarket_markets=self.polymarket,
        )
        self.assertEqual(manifest["event_count"], 1)
        event = manifest["events"][0]
        self.assertIn("correct_score", event["kalshi"])
        self.assertIn("moneyline_3way", event["polymarket"])

    def test_manifest_records_that_no_depth_is_available(self) -> None:
        manifest = build_sibling_manifest(
            dataset_name="wc_knockout_2026",
            kalshi_markets=self.kalshi,
            polymarket_markets=self.polymarket,
        )
        self.assertIsNone(manifest["scope"]["depth_source"])
        for target in manifest["history_targets"]:
            self.assertFalse(target["depth_available"])

    def test_markets_without_identifiers_are_skipped(self) -> None:
        manifest = build_sibling_manifest(
            dataset_name="x",
            kalshi_markets=[{"series_id": "KXWCGAME", "market_type": "moneyline_3way"}],
            polymarket_markets=[{"event_key": "e", "market_type": "moneyline_3way"}],
        )
        self.assertEqual(manifest["history_target_count"], 0)


if __name__ == "__main__":
    unittest.main()
