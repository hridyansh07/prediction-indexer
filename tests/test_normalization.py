from __future__ import annotations

import unittest

from analysis.kalshi import normalize_kalshi_market
from analysis.polymarket import normalize_polymarket_market


class KalshiNormalizationTests(unittest.TestCase):
    def test_ticker_is_oddpool_market_id(self) -> None:
        event = {
            "series_ticker": "KXLOLGAME",
            "event_ticker": "KXLOLGAME-26JUL190600GENT1",
            "title": "Gen.G vs. T1",
            "mutually_exclusive": True,
            "category": "Sports",
        }
        market = {
            "ticker": "KXLOLGAME-26JUL190600GENT1-T1",
            "title": "Will T1 win?",
            "yes_sub_title": "T1",
            "no_sub_title": "Gen.G",
            "rules_primary": "Resolves Yes if T1 wins.",
            "rules_secondary": "Void rules.",
            "close_time": "2026-07-19T13:09:34Z",
        }

        normalized = normalize_kalshi_market(event, market)

        self.assertEqual(
            normalized["oddpool_market_id"],
            "KXLOLGAME-26JUL190600GENT1-T1",
        )
        self.assertEqual(normalized["event_id"], "KXLOLGAME-26JUL190600GENT1")
        self.assertIsNotNone(normalized["rules_hash"])


class PolymarketNormalizationTests(unittest.TestCase):
    def test_condition_and_token_ids_are_preserved(self) -> None:
        event = {"id": "630352", "title": "EWC League of Legends Winner"}
        market = {
            "id": "2671103",
            "question": "Will Gen.G win the EWC League of Legends Tournament",
            "conditionId": "0xabc",
            "clobTokenIds": '["111","222"]',
            "outcomes": '["Yes","No"]',
            "description": "Resolution rules",
        }

        normalized = normalize_polymarket_market(event, market)

        self.assertEqual(normalized["oddpool_market_id"], "0xabc")
        self.assertEqual(normalized["token_ids"], ["111", "222"])
        self.assertEqual(
            normalized["outcome_tokens"],
            [
                {"outcome": "Yes", "asset_id": "111"},
                {"outcome": "No", "asset_id": "222"},
            ],
        )
        self.assertEqual(normalized["warnings"], [])

    def test_placeholder_child_is_flagged(self) -> None:
        event = {"id": "630352"}
        market = {
            "id": "2671122",
            "question": "Will A win the EWC League of Legends Tournament",
            "conditionId": "0xabc",
            "clobTokenIds": '["111","222"]',
        }

        normalized = normalize_polymarket_market(event, market)

        self.assertIn("placeholder_outcome", normalized["warnings"])


if __name__ == "__main__":
    unittest.main()

