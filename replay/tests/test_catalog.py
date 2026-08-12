from __future__ import annotations

import hashlib
import json
import unittest

from replay.catalog import (
    TARGET_RECORD_PROJECTIONS,
    MetadataCatalogue,
    has_fee_terms,
    project_record,
    projection_id,
    projection_sha256,
)
from replay.stream import MemoryByteStreamer


class ResolutionIdentityTests(unittest.TestCase):
    def test_conflicting_oracle_pair_is_not_silently_called_btc(self) -> None:
        record = {
            "title": "ETH Up or Down - 5 Min",
            "description": "Resolution source: Chainlink ETH/USD data stream.",
            "slug": "eth-up-or-down-5-min-1",
            "conditionId": "condition",
            "tokens": {"yes": "yes", "no": "no"},
            "collateralToken": {"decimals": 6},
            "priceOracleMetadata": {
                "chartSource": "chainlink",
                "chainlinkPair": "BTC/USD",
                "symbol": "Crypto.ETH/USD",
            },
        }
        record_hash = hashlib.sha256(
            json.dumps(
                record,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()
        target = {
            "asset_id": "eth-up-or-down-5-min-1",
            "condition_id": "condition",
            "market_id": "1",
            "resolution": {
                "catalogue_record": record,
                "catalogue_record_hash": record_hash,
            },
        }
        document = {
            "metadata_digest": "digest",
            "venue": "limitless",
            "targets": [target],
        }
        streamer = MemoryByteStreamer(
            {
                "live/metadata/limitless/digest.json": (
                    json.dumps(document, separators=(",", ":")).encode()
                )
            }
        )

        metadata = MetadataCatalogue.from_streamer(streamer).by_asset(
            "limitless", "eth-up-or-down-5-min-1"
        )

        self.assertIsNotNone(metadata)
        self.assertEqual(metadata.resolution_source, "chainlink:ETH/USD")
        self.assertEqual(metadata.resolution_identity_status, "CONFLICT")
        self.assertIn(
            "pair_fields_disagree:BTC/USD|ETH/USD",
            metadata.resolution_identity_conflicts,
        )


class ProjectionTests(unittest.TestCase):
    RECORD = {
        "clobTokenIds": '["yes", "no"]',
        "outcomes": '["Yes", "No"]',
        "orderMinSize": "5",
        "endDate": "2026-08-04T00:00:00Z",
        "description": "Resolves against the Binance 1 minute candle.",
        "feesEnabled": False,
        "volume24hr": 1000,
        "volumeNum": 50_000,
        "liquidity": 200,
        "bestBid": 0.41,
        "lastTradePrice": 0.42,
    }

    def test_trading_activity_does_not_move_the_projection(self) -> None:
        # The whole reason the projection exists. Consecutive targeter runs are
        # only 15.1% byte-identical on Polymarket, entirely from these fields —
        # projecting them would mint a fresh validity interval every ten minutes
        # for almost every market, which is noise with a boundary painted on it.
        moved = {
            **self.RECORD,
            "volume24hr": 999_999,
            "volumeNum": 123_456,
            "liquidity": 7,
            "bestBid": 0.99,
            "lastTradePrice": 0.98,
        }
        self.assertNotEqual(self.RECORD, moved)
        self.assertEqual(
            projection_sha256("polymarket", self.RECORD),
            projection_sha256("polymarket", moved),
        )

    def test_a_term_the_reader_consumes_does_move_the_projection(self) -> None:
        for field, value in (
            ("endDate", "2026-09-01T00:00:00Z"),
            ("orderMinSize", "15"),
            ("clobTokenIds", '["other", "no"]'),
            ("description", "Resolves against the Coinbase TWAP."),
            ("feesEnabled", True),
        ):
            with self.subTest(field=field):
                self.assertNotEqual(
                    projection_sha256("polymarket", self.RECORD),
                    projection_sha256("polymarket", {**self.RECORD, field: value}),
                )

    def test_an_absent_field_is_not_stored_as_null(self) -> None:
        # Otherwise a venue that stops publishing a field is indistinguishable
        # from one that publishes it as null, and the interval boundary lands in
        # the wrong place or not at all.
        without = {k: v for k, v in self.RECORD.items() if k != "orderMinSize"}
        explicit_null = {**self.RECORD, "orderMinSize": None}
        self.assertNotIn("orderMinSize", project_record("polymarket", without))
        self.assertIsNone(project_record("polymarket", explicit_null)["orderMinSize"])
        self.assertNotEqual(
            projection_sha256("polymarket", without),
            projection_sha256("polymarket", explicit_null),
        )

    def test_a_venue_the_reader_cannot_read_declares_no_projection(self) -> None:
        # `_instrument` has no Kalshi branch, so there is nothing this module
        # reads and therefore no basis for calling a Kalshi record unchanged.
        # Callers must read this as "cannot compact", never as "nothing changed".
        self.assertNotIn("kalshi", TARGET_RECORD_PROJECTIONS)
        self.assertIsNone(projection_id("kalshi"))
        self.assertIsNone(project_record("kalshi", self.RECORD))
        self.assertIsNone(projection_sha256("kalshi", self.RECORD))

    def test_projection_id_is_versioned(self) -> None:
        self.assertEqual(projection_id("polymarket"), "polymarket.v1")
        self.assertEqual(projection_id("limitless"), "limitless.v1")


class FeeEvidenceTests(unittest.TestCase):
    def test_disabled_fees_are_a_complete_fee_model(self) -> None:
        # `_fee_terms` returns FeeTerms("none", ...) here, so a gate that did
        # not count it was reporting "no fee evidence" for a market the reader
        # can price exactly.
        self.assertTrue(has_fee_terms({"feesEnabled": False}))

    def test_a_published_curve_counts(self) -> None:
        self.assertTrue(
            has_fee_terms({"feeSchedule": {"rate": "0.02", "exponent": "1"}})
        )

    def test_a_bare_fee_type_produces_nothing_and_counts_as_nothing(self) -> None:
        self.assertFalse(has_fee_terms({"feeType": "published_curve"}))
        self.assertFalse(has_fee_terms({"feesEnabled": True}))
        self.assertFalse(has_fee_terms({}))


if __name__ == "__main__":
    unittest.main()
