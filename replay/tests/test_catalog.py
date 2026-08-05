from __future__ import annotations

import hashlib
import json
import unittest

from replay.catalog import MetadataCatalogue
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


if __name__ == "__main__":
    unittest.main()
