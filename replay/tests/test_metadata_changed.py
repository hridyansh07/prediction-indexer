from __future__ import annotations

import json
import unittest

from replay.envelope import parse_envelope
from replay.events import MetadataChanged, normalize
from replay.order import OrderedEnvelope


class MetadataChangedTests(unittest.TestCase):
    def test_normalization_preserves_both_digests_as_an_audit_event(self) -> None:
        payload = {
            "event": "target_metadata_changed",
            "target_digest": "unchanged",
            "from_metadata_digest": "before",
            "to_metadata_digest": "after",
            "metadata_path": "snapshots/after.json",
        }
        line = json.dumps(
            {
                "envelope_version": 2,
                "delivery_index": 7,
                "record_id": "pm-e-7",
                "visible_ns": 11,
                "monotonic_ns": 9,
                "venue": "polymarket",
                "stream": "process",
                "connection_epoch": "e",
                "local_counter": 7,
                "source_cursor": None,
                "kind": "control",
                "raw_payload": json.dumps(payload),
            }
        ).encode()
        item = OrderedEnvelope(
            lane="polymarket",
            object_key="capture.ndjson",
            line_number=1,
            order_ns=9,
            order_clock="monotonic_ns",
            envelope=parse_envelope(line),
        )

        self.assertEqual(
            list(normalize(item)),
            [
                MetadataChanged(
                    venue="polymarket",
                    lane="polymarket",
                    epoch="e",
                    record_id="pm-e-7",
                    delivery_index=7,
                    order_ns=9,
                    visible_ns=11,
                    event_index=0,
                    from_metadata_digest="before",
                    to_metadata_digest="after",
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
