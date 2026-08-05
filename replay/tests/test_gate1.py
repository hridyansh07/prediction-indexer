from __future__ import annotations

import json
import unittest

from replay.gate1 import Gate1Auditor
from replay.stream import MemoryByteStreamer


def _line(index: int, local: int, payload: dict, *, event_kind: str = "venue_frame") -> bytes:
    record = {
        "envelope_version": 2,
        "delivery_index": index,
        "record_id": f"pm-a-{local}",
        "visible_ns": 100 + index,
        "monotonic_ns": 50 + index,
        "venue": "polymarket",
        "stream": "public_book" if event_kind == "venue_frame" else "process",
        "connection_epoch": "a",
        "local_counter": local,
        "source_cursor": None,
        "kind": event_kind,
        "raw_payload": json.dumps(payload, separators=(",", ":")),
    }
    return json.dumps(record, separators=(",", ":")).encode() + b"\n"


class Gate1Tests(unittest.TestCase):
    def test_a_thin_tape_returns_no_and_names_every_missing_observable(self) -> None:
        opened = {
            "event": "connection_opened",
            "target_digest": "x",
            "target_metadata_digest": "missing",
            "asset_ids": ["asset"],
            "delivers_deltas": True,
            "fsync_interval_seconds": 0.25,
            "clock_scope": {
                "scope_id": "boot",
                "comparable_across_processes": True,
            },
        }
        closed = {"event": "connection_closed"}
        data = (
            _line(1, 1, opened, event_kind="control")
            + _line(2, 2, {"event_type": "book", "bids": [], "asks": [], "hash": "h"})
            + _line(3, 3, closed, event_kind="control")
        )
        streamer = MemoryByteStreamer(
            {
                "spool/venue=polymarket/date=2026-01-01/a.ndjson": data,
                "live/coverage.json": b'{"sightings":[]}\n',
            },
            chunk_size=3,
        )
        report = Gate1Auditor().audit(streamer)
        failed = {check.name for check in report.checks if check.status == "FAIL"}
        self.assertFalse(report.passed)
        self.assertIn("market_rules_and_metadata", failed)
        self.assertIn("polymarket_recovery_anchors", failed)
        self.assertIn("trade_and_fill_observability", failed)
        self.assertIn("game_event_observability", failed)

        # Market creation and resolution are reported but do not block. They
        # arrive only on a subscription sending `custom_feature_enabled`, which
        # capture does not set, so a blocking check here would gate every later
        # analysis on a capture change nobody has asked for.
        advisory = {check.name for check in report.checks if check.status == "ADVISORY"}
        self.assertEqual(advisory, {"market_lifecycle_observability"})
        self.assertNotIn("market_lifecycle_observability", failed)

    def test_report_is_identical_across_storage_chunking(self) -> None:
        data = _line(
            1,
            1,
            {
                "event": "connection_opened",
                "target_digest": "broadcast",
                "asset_ids": [],
                "delivers_deltas": False,
                "fsync_interval_seconds": 0.25,
                "clock_scope": {
                    "scope_id": "boot",
                    "comparable_across_processes": True,
                },
            },
            event_kind="control",
        ) + _line(2, 2, {"event": "connection_closed"}, event_kind="control")
        objects = {"spool/venue=polymarket_rtds/date=2026-01-01/a.ndjson": data}
        one = Gate1Auditor().audit(MemoryByteStreamer(objects, chunk_size=1)).as_record()
        many = Gate1Auditor().audit(MemoryByteStreamer(objects, chunk_size=4096)).as_record()
        self.assertEqual(one, many)


if __name__ == "__main__":
    unittest.main()
