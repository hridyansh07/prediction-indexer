from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive.archiver import ARCHIVED, Archiver
from archive.archiver.universe import (
    PUBLISHED,
    read_segment_universe_receipt,
    segment_universe_keys,
)
from archive.common.receipts import read_archive_receipt
from archive.storage import INDEPENDENT, LocalObjectStore
from splices.common.segment import Record, SegmentWriter
from tests.archive_fixtures import BASE_NS, WINDOW_SECONDS


def _envelope(
    delivery_index: int,
    *,
    kind: str,
    event: str | None = None,
    detail: dict | None = None,
) -> Record:
    epoch = "epoch-1"
    payload = "venue bytes" if event is None else json.dumps(
        {"event": event, **(detail or {})}, separators=(",", ":"), sort_keys=True
    )
    document = {
        "envelope_version": 2,
        "delivery_index": delivery_index,
        "record_id": f"pm-{delivery_index}",
        "visible_ns": BASE_NS + delivery_index * 1_000_000_000,
        "monotonic_ns": delivery_index * 1_000_000_000,
        "venue": "polymarket",
        "stream": "public_book",
        "connection_epoch": epoch,
        "local_counter": delivery_index,
        "source_cursor": {"type": "unsequenced", "counter": delivery_index},
        "kind": kind,
        "raw_payload": payload,
    }
    line = (json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n").encode()
    return Record(
        line=line,
        visible_ns=document["visible_ns"],
        delivery_index=delivery_index,
        epoch=epoch,
    )


def _segment(root: Path) -> Path:
    writer = SegmentWriter(
        root,
        "polymarket",
        BASE_NS,
        segment_seconds=WINDOW_SECONDS,
        segment_index=0,
        segment_id="universe",
    )
    writer.write_batch(
        [
            _envelope(
                1,
                kind="control",
                event="connection_opened",
                detail={
                    "target_digest": "digest-a",
                    "target_count": 2,
                    "asset_ids": ["asset-a", "asset-b"],
                },
            ),
            _envelope(2, kind="venue_frame"),
            _envelope(
                3,
                kind="control",
                event="subscription_sent",
                detail={"target_digest": "digest-a", "target_count": 2},
            ),
            _envelope(4, kind="fault", event="connection_failed"),
            _envelope(5, kind="control", event="connection_closed"),
        ]
    )
    writer.seal("boundary")
    return writer.data_path


class SegmentUniverseArtifactTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.spool = self.root / "spool"
        self.segment = _segment(self.spool)
        self.store = LocalObjectStore(
            self.root / "archive",
            store_id="test-production-archive",
            durability=INDEPENDENT,
        )
        self.archiver = Archiver(self.spool, self.store, now_ns=lambda: BASE_NS + 99)

    def test_raw_archival_publishes_exact_control_lines_then_a_segment_receipt(self) -> None:
        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())
        self.assertEqual(result.counts["universe_published"], 1, result.as_record())
        outcome = result.outcomes[0]
        self.assertEqual(outcome.status, ARCHIVED)
        self.assertEqual(outcome.universe_status, PUBLISHED)

        archive_path = self.segment.with_name(
            self.segment.name.removesuffix(".ndjson") + ".archive.json"
        )
        archive_receipt = read_archive_receipt(archive_path)
        control_key, receipt_key = segment_universe_keys(archive_receipt)
        universe = read_segment_universe_receipt(self.store, receipt_key)
        self.assertEqual(universe.control.logical.line_count, 3)
        self.assertEqual(universe.control.first_delivery_index, 1)
        self.assertEqual(universe.control.last_delivery_index, 5)
        self.assertEqual(universe.source_archive_receipt.sha256, outcome.archive_receipt_sha256)

        from encoder import decode_stream

        with self.store.open(control_key, max_bytes=universe.control.stored.byte_length) as source:
            import io

            decoded = io.BytesIO()
            decode_stream(
                source,
                decoded,
                expected_logical=universe.control.logical,
                expected_stored=universe.control.stored,
                max_decoded_bytes=universe.control.logical.byte_length,
            )
        expected = b"".join(
            line
            for line in self.segment.read_bytes().splitlines(keepends=True)
            if json.loads(line)["kind"] == "control"
        )
        self.assertEqual(decoded.getvalue(), expected)

    def test_retry_reuses_the_immutable_universe_receipt(self) -> None:
        first = self.archiver.sweep()
        receipt_key = first.outcomes[0].universe_receipt_key
        assert receipt_key is not None
        before = (self.store.root / receipt_key).read_bytes()

        second = self.archiver.sweep()
        self.assertEqual(second.counts["skipped"], 1)
        self.assertEqual(second.counts["universe_skipped"], 1)
        self.assertEqual((self.store.root / receipt_key).read_bytes(), before)

    def test_sidecar_failure_does_not_invalidate_the_raw_archive(self) -> None:
        # The seal has to describe the malformed source for raw archival to be
        # valid. This models old exact tape whose envelope cannot be classified
        # by the new derivative, not mutation after sealing.
        self.segment.unlink()
        seal = self.segment.with_name(
            self.segment.name.removesuffix(".ndjson") + ".seal.json"
        )
        seal.unlink()
        writer = SegmentWriter(
            self.spool,
            "polymarket",
            BASE_NS,
            segment_seconds=WINDOW_SECONDS,
            segment_index=0,
            segment_id="malformed",
        )
        writer.write_batch(
            [
                Record(
                    line=b'{"kind":\n',
                    visible_ns=BASE_NS + 1_000_000_000,
                    delivery_index=1,
                    epoch="epoch-1",
                )
            ]
        )
        writer.seal("boundary")

        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())
        self.assertEqual(result.counts["universe_failed"], 1, result.as_record())
        self.assertTrue(next(self.spool.rglob("*.archive.json")).is_file())


if __name__ == "__main__":
    unittest.main()
