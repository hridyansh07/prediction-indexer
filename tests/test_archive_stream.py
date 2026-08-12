from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from archive import ArchivedSegmentByteStreamer
from archive.archiver import Archiver
from archive.common.receipts import read_archive_receipt
from archive.storage.base import ObjectKeyError, VerificationFailure
from archive.storage.s3 import S3ObjectStore
from encoder import CodecError
from tests.archive_fixtures import write_sealed_segment
from tests.test_s3store import BUCKET, OWNER, REGION, FakeS3Client


class ArchivedSegmentByteStreamerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.segment = write_sealed_segment(self.spool)
        self.seal = self.segment.with_name(
            self.segment.name[: -len(".ndjson")] + ".seal.json"
        )
        self.client = FakeS3Client()
        self.store = S3ObjectStore(BUCKET, REGION, OWNER, client=self.client)
        outcome = Archiver(self.spool, self.store).sweep()
        self.assertEqual(outcome.counts["archived"], 1, outcome.as_record())
        receipt_path = self.segment.with_name(
            self.segment.name[: -len(".ndjson")] + ".archive.json"
        )
        self.receipt = read_archive_receipt(receipt_path)

    def test_streams_verified_decoded_data_and_seal_under_replay_keys(self) -> None:
        streamer = ArchivedSegmentByteStreamer(
            self.store,
            [self.receipt],
            temp_root=self.root,
            chunk_size=7,
        )
        data_key = next(key for key in streamer.object_keys() if key.endswith(".ndjson"))
        seal_key = next(key for key in streamer.object_keys() if key.endswith(".seal.json"))

        self.assertTrue(data_key.startswith("lane=polymarket/date="))
        self.assertEqual(b"".join(streamer.iter_bytes(data_key)), self.segment.read_bytes())
        self.assertEqual(b"".join(streamer.iter_bytes(seal_key)), self.seal.read_bytes())
        self.assertFalse(list(self.root.glob("archive-replay-*")))

    def test_corrupt_compressed_bytes_are_not_partially_exposed(self) -> None:
        stored = self.client.objects[self.receipt.data_key]["bytes"]
        self.client.objects[self.receipt.data_key]["bytes"] = stored[:-1] + bytes([stored[-1] ^ 1])
        streamer = ArchivedSegmentByteStreamer(self.store, [self.receipt], temp_root=self.root)
        data_key = next(key for key in streamer.object_keys() if key.endswith(".ndjson"))
        iterator = streamer.iter_bytes(data_key)

        with self.assertRaises(CodecError):
            next(iterator)
        self.assertFalse(list(self.root.glob("archive-replay-*")))

    def test_duplicate_logical_keys_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate archived replay object key"):
            ArchivedSegmentByteStreamer(self.store, [self.receipt, self.receipt])

    def test_traversing_archive_key_is_rejected_before_it_is_exposed(self) -> None:
        receipt = replace(
            self.receipt,
            data_key="raw/lane=polymarket/../escaped.ndjson.zst",
        )
        with self.assertRaises(ObjectKeyError):
            ArchivedSegmentByteStreamer(self.store, [receipt])

    def test_unknown_key_fails_loudly(self) -> None:
        streamer = ArchivedSegmentByteStreamer(self.store, [self.receipt])
        with self.assertRaises(VerificationFailure):
            next(streamer.iter_bytes("lane=polymarket/date=2026-07-30/missing.ndjson"))


if __name__ == "__main__":
    unittest.main()
