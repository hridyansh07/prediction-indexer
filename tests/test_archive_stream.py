from __future__ import annotations

import io
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from archive import ArchivedSegmentByteStreamer, read_verified_json
from archive.archiver import Archiver
from archive.common.receipts import read_archive_receipt
from archive.storage.base import ObjectExpectation, ObjectKeyError, VerificationFailure
from archive.storage.local import LocalObjectStore
from encoder import encode_stream, stored_identity_of
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


class VerifiedJsonTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.store = LocalObjectStore(self.root / "archive")

    def test_reads_plain_json_through_verified_bounded_path(self) -> None:
        payload = b'{"cadence": "current"}'
        key = "targeter-v2/run/report.json"
        metadata = self.store.put_immutable(
            key,
            io.BytesIO(payload),
            stored_identity_of(io.BytesIO(payload)),
            content_type="application/json",
        )
        result = read_verified_json(
            self.store,
            ObjectExpectation(
                key,
                metadata.stored,
                metadata.provider_checksum,
                metadata.provider_checksum_algorithm,
                metadata.content_type,
                metadata.content_encoding,
            ),
            max_decoded_bytes=len(payload),
            temp_root=self.root,
        )
        self.assertEqual(result, {"cadence": "current"})

    def test_reads_zstd_json_after_decoding_to_private_staging(self) -> None:
        logical_bytes = b'{"cadence": "current"}\n'
        encoded = io.BytesIO()
        encoded_result = encode_stream(io.BytesIO(logical_bytes), encoded)
        stored = encoded_result.stored
        key = "targeter-v2/run/report.json.zst"
        metadata = self.store.put_immutable(
            key,
            io.BytesIO(encoded.getvalue()),
            stored,
            content_type="application/json",
            content_encoding="zstd",
        )
        result = read_verified_json(
            self.store,
            ObjectExpectation(
                key,
                metadata.stored,
                metadata.provider_checksum,
                metadata.provider_checksum_algorithm,
                metadata.content_type,
                metadata.content_encoding,
            ),
            logical=encoded_result.logical,
            max_decoded_bytes=len(logical_bytes),
            temp_root=self.root,
        )
        self.assertEqual(result, {"cadence": "current"})


if __name__ == "__main__":
    unittest.main()
