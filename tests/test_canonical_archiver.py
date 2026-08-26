"""Canonical windows use the same immutable, receipt-last archive boundary as raw."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive import ArchivedCanonicalByteStreamer
from archive.archiver.canonical import (
    ARCHIVED,
    FAILED,
    CanonicalArchiver,
    canonical_object_keys,
    read_canonical_archive_receipt,
    verify_canonical_archive,
)
from archive.storage import INDEPENDENT, LocalObjectStore, ObjectStoreError
from archive.storage.s3 import S3ObjectStore
from tests.archive_fixtures import write_canonical_receipt
from tests.test_s3store import BUCKET, OWNER, REGION, FakeS3Client


class CanonicalArchiverCase(unittest.TestCase):
    durability = None

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.canonical = self.root / "canonical"
        self.source_receipt = write_canonical_receipt(self.canonical, evidence_lines=2)
        options = {} if self.durability is None else {"durability": self.durability}
        self.store = LocalObjectStore(self.root / "archive", **options)
        self.archiver = CanonicalArchiver(
            self.canonical, self.store, now_ns=lambda: 123
        )

    @property
    def archive_receipt(self) -> Path:
        suffix = (
            "canonical_archive_receipt.json"
            if self.durability is INDEPENDENT
            else "canonical_archive_receipt.local.json"
        )
        return self.source_receipt.with_name(suffix)

    def test_committed_window_archives_existing_zstd_outputs_and_receipt_last(
        self,
    ) -> None:
        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())

        receipt = read_canonical_archive_receipt(self.archive_receipt)
        keys = canonical_object_keys(self.source_receipt)
        self.assertEqual(
            (
                receipt.evidence.key,
                receipt.provenance.key,
                receipt.canonical_receipt.key,
            ),
            keys,
        )
        self.assertEqual(receipt.verified_at_ns, 123)
        verify_canonical_archive(self.store, receipt)

        for key in keys:
            self.assertIsNotNone(self.store.head(key))
        with self.store.open(keys[0]) as archived_evidence:
            self.assertEqual(
                archived_evidence.read(),
                self.source_receipt.with_name("evidence.ndjson.zst").read_bytes(),
            )
        with self.store.open(keys[2]) as archived_receipt:
            self.assertEqual(archived_receipt.read(), self.source_receipt.read_bytes())

    def test_retrieves_canonical_evidence_provenance_and_receipt(self) -> None:
        self.archiver.sweep()
        receipt = read_canonical_archive_receipt(self.archive_receipt)
        streamer = ArchivedCanonicalByteStreamer(
            self.store, (receipt,), temp_root=self.root, chunk_size=7
        )
        evidence_key = next(
            key for key in streamer.object_keys() if key.endswith("/evidence.ndjson")
        )
        provenance_key = next(
            key for key in streamer.object_keys() if key.endswith("/provenance.ndjson")
        )
        receipt_key = next(
            key for key in streamer.object_keys() if key.endswith("/receipt.json")
        )

        self.assertEqual(
            b"".join(streamer.iter_bytes(evidence_key)),
            b'{"canonical":0}\n{"canonical":1}\n',
        )
        self.assertEqual(
            b"".join(streamer.iter_bytes(provenance_key)),
            b'{"canonical_seq":1}\n{"canonical_seq":2}\n',
        )
        self.assertEqual(
            b"".join(streamer.iter_bytes(receipt_key)),
            self.source_receipt.read_bytes(),
        )

    def test_corrupt_zstd_is_rejected_before_any_archive_receipt(self) -> None:
        evidence = self.source_receipt.with_name("evidence.ndjson.zst")
        evidence.write_bytes(evidence.read_bytes()[:-1])
        outcome = self.archiver.archive_window(self.source_receipt)
        self.assertEqual(outcome.status, FAILED)
        self.assertFalse(self.archive_receipt.exists())

    def test_existing_receipt_is_idempotent_only_after_remote_reverification(
        self,
    ) -> None:
        self.archiver.sweep()
        before = self.archive_receipt.read_bytes()
        result = self.archiver.sweep()
        self.assertEqual(result.counts["skipped"], 1)
        self.assertEqual(self.archive_receipt.read_bytes(), before)

    def test_receipt_object_upload_failure_publishes_no_archive_marker(self) -> None:
        class FailingReceiptStore:
            def __init__(self, inner):
                self.inner = inner
                self.store_id = inner.store_id
                self.durability = inner.durability

            def put_immutable(self, key, reader, expected_identity, **kwargs):
                if key.endswith("/receipt.json"):
                    raise ObjectStoreError("receipt upload failed")
                return self.inner.put_immutable(
                    key, reader, expected_identity, **kwargs
                )

            def head(self, key):
                return self.inner.head(key)

            def verify_metadata(self, expected):
                return self.inner.verify_metadata(expected)

            def verify(self, expected):
                return self.inner.verify(expected)

            def open(self, key, **kwargs):
                return self.inner.open(key, **kwargs)

        outcome = CanonicalArchiver(
            self.canonical, FailingReceiptStore(self.store)
        ).archive_window(self.source_receipt)
        self.assertEqual(outcome.status, FAILED)
        self.assertFalse(self.archive_receipt.exists())
        self.assertTrue(self.source_receipt.is_file())


class ProductionCanonicalArchiverTests(CanonicalArchiverCase):
    durability = INDEPENDENT

    def test_production_receipt_records_provider_checksums_for_all_objects(
        self,
    ) -> None:
        self.archiver.sweep()
        document = json.loads(self.archive_receipt.read_text(encoding="utf-8"))
        self.assertEqual(document["canonical_archive_receipt_version"], 2)
        self.assertNotIn("local_canonical_archive_receipt_version", document)
        self.assertEqual(
            document["store"],
            {
                "provider": self.store.provider,
                "location": self.store.store_id,
            },
        )
        for field in ("evidence", "provenance", "canonical_receipt"):
            self.assertTrue(document[field]["provider_checksum"])
            self.assertTrue(document[field]["provider_checksum_algorithm"])


class CanonicalArchivalThroughS3Tests(unittest.TestCase):
    def test_canonical_window_archives_through_the_real_s3_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            canonical = root / "canonical"
            source_receipt = write_canonical_receipt(canonical, evidence_lines=1)
            store = S3ObjectStore(BUCKET, REGION, OWNER, client=FakeS3Client())

            outcome = CanonicalArchiver(canonical, store).archive_window(source_receipt)
            self.assertEqual(outcome.status, ARCHIVED, outcome.detail)
            marker = source_receipt.with_name("canonical_archive_receipt.json")
            receipt = read_canonical_archive_receipt(marker)
            self.assertEqual(receipt.location, BUCKET)
            verify_canonical_archive(store, receipt)


if __name__ == "__main__":
    unittest.main()
