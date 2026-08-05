"""`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` §14 — "Pipeline tests".

`test_s3store.py` proves the adapter in isolation against a fake S3 client.
This proves `Archiver`, `verify_archive`, the manifest builder and `Reaper`
behave correctly with that same adapter standing in as the real `ObjectStore`
— the thing that actually ships, not a mock of it. Nothing here talks to AWS;
`FakeS3Client` is the same in-memory double `test_s3store.py` defines.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from archive.archiver import ARCHIVED, Archiver
from archive.archiver.manifest import build_daily_manifests, discover_archive_receipts, write_daily_manifests
from archive.storage.base import IntegrityConflict, ObjectStoreError
from archive.reaper import AUDIT_MODE, REAPED, RETAINED, Reaper
from archive.common.receipts import PRODUCTION, read_archive_receipt
from archive.storage.s3 import S3ObjectStore
from archive.common.verify import VerificationError, decode_archived_segment, verify_archive
from tests.archive_fixtures import canonical_input_for, write_canonical_receipt, write_sealed_segment
from tests.test_s3store import BUCKET, OWNER, REGION, FakeS3Client

NANOSECONDS = 1_000_000_000


class S3PipelineCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.canonical = self.root / "canonical"
        self.segment = write_sealed_segment(self.spool)
        self.seal = self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".seal.json")
        self.client = FakeS3Client()
        self.store = S3ObjectStore(BUCKET, REGION, OWNER, client=self.client)
        self.archiver = Archiver(self.spool, self.store)

    @property
    def receipt_path(self) -> Path:
        return self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".archive.json")

    @property
    def derivative(self) -> Path:
        return self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".ndjson.zst")


class ArchivalThroughS3Tests(S3PipelineCase):
    def test_a_sealed_segment_archives_as_a_production_receipt_naming_the_bucket(self) -> None:
        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 1, result.as_record())
        self.assertTrue(self.receipt_path.is_file())
        local_receipt = self.receipt_path.with_name(
            self.receipt_path.name.replace(".archive.json", ".archive.local.json")
        )
        self.assertFalse(local_receipt.exists())

        document = json.loads(self.receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(document["object"]["bucket"], BUCKET)
        self.assertEqual(document["seal"]["bucket"], BUCKET)

        receipt = read_archive_receipt(self.receipt_path)
        self.assertTrue(receipt.is_production)
        verify_archive(self.store, receipt)

    def test_data_and_seal_identities_come_from_fresh_s3_heads_not_the_put_response(self) -> None:
        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        data_head = self.store.head(receipt.data_key)
        seal_head = self.store.head(receipt.seal_key)
        assert data_head is not None and seal_head is not None
        self.assertEqual(receipt.data_stored.sha256, data_head.sha256)
        self.assertEqual(receipt.data_stored.byte_length, data_head.byte_length)
        self.assertEqual(receipt.seal_stored.sha256, seal_head.sha256)

    def test_the_archived_object_decodes_back_to_the_exact_source_bytes_through_s3(self) -> None:
        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        destination = self.root / "decoded.ndjson"
        logical = decode_archived_segment(self.store, receipt, destination)
        self.assertEqual(logical, receipt.source)
        self.assertEqual(destination.read_bytes(), self.segment.read_bytes())

    def test_a_receipt_naming_another_bucket_cannot_verify(self) -> None:
        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        impostor = S3ObjectStore("a-different-bucket", REGION, OWNER, client=self.client)
        with self.assertRaises(VerificationError):
            verify_archive(impostor, receipt)


class ArchivalFailureThroughS3Tests(S3PipelineCase):
    def test_a_conditional_write_conflict_leaves_raw_seal_and_derivative_intact(self) -> None:
        from archive.archiver import object_keys
        from archive.common.seal import read_sealed_segment

        data_key, _ = object_keys(read_sealed_segment("polymarket", self.segment))
        self.client.conflicted_keys.add(data_key)

        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertNotEqual(outcome.status, ARCHIVED)
        self.assertFalse(self.receipt_path.exists())
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())

    def test_a_key_already_holding_different_content_is_a_fatal_conflict(self) -> None:
        from archive.archiver import object_keys
        from archive.common.seal import read_sealed_segment

        data_key, _ = object_keys(read_sealed_segment("polymarket", self.segment))
        self.client.seed(data_key, b"a squatter object already lives here\n")

        result = self.archiver.sweep()
        self.assertEqual(result.count("conflict"), 1)
        self.assertIsNotNone(result.halted)
        self.assertFalse(self.receipt_path.exists())
        self.assertTrue(self.segment.exists())


class ManifestThroughS3Tests(S3PipelineCase):
    def setUp(self) -> None:
        super().setUp()
        self.archiver.sweep()
        self.manifest_root = self.root / "manifests"

    def build(self):
        return build_daily_manifests(
            self.store, discover_archive_receipts(self.spool), kind=PRODUCTION
        )

    def test_a_transiently_unavailable_object_is_excluded_without_crashing(self) -> None:
        receipt = read_archive_receipt(self.receipt_path)
        self.client.denied_keys.add(receipt.data_key)
        result = self.build()
        self.assertEqual(result.manifests, [])
        self.assertEqual(len(result.excluded), 1)

    def test_a_stale_manifest_disappears_when_its_last_valid_entry_disappears(self) -> None:
        write_daily_manifests(self.manifest_root, self.build())
        path = self.manifest_root / "date=2026-07-30" / "manifest.json"
        self.assertTrue(path.is_file())

        receipt = read_archive_receipt(self.receipt_path)
        del self.client.objects[receipt.data_key]
        result = write_daily_manifests(self.manifest_root, self.build())
        self.assertFalse(path.exists())
        self.assertEqual(result.removed, [path])


class ReaperThroughS3Tests(S3PipelineCase):
    def setUp(self) -> None:
        super().setUp()
        self.archiver.sweep()
        write_canonical_receipt(self.canonical, inputs=[canonical_input_for(self.segment)])

    def reaper(self, *, destructive: bool) -> Reaper:
        return Reaper(self.spool, self.canonical, self.store, destructive=destructive)

    def test_audit_mode_verifies_both_s3_objects_and_keeps_local_raw(self) -> None:
        result = self.reaper(destructive=False).sweep()
        self.assertEqual(len(result.decisions), 1)
        decision = result.decisions[0]
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, AUDIT_MODE)
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())

    def test_destructive_reaping_deletes_only_when_explicitly_requested(self) -> None:
        result = self.reaper(destructive=True).sweep()
        self.assertEqual(result.decisions[0].decision, REAPED)
        self.assertFalse(self.segment.exists())
        self.assertFalse(self.seal.exists())


if __name__ == "__main__":
    unittest.main()
