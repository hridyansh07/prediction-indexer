"""§9.3 — the raw segment archiver.

The shape of every test here is the same: make the unsafe thing true, then
assert that no archive receipt exists and no local artifact was touched. The
receipt is the archive commit marker, so "published nothing" is the only
statement that distinguishes a failure that preserved the tape from one that
lost it.
"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

from archive.archiver import service as archiver_module
from archive.common import durable as durable_module
from archive.storage import local as objectstore_module
from archive.archiver import ARCHIVED, CONFLICT, FAILED, SKIPPED, Archiver, object_keys
from archive.archiver.manifest import (
    build_daily_manifests,
    discover_archive_receipts,
    write_daily_manifests,
)
from archive.storage import INDEPENDENT, IntegrityConflict, LocalObjectStore, ObjectStoreError
from archive.common.receipts import LOCAL, PRODUCTION, ReceiptError, read_archive_receipt
from archive.common.seal import read_sealed_segment
from archive.common.verify import VerificationError, decode_archived_segment, verify_archive
from encoder import IdentityMismatch, LogicalIdentity, stored_identity_of
from tests.archive_fixtures import BASE_NS, WINDOW_SECONDS, write_sealed_segment

NANOSECONDS = 1_000_000_000


class ArchiveCase(unittest.TestCase):
    """A spool with one sealed segment and a conformance archive beside it."""

    durability = None

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.segment = write_sealed_segment(self.spool)
        self.seal = self.segment.with_name(
            self.segment.name[: -len(".ndjson")] + ".seal.json"
        )
        self.store = self._store()
        self.archiver = Archiver(self.spool, self.store)

    def _store(self) -> LocalObjectStore:
        if self.durability is None:
            return LocalObjectStore(self.root / "archive")
        return LocalObjectStore(self.root / "archive", durability=self.durability)

    @property
    def receipt_path(self) -> Path:
        suffix = ".archive.json" if self.archiver.receipt_kind == PRODUCTION else ".archive.local.json"
        return self.segment.with_name(self.segment.name[: -len(".ndjson")] + suffix)

    @property
    def derivative(self) -> Path:
        return self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".ndjson.zst")

    def receipts_on_disk(self) -> list[Path]:
        return sorted(self.spool.rglob("*.archive*.json"))

    def assert_nothing_published(self) -> None:
        self.assertEqual(self.receipts_on_disk(), [], "an archive receipt was published")
        self.assertTrue(self.segment.exists(), "the raw segment was disturbed")
        self.assertTrue(self.seal.exists(), "the seal was disturbed")
        self.assertEqual(
            [path.name for path in self.spool.rglob("*.open")], [], "a temporary file survived"
        )


class HappyPathTests(ArchiveCase):
    def test_a_sealed_segment_becomes_two_objects_and_a_receipt(self) -> None:
        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 1)

        receipt = read_archive_receipt(self.receipt_path)
        self.assertEqual(receipt.kind, LOCAL)
        self.assertEqual(receipt.lane_id, "polymarket")

        segment = read_sealed_segment("polymarket", self.segment)
        data_key, seal_key = object_keys(segment)
        self.assertEqual(receipt.data_key, data_key)
        self.assertEqual(receipt.seal_key, seal_key)
        self.assertEqual(receipt.source, segment.logical)

        data = self.store.head(data_key)
        seal = self.store.head(seal_key)
        assert data is not None and seal is not None
        self.assertEqual(data.content_type, "application/x-ndjson")
        self.assertEqual(data.content_encoding, "zstd")
        # The seal object is the exact unchanged local seal bytes.
        with self.seal.open("rb") as handle:
            self.assertTrue(seal.matches(stored_identity_of(handle)))

    def test_the_archived_object_decodes_back_to_the_exact_source_bytes(self) -> None:
        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        restored = self.root / "restored.ndjson"
        logical = decode_archived_segment(self.store, receipt, restored)
        self.assertEqual(restored.read_bytes(), self.segment.read_bytes())
        self.assertEqual(logical, receipt.source)

    def test_decoding_cannot_exceed_the_sealed_byte_length(self) -> None:
        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        clipped = self.root / "clipped.ndjson"
        with self.assertRaises(Exception) as raised:
            decode_archived_segment(self.store, receipt, clipped, max_decoded_bytes=16)
        self.assertIn("maximum", str(raised.exception))
        self.assertFalse(clipped.exists(), "a failed decode left output under its final name")

    def test_a_decode_that_fails_verification_leaves_no_file_at_the_destination(self) -> None:
        """§3.5 — nothing partially verified is exposed as trusted evidence.

        The decode itself succeeds; the logical identity is what disagrees, and
        that is only known at the end. Writing straight to the destination would
        leave a complete, correct-looking segment under the name a caller was
        told not to trust.
        """
        import dataclasses

        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        wrong = dataclasses.replace(
            receipt,
            source=LogicalIdentity(
                "0" * 64, receipt.source.byte_length, receipt.source.line_count
            ),
        )
        destination = self.root / "trusted.ndjson"
        with self.assertRaises(IdentityMismatch):
            decode_archived_segment(self.store, wrong, destination)
        self.assertFalse(destination.exists())
        self.assertEqual(list(self.root.glob(".trusted*")), [])

    def test_an_empty_segment_archives_and_decodes(self) -> None:
        """A quiet lane still seals, and what it sealed still has to survive."""
        empty = write_sealed_segment(
            self.spool, start_ns=BASE_NS + WINDOW_SECONDS * NANOSECONDS, records=0,
            segment_id="quiet000",
        )
        result = self.archiver.sweep()
        self.assertEqual(result.counts["archived"], 2)
        receipt_path = empty.with_name(empty.name[: -len(".ndjson")] + ".archive.local.json")
        receipt = read_archive_receipt(receipt_path)
        self.assertEqual(receipt.source.byte_length, 0)
        self.assertEqual(receipt.source.line_count, 0)
        self.assertGreater(receipt.data_stored.byte_length, 0)
        restored = self.root / "empty.ndjson"
        decode_archived_segment(self.store, receipt, restored)
        self.assertEqual(restored.read_bytes(), b"")


class IneligibleInputTests(ArchiveCase):
    def test_an_open_segment_is_invisible_to_the_sweep(self) -> None:
        writing = self.spool / "lane=polymarket" / "date=2026-07-30" / "20260730T003000000000-000-open0000.ndjson.open"
        writing.write_bytes(b'{"delivery_index":1}\n')
        result = self.archiver.sweep()
        self.assertEqual(result.counts["discovered"], 1)
        self.assertEqual(result.outcomes[0].data_file, self.segment.name)

    def test_a_renamed_segment_without_a_seal_is_invisible(self) -> None:
        self.seal.unlink()
        result = self.archiver.sweep()
        self.assertEqual(result.counts["discovered"], 0)
        self.assertEqual(self.receipts_on_disk(), [])

    def test_uncommitted_segments_are_counted_as_pending_not_lost(self) -> None:
        """§8.1's `pending`: not eligible, not a fault, and not zero."""
        directory = self.segment.parent
        (directory / "20260730T003000000000-000-open0000.ndjson.open").write_bytes(b"{}\n")
        (directory / "20260730T003000000000-001-unseal00.ndjson").write_bytes(b"{}\n")
        result = self.archiver.sweep()
        self.assertEqual(result.counts["discovered"], 1)
        self.assertEqual(result.counts["archived"], 1)
        self.assertEqual(result.counts["pending"], 2)

    def test_a_malformed_seal_is_an_integrity_fault_not_pending_work(self) -> None:
        self.seal.write_text("{not json", encoding="utf-8")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assert_nothing_published()

    def test_a_seal_naming_another_file_publishes_nothing(self) -> None:
        document = json.loads(self.seal.read_text(encoding="utf-8"))
        document["data_file"] = "20260730T000000000000-000-somethingelse.ndjson"
        self.seal.write_text(json.dumps(document), encoding="utf-8")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertIn("data_file", outcome.detail)
        self.assert_nothing_published()

    def test_a_seal_whose_window_disagrees_with_its_path_publishes_nothing(self) -> None:
        document = json.loads(self.seal.read_text(encoding="utf-8"))
        document["window_start_ns"] = document["window_start_ns"] + NANOSECONDS
        self.seal.write_text(json.dumps(document), encoding="utf-8")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assert_nothing_published()

    def test_a_segment_moved_out_of_its_lane_partition_publishes_nothing(self) -> None:
        elsewhere = self.spool / "lane=kalshi" / "date=2026-07-30"
        elsewhere.mkdir(parents=True)
        moved = elsewhere / self.segment.name
        moved.write_bytes(self.segment.read_bytes())
        (elsewhere / self.seal.name).write_bytes(self.seal.read_bytes())
        outcome = self.archiver.archive_segment("kalshi", moved)
        self.assertEqual(outcome.status, FAILED)
        self.assertEqual(self.receipts_on_disk(), [])

    def test_a_changed_byte_after_sealing_publishes_nothing(self) -> None:
        """Invariant 1: the seal is a claim, and compression is where it is read."""
        original = self.segment.read_bytes()
        self.segment.write_bytes(original[:-2] + b"X\n")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertIn("sha256", outcome.detail)
        self.assertEqual(self.receipts_on_disk(), [])
        self.assertTrue(self.segment.exists())

    def test_a_truncated_segment_publishes_nothing(self) -> None:
        original = self.segment.read_bytes()
        self.segment.write_bytes(original[: len(original) // 2])
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertEqual(self.receipts_on_disk(), [])


class FailingStore:
    """Wraps a store so one operation can fail the way a network does."""

    def __init__(self, inner, *, fail_put=None, fail_head=None, corrupt_head=False) -> None:
        self.inner = inner
        self.provider = inner.provider
        self.store_id = inner.store_id
        self.durability = inner.durability
        self._fail_put = fail_put
        self._fail_head = fail_head
        self._corrupt_head = corrupt_head

    def put_immutable(self, key, reader, expected_identity, **kwargs):
        if self._fail_put and self._fail_put in key:
            raise ObjectStoreError(f"transient failure uploading {key}")
        return self.inner.put_immutable(key, reader, expected_identity, **kwargs)

    def head(self, key):
        if self._fail_head and self._fail_head in key:
            raise ObjectStoreError(f"transient failure heading {key}")
        metadata = self.inner.head(key)
        if metadata is not None and self._corrupt_head:
            from dataclasses import replace

            return replace(metadata, sha256="f" * 64)
        return metadata

    def verify(self, expected):
        metadata = self.head(expected.key)
        from archive.storage.verification import verify_metadata

        return verify_metadata(metadata, expected)

    def open(self, key, **kwargs):
        return self.inner.open(key, **kwargs)


class UploadFailureTests(ArchiveCase):
    def test_a_transient_upload_failure_publishes_no_receipt(self) -> None:
        archiver = Archiver(self.spool, FailingStore(self.store, fail_put=".ndjson.zst"))
        outcome = archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assert_nothing_published()

    def test_a_seal_upload_failure_publishes_no_receipt(self) -> None:
        archiver = Archiver(self.spool, FailingStore(self.store, fail_put=".seal.json"))
        outcome = archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertEqual(self.receipts_on_disk(), [])
        # The data object may exist; the receipt does not, so nothing treats it
        # as archived and the retry re-verifies it byte for byte.
        self.assertTrue(self.segment.exists())

    def test_a_remote_verification_mismatch_publishes_no_receipt(self) -> None:
        archiver = Archiver(self.spool, FailingStore(self.store, corrupt_head=True))
        outcome = archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertEqual(self.receipts_on_disk(), [])

    def test_a_data_key_holding_different_content_is_a_fatal_conflict(self) -> None:
        segment = read_sealed_segment("polymarket", self.segment)
        data_key, _ = object_keys(segment)
        self.store.put_immutable(data_key, io.BytesIO(b"squatter"), stored_identity_of(io.BytesIO(b"squatter")))
        result = self.archiver.sweep()
        self.assertEqual(result.counts["conflicted"], 1)
        self.assertIsNotNone(result.halted)
        self.assertEqual(self.receipts_on_disk(), [])
        with self.store.open(data_key) as reader:
            self.assertEqual(reader.read(), b"squatter")

    def test_a_seal_key_holding_different_content_is_a_fatal_conflict(self) -> None:
        segment = read_sealed_segment("polymarket", self.segment)
        _, seal_key = object_keys(segment)
        self.store.put_immutable(seal_key, io.BytesIO(b"{}"), stored_identity_of(io.BytesIO(b"{}")))
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, CONFLICT)
        self.assertEqual(self.receipts_on_disk(), [])

    def test_one_bad_segment_does_not_disturb_another_segments_receipt(self) -> None:
        good = write_sealed_segment(
            self.spool, start_ns=BASE_NS + WINDOW_SECONDS * NANOSECONDS, segment_id="good0000"
        )
        self.archiver.sweep()
        good_receipt = good.with_name(good.name[: -len(".ndjson")] + ".archive.local.json")
        before = good_receipt.read_bytes()

        self.seal.write_text("{broken", encoding="utf-8")
        self.receipt_path.unlink()
        result = self.archiver.sweep()
        self.assertEqual(result.counts["failed"], 1)
        self.assertEqual(result.counts["skipped"], 1)
        self.assertEqual(good_receipt.read_bytes(), before)


class CrashWindowTests(ArchiveCase):
    """A crash at every ordered step leaves either no receipt or a valid archive."""

    def injections(self):
        def raiser(message):
            def fail(*_: object, **__: object):
                raise OSError(message)

            return fail

        return {
            "before the derivative is renamed": (
                archiver_module,
                "fsync_directory",
                raiser("directory fsync failed"),
            ),
            "during the object publish": (
                objectstore_module,
                "_link_exclusive",
                raiser("link failed"),
            ),
            "before the receipt is renamed": (
                archiver_module,
                "write_json_durable",
                raiser("receipt write failed"),
            ),
        }

    def test_no_committed_receipt_survives_a_crash_at_any_step(self) -> None:
        for index, (label, (module, attribute, failure)) in enumerate(self.injections().items()):
            with self.subTest(label):
                # A fresh spool *and* a fresh store per injection. Reusing the
                # store would let an earlier iteration's objects make a later
                # publish idempotent, and the step under test would never run.
                spool = self.root / f"crash{index}"
                segment = write_sealed_segment(spool)
                seal = segment.with_name(segment.name[: -len(".ndjson")] + ".seal.json")
                store = LocalObjectStore(self.root / f"crash-archive{index}")
                archiver = Archiver(spool, store)
                receipt_path = segment.with_name(
                    segment.name[: -len(".ndjson")] + ".archive.local.json"
                )

                original = getattr(module, attribute)
                setattr(module, attribute, failure)
                try:
                    outcome = archiver.archive_segment("polymarket", segment)
                finally:
                    setattr(module, attribute, original)
                self.assertEqual(outcome.status, FAILED)
                self.assertEqual(sorted(spool.rglob("*.archive*.json")), [])
                self.assertTrue(segment.exists())
                self.assertTrue(seal.exists())

                # And the retry produces a fully revalidatable archive.
                retry = archiver.archive_segment("polymarket", segment)
                self.assertEqual(retry.status, ARCHIVED)
                verify_archive(store, read_archive_receipt(receipt_path))

    def test_a_receipt_whose_directory_sync_failed_is_not_promoted_by_a_retry(self) -> None:
        """A failed commit must not become a commit by being read again.

        The rename lands before the directory is synced, so a failure at that
        last step leaves the marker visible while the run reports `failed`. The
        write path takes the name back, and if a crash prevented even that, the
        next run re-establishes durability before it will call the segment
        archived.
        """
        original = archiver_module.write_json_durable
        durable_original = durable_module.fsync_directory

        def rename_then_fail(path, document):
            durable_original(path.parent)  # the temporary's own directory entry
            path.write_text(json.dumps(document), encoding="utf-8")
            raise OSError("directory fsync failed")

        archiver_module.write_json_durable = rename_then_fail
        try:
            outcome = self.archiver.archive_segment("polymarket", self.segment)
        finally:
            archiver_module.write_json_durable = original
        self.assertEqual(outcome.status, FAILED)

        # The marker survived the crash this time: prove the retry syncs it
        # rather than inheriting a durability claim nothing established.
        self.assertTrue(self.receipt_path.exists())
        synced: list = []
        durable_module.fsync_directory = lambda path: synced.append(path)
        try:
            retry = self.archiver.archive_segment("polymarket", self.segment)
        finally:
            durable_module.fsync_directory = durable_original
        self.assertEqual(retry.status, SKIPPED)
        self.assertIn(self.receipt_path.parent, synced)

    def test_the_write_path_takes_back_a_marker_it_could_not_make_durable(self) -> None:
        directory_original = durable_module.fsync_directory

        def fail_on_receipt_directory(path):
            if path == self.receipt_path.parent:
                raise OSError("directory fsync failed")
            return directory_original(path)

        durable_module.fsync_directory = fail_on_receipt_directory
        try:
            outcome = self.archiver.archive_segment("polymarket", self.segment)
        finally:
            durable_module.fsync_directory = directory_original
        self.assertEqual(outcome.status, FAILED)
        self.assertFalse(
            self.receipt_path.exists(),
            "a receipt the archiver could not commit was left where a later run reads it",
        )

    def test_an_unreceipted_derivative_is_rebuilt_rather_than_trusted(self) -> None:
        self.derivative.write_bytes(b"this is not a zstd frame of anything")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, ARCHIVED)
        receipt = read_archive_receipt(self.receipt_path)
        with self.derivative.open("rb") as handle:
            self.assertTrue(stored_identity_of(handle) == receipt.data_stored)
        restored = self.root / "restored.ndjson"
        decode_archived_segment(self.store, receipt, restored)
        self.assertEqual(restored.read_bytes(), self.segment.read_bytes())


class RetryTests(ArchiveCase):
    def setUp(self) -> None:
        super().setUp()
        self.archiver.sweep()

    def test_an_existing_valid_receipt_is_idempotent(self) -> None:
        before = self.receipt_path.read_bytes()
        result = self.archiver.sweep()
        self.assertEqual(result.counts["skipped"], 1)
        self.assertEqual(self.receipt_path.read_bytes(), before)

    def test_a_corrupt_receipt_fails_closed_and_is_not_overwritten(self) -> None:
        self.receipt_path.write_text('{"local_archive_receipt_version": 1}', encoding="utf-8")
        before = self.receipt_path.read_bytes()
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertEqual(self.receipt_path.read_bytes(), before)

    def test_a_receipt_whose_object_vanished_fails_closed(self) -> None:
        receipt = read_archive_receipt(self.receipt_path)
        (Path(self.store.root) / receipt.data_key).unlink()
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)
        self.assertIn("absent", outcome.detail)

    def test_a_receipt_whose_object_was_mutated_fails_closed(self) -> None:
        receipt = read_archive_receipt(self.receipt_path)
        path = Path(self.store.root) / receipt.data_key
        path.write_bytes(path.read_bytes() + b"tampered")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)

    def test_a_receipt_that_describes_a_different_source_fails_closed(self) -> None:
        document = json.loads(self.receipt_path.read_text(encoding="utf-8"))
        document["source"]["sha256"] = "a" * 64
        self.receipt_path.write_text(json.dumps(document), encoding="utf-8")
        outcome = self.archiver.archive_segment("polymarket", self.segment)
        self.assertEqual(outcome.status, FAILED)

    def test_a_local_receipt_renamed_to_the_production_name_is_refused(self) -> None:
        """§5.3 — a conformance receipt must not become deletion authority."""
        production_name = self.receipt_path.with_name(
            self.receipt_path.name.replace(".archive.local.json", ".archive.json")
        )
        production_name.write_bytes(self.receipt_path.read_bytes())
        with self.assertRaises(ReceiptError):
            read_archive_receipt(production_name)


class ReceiptPathSafetyTests(ArchiveCase):
    """S3 adapter Gate 0, finding 1: `source.file`/`seal.file` are bare names.

    The reaper resolves both against the directory a receipt was discovered in
    (`archive/reaper/service.py`), so a value carrying a path separator or traversal
    component would let a malformed receipt point that resolution outside its
    own directory.
    """

    durability = INDEPENDENT

    def setUp(self) -> None:
        super().setUp()
        self.archiver.sweep()
        self.document = json.loads(self.receipt_path.read_text(encoding="utf-8"))

    def rewrite(self) -> None:
        self.receipt_path.write_text(json.dumps(self.document), encoding="utf-8")

    def test_a_source_file_containing_a_slash_is_refused(self) -> None:
        self.document["source"]["file"] = "../outside.ndjson"
        self.rewrite()
        with self.assertRaises(ReceiptError):
            read_archive_receipt(self.receipt_path)

    def test_a_source_file_containing_a_backslash_is_refused(self) -> None:
        self.document["source"]["file"] = "sub\\dir.ndjson"
        self.rewrite()
        with self.assertRaises(ReceiptError):
            read_archive_receipt(self.receipt_path)

    def test_a_seal_file_naming_a_path_is_refused(self) -> None:
        self.document["seal"]["file"] = "sub/dir.seal.json"
        self.rewrite()
        with self.assertRaises(ReceiptError):
            read_archive_receipt(self.receipt_path)

    def test_source_and_seal_files_must_share_one_segment_stem(self) -> None:
        self.document["seal"]["file"] = "totally-different-stem.seal.json"
        self.rewrite()
        with self.assertRaises(ReceiptError):
            read_archive_receipt(self.receipt_path)


class ProductionReceiptTests(ArchiveCase):
    """An explicitly authorized backend writes the normative receipt."""

    durability = INDEPENDENT

    def test_the_receipt_carries_the_normative_production_fields(self) -> None:
        self.archiver.sweep()
        document = json.loads(self.receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(document["archive_receipt_version"], 2)
        self.assertNotIn("local_archive_receipt_version", document)
        self.assertEqual(document["store"], {
            "provider": self.store.provider,
            "location": self.store.store_id,
        })
        self.assertEqual(document["object"]["content_encoding"], "zstd")
        self.assertEqual(document["compression"], {
            "algorithm": "zstd",
            "level": 3,
            "frame_checksum": True,
            "dictionary": None,
            "frame_count": 1,
            "encoder": document["compression"]["encoder"],
        })
        self.assertTrue(document["compression"]["encoder"].startswith("python-zstandard/"))
        self.assertIn("provider_checksum", document["object"])
        self.assertIn("provider_checksum_algorithm", document["object"])
        receipt = read_archive_receipt(self.receipt_path)
        self.assertTrue(receipt.is_production)
        verify_archive(self.store, receipt)

    def test_a_provider_checksum_that_disagrees_with_its_own_digest_is_refused(self) -> None:
        self.archiver.sweep()
        document = json.loads(self.receipt_path.read_text(encoding="utf-8"))
        document["object"]["provider_checksum"] = "AAAA"
        self.receipt_path.write_text(json.dumps(document), encoding="utf-8")
        receipt = read_archive_receipt(self.receipt_path)
        with self.assertRaises(VerificationError):
            verify_archive(self.store, receipt)


class ReceiptLocationTests(ArchiveCase):
    """S3 adapter Gate 0, finding 2: a receipt verifies only against its bucket."""

    durability = INDEPENDENT

    def test_a_receipt_cannot_verify_against_a_differently_configured_store(self) -> None:
        self.archiver.sweep()
        receipt = read_archive_receipt(self.receipt_path)
        # Same physical bytes at the same keys, wrapped in a store declared under
        # a different name — exactly the "restored into the wrong bucket" case
        # the check exists for.
        impostor = LocalObjectStore(
            self.store.root, store_id="a-differently-configured-bucket", durability=INDEPENDENT
        )
        with self.assertRaises(VerificationError):
            verify_archive(impostor, receipt)


class ProductionVerificationTighteningTests(ArchiveCase):
    """§10 — a production receipt also commits to requested content metadata."""

    durability = INDEPENDENT

    def setUp(self) -> None:
        super().setUp()
        self.archiver.sweep()
        self.receipt = read_archive_receipt(self.receipt_path)

    def _rewrite_metadata(self, key: str, content_type, content_encoding) -> None:
        import json as _json

        meta_path = Path(self.store.root) / ".objectmeta" / (key + ".json")
        meta_path.write_text(
            _json.dumps({"content_type": content_type, "content_encoding": content_encoding}),
            encoding="utf-8",
        )

    def test_a_data_content_type_that_disagrees_with_the_contract_is_refused(self) -> None:
        self._rewrite_metadata(self.receipt.data_key, "application/octet-stream", "zstd")
        with self.assertRaises(VerificationError):
            verify_archive(self.store, self.receipt)

    def test_a_data_content_encoding_that_disagrees_with_the_contract_is_refused(self) -> None:
        self._rewrite_metadata(self.receipt.data_key, "application/x-ndjson", "identity")
        with self.assertRaises(VerificationError):
            verify_archive(self.store, self.receipt)

    def test_a_seal_content_type_that_disagrees_with_the_contract_is_refused(self) -> None:
        self._rewrite_metadata(self.receipt.seal_key, "text/plain", None)
        with self.assertRaises(VerificationError):
            verify_archive(self.store, self.receipt)

    def test_a_seal_content_encoding_that_should_be_null_is_refused(self) -> None:
        self._rewrite_metadata(self.receipt.seal_key, "application/json", "identity")
        with self.assertRaises(VerificationError):
            verify_archive(self.store, self.receipt)


class DailyManifestTests(ArchiveCase):
    durability = INDEPENDENT

    def setUp(self) -> None:
        super().setUp()
        write_sealed_segment(
            self.spool,
            start_ns=BASE_NS + WINDOW_SECONDS * NANOSECONDS,
            segment_id="bbbb2222",
        )
        write_sealed_segment(self.spool, lane="kalshi", segment_id="cccc3333")
        self.archiver.sweep()
        self.manifest_root = self.root / "manifests"

    def build(self):
        return build_daily_manifests(
            self.store, discover_archive_receipts(self.spool), kind=PRODUCTION
        )

    def test_a_manifest_holds_every_verified_receipt_in_the_specified_order(self) -> None:
        result = self.build()
        self.assertEqual(len(result.manifests), 1)
        manifest = result.manifests[0]
        self.assertEqual(manifest.date, "2026-07-30")
        self.assertEqual(len(manifest.entries), 3)
        keys = [(entry["window_start_ns"], entry["lane"]) for entry in manifest.entries]
        # (window_start_ns, lane rank, segment index, segment id): polymarket
        # ranks ahead of kalshi inside the first window.
        self.assertEqual(
            keys,
            [
                (BASE_NS, "polymarket"),
                (BASE_NS, "kalshi"),
                (BASE_NS + WINDOW_SECONDS * NANOSECONDS, "polymarket"),
            ],
        )

    def test_a_manifest_is_a_pure_function_of_its_receipts(self) -> None:
        write_daily_manifests(self.manifest_root, self.build())
        path = self.manifest_root / "date=2026-07-30" / "manifest.json"
        first = path.read_bytes()
        write_daily_manifests(self.manifest_root, self.build())
        self.assertEqual(path.read_bytes(), first)

    def test_a_receipt_whose_object_is_gone_is_excluded_and_named(self) -> None:
        receipt = read_archive_receipt(
            next(iter(discover_archive_receipts(self.spool)))
        )
        (Path(self.store.root) / receipt.data_key).unlink()
        result = self.build()
        self.assertEqual(len(result.manifests[0].entries), 2)
        self.assertEqual(len(result.excluded), 1)
        self.assertIn("absent", result.excluded[0])

    def test_a_transient_object_store_error_excludes_and_continues(self) -> None:
        """S3 adapter Gate 0, finding 4 — a flaky head must not abort the rebuild."""
        wrapped = FailingStore(self.store, fail_head="ndjson.zst")
        result = build_daily_manifests(
            wrapped, discover_archive_receipts(self.spool), kind=PRODUCTION
        )
        self.assertEqual(result.manifests, [])
        self.assertEqual(len(result.excluded), 3)
        for message in result.excluded:
            self.assertIn("transient failure heading", message)

    def test_a_manifest_rebuilds_after_a_reaping_without_the_local_source(self) -> None:
        """§6.4 — after the source is gone, the receipt and the store are all there is."""
        before = self.build()
        for path in self.spool.rglob("*.ndjson"):
            path.unlink()
        for path in self.spool.rglob("*.seal.json"):
            path.unlink()
        after = self.build()
        self.assertEqual(
            [manifest.as_record() for manifest in before.manifests],
            [manifest.as_record() for manifest in after.manifests],
        )
        self.assertEqual(after.excluded, [])

    def test_a_local_conformance_receipt_is_ignored_by_a_production_manifest(self) -> None:
        conformance_spool = self.root / "spool2"
        segment = write_sealed_segment(conformance_spool)
        conformance_store = LocalObjectStore(self.root / "archive2")
        Archiver(conformance_spool, conformance_store).sweep()
        self.assertTrue(
            (segment.with_name(segment.name[: -len(".ndjson")] + ".archive.local.json")).exists()
        )
        self.assertEqual(discover_archive_receipts(conformance_spool, kind=PRODUCTION), [])

    def test_a_stale_manifest_is_removed_when_its_date_has_zero_valid_receipts(self) -> None:
        """S3 adapter Gate 0, finding 3 — a rebuild must not advertise what it excluded."""
        write_daily_manifests(self.manifest_root, self.build())
        path = self.manifest_root / "date=2026-07-30" / "manifest.json"
        self.assertTrue(path.is_file())

        # Every receipt for that date now fails to verify against the store.
        for data_object in (Path(self.store.root) / "raw").rglob("*.ndjson.zst"):
            data_object.unlink()

        result = self.build()
        self.assertEqual(result.manifests, [])
        self.assertEqual(len(result.excluded), 3)
        written = write_daily_manifests(self.manifest_root, result)
        self.assertFalse(path.exists(), "the stale manifest was left advertising excluded objects")
        self.assertEqual(written.removed, [path])


if __name__ == "__main__":
    unittest.main()
