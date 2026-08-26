from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from archive import ArchivedCanonicalByteStreamer, ArchivedSegmentByteStreamer
from archive.archiver import Archiver
from archive.archiver.canonical import (
    CanonicalArchiver,
    read_canonical_archive_receipt,
)
from archive.common.receipts import ReceiptError, read_archive_receipt
from archive.common.verify import verify_archive
from archive.storage.base import (
    IntegrityConflict,
    ObjectExpectation,
    ObjectStoreError,
    VerificationFailure,
)
from archive.storage.gcs import (
    BYTE_LENGTH_METADATA,
    SHA256_METADATA,
    GCSObjectStore,
    _CRC32C,
)
from encoder import StoredIdentity
from tests.archive_fixtures import write_canonical_receipt, write_sealed_segment


class FakeError(Exception):
    def __init__(self, code: int):
        self.code = code


def crc(data: bytes) -> str:
    value = _CRC32C()
    value.update(data)
    return value.base64()


def identity(data: bytes) -> StoredIdentity:
    return StoredIdentity(hashlib.sha256(data).hexdigest(), len(data))


class FakeBlob:
    def __init__(self, client, name, generation=None, chunk_size=None):
        self.client, self.name, self.requested_generation = client, name, generation
        self.content_type = self.content_encoding = None
        self.metadata = None

    def reload(self):
        obj = self.client.objects.get(self.name)
        if obj is None:
            raise FakeError(404)
        self.generation, self.size, self.crc32c = (
            obj["generation"],
            obj["size"],
            obj["crc"],
        )
        self.metageneration = obj.get("metageneration", 1)
        self.metadata = obj["metadata"]
        self.content_type, self.content_encoding = obj["type"], obj["encoding"]

    def open(self, mode, **kwargs):
        self.client.opens.append((self.name, self.requested_generation, mode, kwargs))
        if mode == "wb":
            self.client.upload_kwargs = kwargs
            if self.name in self.client.objects:
                raise FakeError(412)
            return FakeWriter(self)
        obj = self.client.objects.get(self.name)
        if obj is None or obj["generation"] != self.requested_generation:
            raise FakeError(404)
        return io.BytesIO(obj["data"])


class FakeWriter(io.BytesIO):
    def __init__(self, blob):
        super().__init__()
        self.blob = blob

    def close(self):
        if not self.closed:
            data = self.getvalue()
            self.blob.client.next_generation += 1
            self.blob.client.objects[self.blob.name] = dict(
                data=data,
                size=len(data),
                crc=crc(data),
                generation=self.blob.client.next_generation,
                metageneration=1,
                metadata=self.blob.metadata,
                type=self.blob.content_type,
                encoding=self.blob.content_encoding,
            )
        super().close()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            io.BytesIO.close(self)
            return False
        self.close()
        return False


class FakeBucket:
    def __init__(self, client):
        self.client = client

    def blob(self, name, **kwargs):
        return FakeBlob(self.client, name, **kwargs)


class FakeClient:
    def __init__(self):
        self.objects, self.next_generation, self.opens = {}, 0, []
        self.upload_kwargs = None

    def bucket(self, name):
        return FakeBucket(self)

    def list_blobs(self, bucket, prefix):
        return [
            type("Listed", (), {"name": key})()
            for key in sorted(self.objects)
            if key.startswith(prefix)
        ]

    def seed(self, key, data, **metadata):
        self.next_generation += 1
        self.objects[key] = dict(
            data=data,
            size=len(data),
            crc=crc(data),
            generation=self.next_generation,
            metageneration=1,
            metadata={
                SHA256_METADATA: identity(data).sha256,
                BYTE_LENGTH_METADATA: str(len(data)),
            },
            type=metadata.get("content_type"),
            encoding=metadata.get("content_encoding"),
        )


class GCSStoreTests(unittest.TestCase):
    def setUp(self):
        self.client, self.key, self.data = FakeClient(), "raw/a.zst", b"stored bytes\n"
        self.store = GCSObjectStore("archive-bucket", client=self.client)

    def put(self, data=None, **kwargs):
        data = self.data if data is None else data
        return self.store.put_immutable(
            self.key, io.BytesIO(data), identity(data), **kwargs
        )

    def test_attributes_and_conditional_crc_validated_put(self):
        result = self.put(content_type="application/x-ndjson", content_encoding="zstd")
        self.assertEqual(
            (self.store.provider, self.store.store_id), ("gcs", "archive-bucket")
        )
        self.assertTrue(self.store.durability.independent)
        self.assertEqual(self.client.upload_kwargs["if_generation_match"], 0)
        self.assertEqual(self.client.upload_kwargs["checksum"], "crc32c")
        self.assertEqual(self.client.opens[0][3]["chunk_size"], 1024 * 1024)
        self.assertEqual([entry[2] for entry in self.client.opens], ["wb"])
        self.assertEqual(
            self.client.objects[self.key]["metadata"],
            {
                SHA256_METADATA: identity(self.data).sha256,
                BYTE_LENGTH_METADATA: str(len(self.data)),
            },
        )
        self.assertEqual(result.sha256, identity(self.data).sha256)
        self.assertEqual(
            (result.provider_checksum, result.provider_checksum_algorithm),
            (crc(self.data), "CRC32C"),
        )

    def test_verify_uses_provider_metadata_without_downloading_the_object(self):
        self.client.seed(self.key, self.data)
        metadata = self.store.head(self.key)
        assert metadata is not None
        expected = ObjectExpectation(
            metadata.key,
            metadata.stored,
            metadata.provider_checksum,
            metadata.provider_checksum_algorithm,
            metadata.content_type,
            metadata.content_encoding,
        )
        self.client.opens.clear()

        result = self.store.verify(expected)

        self.assertEqual(result.sha256, identity(self.data).sha256)
        self.assertEqual(self.client.opens, [])

    def test_verify_rejects_receipt_drift_without_downloading_the_object(self):
        self.client.seed(self.key, self.data)
        metadata = self.store.head(self.key)
        assert metadata is not None
        expected = ObjectExpectation(
            metadata.key,
            metadata.stored,
            metadata.provider_checksum,
            metadata.provider_checksum_algorithm,
            metadata.content_type,
            metadata.content_encoding,
        )
        self.client.objects[self.key]["crc"] = crc(b"different")
        self.client.opens.clear()

        with self.assertRaises(VerificationFailure):
            self.store.verify(expected)

        self.assertEqual(self.client.opens, [])

    def test_head_absence_and_errors(self):
        self.assertIsNone(self.store.head(self.key))
        original = FakeBlob.reload
        FakeBlob.reload = lambda _: (_ for _ in ()).throw(FakeError(403))
        try:
            self.assertRaises(ObjectStoreError, self.store.head, self.key)
        finally:
            FakeBlob.reload = original

    def test_head_rejects_malformed_provider_crc_or_length(self):
        self.client.seed(self.key, self.data)
        self.client.objects[self.key]["crc"] = "not-base64"
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)
        self.client.objects[self.key]["crc"] = crc(self.data)
        self.client.objects[self.key]["size"] = -1
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)

    def test_head_rejects_invalid_identity_metadata(self):
        self.client.seed(self.key, self.data)
        self.client.objects[self.key]["metadata"][SHA256_METADATA] = "invalid"
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)
        self.client.objects[self.key]["metadata"] = {
            SHA256_METADATA: identity(self.data).sha256,
            BYTE_LENGTH_METADATA: str(len(self.data) + 1),
        }
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)

    def test_unreceipted_retry_reads_back_and_rejects_generation_change(self):
        self.client.seed(self.key, self.data)
        original = FakeBlob.open

        def replace_after_open(blob, mode, **kwargs):
            handle = original(blob, mode, **kwargs)
            blob.client.objects[blob.name]["generation"] += 1
            return handle

        FakeBlob.open = replace_after_open
        try:
            with self.assertRaises(VerificationFailure):
                self.put()
        finally:
            FakeBlob.open = original

    def test_unreceipted_retry_reads_back_and_rejects_metadata_change(self):
        self.client.seed(self.key, self.data)
        original = FakeBlob.open

        def change_metadata_after_open(blob, mode, **kwargs):
            handle = original(blob, mode, **kwargs)
            blob.client.objects[blob.name]["metageneration"] += 1
            return handle

        FakeBlob.open = change_metadata_after_open
        try:
            with self.assertRaises(VerificationFailure):
                self.put()
        finally:
            FakeBlob.open = original

    def test_unreceipted_identical_retry_keeps_full_readback(self):
        self.client.seed(self.key, self.data)
        self.client.opens.clear()

        self.put()

        reads = [entry for entry in self.client.opens if entry[2] == "rb"]
        self.assertEqual(len(reads), 1)
        self.assertEqual(
            reads[0][1], self.client.objects[self.key]["generation"]
        )
        self.assertTrue(reads[0][3]["raw_download"])

    def test_existing_identical_is_idempotent_but_differences_conflict(self):
        first = self.put()
        self.assertEqual(self.put(), first)
        with self.assertRaises(IntegrityConflict):
            self.put(b"different")
        with self.assertRaises(IntegrityConflict):
            self.put(content_type="text/plain")

    def test_open_is_generation_pinned_and_bounded(self):
        self.client.seed(self.key, self.data)
        with self.store.open(self.key, max_bytes=len(self.data)) as reader:
            self.assertEqual(reader.read(), self.data)
        with self.assertRaises(VerificationFailure):
            self.store.open(self.key, max_bytes=2)

    def test_list_keys_returns_live_matching_keys(self):
        self.client.seed("raw/b", b"b")
        self.client.seed("raw/a", b"a")
        self.client.seed("other/a", b"x")
        self.assertEqual(list(self.store.list_keys("raw/")), ["raw/a", "raw/b"])

    def test_reader_identity_is_verified_during_upload_and_nonseekable_rejected(self):
        with self.assertRaises(VerificationFailure):
            self.store.put_immutable(
                self.key, io.BytesIO(self.data), identity(b"wrong bytes\n")
            )

        class Reader(io.BytesIO):
            def seekable(self):
                return False

        with self.assertRaises(ObjectStoreError):
            self.store.put_immutable(self.key, Reader(self.data), identity(self.data))
        self.assertEqual(self.client.objects, {})

    def test_upload_failure_leaves_no_object(self):
        original = FakeBlob.open

        def fail_upload(blob, mode, **kwargs):
            if mode == "wb":
                raise FakeError(503)
            return original(blob, mode, **kwargs)

        FakeBlob.open = fail_upload
        try:
            with self.assertRaises(ObjectStoreError):
                self.put()
        finally:
            FakeBlob.open = original
        self.assertEqual(self.client.objects, {})

    def test_server_checksum_rejection_publishes_no_object_metadata(self):
        original = FakeBlob.open

        def reject_checksum(blob, mode, **kwargs):
            if mode == "wb":
                raise FakeError(400)
            return original(blob, mode, **kwargs)

        FakeBlob.open = reject_checksum
        try:
            with self.assertRaisesRegex(VerificationFailure, "checksum"):
                self.put()
        finally:
            FakeBlob.open = original
        self.assertEqual(self.client.objects, {})


class GCSArchivePipelineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.client = FakeClient()
        self.store = GCSObjectStore("archive-bucket", client=self.client)

    def test_raw_archiver_writes_a_provider_neutral_production_receipt(self):
        spool = self.root / "spool"
        segment = write_sealed_segment(spool)
        outcome = Archiver(spool, self.store).archive_segment("polymarket", segment)
        self.assertEqual(outcome.status, "archived")
        receipt = read_archive_receipt(outcome.receipt_path)
        self.assertEqual(receipt.provider, "gcs")
        self.assertEqual(receipt.provider_checksum_algorithm, "CRC32C")
        self.assertEqual(receipt.seal_provider_checksum_algorithm, "CRC32C")
        document = json.loads(outcome.receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(document["archive_receipt_version"], 2)
        self.assertEqual(
            document["store"], {"provider": "gcs", "location": "archive-bucket"}
        )
        self.assertEqual(
            [entry[2] for entry in self.client.opens], ["wb", "wb"]
        )

        self.client.opens.clear()
        verify_archive(self.store, receipt)
        self.assertEqual(self.client.opens, [])

        repeated = Archiver(spool, self.store).archive_segment("polymarket", segment)
        self.assertEqual(repeated.status, "skipped")
        self.assertEqual(self.client.opens, [])

        document["unexpected"] = True
        outcome.receipt_path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(ReceiptError):
            read_archive_receipt(outcome.receipt_path)

    def test_raw_retrieval_reads_the_selected_gcs_object_once(self):
        spool = self.root / "spool"
        segment = write_sealed_segment(spool)
        outcome = Archiver(spool, self.store).archive_segment("polymarket", segment)
        receipt = read_archive_receipt(outcome.receipt_path)
        streamer = ArchivedSegmentByteStreamer(
            self.store, (receipt,), temp_root=self.root
        )
        logical_key = next(
            key for key in streamer.object_keys() if key.endswith(".ndjson")
        )
        self.client.opens.clear()

        self.assertEqual(
            b"".join(streamer.iter_bytes(logical_key)), segment.read_bytes()
        )
        reads = [
            entry
            for entry in self.client.opens
            if entry[0] == receipt.data_key and entry[2] == "rb"
        ]
        self.assertEqual(len(reads), 1)

    def test_canonical_archiver_writes_a_provider_neutral_production_receipt(self):
        canonical = self.root / "canonical"
        source = write_canonical_receipt(canonical, evidence_lines=1)
        outcome = CanonicalArchiver(canonical, self.store).archive_window(source)
        self.assertEqual(outcome.status, "archived")
        document = json.loads(outcome.receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(document["canonical_archive_receipt_version"], 2)
        self.assertEqual(
            document["store"], {"provider": "gcs", "location": "archive-bucket"}
        )
        for field in ("evidence", "provenance", "canonical_receipt"):
            self.assertEqual(document[field]["provider_checksum_algorithm"], "CRC32C")
        self.assertEqual(
            [entry[2] for entry in self.client.opens], ["wb", "wb", "wb"]
        )

        self.client.opens.clear()
        repeated = CanonicalArchiver(canonical, self.store).archive_window(source)
        self.assertEqual(repeated.status, "skipped")
        self.assertEqual(self.client.opens, [])

        receipt = read_canonical_archive_receipt(outcome.receipt_path)
        streamer = ArchivedCanonicalByteStreamer(
            self.store, (receipt,), temp_root=self.root
        )
        evidence_key = next(
            key for key in streamer.object_keys() if key.endswith("/evidence.ndjson")
        )
        self.client.opens.clear()
        self.assertEqual(
            b"".join(streamer.iter_bytes(evidence_key)), b'{"canonical":0}\n'
        )
        evidence_reads = [
            entry
            for entry in self.client.opens
            if entry[0] == receipt.evidence.key and entry[2] == "rb"
        ]
        self.assertEqual(len(evidence_reads), 1)


if __name__ == "__main__":
    unittest.main()
