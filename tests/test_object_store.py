"""§9.2 — the immutable object boundary.

These are the tests an S3 adapter will have to pass unchanged. That is the point
of `LocalObjectStore`: if immutability, idempotent retry, conflict detection and
crash-window behaviour are only ever exercised against a real bucket, they are
exercised in code review.
"""

from __future__ import annotations

from dataclasses import replace
import io
import os
import tempfile
import unittest
from pathlib import Path

from archive.storage import local as objectstore
from archive.storage import (
    CONFORMANCE,
    INDEPENDENT,
    IntegrityConflict,
    LocalObjectStore,
    ObjectExpectation,
    ObjectKeyError,
    ObjectStoreError,
    VerificationFailure,
    normalize_key,
    provider_checksum_of,
    verify_objects,
)
from encoder import stored_identity_of


def identity(payload: bytes):
    return stored_identity_of(io.BytesIO(payload))


class KeyNormalizationTests(unittest.TestCase):
    def test_a_normal_key_survives_unchanged(self) -> None:
        key = "raw/lane=polymarket/date=2026-07-31/20260731T000000000000-000-abcd.ndjson.zst"
        self.assertEqual(normalize_key(key), key)

    def test_traversal_and_absolute_and_empty_components_are_refused(self) -> None:
        for key in (
            "",
            "/absolute/key",
            "raw//double",
            "raw/./here",
            "raw/../../escape",
            "..",
            "raw\\windows",
            "raw/nul\x00",
            ".objectmeta/raw/thing",
        ):
            with self.subTest(key), self.assertRaises(ObjectKeyError):
                normalize_key(key)


class ImmutablePutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = LocalObjectStore(Path(self.directory.name) / "archive")
        self.key = "raw/lane=polymarket/date=2026-07-31/segment.ndjson.zst"
        self.payload = b'{"line":1}\n{"line":2}\n'

    def put(self, payload: bytes | None = None, key: str | None = None):
        payload = self.payload if payload is None else payload
        return self.store.put_immutable(
            key or self.key,
            io.BytesIO(payload),
            identity(payload),
            content_type="application/x-ndjson",
            content_encoding="zstd",
        )

    def test_a_new_key_commits_durably_with_recalculated_identity(self) -> None:
        metadata = self.put()
        self.assertEqual(metadata.byte_length, len(self.payload))
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)
        self.assertEqual(
            metadata.provider_checksum, provider_checksum_of(metadata.sha256)
        )
        self.assertEqual(metadata.provider_checksum_algorithm, "SHA256")
        self.assertEqual(metadata.content_type, "application/x-ndjson")
        self.assertEqual(metadata.content_encoding, "zstd")
        self.assertEqual(self.store.head(self.key), metadata)

    def test_an_identical_put_is_idempotent_and_does_not_rewrite(self) -> None:
        first = self.put()
        path = Path(self.store.root) / self.key
        before = path.stat().st_ino, path.stat().st_mtime_ns
        second = self.put()
        self.assertEqual(first, second)
        self.assertEqual((path.stat().st_ino, path.stat().st_mtime_ns), before)

    def test_matching_bytes_with_different_metadata_are_a_conflict(self) -> None:
        self.put()
        with self.assertRaises(IntegrityConflict):
            self.store.put_immutable(
                self.key,
                io.BytesIO(self.payload),
                identity(self.payload),
                content_type="text/plain",
                content_encoding="identity",
            )

    def test_matching_bytes_with_missing_metadata_are_repaired_for_a_retry(
        self,
    ) -> None:
        self.store.put_immutable(
            self.key, io.BytesIO(self.payload), identity(self.payload)
        )
        repaired = self.put()
        self.assertEqual(repaired.content_type, "application/x-ndjson")
        self.assertEqual(repaired.content_encoding, "zstd")
        self.assertEqual(self.store.head(self.key), repaired)

    def test_structurally_invalid_attributes_fail_as_an_object_store_error(
        self,
    ) -> None:
        self.put()
        attributes = self.store._metadata_path(self.key)
        attributes.write_text("[]\n", encoding="utf-8")
        with self.assertRaises(ObjectStoreError):
            self.store.head(self.key)

    def test_a_concurrent_same_byte_publish_with_different_metadata_is_a_conflict(
        self,
    ) -> None:
        original = objectstore._link_exclusive

        def publish_the_other_request(temporary: Path, final: Path) -> None:
            original(temporary, final)
            self.store._write_attributes(self.key, "text/plain", "identity")
            raise FileExistsError

        objectstore._link_exclusive = publish_the_other_request
        try:
            with self.assertRaises(IntegrityConflict):
                self.put()
        finally:
            objectstore._link_exclusive = original

    def test_post_publication_verification_rejects_wrong_metadata(self) -> None:
        original = self.store._write_attributes

        def write_wrong_attributes(key: str, *_: object) -> None:
            original(key, "text/plain", "identity")

        self.store._write_attributes = write_wrong_attributes
        with self.assertRaises(VerificationFailure):
            self.put()

    def test_a_different_value_at_the_same_key_is_a_conflict_and_preserves_the_first(
        self,
    ) -> None:
        self.put()
        with self.assertRaises(IntegrityConflict):
            self.put(b'{"line":"different"}\n')
        with self.store.open(self.key) as reader:
            self.assertEqual(reader.read(), self.payload)

    def test_bytes_that_disagree_with_the_promised_identity_are_refused(self) -> None:
        with self.assertRaises(VerificationFailure):
            self.store.put_immutable(
                self.key, io.BytesIO(b"other\n"), identity(self.payload)
            )
        self.assertIsNone(self.store.head(self.key))
        self.assertEqual(self.temporaries(), [])

    def test_head_of_an_absent_key_is_absence_not_an_error(self) -> None:
        self.assertIsNone(
            self.store.head("raw/lane=x/date=2026-07-31/nothing.ndjson.zst")
        )

    def test_verify_checks_one_complete_receipt_expectation(self) -> None:
        metadata = self.put()
        expected = ObjectExpectation(
            metadata.key,
            metadata.stored,
            metadata.provider_checksum,
            metadata.provider_checksum_algorithm,
            metadata.content_type,
            metadata.content_encoding,
        )
        self.assertEqual(self.store.verify(expected), metadata)

        failures = (
            replace(expected, stored=identity(b"different")),
            replace(expected, provider_checksum="AAAA"),
            replace(expected, provider_checksum_algorithm="CRC32C"),
            replace(expected, content_type="application/json"),
            replace(expected, content_encoding=None),
        )
        for mismatch in failures:
            with self.subTest(mismatch=mismatch), self.assertRaises(
                VerificationFailure
            ):
                self.store.verify(mismatch)

        with self.assertRaisesRegex(VerificationFailure, "absent"):
            self.store.verify(replace(expected, key="raw/absent.ndjson.zst"))

    def test_verify_objects_preserves_receipt_order(self) -> None:
        first = self.put()
        second_key = "raw/lane=polymarket/date=2026-07-31/seal.json"
        second_payload = b'{"sealed":true}\n'
        second = self.store.put_immutable(
            second_key,
            io.BytesIO(second_payload),
            identity(second_payload),
            content_type="application/json",
        )
        expectations = tuple(
            ObjectExpectation(
                item.key,
                item.stored,
                item.provider_checksum,
                item.provider_checksum_algorithm,
                item.content_type,
                item.content_encoding,
            )
            for item in (first, second)
        )
        self.assertEqual(verify_objects(self.store, expectations), (first, second))

    def test_head_detects_post_write_mutation_rather_than_echoing_metadata(
        self,
    ) -> None:
        self.put()
        path = Path(self.store.root) / self.key
        os.chmod(path, 0o644)
        with path.open("ab") as handle:
            handle.write(b'{"appended":true}\n')
        mutated = self.store.head(self.key)
        assert mutated is not None
        self.assertNotEqual(mutated.sha256, identity(self.payload).sha256)
        self.assertEqual(mutated.byte_length, path.stat().st_size)

    def test_head_fails_when_an_object_changes_while_it_is_being_read(self) -> None:
        """A writer finishing during the read window must not be reported as truth."""
        self.put()
        path = Path(self.store.root) / self.key
        original = objectstore._hash_file

        def mutate_after_hashing(target: Path, buffer_bytes: int):
            result = original(target, buffer_bytes)
            with path.open("ab") as writer:
                writer.write(b"x")
            return result

        objectstore._hash_file = mutate_after_hashing
        try:
            with self.assertRaises(VerificationFailure):
                self.store.head(self.key)
        finally:
            objectstore._hash_file = original

    def test_head_fails_closed_when_an_object_disappears_while_being_read(self) -> None:
        self.put()
        path = Path(self.store.root) / self.key
        original = objectstore._hash_file

        def remove_after_hashing(target: Path, buffer_bytes: int):
            result = original(target, buffer_bytes)
            path.unlink()
            return result

        objectstore._hash_file = remove_after_hashing
        try:
            with self.assertRaises(VerificationFailure):
                self.store.head(self.key)
        finally:
            objectstore._hash_file = original

    def test_a_symlinked_key_is_refused_rather_than_followed(self) -> None:
        outside = Path(self.directory.name) / "outside.ndjson"
        outside.write_bytes(b"not mine\n")
        planted = Path(self.store.root) / "raw" / "linked.ndjson.zst"
        planted.parent.mkdir(parents=True, exist_ok=True)
        planted.symlink_to(outside)
        with self.assertRaises((ObjectStoreError, ObjectKeyError)):
            self.store.head("raw/linked.ndjson.zst")

    def test_a_key_reaching_through_a_symlinked_directory_is_refused(self) -> None:
        outside = Path(self.directory.name) / "elsewhere"
        outside.mkdir()
        (Path(self.store.root) / "raw").mkdir(parents=True, exist_ok=True)
        (Path(self.store.root) / "raw" / "escape").symlink_to(outside)
        with self.assertRaises(ObjectKeyError):
            self.put(key="raw/escape/segment.ndjson.zst")

    def test_the_open_reader_refuses_to_run_past_the_recorded_length(self) -> None:
        self.put()
        path = Path(self.store.root) / self.key
        with path.open("ab") as handle:
            handle.write(b"extra\n")
        with self.assertRaises(VerificationFailure):
            with self.store.open(self.key, max_bytes=len(self.payload)) as reader:
                while reader.read(8):
                    pass

    def test_verified_read_streams_once_and_requires_complete_consumption(self) -> None:
        metadata = self.put()
        expected = ObjectExpectation(
            metadata.key,
            metadata.stored,
            metadata.provider_checksum,
            metadata.provider_checksum_algorithm,
            metadata.content_type,
            metadata.content_encoding,
        )
        with self.store.open_verified(expected) as reader:
            self.assertEqual(b"".join(reader), self.payload)
        with self.assertRaisesRegex(VerificationFailure, "not consumed to EOF"):
            with self.store.open_verified(expected) as reader:
                reader.read(1)

    def test_verified_read_rejects_bytes_that_drifted_from_the_receipt(self) -> None:
        metadata = self.put()
        expected = ObjectExpectation(
            metadata.key,
            metadata.stored,
            metadata.provider_checksum,
            metadata.provider_checksum_algorithm,
            metadata.content_type,
            metadata.content_encoding,
        )
        path = Path(self.store.root) / self.key
        path.write_bytes(b"x" * len(self.payload))

        with self.assertRaises(VerificationFailure):
            with self.store.open_verified(expected) as reader:
                while reader.read(8):
                    pass

    def temporaries(self) -> list[str]:
        return sorted(
            path.name
            for path in Path(self.store.root).rglob("*")
            if path.is_file() and path.name.endswith(".open")
        )


class _BrokenReader:
    """A source that dies partway through, as a stalled upload would."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self._served = False

    def read(self, size: int = -1) -> bytes:
        if self._served:
            raise OSError("source stream failed")
        self._served = True
        return self._payload[:1]


class CrashWindowTests(unittest.TestCase):
    """§9.2 — a failure at any step leaves no partially committed final key."""

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "archive"
        self.key = "raw/lane=kalshi/date=2026-07-31/segment.ndjson.zst"
        self.payload = b'{"line":1}\n'

    def attempt(self, reader: io.BytesIO | None = None) -> None:
        store = LocalObjectStore(self.root)
        store.put_immutable(
            self.key, reader or io.BytesIO(self.payload), identity(self.payload)
        )

    def test_an_injected_failure_at_every_step_leaves_no_final_key(self) -> None:
        def failing(message: str):
            def raise_it(*_: object) -> None:
                raise OSError(message)

            return raise_it

        steps = {
            # The bytes themselves fail to arrive: the source dies mid-stream.
            "write": (None, None),
            "fsync file": ("_fsync_file", failing("fsync failed")),
            "link": ("_link_exclusive", failing("link failed")),
            "directory fsync": ("_fsync_directory", failing("directory fsync failed")),
        }
        for label, (attribute, failure) in steps.items():
            with self.subTest(label):
                original = getattr(objectstore, attribute) if attribute else None
                if attribute:
                    setattr(objectstore, attribute, failure)
                try:
                    with self.assertRaises((ObjectStoreError, OSError)):
                        self.attempt(None if attribute else _BrokenReader(self.payload))
                finally:
                    if attribute:
                        setattr(objectstore, attribute, original)

                if label == "directory fsync":
                    # The name is linked before the directory entry is durable,
                    # so it may exist — but nothing has reported it committed,
                    # and a retry re-verifies it byte for byte.
                    store = LocalObjectStore(self.root)
                    published = store.head(self.key)
                    if published is not None:
                        self.assertTrue(published.matches(identity(self.payload)))
                else:
                    store = LocalObjectStore(self.root)
                    self.assertIsNone(store.head(self.key))
                    self.assertEqual(
                        [path.name for path in self.root.rglob("*.open")],
                        [],
                        "a temporary object survived the failure",
                    )
                for path in sorted(self.root.rglob("*")):
                    if path.is_file():
                        path.unlink()

    def test_an_idempotent_retry_re_establishes_the_directory_durability(self) -> None:
        """A key can exist *because* the sync that would make it durable failed.

        The first attempt linked the name and then failed the directory fsync,
        so it reported failure and nothing acted on it. A retry that returns
        success without syncing would be promoting a name a crash can still
        take — the failure would have been converted into a commit by nothing
        more than being looked at twice.
        """
        original = objectstore._fsync_directory
        published = self.root / self.key

        def fail_once_the_name_exists(path):
            # Only the sync that would make the *link* durable fails; the ones
            # that build the directory chain still work, which is what puts the
            # store in the state this test is about.
            if published.exists():
                raise OSError("crash")
            return original(path)

        objectstore._fsync_directory = fail_once_the_name_exists
        try:
            with self.assertRaises(OSError):
                self.attempt()
        finally:
            objectstore._fsync_directory = original

        store = LocalObjectStore(self.root)
        self.assertIsNotNone(store.head(self.key))

        synced: list = []
        objectstore._fsync_directory = lambda path: synced.append(path)
        try:
            self.attempt()
        finally:
            objectstore._fsync_directory = original
        self.assertTrue(
            synced, "the retry accepted an existing key without syncing its directory"
        )

    def test_a_retry_after_a_crash_republishes_the_same_object_idempotently(
        self,
    ) -> None:
        original = objectstore._fsync_directory
        objectstore._fsync_directory = lambda *_: (_ for _ in ()).throw(
            OSError("crash")
        )
        try:
            with self.assertRaises(OSError):
                self.attempt()
        finally:
            objectstore._fsync_directory = original
        self.attempt()
        store = LocalObjectStore(self.root)
        published = store.head(self.key)
        assert published is not None
        self.assertTrue(published.matches(identity(self.payload)))


class DurabilityClassTests(unittest.TestCase):
    def test_a_local_store_is_conformance_only_unless_told_otherwise(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(Path(directory) / "archive")
            self.assertEqual(store.durability, CONFORMANCE)
            self.assertFalse(store.durability.independent)
            self.assertEqual(store.durability.receipt_kind, "local")

    def test_independence_is_configured_explicitly_and_changes_the_receipt_kind(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = LocalObjectStore(
                Path(directory) / "archive", durability=INDEPENDENT
            )
            self.assertTrue(store.durability.independent)
            self.assertEqual(store.durability.receipt_kind, "production")


if __name__ == "__main__":
    unittest.main()
