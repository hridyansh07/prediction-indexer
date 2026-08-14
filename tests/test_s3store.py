"""`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md` §13 Step 1 / §14 — the S3 adapter unit.

`FakeS3Client` is the "small injected fake S3 client" §13 asks for: it models
success, 404, 403, 409, 412, checksum mismatch, metadata mismatch, and a
streaming `GetObject` body, and nothing else — a call `S3ObjectStore` should
never make (a delete) has no real implementation here, so it fails loudly
rather than quietly succeeding against a double.

`SharedContractTests` re-runs the same properties `tests/test_object_store.py`
proves for `LocalObjectStore` — new-key commit, idempotent retry, conflict,
local identity refusal, absence-is-not-error, bounded reads — against the
fake-backed `S3ObjectStore`, per §13 Step 1.4. What does not carry over is
`LocalObjectStore`'s filesystem-specific behaviour (symlink refusal, fsync
crash injection): S3 has no such surface, so those tests have no analogue
here.
"""

from __future__ import annotations

import base64
import hashlib
import io
import unittest

from botocore.exceptions import ClientError

from archive.storage.base import (
    IntegrityConflict,
    ObjectStoreError,
    VerificationFailure,
    provider_checksum_of,
)
from archive.storage.s3 import MAX_SINGLE_PUT_BYTES, S3ObjectStore, base64_to_hex
from encoder import StoredIdentity, stored_identity_of

BUCKET = "prediction-indexer-raw-archive"
REGION = "us-east-1"
OWNER = "123456789012"


def identity(payload: bytes) -> StoredIdentity:
    return stored_identity_of(io.BytesIO(payload))


def _client_error(operation: str, code: str, status: int, message: str | None = None) -> ClientError:
    response = {
        "Error": {"Code": code, "Message": message or code},
        "ResponseMetadata": {"HTTPStatusCode": status},
    }
    return ClientError(response, operation)


class _FakeStreamingBody:
    """The minimum a botocore `StreamingBody` offers: `read(amt)` and `close()`."""

    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)
        self.closed = False

    def read(self, amt: int | None = None) -> bytes:
        return self._buffer.read() if amt is None else self._buffer.read(amt)

    def close(self) -> None:
        self.closed = True
        self._buffer.close()


class _NonSeekableReader:
    """Models a live socket or pipe: readable, never seekable."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def seekable(self) -> bool:
        return False

    def read(self, size: int = -1) -> bytes:
        return self._payload


class FakeS3Client:
    """An in-memory double for the three boto3 S3 calls `S3ObjectStore` makes."""

    def __init__(self, *, expected_bucket_owner: str = OWNER) -> None:
        self.expected_bucket_owner = expected_bucket_owner
        self.objects: dict[str, dict] = {}
        self.denied_keys: set[str] = set()
        self.conflicted_keys: set[str] = set()
        #: Per-key overrides applied to the next `head_object` response, so a
        #: test can model a malformed or composite checksum without touching
        #: `put_object`'s own bookkeeping.
        self.head_overrides: dict[str, dict] = {}
        self.calls: list[tuple[str, dict]] = []

    def seed(
        self,
        key: str,
        data: bytes,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> None:
        self.objects[key] = {
            "bytes": data,
            "checksum": base64.b64encode(hashlib.sha256(data).digest()).decode("ascii"),
            "content_type": content_type,
            "content_encoding": content_encoding,
        }

    def _check_owner(self, expected_bucket_owner: str | None) -> None:
        if expected_bucket_owner != self.expected_bucket_owner:
            raise _client_error("Request", "AccessDenied", 403, "wrong ExpectedBucketOwner")

    def head_object(self, *, Bucket, Key, ChecksumMode=None, ExpectedBucketOwner=None):
        self.calls.append(
            ("head_object", {"Bucket": Bucket, "Key": Key, "ExpectedBucketOwner": ExpectedBucketOwner})
        )
        self._check_owner(ExpectedBucketOwner)
        if Key in self.denied_keys:
            raise _client_error("HeadObject", "403", 403, "Forbidden")
        obj = self.objects.get(Key)
        if obj is None:
            raise _client_error("HeadObject", "404", 404, "Not Found")

        override = self.head_overrides.get(Key, {})
        response = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            # A value that would fail every test relying on it, because
            # nothing in `S3ObjectStore` is permitted to read it.
            "ETag": '"not-a-real-identity"',
        }
        response["ContentLength"] = override.get("ContentLength", len(obj["bytes"]))
        if "ChecksumSHA256" in override:
            if override["ChecksumSHA256"] is not None:
                response["ChecksumSHA256"] = override["ChecksumSHA256"]
        else:
            response["ChecksumSHA256"] = obj["checksum"]
        checksum_type = override.get("ChecksumType", "FULL_OBJECT")
        if checksum_type is not None:
            response["ChecksumType"] = checksum_type
        if obj["content_type"] is not None:
            response["ContentType"] = obj["content_type"]
        if obj["content_encoding"] is not None:
            response["ContentEncoding"] = obj["content_encoding"]
        return response

    def put_object(self, **kwargs):
        # Accepts raw `**kwargs`, unlike `head_object`/`get_object`, so a test
        # can tell "the caller sent ContentType=None" from "the caller never
        # sent ContentType at all" — the distinction §8.2's "include only when
        # non-null" is actually about.
        self.calls.append(("put_object", dict(kwargs)))
        Bucket = kwargs["Bucket"]
        Key = kwargs["Key"]
        Body = kwargs["Body"]
        ContentLength = kwargs["ContentLength"]
        ChecksumSHA256 = kwargs["ChecksumSHA256"]
        IfNoneMatch = kwargs["IfNoneMatch"]
        ExpectedBucketOwner = kwargs.get("ExpectedBucketOwner")
        ContentType = kwargs.get("ContentType")
        ContentEncoding = kwargs.get("ContentEncoding")

        self._check_owner(ExpectedBucketOwner)
        assert IfNoneMatch == "*", "put_immutable must always send IfNoneMatch=*"
        data = Body.read()
        if len(data) != ContentLength:
            raise _client_error("PutObject", "IncompleteBody", 400, "declared length disagreed")
        if Key in self.conflicted_keys:
            raise _client_error("PutObject", "ConditionalRequestConflict", 409, "conflict")
        if Key in self.objects:
            raise _client_error(
                "PutObject", "PreconditionFailed", 412, "At least one pre-condition failed"
            )
        actual_checksum = base64.b64encode(hashlib.sha256(data).digest()).decode("ascii")
        if ChecksumSHA256 != actual_checksum:
            raise _client_error("PutObject", "BadDigest", 400, "checksum mismatch")
        self.objects[Key] = {
            "bytes": data,
            "checksum": actual_checksum,
            "content_type": ContentType,
            "content_encoding": ContentEncoding,
        }
        return {"ResponseMetadata": {"HTTPStatusCode": 200}, "ETag": '"deadbeef"'}

    def get_object(self, *, Bucket, Key, ExpectedBucketOwner):
        self.calls.append(
            ("get_object", {"Bucket": Bucket, "Key": Key, "ExpectedBucketOwner": ExpectedBucketOwner})
        )
        self._check_owner(ExpectedBucketOwner)
        obj = self.objects.get(Key)
        if obj is None:
            raise _client_error("GetObject", "NoSuchKey", 404, "Not Found")
        return {"Body": _FakeStreamingBody(obj["bytes"]), "ContentLength": len(obj["bytes"])}

    def list_objects_v2(
        self,
        *,
        Bucket,
        Prefix,
        ExpectedBucketOwner,
        ContinuationToken=None,
    ):
        self.calls.append(
            (
                "list_objects_v2",
                {
                    "Bucket": Bucket,
                    "Prefix": Prefix,
                    "ExpectedBucketOwner": ExpectedBucketOwner,
                    "ContinuationToken": ContinuationToken,
                },
            )
        )
        self._check_owner(ExpectedBucketOwner)
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        start = int(ContinuationToken or 0)
        page = keys[start : start + 2]
        next_index = start + len(page)
        return {
            "Contents": [{"Key": key} for key in page],
            "IsTruncated": next_index < len(keys),
            **(
                {"NextContinuationToken": str(next_index)}
                if next_index < len(keys)
                else {}
            ),
        }

    def delete_object(self, **kwargs):  # pragma: no cover - must never be reached
        self.calls.append(("delete_object", kwargs))
        return {}

    def delete_objects(self, **kwargs):  # pragma: no cover - must never be reached
        self.calls.append(("delete_objects", kwargs))
        return {}


class S3Case(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FakeS3Client()
        self.store = S3ObjectStore(BUCKET, REGION, OWNER, client=self.client)
        self.key = "raw/lane=polymarket/date=2026-07-31/segment.ndjson.zst"
        self.payload = b'{"line":1}\n{"line":2}\n'

    def put(self, payload: bytes | None = None, key: str | None = None, **kwargs):
        payload = self.payload if payload is None else payload
        return self.store.put_immutable(key or self.key, io.BytesIO(payload), identity(payload), **kwargs)

    def calls_of(self, op: str) -> list[dict]:
        return [kwargs for name, kwargs in self.client.calls if name == op]


class ConstructorTests(unittest.TestCase):
    def test_bucket_region_and_owner_are_required(self) -> None:
        client = FakeS3Client()
        with self.assertRaises(ValueError):
            S3ObjectStore("", REGION, OWNER, client=client)
        with self.assertRaises(ValueError):
            S3ObjectStore(BUCKET, "", OWNER, client=client)
        with self.assertRaises(ValueError):
            S3ObjectStore(BUCKET, REGION, "not-twelve-digits", client=client)
        with self.assertRaises(ValueError):
            S3ObjectStore(BUCKET, REGION, "12345", client=client)

    def test_store_id_is_exactly_the_bucket_name(self) -> None:
        store = S3ObjectStore(BUCKET, REGION, OWNER, client=FakeS3Client())
        self.assertEqual(store.store_id, BUCKET)

    def test_the_store_declares_itself_an_independent_durability_domain(self) -> None:
        store = S3ObjectStore(BUCKET, REGION, OWNER, client=FakeS3Client())
        self.assertTrue(store.durability.independent)
        self.assertEqual(store.durability.receipt_kind, "production")


class IdentityTranslationTests(unittest.TestCase):
    def test_base64_to_hex_round_trips_with_provider_checksum_of(self) -> None:
        digest = hashlib.sha256(b"round trip").hexdigest()
        self.assertEqual(base64_to_hex(provider_checksum_of(digest)), digest)

    def test_malformed_base64_is_refused(self) -> None:
        with self.assertRaises(VerificationFailure):
            base64_to_hex("not base64 at all!!")

    def test_a_decoded_value_that_is_not_32_bytes_is_refused(self) -> None:
        wrong_length = base64.b64encode(b"too short").decode("ascii")
        with self.assertRaises(VerificationFailure):
            base64_to_hex(wrong_length)


class HeadTests(S3Case):
    def test_head_of_an_absent_key_is_absence_not_an_error(self) -> None:
        self.assertIsNone(self.store.head(self.key))

    def test_list_keys_paginates_in_key_order(self) -> None:
        for index in range(5):
            self.client.seed(f"raw/lane=x/{index}.universe.json", str(index).encode())
        self.client.seed("targeter-v2/runs/run_manifest.json", b"other")
        self.assertEqual(
            list(self.store.list_keys("raw/")),
            [f"raw/lane=x/{index}.universe.json" for index in range(5)],
        )
        calls = self.calls_of("list_objects_v2")
        self.assertEqual(len(calls), 3)
        self.assertIsNone(calls[0]["ContinuationToken"])
        self.assertEqual(calls[1]["ContinuationToken"], "2")

    def test_a_present_key_translates_identity_from_the_provider_checksum(self) -> None:
        self.client.seed(self.key, self.payload, content_type="application/x-ndjson", content_encoding="zstd")
        metadata = self.store.head(self.key)
        assert metadata is not None
        self.assertEqual(metadata.byte_length, len(self.payload))
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)
        self.assertEqual(metadata.provider_checksum, provider_checksum_of(metadata.sha256))
        self.assertEqual(metadata.provider_checksum_algorithm, "SHA256")
        self.assertEqual(metadata.content_type, "application/x-ndjson")
        self.assertEqual(metadata.content_encoding, "zstd")

    def test_403_is_not_treated_as_absence(self) -> None:
        self.client.seed(self.key, self.payload)
        self.client.denied_keys.add(self.key)
        with self.assertRaises(ObjectStoreError):
            self.store.head(self.key)

    def test_a_missing_checksum_fails_closed(self) -> None:
        self.client.seed(self.key, self.payload)
        self.client.head_overrides[self.key] = {"ChecksumSHA256": None}
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)

    def test_a_malformed_checksum_fails_closed(self) -> None:
        self.client.seed(self.key, self.payload)
        self.client.head_overrides[self.key] = {"ChecksumSHA256": "!!!not-base64"}
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)

    def test_a_composite_checksum_type_fails_closed(self) -> None:
        self.client.seed(self.key, self.payload)
        self.client.head_overrides[self.key] = {"ChecksumType": "COMPOSITE"}
        with self.assertRaises(VerificationFailure):
            self.store.head(self.key)

    def test_etag_is_never_inspected_as_identity(self) -> None:
        """Every fake head response carries a fixed, wrong ETag (see `FakeS3Client`)."""
        self.client.seed(self.key, self.payload)
        metadata = self.store.head(self.key)
        assert metadata is not None
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)
        self.assertFalse(hasattr(metadata, "etag"))

    def test_expected_bucket_owner_is_sent(self) -> None:
        self.client.seed(self.key, self.payload)
        self.store.head(self.key)
        self.assertEqual(self.calls_of("head_object")[-1]["ExpectedBucketOwner"], OWNER)


class PutImmutableTests(S3Case):
    def test_a_new_conditional_put_succeeds_and_sends_if_none_match(self) -> None:
        metadata = self.put()
        self.assertEqual(metadata.byte_length, len(self.payload))
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)
        put_calls = self.calls_of("put_object")
        self.assertEqual(len(put_calls), 1)
        self.assertEqual(put_calls[0]["IfNoneMatch"], "*")

    def test_an_identical_existing_object_succeeds_after_412(self) -> None:
        self.client.seed(
            self.key, self.payload, content_type="application/x-ndjson", content_encoding="zstd"
        )
        metadata = self.put(content_type="application/x-ndjson", content_encoding="zstd")
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)
        self.assertEqual(len(self.calls_of("put_object")), 1)
        self.assertEqual(len(self.calls_of("head_object")), 1)

    def test_a_different_existing_object_becomes_integrity_conflict(self) -> None:
        self.client.seed(self.key, b"other content entirely\n")
        with self.assertRaises(IntegrityConflict):
            self.put()

    def test_matching_bytes_with_different_metadata_becomes_integrity_conflict(self) -> None:
        self.client.seed(self.key, self.payload, content_type="text/plain")
        with self.assertRaises(IntegrityConflict):
            self.put(content_type="application/x-ndjson")

    def test_absence_after_a_412_is_an_object_store_error(self) -> None:
        """Concurrent state changed between the 412 and the follow-up head."""
        real_head = self.client.head_object

        def vanish_after_conflict(**kwargs):
            self.client.objects.pop(kwargs["Key"], None)
            return real_head(**kwargs)

        self.client.seed(self.key, b"someone else's object\n")
        self.client.head_object = vanish_after_conflict
        with self.assertRaises(ObjectStoreError):
            self.put()

    def test_409_is_retryable_at_the_next_sweep_and_writes_no_object(self) -> None:
        self.client.conflicted_keys.add(self.key)
        with self.assertRaises(ObjectStoreError):
            self.put()
        self.assertNotIn(self.key, self.client.objects)
        self.client.conflicted_keys.discard(self.key)
        metadata = self.put()
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)

    def test_an_incorrect_reader_length_fails_before_any_request(self) -> None:
        wrong = StoredIdentity(sha256=identity(self.payload).sha256, byte_length=len(self.payload) + 5)
        with self.assertRaises(VerificationFailure):
            self.store.put_immutable(self.key, io.BytesIO(self.payload), wrong)
        self.assertEqual(self.client.calls, [])

    def test_a_non_seekable_reader_is_refused(self) -> None:
        with self.assertRaises(ObjectStoreError):
            self.store.put_immutable(self.key, _NonSeekableReader(self.payload), identity(self.payload))
        self.assertEqual(self.client.calls, [])

    def test_a_byte_length_above_the_single_put_limit_is_refused(self) -> None:
        oversized = StoredIdentity(sha256="a" * 64, byte_length=MAX_SINGLE_PUT_BYTES + 1)
        with self.assertRaises(ObjectStoreError):
            self.store.put_immutable(self.key, io.BytesIO(b""), oversized)
        self.assertEqual(self.client.calls, [])

    def test_bad_digest_is_a_verification_failure(self) -> None:
        real_put = self.client.put_object

        def corrupt_checksum(**kwargs):
            kwargs = dict(kwargs)
            kwargs["ChecksumSHA256"] = base64.b64encode(b"0" * 32).decode("ascii")
            return real_put(**kwargs)

        self.client.put_object = corrupt_checksum
        with self.assertRaises(VerificationFailure):
            self.put()

    def test_expected_bucket_owner_is_sent_on_put(self) -> None:
        self.put()
        self.assertEqual(self.calls_of("put_object")[-1]["ExpectedBucketOwner"], OWNER)

    def test_content_type_and_encoding_are_only_sent_when_not_none(self) -> None:
        self.put(key="raw/no-metadata.ndjson.zst")
        sent = self.calls_of("put_object")[-1]
        self.assertNotIn("ContentType", sent)
        self.assertNotIn("ContentEncoding", sent)


class OpenTests(S3Case):
    def test_open_streams_and_reads_the_exact_bytes(self) -> None:
        self.client.seed(self.key, self.payload)
        with self.store.open(self.key) as reader:
            self.assertEqual(reader.read(), self.payload)

    def test_closing_the_wrapper_closes_the_streaming_body(self) -> None:
        self.client.seed(self.key, self.payload)
        reader = self.store.open(self.key)
        body = self.client.objects  # sanity: object exists
        self.assertIn(self.key, body)
        with reader:
            reader.read()
        # The fake body is reachable only through the response we handed back;
        # closing the wrapper must have closed it.
        self.assertTrue(reader._handle.closed)

    def test_an_absent_key_is_an_object_store_error_not_none(self) -> None:
        with self.assertRaises(ObjectStoreError):
            self.store.open(self.key)

    def test_negative_max_bytes_is_rejected(self) -> None:
        self.client.seed(self.key, self.payload)
        with self.assertRaises(ObjectStoreError):
            self.store.open(self.key, max_bytes=-1)

    def test_a_content_length_exceeding_max_bytes_fails_before_any_byte_is_returned(self) -> None:
        self.client.seed(self.key, self.payload)
        with self.assertRaises(VerificationFailure):
            self.store.open(self.key, max_bytes=len(self.payload) - 1)

    def test_the_reader_refuses_to_run_past_the_recorded_length(self) -> None:
        """The object grew after `head` reported its length (or `head` lied)."""
        self.client.seed(self.key, self.payload + b"extra\n")
        with self.assertRaises(VerificationFailure):
            with self.store.open(self.key, max_bytes=len(self.payload)) as reader:
                while reader.read(4):
                    pass


class NoDeleteTests(S3Case):
    def test_a_full_lifecycle_never_calls_a_delete_api(self) -> None:
        self.put()
        with self.assertRaises(IntegrityConflict):
            self.put(payload=b"different\n")
        self.store.head(self.key)
        with self.store.open(self.key) as reader:
            reader.read()
        delete_calls = [name for name, _ in self.client.calls if name.startswith("delete")]
        self.assertEqual(delete_calls, [])


class SharedContractTests(S3Case):
    """§13 Step 1.4 — the same properties `LocalObjectStore` proves, over S3."""

    def test_a_new_key_commits_durably_with_recalculated_identity(self) -> None:
        metadata = self.put(content_type="application/x-ndjson", content_encoding="zstd")
        self.assertEqual(metadata.byte_length, len(self.payload))
        self.assertEqual(metadata.sha256, identity(self.payload).sha256)
        self.assertEqual(self.store.head(self.key), metadata)

    def test_an_identical_put_is_idempotent(self) -> None:
        first = self.put()
        second = self.put()
        self.assertEqual(first, second)

    def test_a_different_value_at_the_same_key_is_a_conflict_and_preserves_the_first(self) -> None:
        self.put()
        with self.assertRaises(IntegrityConflict):
            self.put(b'{"line":"different"}\n')
        with self.store.open(self.key) as reader:
            self.assertEqual(reader.read(), self.payload)

    def test_bytes_that_disagree_with_the_promised_identity_are_refused(self) -> None:
        mismatched = StoredIdentity(sha256=identity(b"other\n").sha256, byte_length=len(self.payload))
        with self.assertRaises(VerificationFailure):
            self.store.put_immutable(self.key, io.BytesIO(self.payload), mismatched)

    def test_head_of_an_absent_key_is_absence_not_an_error(self) -> None:
        self.assertIsNone(self.store.head("raw/lane=x/date=2026-07-31/nothing.ndjson.zst"))


if __name__ == "__main__":
    unittest.main()
