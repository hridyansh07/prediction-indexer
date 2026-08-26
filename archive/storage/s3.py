"""AWS S3 implementation of `ObjectStore` (`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md`).

`Archiver` and `Reaper` depend only on the `ObjectStore` protocol, so this
module adds no methods to either — it is a second implementation of the same
small contract `LocalObjectStore` already makes durable and testable without
cloud credentials:

```text
put_immutable(key, reader, expected_identity) -> ObjectMetadata
head(key)                                     -> ObjectMetadata | None
open(key)                                     -> bounded byte reader
open_verified(expectation)                    -> verified byte reader
list_keys(prefix)                             -> immutable key iterator
```

**Every write is a conditional `PutObject`.** `IfNoneMatch="*"` is what makes a
key written once: S3 refuses the request outright if the key already exists,
so immutability is enforced by the service rather than by a check-then-act
race in this process. A `412` from that refusal is not treated as failure — it
is resolved by heading the key and comparing identities, because the object
that already owns it may be this exact retry.

**Nothing here trusts the upload response for identity.** `head` always
reissues a fresh, checksum-enabled `HeadObject` — after a new upload, after a
`412`, and whenever a caller asks what is stored — because the interesting
failures (a concurrent writer, a bucket eventually settling) are exactly the
ones a cached response cannot see. §6 fixes the one identity translation this
adds over the local adapter: S3's `ChecksumSHA256` is base64 of the raw
32-byte digest, never hex and never the multipart-aware `ETag`.

No method here calls an S3 delete API. Raw deletion is `archive/reaper/service.py`'s
authority alone, and V1 does not extend it to S3 (`archive/S3_RAW_ARCHIVE_ADAPTER_V1.md`
§2).
"""

from __future__ import annotations

import base64
import binascii
import hashlib
from typing import Any, BinaryIO, Iterator

from botocore.exceptions import BotoCoreError, ClientError

from archive.storage.base import (
    INDEPENDENT,
    BoundedReader,
    IntegrityConflict,
    ObjectExpectation,
    ObjectMetadata,
    ObjectStoreError,
    VerificationFailure,
    VerifiedReader,
    normalize_key,
    provider_checksum_of,
)
from archive.storage.verification import consume_verified, match_metadata
from encoder import StoredIdentity

__all__ = [
    "MAX_SINGLE_PUT_BYTES",
    "S3ObjectStore",
    "base64_to_hex",
]

#: §3's conservative single-`PutObject` ceiling. Crossing it is a hard failure
#: for the caller to handle: write no receipt, leave the local segment alone.
#: Multipart upload is out of scope for V1.
MAX_SINGLE_PUT_BYTES = 5_000_000_000

_FULL_OBJECT_CHECKSUM_TYPE = "FULL_OBJECT"


def base64_to_hex(value: str) -> str:
    """The strict inverse of `provider_checksum_of`: base64 -> lowercase hex.

    Rejects malformed base64 and any decoded value that is not exactly 32
    bytes, because a 31- or 33-byte decode is not a SHA-256 whatever else is
    true about it (§6).
    """
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise VerificationFailure(f"{value!r} is not valid base64") from error
    if len(decoded) != 32:
        raise VerificationFailure(
            f"decoded checksum is {len(decoded)} bytes, not the 32 a SHA-256 digest requires"
        )
    return decoded.hex()


def _is_twelve_digit_account_id(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 12 and value.isdigit()


def _error_code_and_status(error: ClientError) -> tuple[str, int | None]:
    response = getattr(error, "response", None) or {}
    code = response.get("Error", {}).get("Code", "")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return code, status


def _validate_expected_identity(identity: StoredIdentity) -> None:
    sha256 = identity.sha256
    if (
        not isinstance(sha256, str)
        or len(sha256) != 64
        or sha256 != sha256.lower()
        or any(character not in "0123456789abcdef" for character in sha256)
    ):
        raise ObjectStoreError(
            f"expected sha256 {sha256!r} is not a lowercase 64-character hex digest"
        )
    byte_length = identity.byte_length
    if (
        not isinstance(byte_length, int)
        or isinstance(byte_length, bool)
        or byte_length < 0
    ):
        raise ObjectStoreError(
            f"expected byte_length {byte_length!r} is not a non-negative integer"
        )


class S3ObjectStore:
    """`ObjectStore` over one AWS S3 general purpose bucket.

    `client` is dependency injection for tests only (§5): production
    configuration goes through `store_factory.py`, which never exposes an
    arbitrary endpoint URL, and the default client resolves credentials
    through boto3's normal provider chain.
    """

    provider = "s3"

    #: Fixed for V1 (§3): an S3 backend is always an independent durability
    #: domain, never inferred, never downgraded by configuration.
    durability = INDEPENDENT

    def __init__(
        self,
        bucket: str,
        region: str,
        expected_bucket_owner: str,
        client: Any = None,
    ) -> None:
        if not isinstance(bucket, str) or not bucket:
            raise ValueError("bucket is required")
        if not isinstance(region, str) or not region:
            raise ValueError("region is required")
        if not _is_twelve_digit_account_id(expected_bucket_owner):
            raise ValueError(
                f"expected_bucket_owner must be a 12-digit AWS account id, got "
                f"{expected_bucket_owner!r}"
            )
        self.bucket = bucket
        self.region = region
        self.expected_bucket_owner = expected_bucket_owner
        # The production receipt field is named `bucket`, and `Archiver` fills
        # it from `store.store_id` — so `store_id` has to be exactly the
        # bucket name for a receipt to name what it actually wrote to (§5).
        self.store_id = bucket
        self._client = client if client is not None else _default_client(region)

    # -- protocol ------------------------------------------------------------

    def head(self, key: str) -> ObjectMetadata | None:
        normalized = normalize_key(key)
        try:
            response = self._client.head_object(
                Bucket=self.bucket,
                Key=normalized,
                ChecksumMode="ENABLED",
                ExpectedBucketOwner=self.expected_bucket_owner,
            )
        except ClientError as error:
            code, status = _error_code_and_status(error)
            if status == 404 or code in ("404", "NoSuchKey"):
                return None
            if status == 403 or code in ("403", "Forbidden", "AccessDenied"):
                # §7: the role holds `s3:ListBucket`, so a missing key is a 404
                # and a 403 here is a configuration or authorization failure,
                # never absence.
                raise ObjectStoreError(
                    f"heading {normalized}: access denied ({error})"
                ) from error
            raise ObjectStoreError(f"heading {normalized}: {error}") from error
        except BotoCoreError as error:
            raise ObjectStoreError(f"heading {normalized}: {error}") from error
        return self._metadata_from_head(normalized, response)

    def verify_metadata(self, expected: ObjectExpectation) -> ObjectMetadata:
        return match_metadata(self.head(expected.key), expected)

    def verify(self, expected: ObjectExpectation) -> None:
        consume_verified(self, expected)

    def put_immutable(
        self,
        key: str,
        reader: BinaryIO,
        expected_identity: StoredIdentity,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> ObjectMetadata:
        normalized = normalize_key(key)
        _validate_expected_identity(expected_identity)
        if expected_identity.byte_length > MAX_SINGLE_PUT_BYTES:
            raise ObjectStoreError(
                f"{normalized}: {expected_identity.byte_length} bytes exceeds the V1 single-PUT "
                f"limit of {MAX_SINGLE_PUT_BYTES} bytes; multipart upload is out of scope"
            )
        if not reader.seekable():
            raise ObjectStoreError(
                f"{normalized}: put_immutable requires a seekable reader"
            )

        # An O(1) file operation, not a read: it proves the reader holds
        # exactly the promised byte count before one byte is uploaded, and
        # restores the position the caller handed in (§8.1).
        start = reader.tell()
        reader.seek(0, 2)
        remaining = reader.tell() - start
        reader.seek(start)
        if remaining != expected_identity.byte_length:
            raise VerificationFailure(
                f"{normalized}: the reader has {remaining} bytes remaining, but the caller "
                f"promised {expected_identity.byte_length}"
            )

        expected_checksum = provider_checksum_of(expected_identity.sha256)
        put_kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Key": normalized,
            "Body": reader,
            "ContentLength": expected_identity.byte_length,
            "ChecksumAlgorithm": "SHA256",
            "ChecksumSHA256": expected_checksum,
            "IfNoneMatch": "*",
            "ExpectedBucketOwner": self.expected_bucket_owner,
        }
        if content_type is not None:
            put_kwargs["ContentType"] = content_type
        if content_encoding is not None:
            put_kwargs["ContentEncoding"] = content_encoding

        try:
            self._client.put_object(**put_kwargs)
        except ClientError as error:
            code, status = _error_code_and_status(error)
            if status == 412 or code == "PreconditionFailed":
                return self._resolve_precondition_failure(
                    normalized, expected_identity, content_type, content_encoding
                )
            if status == 409 or code == "ConditionalRequestConflict":
                # The hourly sweep retries from a newly opened reader; this
                # call must not retry a partially consumed one (§8.3).
                raise ObjectStoreError(
                    f"{normalized}: conditional write conflict (409); retry at the next sweep "
                    "from a newly opened reader"
                ) from error
            if code in ("BadDigest", "InvalidDigest"):
                raise VerificationFailure(
                    f"{normalized}: S3 rejected the declared checksum ({code})"
                ) from error
            raise ObjectStoreError(
                f"{normalized}: PutObject failed: {error}"
            ) from error
        except BotoCoreError as error:
            raise ObjectStoreError(
                f"{normalized}: PutObject failed: {error}"
            ) from error

        metadata = self.head(normalized)
        if metadata is None or not self._matches_request(
            metadata, expected_identity, content_type, content_encoding
        ):
            raise VerificationFailure(
                f"{normalized}: the fresh head after a successful PutObject does not match the "
                "identity or metadata this upload requested"
            )
        return metadata

    def open(self, key: str, *, max_bytes: int | None = None) -> BoundedReader:
        if max_bytes is not None and max_bytes < 0:
            raise ObjectStoreError(f"max_bytes must not be negative, got {max_bytes}")
        normalized = normalize_key(key)
        try:
            response = self._client.get_object(
                Bucket=self.bucket,
                Key=normalized,
                ExpectedBucketOwner=self.expected_bucket_owner,
            )
        except (ClientError, BotoCoreError) as error:
            # Unlike `head`, a 404 here is a fault: the caller asked to open a
            # specific key, so its absence is not folded into any other status.
            raise ObjectStoreError(
                f"{normalized}: GetObject failed: {error}"
            ) from error

        body = response["Body"]
        content_length = response.get("ContentLength")
        if (
            max_bytes is not None
            and isinstance(content_length, int)
            and content_length > max_bytes
        ):
            body.close()
            raise VerificationFailure(
                f"{normalized}: GetObject reports {content_length} bytes, exceeding the "
                f"{max_bytes} byte limit before any byte was returned"
            )
        return BoundedReader(body, normalized, max_bytes)

    def open_verified(self, expected: ObjectExpectation) -> VerifiedReader:
        normalized = normalize_key(expected.key)
        expected_checksum = provider_checksum_of(expected.stored.sha256)
        if (
            expected.provider_checksum_algorithm != "SHA256"
            or expected.provider_checksum != expected_checksum
        ):
            raise VerificationFailure(
                f"{normalized}: receipt has invalid S3 SHA256 checksum evidence"
            )
        try:
            response = self._client.get_object(
                Bucket=self.bucket,
                Key=normalized,
                ChecksumMode="ENABLED",
                ExpectedBucketOwner=self.expected_bucket_owner,
            )
        except ClientError as error:
            code, status = _error_code_and_status(error)
            if status == 404 or code in ("404", "NoSuchKey"):
                raise VerificationFailure(
                    f"{normalized}: receipted S3 object is absent"
                ) from error
            raise ObjectStoreError(
                f"{normalized}: GetObject failed: {error}"
            ) from error
        except BotoCoreError as error:
            raise ObjectStoreError(
                f"{normalized}: GetObject failed: {error}"
            ) from error

        body = response["Body"]
        actual = (
            response.get("ContentLength"),
            response.get("ChecksumSHA256"),
            response.get("ContentType"),
            response.get("ContentEncoding"),
        )
        wanted = (
            expected.stored.byte_length,
            expected.provider_checksum,
            expected.content_type,
            expected.content_encoding,
        )
        checksum_type = response.get("ChecksumType")
        if actual != wanted or checksum_type not in (None, _FULL_OBJECT_CHECKSUM_TYPE):
            body.close()
            raise VerificationFailure(
                f"{normalized}: GetObject metadata disagrees with its receipt"
            )
        return VerifiedReader(
            body,
            expected,
            provider_hasher=hashlib.sha256(),
        )

    def list_keys(self, prefix: str) -> Iterator[str]:
        """Yield every key below a prefix with strict pagination.

        Listing is discovery only.  A consumer must still require and verify
        the owning commit marker before treating any listed object as evidence.
        """
        normalized = _normalize_prefix(prefix)
        continuation: str | None = None
        seen_tokens: set[str] = set()
        seen_keys: set[str] = set()
        while True:
            request: dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": normalized,
                "ExpectedBucketOwner": self.expected_bucket_owner,
            }
            if continuation is not None:
                request["ContinuationToken"] = continuation
            try:
                response = self._client.list_objects_v2(**request)
            except (ClientError, BotoCoreError) as error:
                raise ObjectStoreError(f"listing {normalized}: {error}") from error
            contents = response.get("Contents", [])
            if not isinstance(contents, list):
                raise ObjectStoreError(f"listing {normalized}: malformed Contents")
            for entry in contents:
                key = entry.get("Key") if isinstance(entry, dict) else None
                if not isinstance(key, str):
                    raise ObjectStoreError(f"listing {normalized}: object has no key")
                normalized_key = normalize_key(key)
                if (
                    normalized_key != key
                    or not key.startswith(normalized)
                    or key in seen_keys
                ):
                    raise ObjectStoreError(
                        f"listing {normalized}: malformed or repeated key {key!r}"
                    )
                seen_keys.add(key)
                yield normalized_key
            truncated = response.get("IsTruncated")
            if not isinstance(truncated, bool):
                raise ObjectStoreError(f"listing {normalized}: malformed IsTruncated")
            if not truncated:
                return
            token = response.get("NextContinuationToken")
            if not isinstance(token, str) or not token or token in seen_tokens:
                raise ObjectStoreError(
                    f"listing {normalized}: malformed or repeated continuation token"
                )
            seen_tokens.add(token)
            continuation = token

    # -- head interpretation ---------------------------------------------

    def _metadata_from_head(self, key: str, response: dict[str, Any]) -> ObjectMetadata:
        length = response.get("ContentLength")
        if not isinstance(length, int) or isinstance(length, bool) or length < 0:
            raise ObjectStoreError(
                f"{key}: HeadObject returned a malformed ContentLength {length!r}"
            )

        provider_checksum = response.get("ChecksumSHA256")
        if not provider_checksum:
            raise VerificationFailure(f"{key}: HeadObject returned no ChecksumSHA256")
        sha256 = base64_to_hex(provider_checksum)

        checksum_type = response.get("ChecksumType")
        if checksum_type is not None and checksum_type != _FULL_OBJECT_CHECKSUM_TYPE:
            raise VerificationFailure(
                f"{key}: HeadObject returned checksum type {checksum_type!r}, not "
                f"{_FULL_OBJECT_CHECKSUM_TYPE}"
            )

        return ObjectMetadata(
            key=key,
            byte_length=length,
            sha256=sha256,
            provider_checksum=provider_checksum,
            provider_checksum_algorithm="SHA256",
            content_type=response.get("ContentType"),
            content_encoding=response.get("ContentEncoding"),
        )

    # -- put_immutable resolution ------------------------------------------

    def _resolve_precondition_failure(
        self,
        key: str,
        expected_identity: StoredIdentity,
        content_type: str | None,
        content_encoding: str | None,
    ) -> ObjectMetadata:
        metadata = self.head(key)
        if metadata is None:
            # Concurrent state changed between the 412 and this head: the
            # object the conditional write collided with is already gone.
            # Nothing here is safe to retry against; the next sweep starts
            # over (§8.4).
            raise ObjectStoreError(
                f"{key}: PutObject returned 412 but the object is now absent; the next sweep "
                "must retry from the beginning"
            )
        if self._matches_request(
            metadata, expected_identity, content_type, content_encoding
        ):
            return metadata
        raise IntegrityConflict(
            f"key {key} already holds a different object than this put requested "
            f"({metadata.byte_length} bytes, sha256 {metadata.sha256}, "
            f"content_type={metadata.content_type!r}, content_encoding={metadata.content_encoding!r}); "
            f"expected {expected_identity.byte_length} bytes, sha256 {expected_identity.sha256}, "
            f"content_type={content_type!r}, content_encoding={content_encoding!r}"
        )

    @staticmethod
    def _matches_request(
        metadata: ObjectMetadata,
        expected_identity: StoredIdentity,
        content_type: str | None,
        content_encoding: str | None,
    ) -> bool:
        """§8.4: every requested property, not only the bytes.

        A `412` retry and a fresh upload are both accepted only when byte
        identity, the S3-reported checksum, its algorithm, and the requested
        content type and encoding all agree with what was asked for.
        """
        return (
            metadata.byte_length == expected_identity.byte_length
            and metadata.sha256 == expected_identity.sha256
            and metadata.provider_checksum
            == provider_checksum_of(expected_identity.sha256)
            and metadata.provider_checksum_algorithm == "SHA256"
            and metadata.content_type == content_type
            and metadata.content_encoding == content_encoding
        )


def _default_client(region: str) -> Any:
    import boto3

    return boto3.client("s3", region_name=region)


def _normalize_prefix(prefix: str) -> str:
    if prefix.endswith("/"):
        return normalize_key(prefix[:-1]) + "/"
    return normalize_key(prefix)
