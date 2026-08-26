"""Native Google Cloud Storage implementation of the immutable ObjectStore."""

from __future__ import annotations

import base64
import hashlib
from typing import Any, BinaryIO, Iterator

import google_crc32c
from google.cloud.storage.exceptions import DataCorruption

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
)
from archive.storage.verification import verify_metadata
from encoder import DEFAULT_BUFFER_BYTES, StoredIdentity

__all__ = ["GCSObjectStore"]

SHA256_METADATA = "stored-sha256"
BYTE_LENGTH_METADATA = "stored-byte-length"


class _CRC32C:
    def __init__(self) -> None:
        self._checksum = google_crc32c.Checksum()

    def update(self, data: bytes) -> None:
        self._checksum.update(data)

    def digest(self) -> bytes:
        return self._checksum.digest()

    def base64(self) -> str:
        return base64.b64encode(self.digest()).decode("ascii")


def _status(error: BaseException) -> int | None:
    code = getattr(error, "code", None)
    code = code() if callable(code) else code
    if isinstance(code, int):
        return code
    response = getattr(error, "response", None)
    return getattr(response, "status_code", None)


def _validate_identity(identity: StoredIdentity) -> None:
    if (
        not isinstance(identity.byte_length, int)
        or isinstance(identity.byte_length, bool)
        or identity.byte_length < 0
    ):
        raise ObjectStoreError("expected byte_length must be a non-negative integer")
    if (
        not isinstance(identity.sha256, str)
        or len(identity.sha256) != 64
        or any(c not in "0123456789abcdef" for c in identity.sha256)
    ):
        raise ObjectStoreError(
            "expected sha256 must be a lowercase 64-character hex digest"
        )


class GCSObjectStore:
    provider = "gcs"
    durability = INDEPENDENT

    def __init__(self, bucket: str, client: Any = None) -> None:
        if not isinstance(bucket, str) or not bucket:
            raise ValueError("bucket is required")
        self.bucket = bucket
        self.store_id = bucket
        self._client = client if client is not None else _default_client()

    def put_immutable(
        self,
        key: str,
        reader: BinaryIO,
        expected_identity: StoredIdentity,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> ObjectMetadata:
        key = normalize_key(key)
        _validate_identity(expected_identity)
        if not reader.seekable():
            raise ObjectStoreError(f"{key}: put_immutable requires a seekable reader")

        blob = self._client.bucket(self.bucket).blob(
            key, chunk_size=DEFAULT_BUFFER_BYTES
        )
        blob.content_type = content_type
        blob.content_encoding = content_encoding
        blob.metadata = {
            SHA256_METADATA: expected_identity.sha256,
            BYTE_LENGTH_METADATA: str(expected_identity.byte_length),
        }
        try:
            digest = hashlib.sha256()
            length = 0
            with blob.open(
                "wb",
                chunk_size=DEFAULT_BUFFER_BYTES,
                if_generation_match=0,
                checksum="crc32c",
                content_type=content_type,
            ) as writer:
                while chunk := reader.read(DEFAULT_BUFFER_BYTES):
                    digest.update(chunk)
                    length += len(chunk)
                    writer.write(chunk)
                if (
                    length != expected_identity.byte_length
                    or digest.hexdigest() != expected_identity.sha256
                ):
                    raise VerificationFailure(
                        f"{key}: reader bytes disagree with the promised identity"
                    )
        except VerificationFailure:
            raise
        except Exception as error:
            if _status(error) == 412:
                return self._resolve_existing(
                    key, expected_identity, content_type, content_encoding
                )
            if isinstance(error, DataCorruption) or _status(error) == 400:
                raise VerificationFailure(
                    f"{key}: GCS rejected the upload checksum"
                ) from error
            raise ObjectStoreError(f"{key}: GCS upload failed: {error}") from error

        try:
            blob.reload()
        except Exception as error:
            raise ObjectStoreError(
                f"{key}: reading uploaded metadata failed: {error}"
            ) from error
        return self._uploaded_metadata(
            key,
            blob,
            expected_identity,
            content_type,
            content_encoding,
        )

    def head(self, key: str) -> ObjectMetadata | None:
        key = normalize_key(key)
        blob = self._reload_blob(key)
        return None if blob is None else self._metadata_from_blob(key, blob)

    def _reload_blob(self, key: str) -> Any | None:
        blob = self._client.bucket(self.bucket).blob(key)
        try:
            blob.reload()
        except Exception as error:
            if _status(error) == 404:
                return None
            raise ObjectStoreError(f"heading {key}: {error}") from error
        return blob

    def verify(self, expected: ObjectExpectation) -> ObjectMetadata:
        return verify_metadata(self.head(expected.key), expected)

    def open(self, key: str, *, max_bytes: int | None = None) -> BoundedReader:
        if max_bytes is not None and max_bytes < 0:
            raise ObjectStoreError("max_bytes must not be negative")
        key = normalize_key(key)
        blob = self._client.bucket(self.bucket).blob(key)
        try:
            blob.reload()
            length, generation = blob.size, blob.generation
            if not isinstance(length, int) or generation is None:
                raise ObjectStoreError(f"{key}: malformed GCS metadata")
            if max_bytes is not None and length > max_bytes:
                raise VerificationFailure(
                    f"{key}: object exceeds the {max_bytes} byte limit"
                )
            handle = self._pinned_blob(key, generation).open("rb", raw_download=True)
        except (ObjectStoreError, VerificationFailure):
            raise
        except Exception as error:
            raise ObjectStoreError(f"opening {key}: {error}") from error
        return BoundedReader(handle, key, max_bytes)

    def open_verified(self, expected: ObjectExpectation) -> VerifiedReader:
        key = normalize_key(expected.key)
        _validate_identity(expected.stored)
        if (
            expected.provider_checksum_algorithm != "CRC32C"
            or not expected.provider_checksum
        ):
            raise VerificationFailure(
                f"{key}: receipt has invalid GCS CRC32C checksum evidence"
            )
        blob = self._client.bucket(self.bucket).blob(key)
        try:
            blob.reload()
            metadata = self._metadata_from_blob(key, blob)
            verify_metadata(metadata, expected)
            generation = blob.generation
            metageneration = blob.metageneration
            handle = self._pinned_blob(key, generation).open("rb", raw_download=True)
        except (ObjectStoreError, VerificationFailure):
            raise
        except Exception as error:
            if _status(error) == 404:
                raise VerificationFailure(
                    f"{key}: receipted GCS object is absent"
                ) from error
            raise ObjectStoreError(
                f"opening verified generation of {key}: {error}"
            ) from error

        def recheck() -> None:
            current = self._client.bucket(self.bucket).blob(key)
            try:
                current.reload()
            except Exception as error:
                raise ObjectStoreError(
                    f"rechecking current generation of {key}: {error}"
                ) from error
            if current.generation != generation:
                raise VerificationFailure(
                    f"{key}: current generation changed during retrieval"
                )
            if current.metageneration != metageneration:
                raise VerificationFailure(f"{key}: metadata changed during retrieval")

        return VerifiedReader(
            handle,
            expected,
            provider_hasher=_CRC32C(),
            on_verified=recheck,
        )

    def list_keys(self, prefix: str) -> Iterator[str]:
        normalized = (
            normalize_key(prefix[:-1]) + "/"
            if prefix.endswith("/")
            else normalize_key(prefix)
        )
        try:
            for blob in self._client.list_blobs(self.bucket, prefix=normalized):
                key = normalize_key(blob.name)
                if not key.startswith(normalized):
                    raise ObjectStoreError(
                        f"listing {normalized}: malformed key {key!r}"
                    )
                yield key
        except ObjectStoreError:
            raise
        except Exception as error:
            raise ObjectStoreError(f"listing {normalized}: {error}") from error

    def _pinned_blob(self, key: str, generation: Any) -> Any:
        return self._client.bucket(self.bucket).blob(
            key, generation=generation, chunk_size=DEFAULT_BUFFER_BYTES
        )

    @staticmethod
    def _uploaded_metadata(
        key: str,
        blob: Any,
        expected: StoredIdentity,
        content_type: str | None,
        content_encoding: str | None,
    ) -> ObjectMetadata:
        metadata = GCSObjectStore._metadata_from_blob(key, blob)
        if not metadata.matches_request(expected, content_type, content_encoding):
            raise VerificationFailure(
                f"{key}: uploaded GCS metadata disagrees with the request"
            )
        return metadata

    @staticmethod
    def _metadata_from_blob(key: str, blob: Any) -> ObjectMetadata:
        provider_crc = blob.crc32c
        length = blob.size
        if blob.generation is None or blob.metageneration is None:
            raise VerificationFailure(
                f"{key}: GCS returned no generation or metageneration"
            )
        if (
            not isinstance(provider_crc, str)
            or not isinstance(length, int)
            or isinstance(length, bool)
            or length < 0
        ):
            raise VerificationFailure(f"{key}: GCS returned malformed length or CRC32C")
        try:
            decoded_crc = base64.b64decode(provider_crc, validate=True)
        except Exception as error:
            raise VerificationFailure(f"{key}: GCS returned malformed CRC32C") from error
        if len(decoded_crc) != 4:
            raise VerificationFailure(f"{key}: GCS returned malformed CRC32C")
        custom = blob.metadata
        if not isinstance(custom, dict) or set(custom) != {
            SHA256_METADATA,
            BYTE_LENGTH_METADATA,
        }:
            raise VerificationFailure(f"{key}: GCS identity metadata is malformed")
        sha256 = custom[SHA256_METADATA]
        try:
            custom_length = int(custom[BYTE_LENGTH_METADATA])
        except (TypeError, ValueError) as error:
            raise VerificationFailure(
                f"{key}: GCS identity metadata is malformed"
            ) from error
        identity = StoredIdentity(sha256=sha256, byte_length=custom_length)
        try:
            _validate_identity(identity)
        except ObjectStoreError as error:
            raise VerificationFailure(
                f"{key}: GCS identity metadata is malformed"
            ) from error
        if custom[BYTE_LENGTH_METADATA] != str(length):
            raise VerificationFailure(
                f"{key}: GCS identity metadata disagrees with object length"
            )
        return ObjectMetadata(
            key,
            length,
            sha256,
            provider_crc,
            "CRC32C",
            blob.content_type,
            blob.content_encoding,
        )

    def _verify_blob(
        self, key: str, metadata_blob: Any, generation: Any
    ) -> ObjectMetadata:
        metadata = self._metadata_from_blob(key, metadata_blob)
        metageneration = metadata_blob.metageneration
        try:
            with self._pinned_blob(key, generation).open(
                "rb", raw_download=True
            ) as reader:
                sha256, crc32c, actual_length = self._identity(reader)
        except Exception as error:
            raise ObjectStoreError(
                f"reading generation {generation} of {key}: {error}"
            ) from error
        if (
            actual_length != metadata.byte_length
            or crc32c != metadata.provider_checksum
        ):
            raise VerificationFailure(
                f"{key}: generation-pinned bytes disagree with GCS metadata"
            )
        if sha256 != metadata.sha256:
            raise VerificationFailure(
                f"{key}: GCS identity metadata disagrees with its bytes"
            )
        current = self._client.bucket(self.bucket).blob(key)
        try:
            current.reload()
        except Exception as error:
            raise ObjectStoreError(
                f"rechecking current generation of {key}: {error}"
            ) from error
        if current.generation != generation:
            raise VerificationFailure(
                f"{key}: current generation changed during verification"
            )
        if current.metageneration != metageneration:
            raise VerificationFailure(f"{key}: metadata changed during verification")
        return metadata

    @staticmethod
    def _identity(reader: BinaryIO) -> tuple[str, str, int]:
        sha256, crc32c, length = hashlib.sha256(), _CRC32C(), 0
        while chunk := reader.read(DEFAULT_BUFFER_BYTES):
            sha256.update(chunk)
            crc32c.update(chunk)
            length += len(chunk)
        return sha256.hexdigest(), crc32c.base64(), length

    def _resolve_existing(
        self,
        key: str,
        identity: StoredIdentity,
        content_type: str | None,
        content_encoding: str | None,
    ) -> ObjectMetadata:
        blob = self._reload_blob(key)
        if blob is None:
            raise ObjectStoreError(
                f"{key}: conditional create failed but object is absent"
            )
        metadata = self._verify_blob(key, blob, blob.generation)
        if metadata.matches_request(identity, content_type, content_encoding):
            return metadata
        raise IntegrityConflict(f"key {key} already holds different bytes or metadata")


def _default_client() -> Any:
    try:
        from google.cloud import storage
    except ImportError as error:
        raise ObjectStoreError(
            "google-cloud-storage is required for the GCS adapter"
        ) from error
    return storage.Client()
