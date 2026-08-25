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
        return self._uploaded_metadata(key, blob, expected_identity)

    def head(self, key: str) -> ObjectMetadata | None:
        key = normalize_key(key)
        blob = self._client.bucket(self.bucket).blob(key)
        try:
            blob.reload()
        except Exception as error:
            if _status(error) == 404:
                return None
            raise ObjectStoreError(f"heading {key}: {error}") from error
        generation = blob.generation
        if generation is None:
            raise ObjectStoreError(f"{key}: GCS returned no generation")
        return self._verify_blob(key, blob, generation)

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
            generation = blob.generation
            metageneration = blob.metageneration
            if generation is None or metageneration is None:
                raise VerificationFailure(
                    f"{key}: GCS returned no generation or metageneration"
                )
            if (
                blob.size != expected.stored.byte_length
                or blob.crc32c != expected.provider_checksum
                or blob.metadata
                != {
                    SHA256_METADATA: expected.stored.sha256,
                    BYTE_LENGTH_METADATA: str(expected.stored.byte_length),
                }
                or blob.content_type != expected.content_type
                or blob.content_encoding != expected.content_encoding
            ):
                raise VerificationFailure(
                    f"{key}: GCS object metadata disagrees with its receipt"
                )
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
        key: str, blob: Any, expected: StoredIdentity
    ) -> ObjectMetadata:
        custom = blob.metadata
        if (
            blob.size != expected.byte_length
            or not isinstance(blob.crc32c, str)
            or custom
            != {
                SHA256_METADATA: expected.sha256,
                BYTE_LENGTH_METADATA: str(expected.byte_length),
            }
        ):
            raise VerificationFailure(
                f"{key}: uploaded GCS metadata disagrees with the request"
            )
        return ObjectMetadata(
            key,
            expected.byte_length,
            expected.sha256,
            blob.crc32c,
            "CRC32C",
            blob.content_type,
            blob.content_encoding,
        )

    def _verify_blob(
        self, key: str, metadata_blob: Any, generation: Any
    ) -> ObjectMetadata:
        provider_crc = metadata_blob.crc32c
        length = metadata_blob.size
        metageneration = metadata_blob.metageneration
        if (
            not isinstance(provider_crc, str)
            or not isinstance(length, int)
            or length < 0
        ):
            raise VerificationFailure(f"{key}: GCS returned malformed length or CRC32C")
        if metageneration is None:
            raise VerificationFailure(f"{key}: GCS returned no metageneration")
        try:
            with self._pinned_blob(key, generation).open(
                "rb", raw_download=True
            ) as reader:
                sha256, crc32c, actual_length = self._identity(reader)
        except Exception as error:
            raise ObjectStoreError(
                f"reading generation {generation} of {key}: {error}"
            ) from error
        if actual_length != length or crc32c != provider_crc:
            raise VerificationFailure(
                f"{key}: generation-pinned bytes disagree with GCS metadata"
            )
        if metadata_blob.metadata != {
            SHA256_METADATA: sha256,
            BYTE_LENGTH_METADATA: str(actual_length),
        }:
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
        return ObjectMetadata(
            key,
            actual_length,
            sha256,
            crc32c,
            "CRC32C",
            metadata_blob.content_type,
            metadata_blob.content_encoding,
        )

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
        metadata = self.head(key)
        if metadata is None:
            raise ObjectStoreError(
                f"{key}: conditional create failed but object is absent"
            )
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
