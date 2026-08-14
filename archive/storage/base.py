"""Provider-neutral immutable object-store contract and shared identities."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import BinaryIO, Iterator, Protocol

from encoder import DEFAULT_BUFFER_BYTES, StoredIdentity

__all__ = [
    "CONFORMANCE", "INDEPENDENT", "BoundedReader", "DurabilityClass",
    "IntegrityConflict", "JSON_CONTENT_TYPE", "NDJSON_CONTENT_TYPE",
    "ObjectKeyError", "ObjectMetadata", "ObjectStore", "ObjectStoreError",
    "VerificationFailure", "ZSTD_CONTENT_ENCODING", "normalize_key",
    "provider_checksum_of",
]

METADATA_DIRECTORY = ".objectmeta"
NDJSON_CONTENT_TYPE = "application/x-ndjson"
JSON_CONTENT_TYPE = "application/json"
ZSTD_CONTENT_ENCODING = "zstd"


class ObjectStoreError(RuntimeError):
    """The store could not complete an operation. Retryable unless stated."""


class VerificationFailure(ObjectStoreError):
    """Bytes reached the store, but not the bytes the caller promised."""


class IntegrityConflict(ObjectStoreError):
    """A key already holds different content. Fatal: never repaired by writing."""


class ObjectKeyError(ValueError):
    """A key that is not a normalized relative POSIX path inside the store."""


@dataclass(frozen=True)
class DurabilityClass:
    name: str
    independent: bool
    receipt_kind: str


CONFORMANCE = DurabilityClass("local_conformance", False, "local")
INDEPENDENT = DurabilityClass("independent_durable", True, "production")


@dataclass(frozen=True)
class ObjectMetadata:
    key: str
    byte_length: int
    sha256: str
    provider_checksum: str
    provider_checksum_algorithm: str
    content_type: str | None = None
    content_encoding: str | None = None

    @property
    def stored(self) -> StoredIdentity:
        return StoredIdentity(sha256=self.sha256, byte_length=self.byte_length)

    def matches(self, identity: StoredIdentity) -> bool:
        return self.sha256 == identity.sha256 and self.byte_length == identity.byte_length

    def matches_request(
        self, identity: StoredIdentity, content_type: str | None, content_encoding: str | None
    ) -> bool:
        return (
            self.matches(identity)
            and self.content_type == content_type
            and self.content_encoding == content_encoding
        )


def provider_checksum_of(sha256_hex: str) -> str:
    return base64.b64encode(bytes.fromhex(sha256_hex)).decode("ascii")


def normalize_key(key: str) -> str:
    if not isinstance(key, str) or not key:
        raise ObjectKeyError("object key must be a non-empty string")
    if "\\" in key:
        raise ObjectKeyError(f"object key contains a backslash: {key!r}")
    if key.startswith("/"):
        raise ObjectKeyError(f"object key is absolute: {key!r}")
    if "\x00" in key:
        raise ObjectKeyError("object key contains a NUL byte")
    parts = key.split("/")
    for part in parts:
        if part in ("", ".", ".."):
            raise ObjectKeyError(f"object key has an empty or traversing component: {key!r}")
    if parts[0] == METADATA_DIRECTORY:
        raise ObjectKeyError(f"object key enters the store's metadata area: {key!r}")
    return "/".join(parts)


class ObjectStore(Protocol):
    store_id: str
    durability: DurabilityClass

    def put_immutable(
        self, key: str, reader: BinaryIO, expected_identity: StoredIdentity, *,
        content_type: str | None = None, content_encoding: str | None = None,
    ) -> ObjectMetadata: ...

    def head(self, key: str) -> ObjectMetadata | None: ...
    def open(self, key: str, *, max_bytes: int | None = None) -> BinaryIO: ...
    def list_keys(self, prefix: str) -> Iterator[str]: ...


class BoundedReader:
    def __init__(self, handle: BinaryIO, key: str, max_bytes: int | None) -> None:
        self._handle, self._key, self._max, self._read = handle, key, max_bytes, 0

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = DEFAULT_BUFFER_BYTES
        if self._max is not None:
            remaining = self._max - self._read
            if remaining <= 0:
                if self._handle.read(1):
                    raise VerificationFailure(
                        f"object {self._key} is longer than its recorded {self._max} bytes"
                    )
                return b""
            size = min(size, remaining)
        chunk = self._handle.read(size)
        self._read += len(chunk)
        return chunk

    def close(self) -> None:
        self._handle.close()

    def __enter__(self) -> BoundedReader:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __iter__(self) -> Iterator[bytes]:
        while chunk := self.read(DEFAULT_BUFFER_BYTES):
            yield chunk
