"""The immutable local object-store implementation.

The write-side protocol is deliberately much smaller than an S3 client
(`PHASE_4_RAW_ARCHIVE_REAPER_V1.md` §5.1):

```text
put_immutable(key, reader, expected_identity) -> ObjectMetadata
head(key)                                     -> ObjectMetadata | None
verify_metadata(expectation)                  -> ObjectMetadata
verify(expectation)                           -> None
open(key)                                     -> bounded byte reader
open_verified(expectation)                    -> verified byte reader
list_keys(prefix)                             -> immutable key iterator
```

`expected_identity` is known before publication, always. Passing it in is what
makes retry and conflict semantics explicit rather than emergent: the adapter
verifies that the reader supplied exactly those bytes, and a future S3 adapter
has the checksum a conditional write needs. An upload that "succeeded" without
the service confirming what it stored is not a commit.

**A key is written once.** An existing key holding the expected identity is a
successful retry; an existing key holding anything else is an integrity
conflict, which is fatal to the sweep because it means the namespace or the
data is wrong rather than one segment being malformed. Nothing here overwrites,
versions in place, or accepts a conflicting object as a retry.

**Being an object store is not the same as being durable.** `LocalObjectStore`
exists so immutability, retry, verification and crash behaviour can be exercised
without cloud credentials — but a directory on the capture volume loses both
copies at once, so it declares itself `local_conformance` and the reaper refuses
to delete against it. Authorizing a local backend as an independent durability
domain is possible and explicit; it is never a default (§5.3, invariant 7).
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
from pathlib import Path
from typing import BinaryIO, Iterator

from encoder import DEFAULT_BUFFER_BYTES, StoredIdentity
from archive.storage.base import (
    CONFORMANCE,
    INDEPENDENT,
    BoundedReader,
    DurabilityClass,
    IntegrityConflict,
    JSON_CONTENT_TYPE,
    METADATA_DIRECTORY,
    NDJSON_CONTENT_TYPE,
    ObjectExpectation,
    ObjectKeyError,
    ObjectMetadata,
    ObjectStore,
    ObjectStoreError,
    VerificationFailure,
    VerifiedReader,
    ZSTD_CONTENT_ENCODING,
    normalize_key,
    provider_checksum_of,
)
from archive.storage.verification import consume_verified, match_metadata

__all__ = [
    "CONFORMANCE",
    "INDEPENDENT",
    "BoundedReader",
    "DurabilityClass",
    "IntegrityConflict",
    "LocalObjectStore",
    "ObjectKeyError",
    "ObjectMetadata",
    "ObjectStore",
    "ObjectStoreError",
    "VerificationFailure",
    "normalize_key",
    "provider_checksum_of",
]

# -- durability primitives, module-level so failure injection can reach them ----


def _fsync_file(handle: BinaryIO) -> None:
    handle.flush()
    os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _hash_file(path: Path, buffer_bytes: int) -> tuple[str, int]:
    """Digest and length of a file, read without following a symlink.

    Module-level so a test can wrap it and mutate the object mid-read, which is
    the only way to prove `head` detects a concurrent writer rather than
    reporting whatever it happened to see first.
    """
    digest = hashlib.sha256()
    length = 0
    with open(
        os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb", closefd=True
    ) as handle:
        while True:
            chunk = handle.read(buffer_bytes)
            if not chunk:
                break
            digest.update(chunk)
            length += len(chunk)
    return digest.hexdigest(), length


def _link_exclusive(temporary: Path, final: Path) -> None:
    """Publishes a name that must not already exist.

    `os.link` rather than `os.replace`: replace would silently overwrite a key
    holding different content, and the absence check that precedes it is a
    time-of-check race. Link fails with `EEXIST` instead, so immutability is
    enforced by the filesystem rather than by our own timing.
    """
    os.link(temporary, final)


def _create_directories_durable(path: Path, root: Path) -> None:
    """Creates the chain under `root`, fsyncing each new directory's parent.

    `mkdir -p` alone leaves the *link* to a new directory in its parent's page
    cache, so a crash can take a directory — and everything committed inside
    it — while every file below was individually synced.
    """
    if path.is_dir():
        return
    missing: list[Path] = []
    current = path
    while not current.is_dir():
        missing.append(current)
        if current == root or current.parent == current:
            break
        current = current.parent
    for directory in reversed(missing):
        try:
            directory.mkdir()
        except FileExistsError:
            continue
        _fsync_directory(directory.parent)


class LocalObjectStore:
    """An immutable store under a directory, with a seal's write discipline.

    ```text
    unique .open -> write -> fsync file -> link into place -> fsync directory
    ```

    Its purpose is to make the S3 contract testable: real immutability, real
    retries, real verification, real crash windows, no credentials. Every
    identity it reports is recalculated from the bytes on disk — `head` reopens
    and re-hashes rather than echoing what `put_immutable` was told, because a
    store that repeats the caller's claim back can only ever confirm it.
    """

    provider = "local"

    def __init__(
        self,
        root: Path | str,
        *,
        store_id: str | None = None,
        durability: DurabilityClass = CONFORMANCE,
        buffer_bytes: int = DEFAULT_BUFFER_BYTES,
    ) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.store_id = store_id or f"local:{self.root}"
        self.durability = durability
        self.buffer_bytes = int(buffer_bytes)

    # -- paths -------------------------------------------------------------

    def _path_for(self, key: str) -> Path:
        normalized = normalize_key(key)
        candidate = self.root / normalized
        # `resolve` follows symlinks, so a link planted inside the store that
        # points outside it lands outside the root and is refused here. When the
        # object does not exist yet the deepest existing ancestor is checked
        # instead, which catches a symlinked directory before anything is
        # written through it.
        existing = candidate
        while not existing.exists() and existing != self.root:
            existing = existing.parent
        if not _is_within(existing.resolve(), self.root):
            raise ObjectKeyError(f"object key escapes the store root: {key!r}")
        return candidate

    def _metadata_path(self, key: str) -> Path:
        return self.root / METADATA_DIRECTORY / (normalize_key(key) + ".json")

    # -- protocol ----------------------------------------------------------

    def put_immutable(
        self,
        key: str,
        reader: BinaryIO,
        expected_identity: StoredIdentity,
        *,
        content_type: str | None = None,
        content_encoding: str | None = None,
    ) -> ObjectMetadata:
        """Writes a key that does not exist yet, or proves the one that does."""
        normalized = normalize_key(key)
        path = self._path_for(normalized)

        existing = self.head(normalized)
        if existing is not None:
            if existing.matches(expected_identity):
                requested_attributes = (content_type, content_encoding)
                stored_attributes = (existing.content_type, existing.content_encoding)
                if stored_attributes != requested_attributes:
                    # A process can die after the data link is durable but
                    # before its separate local metadata sidecar is published.
                    # Both stored values being absent is the recognizable
                    # local-only recovery state: repair it from the identical
                    # retry. Any present-but-different value is an immutable
                    # request conflict, just as it is for one atomic S3 put.
                    if stored_attributes == (None, None) and requested_attributes != (
                        None,
                        None,
                    ):
                        self._write_attributes(
                            normalized, content_type, content_encoding
                        )
                        repaired = self.head(normalized)
                        if repaired is None:
                            raise VerificationFailure(
                                f"key {normalized} vanished while repairing its attributes"
                            )
                        existing = repaired
                    else:
                        raise IntegrityConflict(
                            f"key {normalized} already holds the expected bytes but different "
                            f"metadata (content_type={existing.content_type!r}, "
                            f"content_encoding={existing.content_encoding!r}); expected "
                            f"content_type={content_type!r}, content_encoding={content_encoding!r}"
                        )
                if not existing.matches_request(
                    expected_identity, content_type, content_encoding
                ):
                    raise IntegrityConflict(
                        f"key {normalized} could not establish the metadata requested by "
                        "this identical retry"
                    )
                # The key may be present *because* a previous attempt linked it
                # and then failed to sync the directory — it reported failure,
                # so nothing downstream committed to it, but the name is not
                # durable and a crash could take it. Accepting it as a
                # successful put is a promise about durability, so the promise
                # is re-established here rather than assumed from the earlier
                # attempt that did not make it.
                _fsync_directory(path.parent)
                attributes_path = self._metadata_path(normalized)
                if attributes_path.is_file():
                    _fsync_directory(attributes_path.parent)
                return existing
            raise IntegrityConflict(
                f"key {normalized} already holds a different object "
                f"({existing.byte_length} bytes, sha256 {existing.sha256}); "
                f"expected {expected_identity.byte_length} bytes, "
                f"sha256 {expected_identity.sha256}"
            )

        _create_directories_durable(path.parent, self.root)
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.open"
        )
        digest = hashlib.sha256()
        length = 0
        try:
            with temporary.open("wb") as handle:
                while True:
                    chunk = reader.read(self.buffer_bytes)
                    if not chunk:
                        break
                    digest.update(chunk)
                    length += len(chunk)
                    handle.write(chunk)
                _fsync_file(handle)

            actual = StoredIdentity(sha256=digest.hexdigest(), byte_length=length)
            if actual != expected_identity:
                raise VerificationFailure(
                    f"key {normalized} received {actual.byte_length} bytes with sha256 "
                    f"{actual.sha256}; the caller promised {expected_identity.byte_length} "
                    f"bytes with sha256 {expected_identity.sha256}"
                )

            try:
                _link_exclusive(temporary, path)
            except FileExistsError:
                # Another writer published between the head and the link. The
                # winner's content decides whether this is a retry or a
                # conflict; either way we never overwrite it.
                published = self.head(normalized)
                if published is not None and published.matches_request(
                    expected_identity, content_type, content_encoding
                ):
                    _fsync_directory(path.parent)
                    attributes_path = self._metadata_path(normalized)
                    if attributes_path.is_file():
                        _fsync_directory(attributes_path.parent)
                    return published
                raise IntegrityConflict(
                    f"key {normalized} was published concurrently with different content "
                    "or metadata"
                ) from None
            except OSError as error:
                raise ObjectStoreError(f"publishing {normalized}: {error}") from error
            _fsync_directory(path.parent)
        finally:
            # Every path, including the concurrent-publish return above. After a
            # successful link the object has two names and this drops the
            # temporary one; after a failure it drops the only one.
            temporary.unlink(missing_ok=True)

        self._write_attributes(normalized, content_type, content_encoding)
        published = self.head(normalized)
        if published is None or not published.matches_request(
            expected_identity, content_type, content_encoding
        ):
            raise VerificationFailure(
                f"key {normalized} did not survive publication with the requested identity "
                "and metadata"
            )
        return published

    def head(self, key: str) -> ObjectMetadata | None:
        """Recalculates the object's real length and digest, or reports absence."""
        normalized = normalize_key(key)
        path = self._path_for(normalized)
        try:
            before = os.stat(path, follow_symlinks=False)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise ObjectStoreError(f"reading {normalized}: {error}") from error
        if not _is_regular_file(before):
            raise ObjectStoreError(f"key {normalized} is not a regular file")

        try:
            sha256, length = _hash_file(path, self.buffer_bytes)
        except OSError as error:
            raise ObjectStoreError(f"reading {normalized}: {error}") from error

        # Identity metadata bracketing the read. A live writer changing an
        # object makes verification fail loudly instead of producing a
        # time-dependent mixture of old and new bytes.
        try:
            after = os.stat(path, follow_symlinks=False)
        except FileNotFoundError as error:
            raise VerificationFailure(
                f"object {normalized} disappeared while it was being read"
            ) from error
        except OSError as error:
            raise ObjectStoreError(f"rechecking {normalized}: {error}") from error
        mutated = (
            before.st_ino != after.st_ino
            or before.st_mtime_ns != after.st_mtime_ns
            or after.st_size != length
        )
        if mutated:
            raise VerificationFailure(
                f"object {normalized} changed while it was being read"
            )

        content_type, content_encoding = self._read_attributes(normalized)
        return ObjectMetadata(
            key=normalized,
            byte_length=length,
            sha256=sha256,
            provider_checksum=provider_checksum_of(sha256),
            provider_checksum_algorithm="SHA256",
            content_type=content_type,
            content_encoding=content_encoding,
        )

    def verify_metadata(self, expected: ObjectExpectation) -> ObjectMetadata:
        return match_metadata(self.head(expected.key), expected)

    def verify(self, expected: ObjectExpectation) -> None:
        consume_verified(self, expected)

    def open(self, key: str, *, max_bytes: int | None = None) -> BoundedReader:
        normalized = normalize_key(key)
        path = self._path_for(normalized)
        try:
            handle = open(
                os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)),
                "rb",
                closefd=True,
            )
        except FileNotFoundError as error:
            raise ObjectStoreError(f"object {normalized} is absent") from error
        except OSError as error:
            raise ObjectStoreError(f"opening {normalized}: {error}") from error
        return BoundedReader(handle, normalized, max_bytes)

    def open_verified(self, expected: ObjectExpectation) -> VerifiedReader:
        normalized = normalize_key(expected.key)
        if expected.provider_checksum_algorithm not in (None, "SHA256"):
            raise VerificationFailure(
                f"{normalized}: receipt has unsupported local checksum evidence"
            )
        if (
            expected.provider_checksum is not None
            and expected.provider_checksum
            != provider_checksum_of(expected.stored.sha256)
        ):
            raise VerificationFailure(
                f"{normalized}: receipt has invalid local SHA256 checksum evidence"
            )
        path = self._path_for(normalized)
        try:
            before = os.stat(path, follow_symlinks=False)
            if not _is_regular_file(before):
                raise ObjectStoreError(f"key {normalized} is not a regular file")
            attributes = self._read_attributes(normalized)
            if before.st_size != expected.stored.byte_length or attributes != (
                expected.content_type,
                expected.content_encoding,
            ):
                raise VerificationFailure(
                    f"{normalized}: local object metadata disagrees with its receipt"
                )
            handle = open(
                os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)),
                "rb",
                closefd=True,
            )
        except (ObjectStoreError, VerificationFailure):
            raise
        except FileNotFoundError as error:
            raise VerificationFailure(f"object {normalized} is absent") from error
        except OSError as error:
            raise ObjectStoreError(f"opening {normalized}: {error}") from error

        def recheck() -> None:
            try:
                after = os.stat(path, follow_symlinks=False)
            except OSError as error:
                raise ObjectStoreError(f"rechecking {normalized}: {error}") from error
            if (
                before.st_ino != after.st_ino
                or before.st_mtime_ns != after.st_mtime_ns
                or before.st_size != after.st_size
                or self._read_attributes(normalized) != attributes
            ):
                raise VerificationFailure(
                    f"object {normalized} changed while it was being retrieved"
                )

        return VerifiedReader(
            handle,
            expected,
            provider_hasher=hashlib.sha256(),
            on_verified=recheck,
        )

    def list_keys(self, prefix: str) -> Iterator[str]:
        """List immutable object keys below one normalized prefix.

        The metadata sidecar tree is an implementation detail and is never an
        object key.  Sorting makes local development and production S3 scans
        expose the same deterministic order.
        """
        normalized = _normalize_prefix(prefix)
        if not self.root.is_dir():
            return
        for path in sorted(self.root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(self.root).as_posix()
            if relative == METADATA_DIRECTORY or relative.startswith(
                METADATA_DIRECTORY + "/"
            ):
                continue
            if relative.startswith(normalized):
                yield relative

    # -- content type and encoding ----------------------------------------

    def _write_attributes(
        self, key: str, content_type: str | None, content_encoding: str | None
    ) -> None:
        if content_type is None and content_encoding is None:
            return
        path = self._metadata_path(key)
        _create_directories_durable(path.parent, self.root)
        payload = (
            '{"content_type": '
            + _json_or_null(content_type)
            + ', "content_encoding": '
            + _json_or_null(content_encoding)
            + "}\n"
        ).encode("utf-8")
        temporary = path.with_name(
            f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.open"
        )
        try:
            with temporary.open("wb") as handle:
                handle.write(payload)
                _fsync_file(handle)
            os.replace(temporary, path)
            _fsync_directory(path.parent)
        finally:
            temporary.unlink(missing_ok=True)

    def _read_attributes(self, key: str) -> tuple[str | None, str | None]:
        path = self._metadata_path(key)
        if not path.is_file():
            return (None, None)
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ObjectStoreError(
                f"unreadable attributes for {key}: {error}"
            ) from error
        if not isinstance(document, dict):
            raise ObjectStoreError(
                f"unreadable attributes for {key}: document is not an object"
            )
        content_type = document.get("content_type")
        content_encoding = document.get("content_encoding")
        if content_type is not None and not isinstance(content_type, str):
            raise ObjectStoreError(
                f"unreadable attributes for {key}: content_type is not a string or null"
            )
        if content_encoding is not None and not isinstance(content_encoding, str):
            raise ObjectStoreError(
                f"unreadable attributes for {key}: content_encoding is not a string or null"
            )
        return (content_type, content_encoding)


def _json_or_null(value: str | None) -> str:
    return "null" if value is None else json.dumps(value)


def _is_regular_file(status: os.stat_result) -> bool:
    return stat.S_ISREG(status.st_mode)


def _is_within(candidate: Path, root: Path) -> bool:
    try:
        candidate.relative_to(root)
    except ValueError:
        return False
    return True


def _normalize_prefix(prefix: str) -> str:
    if prefix.endswith("/"):
        return normalize_key(prefix[:-1]) + "/"
    return normalize_key(prefix)
