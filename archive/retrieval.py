"""One verified stored-object read path for every archive family."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

from archive.storage.base import (
    JSON_CONTENT_TYPE,
    ObjectExpectation,
    ObjectStore,
    VerificationFailure,
)
from encoder import DEFAULT_BUFFER_BYTES, LogicalIdentity, decode_stream

__all__ = [
    "ArchivedObject",
    "ArchivedObjectByteStreamer",
    "read_verified_json",
    "verify_object",
]


@dataclass(frozen=True)
class ArchivedObject:
    logical_key: str
    expected: ObjectExpectation
    logical: LogicalIdentity | None = None


class ArchivedObjectByteStreamer:
    """Stage and verify receipt-owned objects before exposing logical bytes."""

    def __init__(
        self,
        store: ObjectStore,
        objects: Iterable[ArchivedObject],
        *,
        temp_root: Path | None = None,
        chunk_size: int = DEFAULT_BUFFER_BYTES,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.store = store
        self.temp_root = Path(temp_root) if temp_root is not None else None
        self.chunk_size = int(chunk_size)
        self._objects: dict[str, ArchivedObject] = {}
        for item in objects:
            if item.logical_key in self._objects:
                raise ValueError(f"duplicate archived logical key: {item.logical_key}")
            self._objects[item.logical_key] = item
        self._keys = tuple(sorted(self._objects))

    def object_keys(self) -> tuple[str, ...]:
        return self._keys

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            archived = self._objects[key]
        except KeyError as error:
            raise VerificationFailure(f"unknown archived object key: {key}") from error

        with tempfile.TemporaryDirectory(
            prefix="archive-retrieval-", dir=self.temp_root
        ) as directory:
            staged = Path(directory) / Path(key).name
            try:
                with (
                    self.store.open_verified(archived.expected) as source,
                    _private_file(staged) as sink,
                ):
                    if archived.expected.content_encoding == "zstd":
                        logical = _required_logical(archived)
                        decode_stream(
                            source,
                            sink,
                            expected_logical=logical,
                            expected_stored=archived.expected.stored,
                            max_decoded_bytes=logical.byte_length,
                        )
                    elif archived.expected.content_encoding is None:
                        _copy_plain(source, sink, archived, self.chunk_size)
                    else:
                        raise VerificationFailure(
                            f"{archived.expected.key}: unsupported content encoding "
                            f"{archived.expected.content_encoding!r}"
                        )
            except BaseException:
                staged.unlink(missing_ok=True)
                raise

            with staged.open("rb") as source:
                while chunk := source.read(self.chunk_size):
                    yield chunk


def verify_object(store: ObjectStore, expected: ObjectExpectation) -> None:
    """Consume one exact object without exposing or retaining its bytes."""
    store.verify(expected)


def read_verified_json(
    store: ObjectStore,
    expected: ObjectExpectation,
    *,
    logical: LogicalIdentity | None = None,
    max_decoded_bytes: int,
    temp_root: Path | None = None,
) -> object:
    """Read one bounded JSON object after verified storage and decoding.

    The object is streamed through ``open_verified`` and, for Zstandard,
    ``decode_stream`` into a private temporary file. The returned JSON value is
    parsed only after the complete stored and decoded identities pass. This
    helper deliberately knows nothing about any archive family's schema.
    """
    if max_decoded_bytes < 0:
        raise ValueError("max_decoded_bytes must not be negative")
    if expected.content_type != JSON_CONTENT_TYPE:
        raise VerificationFailure(
            f"{expected.key}: verified JSON requires application/json content type"
        )
    if expected.content_encoding == "zstd" and logical is None:
        raise VerificationFailure(
            f"{expected.key}: compressed JSON lacks a logical identity"
        )
    if expected.content_encoding not in (None, "zstd"):
        raise VerificationFailure(
            f"{expected.key}: unsupported JSON content encoding "
            f"{expected.content_encoding!r}"
        )

    with tempfile.TemporaryDirectory(prefix="archive-json-", dir=temp_root) as directory:
        staged = Path(directory) / Path(expected.key).name
        archived = ArchivedObject(staged.name, expected, logical)
        with store.open_verified(expected) as source, _private_file(staged) as sink:
            if expected.content_encoding == "zstd":
                assert logical is not None
                decode_stream(
                    source,
                    sink,
                    expected_logical=logical,
                    expected_stored=expected.stored,
                    max_decoded_bytes=max_decoded_bytes,
                )
            else:
                if expected.stored.byte_length > max_decoded_bytes:
                    raise VerificationFailure(
                        f"{expected.key}: JSON object exceeds the "
                        f"{max_decoded_bytes}-byte maximum"
                    )
                _copy_plain(source, sink, archived, DEFAULT_BUFFER_BYTES)
        try:
            with staged.open("r", encoding="utf-8") as source:
                return json.load(source)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise VerificationFailure(f"{expected.key}: invalid JSON: {error}") from error


def _copy_plain(
    source: BinaryIO,
    sink: BinaryIO,
    archived: ArchivedObject,
    chunk_size: int,
) -> None:
    line_count = 0
    while chunk := source.read(chunk_size):
        _write_all(sink, chunk)
        line_count += chunk.count(b"\n")
    if archived.logical is not None:
        expected = archived.expected.stored
        if (
            archived.logical.sha256 != expected.sha256
            or archived.logical.byte_length != expected.byte_length
            or archived.logical.line_count != line_count
        ):
            raise VerificationFailure(
                f"{archived.expected.key}: plain logical identity disagrees with its receipt"
            )


def _required_logical(archived: ArchivedObject) -> LogicalIdentity:
    if archived.logical is None:
        raise VerificationFailure(
            f"{archived.expected.key}: compressed object lacks a logical identity"
        )
    return archived.logical


def _private_file(path: Path) -> BinaryIO:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return os.fdopen(descriptor, "wb")


def _write_all(sink: BinaryIO, chunk: bytes) -> None:
    view = memoryview(chunk)
    while view:
        written = sink.write(view)
        if written is None:
            return
        if written <= 0 or written > len(view):
            raise VerificationFailure(
                f"archive staging write made invalid progress: {written}"
            )
        view = view[written:]
