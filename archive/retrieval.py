"""One verified stored-object read path for every archive family."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

from archive.storage.base import ObjectExpectation, ObjectStore, VerificationFailure
from encoder import DEFAULT_BUFFER_BYTES, LogicalIdentity, decode_stream

__all__ = ["ArchivedObject", "ArchivedObjectByteStreamer", "verify_object"]


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
    with store.open_verified(expected) as source:
        while source.read(DEFAULT_BUFFER_BYTES):
            pass


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
