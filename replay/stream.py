"""The sole storage boundary presented to replay.

Replay consumes named immutable byte objects. It does not open paths, call S3,
or know whether an object came from NFS. Object names are logical keys relative
to a dataset root, so identical datasets have identical identities regardless of
where they are mounted.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Mapping, Protocol


class StreamError(RuntimeError):
    """The byte source could not uphold its immutable-snapshot contract."""


class SourceChanged(StreamError):
    """An object changed while it was being streamed."""


class TruncatedObject(StreamError):
    """An NDJSON object ended without its framing newline."""


class ByteStreamer(Protocol):
    """Storage adapters implement exactly these two deterministic operations."""

    def object_keys(self) -> tuple[str, ...]:
        """Return stable logical object keys in lexical order."""

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        """Yield the complete immutable value for ``key`` in non-empty chunks."""


@dataclass(frozen=True)
class TapeLine:
    object_key: str
    line_number: int
    byte_offset: int
    data: bytes


@dataclass(frozen=True)
class ObjectIdentity:
    key: str
    size: int
    sha256: str

    def as_record(self) -> dict[str, object]:
        return {"key": self.key, "size": self.size, "sha256": self.sha256}


@dataclass(frozen=True)
class InputManifest:
    objects: tuple[ObjectIdentity, ...]
    dataset_sha256: str

    def as_record(self) -> dict[str, object]:
        return {
            "version": 1,
            "dataset_sha256": self.dataset_sha256,
            "objects": [item.as_record() for item in self.objects],
        }


@dataclass(frozen=True)
class _FileSnapshot:
    path: Path
    size: int
    mtime_ns: int
    inode: int


class DirectoryByteStreamer:
    """A read-only NFS/local adapter with concurrent-mutation detection.

    The adapter snapshots identity metadata at construction, then validates it
    before and after every read. It is suitable for sealed directories or
    stopped capture fixtures. A live writer changing an object makes replay fail
    loudly instead of producing a time-dependent mixture of old and new bytes.
    """

    def __init__(
        self,
        root: Path,
        *,
        include: Callable[[str], bool] | None = None,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        self.root = Path(root).resolve()
        if not self.root.is_dir():
            raise StreamError(f"stream root is not a directory: {self.root}")
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.chunk_size = int(chunk_size)
        self._objects: dict[str, _FileSnapshot] = {}
        for path in sorted(item for item in self.root.rglob("*") if item.is_file()):
            key = path.relative_to(self.root).as_posix()
            if include is not None and not include(key):
                continue
            stat = path.stat()
            self._objects[key] = _FileSnapshot(
                path=path,
                size=stat.st_size,
                mtime_ns=stat.st_mtime_ns,
                inode=stat.st_ino,
            )

    def object_keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._objects))

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            snapshot = self._objects[key]
        except KeyError as error:
            raise StreamError(f"unknown object key: {key}") from error
        self._assert_unchanged(snapshot)
        read = 0
        with snapshot.path.open("rb") as handle:
            while read < snapshot.size:
                chunk = handle.read(min(self.chunk_size, snapshot.size - read))
                if not chunk:
                    raise SourceChanged(f"object shrank while reading: {key}")
                read += len(chunk)
                yield chunk
            if handle.read(1):
                raise SourceChanged(f"object grew while reading: {key}")
        self._assert_unchanged(snapshot)

    @staticmethod
    def _assert_unchanged(snapshot: _FileSnapshot) -> None:
        stat = snapshot.path.stat()
        observed = (stat.st_size, stat.st_mtime_ns, stat.st_ino)
        expected = (snapshot.size, snapshot.mtime_ns, snapshot.inode)
        if observed != expected:
            raise SourceChanged(f"object changed after stream snapshot: {snapshot.path}")


class MemoryByteStreamer:
    """Deterministic test adapter, useful for arbitrary chunk-boundary fixtures."""

    def __init__(self, objects: Mapping[str, bytes], *, chunk_size: int = 65536) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._objects = {_logical_key(key): bytes(value) for key, value in objects.items()}
        self.chunk_size = int(chunk_size)

    def object_keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._objects))

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            value = self._objects[key]
        except KeyError as error:
            raise StreamError(f"unknown object key: {key}") from error
        for start in range(0, len(value), self.chunk_size):
            yield value[start : start + self.chunk_size]


def iter_ndjson_lines(
    streamer: ByteStreamer, *, keys: Iterable[str] | None = None
) -> Iterator[TapeLine]:
    """Yield complete NDJSON records independently of storage chunk boundaries."""
    selected = streamer.object_keys() if keys is None else tuple(sorted(keys))
    for key in selected:
        if not key.endswith(".ndjson"):
            continue
        pending = bytearray()
        offset = 0
        line_number = 0
        for chunk in streamer.iter_bytes(key):
            if not chunk:
                raise StreamError(f"byte streamer yielded an empty chunk for {key}")
            pending.extend(chunk)
            while True:
                newline = pending.find(b"\n")
                if newline < 0:
                    break
                line_number += 1
                data = bytes(pending[:newline])
                del pending[: newline + 1]
                yield TapeLine(
                    object_key=key,
                    line_number=line_number,
                    byte_offset=offset,
                    data=data,
                )
                offset += newline + 1
        if pending:
            raise TruncatedObject(
                f"{key} ends with {len(pending)} unframed bytes after line {line_number}"
            )


def read_object(streamer: ByteStreamer, key: str) -> bytes:
    return b"".join(streamer.iter_bytes(key))


def build_input_manifest(streamer: ByteStreamer) -> InputManifest:
    """Content-address every input object and the ordered dataset as a whole."""
    identities: list[ObjectIdentity] = []
    for key in streamer.object_keys():
        digest = hashlib.sha256()
        size = 0
        for chunk in streamer.iter_bytes(key):
            if not chunk:
                raise StreamError(f"byte streamer yielded an empty chunk for {key}")
            digest.update(chunk)
            size += len(chunk)
        identities.append(ObjectIdentity(key=key, size=size, sha256=digest.hexdigest()))
    canonical = json.dumps(
        [item.as_record() for item in identities],
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return InputManifest(
        objects=tuple(identities),
        dataset_sha256=hashlib.sha256(b"replay-input-v1\0" + canonical).hexdigest(),
    )


def _logical_key(value: str) -> str:
    key = str(value).replace("\\", "/").lstrip("/")
    if not key or key == "." or any(part in ("", ".", "..") for part in key.split("/")):
        raise ValueError(f"invalid logical object key: {value!r}")
    return key
