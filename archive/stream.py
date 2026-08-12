"""Verified archived segments presented through replay's byte-stream boundary.

The archive receipt remains the commit marker.  This adapter maps each receipted
``.ndjson.zst`` object back to a logical ``.ndjson`` key, verifies the S3/object
store metadata, stages and fully decodes the frame, and only then yields bytes.
Replay therefore sees the same ``object_keys``/``iter_bytes`` shape it receives
from a local directory without learning about S3, receipts, or Zstandard.
"""

from __future__ import annotations

import hashlib
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator

from archive.common.receipts import ArchiveReceipt
from archive.common.verify import decode_archived_segment, verify_archive
from archive.storage.base import ObjectStore, VerificationFailure, normalize_key
from encoder import DEFAULT_BUFFER_BYTES

__all__ = ["ArchivedSegmentByteStreamer"]


@dataclass(frozen=True)
class _ArchivedObject:
    receipt: ArchiveReceipt
    kind: str


class ArchivedSegmentByteStreamer:
    """A replay-compatible streamer over receipt-committed archive objects.

    ``receipts`` are deliberately supplied by the caller. Production archive
    receipts are local commit markers and daily manifests are derived catalogs,
    so listing an S3 prefix cannot establish which objects are trusted.

    Decoded data and seal bytes stay in a private temporary directory until
    their complete recorded identities verify. No partially decoded or
    partially downloaded bytes are yielded.
    """

    def __init__(
        self,
        store: ObjectStore,
        receipts: Iterable[ArchiveReceipt],
        *,
        temp_root: Path | None = None,
        chunk_size: int = DEFAULT_BUFFER_BYTES,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.store = store
        self.temp_root = Path(temp_root) if temp_root is not None else None
        self.chunk_size = int(chunk_size)
        self._objects: dict[str, _ArchivedObject] = {}
        for receipt in receipts:
            self._add(receipt, "data", _logical_key(receipt.data_key, receipt.source_file))
            self._add(receipt, "seal", _logical_key(receipt.seal_key, receipt.seal_file))

    def object_keys(self) -> tuple[str, ...]:
        return tuple(sorted(self._objects))

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            archived = self._objects[key]
        except KeyError as error:
            raise VerificationFailure(f"unknown archived replay object key: {key}") from error

        with tempfile.TemporaryDirectory(
            prefix="archive-replay-",
            dir=self.temp_root,
        ) as directory:
            staged = Path(directory) / Path(key).name
            if archived.kind == "data":
                decode_archived_segment(self.store, archived.receipt, staged)
            else:
                self._stage_seal(archived.receipt, staged)

            with staged.open("rb") as source:
                while chunk := source.read(self.chunk_size):
                    yield chunk

    def _add(self, receipt: ArchiveReceipt, kind: str, key: str) -> None:
        if key in self._objects:
            raise ValueError(f"duplicate archived replay object key: {key}")
        self._objects[key] = _ArchivedObject(receipt=receipt, kind=kind)

    def _stage_seal(self, receipt: ArchiveReceipt, destination: Path) -> None:
        verify_archive(self.store, receipt)
        digest = hashlib.sha256()
        byte_length = 0
        with self.store.open(
            receipt.seal_key,
            max_bytes=receipt.seal_stored.byte_length,
        ) as source, destination.open("xb") as sink:
            while chunk := source.read(self.chunk_size):
                sink.write(chunk)
                digest.update(chunk)
                byte_length += len(chunk)
        if (
            byte_length != receipt.seal_stored.byte_length
            or digest.hexdigest() != receipt.seal_stored.sha256
        ):
            destination.unlink(missing_ok=True)
            raise VerificationFailure(
                f"archived seal {receipt.seal_key} disagrees with its receipt identity"
            )


def _logical_key(object_key: str, expected_name: str) -> str:
    """Drop storage prefixes while preserving the replay lane/date partition."""
    parts = normalize_key(object_key).split("/")
    partition = next(
        (
            index
            for index, part in enumerate(parts)
            if part.startswith("lane=") or part.startswith("venue=")
        ),
        None,
    )
    if partition is None:
        raise ValueError(f"archive object has no lane partition: {object_key!r}")
    if parts[-1] not in (expected_name, expected_name + ".zst"):
        raise ValueError(
            f"archive object {object_key!r} does not name receipted file {expected_name!r}"
        )
    return "/".join((*parts[partition:-1], expected_name))
