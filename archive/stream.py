"""Verified archived segments presented through replay's byte-stream boundary.

The archive receipt remains the commit marker.  This adapter maps each receipted
``.ndjson.zst`` object back to a logical ``.ndjson`` key, verifies the S3/object
store metadata, stages and fully decodes the frame, and only then yields bytes.
Replay therefore sees the same ``object_keys``/``iter_bytes`` shape it receives
from a local directory without learning about S3, receipts, or Zstandard.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

from archive.common.receipts import ArchiveReceipt
from archive.retrieval import ArchivedObject, ArchivedObjectByteStreamer
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    ObjectExpectation,
    ObjectStore,
    VerificationFailure,
    normalize_key,
)
from encoder import DEFAULT_BUFFER_BYTES

__all__ = ["ArchivedSegmentByteStreamer"]


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
        self._objects: dict[str, ArchivedObject] = {}
        for receipt in receipts:
            self._validate_store(receipt)
            self._add(
                ArchivedObject(
                    _logical_key(receipt.data_key, receipt.source_file),
                    ObjectExpectation(
                        receipt.data_key,
                        receipt.data_stored,
                        receipt.provider_checksum,
                        receipt.provider_checksum_algorithm,
                        NDJSON_CONTENT_TYPE,
                        ZSTD_CONTENT_ENCODING,
                    ),
                    receipt.source,
                )
            )
            self._add(
                ArchivedObject(
                    _logical_key(receipt.seal_key, receipt.seal_file),
                    ObjectExpectation(
                        receipt.seal_key,
                        receipt.seal_stored,
                        receipt.seal_provider_checksum,
                        receipt.seal_provider_checksum_algorithm,
                        JSON_CONTENT_TYPE,
                        None,
                    ),
                )
            )
        self._streamer = ArchivedObjectByteStreamer(
            store,
            self._objects.values(),
            temp_root=self.temp_root,
            chunk_size=self.chunk_size,
        )

    def object_keys(self) -> tuple[str, ...]:
        return self._streamer.object_keys()

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            yield from self._streamer.iter_bytes(key)
        except VerificationFailure as error:
            if "unknown archived object key" in str(error):
                raise VerificationFailure(
                    f"unknown archived replay object key: {key}"
                ) from error
            raise

    def _add(self, item: ArchivedObject) -> None:
        if item.logical_key in self._objects:
            raise ValueError(
                f"duplicate archived replay object key: {item.logical_key}"
            )
        self._objects[item.logical_key] = item

    def _validate_store(self, receipt: ArchiveReceipt) -> None:
        if receipt.location != self.store.store_id:
            raise VerificationFailure(
                f"archive receipt names {receipt.location!r}, not {self.store.store_id!r}"
            )
        if (
            receipt.is_production
            and receipt.provider is not None
            and receipt.provider != self.store.provider
        ):
            raise VerificationFailure(
                f"archive receipt names provider {receipt.provider!r}, not "
                f"{self.store.provider!r}"
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
