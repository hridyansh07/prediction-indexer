"""Receipt-driven archived canonical windows exposed as logical byte objects."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

from archive.archiver.canonical import CanonicalArchiveReceipt, CanonicalObject
from archive.common.receipts import CanonicalReceipt, read_canonical_receipt
from archive.retrieval import (
    ArchivedObject,
    ArchivedObjectByteStreamer,
    verify_object,
)
from archive.storage.base import ObjectExpectation, ObjectStore, VerificationFailure
from encoder import DEFAULT_BUFFER_BYTES, stored_identity_of

__all__ = ["ArchivedCanonicalByteStreamer"]


class ArchivedCanonicalByteStreamer:
    """Canonical evidence, provenance, and receipt from immutable storage."""

    def __init__(
        self,
        store: ObjectStore,
        receipts: Iterable[CanonicalArchiveReceipt],
        *,
        temp_root: Path | None = None,
        chunk_size: int = DEFAULT_BUFFER_BYTES,
    ) -> None:
        self.store = store
        objects: list[ArchivedObject] = []
        self._commit_markers: dict[str, ObjectExpectation] = {}
        self._expectations: dict[str, ObjectExpectation] = {}
        for receipt in receipts:
            source = _validate_receipt(store, receipt)
            commit = _expectation(receipt.canonical_receipt)
            for item, logical in (
                (receipt.evidence, source.evidence.decoded),
                (receipt.provenance, source.provenance.decoded),
                (receipt.canonical_receipt, None),
            ):
                logical_key = _logical_key(item)
                if logical_key in self._commit_markers:
                    raise ValueError(
                        f"duplicate archived canonical object key: {logical_key}"
                    )
                expected = _expectation(item)
                self._commit_markers[logical_key] = commit
                self._expectations[logical_key] = expected
                objects.append(ArchivedObject(logical_key, expected, logical))
        self._streamer = ArchivedObjectByteStreamer(
            store,
            objects,
            temp_root=temp_root,
            chunk_size=chunk_size,
        )

    def object_keys(self) -> tuple[str, ...]:
        return self._streamer.object_keys()

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            commit = self._commit_markers[key]
        except KeyError as error:
            raise VerificationFailure(
                f"unknown archived canonical object key: {key}"
            ) from error
        if commit.key != self._expectations[key].key:
            verify_object(self.store, commit)
        yield from self._streamer.iter_bytes(key)


def _validate_receipt(
    store: ObjectStore, receipt: CanonicalArchiveReceipt
) -> CanonicalReceipt:
    if receipt.location != store.store_id:
        raise VerificationFailure(
            f"canonical receipt names {receipt.location!r}, not {store.store_id!r}"
        )
    if (
        receipt.kind == "production"
        and receipt.provider is not None
        and receipt.provider != store.provider
    ):
        raise VerificationFailure(
            f"canonical receipt names provider {receipt.provider!r}, not "
            f"{store.provider!r}"
        )
    source = read_canonical_receipt(receipt.path.with_name("receipt.json"))
    if (
        source.window_start_ns != receipt.window_start_ns
        or source.window_end_ns != receipt.window_end_ns
        or source.evidence.stored != receipt.evidence.stored
        or source.provenance.stored != receipt.provenance.stored
    ):
        raise VerificationFailure(
            "canonical archive receipt disagrees with its retained canonical receipt"
        )
    with source.path.open("rb") as handle:
        source_receipt_identity = stored_identity_of(handle)
    if source_receipt_identity != receipt.canonical_receipt.stored:
        raise VerificationFailure(
            "retained canonical receipt disagrees with its archived identity"
        )
    return source


def _expectation(item: CanonicalObject) -> ObjectExpectation:
    return ObjectExpectation(
        item.key,
        item.stored,
        item.provider_checksum,
        item.provider_checksum_algorithm,
        item.content_type,
        item.content_encoding,
    )


def _logical_key(item: CanonicalObject) -> str:
    return (
        item.key.removesuffix(".zst") if item.content_encoding == "zstd" else item.key
    )
