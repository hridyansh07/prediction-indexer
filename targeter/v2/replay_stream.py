"""Receipt-driven target records exposed through replay's byte boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator

from archive.retrieval import ArchivedObject, ArchivedObjectByteStreamer, verify_object
from archive.storage.base import (
    NDJSON_CONTENT_TYPE,
    ObjectExpectation,
    ObjectStore,
    VerificationFailure,
)
from encoder import DEFAULT_BUFFER_BYTES
from targeter.v2.models import SUPPORTED_VENUES
from targeter.v2.run_archive import (
    ArchivedRunObject,
    RunArchiveReceipt,
    parse_run_id_ns,
)

__all__ = [
    "ArchivedTargeterRunByteStreamer",
    "ArchivedTargetRecordByteStreamer",
    "RunReceiptSelection",
    "RunReceiptSelectionError",
    "select_run_receipts",
]


class RunReceiptSelectionError(ValueError):
    """Run receipts cannot establish metadata for the capture window."""


@dataclass(frozen=True)
class RunReceiptSelection:
    predecessor: RunArchiveReceipt
    in_window: tuple[RunArchiveReceipt, ...]

    @property
    def receipts(self) -> tuple[RunArchiveReceipt, ...]:
        return (self.predecessor, *self.in_window)


def select_run_receipts(
    receipts: Iterable[RunArchiveReceipt],
    *,
    start_ns: int,
    end_ns: int,
) -> RunReceiptSelection:
    """Select the predecessor and every run in ``[start_ns, end_ns)``.

    The predecessor is required even when a run timestamp equals ``start_ns``:
    publication completes after that timestamp, so opening tape records may
    still belong to the preceding generation.
    """
    for value, name in ((start_ns, "start_ns"), (end_ns, "end_ns")):
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise RunReceiptSelectionError(f"{name} must be a non-negative integer")
    if start_ns >= end_ns:
        raise RunReceiptSelectionError("start_ns must precede end_ns")

    by_run: dict[str, tuple[int, RunArchiveReceipt]] = {}
    for receipt in receipts:
        if receipt.run_id in by_run:
            raise RunReceiptSelectionError(f"duplicate run_id: {receipt.run_id}")
        instant = parse_run_id_ns(receipt.run_id)
        if instant is None:
            raise RunReceiptSelectionError(
                f"run receipt has an invalid run_id: {receipt.run_id!r}"
            )
        by_run[receipt.run_id] = (instant, receipt)

    ordered = sorted(by_run.values(), key=lambda item: (item[0], item[1].run_id))
    preceding = [item for item in ordered if item[0] < start_ns]
    if not preceding:
        raise RunReceiptSelectionError(
            f"no targeter run receipt precedes capture start {start_ns}"
        )
    predecessor = preceding[-1][1]
    in_window = tuple(
        receipt for instant, receipt in ordered if start_ns <= instant < end_ns
    )
    return RunReceiptSelection(predecessor=predecessor, in_window=in_window)


class ArchivedTargeterRunByteStreamer:
    """Every receipt-owned artifact from one or more complete Targeter runs."""

    def __init__(
        self,
        store: ObjectStore,
        receipts: Iterable[RunArchiveReceipt],
        *,
        temp_root: Path | None = None,
        chunk_size: int = DEFAULT_BUFFER_BYTES,
    ) -> None:
        self.store = store
        objects: list[ArchivedObject] = []
        self._manifests: dict[str, ObjectExpectation] = {}
        self._expectations: dict[str, ObjectExpectation] = {}
        for receipt in receipts:
            _validate_receipt(store, receipt)
            manifest = _expectation(receipt.manifest)
            for item in receipt.objects:
                logical_key = _logical_key(item)
                if logical_key in self._manifests:
                    raise ValueError(
                        f"duplicate archived Targeter run key: {logical_key}"
                    )
                self._manifests[logical_key] = manifest
                expected = _expectation(item)
                self._expectations[logical_key] = expected
                objects.append(ArchivedObject(logical_key, expected, item.logical))
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
            manifest = self._manifests[key]
        except KeyError as error:
            raise VerificationFailure(
                f"unknown archived Targeter run object key: {key}"
            ) from error
        if manifest.key != self._expectations[key].key:
            verify_object(self.store, manifest)
        yield from self._streamer.iter_bytes(key)


class ArchivedTargetRecordByteStreamer:
    """Verified target-record artifacts for one capture window.

    Local production run receipts drive discovery. S3 listings are not commit
    evidence and are never consulted. Each read freshly verifies the receipted
    remote manifest and selected object, then stages the complete decoded file
    before exposing its first byte.
    """

    def __init__(
        self,
        store: ObjectStore,
        receipts: Iterable[RunArchiveReceipt],
        *,
        start_ns: int,
        end_ns: int,
        temp_root: Path | None = None,
        chunk_size: int = DEFAULT_BUFFER_BYTES,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.store = store
        self.temp_root = Path(temp_root) if temp_root is not None else None
        self.chunk_size = int(chunk_size)
        self.selection = select_run_receipts(
            receipts,
            start_ns=start_ns,
            end_ns=end_ns,
        )
        self._runs = ArchivedTargeterRunByteStreamer(
            store,
            self.selection.receipts,
            temp_root=self.temp_root,
            chunk_size=self.chunk_size,
        )
        keys: set[str] = set()
        for receipt in self.selection.receipts:
            expected_names = {
                f"target_records_{venue}.ndjson" for venue in SUPPORTED_VENUES
            } | {f"target_records_{venue}.ndjson.zst" for venue in SUPPORTED_VENUES}
            for archived in receipt.objects:
                if archived.file not in expected_names:
                    continue
                _validate_target_record(receipt, archived)
                logical_key = _logical_key(archived)
                if logical_key in keys:
                    raise ValueError(
                        f"duplicate archived target-record key: {logical_key}"
                    )
                keys.add(logical_key)
        self._keys = tuple(sorted(keys))

    def object_keys(self) -> tuple[str, ...]:
        return self._keys

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        if key not in self._keys:
            raise VerificationFailure(
                f"unknown archived target-record object key: {key}"
            )
        yield from self._runs.iter_bytes(key)


def _validate_receipt(store: ObjectStore, receipt: RunArchiveReceipt) -> None:
    if not receipt.is_production:
        raise VerificationFailure(
            f"Targeter retrieval requires a production run receipt: {receipt.path}"
        )
    if receipt.location != store.store_id:
        raise VerificationFailure(
            f"run receipt {receipt.run_id} names {receipt.location!r}, not "
            f"store {store.store_id!r}"
        )
    if receipt.provider is not None and receipt.provider != store.provider:
        raise VerificationFailure(
            f"run receipt {receipt.run_id} names provider {receipt.provider!r}, not "
            f"{store.provider!r}"
        )
    if not store.durability.independent:
        raise VerificationFailure(
            "Targeter retrieval requires an independent archive store"
        )
    date = datetime.strptime(receipt.run_id[:8], "%Y%m%d").date()
    expected_prefix = f"targeter-v2/runs/date={date.isoformat()}/run={receipt.run_id}"
    if receipt.prefix != expected_prefix:
        raise VerificationFailure(
            f"run receipt {receipt.run_id} has unexpected prefix {receipt.prefix!r}"
        )


def _validate_target_record(
    receipt: RunArchiveReceipt, archived: ArchivedRunObject
) -> None:
    if archived.key != f"{receipt.prefix}/{archived.file}":
        raise VerificationFailure(
            f"target-record key disagrees with run prefix: {archived.key}"
        )
    if archived.logical is None:
        raise VerificationFailure(
            f"target-record object lacks decoded identity: {archived.key}"
        )
    if archived.content_type != NDJSON_CONTENT_TYPE:
        raise VerificationFailure(
            f"target-record object has wrong content type: {archived.key}"
        )


def _expectation(archived: ArchivedRunObject) -> ObjectExpectation:
    return ObjectExpectation(
        archived.key,
        archived.stored,
        archived.provider_checksum,
        archived.provider_checksum_algorithm,
        archived.content_type,
        archived.content_encoding,
    )


def _logical_key(archived: ArchivedRunObject) -> str:
    return (
        archived.key.removesuffix(".zst")
        if archived.content_encoding == "zstd"
        else archived.key
    )
