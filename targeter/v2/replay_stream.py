"""Receipt-driven target records exposed through replay's byte boundary."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator

from archive.storage.base import (
    NDJSON_CONTENT_TYPE,
    ObjectStore,
    VerificationFailure,
)
from encoder import (
    DEFAULT_BUFFER_BYTES,
    LogicalIdentity,
    StoredIdentity,
    decode_stream,
)
from targeter.v2.domain import SUPPORTED_VENUES
from targeter.v2.run_archive import (
    ArchivedRunObject,
    RunArchiveReceipt,
    parse_run_id_ns,
    verify_run_archive_objects,
)

__all__ = [
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


@dataclass(frozen=True)
class _TargetRecordObject:
    receipt: RunArchiveReceipt
    archived: ArchivedRunObject


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
        self._objects: dict[str, _TargetRecordObject] = {}
        for receipt in self.selection.receipts:
            self._add_receipt(receipt)
        self._keys = tuple(sorted(self._objects))

    def object_keys(self) -> tuple[str, ...]:
        return self._keys

    def iter_bytes(self, key: str) -> Iterator[bytes]:
        try:
            selected = self._objects[key]
        except KeyError as error:
            raise VerificationFailure(
                f"unknown archived target-record object key: {key}"
            ) from error

        verify_run_archive_objects(
            self.store,
            selected.receipt,
            (selected.receipt.manifest, selected.archived),
        )
        with tempfile.TemporaryDirectory(
            prefix="target-record-replay-",
            dir=self.temp_root,
        ) as directory:
            staged = Path(directory) / Path(key).name
            if selected.archived.content_encoding == "zstd":
                self._decode(selected.archived, staged)
            else:
                self._copy_plain(selected.archived, staged)
            with staged.open("rb") as source:
                while chunk := source.read(self.chunk_size):
                    yield chunk

    def _add_receipt(self, receipt: RunArchiveReceipt) -> None:
        if not receipt.is_production:
            raise VerificationFailure(
                f"target records require a production run receipt: {receipt.path}"
            )
        if receipt.location != self.store.store_id:
            raise VerificationFailure(
                f"run receipt {receipt.run_id} names {receipt.location!r}, not "
                f"store {self.store.store_id!r}"
            )
        if not self.store.durability.independent:
            raise VerificationFailure("target records require an independent archive store")
        date = datetime.strptime(receipt.run_id[:8], "%Y%m%d").date()
        expected_prefix = (
            f"targeter-v2/runs/date={date.isoformat()}/run={receipt.run_id}"
        )
        if receipt.prefix != expected_prefix:
            raise VerificationFailure(
                f"run receipt {receipt.run_id} has unexpected prefix {receipt.prefix!r}"
            )

        expected_names = {
            f"target_records_{venue}.ndjson" for venue in SUPPORTED_VENUES
        } | {
            f"target_records_{venue}.ndjson.zst" for venue in SUPPORTED_VENUES
        }
        for archived in receipt.objects:
            if archived.file not in expected_names:
                continue
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
            logical_key = archived.key.removesuffix(".zst")
            if logical_key in self._objects:
                raise ValueError(f"duplicate archived target-record key: {logical_key}")
            self._objects[logical_key] = _TargetRecordObject(receipt, archived)

    def _decode(self, archived: ArchivedRunObject, destination: Path) -> None:
        logical = _required_logical(archived)
        with self.store.open(
            archived.key,
            max_bytes=archived.stored.byte_length,
        ) as source, _private_file(destination) as sink:
            decode_stream(
                source,
                sink,
                expected_logical=logical,
                expected_stored=archived.stored,
                max_decoded_bytes=logical.byte_length,
            )

    def _copy_plain(self, archived: ArchivedRunObject, destination: Path) -> None:
        logical = _required_logical(archived)
        stored_digest = hashlib.sha256()
        logical_digest = hashlib.sha256()
        byte_length = 0
        line_count = 0
        with self.store.open(
            archived.key,
            max_bytes=archived.stored.byte_length,
        ) as source, _private_file(destination) as sink:
            while chunk := source.read(self.chunk_size):
                _write_all(sink, chunk)
                stored_digest.update(chunk)
                logical_digest.update(chunk)
                byte_length += len(chunk)
                line_count += chunk.count(b"\n")
        observed_stored = StoredIdentity(stored_digest.hexdigest(), byte_length)
        observed_logical = LogicalIdentity(
            logical_digest.hexdigest(), byte_length, line_count
        )
        if observed_stored != archived.stored or observed_logical != logical:
            destination.unlink(missing_ok=True)
            raise VerificationFailure(
                f"plain target-record object disagrees with its receipt: {archived.key}"
            )


def _required_logical(archived: ArchivedRunObject) -> LogicalIdentity:
    if archived.logical is None:
        raise VerificationFailure(
            f"target-record object lacks decoded identity: {archived.key}"
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
                f"target-record staging write made invalid progress: {written}"
            )
        view = view[written:]
