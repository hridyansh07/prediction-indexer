"""Raw-segment archiver: one sealed segment, one ordered commit, one receipt.

```text
1  parse and structurally validate the seal against its path
2  create a unique <segment>.ndjson.zst.open
3  stream the raw bytes through the encoder, calculating both identities
4  compare logical sha256, byte length and LF count with the seal
5  fsync the derivative, rename it, fsync the directory
6  calculate the unchanged seal object's length and digest
7  publish data and seal through immutable object-store writes
8  head both objects and verify their actual remote identities
9  encode the receipt the backend's durability class permits
10 write it through .open, fsync, rename, fsync the directory
```

**Step 10 is the archive commit.** A compressed file is not one. A successful
upload is not one. A key existing in the store is not one. Everything before the
receipt is a derivative that may be deleted and rebuilt, which is what makes a
crash at any step recoverable without a decision about what half-happened.

**This module never deletes raw input.** Not on success, not after verification,
not as a callback. Deletion is a separate authority with a separate command and
a second receipt — see `archive/reaper/service.py` and §7.

The unit is one segment. An hourly sweep may process hundreds, but their frames
and commit boundaries never mix: one malformed segment must not erase or rewrite
another's successful receipt. The exception is an immutable-key conflict, which
stops the sweep, because a key holding unexpected content means the namespace or
the store is wrong rather than one file being bad.
"""

from __future__ import annotations

import os
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from archive.common.durable import confirm_durable, fsync_directory, write_json_durable
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    IntegrityConflict,
    ObjectStore,
    ObjectStoreError,
)
from archive.common.receipts import (
    LOCAL,
    PRODUCTION,
    ArchiveReceipt,
    ReceiptError,
    archive_receipt_path,
    build_archive_receipt,
    parse_archive_receipt,
    read_archive_receipt,
)
from archive.common.seal import (
    DERIVATIVE_SUFFIX,
    SealError,
    SealedSegment,
    logical_matches_seal,
    pending_segments,
    read_sealed_segment,
    sealed_segments,
)
from archive.common.verify import VerificationError, verify_archive
from encoder import (
    DEFAULT_ZSTD_LEVEL,
    CodecError,
    encode_stream,
    encoder_version,
    stored_identity_of,
)

__all__ = [
    "ARCHIVED",
    "CONFLICT",
    "FAILED",
    "SKIPPED",
    "Archiver",
    "SegmentOutcome",
    "SweepResult",
    "object_keys",
]

#: A receipt was published by this run.
ARCHIVED = "archived"
#: A valid receipt already existed and re-verified against the store.
SKIPPED = "skipped"
#: Something went wrong for this segment alone. Local artifacts are preserved.
FAILED = "failed"
#: A key holds different content. Fatal to the sweep.
CONFLICT = "conflict"

KEY_PREFIX = "raw"


@dataclass(frozen=True)
class SegmentOutcome:
    lane: str
    data_file: str
    status: str
    detail: str
    receipt_path: Path | None = None
    #: Present when the segment was archived or re-verified by this run.
    logical_sha256: str | None = None
    stored_byte_length: int | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "lane": self.lane,
            "data_file": self.data_file,
            "status": self.status,
            "detail": self.detail,
            "receipt": str(self.receipt_path) if self.receipt_path else None,
            "source_sha256": self.logical_sha256,
            "stored_byte_length": self.stored_byte_length,
        }


@dataclass
class SweepResult:
    outcomes: list[SegmentOutcome] = field(default_factory=list)
    #: Segments that exist but are not committed yet — open, or renamed and
    #: awaiting recovery. Not eligible, not faults, and not zero-information: a
    #: sweep that archived nothing because every lane is mid-window reads very
    #: differently from one that found nothing at all.
    pending: int = 0
    #: Set when a key conflict stopped the sweep before it finished.
    halted: str | None = None

    @property
    def discovered(self) -> int:
        return len(self.outcomes)

    def count(self, status: str) -> int:
        return sum(1 for outcome in self.outcomes if outcome.status == status)

    @property
    def counts(self) -> dict[str, int]:
        return {
            "discovered": self.discovered,
            "archived": self.count(ARCHIVED),
            "skipped": self.count(SKIPPED),
            "pending": self.pending,
            "failed": self.count(FAILED),
            "conflicted": self.count(CONFLICT),
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "counts": self.counts,
            "halted": self.halted,
            "segments": [outcome.as_record() for outcome in self.outcomes],
        }


def object_keys(segment: SealedSegment, *, prefix: str = KEY_PREFIX) -> tuple[str, str]:
    """`(data key, seal key)` for a segment, from its lane and its own window.

    Derived from the seal's window rather than the directory it was found in —
    the two are checked against each other during validation, so by this point
    they agree, and deriving from the validated value keeps the key stable if a
    segment is ever moved.
    """
    base = f"{prefix}/lane={segment.lane}/date={segment.date_partition}"
    return (
        f"{base}/{segment.segment_stem}{DERIVATIVE_SUFFIX}",
        f"{base}/{segment.seal_path.name}",
    )


class Archiver:
    """Publishes sealed segments to an object store, receipt last."""

    def __init__(
        self,
        spool_root: Path | str,
        store: ObjectStore,
        *,
        key_prefix: str = KEY_PREFIX,
        now_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.spool_root = Path(spool_root)
        self.store = store
        self.key_prefix = key_prefix
        # Not configurable. The receipt states `"level": 3` and readers check
        # it, so an archiver that could be pointed at another level would write
        # receipts describing frames it did not produce — which is exactly what
        # a compression contract exists to make impossible.
        self.level = DEFAULT_ZSTD_LEVEL
        self._now_ns = now_ns

    @property
    def receipt_kind(self) -> str:
        """Which receipt this backend is allowed to write (§5.3).

        Not a parameter. A caller that could choose would eventually choose the
        production shape against a conformance store, which is precisely the
        confusion the two filenames exist to prevent.
        """
        return PRODUCTION if self.store.durability.receipt_kind == "production" else LOCAL

    # -- sweep -------------------------------------------------------------

    def sweep(self, segments: Iterable[tuple[str, Path]] | None = None) -> SweepResult:
        """One complete pass. A key conflict stops it; nothing else does."""
        result = SweepResult(pending=len(pending_segments(self.spool_root)))
        discovered = sealed_segments(self.spool_root) if segments is None else list(segments)
        for lane, data_path in discovered:
            outcome = self.archive_segment(lane, data_path)
            result.outcomes.append(outcome)
            if outcome.status == CONFLICT:
                result.halted = (
                    f"{data_path.name}: {outcome.detail}. An immutable key holding "
                    "unexpected content is a namespace or integrity failure, not one "
                    "malformed segment; the sweep stops rather than publishing around it."
                )
                break
        return result

    # -- one segment -------------------------------------------------------

    def archive_segment(self, lane: str, data_path: Path) -> SegmentOutcome:
        data_path = Path(data_path)
        try:
            segment = read_sealed_segment(lane, data_path)
        except SealError as error:
            # An unreadable or incoherent seal is an integrity fault to report,
            # never "not sealed yet" — that case has no sidecar at all and is
            # invisible to discovery.
            return SegmentOutcome(lane, data_path.name, FAILED, str(error))

        receipt_path = archive_receipt_path(data_path, self.receipt_kind)
        try:
            existing = self._existing_receipt(segment, receipt_path)
            if existing is not None:
                return SegmentOutcome(
                    lane,
                    data_path.name,
                    SKIPPED,
                    "an existing receipt re-verified against the store",
                    receipt_path,
                    existing.source.sha256,
                    existing.data_stored.byte_length,
                )
            return self._archive(segment, receipt_path)
        except IntegrityConflict as error:
            return SegmentOutcome(lane, data_path.name, CONFLICT, str(error))
        except (SealError, CodecError, ObjectStoreError, VerificationError, ReceiptError) as error:
            return SegmentOutcome(lane, data_path.name, FAILED, str(error))
        except OSError as error:
            return SegmentOutcome(lane, data_path.name, FAILED, f"local I/O failure: {error}")

    def _existing_receipt(
        self, segment: SealedSegment, receipt_path: Path
    ) -> ArchiveReceipt | None:
        """Idempotency, but only after the receipt proves itself again (§6.4).

        A receipt is not evidence that the archive is still there. Every
        acceptance here re-heads both objects; a malformed receipt, a missing
        object or a checksum mismatch fails closed and is never repaired by
        overwriting either.
        """
        if not receipt_path.is_file():
            return None
        receipt = read_archive_receipt(receipt_path)

        if receipt.lane_id != segment.lane or receipt.source_file != segment.data_path.name:
            raise ReceiptError(
                f"archive receipt {receipt_path.name} names {receipt.lane_id}/"
                f"{receipt.source_file}, not {segment.lane}/{segment.data_path.name}"
            )
        if receipt.source != segment.logical:
            raise ReceiptError(
                f"archive receipt {receipt_path.name} describes a different source than the "
                "seal beside it"
            )
        if (receipt.window_start_ns, receipt.segment_index, receipt.segment_id) != (
            segment.window_start_ns,
            segment.segment_index,
            segment.segment_id,
        ):
            raise ReceiptError(
                f"archive receipt {receipt_path.name} names a different segment than its path"
            )

        # The raw source is checked by length rather than by digest here: the
        # bytes are immutable once sealed, this runs on every hourly sweep, and
        # rehashing every retained segment each hour is the unbounded scan the
        # pipeline refuses elsewhere. The reaper rehashes in full, once, at the
        # only point where being wrong is unrecoverable.
        actual_length = segment.data_path.stat().st_size
        if actual_length != receipt.source.byte_length:
            raise ReceiptError(
                f"{segment.data_path.name} is now {actual_length} bytes; the receipt records "
                f"{receipt.source.byte_length}"
            )
        seal_identity = _file_identity(segment.seal_path)
        if seal_identity != receipt.seal_stored:
            raise ReceiptError(
                f"{segment.seal_path.name} no longer matches the receipted seal object"
            )

        data_key, seal_key = object_keys(segment, prefix=self.key_prefix)
        if (receipt.data_key, receipt.seal_key) != (data_key, seal_key):
            raise ReceiptError(
                f"archive receipt {receipt_path.name} names keys this deployment would not write"
            )
        verify_archive(self.store, receipt)
        # This run is about to report the segment archived on the strength of a
        # receipt an earlier run may have renamed without syncing the directory.
        # Promoting a marker to a commit means guaranteeing it survives a crash,
        # so the guarantee is established here rather than inherited.
        confirm_durable(receipt_path)
        return receipt

    def _archive(self, segment: SealedSegment, receipt_path: Path) -> SegmentOutcome:
        derivative = segment.derivative_path
        # An unreceipted `.ndjson.zst` is untrusted whatever it contains, so it
        # is rebuilt rather than uploaded on the strength of its filename. The
        # rebuild is deterministic for one library and level, and the identity
        # comparison in step 4 is what actually decides.
        open_path = derivative.with_name(
            f"{derivative.name}.{os.getpid()}.{secrets.token_hex(4)}.open"
        )
        try:
            with segment.data_path.open("rb") as source, open_path.open("wb") as sink:
                result = encode_stream(source, sink, level=self.level)
                sink.flush()
                os.fsync(sink.fileno())

            # Step 4: the seal is a claim, and this is where it is falsified.
            # The logical identity was recomputed from the source bytes as they
            # passed into the encoder, not read back from anything.
            logical_matches_seal(result.logical, segment)

            os.replace(open_path, derivative)
            fsync_directory(derivative.parent)
        except Exception:
            open_path.unlink(missing_ok=True)
            raise

        seal_identity = _file_identity(segment.seal_path)
        data_key, seal_key = object_keys(segment, prefix=self.key_prefix)

        with derivative.open("rb") as reader:
            data_metadata = self.store.put_immutable(
                data_key,
                reader,
                result.stored,
                content_type=NDJSON_CONTENT_TYPE,
                content_encoding=ZSTD_CONTENT_ENCODING,
            )
        with segment.seal_path.open("rb") as reader:
            self.store.put_immutable(
                seal_key,
                reader,
                seal_identity,
                content_type=JSON_CONTENT_TYPE,
            )

        document = build_archive_receipt(
            kind=self.receipt_kind,
            lane_id=segment.lane,
            window_start_ns=segment.window_start_ns,
            window_end_ns=segment.window_end_ns,
            segment_id=segment.segment_id,
            segment_index=segment.segment_index,
            source_file=segment.data_path.name,
            source=result.logical,
            seal_file=segment.seal_path.name,
            seal_key=seal_key,
            seal_stored=seal_identity,
            data_key=data_key,
            data_stored=result.stored,
            location=self._location(),
            provider_checksum=(
                data_metadata.provider_checksum if self.receipt_kind == PRODUCTION else None
            ),
            encoder=encoder_version(),
            verified_at_ns=0,
        )
        # Step 8: the remote identities are re-read from the store rather than
        # taken from the put response. `verified_at_ns` is stamped afterwards
        # because it records when verification happened, and a receipt whose
        # timestamp precedes its own proof would be describing an intention.
        verify_archive(self.store, _as_receipt(document, receipt_path))
        document["verified_at_ns"] = self._now_ns()

        write_json_durable(receipt_path, document)
        return SegmentOutcome(
            segment.lane,
            segment.data_path.name,
            ARCHIVED,
            f"published {data_key}",
            receipt_path,
            result.logical.sha256,
            result.stored.byte_length,
        )

    def _location(self) -> str:
        return self.store.store_id


def _as_receipt(document: dict[str, Any], path: Path) -> ArchiveReceipt:
    """Parses the receipt this run is about to write, before it writes it.

    Encoding a document and trusting it is not the same as producing one that
    its own validator accepts. Everything downstream — manifests, the reaper —
    reads receipts through `parse_archive_receipt`, so the archiver proves the
    document survives that path before committing it.
    """
    return parse_archive_receipt(document, path=path)


def _file_identity(path: Path):
    with Path(path).open("rb") as handle:
        return stored_identity_of(handle)
