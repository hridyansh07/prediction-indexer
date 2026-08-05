"""One UTC-aligned segment of a lane's tape, and the seal that commits it.

A segment is a fixed wall-clock window of one lane's records — thirty minutes in
production. It replaces the old one-file-per-connection layout, and the change is
not cosmetic: a reconnect no longer rolls a file, so **a segment deliberately
contains several connection epochs**. The marker between them is the
`connection_epoch` field carried on every record, which both readers already key
continuity on, so nothing needs the filename to identify a connection.

**The seal is the commit marker.** An `.ndjson` with no valid seal is never
eligible for merge or archive, which is what lets a reader distinguish "this lane
had nothing to say in this window" from "this lane has not finished this window
yet" — a distinction the old layout could not express at all, because a closed
file was byte-identical to an open one.

Written synchronously and deliberately so. It knows nothing about asyncio,
sockets or venues; `splices/common/writer.py` owns the concurrency and hands it
batches. That split is what makes the five-step seal testable by asserting a
syscall order rather than by inspection.

See `docs/SEALED_CAPTURE_PIPELINE_V1.md` §3.
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

__all__ = [
    "DEFAULT_SEGMENT_SECONDS",
    "OPEN_SUFFIX",
    "SEAL_SUFFIX",
    "Seal",
    "SegmentError",
    "SegmentWriter",
    "read_seal",
    "seal_path_for",
    "segment_filename",
    "window_start_ns",
]

SEAL_VERSION = 1
WRITER_VERSION = 1

DEFAULT_SEGMENT_SECONDS = 1800

#: The suffix a segment wears while the writer still owns it. Chosen so the
#: file's *extension* is `open`, not `ndjson` — the Rust ingester filters on
#: `extension == "ndjson"` and therefore skips an in-flight segment without
#: needing to know segments exist.
OPEN_SUFFIX = ".ndjson.open"
SEAL_SUFFIX = ".seal.json"

NANOSECONDS = 1_000_000_000

#: sha256 of zero bytes. A quiet lane still seals an empty segment, and this is
#: what its digest is.
EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


class SegmentError(RuntimeError):
    """A segment could not be opened, written, or sealed."""


def window_start_ns(now_ns: int, segment_seconds: int) -> int:
    """Floors a receive time to its UTC-aligned window start.

    Alignment is to the Unix epoch, which is midnight UTC, so any period that
    divides a day evenly produces windows that also align to the day. That is
    why `Spool` refuses periods which do not.
    """
    if segment_seconds <= 0:
        raise ValueError("segment_seconds must be positive")
    period = segment_seconds * NANOSECONDS
    return (now_ns // period) * period


def segment_filename(start_ns: int, segment_index: int, segment_id: str) -> str:
    """`<window-start>-<index>-<segment-id>`, so a plain sort is capture order.

    Keeps the exact `%Y%m%dT%H%M%S%f` stamp the old per-connection names used,
    with the microseconds always `000000` because windows are aligned. That is
    deliberate rather than tidy: a shorter spelling such as `20260730T163000Z`
    sorts *after* a legacy `20260730T163001000000` name, because `Z` (0x5A)
    exceeds `1` (0x31).

    **The index is load-bearing, not decoration.** A window normally holds one
    segment, but a restart inside a window necessarily produces a second, since
    a process only ever appends to a segment it opened. With the random id alone
    deciding order, the two sorted arbitrarily — and a live crash-recovery run
    produced `delivery_index` reading 1..3, 300..549, 4..299, 550..600 in file
    order, breaking the one ordering property this layout does guarantee: that
    within a lane, file order *is* receive order. Zero-padded so it sorts
    numerically as a string.
    """
    moment = datetime.fromtimestamp(start_ns / NANOSECONDS, tz=timezone.utc)
    return f"{moment.strftime('%Y%m%dT%H%M%S%f')}-{segment_index:03d}-{segment_id}"


def next_segment_index(root: Path, lane: str, start_ns: int) -> int:
    """How many segments this window already has on disk.

    Read from the filesystem rather than kept in memory, because the case it
    exists for is a *new process* opening a window a dead one had already
    started. In-process state cannot know about that.
    """
    directory = segment_directory(root, lane, start_ns)
    if not directory.is_dir():
        return 0
    moment = datetime.fromtimestamp(start_ns / NANOSECONDS, tz=timezone.utc)
    stamp = moment.strftime("%Y%m%dT%H%M%S%f")
    # Data files only. A segment owns two or three paths — `.ndjson`, its
    # `.seal.json`, and `.ndjson.open` before sealing — and counting paths rather
    # than segments would skip an index on every restart.
    return sum(
        1
        for path in directory.glob(f"{stamp}-*")
        if path.name.endswith(".ndjson") or path.name.endswith(OPEN_SUFFIX)
    )


def segment_directory(root: Path, lane: str, start_ns: int) -> Path:
    moment = datetime.fromtimestamp(start_ns / NANOSECONDS, tz=timezone.utc)
    return Path(root) / f"lane={lane}" / f"date={moment.strftime('%Y-%m-%d')}"


def seal_path_for(data_path: Path) -> Path:
    """The sidecar beside a sealed segment, or beside one still open."""
    name = data_path.name
    for suffix in (OPEN_SUFFIX, ".ndjson"):
        if name.endswith(suffix):
            return data_path.with_name(name[: -len(suffix)] + SEAL_SUFFIX)
    raise SegmentError(f"not a segment path: {data_path}")


@dataclass(frozen=True)
class Seal:
    """What a sealed segment asserts about itself.

    `sha256` is a plain digest of the file bytes, **not domain-separated**, even
    though this repository domain-separates hashes elsewhere. It has to equal
    `replay.stream.ObjectIdentity.sha256` exactly, because that is what makes
    `build_input_manifest` a ready-made seal verifier rather than a second
    implementation to keep in step.
    """

    seal_version: int
    lane_id: str
    window_start_ns: int
    window_end_ns: int
    data_file: str
    byte_length: int
    line_count: int
    sha256: str
    first_delivery_index: int | None
    last_delivery_index: int | None
    first_visible_ns: int | None
    last_visible_ns: int | None
    visible_non_decreasing: bool
    delivery_index_dense: bool
    segment_id: str
    #: Which segment this is for its window. Normally 0. A restart inside a
    #: window necessarily produces a second one, because a process only ever
    #: appends to a segment it opened.
    segment_index: int
    seal_reason: str
    #: `ok`, or `visible_clock_regression` per §2 step 2.
    ordering_status: str
    #: Every connection epoch whose records landed here, in order. A segment
    #: spanning epochs is the point of the design, so it is recorded rather than
    #: left to be recovered by reparsing.
    epochs: tuple[str, ...]
    repaired_bytes: int
    created_ns: int
    writer_version: int
    extra: dict[str, Any] = field(default_factory=dict)

    def as_record(self) -> dict[str, Any]:
        record = {
            "seal_version": self.seal_version,
            "lane_id": self.lane_id,
            "window_start_ns": self.window_start_ns,
            "window_end_ns": self.window_end_ns,
            "data_file": self.data_file,
            "byte_length": self.byte_length,
            "line_count": self.line_count,
            "sha256": self.sha256,
            "first_delivery_index": self.first_delivery_index,
            "last_delivery_index": self.last_delivery_index,
            "first_visible_ns": self.first_visible_ns,
            "last_visible_ns": self.last_visible_ns,
            "visible_non_decreasing": self.visible_non_decreasing,
            "delivery_index_dense": self.delivery_index_dense,
            "segment_id": self.segment_id,
            "segment_index": self.segment_index,
            "seal_reason": self.seal_reason,
            "ordering_status": self.ordering_status,
            "epochs": list(self.epochs),
            "repaired_bytes": self.repaired_bytes,
            "created_ns": self.created_ns,
            "writer_version": self.writer_version,
        }
        record.update(self.extra)
        return record


def read_seal(path: Path) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SegmentError(f"unreadable seal {path}: {error}") from error


@dataclass
class _Accumulator:
    """Everything the seal asserts, maintained as bytes are written.

    Computed incrementally so sealing never rereads the file — §3's requirement,
    and at a gigabyte per window the difference between a metadata write and a
    full pass over the disk.
    """

    digest: Any = field(default_factory=hashlib.sha256)
    byte_length: int = 0
    line_count: int = 0
    first_delivery_index: int | None = None
    last_delivery_index: int | None = None
    first_visible_ns: int | None = None
    last_visible_ns: int | None = None
    visible_non_decreasing: bool = True
    delivery_index_dense: bool = True
    epochs: list[str] = field(default_factory=list)

    #: The last receive time the *previous* segment ended on, so the very first
    #: record of a new one is still checked. §2 requires the restart boundary be
    #: validated; without this seed the first record of every segment has nothing
    #: to compare against and a clock that stepped backwards across a restart
    #: would be recorded as a clean start.
    previous_visible_ns: int | None = None

    def observe(self, line: bytes, visible_ns: int, delivery_index: int, epoch: str) -> None:
        self.digest.update(line)
        self.byte_length += len(line)
        self.line_count += 1
        preceding = self.last_visible_ns if self.line_count > 1 else self.previous_visible_ns
        if preceding is not None and visible_ns < preceding:
            self.visible_non_decreasing = False
        if self.first_delivery_index is None:
            self.first_delivery_index = delivery_index
            self.first_visible_ns = visible_ns
        elif delivery_index != (self.last_delivery_index or 0) + 1:
            self.delivery_index_dense = False
        self.last_delivery_index = delivery_index
        self.last_visible_ns = visible_ns
        if not self.epochs or self.epochs[-1] != epoch:
            self.epochs.append(epoch)


@dataclass(frozen=True)
class Record:
    """One encoded envelope plus the three fields the seal needs from it.

    Carried alongside rather than reparsed: the producer already has them, and
    re-decoding every line on the writer thread would spend the cost twice.
    """

    line: bytes
    visible_ns: int
    delivery_index: int
    epoch: str


class SegmentWriter:
    """Owns exactly one `.ndjson.open` handle and everything derived from it."""

    def __init__(
        self,
        root: Path,
        lane: str,
        start_ns: int,
        *,
        segment_seconds: int = DEFAULT_SEGMENT_SECONDS,
        segment_index: int = 0,
        fsync_interval_seconds: float = 0.25,
        segment_id: str | None = None,
        previous_visible_ns: int | None = None,
        monotonic: Any = time.monotonic,
    ) -> None:
        self.root = Path(root)
        self.lane = lane
        self.start_ns = start_ns
        self.segment_seconds = segment_seconds
        self.end_ns = start_ns + segment_seconds * NANOSECONDS
        self.segment_index = segment_index
        self.segment_id = segment_id or secrets.token_hex(4)
        self.fsync_interval_seconds = float(fsync_interval_seconds)
        self._monotonic = monotonic

        directory = segment_directory(self.root, lane, start_ns)
        directory.mkdir(parents=True, exist_ok=True)
        stem = segment_filename(start_ns, self.segment_index, self.segment_id)
        self.open_path = directory / f"{stem}{OPEN_SUFFIX}"
        self.data_path = directory / f"{stem}.ndjson"
        self.seal_path = directory / f"{stem}{SEAL_SUFFIX}"
        if self.open_path.exists() or self.data_path.exists():
            raise SegmentError(f"segment already exists: {self.open_path}")

        # Unbuffered by design. If a buffered flush writes a prefix and then
        # raises, Python keeps an opaque unwritten suffix in user-space and a
        # retry cannot know which bytes are already on disk. FileIO makes every
        # successful byte count explicit, so a failed batch can be truncated
        # back to the accumulator's last committed offset before it is retried.
        self._handle = self.open_path.open("ab", buffering=0)
        self._accumulator = _Accumulator(previous_visible_ns=previous_visible_ns)
        self._last_fsync = self._monotonic()
        self._unsynced = 0
        self._write_failure: Exception | None = None
        self.repaired_bytes = 0
        self.ordering_status = "ok"
        self.sealed: Seal | None = None

    # -- writing -----------------------------------------------------------

    def write_batch(self, records: Sequence[Record]) -> int:
        """Writes a batch as one call, then folds it into the seal state.

        The accumulator advances only *after* the write returns. Updating it
        per-record beforehand would leave the digest describing bytes that a
        partial write never put on disk, and a digest that disagrees with the
        file is worse than no digest at all — it would fail verification later
        and be indistinguishable from corruption.
        """
        if self._handle is None:
            raise SegmentError("segment is already sealed")
        if self._write_failure is not None:
            raise SegmentError("segment is unsafe after a failed batch rollback") from self._write_failure
        if not records:
            return 0
        payload = b"".join(record.line for record in records)
        committed_offset = self._accumulator.byte_length
        try:
            remaining = memoryview(payload)
            while remaining:
                written = self._handle.write(remaining)
                if written is None or written <= 0:
                    raise OSError(f"segment write made no progress at byte {len(payload) - len(remaining)}")
                remaining = remaining[written:]
            self._handle.flush()
        except Exception as write_error:
            try:
                self._handle.truncate(committed_offset)
                self._handle.seek(committed_offset, os.SEEK_SET)
                self._handle.flush()
                # A crash after an in-memory rollback must not resurrect the
                # partial prefix during orphan recovery. Persist the truncation
                # before allowing LaneWriter to retry the retained batch.
                os.fsync(self._handle.fileno())
            except Exception as rollback_error:
                self._write_failure = rollback_error
                raise SegmentError(
                    f"batch write failed and the segment could not roll back to "
                    f"byte {committed_offset}: {rollback_error}"
                ) from write_error
            raise
        for record in records:
            self._accumulator.observe(
                record.line, record.visible_ns, record.delivery_index, record.epoch
            )
            if not self._accumulator.visible_non_decreasing:
                self.ordering_status = "visible_clock_regression"
        self._unsynced += len(payload)
        self.fsync_if_due()
        return len(payload)

    def fsync_if_due(self, *, force: bool = False) -> bool:
        """Fsyncs on the interval, or when the caller knows it has gone quiet.

        `force` exists because interval-only fsyncing has a hole: a lane that
        receives one record and then falls silent keeps that record in page
        cache indefinitely, since nothing but a later append can trigger the
        check. The writer calls this with `force` whenever it drains the queue
        to empty, which bounds the exposure to the time between arrivals rather
        than leaving it unbounded.
        """
        if self._handle is None or self._unsynced == 0:
            return False
        now = self._monotonic()
        if not force and now - self._last_fsync < self.fsync_interval_seconds:
            return False
        os.fsync(self._handle.fileno())
        self._last_fsync = now
        self._unsynced = 0
        return True

    # -- sealing -----------------------------------------------------------

    def seal(self, reason: str, *, extra: dict[str, Any] | None = None) -> Seal:
        """The five-step commit from §3, plus one extra directory fsync.

        1. stop assigning — the handle is dropped before any I/O below;
        2. flush and fsync the data file;
        3. atomically rename `.ndjson.open` to `.ndjson`;
        4. write the seal through a temporary file and rename;
        5. fsync the containing directory.

        **Two directory fsyncs, not one.** The spec's single trailing fsync
        leaves a window in which a crash between steps 4 and 5 makes the seal
        durable while the rename is not — producing a seal that names a file
        which does not exist under that name. Syncing after step 3 as well
        removes that case for one extra fsync per window. The remaining window,
        between 3 and 4, yields an `.ndjson` with no seal, which §3 designs for
        explicitly: an unsealed segment is never eligible, and recovery reseals
        it.
        """
        if self.sealed is not None:
            return self.sealed
        handle, self._handle = self._handle, None
        if handle is not None:
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()
            os.replace(self.open_path, self.data_path)
            _fsync_directory(self.data_path.parent)

        accumulator = self._accumulator
        seal = Seal(
            seal_version=SEAL_VERSION,
            lane_id=self.lane,
            window_start_ns=self.start_ns,
            window_end_ns=self.end_ns,
            data_file=self.data_path.name,
            byte_length=accumulator.byte_length,
            line_count=accumulator.line_count,
            sha256=accumulator.digest.hexdigest(),
            first_delivery_index=accumulator.first_delivery_index,
            last_delivery_index=accumulator.last_delivery_index,
            first_visible_ns=accumulator.first_visible_ns,
            last_visible_ns=accumulator.last_visible_ns,
            visible_non_decreasing=accumulator.visible_non_decreasing,
            delivery_index_dense=accumulator.delivery_index_dense,
            segment_id=self.segment_id,
            segment_index=self.segment_index,
            seal_reason=reason,
            ordering_status=self.ordering_status,
            epochs=tuple(accumulator.epochs),
            repaired_bytes=self.repaired_bytes,
            created_ns=time.time_ns(),
            writer_version=WRITER_VERSION,
            extra=dict(extra or {}),
        )
        _write_seal(self.seal_path, seal)
        self.sealed = seal
        return seal

    # -- introspection -----------------------------------------------------

    @property
    def line_count(self) -> int:
        return self._accumulator.line_count

    @property
    def byte_length(self) -> int:
        return self._accumulator.byte_length

    @property
    def epochs(self) -> tuple[str, ...]:
        return tuple(self._accumulator.epochs)

    def covers(self, now_ns: int) -> bool:
        return self.start_ns <= now_ns < self.end_ns


def _write_seal(path: Path, seal: Seal) -> None:
    """Temporary file, fsync, rename, fsync directory — steps 4 and 5."""
    temporary = path.with_name(f".{path.name}.tmp")
    encoded = json.dumps(seal.as_record(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    with temporary.open("w", encoding="utf-8") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    # Linux supports and requires fsync on a directory for rename durability.
    # This repository deploys on Linux, so hiding an error here would let
    # `seal()` report a commit whose data or sidecar name can disappear after a
    # crash. O_DIRECTORY also prevents accidentally syncing an unexpected path.
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def parse_segment_name(name: str) -> tuple[int, int, str]:
    """`<stamp>-<index>-<id>` back into `(window_start_ns, index, segment_id)`."""
    stem = name
    for suffix in (OPEN_SUFFIX, SEAL_SUFFIX, ".ndjson"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    parts = stem.split("-")
    if len(parts) != 3:
        raise SegmentError(f"unparseable segment name {name!r}: expected stamp-index-id")
    stamp, index, segment_id = parts
    try:
        moment = datetime.strptime(stamp, "%Y%m%dT%H%M%S%f").replace(tzinfo=timezone.utc)
        segment_index = int(index)
    except ValueError as error:
        raise SegmentError(f"unparseable segment name {name!r}: {error}") from error
    return int(moment.timestamp() * NANOSECONDS), segment_index, segment_id


def repair_torn_tail(path: Path) -> int:
    """Drops a partial final line, returning the bytes removed.

    A record is durable only once its terminating newline is on disk, so a file
    not ending in one was interrupted mid-write. Truncating to the last complete
    line is the only repair that preserves the invariant every reader depends
    on — that a spool is a sequence of whole records — and it discards a record
    the splice never counted as written.

    **This is the single exception to never mutating a spool**, and it can only
    remove bytes no reader was entitled to interpret. It therefore refuses a
    sealed segment outright: those bytes have a committed digest, and truncating
    them would turn a durable record into a verification failure.
    """
    path = Path(path)
    if path.name.endswith(".ndjson") and seal_path_for(path).exists():
        raise SegmentError(f"refusing to repair a sealed segment: {path}")
    if not path.exists() or path.stat().st_size == 0:
        return 0
    with path.open("rb+") as handle:
        handle.seek(-1, os.SEEK_END)
        if handle.read(1) == b"\n":
            return 0
        handle.seek(0)
        content = handle.read()
        cut = content.rfind(b"\n")
        keep = cut + 1 if cut >= 0 else 0
        handle.seek(keep)
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())
        return len(content) - keep


def seal_orphan(
    path: Path,
    lane: str,
    *,
    segment_seconds: int = DEFAULT_SEGMENT_SECONDS,
    reason: str = "recovery",
) -> Seal:
    """Seals a segment whose writer died, rereading it to rebuild the digest.

    The one place a full reread is correct. §3's "sealing does not reread a large
    file" governs the boundary path, where the digest was maintained as the bytes
    were written; here the process that maintained it is gone, and the bytes on
    disk are the only remaining source of truth.
    """
    path = Path(path)
    repaired = repair_torn_tail(path)
    start_ns, segment_index, segment_id = parse_segment_name(path.name)
    # Handles both crash shapes: an `.ndjson.open` whose writer died, and a
    # `.ndjson` already renamed but not yet sealed — the window between §3's
    # steps 3 and 4, which the design leaves open on purpose and expects
    # recovery to close.
    already_renamed = path.name.endswith(".ndjson")
    data_path = path if already_renamed else path.with_name(
        path.name[: -len(OPEN_SUFFIX)] + ".ndjson"
    )

    accumulator = _Accumulator()
    ordering_status = "ok"
    with path.open("rb") as handle:
        for line in handle:
            document = json.loads(line)
            accumulator.observe(
                line,
                int(document["visible_ns"]),
                int(document["delivery_index"]),
                str(document["connection_epoch"]),
            )
            if not accumulator.visible_non_decreasing:
                ordering_status = "visible_clock_regression"

    # Fsync the data before publishing it under its committed name, even when the
    # tail was already complete and `repair_torn_tail` had nothing to truncate. A
    # dead process's bytes can sit entirely in page cache; renaming first and
    # sealing after would publish a digest for content a second crash could still
    # lose, which is the one thing a commit marker must never do.
    with path.open("rb") as handle:
        os.fsync(handle.fileno())
    if not already_renamed:
        os.replace(path, data_path)
        _fsync_directory(data_path.parent)
    seal = Seal(
        seal_version=SEAL_VERSION,
        lane_id=lane,
        window_start_ns=start_ns,
        window_end_ns=start_ns + segment_seconds * NANOSECONDS,
        data_file=data_path.name,
        byte_length=accumulator.byte_length,
        line_count=accumulator.line_count,
        sha256=accumulator.digest.hexdigest(),
        first_delivery_index=accumulator.first_delivery_index,
        last_delivery_index=accumulator.last_delivery_index,
        first_visible_ns=accumulator.first_visible_ns,
        last_visible_ns=accumulator.last_visible_ns,
        visible_non_decreasing=accumulator.visible_non_decreasing,
        delivery_index_dense=accumulator.delivery_index_dense,
        segment_id=segment_id,
        segment_index=segment_index,
        seal_reason=reason,
        ordering_status=ordering_status,
        epochs=tuple(accumulator.epochs),
        repaired_bytes=repaired,
        created_ns=time.time_ns(),
        writer_version=WRITER_VERSION,
    )
    _write_seal(seal_path_for(data_path), seal)
    return seal


def sealed_segments(root: Path, lane: str) -> list[Path]:
    """Every *sealed* segment for a lane, in window order.

    A `.ndjson` whose seal is absent is excluded. §3 makes the sidecar the commit
    marker, not the suffix: the rename happens before the seal is written, so a
    crash in that window leaves a complete-looking data file that no reader is
    entitled to treat as evidence. Recovery seals it; until then it is invisible.

    Timestamp-prefixed names mean a lexicographic sort is window order, so this
    needs no index of its own.
    """
    return sorted(
        (path for path in all_segments(root, lane) if seal_path_for(path).exists()),
        key=lambda path: (path.parent.name, path.name),
    )


def all_segments(root: Path, lane: str) -> list[Path]:
    """Every renamed segment, sealed or not. Recovery's input."""
    directory = Path(root) / f"lane={lane}"
    if not directory.is_dir():
        return []
    return sorted(
        (path for path in directory.glob("date=*/*.ndjson") if path.is_file()),
        key=lambda path: (path.parent.name, path.name),
    )


def unsealed_segments(root: Path, lane: str) -> list[Path]:
    """Renamed segments still missing their commit marker."""
    return [path for path in all_segments(root, lane) if not seal_path_for(path).exists()]


def open_segments(root: Path, lane: str) -> list[Path]:
    """Segments a writer still owns. Expected to be at most one per lane."""
    directory = Path(root) / f"lane={lane}"
    if not directory.is_dir():
        return []
    return sorted(
        (path for path in directory.glob(f"date=*/*{OPEN_SUFFIX}") if path.is_file()),
        key=lambda path: (path.parent.name, path.name),
    )


def iter_records(lines: Iterable[bytes]) -> Iterable[Record]:
    """Decodes encoded envelopes back into `Record`s — recovery paths only."""
    for line in lines:
        document = json.loads(line)
        yield Record(
            line=line,
            visible_ns=int(document["visible_ns"]),
            delivery_index=int(document["delivery_index"]),
            epoch=str(document["connection_epoch"]),
        )
