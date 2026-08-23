"""The boundary between the Python capture half and the Rust sequencing half.

A file rather than a socket, for one reason: a socket makes the boundary lossy
exactly when things go wrong. A spool inverts that — the splice fsyncs bytes it
owns, and the ingester is free to be absent for an hour without costing a frame.

So the spool *is* the raw evidence and the ingester's store is derived from it.
Losing the store costs a rebuild; losing the spool is unrecoverable, which is why
nothing here ever rewrites or truncates a committed line.

Layout is hive-partitioned by **lane**, not venue:

    <root>/lane=<lane>/date=<YYYY-MM-DD>/<window-start>-<segment-id>.ndjson

A lane is one splice process. Polymarket runs four of them and every record from
all four says `venue: polymarket` in its envelope, so the partition key and the
envelope field answer different questions and must not share a name.

**One file per UTC-aligned window, not per connection.** A reconnect no longer
rolls a file, so a segment deliberately holds several connection epochs; the
`connection_epoch` on every record is the marker between their independent
`local_counter` runs, and both readers already key continuity on it. What a
segment gains in exchange is a *seal*: a checksummed sidecar written after the
data is durable, which is the first thing in this system able to say "this file
is finished" — a distinction the old layout could not express, because a closed
file was byte-identical to an open one.

`Spool` is a thin facade. `segment.py` owns one file and its seal, `writer.py`
owns the queue and the thread that touches disk. See
`docs/SEALED_CAPTURE_PIPELINE_V1.md` §3 and §4.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from splices.common.envelope import encode_envelope
from splices.common.segment import (
    DEFAULT_SEGMENT_SECONDS,
    OPEN_SUFFIX,
    Record,
    Seal,
    SegmentError,
    open_segments,
    unsealed_segments,
    read_seal,
    repair_torn_tail,
    seal_orphan,
    seal_path_for,
    sealed_segments,
)
from splices.common.writer import DEFAULT_QUEUE_CAPACITY, LaneWriter, validate_segment_seconds

__all__ = [
    "DEFAULT_FSYNC_INTERVAL_SECONDS",
    "ResumeState",
    "Spool",
    "SpoolError",
    "repair_torn_tail",
    "resume_state",
    "spool_files",
]

#: Bytes we are willing to lose to a power cut. A frame per fsync would cap the
#: splice at disk-latency throughput, which a busy crypto ladder exceeds; a long
#: interval turns a crash into a visible hole. The interval is recorded in the
#: connection's control record so a gap of this size is explainable after the
#: fact rather than mysterious.
#:
#: Since the writer moved to its own thread this no longer costs the socket
#: anything, so the interval trades only durability against disk load.
DEFAULT_FSYNC_INTERVAL_SECONDS = 0.25


class SpoolError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResumeState:
    """What the tape says about where a restarting splice should continue."""

    next_delivery_index: int
    repaired_bytes: int
    #: The last receive time on the previous run, for §2's restart boundary
    #: check. `None` on a fresh tree.
    last_visible_ns: int | None
    #: How the answer was reached, so a slow start is explainable.
    source: str


class Spool:
    """One lane's tape. Records in, sealed segments out."""

    def __init__(
        self,
        root: Path,
        lane: str,
        *,
        fsync_interval_seconds: float = DEFAULT_FSYNC_INTERVAL_SECONDS,
        segment_seconds: int = DEFAULT_SEGMENT_SECONDS,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
        clock: Any = time.time_ns,
    ) -> None:
        self.root = Path(root)
        self.lane = lane
        self.fsync_interval_seconds = float(fsync_interval_seconds)
        self.segment_seconds = validate_segment_seconds(int(segment_seconds))
        self.queue_capacity = int(queue_capacity)
        self._clock = clock

        self._writer: LaneWriter | None = None
        self._epoch = ""
        self._opened_epochs: set[str] = set()
        self.records_written = 0

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        """Opens the first segment and starts the writer. Needs a running loop.

        Reads the resume state itself rather than being handed it, so the
        restart clock boundary travels with the spool no matter who constructed
        it. `resume_state` is idempotent — it recovery-seals orphans on the first
        call and finds none on the second — so a caller that already asked pays
        only a directory listing.
        """
        if self._writer is not None:
            return
        self._writer = LaneWriter(
            self.root,
            self.lane,
            segment_seconds=self.segment_seconds,
            queue_capacity=self.queue_capacity,
            fsync_interval_seconds=self.fsync_interval_seconds,
            clock=self._clock,
            previous_visible_ns=resume_state(self.root, self.lane).last_visible_ns,
        )
        self._writer.start()

    def begin_epoch(self, epoch: str) -> None:
        """Marks a new connection. Touches no file — segments span epochs now.

        The reuse guard survives the layout change and matters more than before.
        It used to prevent two epochs sharing a file with no marker between their
        ascending `local_counter` runs; a segment now holds several epochs *by
        design*, and `connection_epoch` is that marker. Minting the same epoch
        twice would therefore produce two indistinguishable counter runs inside
        one segment, which is exactly the ambiguity the old file boundary existed
        to prevent.

        Tracked in-process because that is where the only realistic cause lives:
        epochs are UUIDs, so a collision across restarts does not happen, but a
        bug reusing one inside a single splice would.
        """
        if epoch in self._opened_epochs:
            raise SpoolError(f"epoch already used by this splice: {epoch}")
        self._opened_epochs.add(epoch)
        self._epoch = epoch

    async def append(self, record: dict[str, Any]) -> int:
        """Encodes one envelope and hands it to the writer, awaiting if full.

        Returns the encoded byte length. The await is the backpressure point:
        while the queue is full this suspends the caller, which stops the socket
        being read, which is a bounded and *observable* stall rather than a
        silent drop. See `writer.py` for why fullness must never reconnect.
        """
        if self._writer is None:
            raise SpoolError("spool has not been started")
        line = encode_envelope(record)
        await self._writer.append(
            Record(
                line=line,
                visible_ns=int(record["visible_ns"]),
                delivery_index=int(record["delivery_index"]),
                epoch=str(record["connection_epoch"]),
            )
        )
        self.records_written += 1
        return len(line)

    async def wait_for_writer_failure(self) -> None:
        """Propagates a background storage failure to the splice run task."""
        if self._writer is None:
            raise SpoolError("spool has not been started")
        await self._writer.wait_failed()

    def sync(self) -> None:
        """Best-effort durability point for the cancellation path.

        `close()` is the real one and always runs after this; this exists so a
        cancelled splice has flushed before the closing record is written.
        """
        if self._writer is not None:
            self._writer.segment.fsync_if_due(force=True)

    def close(self, reason: str = "shutdown") -> Seal | None:
        """Drains, seals, and stops. Synchronous, and safe to call twice.

        Synchronous because it runs inside a `finally` during cancellation,
        where awaiting is unreliable — the task is already being torn down. The
        SIGINT path in `splices/run.py` and every test that cancels `run()` and
        then reads records back both depend on that.
        """
        if self._writer is None:
            return None
        seal = self._writer.close(reason)
        return seal

    # -- introspection -----------------------------------------------------

    @property
    def path(self) -> Path | None:
        return self._writer.segment.open_path if self._writer else None

    @property
    def seals(self) -> list[Seal]:
        return self._writer.seals if self._writer else []

    def metrics(self) -> dict[str, Any]:
        if self._writer is None:
            return {}
        return {
            "segment_seconds": self.segment_seconds,
            "queue_capacity": self.queue_capacity,
            "queue_high_water": self._writer.queue_high_water,
            "queue_full_events": self._writer.queue_full_events,
            "segments_sealed": len(self._writer.seals),
        }

    def __enter__(self) -> Spool:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


def spool_files(root: Path, lane: str, *, include_open: bool = False) -> list[Path]:
    """Every segment for a lane, in window order.

    Sealed only by default. An `.ndjson.open` has no committed length and no
    digest, so a reader treating it as evidence would be reading bytes the writer
    has not finished claiming.
    """
    found = sealed_segments(root, lane)
    if include_open:
        found = sorted(
            found + open_segments(root, lane),
            key=lambda path: (path.parent.name, path.name),
        )
    return found


def resume_state(root: Path, lane: str) -> ResumeState:
    """Recovers where to continue, from seals rather than from every file.

    `delivery_index` is dense across the splice's whole lifetime, not per
    connection, so a restart has to continue the count rather than restart it.

    The old implementation read that from the tape directly, opening and
    back-scanning every file the lane had ever written — O(all files), which at
    48 segments a day stops being viable within a week. Its docstring argued
    against a sidecar on the grounds that "a sidecar state file could disagree
    with the tape after a crash, and then the tape would be the thing that was
    wrong." That objection is correct about a *mutable rolling* state file and it
    is not what a seal is:

    * A seal is written only after its data file is fsynced and renamed, so it
      cannot claim a record that is not on disk. The reachable inconsistency is
      the opposite one — a complete file with no seal — and that is repaired
      here by sealing it.
    * There is one immutable seal per closed segment describing exactly those
      bytes. There is no single "current state" for the tape to contradict.
    * It carries `byte_length` and `sha256`, so it is falsifiable against the
      tape, and this function falls back to reading the tape when it is falsified.

    The tape remains the authority. The seal is a verifiable index over it, and
    the one segment that has no seal — the `.open` — is still read directly.
    """
    repaired = 0
    # Both crash shapes. An `.ndjson.open` is a writer that died mid-window; a
    # renamed `.ndjson` with no sidecar is the narrow window between §3's steps 3
    # and 4, which the design leaves open deliberately and expects recovery to
    # close. Leaving the second orphaned would hide a complete segment from every
    # reader forever, since the seal — not the suffix — is the commit marker.
    for orphan in [*open_segments(root, lane), *unsealed_segments(root, lane)]:
        seal = seal_orphan(orphan, lane)
        repaired += seal.repaired_bytes

    best: dict[str, Any] | None = None
    unsealed: list[Path] = []
    for segment in sealed_segments(root, lane):
        sidecar = seal_path_for(segment)
        if not sidecar.exists():
            unsealed.append(segment)
            continue
        try:
            record = read_seal(sidecar)
        except SegmentError:
            unsealed.append(segment)
            continue
        if segment.stat().st_size != record.get("byte_length"):
            unsealed.append(segment)
            continue
        if record.get("last_delivery_index") is None:
            continue
        if best is None or record["last_delivery_index"] > best["last_delivery_index"]:
            best = record

    if unsealed:
        # A segment whose seal is missing or disagrees with the file on disk.
        # Fall back to reading the bytes: slower, and the only answer that cannot
        # be wrong.
        highest, scanned = _scan_for_highest(root, lane)
        if best is None or highest > best["last_delivery_index"]:
            return ResumeState(highest + 1, repaired, scanned, "full_scan")

    if best is None:
        return ResumeState(1, repaired, None, "empty")
    return ResumeState(
        best["last_delivery_index"] + 1, repaired, best.get("last_visible_ns"), "seal"
    )


def _scan_for_highest(root: Path, lane: str) -> tuple[int, int | None]:
    highest = 0
    last_visible: int | None = None
    for segment in spool_files(root, lane, include_open=True):
        index, visible = _last_record(segment)
        if index > highest:
            highest, last_visible = index, visible
    return highest, last_visible


def _last_record(path: Path) -> tuple[int, int | None]:
    """The final record's indices, scanning backwards from the end.

    Segments reach a gigabyte on a busy day and only the last line matters, so
    this walks back in blocks rather than reading the file into memory.
    """
    size = path.stat().st_size
    if size == 0:
        return 0, None
    block = 8192
    with path.open("rb") as handle:
        buffer = b""
        offset = size
        while offset > 0:
            step = min(block, offset)
            offset -= step
            handle.seek(offset)
            buffer = handle.read(step) + buffer
            lines = [line for line in buffer.split(b"\n") if line.strip()]
            if lines and (offset == 0 or buffer.count(b"\n") > 1):
                try:
                    document = json.loads(lines[-1])
                except json.JSONDecodeError as error:
                    raise SpoolError(f"unreadable final record in {path}: {error}") from error
                return int(document["delivery_index"]), int(document["visible_ns"])
    return 0, None
