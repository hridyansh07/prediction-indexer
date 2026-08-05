"""The lane writer: a bounded queue, one drain coroutine, one writer thread.

`Spool.append` used to write, flush and sometimes `os.fsync` inline in the frame
receive loop. An fsync there stalls the event loop, so the socket stops being
read for its duration — and the whole reason for a fixed fsync interval is that
fsync is slow. §4 of `docs/SEALED_CAPTURE_PIPELINE_V1.md` asks for the socket to
keep receiving *during* an fsync, which an asyncio-only writer cannot provide:
a coroutine calling `os.fsync` blocks the loop just as surely as the producer
did. Hence a real thread.

```
producer (_emit)            drain coroutine              writer thread
  encode envelope             await queue.get()            SegmentWriter
  await queue.put()   ──▶     batch greedily        ──▶      write / fsync / seal
                              run_in_executor
```

**Backpressure, never loss.** The queue is bounded and the producer awaits, so a
storage stall suspends the receive loop, the kernel socket buffer absorbs what
arrives, and eventually TCP advertises a zero window. That is a bounded budget —
about ten seconds at Kalshi's rate against a 6 MB `rcvbuf`, per §4's stall
budget — and past it the venue disconnects us. What must never happen is a
*self-inflicted* reconnect: queue fullness is a storage fault, and reconnecting
the socket cannot repair a disk, it only opens an unobservable loss window on a
venue that publishes no sequence.

The rotation barrier travels through the same FIFO as the records, so a segment
boundary cannot overtake writes that belong before it. If the disk is stalled,
the seal waits — which is correct, since a segment whose bytes have not landed
cannot be sealed.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from splices.common.segment import (
    DEFAULT_SEGMENT_SECONDS,
    NANOSECONDS,
    Record,
    Seal,
    SegmentError,
    SegmentWriter,
    next_segment_index,
    window_start_ns,
)

__all__ = ["DEFAULT_QUEUE_CAPACITY", "WRITER_BATCH", "LaneWriter", "validate_segment_seconds"]

#: Records handed to the writer thread per hop. Matches the Rust ingester's
#: `BATCH`, for the same reason: large enough that the per-call cost amortises,
#: small enough that a crash re-reads seconds rather than minutes.
WRITER_BATCH = 512

#: ~25 seconds of Kalshi at its measured steady rate of 811 records/second, or
#: about 14 MB per lane at ~700 bytes per record.
#:
#: Provisional. §4 requires this be re-derived on Linux over a longer window, and
#: `queue_high_water` is carried on every seal so that re-derivation has data
#: rather than a guess behind it.
DEFAULT_QUEUE_CAPACITY = 20_000

SECONDS_PER_DAY = 86_400


def validate_segment_seconds(seconds: int) -> int:
    """Windows must tile the UTC day exactly.

    Alignment is to the Unix epoch, so a period that does not divide a day
    produces windows that drift across midnight — and the `date=` partition,
    which is derived from the window start, would then disagree with the records
    inside it.
    """
    if seconds <= 0 or SECONDS_PER_DAY % seconds != 0:
        raise ValueError(
            f"segment_seconds must be a positive divisor of {SECONDS_PER_DAY}, got {seconds}"
        )
    return seconds


@dataclass(frozen=True)
class _Rotate:
    """A segment boundary, travelling in-band so it cannot overtake data."""

    start_ns: int


class LaneWriter:
    """Owns one lane's segments, its queue, and the thread that touches disk."""

    def __init__(
        self,
        root: Path,
        lane: str,
        *,
        segment_seconds: int = DEFAULT_SEGMENT_SECONDS,
        queue_capacity: int = DEFAULT_QUEUE_CAPACITY,
        fsync_interval_seconds: float = 0.25,
        clock: Any = time.time_ns,
        start_ns: int | None = None,
        previous_visible_ns: int | None = None,
    ) -> None:
        self.root = Path(root)
        self.lane = lane
        self.segment_seconds = validate_segment_seconds(int(segment_seconds))
        self.queue_capacity = int(queue_capacity)
        self.fsync_interval_seconds = float(fsync_interval_seconds)
        self._clock = clock

        self._queue: asyncio.Queue[Record | _Rotate] = asyncio.Queue(maxsize=self.queue_capacity)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"spool-{lane}")
        self._drain: asyncio.Task[None] | None = None
        self._rotation: asyncio.Task[None] | None = None
        self._closed = False

        self.seals: list[Seal] = []
        self.queue_high_water = 0
        self.queue_full_events = 0
        self.records_written = 0
        #: Records the disk refused. Retained rather than dropped so a seal can
        #: never be published past a record the queue already accepted.
        self._unwritten: list[Record] = []
        self._fatal: Exception | None = None
        #: Where the previous run's tape ended, so §2's restart boundary check
        #: covers the very first record of this one.
        self._previous_visible_ns = previous_visible_ns

        opening = start_ns if start_ns is not None else window_start_ns(
            self._clock(), self.segment_seconds
        )
        self._segment = self._open(opening)

    # -- lifecycle ---------------------------------------------------------

    def _open(self, start_ns: int) -> SegmentWriter:
        return SegmentWriter(
            self.root,
            self.lane,
            start_ns,
            segment_seconds=self.segment_seconds,
            # Read off disk, not from memory. The case this exists for is a new
            # process opening a window a dead one had already started, which
            # in-process state cannot know about.
            segment_index=next_segment_index(self.root, self.lane, start_ns),
            fsync_interval_seconds=self.fsync_interval_seconds,
            previous_visible_ns=self._previous_visible_ns,
        )

    def start(self) -> None:
        """Starts the drain and rotation tasks. Must run inside a loop."""
        if self._drain is not None:
            return
        self._drain = asyncio.create_task(self._drain_forever())
        self._rotation = asyncio.create_task(self._rotate_forever())

    async def append(self, record: Record) -> None:
        """Hands one record to the writer, blocking while the queue is full.

        The await is deliberately *not* wrapped in a timeout. A timeout here
        would surface as an exception in the receive loop, which the splice's
        blanket handler converts into `connection_failed` and a reconnect — the
        one response to a full queue that this design forbids.
        """
        if self._closed:
            raise RuntimeError("lane writer is closed")
        if self._queue.full():
            self.queue_full_events += 1
        await self._queue.put(record)
        self.queue_high_water = max(self.queue_high_water, self._queue.qsize())

    async def rotate_at(self, start_ns: int) -> None:
        """Queues a segment boundary in-band, exactly as the clock does.

        Public because in-band is the whole property worth testing: a boundary
        that could be applied out of band would let a seal commit before the
        records that belong inside it had landed.
        """
        await self._queue.put(_Rotate(start_ns))

    # -- the loop ----------------------------------------------------------

    async def _rotate_forever(self) -> None:
        while True:
            now = self._clock()
            period = self.segment_seconds * NANOSECONDS
            following = window_start_ns(now, self.segment_seconds) + period
            await asyncio.sleep(max(0.0, (following - now) / NANOSECONDS))
            # Computed from absolute wall time every iteration rather than by
            # sleeping a fixed period, so process suspension and drift correct
            # themselves instead of accumulating.
            await self._queue.put(_Rotate(following))

    async def _drain_forever(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            first = await self._queue.get()
            batch: list[Record | _Rotate] = [first]
            while len(batch) < WRITER_BATCH:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            drained = self._queue.empty()
            await loop.run_in_executor(self._executor, self._apply, batch, drained)

    def _consume(self, items: list[Record | _Rotate]) -> None:
        """Places records in the window their own receive time belongs to.

        Shared by the drain loop and by `close()`. They used to differ: shutdown
        drained the queue straight into `_flush`, so a record that arrived after
        a boundary but before the timer fired was filed under the previous window
        — and its seal then asserted a range that did not contain its own
        records.
        """
        pending: list[Record] = []
        for item in items:
            if isinstance(item, _Rotate):
                self._flush(pending)
                pending = []
                self._rotate_to(item.start_ns)
                continue
            # The timer is a liveness mechanism for quiet lanes; the data itself
            # decides which window it belongs to. Scheduler pressure, a storage
            # stall or a suspended process can all delay the timer past a
            # boundary, and none of them may misfile a record.
            if item.visible_ns >= self._segment.end_ns:
                self._flush(pending)
                pending = []
                self._rotate_to(window_start_ns(item.visible_ns, self.segment_seconds))
            pending.append(item)
        self._flush(pending)

    def _apply(self, batch: list[Record | _Rotate], drained: bool) -> None:
        """Runs on the writer thread. The only place that touches the disk."""
        self._consume(batch)
        if drained:
            # The queue has gone quiet, so nothing will arrive to trigger the
            # interval check. Without this a lane that receives one record and
            # then falls silent keeps it in page cache indefinitely.
            self._segment.fsync_if_due(force=True)

    def _flush(self, records: list[Record]) -> None:
        """Writes a batch, retaining it if the disk refuses.

        A record the queue accepted has already been counted by the producer's
        `delivery_index`. If the write fails and the batch were simply dropped,
        `close()` would go on to publish a seal whose digest and line count omit
        it — a silent loss wearing a commit marker, which is the worst failure
        this design can produce. So the batch is retained and the error latched:
        a later flush retries it, and `close()` refuses to seal while anything
        remains unwritten.
        """
        if not records:
            return
        try:
            self._segment.write_batch(records)
        except Exception as error:  # noqa: BLE001 - retained, re-raised, never dropped
            self._unwritten.extend(records)
            self._fatal = error
            raise
        self.records_written += len(records)

    def _rotate_to(self, start_ns: int) -> None:
        seal = self._segment.seal("boundary", extra=self._metrics())
        self.seals.append(seal)
        # Carry the boundary forward so the next segment's first record is
        # checked against the last one's, not against nothing.
        if seal.last_visible_ns is not None:
            self._previous_visible_ns = seal.last_visible_ns
        self._segment = self._open(start_ns)

    def _metrics(self) -> dict[str, Any]:
        return {
            "queue_capacity": self.queue_capacity,
            "queue_high_water": self.queue_high_water,
            "queue_full_events": self.queue_full_events,
            "fsync_interval_seconds": self.fsync_interval_seconds,
        }

    # -- shutdown ----------------------------------------------------------

    def close(self, reason: str = "shutdown") -> Seal | None:
        """Drains, seals, and stops — synchronously, and callable while cancelling.

        Synchronous on purpose. The splice's shutdown path runs inside a
        `finally` during cancellation, where awaiting anything is unreliable:
        the task is already being torn down. Every existing test that cancels
        `run()` and then reads records back depends on this working, and so does
        the SIGINT handler in production.

        Order matters. The executor is shut down *before* the queue is drained
        here, so an in-flight batch finishes on the writer thread and this thread
        never races it for the same file handle.
        """
        if self._closed:
            return self.seals[-1] if self.seals else None
        self._closed = True

        for task in (self._rotation, self._drain):
            if task is not None:
                task.cancel()
        self._rotation = self._drain = None

        self._executor.shutdown(wait=True)

        # Anything a previous flush could not write goes first: it was accepted
        # before everything still queued, and order is the point.
        remaining: list[Record | _Rotate] = list(self._unwritten)
        self._unwritten = []
        while True:
            try:
                remaining.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        self._consume(remaining)

        if self._unwritten:
            # The retry failed too. Refusing to seal is the whole point: a seal
            # is a claim that these bytes are complete, and publishing one while
            # holding records it omits would convert a visible storage fault into
            # an invisible hole in the tape.
            raise SegmentError(
                f"{len(self._unwritten)} accepted record(s) could not be written; "
                f"refusing to seal {self._segment.data_path.name}"
            ) from self._fatal

        seal = self._segment.seal(reason, extra=self._metrics())
        self.seals.append(seal)
        return seal

    @property
    def segment(self) -> SegmentWriter:
        return self._segment

    @property
    def pending(self) -> int:
        return self._queue.qsize()
