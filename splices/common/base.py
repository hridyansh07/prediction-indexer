"""What every splice does the same way, and the four things each one does its own.

A splice's job decomposes cleanly: the connection lifecycle, the counters, and the
tape discipline are identical everywhere, while the transport, the subscribe
message, the heartbeat, and the cursor shape are venue facts. Everything in the
first group lives here so that a new venue cannot accidentally get it wrong, and
everything in the second is an abstract method so a new venue cannot forget it.

The rule the base class exists to enforce is the one that is easiest to violate
under deadline: **a splice does not filter.** Every application message that
arrives becomes exactly one record, verbatim, including heartbeats, including
frames whose shape we do not recognise. `_emit_frame` is the only path to the
tape and it takes no predicate. A venue subclass that wants to drop a message has
to work against this file rather than merely forget to record something.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Protocol

from splices.common.envelope import (
    KIND_CONTROL,
    KIND_FAULT,
    KIND_VENUE_FRAME,
    STREAM_PROCESS,
    build_envelope,
)
from splices.common.clock import CaptureClock
from splices.common.spool import Spool
from targeter.targets import TargetSet, TargetsError, load_targets

LOGGER = logging.getLogger(__name__)


class Transport(Protocol):
    """A venue connection reduced to what the capture loop needs.

    Pull-based on purpose. Some venues push through callbacks rather than a read
    call, and adapting those into `recv` — rather than letting the base class grow
    a second, callback-shaped loop — keeps one implementation of the ordering,
    counting, and lifecycle rules that must not diverge between venues.
    """

    async def send(self, message: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def __aenter__(self) -> Transport: ...
    async def __aexit__(self, *exception: object) -> bool | None: ...


@dataclass(frozen=True)
class BackoffPolicy:
    """Reconnect timing.

    Jitter is not decoration. Every market on a venue reconnects from the same
    outage, so a fixed schedule turns one blip into a synchronised stampede that
    the venue then rate-limits — converting a short outage into a long one.
    """

    initial_seconds: float = 1.0
    maximum_seconds: float = 60.0
    multiplier: float = 2.0
    jitter: float = 0.25

    def delay(self, attempt: int) -> float:
        raw = self.initial_seconds * (self.multiplier ** max(0, attempt - 1))
        capped = min(raw, self.maximum_seconds)
        return capped * (1.0 + random.uniform(-self.jitter, self.jitter))


@dataclass(frozen=True)
class ConnectionOutcome:
    clean: bool
    stop: bool


@dataclass(frozen=True)
class ClockRegressionAlert:
    """The high-priority signal §2 requires before seal-time validation.

    Scope ids are explicit. The previous one is unavailable for the first record
    after a process restart because V1 deliberately does not introduce a
    cross-boot clock epoch; `None` states that limitation rather than pretending
    the current boot scope also described the prior record.
    """

    lane_id: str
    previous_visible_ns: int
    current_visible_ns: int
    previous_delivery_index: int | None
    current_delivery_index: int
    previous_epoch: str | None
    current_epoch: str
    previous_scope_id: str | None
    current_scope_id: str

    def as_record(self) -> dict[str, Any]:
        return {
            "event": "visible_clock_regression",
            "severity": "critical",
            "lane_id": self.lane_id,
            "previous_visible_ns": self.previous_visible_ns,
            "current_visible_ns": self.current_visible_ns,
            "previous_delivery_index": self.previous_delivery_index,
            "current_delivery_index": self.current_delivery_index,
            "previous_epoch": self.previous_epoch,
            "current_epoch": self.current_epoch,
            "previous_scope_id": self.previous_scope_id,
            "current_scope_id": self.current_scope_id,
        }


def _log_clock_regression(alert: ClockRegressionAlert) -> None:
    """Default external notification path: a structured critical container log."""
    LOGGER.critical(
        "capture_clock_regression %s",
        json.dumps(alert.as_record(), separators=(",", ":"), sort_keys=True),
    )


class BaseSplice:
    """The connection lifecycle, the counters, and the tape discipline."""

    #: Wire vocabulary. Subclasses set all three.
    venue: str = ""
    record_prefix: str = ""
    frame_stream: str = ""

    #: Whether this venue's book feed carries incremental deltas. A snapshot-only
    #: venue has permanently lower replay fidelity, and saying so here means the
    #: fact travels with the data instead of living in someone's memory.
    delivers_deltas: bool = True

    #: Whether the feed is subscription-driven. A reference feed broadcasts the
    #: whole world to every client and has nothing to select, so waiting for a
    #: targets file would idle a working socket forever.
    #:
    #: The distinction is recorded rather than inferred from an empty target set,
    #: because "subscribed to nothing" and "subscription does not apply" produce
    #: identical silence on the tape and mean opposite things.
    requires_targets: bool = True

    def __init__(
        self,
        spool: Spool,
        targets_path: Path | None,
        *,
        backoff: BackoffPolicy | None = None,
        target_poll_seconds: float = 30.0,
        heartbeat_seconds: float = 10.0,
        loop_wake_seconds: float = 1.0,
        clock: CaptureClock | None = None,
        clock_regression_alert: Callable[[ClockRegressionAlert], None] | None = None,
    ) -> None:
        from splices.common.spool import resume_state

        self.spool = spool
        # None only for a feed that selects nothing. A subscription splice given
        # None fails at the first `load_targets` rather than here, which is the
        # right place: the failure names the missing file.
        self.targets_path = Path(targets_path) if targets_path is not None else None
        self.backoff = backoff or BackoffPolicy()
        self.target_poll_seconds = float(target_poll_seconds)
        self.heartbeat_seconds = float(heartbeat_seconds)
        # How long a read blocks before the loop rechecks its own conditions: the
        # heartbeat clock, the targets file, the stop deadline. It bounds how
        # stale a subscription may be, so it has to be smaller than the poll
        # interval it serves — a quiet socket would otherwise pin the loop for a
        # full second regardless of what the targeter asked for.
        self.loop_wake_seconds = float(loop_wake_seconds)
        self.clock = clock or CaptureClock(spool.lane)
        self._clock_regression_alert = clock_regression_alert or _log_clock_regression

        resumed = resume_state(spool.root, spool.lane)
        self._delivery_index = resumed.next_delivery_index
        self._repaired_bytes = resumed.repaired_bytes
        #: The last receive time the previous run wrote, for §2's restart
        #: boundary check. A durable filesystem does not make time walk
        #: backwards; if the host clock did, this is where it becomes explicit.
        self._resumed_visible_ns = resumed.last_visible_ns
        self._last_visible_ns = resumed.last_visible_ns
        self._last_scope_id: str | None = None
        self._last_epoch: str | None = None
        self._resume_source = resumed.source
        self._local_counter = 0
        self._epoch = ""
        self.connections = 0
        self.frames = 0

    # -- venue hooks -------------------------------------------------------

    def open_connection(self) -> Any:
        """Returns an async context manager yielding a `Transport`."""
        raise NotImplementedError

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        raise NotImplementedError

    async def send_heartbeat(self, transport: Transport) -> None:
        """Default: nothing. Override where the venue closes an idle socket."""
        return None

    async def after_frame(self, transport: Transport, message: str) -> None:
        """Reply to a frame that demands one — a server-driven ping, typically.

        Deliberately called *after* the frame has already been written, so the
        tape shows what arrived even when answering it fails. A hook that ran
        first could drop the record on the way to the reply, which is the one
        outcome this system is built to prevent.

        It is not a filter and cannot become one: `_emit_frame` has already run
        and its return value is ignored here.
        """
        return None

    def stream_for(self, message: str) -> str:
        """Which stream this frame belongs on. Defaults to the venue's single one.

        Overridden where one socket multiplexes channels that number themselves
        independently. The ingester keys continuity on `(venue, stream, epoch)`,
        so two counters sharing a stream are compared against each other and
        every interleave reads as the cursor going backwards.
        """
        return self.frame_stream

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any] | None:
        """What the venue said about its own continuity, or None if it said nothing.

        Never an ordering. Replay walks our `delivery_index`; this is evidence
        about the venue, interpreted later in the analysis layer.
        """
        raise NotImplementedError

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        """Extra fields for the `connection_opened` record."""
        return {}

    # -- record emission ---------------------------------------------------

    async def _emit(self, *, stream: str, kind: str, payload: str, cursor: dict[str, Any] | None) -> None:
        self._local_counter += 1
        clock = self.clock.sample()
        if self._last_visible_ns is not None and clock.visible_ns < self._last_visible_ns:
            alert = ClockRegressionAlert(
                lane_id=self.spool.lane,
                previous_visible_ns=self._last_visible_ns,
                current_visible_ns=clock.visible_ns,
                previous_delivery_index=(
                    self._delivery_index - 1 if self._delivery_index > 1 else None
                ),
                current_delivery_index=self._delivery_index,
                previous_epoch=self._last_epoch,
                current_epoch=self._epoch,
                previous_scope_id=self._last_scope_id,
                current_scope_id=self.clock.scope.scope_id,
            )
            try:
                self._clock_regression_alert(alert)
            except Exception:  # noqa: BLE001 - alert transport must never drop evidence
                LOGGER.exception(
                    "clock regression alert transport failed: %s",
                    json.dumps(alert.as_record(), separators=(",", ":"), sort_keys=True),
                )
        record = build_envelope(
            delivery_index=self._delivery_index,
            record_id=f"{self.record_prefix}-{self._epoch}-{self._local_counter}",
            visible_ns=clock.visible_ns,
            monotonic_ns=clock.monotonic_ns,
            venue=self.venue,
            stream=stream,
            connection_epoch=self._epoch,
            local_counter=self._local_counter,
            source_cursor=cursor,
            kind=kind,
            raw_payload=payload,
        )
        await self.spool.append(record)
        self._last_visible_ns = clock.visible_ns
        self._last_scope_id = self.clock.scope.scope_id
        self._last_epoch = self._epoch
        self._delivery_index += 1

    async def _emit_frame(self, message: str | bytes) -> str:
        """One socket message, one record, bytes unaltered. Takes no predicate.

        A binary frame is decoded permissively rather than dropped: an undecodable
        byte is still evidence, and `errors="replace"` keeps the record readable
        while the fault record says the substitution happened.

        Returns the recorded text purely so a caller can answer it. There is no
        path by which the return value can suppress the write above it.
        """
        if isinstance(message, bytes):
            try:
                text = message.decode("utf-8")
            except UnicodeDecodeError:
                await self._emit_control("frame_not_utf8", {"bytes": len(message)}, kind=KIND_FAULT)
                text = message.decode("utf-8", errors="replace")
        else:
            text = message
        await self._emit(
            stream=self.stream_for(text),
            kind=KIND_VENUE_FRAME,
            payload=text,
            cursor=self.frame_cursor(self._local_counter + 1, text),
        )
        self.frames += 1
        return text

    async def _emit_control(
        self, event: str, detail: dict[str, Any], *, kind: str = KIND_CONTROL
    ) -> None:
        """Lifecycle and faults share the tape with the frames they explain.

        Putting them in a separate log would mean the gap and the reason for the
        gap have to be rejoined by wall-clock time later — and the reason is
        exactly the part you need when deciding whether to trust the window
        around it.
        """
        payload = json.dumps(
            {"event": event, **detail}, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )
        await self._emit(stream=STREAM_PROCESS, kind=kind, payload=payload, cursor=None)

    # -- the loop ----------------------------------------------------------

    async def run(
        self,
        *,
        stop_after_seconds: float | None = None,
        max_connections: int | None = None,
    ) -> dict[str, Any]:
        """Connects, records, reconnects, until a stop condition is reached.

        The bounds exist so a short probe and the production run take the same
        path — a smoke test that exercised different code would prove nothing
        about the thing actually left running.
        """
        started = time.monotonic()
        attempt = 0
        # The writer outlives every connection, so it starts here rather than per
        # epoch. It has to keep rotating while the lane is disconnected, backing
        # off, or waiting on targets — that idle case is exactly the quiet-lane
        # empty segment that lets a reader tell "nothing happened" from "not
        # finished yet".
        self.spool.start()

        try:
            return await self._run_until_stopped(
                started=started,
                attempt=attempt,
                stop_after_seconds=stop_after_seconds,
                max_connections=max_connections,
            )
        finally:
            # Seals whatever segment is open, whether the loop ended cleanly,
            # raised, or was cancelled. Synchronous on purpose: during
            # cancellation there is no reliable way to await anything.
            self.spool.close()

    async def _run_until_stopped(
        self,
        *,
        started: float,
        attempt: int,
        stop_after_seconds: float | None,
        max_connections: int | None,
    ) -> dict[str, Any]:
        while True:
            if max_connections is not None and self.connections >= max_connections:
                break
            if stop_after_seconds is not None and time.monotonic() - started >= stop_after_seconds:
                break

            if self.requires_targets:
                try:
                    targets = load_targets(self.targets_path, venue=self.venue)
                except TargetsError:
                    # Before the first connection there is no epoch and so nowhere
                    # to record this; the caller surfaces it. Once running, a bad
                    # targets file must never take a live connection down with it.
                    if not self._epoch:
                        raise
                    await asyncio.sleep(self.target_poll_seconds)
                    continue

                if not targets.asset_ids():
                    await asyncio.sleep(self.target_poll_seconds)
                    continue
            else:
                targets = self.broadcast_target_set()

            attempt += 1
            outcome = await self._run_one_connection(
                targets, stop_after_seconds=stop_after_seconds, started=started
            )
            if outcome.clean:
                attempt = 0
            if outcome.stop:
                break
            await asyncio.sleep(self.backoff.delay(attempt))

        return self.summary()

    async def _run_one_connection(
        self, targets: TargetSet, *, stop_after_seconds: float | None, started: float
    ) -> ConnectionOutcome:
        """One epoch: connect, subscribe, drain until something ends it.

        A fresh epoch identifier per connection is what lets the ingester treat
        the book as unproven again. Carrying one across a reconnect would let a
        delta arriving on the new socket fold onto a book assembled from the old
        one, which yields a corrupt book rather than an error.
        """
        self._epoch = uuid.uuid4().hex
        self._local_counter = 0
        self.connections += 1
        self.spool.begin_epoch(self._epoch)

        opened_at = time.monotonic()
        clean = False
        stop = False
        try:
            async with self.open_connection() as transport:
                await self._emit_control(
                    "connection_opened",
                    {
                        "target_digest": targets.digest,
                        "target_count": len(targets),
                        "asset_ids": list(targets.asset_ids()),
                        "targets_path": targets.source_path,
                        "target_metadata_digest": targets.metadata_digest,
                        "target_metadata_path": targets.metadata_path,
                        "delivers_deltas": self.delivers_deltas,
                        "fsync_interval_seconds": self.spool.fsync_interval_seconds,
                        "repaired_bytes_on_start": self._repaired_bytes,
                        "clock_scope": self.clock.scope.as_record(),
                        **self.connection_detail(targets),
                    },
                )
                await self.send_subscription(transport, targets)
                await self._emit_control(
                    "subscription_sent",
                    {"target_digest": targets.digest, "target_count": len(targets)},
                )

                last_heartbeat = time.monotonic()
                last_target_check = time.monotonic()
                while True:
                    now = time.monotonic()
                    if stop_after_seconds is not None and now - started >= stop_after_seconds:
                        stop = clean = True
                        await self._emit_control("connection_closing", {"reason": "time_limit"})
                        break
                    if self.requires_targets and now - last_target_check >= self.target_poll_seconds:
                        last_target_check = now
                        latest = await self._reload_targets(targets)
                        if latest is not None:
                            if latest.digest != targets.digest:
                                clean = True
                                break
                            # Raw catalogue/rules evidence changed but the socket
                            # subscription did not. Advance the observed metadata
                            # version so the next poll does not repeat the event.
                            targets = latest
                    if self.heartbeat_seconds and now - last_heartbeat >= self.heartbeat_seconds:
                        last_heartbeat = now
                        await self.send_heartbeat(transport)

                    try:
                        message = await asyncio.wait_for(
                            transport.recv(), timeout=self.loop_wake_seconds
                        )
                    except asyncio.TimeoutError:
                        continue
                    await self.after_frame(transport, await self._emit_frame(message))
        except asyncio.CancelledError:
            await self._emit_control("connection_closing", {"reason": "cancelled"})
            self.spool.sync()
            raise
        except Exception as error:  # noqa: BLE001 - every failure is a recordable event
            await self._emit_control(
                "connection_failed",
                {
                    "error_type": type(error).__name__,
                    "error": str(error)[:500],
                    "seconds_open": round(time.monotonic() - opened_at, 3),
                    "frames_this_epoch": self._local_counter,
                },
                kind=KIND_FAULT,
            )
        finally:
            if self._epoch:
                await self._emit_control(
                    "connection_closed",
                    {
                        "seconds_open": round(time.monotonic() - opened_at, 3),
                        "records_this_epoch": self._local_counter,
                    },
                )
            # Deliberately no `spool.close()` here. A segment spans connections;
            # sealing on reconnect would roll a file every time a socket blipped
            # and defeat the whole point of a wall-clock window.
        return ConnectionOutcome(clean=clean, stop=stop)

    def broadcast_target_set(self) -> TargetSet:
        """The stand-in subscription for a feed that selects nothing.

        A literal digest rather than `target_digest(venue, ())`: the real function
        would hash an empty asset list and return a plausible-looking hex string,
        making a broadcast feed indistinguishable on the tape from a subscription
        feed that was handed an empty file. Those are opposite conditions — one is
        working, the other is blind — so they get visibly different spellings.
        """
        return TargetSet(
            venue=self.venue,
            targets=(),
            digest="broadcast",
            source_path="",
            metadata_digest="broadcast",
            metadata_path=None,
        )

    async def _reload_targets(self, active: TargetSet) -> TargetSet | None:
        """Returns the new set when the subscription must change, else None."""
        try:
            latest = load_targets(self.targets_path, venue=self.venue)
        except TargetsError as error:
            await self._emit_control("targets_unreadable", {"error": str(error)}, kind=KIND_FAULT)
            return None
        if latest.digest == active.digest:
            if latest.metadata_digest == active.metadata_digest:
                return None
            await self._emit_control(
                "target_metadata_changed",
                {
                    "target_digest": latest.digest,
                    "from_metadata_digest": active.metadata_digest,
                    "to_metadata_digest": latest.metadata_digest,
                    "metadata_path": latest.metadata_path,
                },
            )
            return latest
        await self._emit_control(
            "subscription_changed",
            {
                "from_digest": active.digest,
                "to_digest": latest.digest,
                "added": sorted(set(latest.asset_ids()) - set(active.asset_ids())),
                "removed": sorted(set(active.asset_ids()) - set(latest.asset_ids())),
            },
        )
        return latest

    def summary(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "lane": self.spool.lane,
            "stream": self.frame_stream,
            "connections": self.connections,
            "frames": self.frames,
            "records_written": self.spool.records_written,
            "next_delivery_index": self._delivery_index,
            "repaired_bytes_on_start": self._repaired_bytes,
            "resumed_from": self._resume_source,
            "delivers_deltas": self.delivers_deltas,
            **self.spool.metrics(),
        }
