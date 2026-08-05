"""Polled full books: the recovery points the websocket stops providing.

Polymarket sends a `book` event when you subscribe and then, for most assets,
never again. Measured on 12 liquid assets for 90 seconds: **8 received exactly
one** — the subscribe-time snapshot — while four re-anchored irregularly at
2.3–37.1s, most likely on trades. So a connection held for six hours yields one
anchor and six hours of unverified deltas, on the one venue that publishes no
sequence and where a dropped frame therefore leaves no trace at all.

Note which assets re-anchor: the busy ones. The marginal value of polling is
highest on quiet markets, which is the opposite of where attention naturally goes.

## Why a poll can be positioned exactly, despite taking a second to arrive

The obvious objection is staleness. The round trip is ~955 ms, during which an
active book moves, so comparing "our book at T" against "their book at T" seems
to compare two different instants and prove nothing.

It would, if the comparison were by time. It is not. **Every REST book carries the
same `hash` the streaming `price_change` entries carry** — 24 of 24 polled hashes
were found in the matching asset's websocket hash stream, exactly. So a snapshot
is located in our sequence by hash equality:

    find the delivery whose hash equals this snapshot's hash
    -> that is precisely where this book state sits in our stream

No clock alignment, no timestamp comparison, no assumption about round-trip
duration. The 955 ms stops mattering entirely.

This also means the canonical-serialisation bootstrap is unnecessary. Two checks
fall out, and neither requires us to reproduce Polymarket's hash function:

1. **REST hash against streamed hash** — positions the snapshot. String equality.
2. **Our reconstructed book against the snapshot's levels** — verifies our delta
   application. Compares contents, not hashes.

## What it does and does not recover

It does not recover the missed messages. How many orders were lost, and what they
were, is gone the moment the frame is missed and nothing here brings it back.

What it does is *bracket*. A snapshot matching at delivery N proves the book was
correct through N; the next one failing at M places the loss inside `(N, M]`, and
re-anchors the book so the damage stops there. Contamination is bounded by the
poll interval instead of running to the end of the epoch. For deciding whether a
window is trustworthy — which is the question — that is the whole answer.

## Cost

`POST /books` batches: 100 books in 1.29 s, 10 in 0.58 s, so cost is sub-linear
and dominated by the round trip rather than by the batch. At 788 assets a full
cycle is eight requests and roughly ten seconds, which a 60-second cadence absorbs
comfortably. Kalshi has no batch endpoint at all — its equivalent cycle is 709
seconds serial — which is why this exists for Polymarket alone.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request
from collections import deque
from typing import Any

from splices.common.base import BackoffPolicy, BaseSplice, Transport
from splices.common.envelope import (
    STREAM_PUBLIC_SNAPSHOT,
    VENUE_POLYMARKET,
    snapshot_time_cursor,
    unsequenced_cursor,
)
from targeter.targets import TargetSet

__all__ = ["BackoffPolicy", "PolymarketSnapshotSplice", "BOOKS_URL", "SPOOL_LANE"]

BOOKS_URL = "https://clob.polymarket.com/books"

SPOOL_LANE = "polymarket_snapshots"

#: Verified at 100 in 1.29 s. Larger batches were not tested — the available pool
#: was 200 — so this is the largest size with evidence behind it rather than the
#: largest the endpoint might accept.
DEFAULT_BATCH_SIZE = 100

DEFAULT_POLL_SECONDS = 60.0

#: Generous relative to the ~955 ms median because a slow poll is worth waiting
#: for: the alternative is no recovery point this cycle.
REQUEST_TIMEOUT_SECONDS = 45.0

#: Required, not decoration. urllib's default `Python-urllib/3.13` is refused by
#: the edge with a bare 403 — the first live run produced six reconnects and zero
#: books before this was set. Naming the client also means a venue investigating
#: traffic can identify it rather than seeing anonymous automation.
HEADERS = {
    "User-Agent": "prediction-indexer/1.0 (capture; contact via repository)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


class _BookPollTransport:
    """Adapts a REST poll cadence onto the pull-based `Transport` shape.

    Written as a transport rather than as a separate loop so the poller inherits
    the tape discipline that already exists — delivery indices, epochs, lifecycle
    records, fsync policy — instead of growing a second implementation of rules
    that must not diverge between capture paths.

    `recv` returns one book per call, draining the current cycle before issuing
    the next. One book is one record, which keeps the unit of evidence the same
    here as it is on the socket.
    """

    def __init__(self, url: str, batch_size: int, poll_seconds: float) -> None:
        self.url = url
        self.batch_size = batch_size
        self.poll_seconds = poll_seconds
        self.asset_ids: tuple[str, ...] = ()
        self.cycles = 0
        self._pending: deque[str] = deque()
        self._task: asyncio.Task[None] | None = None

    async def send(self, message: str) -> None:
        """The subscription is the asset list; it starts the polling task."""
        self.asset_ids = tuple(json.loads(message)["asset_ids"])
        if self._task is None:
            self._task = asyncio.create_task(self._run())

    async def recv(self) -> str:
        """Hands back one already-fetched book, never fetching inline.

        The polling runs in its own task for a reason that cost a live run to
        find: the base loop caps every `recv` at `loop_wake_seconds`, so a fetch
        awaited here is cancelled roughly a second in — and one cycle over 120
        assets takes longer than that. The first version polled inline, was
        cancelled mid-flight every single time, and produced zero books while
        looking like a healthy connection.

        Draining a deque rather than awaiting an `asyncio.Queue` keeps that
        cancellation harmless by construction: `popleft` either happens or does
        not, and being cancelled during the sleep below cannot strand an item
        that was handed out but never returned.
        """
        while True:
            if self._task is not None and self._task.done():
                # Re-raises the poll failure so it becomes a `connection_failed`
                # record and a backoff, rather than a silent permanent stall.
                self._task.result()
                raise RuntimeError("snapshot poll task ended unexpectedly")
            if self._pending:
                return self._pending.popleft()
            await asyncio.sleep(0.02)

    async def _run(self) -> None:
        while True:
            started = time.monotonic()
            await self._poll()
            # Measured from cycle start, so a slow cycle does not push the next
            # one later and later until the cadence has silently drifted.
            await asyncio.sleep(max(0.0, self.poll_seconds - (time.monotonic() - started)))

    async def _poll(self) -> None:
        if not self.asset_ids:
            return
        self.cycles += 1
        for start in range(0, len(self.asset_ids), self.batch_size):
            batch = self.asset_ids[start:start + self.batch_size]
            books = await asyncio.to_thread(self._fetch, batch)
            # Each book verbatim, exactly as the venue returned it. The batch is
            # a request-efficiency detail and must not become the unit of
            # evidence — a batch boundary is ours, not the venue's.
            self._pending.extend(json.dumps(book, separators=(",", ":")) for book in books)

    def _fetch(self, batch: tuple[str, ...]) -> list[dict[str, Any]]:
        body = json.dumps([{"token_id": asset} for asset in batch]).encode()
        request = urllib.request.Request(self.url, data=body, method="POST", headers=HEADERS)
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            parsed = json.loads(response.read())
        return parsed if isinstance(parsed, list) else [parsed]

    async def __aenter__(self) -> _BookPollTransport:
        return self

    async def __aexit__(self, *_: object) -> bool:
        if self._task is not None:
            self._task.cancel()
            self._task = None
        return False


class PolymarketSnapshotSplice(BaseSplice):
    venue = VENUE_POLYMARKET
    record_prefix = "pmk"
    frame_stream = STREAM_PUBLIC_SNAPSHOT

    #: Polls exactly what the market splice subscribes to, from the same targets
    #: file. A snapshot for an asset nobody is streaming anchors nothing.
    requires_targets = True

    #: Every record here is a whole book by definition.
    delivers_deltas = False

    def __init__(
        self,
        *args: Any,
        url: str = BOOKS_URL,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        transport_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("heartbeat_seconds", 0.0)
        super().__init__(*args, **kwargs)
        self.url = url
        self.poll_seconds = float(poll_seconds)
        self.batch_size = int(batch_size)
        self._transport_factory = transport_factory

    def open_connection(self) -> Any:
        if self._transport_factory is not None:
            return self._transport_factory(self.url)
        return _BookPollTransport(self.url, self.batch_size, self.poll_seconds)

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        await transport.send(json.dumps({"asset_ids": list(targets.asset_ids())}))

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any]:
        """The book's own `timestamp`, which dates it and nothing more.

        Not a sequence and never an ordering. One cycle covers hundreds of assets,
        so the classifier's lane-wide monotonic view compares unrelated books —
        `snapshot` is the cursor class that says exactly that, and it is why this
        cannot report a false gap the way a dense-cursor reading would.

        The load-bearing evidence is the `hash` inside the payload, which locates
        the snapshot in the delta stream far more precisely than any timestamp.
        It stays in the payload rather than being lifted into the cursor: the
        cursor vocabulary is closed and shared across venues, and the ingester
        deliberately does not interpret book contents.
        """
        try:
            stamped = json.loads(message).get("timestamp")
            if stamped is not None:
                return snapshot_time_cursor(int(stamped))
        except (json.JSONDecodeError, TypeError, ValueError, AttributeError):
            pass
        return unsequenced_cursor(counter)

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        return {
            "url": self.url,
            "feed": "book_snapshots",
            "poll_seconds": self.poll_seconds,
            "batch_size": self.batch_size,
        }
