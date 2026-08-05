"""The Limitless splice — third venue, and the lowest replay fidelity of the three.

Limitless answers what the capture spec listed as its first open question, and the
live socket answers it differently from the documentation — the third time in this
project that the wire and the reference have disagreed.

Measured over 35 seconds against 15 live markets, 491 updates:

* **`orderbookUpdate` carries the full book every time.** No incremental delta
  stream, as documented. Reconstruction is not possible and not needed.
* **A `version` field is present on 100% of messages**, which the documentation
  does not mention at all. It is strictly monotonic per market.
* **It is not dense.** Consecutive updates for one market jump by thousands, and
  the ranges overlap across markets, so it behaves like a server-wide counter
  sampled per book rather than a per-market sequence.

That combination decides how it may be used: `version` orders two books and dates
one, so it detects a *stale* book — but a missing update leaves no hole, so it
cannot detect a *dropped* one. It is recorded as `SnapshotId` because that is what
it is, with the caveat that the ingester's continuity must treat this venue's ids
as monotonic-but-sparse; a classifier expecting Binance-style density here would
report a gap on almost every message.

So Limitless remains the lowest-fidelity venue of the three, and
`delivers_deltas = False` travels with every connection record, so an analysis
pooling it with Polymarket has to do so knowingly. It is still worth capturing: it
prices the same Ω with independent flow, and its hourly and 5-minute crypto
ladders are precisely the P0 instrument class.

Two protocol differences from Polymarket shape the code:

*It is Socket.IO, not raw WebSocket.* Messages arrive through named event
handlers rather than a read call, so the transport below adapts them onto a queue
and the shared capture loop is unchanged. Unifying here rather than growing a
second callback-shaped loop in the base class is what keeps the ordering and
counting rules identical across venues.

*The event name is part of the delivery.* A Socket.IO frame is a name plus a
payload, and the name is not recoverable from the payload — `marketResolved` and
`orderbookUpdate` are different facts. So the recorded `raw_payload` is
`{"event": <name>, "data": <payload verbatim>}`. That wrapper is the single piece
of framing any splice performs, it is lossless, and it is documented here because
it is the one place a reader could otherwise mistake our structure for the
venue's.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from splices.common.base import BaseSplice, Transport
from splices.common.envelope import (
    STREAM_PUBLIC_BOOK,
    VENUE_LIMITLESS,
    snapshot_id_cursor,
    snapshot_time_cursor,
    unsequenced_cursor,
)
from targeter.targets import TargetSet

__all__ = ["LimitlessSplice", "SOCKET_URL", "MARKETS_NAMESPACE"]

SOCKET_URL = "wss://ws.limitless.exchange"
MARKETS_NAMESPACE = "/markets"

#: Server-to-client events on the public namespace. Subscribed wholesale rather
#: than selectively: a splice does not choose which of a venue's public facts are
#: worth keeping, and `marketCreated` in particular is the push feed the capture
#: spec wanted for discovery — a market we learn about from a poll has already
#: lost its opening price-discovery window.
PUBLIC_EVENTS = (
    "newPriceData",
    "orderbookUpdate",
    "marketCreated",
    "marketResolved",
    "system",
    "exception",
)


class _SocketIoTransport:
    """Adapts Socket.IO's callback delivery onto the pull interface the loop uses.

    The queue is unbounded on purpose. A bound would mean dropping frames under
    load, and dropping is the one thing a splice may never do — memory pressure is
    a problem we can see and fix, whereas a silently discarded book update is a
    hole nobody finds.
    """

    def __init__(self, url: str, namespace: str, events: tuple[str, ...]) -> None:
        self._url = url
        self._namespace = namespace
        self._events = events
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._client: Any = None

    async def __aenter__(self) -> _SocketIoTransport:
        import socketio

        self._client = socketio.AsyncClient(reconnection=False, logger=False, engineio_logger=False)
        for name in self._events:
            self._client.on(name, handler=self._make_handler(name), namespace=self._namespace)
        await self._client.connect(
            self._url, namespaces=[self._namespace], transports=["websocket"], wait_timeout=20
        )
        return self

    async def __aexit__(self, *_: object) -> bool:
        if self._client is not None:
            try:
                await self._client.disconnect()
            finally:
                self._client = None
        return False

    def _make_handler(self, name: str):
        async def handler(data: Any = None) -> None:
            await self._queue.put(
                json.dumps({"event": name, "data": data}, ensure_ascii=False, separators=(",", ":"))
            )

        return handler

    async def send(self, message: str) -> None:
        """Socket.IO sends a named event, so the caller passes a JSON envelope."""
        parsed = json.loads(message)
        await self._client.emit(
            parsed["event"], parsed.get("data"), namespace=self._namespace
        )

    async def recv(self) -> str:
        return await self._queue.get()


class LimitlessSplice(BaseSplice):
    venue = VENUE_LIMITLESS
    record_prefix = "lm"
    frame_stream = STREAM_PUBLIC_BOOK
    #: Full books only. See the module docstring — this is the venue's fidelity
    #: ceiling, not a configuration choice.
    delivers_deltas = False

    def __init__(
        self,
        *args: Any,
        url: str = SOCKET_URL,
        namespace: str = MARKETS_NAMESPACE,
        connect_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        # Socket.IO runs its own engine-level ping, so an application heartbeat
        # would be a third liveness mechanism with nothing to add.
        kwargs.setdefault("heartbeat_seconds", 0.0)
        super().__init__(*args, **kwargs)
        self.url = url
        self.namespace = namespace
        self._connect_factory = connect_factory

    def open_connection(self) -> Any:
        if self._connect_factory is not None:
            return self._connect_factory(self.url)
        return _SocketIoTransport(self.url, self.namespace, PUBLIC_EVENTS)

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        """Limitless subscribes by market slug or address, not by token id.

        `asset_id` in a Limitless targets file therefore holds a slug. The
        distinction matters at the targeter, not here — a splice takes the
        identifiers it is given and does not decide what a market is.
        """
        await transport.send(
            json.dumps(
                {
                    "event": "subscribe_market_prices",
                    "data": {"marketSlugs": list(targets.asset_ids()), "marketAddresses": []},
                }
            )
        )
        await transport.send(json.dumps({"event": "subscribe_market_lifecycle", "data": None}))

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any]:
        """The venue's `version` where it gave one, then its timestamp, then ours.

        Ordered by strength of the claim each makes. `version` is monotonic per
        market, so it orders books and catches a stale one; `timestamp` bounds
        staleness only; our own counter asserts nothing about the venue at all.
        Recording which of the three applied is the point — a cursor that silently
        degraded from one to another would make a lifecycle event look like a book
        update in any later continuity check.
        """
        try:
            payload = json.loads(message)
            data = payload.get("data") or {}
            version = data.get("version")
            if version is not None:
                return snapshot_id_cursor(int(version))
            stamp = data.get("timestamp")
            if stamp is not None:
                return snapshot_time_cursor(int(stamp))
        except (json.JSONDecodeError, AttributeError, TypeError, ValueError):
            pass
        return unsequenced_cursor(counter)

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        return {"url": self.url, "namespace": self.namespace, "events": list(PUBLIC_EVENTS)}
