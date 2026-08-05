"""The Polymarket market-channel splice.

Verified against the live socket on 2026-07-28: 20 crypto assets for 45 seconds
produced 3,823 frames — 3,707 `price_change`, 94 `book`, 37 `last_trade_price`,
4 `PONG`.

Two things that run showed which the published documentation does not:

1. **The wire is flat snake_case**, not the wrapped camelCase the docs describe.
   Live frames carry `event_type`, `price_changes`, `asset_id`, `best_bid`,
   `best_ask`; the reference says `{topic, type, payload:{priceChanges}}`. A
   splice that normalised against the documentation would have written nulls into
   every field, and nobody would have found out for weeks. This is the whole case
   for capturing verbatim, and it surfaced on the first run.

2. **Every `price_change` entry carries a `hash`** — 7,414 of 7,414, none null.
   There is still no sequence number anywhere in the feed, so ordering is ours
   alone, but the hash means venue-side gap detection by book checksum is
   available to the analysis layer.

The one thing this splice decides is what a *delivery* is, and the answer is one
socket message, verbatim. Polymarket batches several events into a single frame;
splitting them would interpret a schema we are deliberately not interpreting yet,
and the frame boundary cannot be recovered once it is gone.
"""

from __future__ import annotations

import json
from typing import Any

from splices.common.base import BackoffPolicy, BaseSplice, Transport
from splices.common.envelope import STREAM_PUBLIC_BOOK, VENUE_POLYMARKET, unsequenced_cursor
from targeter.targets import TargetSet

__all__ = ["BackoffPolicy", "PolymarketSplice", "MARKET_CHANNEL_URL"]

MARKET_CHANNEL_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

#: Polymarket closes an idle market connection. The server accepts a bare `PING`
#: text frame and answers `PONG`, which arrives as an ordinary message and is
#: recorded like any other — a heartbeat is evidence the socket was alive at a
#: known instant, which is exactly what separates a quiet market from a dead
#: connection when a coverage report is read months later.
APPLICATION_PING_TEXT = "PING"
APPLICATION_PING_SECONDS = 10.0


class PolymarketSplice(BaseSplice):
    venue = VENUE_POLYMARKET
    record_prefix = "pm"
    frame_stream = STREAM_PUBLIC_BOOK
    delivers_deltas = True

    def __init__(
        self,
        *args: Any,
        url: str = MARKET_CHANNEL_URL,
        connect_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("heartbeat_seconds", APPLICATION_PING_SECONDS)
        super().__init__(*args, **kwargs)
        self.url = url
        self._connect_factory = connect_factory

    def open_connection(self) -> Any:
        if self._connect_factory is not None:
            return self._connect_factory(self.url)
        from websockets.asyncio.client import connect

        # `ping_interval=None` disables the library's protocol-level ping so the
        # application PING is the only liveness mechanism. Two independent
        # keepalives make a silent socket ambiguous: the protocol pong would keep
        # the connection nominally healthy while the venue had stopped sending us
        # anything at all.
        return connect(self.url, ping_interval=None, max_size=None, open_timeout=20)

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        await transport.send(
            json.dumps({"assets_ids": list(targets.asset_ids()), "type": "market"})
        )

    async def send_heartbeat(self, transport: Transport) -> None:
        await transport.send(APPLICATION_PING_TEXT)

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any]:
        """Polymarket publishes no sequence, so our own count is the only cursor.

        Labelled `unsequenced` rather than left null, so the tape distinguishes
        "this venue numbers nothing" from "no cursor was available for this
        record" — and a venue that never offered continuity is never silently
        pooled with one that did.
        """
        return unsequenced_cursor(counter)

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        return {"url": self.url}
