"""The Polymarket RTDS splice: the underlying spot price as a reference clock.

The sports feed answers "who reacted first to a goal". This one answers the same
question for the instrument class this project actually pivoted to. A short-dated
BTC ladder is a function of one number, and every venue quoting a strike is
tracking that number continuously — so the spot tick is a reference event that
arrives several times a second, on every venue, all night, rather than a handful
of times per match. For measuring cross-venue skew on crypto ladders it is the
denser and better instrument by a wide margin.

Verified against the live socket on 2026-07-29 18:09 UTC: 236 frames in 40
seconds across every symbol the feed carries.

**The documented `filters` parameter silently returns nothing.** The reference
gives `{"topic":"crypto_prices","type":"update","filters":"btcusdt,ethusdt"}`.
Subscribing that way produced *zero* data frames in 40 seconds — not an error, not
a rejection, just silence. So did `type: "*"` with the same filter. Dropping
`filters` entirely produced 236 frames immediately. Both filtered variants were
tested twice, against a feed proven live in the same 40-second window by the
unfiltered one running beside them.

That failure mode is the dangerous kind: a filtered subscription looks exactly
like a market with nothing happening. Following the documentation here would have
produced a capture that ran for weeks, wrote clean healthy tapes, reconnected
properly, and contained no data at all.

So the subscription is unfiltered, and every symbol is recorded. Selecting down to
the three that matter is analysis, and analysis is the reversible side.

Two further wire facts, neither documented:

* **The first frame on every connection is an empty text frame.** It is recorded
  like anything else rather than skipped — it is the cheapest possible proof the
  socket reached application level, and a splice that dropped it would leave the
  connection's first evidence to a control record written before the handshake
  finished.
* **Two timestamps that mean different things.** The outer `timestamp` is when
  Polymarket emitted the frame; `payload.timestamp` is the source exchange's own,
  aligned to a whole second. Their difference is Polymarket's ingestion lag, and
  `visible_ns` minus the outer one is ours. Measured separately in the analysis
  layer, because only the pair distinguishes a slow venue from a slow us.
"""

from __future__ import annotations

import json
from typing import Any

from splices.common.base import BackoffPolicy, BaseSplice, Transport
from splices.common.envelope import (
    STREAM_REFERENCE_EVENT,
    VENUE_POLYMARKET,
    unsequenced_cursor,
)
from targeter.targets import TargetSet

__all__ = ["BackoffPolicy", "PolymarketRtdsSplice", "RTDS_URL", "SPOOL_LANE"]

RTDS_URL = "wss://ws-live-data.polymarket.com"

#: Separate from both the market channel and the sports feed, for the reason given
#: in `sports.py`: `delivery_index` is dense per splice lifetime and two processes
#: cannot share one lane without corrupting it.
SPOOL_LANE = "polymarket_rtds"

#: Every price topic this feed carries, subscribed without `filters` because the
#: documented filter form returns silence. `type: "*"` takes the snapshot as well
#: as the updates — the snapshot is a second-by-second backfill that dates the
#: connection against the source exchange's clock before the first tick arrives.
DEFAULT_TOPICS = ("crypto_prices", "crypto_prices_chainlink")

APPLICATION_PING_TEXT = "PING"
APPLICATION_PING_SECONDS = 5.0


class PolymarketRtdsSplice(BaseSplice):
    venue = VENUE_POLYMARKET
    record_prefix = "pmr"
    frame_stream = STREAM_REFERENCE_EVENT

    #: One socket carries every symbol; there is nothing per-market to select.
    requires_targets = False

    #: Each tick is a full price, not a change to one.
    delivers_deltas = False

    def __init__(
        self,
        *args: Any,
        url: str = RTDS_URL,
        topics: tuple[str, ...] = DEFAULT_TOPICS,
        connect_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("heartbeat_seconds", APPLICATION_PING_SECONDS)
        super().__init__(*args, **kwargs)
        self.url = url
        self.topics = tuple(topics)
        self._connect_factory = connect_factory

    def open_connection(self) -> Any:
        if self._connect_factory is not None:
            return self._connect_factory(self.url)
        from websockets.asyncio.client import connect

        # Same reasoning as the market channel: our application PING is the single
        # liveness mechanism, so the library's protocol ping is disabled rather
        # than left to keep a feed-silent socket nominally healthy.
        return connect(self.url, ping_interval=None, max_size=None, open_timeout=20)

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        await transport.send(
            json.dumps(
                {
                    "action": "subscribe",
                    # No `filters` key. See the module docstring: including one
                    # produces a subscription that looks healthy and delivers
                    # nothing.
                    "subscriptions": [{"topic": topic, "type": "*"} for topic in self.topics],
                }
            )
        )

    async def send_heartbeat(self, transport: Transport) -> None:
        await transport.send(APPLICATION_PING_TEXT)

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any]:
        """Our own count, despite two timestamps being available on most frames.

        The outer `timestamp` is a genuine per-frame venue clock and would make a
        defensible `snapshot_time` cursor. It is refused anyway, because one
        socket multiplexes every symbol: consecutive frames describe different
        instruments, so a lane-wide monotonic check compares unrelated series and
        reports a fault whenever two symbols tick out of order. That exact mistake
        produced 7 phantom faults on Limitless before it was found there.
        """
        return unsequenced_cursor(counter)

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        return {
            "url": self.url,
            "feed": "rtds",
            "reference_feed": True,
            "topics": list(self.topics),
            "filtered": False,
        }
