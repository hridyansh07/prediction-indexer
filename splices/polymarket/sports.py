"""The Polymarket sports splice: game state as an exogenous reference clock.

This feed prices nothing. It exists so that two venues' reaction times become
comparable: a goal is a single instant in the world, and every venue quoting that
match is reacting to the same instant. Our own receive clock already orders every
venue's frames against each other — what it cannot do alone is say which venue
*learned something first*, because a quiet market and a slow market look
identical. A shared external event is what separates them.

Verified against the live socket on 2026-07-29 18:06 UTC: 64 frames in 45 seconds
covering 33 games across 11 leagues (lol, cs2, dota2, val, arg, ucl, atp, wta,
wta challenger, challenger, cricket).

**The published schema is wrong, and this time the machine-readable one is the
wrong one.** `asyncapi-sports.json` declares `slug` the sole *required* field of
every message. It never appeared, in any of 64 frames. What arrives instead is the
shape from the prose guide — `gameId`, `leagueAbbreviation`, `homeTeam`,
`awayTeam`, `status` — plus three things no document mentions:

1. **`eventState`**, an undocumented nested object on 12 of 64 frames, carrying
   `createdAt`/`updatedAt` as RFC-3339 with nanosecond precision. It is the only
   venue-side timestamp anywhere in this feed, which makes it the only thing that
   can measure Polymarket's own delivery lag. A splice written to the published
   schema would have discarded it as an unknown field.
2. **`metadataGameId`** replacing `gameId` on cricket frames — a *different
   identifier field*, so keying on `gameId` alone silently drops a whole sport.
3. **`status` spellings that disagree across leagues**: `InProgress` (soccer),
   `inprogress` (tennis), `running` (esports), `finished`, and the literal string
   `None`. Any normaliser would have had to guess, and would have guessed wrong
   for at least one league.

The fourth documented-versus-actual discrepancy in this project, and the first
where the authoritative spec is further from the wire than the prose.

**Liveness is inverted here**, and deliberately so. Every other splice sends its
own application heartbeat with the library's keepalive disabled, because two
independent keepalives make a silent socket ambiguous. This feed sends us nothing
to answer — both documents promise a server `ping` every 5 seconds and none
arrived in 45 — so the library's keepalive is left *on* and is the only liveness
mechanism, which is the same property by the opposite route. It matters more here
than anywhere else: at 04:00 UTC there may be no live game on earth, so a dead
socket and a quiet night produce byte-identical tapes. Without a keepalive the
feed could be dead for hours and every record would look healthy.
"""

from __future__ import annotations

from typing import Any

from splices.common.base import BackoffPolicy, BaseSplice, Transport
from splices.common.envelope import (
    STREAM_REFERENCE_EVENT,
    VENUE_POLYMARKET,
    unsequenced_cursor,
)
from targeter.targets import TargetSet

__all__ = ["BackoffPolicy", "PolymarketSportsSplice", "SPORTS_CHANNEL_URL", "SPOOL_LANE"]

SPORTS_CHANNEL_URL = "wss://sports-api.polymarket.com/ws"

#: Its own spool directory, separate from the market channel's.
#:
#: `delivery_index` is dense across one splice's lifetime, and two processes
#: appending to one venue directory would each resume from the other's last index
#: and then interleave two counters into one file set — destroying the property
#: the index exists to provide. The envelope still says `polymarket`; only the
#: storage lane differs, so provenance is unchanged.
SPOOL_LANE = "polymarket_sports"

#: Documented at 5s from the server, observed never. Answered reactively if it
#: ever shows up, and never depended on.
SERVER_PING_TEXT = "ping"
CLIENT_PONG_TEXT = "pong"

#: The library's own keepalive, this feed's only liveness mechanism. 20s is well
#: inside the 45s of silence that would otherwise be indistinguishable from a
#: night with no fixtures anywhere.
KEEPALIVE_SECONDS = 20.0


class PolymarketSportsSplice(BaseSplice):
    venue = VENUE_POLYMARKET
    record_prefix = "pms"
    frame_stream = STREAM_REFERENCE_EVENT

    #: The socket broadcasts every active game to every client. There is nothing
    #: to select and so nothing for the targeter to say.
    requires_targets = False

    #: Each frame is a whole game state, not a change to one. The transition — the
    #: goal — is derived by differencing consecutive frames in the analysis layer,
    #: which is why every frame must be kept even when it repeats its predecessor.
    delivers_deltas = False

    def __init__(
        self,
        *args: Any,
        url: str = SPORTS_CHANNEL_URL,
        connect_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        # Nothing to send on a timer; the server drives liveness.
        kwargs.setdefault("heartbeat_seconds", 0.0)
        super().__init__(*args, **kwargs)
        self.url = url
        self._connect_factory = connect_factory
        self.pongs_sent = 0

    def open_connection(self) -> Any:
        if self._connect_factory is not None:
            return self._connect_factory(self.url)
        from websockets.asyncio.client import connect

        return connect(
            self.url,
            ping_interval=KEEPALIVE_SECONDS,
            ping_timeout=KEEPALIVE_SECONDS,
            max_size=None,
            open_timeout=20,
        )

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        """Nothing to send. The server streams from the moment the socket opens.

        The `subscription_sent` record is still written by the base loop, and that
        is correct: it marks the instant after which frames were expected, which
        is what a coverage claim needs whether or not bytes went out.
        """
        return None

    async def after_frame(self, transport: Transport, message: str) -> None:
        """Answer a server ping — after the ping itself is already on the tape.

        Compared on the stripped text rather than parsed, because this frame is
        not JSON. A game update never equals `ping`, so no data frame can reach
        this branch.
        """
        if message.strip() == SERVER_PING_TEXT:
            await transport.send(CLIENT_PONG_TEXT)
            self.pongs_sent += 1

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any]:
        """No sequence of any kind, so our own count is the only cursor.

        Worth stating plainly because it is tempting to reach for
        `eventState.updatedAt` as a `snapshot_time` cursor: it is a timestamp, it
        is monotonic-looking, and it is right there. It would be wrong. That field
        is absent from 52 of 64 frames, and it belongs to a *game*, not to the
        connection — consecutive frames describe different matches, so ordering
        one against the next compares two unrelated clocks. It is measured in the
        analysis layer, where the game is known.
        """
        return unsequenced_cursor(counter)

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        return {
            "url": self.url,
            "feed": "sports",
            "reference_feed": True,
            "keepalive_seconds": KEEPALIVE_SECONDS,
        }

    def summary(self) -> dict[str, Any]:
        return {**super().summary(), "pongs_sent": self.pongs_sent}
