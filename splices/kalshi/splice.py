"""The Kalshi splice.

Complete and plug-and-play: it runs the moment `KALSHI_API_KEY_ID` and a private
key are configured. See `splices/kalshi/auth.py` for exactly what to set.

**Written from the published specification and never exercised against Kalshi's
servers**, because the credential needed to do that does not exist yet. That is
worth stating plainly, since both other venues in this project contradicted their
own documentation — Polymarket's live wire is flat snake_case where the docs
describe wrapped camelCase, and Limitless carries a `version` field its reference
says does not exist.

The architecture makes most of that risk cheap. A splice records verbatim and
normalises nothing, so if the message *shapes* differ from the spec the frames
still land on the tape correctly and only the analysis layer needs updating. Only
three things here can actually fail on contact:

1. the signature construction (`auth.py`),
2. the subscribe command shape (`send_subscription`),
3. cursor extraction (`frame_cursor`).

All three are isolated, and the third degrades to `unsequenced` rather than
raising, so a schema surprise costs continuity metadata and never a frame.

**Why Kalshi is the highest-fidelity venue.** It is the only one publishing a real
snapshot-then-delta feed, and its `seq` is per *subscription* rather than per
market. One subscription covering an entire strike ladder therefore yields a
single dense sequence, which is the only arrangement in which the ingester can
*prove* a dropped message rather than merely suspect one. That is why the cursor
below is `update_range` and why this splice subscribes every target in one call
rather than one connection per market.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from splices.common.base import BaseSplice, Transport
from splices.common.envelope import (
    STREAM_PUBLIC_BOOK,
    STREAM_PUBLIC_QUOTE,
    STREAM_PUBLIC_TRADE,
    VENUE_KALSHI,
    unsequenced_cursor,
    update_range_cursor,
)
from splices.kalshi.auth import (
    WEBSOCKET_SIGNING_PATH,
    KalshiCredentials,
    KalshiCredentialsError,
    load_credentials,
)
from targeter.targets import TargetSet

__all__ = [
    "KalshiProtocolError",
    "KalshiSplice",
    "WEBSOCKET_URL",
    "DEMO_WEBSOCKET_URL",
]

WEBSOCKET_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
DEMO_WEBSOCKET_URL = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"

#: `orderbook_delta` carries the book. `trade` and `ticker` are public and cost
#: nothing extra on a connection we are already paying to authenticate, and both
#: answer questions the book alone cannot — what actually executed, and at what
#: size.
DEFAULT_CHANNELS = ("orderbook_delta", "trade", "ticker")

#: Kalshi defaults this to false. Without an initial snapshot a delta stream
#: describes mutations to a book we never received, which is unusable — so it is
#: sent explicitly rather than relied on.
SEND_INITIAL_SNAPSHOT = True


class KalshiProtocolError(RuntimeError):
    """A server-declared WebSocket error that requires a fresh connection."""


class KalshiSplice(BaseSplice):
    venue = VENUE_KALSHI
    record_prefix = "kx"
    frame_stream = STREAM_PUBLIC_BOOK
    #: The only venue of the three with a true incremental feed.
    delivers_deltas = True

    def __init__(
        self,
        *args: Any,
        url: str = WEBSOCKET_URL,
        channels: tuple[str, ...] = DEFAULT_CHANNELS,
        credentials: KalshiCredentials | None = None,
        dotenv_path: Path | None = None,
        connect_factory: Any = None,
        **kwargs: Any,
    ) -> None:
        # Kalshi's own protocol ping keeps the socket alive, so an application
        # heartbeat would be a third liveness mechanism with nothing to add.
        kwargs.setdefault("heartbeat_seconds", 0.0)
        super().__init__(*args, **kwargs)
        self.url = url
        self.channels = tuple(channels)
        self.dotenv_path = dotenv_path
        self._credentials = credentials
        self._connect_factory = connect_factory
        self._command_id = 0

    # -- credentials -------------------------------------------------------

    def credentials(self) -> KalshiCredentials:
        """Loaded once, lazily, so constructing the splice never needs a key.

        `splices/run.py` builds every venue's splice by name, and a constructor
        that demanded credentials would make an unconfigured Kalshi break the
        `--help` output for the venues that are configured.
        """
        if self._credentials is None:
            self._credentials = load_credentials(dotenv_path=self.dotenv_path)
        return self._credentials

    def open_connection(self) -> Any:
        if self._connect_factory is not None:
            return self._connect_factory(self.url)
        from websockets.asyncio.client import connect

        headers = self.credentials().signature_headers("GET", WEBSOCKET_SIGNING_PATH)
        # The handshake itself is signed, and the signature covers a timestamp, so
        # the headers are built per connection attempt rather than once. A
        # long-backed-off reconnect reusing a stale timestamp is exactly the case
        # that would fail intermittently and look like a network problem.
        return connect(
            self.url,
            additional_headers=headers,
            ping_interval=10,
            ping_timeout=20,
            max_size=None,
            open_timeout=20,
        )

    # -- protocol ----------------------------------------------------------

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        """One subscription covering every target.

        Deliberately not one per market. `seq` is per subscription, so a single
        subscription across the whole ladder produces one dense sequence the
        ingester can verify; splitting it would give N independent sequences and
        lose the property that makes this venue worth the auth.
        """
        self._command_id += 1
        await transport.send(
            json.dumps(
                {
                    "id": self._command_id,
                    "cmd": "subscribe",
                    "params": {
                        "channels": list(self.channels),
                        "market_tickers": list(targets.asset_ids()),
                        "send_initial_snapshot": SEND_INITIAL_SNAPSHOT,
                    },
                }
            )
        )

    #: Message type to stream. Verified against the live socket on 2026-07-30:
    #: one subscribe command produced three subscription ids, and **`seq` is dense
    #: per `sid`, not per connection** —
    #:
    #:     sid=1  orderbook_snapshot + orderbook_delta   seq 1..19437  dense
    #:     sid=2  trade                                  seq 1..431    dense
    #:     sid=3  ticker                                 no seq at all
    #:
    #: The two counters are independent and collide: sid 1 and sid 2 shared 431
    #: values in a single minute. Left on one stream the ingester compares them
    #: against each other, and the first authenticated run scored **431
    #: `cursor_went_backwards` — exactly the trade count, none of them real** —
    #: and marked the epoch `Stale`.
    #:
    #: Splitting by stream is the fix because the ingester already keys continuity
    #: on `(venue, stream, epoch)`. It needs no new concept, and it keeps the one
    #: venue that can *prove* a loss from drowning that signal in phantom faults.
    STREAM_BY_TYPE = {
        "orderbook_snapshot": STREAM_PUBLIC_BOOK,
        "orderbook_delta": STREAM_PUBLIC_BOOK,
        "trade": STREAM_PUBLIC_TRADE,
        "ticker": STREAM_PUBLIC_QUOTE,
    }

    def stream_for(self, message: str) -> str:
        """Routes by the venue's own message type, defaulting to the book lane.

        An unrecognised type lands on `public_book` with no cursor, so a shape the
        spec did not describe is still recorded and still cannot disturb the
        sequence it does not participate in.
        """
        try:
            payload = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return self.frame_stream
        if not isinstance(payload, dict):
            return self.frame_stream
        return self.STREAM_BY_TYPE.get(str(payload.get("type")), self.frame_stream)

    async def after_frame(self, transport: Transport, message: str) -> None:
        """Fail a server-declared protocol error after preserving it on the tape.

        Kalshi can keep the WebSocket open after rejecting a subscription. Merely
        recording that response would leave the process looking connected while
        it receives none of the requested market data. `BaseSplice` invokes this
        hook only after `_emit_frame`, so raising here cannot erase the venue's
        explanation; it closes the failed epoch and lets the ordinary reconnect
        policy retry with a fresh signed handshake.
        """
        try:
            payload = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(payload, dict) or payload.get("type") != "error":
            return

        nested = payload.get("msg")
        if isinstance(nested, dict):
            code = nested.get("code", payload.get("code"))
            reason = nested.get("msg") or nested.get("message")
        else:
            code = payload.get("code")
            reason = nested or payload.get("message")

        detail = "Kalshi WebSocket protocol error"
        if code is not None:
            detail += f" code={code}"
        if reason:
            detail += f": {str(reason)[:300]}"
        raise KalshiProtocolError(detail)

    def frame_cursor(self, counter: int, message: str) -> dict[str, Any]:
        """Kalshi's `seq`, expressed as the one-position range it actually is.

        Each message carries a single `seq` rather than a range, so `first` and
        `last` are that value and `previous_last` is **`seq - 1`** — the venue's
        own claim about what precedes this message, which for a counter Kalshi
        promises is dense is true by definition.

        It is emphatically *not* the last sequence this splice happened to see.
        An earlier version did that, and it silently destroyed the only signal
        this venue offers: setting `previous_last` to the last observed value
        makes every message continuous with its predecessor by construction, so a
        jump from 6 to 9 arrives labelled as continuous and the two lost messages
        become undetectable. An end-to-end run through the real ingester scored a
        deliberate 7–8 hole as `continuous: 6, proven_gaps: 0`.

        The general rule that mistake violates: a cursor records what the *venue*
        asserted, never what the splice inferred. Splice bookkeeping dressed up as
        venue evidence is worse than no evidence, because it looks trustworthy.

        Anything without a usable `seq` falls back to `unsequenced` here. A schema
        surprise should cost continuity metadata, never a frame. A declared
        protocol error also receives that cursor and is written normally; the
        post-record `after_frame` hook then ends the rejected connection.
        """
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return unsequenced_cursor(counter)
        if not isinstance(payload, dict):
            return unsequenced_cursor(counter)

        sequence = payload.get("seq")
        if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
            return unsequenced_cursor(counter)

        return update_range_cursor(sequence, sequence, max(sequence - 1, 0))

    def connection_detail(self, targets: TargetSet) -> dict[str, Any]:
        detail: dict[str, Any] = {
            "url": self.url,
            "channels": list(self.channels),
            "send_initial_snapshot": SEND_INITIAL_SNAPSHOT,
            "verified_against_live_socket": False,
        }
        try:
            detail["key_id"] = self.credentials().key_id
        except KalshiCredentialsError:
            detail["key_id"] = None
        return detail
