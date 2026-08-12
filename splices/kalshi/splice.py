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
four things here can actually fail on contact:

1. the signature construction (`auth.py`),
2. the subscribe command shape (`send_subscription`),
3. cursor extraction (`frame_cursor`),
4. the snapshot poller's `update_subscription` command
   (`request_stale_snapshots`).

The first three are isolated, and the third degrades to `unsequenced` rather than
raising, so a schema surprise costs continuity metadata and never a frame. The
fourth is the only one that *sends*, so every one of its failure branches stops
sending rather than retrying, and it is off unless configured on: `splices/run.py`
defaults it to zero, which keeps enabling it in capture a recorded decision.

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
import time
from pathlib import Path
from typing import Any, Callable

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
    "get_snapshot_command",
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

#: The channel whose subscription carries the book, and therefore the only `sid`
#: the poller may address. One `subscribe` produces three — book, trade, ticker —
#: and a snapshot request aimed at the wrong one is a well-formed command about
#: the wrong subscription, which the venue would answer.
BOOK_CHANNEL = "orderbook_delta"

#: How often the sweep runs. It bounds the command rate to 2/min no matter how
#: many markets are stale, because `market_tickers` is a list and one sweep is
#: one command.
DEFAULT_SNAPSHOT_SWEEP_SECONDS = 30.0

#: How long a market's book may go unrefreshed before it is asked for again.
#: Measured over three production days, the natural interval between snapshots
#: for one market is p50 10.2 min with a tail to 120 min — so this leaves the
#: common case alone and spends only on the long epochs where a dropped delta
#: would otherwise stay unrepairable for hours.
DEFAULT_SNAPSHOT_MAX_AGE_SECONDS = 600.0

#: How long a requested market is left alone before being asked again. A separate
#: clock from staleness on purpose: resetting the staleness clock on *request*
#: would silence a market permanently the first time a response went missing.
DEFAULT_SNAPSHOT_REQUEST_COOLDOWN_SECONDS = 60.0

#: Ceiling on the rate-limit backoff, chosen below
#: `DEFAULT_SNAPSHOT_MAX_AGE_SECONDS` so that even fully backed off a sweep still
#: comes round more often than the staleness threshold it serves. A sweep slower
#: than the age it is checking is not a poller, it is a rounding error.
MAX_SNAPSHOT_SWEEP_SECONDS = 480.0

#: "the subscription exceeded its command rate limit". The one error reply that
#: means *slow down* rather than *stop*, and the only reason this splice ever
#: changes its own cadence.
RATE_LIMIT_ERROR_CODE = 27


def get_snapshot_command(command_id: int, sid: int, market_tickers: list[str]) -> str:
    """The `update_subscription` that asks for snapshots without changing anything.

    Module level, and public, so the live exit gate in
    `scripts/kalshi_live_get_snapshot_gate.py` sends the exact bytes capture will
    send. A gate that proved a *copy* of this command works would have proved
    nothing about the command that actually reaches production.

    `action: get_snapshot` returns an `orderbook_snapshot` for the named markets
    and does not modify the subscription, so no new `sid` appears and the single
    dense `seq` this venue is worth the auth for stays single and dense.
    """
    return json.dumps(
        {
            "id": command_id,
            "cmd": "update_subscription",
            "params": {
                "sids": [sid],
                "market_tickers": list(market_tickers),
                "action": "get_snapshot",
            },
        }
    )


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
        snapshot_max_age_seconds: float | None = DEFAULT_SNAPSHOT_MAX_AGE_SECONDS,
        snapshot_sweep_seconds: float = DEFAULT_SNAPSHOT_SWEEP_SECONDS,
        snapshot_request_cooldown_seconds: float = DEFAULT_SNAPSHOT_REQUEST_COOLDOWN_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
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
        # Injectable because the poller's entire subject is elapsed time. A test
        # for a ten-minute threshold either moves this clock or waits ten
        # minutes, and the third option — asserting only that the code ran — is
        # how a threshold ships wrong.
        self._monotonic = monotonic

        self.snapshot_max_age_seconds = float(snapshot_max_age_seconds or 0.0)
        self.snapshot_request_cooldown_seconds = float(snapshot_request_cooldown_seconds)
        # 0 or None turns the poller off, and it does so by leaving the base
        # loop's sweep interval at zero, so the loop never calls the hook at all.
        # On the capture path "disabled" has to mean the code does not run, not
        # that it runs and decides to do nothing.
        self._configured_sweep_seconds = (
            float(snapshot_sweep_seconds) if self.snapshot_max_age_seconds > 0 else 0.0
        )
        self.snapshot_sweep_seconds = self._configured_sweep_seconds

        self._book_sid: int | None = None
        self._epoch_started_at = 0.0
        self._last_snapshot_at: dict[str, float] = {}
        self._last_request_at: dict[str, float] = {}
        self._snapshot_command_ids: set[int] = set()
        self._poller_disabled_reason: str | None = None

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

    def next_command_id(self) -> int:
        """The next `id` to put on a command, unique for this splice's lifetime.

        Deliberately not reset per epoch. Ids are how an `error` reply is
        attributed to the command that caused it, and a counter that restarted
        would let a reply arriving across a reconnect be matched to a command
        from the previous connection.
        """
        self._command_id += 1
        return self._command_id

    async def send_subscription(self, transport: Transport, targets: TargetSet) -> None:
        """One subscription covering every target.

        Deliberately not one per market. `seq` is per subscription, so a single
        subscription across the whole ladder produces one dense sequence the
        ingester can verify; splitting it would give N independent sequences and
        lose the property that makes this venue worth the auth.

        Also the poller's epoch boundary, because it is the one point that runs
        exactly once per connection and before any frame arrives.
        """
        self._begin_snapshot_epoch()
        await transport.send(
            json.dumps(
                {
                    "id": self.next_command_id(),
                    "cmd": "subscribe",
                    "params": {
                        "channels": list(self.channels),
                        "market_tickers": list(targets.asset_ids()),
                        "send_initial_snapshot": SEND_INITIAL_SNAPSHOT,
                    },
                }
            )
        )

    # -- snapshot poller ---------------------------------------------------

    def _begin_snapshot_epoch(self) -> None:
        """Discards every poller fact the previous connection established.

        Correct rather than merely tidy. A `sid` belongs to one connection's
        subscription; a new epoch opens with `send_initial_snapshot`, so every
        market's book is re-established at this instant and no staleness carries
        over. A widened sweep interval resets here too, because the command rate
        limit that widened it was the old subscription's.

        Markets never seen here start from `_epoch_started_at`, which is the only
        honest answer available: the splice knows when this connection opened and
        knows no snapshot has arrived since.
        """
        self._book_sid = None
        self._epoch_started_at = self._monotonic()
        self._last_snapshot_at = {}
        self._last_request_at = {}
        self._snapshot_command_ids = set()
        self._poller_disabled_reason = None
        self.snapshot_sweep_seconds = self._configured_sweep_seconds

    async def request_stale_snapshots(self, transport: Transport, targets: TargetSet) -> None:
        """One command for every market whose book has gone quiet.

        The trigger is the *absence of a snapshot*, never the detection of a gap.
        "When did a snapshot last land for market M" is bookkeeping about what
        arrived; "something is missing" is an interpretation of what did not, and
        `frame_cursor` below records what it cost the last time this splice was
        allowed to interpret. Replay decides what a gap means, with the whole tape
        in front of it rather than one connection's worth.

        Every branch that cannot proceed returns without sending. The capture
        path's failure mode is data loss and it is the only irrecoverable one
        here: a poller that never runs costs a wider trust interval, while a
        poller that kills the connection costs tape.
        """
        if self._poller_disabled_reason is not None:
            return
        if self.snapshot_max_age_seconds <= 0:
            return
        if self._book_sid is None:
            # Never guessed, not even when the ladder's `sid` has been 1 on every
            # connection so far. The acknowledgement is the venue stating which
            # subscription is the book's, and absent that statement the only
            # command available is one aimed at a subscription we picked.
            await self._disable_poller(
                "no_subscribed_acknowledgement", {"channel": BOOK_CHANNEL}
            )
            return

        now = self._monotonic()
        stale: list[str] = []
        for ticker in targets.asset_ids():
            last_snapshot = self._last_snapshot_at.get(ticker, self._epoch_started_at)
            if now - last_snapshot < self.snapshot_max_age_seconds:
                continue
            requested = self._last_request_at.get(ticker)
            if (
                requested is not None
                and now - requested < self.snapshot_request_cooldown_seconds
            ):
                continue
            stale.append(ticker)
        if not stale:
            return

        command_id = self.next_command_id()
        # Claimed as ours before it goes out. An error reply is attributed by this
        # id, and a reply that overtook the bookkeeping would be read as a
        # rejected subscription and end an epoch that was capturing fine.
        self._snapshot_command_ids.add(command_id)
        try:
            await transport.send(get_snapshot_command(command_id, self._book_sid, stale))
        except Exception as error:  # noqa: BLE001 - the poller may not end an epoch
            # Recorded and swallowed, which is the entire difference between this
            # failing and the tape failing. A genuinely dead socket says so at the
            # very next `recv` and the ordinary reconnect path runs; anything else
            # is a poller that could not send, which is no reason to drop a
            # connection that is still delivering frames.
            await self._disable_poller(
                "send_failed",
                {
                    "command_id": command_id,
                    "error_type": type(error).__name__,
                    "error": str(error)[:300],
                },
            )
            return

        for ticker in stale:
            self._last_request_at[ticker] = now
        await self._emit_control(
            "orderbook_reconciliation_request",
            {
                "sid": self._book_sid,
                "command_id": command_id,
                "market_tickers": stale,
                "reason": f"snapshot_older_than_{self.snapshot_max_age_seconds:g}s",
            },
        )

    async def _disable_poller(self, reason: str, detail: dict[str, Any]) -> None:
        """Off for this epoch, once, with the reason on the tape beside the frames.

        Per epoch rather than per process because the next connection subscribes
        afresh: whatever the venue objected to may have been about this
        subscription, and a permanent decision taken from one reply would silently
        outlive the condition that caused it.
        """
        if self._poller_disabled_reason is not None:
            return
        self._poller_disabled_reason = reason
        await self._emit_control(
            "orderbook_reconciliation_disabled", {"reason": reason, **detail}
        )

    def _note_subscription(self, payload: dict[str, Any]) -> None:
        """Keeps the book channel's `sid` and ignores the other two."""
        message = payload.get("msg")
        if not isinstance(message, dict) or message.get("channel") != BOOK_CHANNEL:
            return
        sid = message.get("sid")
        if isinstance(sid, int) and not isinstance(sid, bool):
            self._book_sid = sid

    def _note_snapshot(self, payload: dict[str, Any]) -> None:
        """`last_snapshot_at[ticker] = now`, which is the whole state model.

        The ticker is looked for in two places because every venue in this
        project has contradicted its own documentation at least once, and a
        snapshot whose ticker went unread would leave a market being asked for
        while it was answering.
        """
        message = payload.get("msg")
        ticker = message.get("market_ticker") if isinstance(message, dict) else None
        if not isinstance(ticker, str) or not ticker:
            ticker = payload.get("market_ticker")
        if isinstance(ticker, str) and ticker:
            self._last_snapshot_at[ticker] = self._monotonic()

    async def _snapshot_command_rejected(
        self, command_id: int, code: Any, reason: Any
    ) -> None:
        """The venue refused the poller's own command. It must not cost the socket.

        Both branches send less. Code 27 is a statement about cadence rather than
        about validity — one command per sweep against a limit on commands — so
        the sweep widens and the poller keeps working. The interval only ever
        widens within an epoch: recovering by tightening it again would trade a
        recorded slowdown for an oscillation, and the reset that matters happens
        at the next connection anyway.

        Any other code means this splice formed a command the venue would not
        accept, and a poller that cannot be trusted to form a valid command must
        stop forming them.
        """
        # Compared as text because a code arriving as "27" rather than 27 is
        # exactly the class of surprise this venue's siblings have produced, and
        # misreading it would widen nothing while disabling the poller outright.
        if str(code) == str(RATE_LIMIT_ERROR_CODE):
            previous = self.snapshot_sweep_seconds
            # The ceiling can only stop the widening, never reverse it. A sweep
            # already configured slower than the ceiling would otherwise be
            # *tightened* by the venue telling us to slow down.
            self.snapshot_sweep_seconds = max(
                previous, min(previous * 2.0, MAX_SNAPSHOT_SWEEP_SECONDS)
            )
            await self._emit_control(
                "orderbook_reconciliation_backoff",
                {
                    "command_id": command_id,
                    "code": code,
                    "from_sweep_seconds": previous,
                    "to_sweep_seconds": self.snapshot_sweep_seconds,
                    "detail": str(reason)[:300] if reason else None,
                },
            )
            return
        await self._disable_poller(
            "command_rejected",
            {
                "command_id": command_id,
                "code": code,
                "detail": str(reason)[:300] if reason else None,
            },
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
        """Read the venue's own bookkeeping, then fail a declared protocol error.

        Three jobs in ascending order of consequence: note which `sid` the book
        channel was given, note that a market's book was re-established, and end
        the epoch when the venue rejects something.

        Kalshi can keep the WebSocket open after rejecting a subscription. Merely
        recording that response would leave the process looking connected while
        it receives none of the requested market data. `BaseSplice` invokes this
        hook only after `_emit_frame`, so raising here cannot erase the venue's
        explanation; it closes the failed epoch and lets the ordinary reconnect
        policy retry with a fresh signed handshake.

        The two notes are pure bookkeeping about frames that already reached the
        tape. Nothing here filters, rewrites, or delays a record, and the
        poller's view of the world is derived from the same bytes a reader will
        see rather than from a second, private account of them.
        """
        try:
            payload = json.loads(message)
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(payload, dict):
            return

        kind = payload.get("type")
        if kind == "subscribed":
            self._note_subscription(payload)
            return
        if kind == "orderbook_snapshot":
            self._note_snapshot(payload)
            return
        if kind != "error":
            return

        nested = payload.get("msg")
        if isinstance(nested, dict):
            code = nested.get("code", payload.get("code"))
            reason = nested.get("msg") or nested.get("message")
        else:
            code = payload.get("code")
            reason = nested or payload.get("message")

        # Attribution is by the venue's own echoed command id and by nothing
        # else. An error naming a command this splice did not issue for a
        # snapshot — including one naming no command at all — keeps the
        # pre-existing behaviour of ending the epoch, because assuming an
        # unattributable rejection was the poller's would swallow a rejected
        # *subscription* and leave a live-looking socket delivering no market
        # data, which is the failure this hook exists to prevent.
        command_id = payload.get("id")
        if isinstance(command_id, int) and command_id in self._snapshot_command_ids:
            await self._snapshot_command_rejected(command_id, code, reason)
            return

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
            # The poller's settings are the recovery bound this epoch can reach,
            # so a reader deciding how far to trust a window can read it off the
            # tape instead of inferring it from a deployment file it does not
            # have. Zero says the poller was off, which is a different fact from
            # "it was on and asked for nothing". The configured interval rather
            # than the live one: this record is written before the epoch resets
            # a backoff the previous connection was carrying, and an
            # `orderbook_reconciliation_backoff` record states any in-epoch
            # widening at the moment it happens.
            "snapshot_sweep_seconds": self._configured_sweep_seconds,
            "snapshot_max_age_seconds": self.snapshot_max_age_seconds,
            "snapshot_request_cooldown_seconds": self.snapshot_request_cooldown_seconds,
        }
        try:
            detail["key_id"] = self.credentials().key_id
        except KalshiCredentialsError:
            detail["key_id"] = None
        return detail
