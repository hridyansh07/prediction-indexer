"""Lossless venue-frame interpretation for book replay.

Known events gain typed views; every unknown delivery still produces a RawEvent.
Normalisation is therefore revisable without becoming a capture filter.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterator

from replay.order import OrderedEnvelope


@dataclass(frozen=True)
class EventBase:
    venue: str
    lane: str
    epoch: str
    record_id: str
    delivery_index: int
    order_ns: int
    visible_ns: int
    event_index: int


@dataclass(frozen=True)
class Level:
    price: Decimal
    size: Decimal


@dataclass(frozen=True)
class ConnectionOpened(EventBase):
    asset_ids: tuple[str, ...]
    delivers_deltas: bool
    target_metadata_digest: str | None = None


@dataclass(frozen=True)
class ConnectionClosed(EventBase):
    pass


@dataclass(frozen=True)
class MetadataChanged(EventBase):
    from_metadata_digest: str | None
    to_metadata_digest: str


@dataclass(frozen=True)
class FullBook(EventBase):
    market_id: str | None
    asset_id: str
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    state_hash: str | None
    source_time: str | int | None
    source_version: str | int | None
    independent_snapshot: bool


@dataclass(frozen=True)
class BookDelta(EventBase):
    market_id: str | None
    asset_id: str
    side: str
    price: Decimal
    size: Decimal
    state_hash: str | None
    source_time: str | int | None


@dataclass(frozen=True)
class Trade(EventBase):
    market_id: str | None
    asset_id: str
    price: Decimal
    size: Decimal
    side: str
    source_time: str | int | None
    transaction_id: str | None


@dataclass(frozen=True)
class ReferenceTick(EventBase):
    topic: str
    symbol: str | None
    value: Decimal | None
    source_time: str | int | None


@dataclass(frozen=True)
class GameState(EventBase):
    """One whole game state as the venue reported it, tagged and nothing more.

    This is an exogenous clock. A goal is one instant in the world that every
    venue quoting the match reacts to, so the interval between it and a book
    moving is what separates a venue that led from a venue that merely arrived
    first on our socket. Two consecutive states of the same game bracket that
    instant; nothing here computes the bracket, because the transition is a
    comparison between records and belongs wherever the comparison is made.

    Every typed field below is a convenience read. `raw` is the entire frame and
    `event_state` the entire `eventState` block, both verbatim, because the
    interpretation of this feed is not settled: `score` is a compound
    league-specific encoding (`000-000|1-1|Bo3` carries round score, map score,
    and series format in one string), `status` is spelled differently per league,
    and `eventState` carries sport-specific extras — `tournamentName`,
    `tennisRound` — that no fixed field set anticipates. Storing the block whole
    means a later reading is a code change rather than a re-collection.

    `state_updated_at` is the closest thing this feed has to the moment the world
    changed. It is present on roughly a quarter of frames, so it is an anchor
    where it exists and never a requirement.
    """

    #: `gameId`, or `metadataGameId` where the provider sends that instead —
    #: cricket does, and keying on `gameId` alone drops the sport in silence.
    game_id: str
    game_id_field: str
    league: str | None
    home: str | None
    away: str | None
    status: str | None
    score: str | None
    period: str | None
    elapsed: str | None
    live: bool | None
    ended: bool | None
    #: Present once a match reports finishing. Not terminal: a cricket match was
    #: observed carrying this, then reverting to `Scheduled` with `ended: false`
    #: six seconds later under the same identifier. Anything treating `ended` as
    #: an absorbing state, or the first `ended: true` as a resolution time, is
    #: wrong for that match.
    finished_timestamp: str | None
    #: The venue's own claim about when this state changed, from
    #: `eventState.updatedAt`. The one field that dates the event rather than its
    #: delivery.
    state_updated_at: str | None
    state_created_at: str | None
    event_state: dict[str, Any] | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class MarketLifecycle(EventBase):
    lifecycle: str
    market_id: str | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class RawEvent(EventBase):
    stream: str
    name: str
    raw: Any


ReplayEvent = (
    ConnectionOpened
    | ConnectionClosed
    | MetadataChanged
    | FullBook
    | BookDelta
    | Trade
    | ReferenceTick
    | GameState
    | MarketLifecycle
    | RawEvent
)

#: Fields that mark a `reference_event` frame as game state rather than a price
#: tick. Both feeds share one venue and one stream, so the discriminator has to
#: be the payload's own shape — the lane is not in the envelope, and inferring it
#: from the `record_id` prefix would make a naming convention load-bearing.
GAME_STATE_MARKERS = (
    "gameId",
    "metadataGameId",
    "leagueAbbreviation",
    "homeTeam",
    "awayTeam",
    "eventState",
)


def normalize(item: OrderedEnvelope) -> Iterator[ReplayEvent]:
    envelope = item.envelope
    base = dict(
        venue=envelope.venue,
        lane=item.lane,
        epoch=envelope.connection_epoch,
        record_id=envelope.record_id,
        delivery_index=envelope.delivery_index,
        order_ns=item.order_ns,
        visible_ns=envelope.visible_ns,
    )
    if envelope.stream == "process":
        try:
            payload = json.loads(envelope.raw_payload)
        except json.JSONDecodeError:
            yield RawEvent(**base, event_index=0, stream=envelope.stream, name="text", raw=envelope.raw_payload)
            return
        event = payload.get("event") if isinstance(payload, dict) else None
        if event == "connection_opened":
            yield ConnectionOpened(
                **base,
                event_index=0,
                asset_ids=tuple(str(value) for value in payload.get("asset_ids") or []),
                delivers_deltas=bool(payload.get("delivers_deltas")),
                target_metadata_digest=_optional_text(
                    payload.get("target_metadata_digest")
                ),
            )
        elif event == "connection_closed":
            yield ConnectionClosed(**base, event_index=0)
        elif event == "target_metadata_changed" and payload.get(
            "to_metadata_digest"
        ):
            yield MetadataChanged(
                **base,
                event_index=0,
                from_metadata_digest=_optional_text(
                    payload.get("from_metadata_digest")
                ),
                to_metadata_digest=str(payload["to_metadata_digest"]),
            )
        else:
            yield RawEvent(
                **base,
                event_index=0,
                stream=envelope.stream,
                name=str(event or "control"),
                raw=payload,
            )
        return

    try:
        payload = json.loads(envelope.raw_payload)
    except json.JSONDecodeError:
        yield RawEvent(
            **base,
            event_index=0,
            stream=envelope.stream,
            name="text",
            raw=envelope.raw_payload,
        )
        return

    if envelope.stream == "public_snapshot" and isinstance(payload, dict):
        book = _full_book(
            base,
            0,
            payload,
            market_id=payload.get("market"),
            asset_id=payload.get("asset_id") or payload.get("asset"),
            independent=True,
            source_version=None,
        )
        if book is not None:
            yield book
        else:
            yield RawEvent(
                **base,
                event_index=0,
                stream=envelope.stream,
                name="snapshot",
                raw=payload,
            )
        return

    if envelope.stream == "reference_event" and isinstance(payload, dict):
        # Checked before the price path because both feeds land on this stream.
        # Previously every game frame fell through to the tick branch and became
        # `ReferenceTick(topic='', symbol=None, value=None, source_time=None)` —
        # teams, score, period, live, ended and the whole `eventState` block
        # discarded at normalisation while sitting intact on the tape.
        if any(marker in payload for marker in GAME_STATE_MARKERS):
            yield _game_state(base, payload)
            return
        inner = payload.get("payload")
        symbol = inner.get("symbol") if isinstance(inner, dict) else None
        raw_value = (
            inner.get("full_accuracy_value", inner.get("value"))
            if isinstance(inner, dict)
            else None
        )
        yield ReferenceTick(
            **base,
            event_index=0,
            topic=str(payload.get("topic") or ""),
            symbol=str(symbol) if symbol is not None else None,
            value=_decimal_or_none(raw_value),
            source_time=(
                inner.get("timestamp")
                if isinstance(inner, dict) and inner.get("timestamp") is not None
                else payload.get("timestamp")
            ),
        )
        return

    raw_events = payload if isinstance(payload, list) else [payload]
    for outer_index, raw_event in enumerate(raw_events):
        if not isinstance(raw_event, dict):
            yield RawEvent(
                **base,
                event_index=outer_index,
                stream=envelope.stream,
                name=type(raw_event).__name__,
                raw=raw_event,
            )
            continue
        if envelope.venue == "polymarket":
            yield from _polymarket(base, outer_index, raw_event, envelope.stream)
        elif envelope.venue == "limitless":
            yield from _limitless(base, outer_index, raw_event, envelope.stream)
        else:
            yield RawEvent(
                **base,
                event_index=outer_index,
                stream=envelope.stream,
                name=str(raw_event.get("event") or raw_event.get("event_type") or "unknown"),
                raw=raw_event,
            )


def _game_state(base: dict[str, Any], payload: dict[str, Any]) -> ReplayEvent:
    """Tags one game frame. Interprets nothing and drops nothing.

    A frame carrying game markers but neither identifier becomes a named
    `RawEvent` rather than a `GameState` with a synthetic key. Cricket already
    proved the provider will introduce a second identifier field without notice,
    and a third would otherwise be keyed to the empty string and silently merge
    every such match into one.
    """
    identifier = payload.get("gameId")
    field = "gameId"
    if identifier is None:
        identifier = payload.get("metadataGameId")
        field = "metadataGameId"
    if identifier is None:
        return RawEvent(
            **base,
            event_index=0,
            stream="reference_event",
            name="game_state_without_identifier",
            raw=payload,
        )

    state = payload.get("eventState") if isinstance(payload.get("eventState"), dict) else None
    return GameState(
        **base,
        event_index=0,
        game_id=str(identifier),
        game_id_field=field,
        league=_optional_text(payload.get("leagueAbbreviation")),
        home=_optional_text(payload.get("homeTeam")),
        away=_optional_text(payload.get("awayTeam")),
        status=_optional_text(payload.get("status")),
        # Read from `eventState` first where it exists: that is the block the
        # provider timestamps, and taking the value from one place and the time
        # from another would let them describe different instants with nothing on
        # the record showing it.
        score=_optional_text(_prefer(state, payload, "score")),
        period=_optional_text(_prefer(state, payload, "period")),
        elapsed=_optional_text(_prefer(state, payload, "elapsed")),
        live=_optional_bool(_prefer(state, payload, "live")),
        ended=_optional_bool(_prefer(state, payload, "ended")),
        finished_timestamp=_optional_text(payload.get("finishedTimestamp")),
        state_updated_at=_optional_text(state.get("updatedAt")) if state else None,
        state_created_at=_optional_text(state.get("createdAt")) if state else None,
        event_state=state,
        raw=payload,
    )


def _prefer(state: dict[str, Any] | None, payload: dict[str, Any], field: str) -> Any:
    if state is not None and field in state:
        return state[field]
    return payload.get(field)


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _polymarket(
    base: dict[str, Any], index: int, event: dict[str, Any], stream: str
) -> Iterator[ReplayEvent]:
    name = str(event.get("event_type") or event.get("event") or "")
    if name == "book":
        book = _full_book(
            base,
            index,
            event,
            market_id=event.get("market"),
            asset_id=event.get("asset_id"),
            independent=False,
            source_version=None,
        )
        if book is not None:
            yield book
            return
    elif name == "price_change":
        changes = event.get("price_changes")
        if isinstance(changes, list):
            for change_index, change in enumerate(changes):
                delta: ReplayEvent | None = None
                if isinstance(change, dict):
                    try:
                        delta = BookDelta(
                            **base,
                            event_index=index * 1_000_000 + change_index,
                            market_id=_optional_text(event.get("market")),
                            asset_id=str(change["asset_id"]),
                            side=str(change["side"]).upper(),
                            price=_decimal(change["price"]),
                            size=_decimal(change["size"]),
                            state_hash=_optional_text(change.get("hash")),
                            source_time=event.get("timestamp"),
                        )
                    except (KeyError, ValueError):
                        delta = None
                if delta is not None:
                    yield delta
                    continue
                # A change we cannot type still becomes an event. This module
                # promises that every delivery produces something, and it did not
                # keep that promise here: an unparseable entry was skipped and a
                # frame whose entries were all unparseable produced nothing at
                # all — no record, no counter, nothing downstream able to notice.
                # The bytes were safe on the tape and invisible to every reader,
                # which is the one failure shape this whole design exists to
                # prevent.
                yield RawEvent(
                    **base,
                    event_index=index * 1_000_000 + change_index,
                    stream=stream,
                    name="unparseable_price_change",
                    raw=change,
                )
            return
    elif name == "last_trade_price":
        try:
            yield Trade(
                **base,
                event_index=index,
                market_id=_optional_text(event.get("market")),
                asset_id=str(event["asset_id"]),
                price=_decimal(event["price"]),
                size=_decimal(event["size"]),
                side=str(event["side"]).upper(),
                source_time=event.get("timestamp"),
                transaction_id=_optional_text(event.get("transaction_hash")),
            )
            return
        except (KeyError, ValueError):
            pass
    normalized = name.lower()
    if normalized in {"newmarketevent", "new_market", "market_created"}:
        yield MarketLifecycle(
            **base,
            event_index=index,
            lifecycle="CREATED",
            market_id=_optional_text(event.get("market") or event.get("condition_id")),
            raw=event,
        )
        return
    if normalized in {"marketresolved", "market_resolved", "resolved"}:
        yield MarketLifecycle(
            **base,
            event_index=index,
            lifecycle="RESOLVED",
            market_id=_optional_text(event.get("market") or event.get("condition_id")),
            raw=event,
        )
        return
    yield RawEvent(**base, event_index=index, stream=stream, name=name or "unknown", raw=event)


def _limitless(
    base: dict[str, Any], index: int, event: dict[str, Any], stream: str
) -> Iterator[ReplayEvent]:
    name = str(event.get("event") or "")
    data = event.get("data")
    if name == "orderbookUpdate" and isinstance(data, dict):
        book_data = data.get("orderbook")
        if isinstance(book_data, dict):
            combined = {
                **book_data,
                "hash": None,
                "timestamp": data.get("timestamp"),
            }
            book = _full_book(
                base,
                index,
                combined,
                market_id=data.get("marketSlug"),
                asset_id=data.get("marketSlug"),
                independent=True,
                source_version=data.get("version"),
            )
            if book is not None:
                yield book
                return
    normalized = name.lower()
    if normalized in {"marketcreated", "market_created"}:
        yield MarketLifecycle(
            **base,
            event_index=index,
            lifecycle="CREATED",
            market_id=_lifecycle_market_id(data),
            raw=event,
        )
        return
    if normalized in {"marketresolved", "market_resolved"}:
        yield MarketLifecycle(
            **base,
            event_index=index,
            lifecycle="RESOLVED",
            market_id=_lifecycle_market_id(data),
            raw=event,
        )
        return
    yield RawEvent(**base, event_index=index, stream=stream, name=name or "unknown", raw=event)


def _full_book(
    base: dict[str, Any],
    index: int,
    value: dict[str, Any],
    *,
    market_id: Any,
    asset_id: Any,
    independent: bool,
    source_version: str | int | None,
) -> FullBook | None:
    if asset_id is None or not isinstance(value.get("bids"), list) or not isinstance(value.get("asks"), list):
        return None
    try:
        bids = tuple(_level(level) for level in value["bids"])
        asks = tuple(_level(level) for level in value["asks"])
    except (KeyError, ValueError, TypeError):
        return None
    return FullBook(
        **base,
        event_index=index,
        market_id=_optional_text(market_id),
        asset_id=str(asset_id),
        bids=bids,
        asks=asks,
        state_hash=_optional_text(value.get("hash")),
        source_time=value.get("timestamp"),
        source_version=source_version,
        independent_snapshot=independent,
    )


def _level(value: Any) -> Level:
    if not isinstance(value, dict):
        raise TypeError("book level is not an object")
    return Level(price=_decimal(value["price"]), size=_decimal(value["size"]))


def _decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("boolean is not decimal")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"not a decimal: {value!r}") from error
    if not parsed.is_finite() or parsed < 0:
        raise ValueError(f"decimal must be finite and non-negative: {value!r}")
    return parsed


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return _decimal(value)
    except ValueError:
        return None


def _optional_text(value: Any) -> str | None:
    return str(value) if value is not None and str(value) else None


def _lifecycle_market_id(data: Any) -> str | None:
    if not isinstance(data, dict):
        return None
    return _optional_text(data.get("marketSlug") or data.get("slug") or data.get("id"))
