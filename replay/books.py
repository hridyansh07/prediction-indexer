"""Deterministic book reconstruction and independent Polymarket anchor checks."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable

from replay.events import (
    BookDelta,
    ConnectionOpened,
    FullBook,
    Level,
    ReplayEvent,
)


@dataclass(frozen=True)
class CanonicalBook:
    bids: tuple[tuple[str, str], ...]
    asks: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class AnchorCheck:
    asset_id: str
    state_hash: str
    stream_order_ns: int | None
    snapshot_receive_ns: int
    matched: bool
    reason: str

    def as_record(self) -> dict[str, object]:
        return {
            "asset_id": self.asset_id,
            "state_hash": self.state_hash,
            "stream_order_ns": self.stream_order_ns,
            "snapshot_receive_ns": self.snapshot_receive_ns,
            "matched": self.matched,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ReplayedBookState:
    venue: str
    market_id: str
    asset_id: str
    order_ns: int
    visible_ns: int
    state_hash: str | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    source: str


@dataclass(frozen=True)
class _PendingState:
    market_id: str
    state_hash: str
    order_ns: int
    visible_ns: int


class BookReplay:
    """Two-pass verifier over a replayable event iterable factory."""

    def verify_polymarket_anchors(
        self, first_pass: Iterable[ReplayEvent], second_pass: Iterable[ReplayEvent]
    ) -> tuple[AnchorCheck, ...]:
        anchors: list[FullBook] = [
            event
            for event in first_pass
            if isinstance(event, FullBook)
            and event.venue == "polymarket"
            and event.independent_snapshot
            and event.state_hash is not None
        ]
        wanted = {(event.asset_id, event.state_hash) for event in anchors}
        recovery_books: dict[tuple[str, str], CanonicalBook] = {}
        recovery_conflicts: set[tuple[str, str]] = set()
        for anchor in anchors:
            key = (anchor.asset_id, anchor.state_hash or "")
            book = canonical_book(anchor)
            previous = recovery_books.get(key)
            if previous is not None and previous != book:
                recovery_conflicts.add(key)
            else:
                recovery_books[key] = book
        candidates: dict[tuple[str, str], tuple[int, CanonicalBook]] = {}
        conflicts: set[tuple[str, str]] = set()
        states: dict[str, _MutableBook] = {}
        pending: dict[str, tuple[str, int]] = {}

        for event in second_pass:
            if isinstance(event, ConnectionOpened) and event.lane == "polymarket":
                for asset_id in event.asset_ids:
                    self._flush(
                        asset_id,
                        states,
                        pending,
                        wanted,
                        recovery_books,
                        recovery_conflicts,
                        candidates,
                        conflicts,
                    )
                    states.pop(asset_id, None)
                continue
            if isinstance(event, FullBook):
                if event.venue != "polymarket" or event.independent_snapshot:
                    continue
                if (
                    event.asset_id in pending
                    and pending[event.asset_id][0] != event.state_hash
                ):
                    self._flush(
                        event.asset_id,
                        states,
                        pending,
                        wanted,
                        recovery_books,
                        recovery_conflicts,
                        candidates,
                        conflicts,
                    )
                state = _MutableBook.from_full(event)
                states[event.asset_id] = state
                if event.state_hash is not None:
                    pending[event.asset_id] = (event.state_hash, event.order_ns)
            elif isinstance(event, BookDelta) and event.venue == "polymarket":
                if (
                    event.asset_id in pending
                    and pending[event.asset_id][0] != event.state_hash
                ):
                    self._flush(
                        event.asset_id,
                        states,
                        pending,
                        wanted,
                        recovery_books,
                        recovery_conflicts,
                        candidates,
                        conflicts,
                    )
                state = states.get(event.asset_id)
                if state is None:
                    continue
                state.apply(event)
                if event.state_hash is not None:
                    pending[event.asset_id] = (event.state_hash, event.order_ns)

        for asset_id in tuple(pending):
            self._flush(
                asset_id,
                states,
                pending,
                wanted,
                recovery_books,
                recovery_conflicts,
                candidates,
                conflicts,
            )

        checks: list[AnchorCheck] = []
        for anchor in anchors:
            key = (anchor.asset_id, anchor.state_hash or "")
            expected = canonical_book(anchor)
            candidate = candidates.get(key)
            if key in conflicts:
                checks.append(
                    AnchorCheck(
                        asset_id=anchor.asset_id,
                        state_hash=key[1],
                        stream_order_ns=candidate[0] if candidate else None,
                        snapshot_receive_ns=anchor.order_ns,
                        matched=False,
                        reason="stream_hash_mapped_to_conflicting_reconstructed_states",
                    )
                )
            elif candidate is None:
                checks.append(
                    AnchorCheck(
                        asset_id=anchor.asset_id,
                        state_hash=key[1],
                        stream_order_ns=None,
                        snapshot_receive_ns=anchor.order_ns,
                        matched=False,
                        reason="snapshot_hash_not_observed_in_stream",
                    )
                )
            else:
                checks.append(
                    AnchorCheck(
                        asset_id=anchor.asset_id,
                        state_hash=key[1],
                        stream_order_ns=candidate[0],
                        snapshot_receive_ns=anchor.order_ns,
                        matched=candidate[1] == expected,
                        reason=(
                            "levels_match"
                            if candidate[1] == expected
                            else "reconstructed_levels_differ_from_snapshot"
                        ),
                    )
                )
        return tuple(
            sorted(
                checks,
                key=lambda item: (
                    item.stream_order_ns
                    if item.stream_order_ns is not None
                    else item.snapshot_receive_ns,
                    item.asset_id,
                    item.state_hash,
                ),
            )
        )

    def states(
        self, first_pass: Iterable[ReplayEvent], second_pass: Iterable[ReplayEvent]
    ) -> Iterable[ReplayedBookState]:
        recovery_books: dict[tuple[str, str], CanonicalBook] = {}
        recovery_conflicts: set[tuple[str, str]] = set()
        for event in first_pass:
            if not (
                isinstance(event, FullBook)
                and event.venue == "polymarket"
                and event.independent_snapshot
                and event.state_hash is not None
            ):
                continue
            key = (event.asset_id, event.state_hash)
            book = canonical_book(event)
            previous = recovery_books.get(key)
            if previous is not None and previous != book:
                recovery_conflicts.add(key)
            else:
                recovery_books[key] = book

        states: dict[str, _MutableBook] = {}
        pending: dict[str, _PendingState] = {}
        observed_anchor_keys: set[tuple[str, str]] = set()
        for event in second_pass:
            if isinstance(event, ConnectionOpened) and event.venue == "polymarket":
                if not event.delivers_deltas:
                    continue
                for asset_id in event.asset_ids:
                    completed = self._flush_state(
                        asset_id,
                        states,
                        pending,
                        recovery_books,
                        recovery_conflicts,
                        observed_anchor_keys,
                    )
                    if completed is not None:
                        yield completed
                    states.pop(asset_id, None)
                continue
            if not isinstance(event, FullBook | BookDelta):
                continue
            if event.venue == "limitless" and isinstance(event, FullBook):
                if event.market_id is None:
                    continue
                yield ReplayedBookState(
                    venue=event.venue,
                    market_id=event.market_id,
                    asset_id=event.asset_id,
                    order_ns=event.order_ns,
                    visible_ns=event.visible_ns,
                    state_hash=event.state_hash,
                    bids=tuple(
                        sorted(event.bids, key=lambda level: level.price, reverse=True)
                    ),
                    asks=tuple(
                        sorted(event.asks, key=lambda level: level.price)
                    ),
                    source="venue_full_book",
                )
                continue
            if event.venue != "polymarket" or event.market_id is None:
                continue
            if isinstance(event, FullBook) and event.independent_snapshot:
                completed = self._flush_state(
                    event.asset_id,
                    states,
                    pending,
                    recovery_books,
                    recovery_conflicts,
                    observed_anchor_keys,
                )
                if completed is not None:
                    yield completed
                key = (
                    (event.asset_id, event.state_hash)
                    if event.state_hash is not None
                    else None
                )
                if key is None or key not in observed_anchor_keys:
                    states[event.asset_id] = _MutableBook.from_full(event)
                state = states.get(event.asset_id)
                if state is not None:
                    yield self._state_record(
                        venue=event.venue,
                        market_id=event.market_id,
                        asset_id=event.asset_id,
                        order_ns=event.order_ns,
                        visible_ns=event.visible_ns,
                        state_hash=event.state_hash,
                        state=state,
                        source="independent_snapshot_recovery_available",
                    )
                continue
            current = pending.get(event.asset_id)
            if current is not None and current.state_hash != event.state_hash:
                completed = self._flush_state(
                    event.asset_id,
                    states,
                    pending,
                    recovery_books,
                    recovery_conflicts,
                    observed_anchor_keys,
                )
                if completed is not None:
                    yield completed
            if isinstance(event, FullBook):
                states[event.asset_id] = _MutableBook.from_full(event)
            else:
                state = states.get(event.asset_id)
                if state is None:
                    continue
                state.apply(event)
            if event.state_hash is not None:
                pending[event.asset_id] = _PendingState(
                    market_id=event.market_id,
                    state_hash=event.state_hash,
                    order_ns=event.order_ns,
                    visible_ns=event.visible_ns,
                )

        for asset_id in tuple(pending):
            completed = self._flush_state(
                asset_id,
                states,
                pending,
                recovery_books,
                recovery_conflicts,
                observed_anchor_keys,
            )
            if completed is not None:
                yield completed

    @classmethod
    def _flush_state(
        cls,
        asset_id: str,
        states: dict[str, "_MutableBook"],
        pending: dict[str, _PendingState],
        recovery_books: dict[tuple[str, str], CanonicalBook],
        recovery_conflicts: set[tuple[str, str]],
        observed_anchor_keys: set[tuple[str, str]],
    ) -> ReplayedBookState | None:
        value = pending.pop(asset_id, None)
        state = states.get(asset_id)
        if value is None or state is None:
            return None
        record = cls._state_record(
            venue="polymarket",
            market_id=value.market_id,
            asset_id=asset_id,
            order_ns=value.order_ns,
            visible_ns=value.visible_ns,
            state_hash=value.state_hash,
            state=state,
            source="venue_hash_state",
        )
        key = (asset_id, value.state_hash)
        if key in recovery_books:
            observed_anchor_keys.add(key)
            if key not in recovery_conflicts:
                states[asset_id] = _MutableBook.from_canonical(
                    recovery_books[key]
                )
        return record

    @staticmethod
    def _state_record(
        *,
        venue: str,
        market_id: str,
        asset_id: str,
        order_ns: int,
        visible_ns: int,
        state_hash: str | None,
        state: "_MutableBook",
        source: str,
    ) -> ReplayedBookState:
        return ReplayedBookState(
            venue=venue,
            market_id=market_id,
            asset_id=asset_id,
            order_ns=order_ns,
            visible_ns=visible_ns,
            state_hash=state_hash,
            bids=state.levels("BUY"),
            asks=state.levels("SELL"),
            source=source,
        )

    @classmethod
    def _flush(
        cls,
        asset_id: str,
        states: dict[str, "_MutableBook"],
        pending: dict[str, tuple[str, int]],
        wanted: set[tuple[str, str]],
        recovery_books: dict[tuple[str, str], CanonicalBook],
        recovery_conflicts: set[tuple[str, str]],
        candidates: dict[tuple[str, str], tuple[int, CanonicalBook]],
        conflicts: set[tuple[str, str]],
    ) -> None:
        value = pending.pop(asset_id, None)
        state = states.get(asset_id)
        if value is None or state is None:
            return
        state_hash, order_ns = value
        key = (asset_id, state_hash)
        if key not in wanted:
            return
        observed = state.canonical()
        previous = candidates.get(key)
        if previous is not None and previous[1] != observed:
            conflicts.add(key)
        else:
            candidates[key] = (order_ns, observed)
        if key not in recovery_conflicts:
            states[asset_id] = _MutableBook.from_canonical(recovery_books[key])


class _MutableBook:
    def __init__(self, bids: dict[Decimal, Decimal], asks: dict[Decimal, Decimal]) -> None:
        self.bids = bids
        self.asks = asks

    @classmethod
    def from_full(cls, event: FullBook) -> "_MutableBook":
        return cls(
            bids={level.price: level.size for level in event.bids if level.size > 0},
            asks={level.price: level.size for level in event.asks if level.size > 0},
        )

    @classmethod
    def from_canonical(cls, book: CanonicalBook) -> "_MutableBook":
        return cls(
            bids={Decimal(price): Decimal(size) for price, size in book.bids},
            asks={Decimal(price): Decimal(size) for price, size in book.asks},
        )

    def apply(self, event: BookDelta) -> None:
        side = self.bids if event.side == "BUY" else self.asks
        if event.size == 0:
            side.pop(event.price, None)
        else:
            side[event.price] = event.size

    def canonical(self) -> CanonicalBook:
        return CanonicalBook(
            bids=_levels(self.bids, reverse=True),
            asks=_levels(self.asks, reverse=False),
        )

    def levels(self, side: str) -> tuple[Level, ...]:
        values = self.bids if side == "BUY" else self.asks
        return tuple(
            Level(price, values[price])
            for price in sorted(values, reverse=side == "BUY")
        )


def canonical_book(event: FullBook) -> CanonicalBook:
    return _MutableBook.from_full(event).canonical()


def _levels(
    levels: dict[Decimal, Decimal], *, reverse: bool
) -> tuple[tuple[str, str], ...]:
    return tuple(
        (_decimal_text(price), _decimal_text(levels[price]))
        for price in sorted(levels, reverse=reverse)
    )


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f")
