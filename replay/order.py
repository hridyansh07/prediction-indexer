"""Deterministic scope-aware merge of independent capture lanes."""

from __future__ import annotations

import heapq
import json
from dataclasses import dataclass
from typing import Iterator

from replay.envelope import Envelope, parse_envelope
from replay.lanes import lane_of, lane_rank
from replay.stream import ByteStreamer, iter_ndjson_lines


@dataclass(frozen=True)
class OrderedEnvelope:
    lane: str
    object_key: str
    line_number: int
    order_ns: int
    order_clock: str
    envelope: Envelope


@dataclass(frozen=True)
class OrderingDecision:
    clock: str
    scope_id: str | None
    reason: str

    def as_record(self) -> dict[str, str | None]:
        return {"clock": self.clock, "scope_id": self.scope_id, "reason": self.reason}


class OrderedTape:
    """Reads a byte dataset repeatedly but never retains all records in memory."""

    def __init__(self, streamer: ByteStreamer, *, ordering: OrderingDecision | None = None) -> None:
        self.streamer = streamer
        self._keys_by_lane = _keys_by_lane(streamer)
        # Deciding the clock costs a full parse of every record. It is a property
        # of the dataset, not of one traversal, so a caller that already paid for
        # it passes it back rather than paying again — `ReplayPipeline` walks the
        # tape four times per analysis and was re-deriving this on each one.
        self.ordering = ordering if ordering is not None else self._decide_clock()
        #: Delivery-index discontinuities seen while reading, per lane. Empty for
        #: a complete dataset; populated for a slice. Never raised — see below.
        self.delivery_breaks: list[str] = []

    def __iter__(self) -> Iterator[OrderedEnvelope]:
        iterators = {
            lane: self._lane_records(lane, keys)
            for lane, keys in self._keys_by_lane.items()
        }
        heap: list[tuple[tuple[object, ...], str, OrderedEnvelope]] = []
        for lane, iterator in iterators.items():
            try:
                item = next(iterator)
            except StopIteration:
                continue
            heapq.heappush(heap, (_sort_key(item), lane, item))
        while heap:
            _, lane, item = heapq.heappop(heap)
            yield item
            try:
                following = next(iterators[lane])
            except StopIteration:
                continue
            heapq.heappush(heap, (_sort_key(following), lane, following))

    def _lane_records(self, lane: str, keys: tuple[str, ...]) -> Iterator[OrderedEnvelope]:
        previous_index: int | None = None
        for line in iter_ndjson_lines(self.streamer, keys=keys):
            envelope = parse_envelope(line.data)
            if previous_index is not None and envelope.delivery_index != previous_index + 1:
                # Recorded, not raised. A gap here has two causes that look
                # identical from inside the merge: capture lost records, or the
                # dataset is a deliberate slice — one day out of a month, one
                # lane, a filtered subset. Replaying a slice is a routine thing
                # to want, and refusing it made the common case pay for the rare
                # one, with a ValueError thrown from inside a generator halfway
                # through a k-way merge.
                #
                # Judging is Gate 1's job and it already does it: it fails
                # `deterministic_capture_order` on exactly this condition for a
                # dataset that claims to be complete. Reading permissively and
                # judging separately keeps a slice usable without making a real
                # loss any quieter — `delivery_breaks` carries it either way.
                self.delivery_breaks.append(
                    f"{lane}:{previous_index}->{envelope.delivery_index}"
                )
            previous_index = envelope.delivery_index
            if self.ordering.clock == "monotonic_ns":
                if envelope.monotonic_ns is None:
                    raise ValueError("monotonic ordering selected but a v1 record was encountered")
                order_ns = envelope.monotonic_ns
            else:
                order_ns = envelope.visible_ns
            yield OrderedEnvelope(
                lane=lane,
                object_key=line.object_key,
                line_number=line.line_number,
                order_ns=order_ns,
                order_clock=self.ordering.clock,
                envelope=envelope,
            )

    def _decide_clock(self) -> OrderingDecision:
        scopes: list[dict[str, object]] = []
        all_v2 = True
        for line in iter_ndjson_lines(self.streamer):
            envelope = parse_envelope(line.data)
            all_v2 = all_v2 and envelope.monotonic_ns is not None
            if envelope.kind != "control" or envelope.stream != "process":
                continue
            try:
                payload = json.loads(envelope.raw_payload)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and payload.get("event") == "connection_opened":
                scope = payload.get("clock_scope")
                if isinstance(scope, dict):
                    scopes.append(scope)
        scope_ids = {
            str(scope.get("scope_id"))
            for scope in scopes
            if scope.get("scope_id") is not None
        }
        comparable = bool(scopes) and all(
            scope.get("comparable_across_processes") is True for scope in scopes
        )
        if all_v2 and comparable and len(scope_ids) == 1:
            return OrderingDecision(
                clock="monotonic_ns",
                scope_id=next(iter(scope_ids)),
                reason="all connection scopes are cross-process comparable within one boot",
            )
        return OrderingDecision(
            clock="visible_ns",
            scope_id=None,
            reason=(
                "monotonic clocks are not globally comparable across every lane; "
                "wall receive time is the deterministic fallback"
            ),
        )


def _keys_by_lane(streamer: ByteStreamer) -> dict[str, tuple[str, ...]]:
    grouped: dict[str, list[str]] = {}
    for key in streamer.object_keys():
        if not key.endswith(".ndjson"):
            continue
        lane = lane_of(key)
        grouped.setdefault(lane, []).append(key)
    return {lane: tuple(sorted(keys)) for lane, keys in sorted(grouped.items())}


def _sort_key(item: OrderedEnvelope) -> tuple[object, ...]:
    """`(order_ns, lane_rank, delivery_index)`, then captured facts.

    The rank comes from `replay.lanes`, mirroring the finalizer's table, so a
    dataset replayed from raw segments and the same dataset replayed from
    canonical evidence resolve an exact timestamp collision the same way.

    This used to sort ties on `(venue, lane)` as plain ascending strings, which
    disagreed with `docs/SEALED_CAPTURE_PIPELINE_V1.md` §1 in two places. The
    obvious one is venue order: alphabetically Polymarket sorts last where §1
    ranks it first. The quieter one is inside Polymarket — all four of its lanes
    carry `venue: polymarket`, so the `venue` term never separated them and the
    `lane` term ordered them alphabetically, putting `polymarket_rtds` second
    where §1 ranks it fourth. `replay/tests/test_lane_rank.py` holds both orders
    side by side.

    Rank decides a tie and nothing else — see `replay.lanes.LANE_RANK` for what
    analysis may not read out of it. The remaining terms are all captured facts;
    source path is last and matters only for an exact collision with identical
    counters.
    """
    return (
        item.order_ns,
        lane_rank(item.lane),
        item.lane,
        item.envelope.delivery_index,
        item.envelope.record_id,
        item.object_key,
        item.line_number,
    )
