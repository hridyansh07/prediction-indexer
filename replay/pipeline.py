"""Replay orchestration over the storage-agnostic byte boundary."""

from __future__ import annotations

from typing import Iterator

from replay.books import AnchorCheck, BookReplay, ReplayedBookState
from replay.events import ReplayEvent, normalize
from replay.order import OrderedTape, OrderingDecision
from replay.stream import ByteStreamer


class ReplayPipeline:
    def __init__(self, streamer: ByteStreamer) -> None:
        self.streamer = streamer
        self.tape = OrderedTape(streamer)

    @property
    def ordering(self) -> OrderingDecision:
        return self.tape.ordering

    def events(self) -> Iterator[ReplayEvent]:
        # A fresh tape per traversal so concurrent passes keep independent
        # per-lane cursors, but the ordering decision is carried over rather than
        # re-derived: computing it parses every record in the dataset, and a
        # single analysis walks the tape four times. Measured on a real 1,478
        # record capture, this took the run from nine full parses to five.
        for envelope in OrderedTape(self.streamer, ordering=self.ordering):
            yield from normalize(envelope)

    def polymarket_anchor_checks(self) -> tuple[AnchorCheck, ...]:
        return BookReplay().verify_polymarket_anchors(self.events(), self.events())

    def book_states(self) -> Iterator[ReplayedBookState]:
        yield from BookReplay().states(self.events(), self.events())
