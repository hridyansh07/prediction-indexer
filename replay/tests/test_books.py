from __future__ import annotations

import unittest
from decimal import Decimal

from replay.books import BookReplay
from replay.events import BookDelta, FullBook, Level


def _base(index: int) -> dict[str, object]:
    return {
        "venue": "polymarket",
        "lane": "polymarket",
        "epoch": "epoch",
        "record_id": f"record-{index}",
        "delivery_index": index,
        "order_ns": index,
        "visible_ns": index,
        "event_index": 0,
    }


def _full(
    index: int,
    *,
    bids: tuple[tuple[str, str], ...],
    state_hash: str,
    independent: bool,
) -> FullBook:
    return FullBook(
        **_base(index),
        market_id="market",
        asset_id="asset",
        bids=tuple(Level(Decimal(price), Decimal(size)) for price, size in bids),
        asks=(),
        state_hash=state_hash,
        source_time=index,
        source_version=None,
        independent_snapshot=independent,
    )


def _delta(index: int, price: str, size: str, state_hash: str) -> BookDelta:
    return BookDelta(
        **_base(index),
        market_id="market",
        asset_id="asset",
        side="BUY",
        price=Decimal(price),
        size=Decimal(size),
        state_hash=state_hash,
        source_time=10,
    )


class BookReplayTests(unittest.TestCase):
    def test_repeated_hash_across_frames_is_checked_after_complete_hash_run(self) -> None:
        initial = _full(
            1,
            bids=(("0.1", "1"),),
            state_hash="initial",
            independent=False,
        )
        first_part = _delta(2, "0.2", "2", "final")
        second_part = _delta(3, "0.3", "3", "final")
        anchor = _full(
            4,
            bids=(("0.1", "1"), ("0.2", "2"), ("0.3", "3")),
            state_hash="final",
            independent=True,
        )

        (check,) = BookReplay().verify_polymarket_anchors(
            [anchor], [initial, first_part, second_part]
        )

        self.assertTrue(check.matched)
        self.assertEqual(check.reason, "levels_match")

    def test_mismatched_snapshot_recovers_the_following_hash_chain(self) -> None:
        initial = _full(
            1,
            bids=(("0.1", "1"),),
            state_hash="initial",
            independent=False,
        )
        first_stream_state = _delta(2, "0.2", "2", "first")
        first_anchor = _full(
            3,
            bids=(("0.1", "2"), ("0.2", "2")),
            state_hash="first",
            independent=True,
        )
        second_stream_state = _delta(4, "0.3", "3", "second")
        second_anchor = _full(
            5,
            bids=(("0.1", "2"), ("0.2", "2"), ("0.3", "3")),
            state_hash="second",
            independent=True,
        )

        checks = BookReplay().verify_polymarket_anchors(
            [first_anchor, second_anchor],
            [initial, first_stream_state, second_stream_state],
        )

        self.assertFalse(checks[0].matched)
        self.assertTrue(checks[1].matched)


if __name__ == "__main__":
    unittest.main()
