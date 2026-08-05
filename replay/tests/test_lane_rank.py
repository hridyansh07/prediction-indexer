"""The lane rank table: parity with the Rust authority, and what it orders.

`ingester/crates/finalize/src/rank.rs` owns the table, because the finalizer is
the only thing that turns it into bytes — canonical evidence and its
`EvidenceSeq` are reproducible only if one definition decides ties.
`replay/lanes.py` mirrors it for the raw-segment replay path, and a mirror with
nothing checking it is how `_lane_of` came to exist twice and be wrong twice.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

from replay.lanes import LANE_RANK, lane_rank

RANK_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "ingester"
    / "crates"
    / "finalize"
    / "src"
    / "rank.rs"
)

#: Matches one `("<lane>", <rank>),` entry of `LANE_RANKS`. The Rust side is
#: documented to keep that shape precisely so this can read it without a build.
ENTRY = re.compile(r'\(\s*"([a-z_]+)"\s*,\s*(\d+)\s*\)')


def rust_lane_ranks() -> dict[str, int]:
    """The authority's table, parsed out of its source.

    Read from source rather than by shelling out to `cargo run`, so the check
    costs milliseconds and works in an environment with no Rust toolchain. The
    table is a flat literal, which is the only reason this is reasonable.
    """
    text = RANK_SOURCE.read_text(encoding="utf-8")
    start = text.index("pub const LANE_RANKS")
    body = text[start : text.index("];", start)]
    return {lane: int(rank) for lane, rank in ENTRY.findall(body)}


class LaneRankParityTests(unittest.TestCase):
    def test_the_python_mirror_matches_the_rust_authority(self):
        self.assertEqual(
            rust_lane_ranks(),
            LANE_RANK,
            "the mirror has drifted from ingester/crates/finalize/src/rank.rs; "
            "the canonical file and raw-segment replay would order ties differently",
        )

    def test_the_table_is_the_one_the_spec_states(self):
        # §1 of docs/SEALED_CAPTURE_PIPELINE_V1.md, transcribed independently of
        # either implementation so a matching pair of wrong tables still fails.
        self.assertEqual(
            LANE_RANK,
            {
                "polymarket": 0,
                "polymarket_snapshots": 1,
                "polymarket_sports": 2,
                "polymarket_rtds": 3,
                "kalshi": 10,
                "limitless": 20,
            },
        )

    def test_ranks_are_distinct(self):
        # A shared rank pushes the decision onto `delivery_index`, comparing
        # counters minted by two independent splices as if they were one run.
        self.assertEqual(len(set(LANE_RANK.values())), len(LANE_RANK))


class TieOrderTests(unittest.TestCase):
    """What the two tie-break rules actually produce, side by side.

    `replay/order.py` sorts ties on `(order_ns, venue, lane, …)` — plain strings,
    ascending. This records exactly where that lands relative to the ranked order
    so the difference is a fact on the record rather than an argument.
    """

    LANES = (
        "polymarket",
        "polymarket_snapshots",
        "polymarket_sports",
        "polymarket_rtds",
        "kalshi",
        "limitless",
    )

    @staticmethod
    def venue_of(lane: str) -> str:
        # What the envelope carries. All four Polymarket lanes say `polymarket`.
        return lane.split("_")[0]

    def ranked_order(self) -> list[str]:
        return sorted(self.LANES, key=lane_rank)

    def string_order(self) -> list[str]:
        return sorted(self.LANES, key=lambda lane: (self.venue_of(lane), lane))

    def test_the_ranked_order_is_the_spec_order(self):
        self.assertEqual(
            self.ranked_order(),
            [
                "polymarket",
                "polymarket_snapshots",
                "polymarket_sports",
                "polymarket_rtds",
                "kalshi",
                "limitless",
            ],
        )

    def test_the_string_order_differs_at_the_venue_level(self):
        # Alphabetical venues put Polymarket last where the spec puts it first.
        self.assertEqual(
            [self.venue_of(lane) for lane in self.string_order()][:1], ["kalshi"]
        )
        self.assertEqual(
            [self.venue_of(lane) for lane in self.ranked_order()][:1], ["polymarket"]
        )

    def test_the_string_order_also_differs_inside_polymarket(self):
        # The subtler half. `venue` is `polymarket` for all four lanes, so it
        # cannot separate them and the `lane` term sorts them alphabetically —
        # which puts `polymarket_rtds` second where §1 ranks it fourth.
        polymarket_only = [
            lane for lane in self.string_order() if self.venue_of(lane) == "polymarket"
        ]
        self.assertEqual(
            polymarket_only,
            [
                "polymarket",
                "polymarket_rtds",
                "polymarket_snapshots",
                "polymarket_sports",
            ],
        )
        self.assertEqual(
            [lane for lane in self.ranked_order() if self.venue_of(lane) == "polymarket"],
            [
                "polymarket",
                "polymarket_snapshots",
                "polymarket_sports",
                "polymarket_rtds",
            ],
        )

    def test_the_two_orders_are_not_the_same(self):
        # The claim in one assertion. If `replay/order.py` is ever switched onto
        # `lane_rank`, this is the test that has to be revisited deliberately.
        self.assertNotEqual(self.string_order(), self.ranked_order())


if __name__ == "__main__":
    unittest.main()
