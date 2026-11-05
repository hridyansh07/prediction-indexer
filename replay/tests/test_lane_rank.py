"""The lane rank table: parity with the Rust authority, and what it orders.

`ingester/crates/finalize/src/rank.rs` owns the table, because the finalizer is
the only thing that turns it into bytes — canonical evidence and its
`EvidenceSeq` are reproducible only if one definition decides ties.
`replay/lanes.py` mirrors it for the raw-segment replay path, and a mirror with
nothing checking it is how `_lane_of` came to exist twice and be wrong twice.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

from replay.lanes import LANE_RANK
from replay.legacy.order import OrderedTape
from replay.stream import MemoryByteStreamer

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
        # ingester/README.md (Orderings), transcribed independently of
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
    def test_tape_merges_equal_timestamps_by_lane_then_delivery(self):
        lanes = (
            "polymarket", "polymarket_snapshots", "polymarket_sports",
            "polymarket_rtds", "kalshi", "limitless",
        )
        objects = {}
        for lane in reversed(lanes):
            records = []
            for index in (1, 2):
                records.append(json.dumps({
                    "envelope_version": 2,
                    "delivery_index": index,
                    "record_id": f"{lane}:{index}",
                    "visible_ns": 100,
                    "monotonic_ns": 200 + index,
                    "venue": lane.split("_")[0],
                    "stream": "public_book",
                    "connection_epoch": "epoch",
                    "local_counter": index,
                    "source_cursor": None,
                    "kind": "venue_frame",
                    "raw_payload": "{}",
                }).encode() + b"\n")
            objects[f"lane={lane}/date=2026-01-01/segment.ndjson"] = b"".join(records)
        tape = OrderedTape(MemoryByteStreamer(objects))
        self.assertEqual(
            [(item.lane, item.envelope.delivery_index) for item in tape],
            [(lane, index) for lane in lanes for index in (1, 2)],
        )
        self.assertEqual(tape.delivery_breaks, [])


if __name__ == "__main__":
    unittest.main()
