"""Which capture process wrote a file, and how the lanes rank against each other.

A lane is one splice process. It is not a venue: Polymarket runs four of them —
the market channel, the snapshot poller, the sports reference feed and the RTDS
price feed — and every record from all four carries `venue: polymarket` in its
envelope. The directory partition names the lane; the envelope names the venue.
Conflating them is what made `ClockScope.venue` wrong before it became
`ClockScope.lane`.

Two invariants depend on getting this right, and both are per-lane rather than
global: `delivery_index` is dense within one lane because each splice runs its
own counter, and the k-way merge needs exactly one iterator per lane.

This module exists because the resolver used to be duplicated byte-for-byte in
`replay/order.py` and `replay/gate1.py`. Neither imported the other, so when the
partition key changed both were wrong in the same way at the same time — which
is precisely the failure a second copy is for.
"""

from __future__ import annotations

__all__ = ["LANE_RANK", "PARTITION_PREFIXES", "LaneError", "lane_of", "lane_rank"]

#: Accepted hive partition spellings, in resolution order.
#:
#: `venue=` is the historical spelling, kept only so a pre-cutover tree still
#: resolves rather than raising. Writers emit `lane=`.
PARTITION_PREFIXES = ("lane=", "venue=")

#: Tie-break order for records sharing an exact `visible_ns`, from
#: `docs/SEALED_CAPTURE_PIPELINE_V1.md` §1. Lower wins.
#:
#: **This is a serialization rule, not evidence that one venue moved first.** The
#: canonical file needs a total order so its bytes and `EvidenceSeq` are
#: reproducible; analysis must treat records from different lanes with equal
#: `visible_ns` as a capture-time tie and must not read lead-lag out of the order
#: this table imposes.
LANE_RANK = {
    "polymarket": 0,
    "polymarket_snapshots": 1,
    "polymarket_sports": 2,
    "polymarket_rtds": 3,
    "kalshi": 10,
    "limitless": 20,
}

#: Where an unranked lane sorts. Above every known lane, so a lane added to the
#: capture side before this table is updated still merges deterministically
#: rather than colliding with `polymarket` at rank 0.
UNRANKED_LANE_RANK = 1_000


class LaneError(ValueError):
    """An object key that names no lane."""


def lane_of(object_key: str) -> str:
    """The lane that wrote `object_key`, from its hive partition.

    Raises rather than guessing. The previous implementation fell back to the
    parent directory when no partition matched, which never failed and therefore
    never surfaced a layout change — it just answered wrongly. Under a `lane=`
    tree it returned `spool/lane=polymarket/date=2026-07-30`, a *per-date* lane,
    so one lane spanning midnight became two lanes with no continuity checked
    between them. A `delivery_index` gap across that boundary was invisible: the
    first record after midnight had nothing to be compared against.

    Every key that reaches here comes from a partitioned spool tree, so the
    fallback was only ever reachable on malformed input — where an exception is
    the answer that gets noticed.
    """
    for part in object_key.split("/"):
        for prefix in PARTITION_PREFIXES:
            if part.startswith(prefix):
                lane = part.removeprefix(prefix)
                if not lane:
                    raise LaneError(f"empty lane partition in {object_key!r}")
                return lane
    raise LaneError(
        f"no lane partition in {object_key!r}; "
        f"expected a path segment starting with {' or '.join(PARTITION_PREFIXES)}"
    )


def lane_rank(lane: str) -> int:
    """Tie-break rank for `lane`. See `LANE_RANK` for what this may not be used for."""
    return LANE_RANK.get(lane, UNRANKED_LANE_RANK)
