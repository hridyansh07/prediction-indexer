//! Lane ranks: how a timestamp tie is broken, for every lane this build supports.
//!
//! **This is not the expected-lane set, and conflating the two is a defect.**
//! The distinction is easy to miss and expensive to get wrong:
//!
//! | | |
//! |---|---|
//! | *supported* | every lane this build knows how to rank — a property of the code |
//! | *expected* | the lanes a particular deployment actually runs — a property of the manifest |
//!
//! §3 settles it in one sentence: "The deployment manifest defines which lanes
//! are expected; a disabled Kalshi profile is not waited on." `compose.yaml`
//! makes that concrete — `splice-kalshi`, `splice-polymarket-sports` and
//! `splice-polymarket-rtds` sit behind opt-in profiles, so the default
//! deployment runs three of the six lanes below. Treating this table as the
//! expectation would mark every window of that deployment `incomplete` with
//! three phantom `lane_missing` entries, which makes the completeness verdict
//! worse than useless: it would report an outage that is a configuration choice,
//! and a reader who learned to ignore it would also ignore a real one.
//!
//! Ranking must still cover a lane that is not expected, because a lane can be
//! *present without being expected* — a profile enabled for one run, or a
//! segment left behind by an earlier configuration. Rank answers "where does
//! this record sort"; the manifest answers "should I wait for this lane".
//!
//! ## What rank is not
//!
//! From §1, and it governs how downstream analysis may read the canonical file:
//!
//! > Lane rank is a serialization rule, not evidence that one venue moved first.
//! > The canonical file needs a total order so that its bytes and `EvidenceSeq`
//! > are reproducible, but analysis must treat records from different lanes with
//! > equal `visible_ns` as a capture-time tie. It must not derive positive or
//! > negative lead-lag from their rank-imposed order.
//!
//! Rank decides a tie and nothing else. It never moves a Polymarket record ahead
//! of a Kalshi record with an earlier timestamp.
//!
//! ## Mirrored in Python
//!
//! `replay/lanes.py::LANE_RANK` carries the same table for the raw-segment replay
//! path. This is the authority; that is the mirror, and
//! `replay/tests/test_lane_rank.py` parses the literal below and fails if
//! the two drift. Keep the `("<lane>", <rank>),` shape — that test reads it.

/// Lane ranks from §1. Lower wins a tie.
pub const LANE_RANKS: &[(&str, u32)] = &[
    ("polymarket", 0),
    ("polymarket_snapshots", 1),
    ("polymarket_sports", 2),
    ("polymarket_rtds", 3),
    ("kalshi", 10),
    ("limitless", 20),
];

/// Where a lane absent from the table sorts.
///
/// Above every known lane, so a lane added to capture before this table is
/// updated still merges rather than colliding with `polymarket` at rank 0 and
/// having the tie fall to `delivery_index` — two unrelated lanes' counters,
/// compared as if they meant something together.
///
/// Note that *several* unranked lanes all land here and so tie with each other.
/// That is why the merge key's final term is the lane **name** and not the
/// lane's position in the input: with two unranked lanes, a shared rank plus a
/// shared instant and index would otherwise leave discovery order deciding the
/// canonical bytes. See `merge::Pending::key`.
pub const UNRANKED_LANE_RANK: u32 = 1_000;

/// Tie-break rank for a lane. See the module docs for what this may not be used for.
pub fn lane_rank(lane: &str) -> u32 {
    LANE_RANKS
        .iter()
        .find(|(name, _)| *name == lane)
        .map(|(_, rank)| *rank)
        .unwrap_or(UNRANKED_LANE_RANK)
}

/// Whether the registry knows this lane.
pub fn is_known_lane(lane: &str) -> bool {
    LANE_RANKS.iter().any(|(name, _)| *name == lane)
}

/// Every lane this build can rank, in rank order.
///
/// Supported, **not expected**. A complete window contains the lanes the
/// deployment manifest declares, which is a subset chosen by configuration; see
/// the module docs. This exists for diagnostics and for validating a declared
/// expectation against something, not for deciding completeness.
pub fn supported_lanes() -> Vec<&'static str> {
    let mut lanes: Vec<(&'static str, u32)> = LANE_RANKS.to_vec();
    lanes.sort_by_key(|(name, rank)| (*rank, *name));
    lanes.into_iter().map(|(name, _)| name).collect()
}

/// The table as JSON, for the binary's `--print-lane-ranks`.
///
/// The Python parity test reads the literal above directly rather than calling
/// this, so drift is caught without a Rust toolchain or a build step.
pub fn ranks_as_json() -> String {
    let body: Vec<String> = LANE_RANKS
        .iter()
        .map(|(name, rank)| {
            format!(
                "  {}: {rank}",
                serde_json::to_string(name).expect("lane name")
            )
        })
        .collect();
    format!("{{\n{}\n}}", body.join(",\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranks_are_unique_and_ordered_as_the_spec_states() {
        // A duplicate rank would make two lanes tie with each other and push the
        // decision onto `delivery_index`, comparing counters from independent
        // splices as if they were one sequence.
        let mut ranks: Vec<u32> = LANE_RANKS.iter().map(|(_, rank)| *rank).collect();
        let count = ranks.len();
        ranks.sort_unstable();
        ranks.dedup();
        assert_eq!(ranks.len(), count, "lane ranks must be distinct");

        assert_eq!(
            supported_lanes(),
            vec![
                "polymarket",
                "polymarket_snapshots",
                "polymarket_sports",
                "polymarket_rtds",
                "kalshi",
                "limitless",
            ],
            "§1's table, in rank order"
        );
    }

    #[test]
    fn an_unknown_lane_sorts_above_every_known_one() {
        let highest = LANE_RANKS
            .iter()
            .map(|(_, rank)| *rank)
            .max()
            .expect("non-empty");
        assert!(lane_rank("binance") > highest);
        assert!(!is_known_lane("binance"));
    }

    #[test]
    fn polymarket_wins_a_tie_against_both_other_venues() {
        // §8's required case, stated as the comparison the merge actually makes.
        assert!(lane_rank("polymarket") < lane_rank("kalshi"));
        assert!(lane_rank("kalshi") < lane_rank("limitless"));
    }
}
