//! The sealed-window finalizer: `docs/SEALED_CAPTURE_PIPELINE_V1.md` §5.
//!
//! `indexer-ingest` assigns positions in the order it reads files, which is
//! capture order *within* a lane and meaningless *across* lanes — whole files are
//! consumed atomically, so a record received between two records of another lane
//! is sequenced after both. `crates/cli/tests/ordering.rs` pins that as a
//! permanent property of that binary. This crate is the other order: for each
//! sealed UTC window, one k-way merge on `(visible_ns, lane_rank,
//! delivery_index)` producing canonical evidence whose line ordinal is the
//! spec's `EvidenceSeq`.
//!
//! Both orders stay available and both are honest about what they are. §5 keeps
//! the filename ordering as `file_order`; §8.3 puts this one behind its own
//! command. Neither is venue event order, and V1 does not claim to recover one.
//!
//! ```text
//! sealed segments ──> per-lane readers ──> k-way merge ──> tie groups ──> canonical evidence
//!      seals              schema check        visible_ns        §5             provenance
//!                                              lane_rank                       receipt
//!                                          delivery_index
//! ```

pub mod audit;
pub mod canonical;
pub mod continuity;
mod identity;
pub mod merge;
pub mod rank;
pub mod reader;
pub mod watermark;
pub mod window;

pub use audit::{AuditReport, audit_canonical_root};
pub use canonical::{
    CanonicalOutput, CanonicalOutputsState, CompressionContract, DecodedIdentity, FinalizedWindow,
    InputSegment, LaneFault, Receipt, RootLease, StoredIdentity, WindowOutcome,
    advance_delivery_continuity, canonical_outputs_state, committed_windows,
    create_dir_all_durable, delivery_continuity, finalize_window, late_segments, read_receipt,
    receipt_path, window_directory,
};
pub use continuity::{ClassifiedLine, LaneClocks, OrderingState, SeenEpochs, WrittenLine};
pub use merge::{LaneRecord, LaneStream, Merge, MergeFault, MergedRecord, SourceRef, TieGroups};
pub use rank::{
    LANE_RANKS, UNRANKED_LANE_RANK, is_known_lane, lane_rank, ranks_as_json, supported_lanes,
};
pub use reader::{LaneReader, SegmentClaims, SegmentInput, lane_of, segment_inputs};
pub use watermark::{Carried, ClockFault, Watermark, clock_faults};
pub use window::{
    Assembly, DeliveryContinuity, Eligibility, LaneDeliverySpan, LaneSegments, LaneStatus, Window,
    WindowKey, WindowStatus, assemble, date_partition, eligibility, ready_windows, segment_stamp,
    tile_absent_windows, validate_window, window_start_from_filename,
};

/// Merges lane streams and annotates cross-lane ties — the whole ordering
/// pipeline as one call.
///
/// Fallible because the lane set is validated up front: a repeated lane name is
/// rejected before a single record is read, rather than surfacing as a strange
/// ordering halfway through a window.
pub fn merged(lanes: Vec<(String, LaneStream<'_>)>) -> Result<TieGroups<Merge<'_>>, String> {
    Ok(TieGroups::new(Merge::new(lanes)?))
}
