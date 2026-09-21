//! Profile-2 transport and receipt evidence. No venue-role or book inference.
use std::collections::{BTreeMap, BTreeSet};

use replay_domain::{EventHeader, LaneId, Sha256};
use serde::{Deserialize, Serialize};

use crate::SourceReceipt;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceEvidence {
    pub source_version: u16,
    pub header: EventHeader,
    pub connection_epoch: String,
}

impl SourceEvidence {
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let value: Self = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
        if value.source_version != 1
            || value.connection_epoch.is_empty()
            || !value
                .connection_epoch
                .bytes()
                .all(|b| b.is_ascii() && !b.is_ascii_control() && b != b'"' && b != b'\\')
            || value.header.address().event_index() != 0
            || serde_json::to_vec(&value).map_err(|e| e.to_string())? != bytes
        {
            return Err("invalid source evidence record".into());
        }
        value.header.validate().map_err(|e| e.to_string())?;
        Ok(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaneState {
    /// Neither expected nor observed in the upstream receipt.
    NotExpected,
    Present {
        records: u64,
    },
    Missing,
    Invalid {
        detail: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaneCoverage {
    pub expected: bool,
    pub state: LaneState,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceFaultReason {
    LaneMissing,
    LaneInvalid {
        detail: Option<String>,
    },
    VisibleClockRegression {
        previous_visible_ns: u64,
        observed_visible_ns: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceFault {
    pub lane: LaneId,
    pub start_ns: u64,
    pub end_ns: u64,
    pub reason: SourceFaultReason,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverageEvidence {
    lanes: BTreeMap<LaneId, LaneCoverage>,
    faults: Vec<SourceFault>,
    deadline_seconds: u64,
    deadline_expired: bool,
    records: u64,
    sequence: (Option<i64>, Option<i64>),
}

impl CoverageEvidence {
    pub fn lanes(&self) -> &BTreeMap<LaneId, LaneCoverage> {
        &self.lanes
    }
    pub fn lane(&self, lane: &LaneId) -> LaneCoverage {
        self.lanes.get(lane).cloned().unwrap_or(LaneCoverage {
            expected: false,
            state: LaneState::NotExpected,
        })
    }
    pub fn faults(&self) -> &[SourceFault] {
        &self.faults
    }
    pub fn deadline_seconds(&self) -> u64 {
        self.deadline_seconds
    }
    pub fn deadline_expired(&self) -> bool {
        self.deadline_expired
    }
    pub(crate) fn records(&self) -> u64 {
        self.records
    }
    pub(crate) fn sequence(&self) -> (Option<i64>, Option<i64>) {
        self.sequence
    }

    pub(crate) fn from_source(source: &SourceReceipt) -> Result<Self, String> {
        let document = source
            .document
            .as_ref()
            .ok_or("source coverage not recorded")?;
        if document.len() as u64 != source.byte_length
            || Sha256::digest(document.as_bytes()) != source.sha256
        {
            return Err("source receipt document identity mismatch".into());
        }
        let receipt: indexer_finalize::Receipt =
            serde_json::from_str(document).map_err(|e| e.to_string())?;
        // Canonical receipts historically defaulted an absent clock_faults field.
        // Profile 2 promises recorded evidence, not that compatibility default.
        #[derive(Deserialize)]
        struct RecordedClockEvidence {
            clock_faults: Vec<indexer_finalize::ClockFault>,
        }
        let clocks: RecordedClockEvidence =
            serde_json::from_str(document).map_err(|e| e.to_string())?;
        debug_assert_eq!(clocks.clock_faults, receipt.clock_faults);
        if receipt.receipt_version != 1
            || receipt.finalizer_version != 1
            || receipt.window_start_ns != source.window_start_ns
            || receipt.window_end_ns != source.window_end_ns
            || receipt.certified != source.certified
        {
            return Err("source receipt document disagrees with identity".into());
        }
        let names = |values: &[String]| -> Result<BTreeSet<LaneId>, String> {
            let set: BTreeSet<_> = values
                .iter()
                .map(|v| LaneId::new(v.clone()).map_err(|e| e.to_string()))
                .collect::<Result<_, _>>()?;
            if set.len() != values.len() {
                return Err("duplicate coverage lane".into());
            }
            Ok(set)
        };
        let expected = names(&receipt.expected_lanes)?;
        let present = names(&receipt.present_lanes)?;
        let unexpected = names(&receipt.unexpected_lanes)?;
        let mut lanes = BTreeMap::new();
        for lane in &present {
            lanes.insert(
                lane.clone(),
                LaneCoverage {
                    expected: expected.contains(lane),
                    state: LaneState::Present { records: 0 },
                },
            );
        }
        let mut faults = Vec::new();
        for (entries, missing) in [
            (&receipt.missing_lanes, true),
            (&receipt.invalid_lanes, false),
        ] {
            for entry in entries {
                let lane = LaneId::new(entry.lane.clone()).map_err(|e| e.to_string())?;
                if entry.reason
                    != if missing {
                        "lane_missing"
                    } else {
                        "lane_invalid"
                    }
                    || (missing && (!expected.contains(&lane) || entry.detail.is_some()))
                {
                    return Err("invalid upstream lane diagnosis".into());
                }
                let state = if missing {
                    LaneState::Missing
                } else {
                    LaneState::Invalid {
                        detail: entry.detail.clone(),
                    }
                };
                if lanes
                    .insert(
                        lane.clone(),
                        LaneCoverage {
                            expected: expected.contains(&lane),
                            state,
                        },
                    )
                    .is_some()
                {
                    return Err("overlapping coverage lane states".into());
                }
                faults.push(SourceFault {
                    lane,
                    start_ns: source.window_start_ns,
                    end_ns: source.window_end_ns,
                    reason: if missing {
                        SourceFaultReason::LaneMissing
                    } else {
                        SourceFaultReason::LaneInvalid {
                            detail: entry.detail.clone(),
                        }
                    },
                });
            }
        }
        if expected.iter().any(|lane| !lanes.contains_key(lane))
            || unexpected
                != lanes
                    .keys()
                    .filter(|lane| !expected.contains(*lane))
                    .cloned()
                    .collect()
        {
            return Err("source coverage inventory is inconsistent".into());
        }
        let complete = lanes
            .values()
            .all(|lane| !lane.expected || matches!(lane.state, LaneState::Present { .. }));
        if receipt.completeness != if complete { "complete" } else { "incomplete" }
            || receipt.certified != (complete && receipt.clock_faults.is_empty())
        {
            return Err("source certification disagrees with coverage".into());
        }
        let mut clock_lanes = BTreeSet::new();
        for clock in &receipt.clock_faults {
            let lane = LaneId::new(clock.lane.clone()).map_err(|e| e.to_string())?;
            if !present.contains(&lane)
                || !clock_lanes.insert(lane.clone())
                || clock.window_start_ns != source.window_start_ns
                || clock.observed_visible_ns >= clock.previous_visible_ns
                || clock.observed_visible_ns < source.window_start_ns
                || clock.observed_visible_ns >= source.window_end_ns
            {
                return Err("invalid upstream clock diagnosis".into());
            }
            faults.push(SourceFault {
                lane,
                start_ns: source.window_start_ns,
                end_ns: source.window_end_ns,
                reason: SourceFaultReason::VisibleClockRegression {
                    previous_visible_ns: clock.previous_visible_ns,
                    observed_visible_ns: clock.observed_visible_ns,
                },
            });
        }
        let mut input_lanes = BTreeSet::new();
        let mut input_ids = BTreeSet::new();
        let mut records = 0u64;
        for input in &receipt.inputs {
            let lane = LaneId::new(input.lane.clone()).map_err(|e| e.to_string())?;
            input_lanes.insert(lane.clone());
            if !input_ids.insert((lane.clone(), input.segment_index)) {
                return Err("duplicate coverage input segment".into());
            }
            let Some(LaneCoverage {
                state: LaneState::Present { records: count },
                ..
            }) = lanes.get_mut(&lane)
            else {
                return Err("coverage input belongs to excluded lane".into());
            };
            let span = match (input.first_delivery_index, input.last_delivery_index) {
                (None, None) if input.line_count == 0 => 0,
                (Some(first), Some(last)) => last
                    .checked_sub(first)
                    .and_then(|n| n.checked_add(1))
                    .ok_or("invalid coverage input range")?,
                _ => return Err("invalid coverage input range".into()),
            };
            if span != input.line_count {
                return Err("coverage input count mismatch".into());
            }
            *count = count
                .checked_add(span)
                .ok_or("coverage input count overflow")?;
            records = records
                .checked_add(span)
                .ok_or("coverage input count overflow")?;
        }
        if input_lanes != present
            || records != receipt.evidence.decoded.line_count
            || records != receipt.provenance.decoded.line_count
        {
            return Err("coverage input totals disagree".into());
        }
        match (receipt.first_canonical_seq, receipt.last_canonical_seq) {
            (None, None) if records == 0 => {}
            (Some(first), Some(last))
                if first > 0 && last >= first && (last - first + 1) as u64 == records => {}
            _ => return Err("coverage sequence bounds disagree".into()),
        }
        faults.sort_by(|a, b| a.lane.cmp(&b.lane));
        Ok(Self {
            lanes,
            faults,
            deadline_seconds: receipt.finalization_deadline_seconds,
            deadline_expired: receipt.deadline_expired,
            records,
            sequence: (receipt.first_canonical_seq, receipt.last_canonical_seq),
        })
    }
}
