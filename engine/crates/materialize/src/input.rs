//! Canonical-prefix scanner used only for excluded, receipt-pinned events.
//! This is not a JSON parser. Any layout/selection mismatch takes the existing
//! serde path; untouched body bytes rely on the stored-frame digest at EOF.
use std::ops::Range;

use replay_domain::{CanonicalProvenance, ContinuityVerdict, EventAddress, EventHeader, LaneId};

use crate::{ScopeFilter, Sha256};

pub(crate) enum ParsedHeader {
    Typed(EventHeader),
    Scanned(ScannedHeader),
}

impl ParsedHeader {
    pub(crate) fn sequence(&self) -> i64 {
        match self {
            Self::Typed(h) => h.address().canonical_seq(),
            Self::Scanned(h) => h.sequence,
        }
    }

    pub(crate) fn index(&self) -> u32 {
        match self {
            Self::Typed(h) => h.address().event_index(),
            Self::Scanned(h) => h.index,
        }
    }

    pub(crate) fn to_owned(&self, line: &[u8]) -> EventHeader {
        match self {
            Self::Typed(h) => h.clone(),
            Self::Scanned(h) => EventHeader::new(
                h.visible_ns,
                h.visible_ns,
                h.tie,
                EventAddress::new(
                    h.sequence,
                    LaneId::new(h.text(line, &h.lane)).unwrap(),
                    h.delivery,
                    h.index,
                )
                .unwrap(),
                h.text(line, &h.record_id),
                h.provenance.clone(),
            )
            .unwrap(),
        }
    }

    pub(crate) fn same_source(&self, source: &EventHeader, line: &[u8]) -> bool {
        match self {
            Self::Typed(h) => {
                source.order_ns() == h.order_ns()
                    && source.visible_ns() == h.visible_ns()
                    && source.visible_tie_group() == h.visible_tie_group()
                    && source.record_id() == h.record_id()
                    && source.provenance() == h.provenance()
                    && source.address().canonical_seq() == h.address().canonical_seq()
                    && source.address().lane() == h.address().lane()
                    && source.address().delivery_index() == h.address().delivery_index()
            }
            Self::Scanned(h) => {
                source.order_ns() == h.visible_ns
                    && source.visible_ns() == h.visible_ns
                    && source.visible_tie_group() == h.tie
                    && source.record_id() == h.text(line, &h.record_id)
                    && source.provenance() == &h.provenance
                    && source.address().canonical_seq() == h.sequence
                    && source.address().lane().as_str() == h.text(line, &h.lane)
                    && source.address().delivery_index() == h.delivery
            }
        }
    }
}

// Ranges borrow VerifiedLines' current buffer, which is not advanced until all
// comparisons and any retained header have been made. No per-child text copy.
pub(crate) struct ScannedHeader {
    visible_ns: u64,
    tie: Option<u64>,
    sequence: i64,
    lane: Range<usize>,
    delivery: u64,
    index: u32,
    record_id: Range<usize>,
    provenance: CanonicalProvenance,
}

impl ScannedHeader {
    fn text<'a>(&self, line: &'a [u8], range: &Range<usize>) -> &'a str {
        std::str::from_utf8(&line[range.clone()]).unwrap()
    }
}

pub(crate) fn excluded_header(line: &[u8], scope: &ScopeFilter) -> Option<(u16, ParsedHeader)> {
    let mut s = Scanner { line, offset: 0 };
    s.exact(b"{\"schema_version\":")?;
    let version = u16::try_from(s.number()?).ok()?;
    if !matches!(
        version,
        replay_domain::SEGMENT_SCHEMA_V3 | replay_domain::SEGMENT_SCHEMA_V4
    ) {
        return None;
    }
    s.exact(b",\"header\":{\"order_ns\":")?;
    let order = s.number()?;
    s.exact(b",\"visible_ns\":")?;
    let visible_ns = s.number()?;
    if order != visible_ns {
        return None;
    }
    s.exact(b",\"visible_tie_group\":")?;
    let tie = if s.remaining().starts_with(b"null") {
        s.exact(b"null")?;
        None
    } else {
        Some(s.number()?)
    };
    s.exact(b",\"address\":{\"canonical_seq\":")?;
    let sequence = i64::try_from(s.number()?).ok()?;
    if sequence == 0 {
        return None;
    }
    s.exact(b",\"lane\":")?;
    let lane = s.text()?;
    s.exact(b",\"delivery_index\":")?;
    let delivery = s.number()?;
    s.exact(b",\"event_index\":")?;
    let index = u32::try_from(s.number()?).ok()?;
    s.exact(b"},\"record_id\":")?;
    let record_id = s.text()?;
    s.exact(b",\"provenance\":{\"source_segment_sha256\":")?;
    let source_hash = s.hash()?;
    s.exact(b",\"source_line_number\":")?;
    let source_line = s.number()?;
    s.exact(b",\"content_hash\":")?;
    let content_hash = s.hash()?;
    s.exact(b",\"continuity\":")?;
    let continuity = s.text()?;
    let continuity = match &line[continuity] {
        b"lifecycle" => ContinuityVerdict::Lifecycle,
        b"bootstrap" => ContinuityVerdict::Bootstrap,
        b"unsequenced_venue" => ContinuityVerdict::UnsequencedVenue,
        b"sparse_monotonic" => ContinuityVerdict::SparseMonotonic,
        b"continuous" => ContinuityVerdict::Continuous,
        b"gap_proven" => ContinuityVerdict::GapProven,
        b"cursor_went_backwards" => ContinuityVerdict::CursorWentBackwards,
        b"local_counter_broken" => ContinuityVerdict::LocalCounterBroken,
        b"duplicate" => ContinuityVerdict::Duplicate,
        b"conflict" => ContinuityVerdict::Conflict,
        _ => return None,
    };
    let provenance =
        CanonicalProvenance::new(source_hash, source_line, content_hash, continuity).ok()?;
    s.exact(b"}},\"event\":")?;
    // Only locate the body and inspect its selection prefix, never bound it by
    // lexing. A missing canonical outer terminator takes the fallback.
    let body = s.remaining().strip_suffix(b"}")?;
    if !body.ends_with(b"}}") {
        return None;
    }
    let instrument = [
        b"{\"kind\":\"book\",\"value\":{\"kind\":\"full\",\"value\":{\"instrument\":".as_slice(),
        b"{\"kind\":\"book\",\"value\":{\"kind\":\"delta\",\"value\":{\"instrument\":".as_slice(),
        b"{\"kind\":\"trade\",\"value\":{\"instrument\":".as_slice(),
        b"{\"kind\":\"audit_anchor\",\"value\":{\"instrument\":".as_slice(),
    ]
    .into_iter()
    .find(|prefix| body.starts_with(prefix));
    let keep = if let Some(prefix) = instrument {
        s.offset += prefix.len();
        let instrument = s.text()?;
        s.exact(b",\"orientation\":")?;
        let id = std::str::from_utf8(&line[instrument]).ok()?;
        let (venue, native) = id.split_once(':')?;
        if venue.is_empty() || native.is_empty() {
            return None;
        }
        scope.instruments.contains(id)
    } else {
        let control = body.strip_prefix(b"{\"kind\":\"control\",\"value\":{\"kind\":\"")?;
        let kind_end = control.iter().position(|&b| b == b'"')?;
        if !matches!(
            &control[..kind_end],
            b"connection_opened"
                | b"connection_closed"
                | b"connection_failed"
                | b"subscription_changed"
                | b"metadata_changed"
        ) || !control[kind_end..].starts_with(b"\",\"value\":{")
        {
            return None;
        }
        let lane_text = std::str::from_utf8(&line[lane.clone()]).ok()?;
        scope.lanes.contains(lane_text)
    };
    if keep {
        return None; // Kept events retain the complete T4 typed-body path.
    }
    Some((
        version,
        ParsedHeader::Scanned(ScannedHeader {
            visible_ns,
            tie,
            sequence,
            lane,
            delivery,
            index,
            record_id,
            provenance,
        }),
    ))
}

struct Scanner<'a> {
    line: &'a [u8],
    offset: usize,
}

impl Scanner<'_> {
    fn remaining(&self) -> &[u8] {
        &self.line[self.offset..]
    }

    fn exact(&mut self, prefix: &[u8]) -> Option<()> {
        if !self.remaining().starts_with(prefix) {
            return None;
        }
        self.offset += prefix.len();
        Some(())
    }

    fn number(&mut self) -> Option<u64> {
        let start = self.offset;
        let mut value = 0u64;
        while let Some(&byte @ b'0'..=b'9') = self.line.get(self.offset) {
            value = value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
            self.offset += 1;
        }
        if self.offset == start || (self.offset > start + 1 && self.line[start] == b'0') {
            return None;
        }
        Some(value)
    }

    fn text(&mut self) -> Option<Range<usize>> {
        self.exact(b"\"")?;
        let start = self.offset;
        let end = start
            + self
                .remaining()
                .iter()
                .position(|&b| b == b'"' || b == b'\\' || b < 32)?;
        if self.line[end] != b'"' {
            return None;
        }
        let text = std::str::from_utf8(&self.line[start..end]).ok()?;
        if text.is_empty() || text.chars().any(char::is_control) {
            return None;
        }
        self.offset = end + 1;
        Some(start..end)
    }

    fn hash(&mut self) -> Option<Sha256> {
        self.exact(b"\"")?;
        let hex = self.line.get(self.offset..self.offset + 64)?;
        let hash = Sha256::from_hex(std::str::from_utf8(hex).ok()?).ok()?;
        self.offset += 64;
        self.exact(b"\"")?;
        Some(hash)
    }
}
