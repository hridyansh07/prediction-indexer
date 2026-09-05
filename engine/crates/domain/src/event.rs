use core::fmt;

use serde::{Deserialize, Serialize};

use crate::{Px, Qty, SEGMENT_SCHEMA_VERSION};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainError {
    Empty(&'static str),
    InvalidInstrumentId,
    InvalidDigest,
    InvalidCanonicalSequence,
    InvalidSourceLine,
    OrderClockMismatch,
    NonPositiveLevel,
    NegativeAbsoluteLevel,
    ZeroRelativeLevel,
    ScaleMismatch,
    DuplicatePrice,
    NonCanonicalLevelOrder,
    UnsupportedSchemaVersion(u16),
    NonCanonicalEncoding,
    Json(String),
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty(field) => write!(formatter, "{field} cannot be empty"),
            Self::InvalidInstrumentId => {
                formatter.write_str("instrument_id must be venue-qualified")
            }
            Self::InvalidDigest => formatter.write_str("digest must be 64 lowercase hex bytes"),
            Self::InvalidCanonicalSequence => formatter.write_str("canonical_seq must be positive"),
            Self::InvalidSourceLine => formatter.write_str("source_line_number must be positive"),
            Self::OrderClockMismatch => {
                formatter.write_str("order_ns must equal visible_ns in schema v1")
            }
            Self::NonPositiveLevel => formatter.write_str("full-book quantities must be positive"),
            Self::NegativeAbsoluteLevel => {
                formatter.write_str("absolute level quantity cannot be negative")
            }
            Self::ZeroRelativeLevel => {
                formatter.write_str("relative level quantity cannot be zero")
            }
            Self::ScaleMismatch => {
                formatter.write_str("book levels must use one price and quantity scale")
            }
            Self::DuplicatePrice => formatter.write_str("book side contains a duplicate price"),
            Self::NonCanonicalLevelOrder => {
                formatter.write_str("book levels are not in canonical side order")
            }
            Self::UnsupportedSchemaVersion(version) => {
                write!(formatter, "unsupported segment schema version {version}")
            }
            Self::NonCanonicalEncoding => formatter.write_str("JSON is valid but not canonical"),
            Self::Json(error) => write!(formatter, "invalid segment JSON: {error}"),
        }
    }
}

impl std::error::Error for DomainError {}

fn validate_text(value: &str, field: &'static str) -> Result<(), DomainError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        Err(DomainError::Empty(field))
    } else {
        Ok(())
    }
}

macro_rules! text_id {
    ($name:ident, $field:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                validate_text(&value, $field)?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
            }
        }
    };
}

text_id!(LaneId, "lane");

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct InstrumentId(String);

impl InstrumentId {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        validate_text(&value, "instrument_id")?;
        let Some((venue, native)) = value.split_once(':') else {
            return Err(DomainError::InvalidInstrumentId);
        };
        if venue.is_empty() || native.is_empty() {
            return Err(DomainError::InvalidInstrumentId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for InstrumentId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
struct Sha256Digest(String);

impl Sha256Digest {
    fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DomainError::InvalidDigest);
        }
        Ok(Self(value))
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuityVerdict {
    Lifecycle,
    Bootstrap,
    UnsequencedVenue,
    SparseMonotonic,
    Continuous,
    GapProven,
    CursorWentBackwards,
    LocalCounterBroken,
    Duplicate,
    Conflict,
}

/// Replay-owned downstream provenance. The future Phase-0 adapter converts from
/// `indexer_finalize::CanonicalProvenance`; no finalizer receipt type is copied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalProvenance {
    source_segment_sha256: Sha256Digest,
    source_line_number: u64,
    content_hash: Sha256Digest,
    continuity: ContinuityVerdict,
}

impl CanonicalProvenance {
    pub fn new(
        source_segment_sha256: impl Into<String>,
        source_line_number: u64,
        content_hash: impl Into<String>,
        continuity: ContinuityVerdict,
    ) -> Result<Self, DomainError> {
        if source_line_number == 0 {
            return Err(DomainError::InvalidSourceLine);
        }
        Ok(Self {
            source_segment_sha256: Sha256Digest::new(source_segment_sha256)?,
            source_line_number,
            content_hash: Sha256Digest::new(content_hash)?,
            continuity,
        })
    }

    pub fn source_segment_sha256(&self) -> &str {
        self.source_segment_sha256.as_str()
    }

    pub const fn source_line_number(&self) -> u64 {
        self.source_line_number
    }

    pub fn content_hash(&self) -> &str {
        self.content_hash.as_str()
    }

    pub const fn continuity(&self) -> ContinuityVerdict {
        self.continuity
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.source_line_number == 0 {
            Err(DomainError::InvalidSourceLine)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventAddress {
    canonical_seq: i64,
    lane: LaneId,
    delivery_index: u64,
    event_index: u32,
}

impl EventAddress {
    pub fn new(
        canonical_seq: i64,
        lane: LaneId,
        delivery_index: u64,
        event_index: u32,
    ) -> Result<Self, DomainError> {
        if canonical_seq <= 0 {
            return Err(DomainError::InvalidCanonicalSequence);
        }
        Ok(Self {
            canonical_seq,
            lane,
            delivery_index,
            event_index,
        })
    }

    /// Derives the deterministic zero-based address of a normalized child while
    /// retaining the canonical delivery address exactly.
    pub fn child(&self, event_index: u32) -> Self {
        Self {
            event_index,
            ..self.clone()
        }
    }

    pub const fn canonical_seq(&self) -> i64 {
        self.canonical_seq
    }

    pub const fn delivery_index(&self) -> u64 {
        self.delivery_index
    }

    pub const fn event_index(&self) -> u32 {
        self.event_index
    }

    pub fn lane(&self) -> &LaneId {
        &self.lane
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.canonical_seq <= 0 {
            Err(DomainError::InvalidCanonicalSequence)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventHeader {
    order_ns: u64,
    visible_ns: u64,
    visible_tie_group: Option<u64>,
    address: EventAddress,
    record_id: String,
    provenance: CanonicalProvenance,
}

impl EventHeader {
    pub fn new(
        order_ns: u64,
        visible_ns: u64,
        visible_tie_group: Option<u64>,
        address: EventAddress,
        record_id: impl Into<String>,
        provenance: CanonicalProvenance,
    ) -> Result<Self, DomainError> {
        if order_ns != visible_ns {
            return Err(DomainError::OrderClockMismatch);
        }
        let record_id = record_id.into();
        validate_text(&record_id, "record_id")?;
        Ok(Self {
            order_ns,
            visible_ns,
            visible_tie_group,
            address,
            record_id,
            provenance,
        })
    }

    pub const fn order_ns(&self) -> u64 {
        self.order_ns
    }

    pub const fn visible_ns(&self) -> u64 {
        self.visible_ns
    }

    pub const fn visible_tie_group(&self) -> Option<u64> {
        self.visible_tie_group
    }

    pub fn address(&self) -> &EventAddress {
        &self.address
    }

    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    pub fn provenance(&self) -> &CanonicalProvenance {
        &self.provenance
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.order_ns != self.visible_ns {
            return Err(DomainError::OrderClockMismatch);
        }
        validate_text(&self.record_id, "record_id")?;
        self.address.validate()?;
        self.provenance.validate()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Bid,
    Ask,
}

/// Whether prices refer to the named outcome or its logical complement.
/// Conversion between orientations is never implicit at this boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractOrientation {
    Outcome,
    Complement,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Level {
    price: Px,
    quantity: Qty,
}

impl Level {
    pub fn new(price: Px, quantity: Qty) -> Result<Self, DomainError> {
        if quantity.atoms() <= 0 {
            return Err(DomainError::NonPositiveLevel);
        }
        Ok(Self { price, quantity })
    }

    pub const fn price(self) -> Px {
        self.price
    }

    pub const fn quantity(self) -> Qty {
        self.quantity
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.quantity.atoms() <= 0 {
            Err(DomainError::NonPositiveLevel)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullBook {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    bids: Vec<Level>,
    asks: Vec<Level>,
    snapshot_hash: Option<String>,
    source_observed_ns: Option<u64>,
}

impl FullBook {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        snapshot_hash: Option<String>,
        source_observed_ns: Option<u64>,
    ) -> Result<Self, DomainError> {
        validate_optional_text(&snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&bids, &asks)?;
        bids.sort_by_key(|level| core::cmp::Reverse(level.price.atoms()));
        asks.sort_by_key(|level| level.price.atoms());
        reject_duplicate_prices(&bids)?;
        reject_duplicate_prices(&asks)?;
        Ok(Self {
            instrument,
            orientation,
            bids,
            asks,
            snapshot_hash,
            source_observed_ns,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    pub fn snapshot_hash(&self) -> Option<&str> {
        self.snapshot_hash.as_deref()
    }

    pub const fn source_observed_ns(&self) -> Option<u64> {
        self.source_observed_ns
    }

    fn validate(&self) -> Result<(), DomainError> {
        validate_optional_text(&self.snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&self.bids, &self.asks)?;
        validate_order(&self.bids, Side::Bid)?;
        validate_order(&self.asks, Side::Ask)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LevelSizeMode {
    Absolute,
    Relative,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LevelSize {
    mode: LevelSizeMode,
    quantity: Qty,
}

impl LevelSize {
    pub fn absolute(quantity: Qty) -> Result<Self, DomainError> {
        if quantity.atoms() < 0 {
            Err(DomainError::NegativeAbsoluteLevel)
        } else {
            Ok(Self {
                mode: LevelSizeMode::Absolute,
                quantity,
            })
        }
    }

    pub fn relative(quantity: Qty) -> Result<Self, DomainError> {
        if quantity.atoms() == 0 {
            Err(DomainError::ZeroRelativeLevel)
        } else {
            Ok(Self {
                mode: LevelSizeMode::Relative,
                quantity,
            })
        }
    }

    pub const fn mode(self) -> LevelSizeMode {
        self.mode
    }

    pub const fn quantity(self) -> Qty {
        self.quantity
    }

    fn validate(self) -> Result<(), DomainError> {
        match self.mode {
            LevelSizeMode::Absolute if self.quantity.atoms() < 0 => {
                Err(DomainError::NegativeAbsoluteLevel)
            }
            LevelSizeMode::Relative if self.quantity.atoms() == 0 => {
                Err(DomainError::ZeroRelativeLevel)
            }
            _ => Ok(()),
        }
    }
}

impl<'de> Deserialize<'de> for LevelSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            mode: LevelSizeMode,
            quantity: Qty,
        }

        let wire = Wire::deserialize(deserializer)?;
        match wire.mode {
            LevelSizeMode::Absolute => Self::absolute(wire.quantity),
            LevelSizeMode::Relative => Self::relative(wire.quantity),
        }
        .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookDelta {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    side: Side,
    price: Px,
    size: LevelSize,
    book_hash: Option<String>,
}

impl BookDelta {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        side: Side,
        price: Px,
        size: LevelSize,
        book_hash: Option<String>,
    ) -> Result<Self, DomainError> {
        size.validate()?;
        validate_optional_text(&book_hash, "book_hash")?;
        Ok(Self {
            instrument,
            orientation,
            side,
            price,
            size,
            book_hash,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub const fn side(&self) -> Side {
        self.side
    }

    pub const fn price(&self) -> Px {
        self.price
    }

    pub const fn size(&self) -> LevelSize {
        self.size
    }

    fn validate(&self) -> Result<(), DomainError> {
        self.size.validate()?;
        validate_optional_text(&self.book_hash, "book_hash")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum BookEvent {
    Full(FullBook),
    Delta(BookDelta),
}

impl BookEvent {
    fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Full(book) => book.validate(),
            Self::Delta(delta) => delta.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditAnchor {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    bids: Vec<Level>,
    asks: Vec<Level>,
    snapshot_hash: String,
    source_observed_ns: Option<u64>,
}

impl AuditAnchor {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        snapshot_hash: impl Into<String>,
        source_observed_ns: Option<u64>,
    ) -> Result<Self, DomainError> {
        let snapshot_hash = snapshot_hash.into();
        validate_text(&snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&bids, &asks)?;
        bids.sort_by_key(|level| core::cmp::Reverse(level.price.atoms()));
        asks.sort_by_key(|level| level.price.atoms());
        reject_duplicate_prices(&bids)?;
        reject_duplicate_prices(&asks)?;
        Ok(Self {
            instrument,
            orientation,
            bids,
            asks,
            snapshot_hash,
            source_observed_ns,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub fn bids(&self) -> &[Level] {
        &self.bids
    }

    pub fn asks(&self) -> &[Level] {
        &self.asks
    }

    pub fn snapshot_hash(&self) -> &str {
        &self.snapshot_hash
    }

    pub const fn source_observed_ns(&self) -> Option<u64> {
        self.source_observed_ns
    }

    fn validate(&self) -> Result<(), DomainError> {
        validate_text(&self.snapshot_hash, "snapshot_hash")?;
        validate_level_scales(&self.bids, &self.asks)?;
        validate_order(&self.bids, Side::Bid)?;
        validate_order(&self.asks, Side::Ask)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum FaultImpact {
    Instrument(InstrumentId),
    RequestedVenueBooks(String),
    AuditCoverageOnly(String),
    UnattributedLane(LaneId),
}

impl FaultImpact {
    fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::RequestedVenueBooks(venue) | Self::AuditCoverageOnly(venue) => {
                validate_text(venue, "venue")
            }
            Self::Instrument(_) | Self::UnattributedLane(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizationFault {
    reject_id: String,
    impact: FaultImpact,
}

impl NormalizationFault {
    pub fn new(reject_id: impl Into<String>, impact: FaultImpact) -> Result<Self, DomainError> {
        let reject_id = reject_id.into();
        validate_text(&reject_id, "reject_id")?;
        impact.validate()?;
        Ok(Self { reject_id, impact })
    }

    pub fn reject_id(&self) -> &str {
        &self.reject_id
    }

    pub fn impact(&self) -> &FaultImpact {
        &self.impact
    }

    fn validate(&self) -> Result<(), DomainError> {
        validate_text(&self.reject_id, "reject_id")?;
        self.impact.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ControlEvent {
    ConnectionOpened {
        epoch: String,
        instruments: Vec<InstrumentId>,
        delivers_deltas: bool,
        target_digest: Option<String>,
    },
    ConnectionClosed {
        epoch: String,
    },
    ConnectionFailed {
        epoch: String,
        reason: String,
    },
    SubscriptionChanged {
        from: Option<String>,
        to: String,
    },
    MetadataChanged {
        from: Option<String>,
        to: String,
    },
}

impl ControlEvent {
    fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::ConnectionOpened {
                epoch,
                target_digest,
                ..
            } => {
                validate_text(epoch, "epoch")?;
                validate_optional_text(target_digest, "target_digest")
            }
            Self::ConnectionClosed { epoch } => validate_text(epoch, "epoch"),
            Self::ConnectionFailed { epoch, reason } => {
                validate_text(epoch, "epoch")?;
                validate_text(reason, "reason")
            }
            Self::SubscriptionChanged { from, to } | Self::MetadataChanged { from, to } => {
                validate_optional_text(from, "from")?;
                validate_text(to, "to")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeEvent {
    instrument: InstrumentId,
    orientation: ContractOrientation,
    price: Px,
    quantity: Qty,
    aggressor: Option<Side>,
}

impl TradeEvent {
    pub fn new(
        instrument: InstrumentId,
        orientation: ContractOrientation,
        price: Px,
        quantity: Qty,
        aggressor: Option<Side>,
    ) -> Result<Self, DomainError> {
        if quantity.atoms() <= 0 {
            return Err(DomainError::NonPositiveLevel);
        }
        Ok(Self {
            instrument,
            orientation,
            price,
            quantity,
            aggressor,
        })
    }

    pub fn instrument(&self) -> &InstrumentId {
        &self.instrument
    }

    pub const fn orientation(&self) -> ContractOrientation {
        self.orientation
    }

    pub const fn price(&self) -> Px {
        self.price
    }

    pub const fn quantity(&self) -> Qty {
        self.quantity
    }

    pub const fn aggressor(&self) -> Option<Side> {
        self.aggressor
    }

    fn validate(&self) -> Result<(), DomainError> {
        if self.quantity.atoms() <= 0 {
            Err(DomainError::NonPositiveLevel)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SegmentEvent {
    Control(ControlEvent),
    Book(BookEvent),
    AuditAnchor(AuditAnchor),
    NormalizationFault(NormalizationFault),
    Trade(TradeEvent),
}

impl SegmentEvent {
    fn validate(&self) -> Result<(), DomainError> {
        match self {
            Self::Control(control) => control.validate(),
            Self::Book(book) => book.validate(),
            Self::AuditAnchor(anchor) => anchor.validate(),
            Self::NormalizationFault(fault) => fault.validate(),
            Self::Trade(trade) => trade.validate(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentRecord {
    schema_version: u16,
    header: EventHeader,
    event: SegmentEvent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentRecordWire {
    schema_version: u16,
    header: EventHeader,
    event: SegmentEvent,
}

impl SegmentRecord {
    pub fn new(header: EventHeader, event: SegmentEvent) -> Result<Self, DomainError> {
        header.validate()?;
        event.validate()?;
        Ok(Self {
            schema_version: SEGMENT_SCHEMA_VERSION,
            header,
            event,
        })
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub fn header(&self) -> &EventHeader {
        &self.header
    }

    pub fn event(&self) -> &SegmentEvent {
        &self.event
    }

    /// Compact UTF-8 JSON with stable struct-field and variant ordering.
    pub fn to_canonical_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("validated Replay domain values always serialize")
    }

    /// Decodes only the exact canonical representation. This rejects unknown
    /// fields/variants, unsupported versions, invalid states, alternate field
    /// order, and insignificant whitespace.
    pub fn from_canonical_json(bytes: &[u8]) -> Result<Self, DomainError> {
        let wire: SegmentRecordWire =
            serde_json::from_slice(bytes).map_err(|error| DomainError::Json(error.to_string()))?;
        let decoded = Self::from_wire(wire)?;
        if decoded.to_canonical_json() != bytes {
            return Err(DomainError::NonCanonicalEncoding);
        }
        Ok(decoded)
    }

    fn from_wire(wire: SegmentRecordWire) -> Result<Self, DomainError> {
        if wire.schema_version != SEGMENT_SCHEMA_VERSION {
            return Err(DomainError::UnsupportedSchemaVersion(wire.schema_version));
        }
        wire.header.validate()?;
        wire.event.validate()?;
        Ok(Self {
            schema_version: wire.schema_version,
            header: wire.header,
            event: wire.event,
        })
    }
}

impl<'de> Deserialize<'de> for SegmentRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Self::from_wire(SegmentRecordWire::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}

fn validate_optional_text(value: &Option<String>, field: &'static str) -> Result<(), DomainError> {
    match value {
        Some(value) => validate_text(value, field),
        None => Ok(()),
    }
}

fn validate_level_scales(bids: &[Level], asks: &[Level]) -> Result<(), DomainError> {
    let mut levels = bids.iter().chain(asks);
    let Some(first) = levels.next() else {
        return Ok(());
    };
    first.validate()?;
    for level in levels {
        level.validate()?;
        if level.price.scale() != first.price.scale()
            || level.quantity.scale() != first.quantity.scale()
        {
            return Err(DomainError::ScaleMismatch);
        }
    }
    Ok(())
}

fn reject_duplicate_prices(levels: &[Level]) -> Result<(), DomainError> {
    if levels
        .windows(2)
        .any(|pair| pair[0].price.atoms() == pair[1].price.atoms())
    {
        Err(DomainError::DuplicatePrice)
    } else {
        Ok(())
    }
}

fn validate_order(levels: &[Level], side: Side) -> Result<(), DomainError> {
    reject_duplicate_prices(levels)?;
    if levels.windows(2).any(|pair| match side {
        Side::Bid => pair[0].price.atoms() <= pair[1].price.atoms(),
        Side::Ask => pair[0].price.atoms() >= pair[1].price.atoms(),
    }) {
        Err(DomainError::NonCanonicalLevelOrder)
    } else {
        Ok(())
    }
}
