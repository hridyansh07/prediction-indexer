use core::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainError {
    Empty(&'static str),
    InvalidInstrumentId,
    InvalidCanonicalSequence,
    InvalidSourceLine,
    OrderClockMismatch,
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
            Self::InvalidCanonicalSequence => formatter.write_str("canonical_seq must be positive"),
            Self::InvalidSourceLine => formatter.write_str("source_line_number must be positive"),
            Self::OrderClockMismatch => {
                formatter.write_str("order_ns must equal visible_ns in schema v1")
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
