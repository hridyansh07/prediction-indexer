//! Typed rejections. Every one names the field it is about, because an envelope
//! rejection is reported to an operator and "which field, and what did you mean"
//! is the actionable part.

use core::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnvelopeError {
    NotJson(String),
    InvalidUtf8,
    MissingField(&'static str),
    DuplicateField(&'static str),
    UnknownField(String),
    InvalidInteger {
        field: &'static str,
    },
    /// An identifier the parser cannot borrow verbatim: escaped, non-ASCII, empty,
    /// or carrying a control character.
    UnsupportedIdentifier {
        field: &'static str,
    },
    UnknownVenue(String),
    UnknownStream(String),
    UnknownRecordKind(String),
    InvalidSourceCursor,
    UnsupportedEnvelopeVersion(u64),
    InvalidEnvelopeVersionShape,
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotJson(detail) => write!(formatter, "not valid JSON: {detail}"),
            Self::InvalidUtf8 => formatter.write_str("line is not valid UTF-8"),
            Self::MissingField(field) => write!(formatter, "missing field: {field}"),
            Self::DuplicateField(field) => write!(formatter, "duplicate field: {field}"),
            Self::UnknownField(field) => write!(formatter, "unknown field: {field}"),
            Self::InvalidInteger { field } => {
                write!(
                    formatter,
                    "{field} must be a non-negative integer in plain digits"
                )
            }
            Self::UnsupportedIdentifier { field } => {
                write!(formatter, "{field} must be plain unescaped ASCII")
            }
            Self::UnknownVenue(value) => write!(formatter, "unknown venue: {value}"),
            Self::UnknownStream(value) => write!(formatter, "unknown stream: {value}"),
            Self::UnknownRecordKind(value) => write!(formatter, "unknown kind: {value}"),
            Self::InvalidSourceCursor => formatter.write_str("source_cursor shape not recognised"),
            Self::UnsupportedEnvelopeVersion(version) => {
                write!(formatter, "unsupported envelope version: {version}")
            }
            Self::InvalidEnvelopeVersionShape => formatter.write_str(
                "envelope_version and monotonic_ns must either both be absent (v1) or form v2",
            ),
        }
    }
}

impl std::error::Error for EnvelopeError {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeError {
    InvalidField { field: &'static str },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidField { field } => write!(formatter, "invalid field: {field}"),
        }
    }
}

impl std::error::Error for DecodeError {}
