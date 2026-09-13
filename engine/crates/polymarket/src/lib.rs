//! Polymarket venue support for canonical-envelope normalization.

mod adapter;
mod config;
mod error;
mod message;
mod value;

pub use adapter::Polymarket;
pub use config::Config;

pub const PARSER_VERSION: u32 = 1;
pub const NORMALIZER_BUNDLE_ID: &str = "prediction-indexer/polymarket-normalizer/v1";
