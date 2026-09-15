//! Polymarket venue support for canonical-envelope normalization.

mod adapter;
mod config;
mod error;
mod event;
mod message;
mod value;
mod wire;

pub use adapter::Polymarket;
pub use config::Config;

pub const PARSER_VERSION: u32 = 2;
pub const NORMALIZER_BUNDLE_ID: &str = "prediction-indexer/polymarket-normalizer/v2";
