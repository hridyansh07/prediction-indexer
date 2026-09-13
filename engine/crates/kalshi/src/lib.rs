//! Kalshi venue support for canonical-envelope normalization.

mod adapter;
mod config;
mod error;
mod event;
mod message;
mod process;
mod value;
mod wire;

pub use adapter::Kalshi;
pub use config::Config;

pub const PARSER_VERSION: u32 = 3;
pub const NORMALIZER_BUNDLE_ID: &str = "prediction-indexer/kalshi-normalizer/v3";
