//! Kalshi venue support for the shared Replay normalizer.

mod adapter;
mod config;
mod error;
mod event;
mod message;
mod value;

pub use adapter::Kalshi;
pub use config::Config;

pub const PARSER_VERSION: u32 = 1;
pub const ADAPTER_BUNDLE_ID: &str = "prediction-indexer/replay-kalshi/v1";
