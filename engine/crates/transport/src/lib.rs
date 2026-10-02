//! One single-producer, fixed-membership Redis attempt. No retry or resumption.
pub mod buffer;
mod redis_sink;
pub mod wire;
pub use buffer::{
    DEFAULT_BATCH_ENTRIES, Entry, MAX_BATCH_ENTRIES, MIN_BATCH_ENTRIES, PublishBuffer,
    PublishStats, QueueWaits, StreamSink, hard_capacity,
};
pub use redis_sink::RedisSink;
use replay_domain::*;
use replay_materialize::{ReadLimits, inspect_pinned};
use replay_normalizers::CanonicalNormalizerIdentity;
use replay_risk::{BookPlan, RiskEngine, RiskLimits};
use replay_tape::{DerivativePin, LowerBoundPolicy, PinnedDerivative};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Stream/node/group/hash overhead was measured at ~22% of payload; Redis
/// `maxmemory` must be at least 13/10 of the payload budget at setup.
const MAXMEMORY_NUMERATOR: u128 = 13;
const MAXMEMORY_DENOMINATOR: u128 = 10;
/// True when `maxmemory` leaves room for the whole queue byte budget plus
/// stream overhead. Exact integer arithmetic; no rounding admits a short Redis.
pub fn maxmemory_fits_queue(maxmemory: u64, max_queue_bytes: usize) -> bool {
    u128::from(maxmemory) * MAXMEMORY_DENOMINATOR >= max_queue_bytes as u128 * MAXMEMORY_NUMERATOR
}
#[derive(Debug)]
pub enum Error {
    Protocol(String),
    Risk(String),
    Transport,
    Resource,
    Poisoned,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
pub(crate) fn redis_error(e: redis::RedisError) -> Error {
    if e.code() == Some("OOM") {
        Error::Resource
    } else if e.code() == Some("REPLAY") {
        if e.detail() == Some("resource_limit") {
            Error::Resource
        } else {
            Error::Protocol(e.detail().unwrap_or("invariant").into())
        }
    } else {
        Error::Transport
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub instrument: InstrumentId,
    pub orientation: ContractOrientation,
    pub lane: LaneId,
    pub venue: String,
    pub price_scale: String,
    pub quantity_scale: String,
}
impl Plan {
    fn risk(&self) -> Result<BookPlan> {
        let scale = |s: &str| {
            DecimalScale::new(
                integer(s)?
                    .try_into()
                    .map_err(|_| Error::Protocol("scale".into()))?,
            )
            .map_err(|_| Error::Protocol("scale".into()))
        };
        Ok(BookPlan {
            key: BookKey {
                instrument: self.instrument.clone(),
                orientation: self.orientation,
            },
            lane: self.lane.clone(),
            venue: self.venue.clone(),
            price_scale: scale(&self.price_scale)?,
            quantity_scale: scale(&self.quantity_scale)?,
        })
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Input {
    pub directory: std::path::PathBuf,
    pub derivative_address: String,
    pub receipt_sha256: Sha256,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub run_id: String,
    pub attempt_id: String,
    pub scope: String,
    pub normalizer: CanonicalNormalizerIdentity,
    pub inputs: Vec<Input>,
    pub start_ns: String,
    pub end_ns: String,
    pub lower_bound: String,
    pub plans: Vec<Plan>,
    pub groups: Vec<String>,
    pub command_timeout_ms: u64,
    pub max_entry_bytes: usize,
    pub max_queue_bytes: usize,
    /// Publisher buffering only (1..=1024; 100 when absent). It never changes
    /// delivered content, so it is not part of the `initial` wire record.
    #[serde(default = "default_publish_batch_entries")]
    pub publish_batch_entries: usize,
}
fn default_publish_batch_entries() -> usize {
    DEFAULT_BATCH_ENTRIES
}
pub fn integer(s: &str) -> Result<u64> {
    if s.is_empty() || (s.len() > 1 && s.starts_with('0')) || !s.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(Error::Protocol("integer syntax".into()));
    }
    s.parse()
        .map_err(|_| Error::Protocol("integer range".into()))
}
impl Config {
    /// Completes all caller-controlled derivative/normalizer/plan checks before
    /// a Redis client is constructed or any Redis command can be issued.
    pub fn validate(&self) -> Result<()> {
        let valid = |s: &str| {
            !s.is_empty()
                && s.len() <= 128
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
        };
        if !valid(&self.run_id)
            || !valid(&self.attempt_id)
            || !valid(&self.scope)
            || self.groups.is_empty()
            || self.groups.len() > 128
            || self.groups.iter().any(|s| !valid(s))
            || self
                .groups
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.groups.len()
            || self.command_timeout_ms == 0
            || self.command_timeout_ms > 60_000
            || self.max_entry_bytes == 0
            || self.max_queue_bytes < self.max_entry_bytes
            || self.max_queue_bytes > 1_000_000_000
            || !(MIN_BATCH_ENTRIES..=MAX_BATCH_ENTRIES).contains(&self.publish_batch_entries)
        {
            return Err(Error::Protocol("configuration".into()));
        }
        if self.inputs.is_empty() || self.inputs.len() > ReadLimits::default().max_windows {
            return Err(Error::Protocol("input count".into()));
        }
        let descriptor = self
            .normalizer
            .descriptor()
            .map_err(|error| Error::Protocol(error.to_string()))?;
        for input in &self.inputs {
            let pinned = PinnedDerivative {
                directory: input.directory.clone(),
                pin: DerivativePin {
                    derivative_address: input.derivative_address.clone(),
                    receipt_sha256: input.receipt_sha256,
                },
            };
            let metadata = inspect_pinned(&pinned, &ReadLimits::default())
                .map_err(|error| Error::Protocol(format!("pinned derivative: {error}")))?;
            if !metadata.supports_source_evidence() || metadata.manifest().materializer_version != 2
            {
                return Err(Error::Protocol(
                    "pinned derivative is not source-evidence profile 2".into(),
                ));
            }
            if metadata.manifest().normalizer_bundle_sha256 != descriptor.bundle_sha256 {
                return Err(Error::Protocol(
                    "pinned derivative normalizer bundle mismatch".into(),
                ));
            }
            if metadata.manifest().normalizer_config_sha256 != descriptor.config_sha256 {
                return Err(Error::Protocol(
                    "pinned derivative normalizer config mismatch".into(),
                ));
            }
        }
        for plan in &self.plans {
            let (price_scale, quantity_scale) = self
                .normalizer
                .scales(&plan.venue)
                .map_err(|error| Error::Protocol(error.to_string()))?;
            if plan.price_scale != price_scale.to_string()
                || plan.quantity_scale != quantity_scale.to_string()
            {
                return Err(Error::Protocol(format!(
                    "plan scales do not match normalizer identity for {}",
                    plan.venue
                )));
            }
        }
        Ok(())
    }
    pub fn initial(&self) -> Value {
        let pins: Vec<_> = self.inputs.iter().map(|i| json!({"derivative_address":i.derivative_address,"receipt_sha256":i.receipt_sha256})).collect();
        wire::exact(
            json!({"pins":pins,"start_ns":self.start_ns,"end_ns":self.end_ns,"lower_bound":self.lower_bound,"plans":self.plans,"groups":self.groups,"max_entry_bytes":self.max_entry_bytes,"max_queue_bytes":self.max_queue_bytes}),
        )
    }
    pub fn keys(&self) -> [String; 2] {
        [
            format!(
                "replay:{}:{}:{}:stream",
                self.scope, self.run_id, self.attempt_id
            ),
            format!(
                "replay:{}:{}:{}:state",
                self.scope, self.run_id, self.attempt_id
            ),
        ]
    }
}

/// Owns both the risk lifecycle and publication sequence. Terminal cannot be
/// supplied by callers, nor can a risk cut from a different engine be inserted.
/// Every stream-backend write goes through the `PublishBuffer`; the backend is
/// the `StreamSink` adapter `S` (Redis by default).
pub struct Publisher<S: StreamSink = RedisSink> {
    config: Config,
    engine: Option<RiskEngine>,
    buffer: PublishBuffer<S>,
    sequence: u64,
    poisoned: bool,
    terminal: bool,
}
fn risk_engine(config: &Config, limits: RiskLimits) -> Result<RiskEngine> {
    let policy = match config.lower_bound.as_str() {
        "clip" => LowerBoundPolicy::Clip,
        "expand_to_window_start" => LowerBoundPolicy::ExpandToWindowStart,
        "require_window_boundary" => LowerBoundPolicy::RequireWindowBoundary,
        _ => return Err(Error::Protocol("lower_bound".into())),
    };
    let inputs = config
        .inputs
        .iter()
        .map(|i| PinnedDerivative {
            directory: i.directory.clone(),
            pin: DerivativePin {
                derivative_address: i.derivative_address.clone(),
                receipt_sha256: i.receipt_sha256,
            },
        })
        .collect();
    RiskEngine::open(
        inputs,
        integer(&config.start_ns)?,
        integer(&config.end_ns)?,
        policy,
        config.plans.iter().map(Plan::risk).collect::<Result<_>>()?,
        limits,
    )
    .map_err(Error::Risk)
}
impl Publisher<RedisSink> {
    /// Validates, opens Risk, connects and checks Redis, then creates the
    /// attempt keys and publishes `initial` before returning (the CLI writes
    /// its readiness file only afterwards).
    pub fn open(url: &str, config: Config, limits: RiskLimits) -> Result<Self> {
        config.validate()?;
        let engine = risk_engine(&config, limits)?;
        let sink = RedisSink::connect(url, &config)?;
        Self::start(config, engine, sink)
    }
}
impl<S: StreamSink> Publisher<S> {
    /// The same lifecycle as `open` over any stream backend adapter.
    pub fn with_sink(sink: S, config: Config, limits: RiskLimits) -> Result<Self> {
        config.validate()?;
        let engine = risk_engine(&config, limits)?;
        Self::start(config, engine, sink)
    }
    fn start(config: Config, engine: RiskEngine, sink: S) -> Result<Self> {
        let buffer = PublishBuffer::new(
            sink,
            config.publish_batch_entries,
            config.max_entry_bytes,
            config.max_queue_bytes,
        );
        let mut p = Self {
            config,
            engine: Some(engine),
            buffer,
            sequence: 0,
            poisoned: false,
            terminal: false,
        };
        let groups = p.config.groups.clone();
        let (entry, queue) = (p.config.max_entry_bytes, p.config.max_queue_bytes);
        p.buffer.sink_mut().setup(&groups, entry, queue)?;
        // Initial must be visible before readiness: insert, then drain fully.
        let initial = p.config.initial();
        let published = p
            .insert("initial", initial)
            .and_then(|()| p.buffer.flush());
        if published.is_err() {
            p.poison();
        }
        published?;
        Ok(p)
    }
    fn poison(&mut self) {
        self.poisoned = true;
        // Best effort only; supervisor must also regard local failure as fatal.
        self.buffer.sink_mut().poison();
    }
    fn insert(&mut self, kind: &str, body: Value) -> Result<()> {
        let record = self.record(kind, body);
        self.buffer.insert(Entry {
            sequence: self.sequence,
            record: serde_json::to_string(&record).unwrap(),
            terminal: kind == "terminal",
        })
    }
    /// Totals of queue-full waits and backoff sleep so far.
    pub fn queue_waits(&self) -> QueueWaits {
        self.buffer.stats().waits
    }
    /// Queue waits plus append calls and entries written so far.
    pub fn publish_stats(&self) -> PublishStats {
        self.buffer.stats()
    }
    fn record(&self, kind: &str, body: Value) -> Value {
        json!({"version":"1","run_id":self.config.run_id,"attempt_id":self.config.attempt_id,"sequence":self.sequence.to_string(),"kind":kind,"body":body})
    }
    /// Buffers one cut, or EOF terminal. Returns false only after terminal,
    /// and only once every buffered entry, terminal included, is appended.
    pub fn step(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        if self.terminal {
            return Ok(false);
        }
        let result = self.step_inner();
        if result.is_err() {
            self.poison();
        }
        result
    }
    fn step_inner(&mut self) -> Result<bool> {
        let cut = self
            .engine
            .as_mut()
            .unwrap()
            .next_cut()
            .map_err(Error::Risk)?;
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Resource)?;
        if let Some(cut) = cut {
            if cut.sequence() != self.sequence {
                return Err(Error::Protocol("risk sequence".into()));
            }
            self.insert("cut", wire::cut(&cut))?;
            Ok(true)
        } else {
            let engine = self.engine.take().unwrap();
            // Read-only view of final Risk books, taken before finish consumes it.
            let books_sha256 = wire::books_sha256(&engine);
            let finished = engine.finish().map_err(Error::Risk)?;
            self.insert("terminal", wire::terminal(finished.cuts(), books_sha256))?;
            self.buffer.flush()?;
            self.terminal = true;
            Ok(false)
        }
    }
    /// Values use Redis entry sequence (wire sequence + 1), with -1 before ACK.
    pub fn progress(&mut self) -> Result<BTreeMap<String, String>> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let result = self.buffer.sink_mut().progress();
        if result.is_err() {
            self.poison();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maxmemory_guard_requires_thirteen_tenths_of_queue_bytes() {
        const MIB: u64 = 1024 * 1024;
        // Production `small` preset (64 MiB queue) on a 150 MiB Redis.
        assert!(maxmemory_fits_queue(150 * MIB, 64 * MIB as usize));
        // Exact boundary: 10 * maxmemory == 13 * queue passes; one byte less fails.
        assert!(maxmemory_fits_queue(13, 10));
        assert!(!maxmemory_fits_queue(12, 10));
        assert!(maxmemory_fits_queue(1_300_000_000, 1_000_000_000));
        assert!(!maxmemory_fits_queue(1_299_999_999, 1_000_000_000));
        // A Redis sized to the payload alone has no room for stream overhead.
        assert!(!maxmemory_fits_queue(64 * MIB, 64 * MIB as usize));
        // No overflow at the extremes of the operand types.
        assert!(maxmemory_fits_queue(u64::MAX, 1_000_000_000));
        assert!(!maxmemory_fits_queue(1, usize::MAX));
    }

    #[test]
    fn redis_oom_is_resource_but_other_server_errors_are_not() {
        assert!(matches!(
            redis_error(
                redis::parse_redis_value(b"-OOM memory exhausted\r\n")
                    .unwrap()
                    .extract_error()
                    .unwrap_err()
            ),
            Error::Resource
        ));
        assert!(matches!(
            redis_error(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "ERR"
            ))),
            Error::Transport
        ));
    }
}
