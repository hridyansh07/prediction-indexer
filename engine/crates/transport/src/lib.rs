//! One single-producer, fixed-membership Redis attempt. No retry or resumption.
pub mod wire;
use replay_domain::*;
use replay_risk::{BookPlan, RiskEngine, RiskLimits};
use replay_tape::{DerivativePin, LowerBoundPolicy, PinnedDerivative};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

const SCRIPT: &str = include_str!("../../../../replay/streams/attempt.lua");
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
fn redis_error(e: redis::RedisError) -> Error {
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
    pub inputs: Vec<Input>,
    pub start_ns: String,
    pub end_ns: String,
    pub lower_bound: String,
    pub plans: Vec<Plan>,
    pub groups: Vec<String>,
    pub command_timeout_ms: u64,
    pub max_entry_bytes: usize,
    pub max_queue_bytes: usize,
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
    fn validate(&self) -> Result<()> {
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
        {
            return Err(Error::Protocol("configuration".into()));
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
pub struct Publisher {
    config: Config,
    engine: Option<RiskEngine>,
    connection: redis::Connection,
    script: redis::Script,
    sequence: u64,
    poisoned: bool,
    terminal: bool,
}
impl Publisher {
    pub fn open(url: &str, config: Config, limits: RiskLimits) -> Result<Self> {
        config.validate()?;
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
        let engine = RiskEngine::open(
            inputs,
            integer(&config.start_ns)?,
            integer(&config.end_ns)?,
            policy,
            config.plans.iter().map(Plan::risk).collect::<Result<_>>()?,
            limits,
        )
        .map_err(Error::Risk)?;
        let timeout = Duration::from_millis(config.command_timeout_ms);
        let mut connection = redis::Client::open(url)
            .map_err(redis_error)?
            .get_connection_with_timeout(timeout)
            .map_err(redis_error)?;
        connection
            .set_read_timeout(Some(timeout))
            .map_err(redis_error)?;
        connection
            .set_write_timeout(Some(timeout))
            .map_err(redis_error)?;
        let info: String = redis::cmd("INFO")
            .arg("server")
            .query(&mut connection)
            .map_err(redis_error)?;
        let version = info
            .lines()
            .find_map(|l| l.strip_prefix("redis_version:"))
            .unwrap_or("");
        let parts: Vec<u64> = version
            .trim()
            .split('.')
            .take(2)
            .filter_map(|s| s.parse().ok())
            .collect();
        let settings: BTreeMap<String, String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory")
            .arg("maxmemory-policy")
            .query(&mut connection)
            .map_err(redis_error)?;
        if parts.len() != 2
            || (parts[0], parts[1]) < (8, 2)
            || settings.get("maxmemory-policy").map(String::as_str) != Some("noeviction")
            || settings
                .get("maxmemory")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
                == 0
        {
            return Err(Error::Protocol(
                "Redis >=8.2 with maxmemory and noeviction required".into(),
            ));
        }
        let mut p = Self {
            config,
            engine: Some(engine),
            connection,
            script: redis::Script::new(SCRIPT),
            sequence: 0,
            poisoned: false,
            terminal: false,
        };
        let setup = p.eval::<String>(&[
            "setup".into(),
            serde_json::to_string(&p.config.groups).unwrap(),
            p.config.max_queue_bytes.to_string(),
            p.config.max_entry_bytes.to_string(),
        ]);
        setup?;
        let initial = p.config.initial();
        p.publish("initial", initial, "-1")?;
        Ok(p)
    }
    fn eval<T: redis::FromRedisValue>(&mut self, args: &[String]) -> Result<T> {
        let result = redis::cmd("EVALSHA")
            .arg(self.script.get_hash())
            .arg(2)
            .arg(&self.config.keys())
            .arg(args)
            .query(&mut self.connection);
        match result {
            Err(e) if e.kind() == redis::ErrorKind::NoScriptError => {
                // NOSCRIPT guarantees no execution. All ambiguous failures stay fatal.
                redis::cmd("EVAL")
                    .arg(SCRIPT)
                    .arg(2)
                    .arg(&self.config.keys())
                    .arg(args)
                    .query(&mut self.connection)
                    .map_err(redis_error)
            }
            other => other.map_err(redis_error),
        }
    }
    fn poison(&mut self) {
        self.poisoned = true;
        // Best effort only; supervisor must also regard local failure as fatal.
        let _: redis::RedisResult<()> = redis::cmd("HSET")
            .arg(&self.config.keys()[1])
            .arg("poisoned")
            .arg("1")
            .query(&mut self.connection);
    }
    fn publish(&mut self, kind: &str, body: Value, previous: &str) -> Result<()> {
        let record = self.record(kind, body);
        let bytes = serde_json::to_string(&record).unwrap();
        if bytes.len() > self.config.max_entry_bytes {
            self.poison();
            return Err(Error::Resource);
        }
        // Redis IDs start at 1; wire sequences start at 0 (initial).
        let result = self.eval::<String>(&[
            "publish".into(),
            previous.into(),
            (self.sequence + 1).to_string(),
            bytes,
            if kind == "terminal" { "1" } else { "0" }.into(),
        ]);
        if result.is_err() {
            self.poison();
        }
        result.map(|_| ())
    }
    fn record(&self, kind: &str, body: Value) -> Value {
        json!({"version":"1","run_id":self.config.run_id,"attempt_id":self.config.attempt_id,"sequence":self.sequence.to_string(),"kind":kind,"body":body})
    }
    /// Publishes one cut, or EOF terminal. Returns false only after terminal.
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
        let previous = (self.sequence + 1).to_string();
        self.sequence = self.sequence.checked_add(1).ok_or(Error::Resource)?;
        if let Some(cut) = cut {
            if cut.sequence() != self.sequence {
                return Err(Error::Protocol("risk sequence".into()));
            }
            self.publish("cut", wire::cut(&cut), &previous)?;
            Ok(true)
        } else {
            let finished = self.engine.take().unwrap().finish().map_err(Error::Risk)?;
            self.publish(
                "terminal",
                json!({"cuts":finished.cuts().to_string()}),
                &previous,
            )?;
            self.terminal = true;
            Ok(false)
        }
    }
    /// Values use Redis entry sequence (wire sequence + 1), with -1 before ACK.
    pub fn progress(&mut self) -> Result<BTreeMap<String, String>> {
        if self.poisoned {
            return Err(Error::Poisoned);
        }
        let result = self.eval(&["check".into()]);
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
