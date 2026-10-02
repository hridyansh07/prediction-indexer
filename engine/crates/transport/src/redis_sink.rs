//! The Redis/Lua `StreamSink`: one synchronous connection, no retry, and every
//! write performed by `replay/streams/attempt.lua` (setup, batch append, check).
use crate::{Config, Error, Result, StreamSink, maxmemory_fits_queue, redis_error};
use std::{collections::BTreeMap, time::Duration};

const SCRIPT: &str = include_str!("../../../../replay/streams/attempt.lua");

pub struct RedisSink {
    connection: redis::Connection,
    script: redis::Script,
    keys: [String; 2],
}

impl RedisSink {
    /// Connects with finite timeouts and enforces the deployment requirements
    /// (Redis >= 8.2, positive maxmemory, noeviction, 13/10 queue headroom).
    /// Every requirement fails before any key is created.
    pub fn connect(url: &str, config: &Config) -> Result<Self> {
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
        let maxmemory = settings
            .get("maxmemory")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        if parts.len() != 2
            || (parts[0], parts[1]) < (8, 2)
            || settings.get("maxmemory-policy").map(String::as_str) != Some("noeviction")
            || maxmemory == 0
        {
            return Err(Error::Protocol(
                "Redis >=8.2 with maxmemory and noeviction required".into(),
            ));
        }
        // Waiting on a full queue is only safe if the queue itself cannot drive
        // Redis into OOM. A deterministic deployment mismatch: never retried.
        if !maxmemory_fits_queue(maxmemory, config.max_queue_bytes) {
            return Err(Error::Protocol(format!(
                "Redis maxmemory {maxmemory} is below 13/10 of max_queue_bytes {}",
                config.max_queue_bytes
            )));
        }
        Ok(Self {
            connection,
            script: redis::Script::new(SCRIPT),
            keys: config.keys(),
        })
    }
    fn eval<T: redis::FromRedisValue>(&mut self, head: &[&str], entries: &[String]) -> Result<T> {
        let result = redis::cmd("EVALSHA")
            .arg(self.script.get_hash())
            .arg(2)
            .arg(&self.keys)
            .arg(head)
            .arg(entries)
            .query(&mut self.connection);
        match result {
            Err(e) if e.kind() == redis::ErrorKind::NoScriptError => {
                // NOSCRIPT guarantees no execution. All ambiguous failures stay fatal.
                redis::cmd("EVAL")
                    .arg(SCRIPT)
                    .arg(2)
                    .arg(&self.keys)
                    .arg(head)
                    .arg(entries)
                    .query(&mut self.connection)
                    .map_err(redis_error)
            }
            other => other.map_err(redis_error),
        }
    }
}

impl StreamSink for RedisSink {
    fn setup(
        &mut self,
        groups: &[String],
        max_entry_bytes: usize,
        max_queue_bytes: usize,
    ) -> Result<()> {
        let groups = serde_json::to_string(groups).unwrap();
        let (queue, entry) = (max_queue_bytes.to_string(), max_entry_bytes.to_string());
        let _: String = self.eval(&["setup", &groups, &queue, &entry], &[])?;
        Ok(())
    }
    fn append(&mut self, first: u64, entries: &[String], terminal: bool) -> Result<usize> {
        // Redis entry IDs are wire sequence + 1; `published` is -1 before initial.
        let previous = if first == 0 {
            "-1".to_string()
        } else {
            first.to_string()
        };
        let id = first.checked_add(1).ok_or(Error::Resource)?.to_string();
        let flag = if terminal { "1" } else { "0" };
        let reply: redis::Value = self.eval(&["append", &previous, &id, flag], entries)?;
        match reply {
            redis::Value::Int(n) if n >= 0 && (n as u64) <= entries.len() as u64 => {
                Ok(n as usize)
            }
            _ => Err(Error::Protocol("append reply".into())),
        }
    }
    fn progress(&mut self) -> Result<BTreeMap<String, String>> {
        self.eval(&["check"], &[])
    }
    fn poison(&mut self) {
        let _: redis::RedisResult<()> = redis::cmd("HSET")
            .arg(&self.keys[1])
            .arg("poisoned")
            .arg("1")
            .query(&mut self.connection);
    }
}
