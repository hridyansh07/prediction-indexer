//! One-shot retention for closed UTC ingest-store partitions.
//!
//! The database is derived from the raw spool, so its deletion gate is narrower
//! than the raw-data reaper's dual-receipt gate. This command still fails closed:
//! only an immutable `store.db` with a valid partition receipt, an exact recorded
//! byte identity, and a closure age at least as large as the retention period is
//! eligible. The active `store.db.open` is never touched. Receipts remain as the
//! durable consumed-segment ledger after their database is gone.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::ingest_partition::{
    ACTIVE_FILE, DATABASE_FILE, OPEN_DATABASE_FILE, RECEIPT_FILE, partition_directories,
    read_receipts, remove_durable, sha256_file_streaming, write_json_file_durable,
};

pub const SWEEP_VERSION: u64 = 1;
pub const MIN_RETENTION_HOURS: u64 = 24;
const NS_PER_HOUR: u64 = 3_600_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReapMode {
    Audit,
    Delete,
}

impl ReapMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "audit" => Ok(Self::Audit),
            "delete" => Ok(Self::Delete),
            _ => Err(format!(
                "invalid reaper mode {value:?}; expected audit or delete"
            )),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct SweepCounts {
    pub considered: u64,
    pub retained: u64,
    pub reapable: u64,
    pub reaped: u64,
    pub already_reaped: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReapDecision {
    pub partition_date: String,
    pub action: String,
    pub reason: String,
    pub closed_at_ns: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReapSweep {
    pub ingest_store_reaper_sweep_version: u64,
    pub store_root: String,
    pub now_ns: u64,
    pub retention_hours: u64,
    pub mode: ReapMode,
    pub counts: SweepCounts,
    pub decisions: Vec<ReapDecision>,
}

impl ReapSweep {
    pub fn write(&self, path: &Path) -> Result<(), String> {
        write_json_file_durable(path, self)
    }
}

pub fn sweep(
    root: &Path,
    now_ns: u64,
    retention_hours: u64,
    mode: ReapMode,
) -> Result<ReapSweep, String> {
    if retention_hours < MIN_RETENTION_HOURS {
        return Err(format!(
            "ingest-store retention must be at least {MIN_RETENTION_HOURS} hours"
        ));
    }
    let retention_ns = retention_hours
        .checked_mul(NS_PER_HOUR)
        .ok_or_else(|| "retention period is too large".to_owned())?;
    let receipts = read_receipts(root)?;
    let receipts_by_date = receipts
        .into_iter()
        .map(|record| (record.receipt.partition_date.clone(), record.receipt))
        .collect::<BTreeMap<_, _>>();

    let mut report = ReapSweep {
        ingest_store_reaper_sweep_version: SWEEP_VERSION,
        store_root: root.display().to_string(),
        now_ns,
        retention_hours,
        mode,
        counts: SweepCounts::default(),
        decisions: Vec::new(),
    };

    for directory in partition_directories(root)? {
        let date = partition_date(&directory)?;
        let open = directory.join(OPEN_DATABASE_FILE);
        let closed = directory.join(DATABASE_FILE);
        let active = directory.join(ACTIVE_FILE);
        let receipt_path = directory.join(RECEIPT_FILE);
        let has_open = open.exists();
        let has_closed = closed.exists();
        let has_active = active.exists();
        let has_receipt = receipt_path.exists();

        if (has_open && has_closed) || (has_receipt && (has_open || has_active)) {
            return Err(format!(
                "contradictory ingest partition state in {}",
                directory.display()
            ));
        }
        if !(has_open || has_closed || has_active || has_receipt) {
            continue;
        }

        report.counts.considered += 1;
        let Some(receipt) = receipts_by_date.get(&date) else {
            report.counts.retained += 1;
            report.decisions.push(decision(
                date,
                "retained",
                if has_open || has_active {
                    "active_partition"
                } else {
                    "receipt_missing"
                },
                None,
            ));
            continue;
        };

        if !has_closed {
            report.counts.already_reaped += 1;
            report.decisions.push(decision(
                date,
                "absent",
                "database_already_reaped",
                Some(receipt.closed_at_ns),
            ));
            continue;
        }

        let Some(age_ns) = now_ns.checked_sub(receipt.closed_at_ns) else {
            report.counts.retained += 1;
            report.decisions.push(decision(
                date,
                "retained",
                "clock_before_partition_close",
                Some(receipt.closed_at_ns),
            ));
            continue;
        };
        if age_ns < retention_ns {
            report.counts.retained += 1;
            report.decisions.push(decision(
                date,
                "retained",
                "retention_floor",
                Some(receipt.closed_at_ns),
            ));
            continue;
        }

        let current_length = closed
            .metadata()
            .map_err(|error| format!("reading {} metadata: {error}", closed.display()))?
            .len();
        if current_length != receipt.database_byte_length
            || sha256_file_streaming(&closed)? != receipt.database_sha256
        {
            report.counts.retained += 1;
            report.decisions.push(decision(
                date,
                "retained",
                "database_identity_mismatch",
                Some(receipt.closed_at_ns),
            ));
            continue;
        }

        report.counts.reapable += 1;
        match mode {
            ReapMode::Audit => {
                report.counts.retained += 1;
                report.decisions.push(decision(
                    date,
                    "retained",
                    "audit_mode",
                    Some(receipt.closed_at_ns),
                ));
            }
            ReapMode::Delete => {
                remove_durable(&closed)?;
                report.counts.reaped += 1;
                report.decisions.push(decision(
                    date,
                    "reaped",
                    "retention_elapsed",
                    Some(receipt.closed_at_ns),
                ));
            }
        }
    }

    Ok(report)
}

fn partition_date(directory: &Path) -> Result<String, String> {
    directory
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("date="))
        .map(str::to_owned)
        .ok_or_else(|| format!("invalid ingest partition path {}", directory.display()))
}

fn decision(
    partition_date: String,
    action: &str,
    reason: &str,
    closed_at_ns: Option<u64>,
) -> ReapDecision {
    ReapDecision {
        partition_date,
        action: action.to_owned(),
        reason: reason.to_owned(),
        closed_at_ns,
    }
}

pub fn default_report_path(root: &Path) -> PathBuf {
    root.parent()
        .unwrap_or(root)
        .join("ops/last_ingest_store_reaper_sweep.json")
}
