//! UTC ingestion-day partitions and their retained rollover receipts.
//!
//! The SQLite fact store is derived, but it used to grow for the lifetime of the
//! deployment. One writable database now owns all segments ingested during one
//! UTC day. At rollover it is checkpointed, closed, and committed by a receipt;
//! the next database continues the global `file_order` sequence from the bounded
//! carry in that receipt. Identity deliberately starts empty in every partition.
//!
//! Receipts survive database reaping. Their segment list is therefore also the
//! durable skip ledger: an archived raw segment may remain in the spool after the
//! database that first consumed it is gone, and must not be ingested again.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use indexer_continuity::{
    Classifier, ClassifierState, ContinuityFact, EpochKey, EpochState, LaneId,
};
use indexer_finalize::date_partition;
use indexer_store::{ConsumedSegment, Store};
use indexer_types::{EnvelopeView, Stream, Venue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const ACTIVE_VERSION: u64 = 1;
pub const PARTITION_RECEIPT_VERSION: u64 = 1;
pub const IDENTITY_SCOPE: &str = "ingest_partition";
pub const ACTIVE_FILE: &str = "active.json";
pub const RECEIPT_FILE: &str = "receipt.json";
pub const OPEN_DATABASE_FILE: &str = "store.db.open";
pub const DATABASE_FILE: &str = "store.db";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCarry {
    pub venue: String,
    pub epoch: String,
    pub local_counter: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EpochCarry {
    pub venue: String,
    pub stream: String,
    pub epoch: String,
    pub monotonic_key: u64,
}

/// Classification-critical state crossing one UTC ingestion boundary.
///
/// Only connections and stream epochs observed in the closing partition are
/// carried. A connection silent for a complete partition therefore restarts at
/// bootstrap if it later speaks again: the continuity and identity claim are
/// exact over the retained ingest horizon, not deployment lifetime.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IngestCarry {
    pub connections: Vec<ConnectionCarry>,
    pub epochs: Vec<EpochCarry>,
    pub duplicates: u64,
    pub conflicts: u64,
}

impl IngestCarry {
    fn restore(&self) -> Result<ClassifierState, String> {
        let mut state = ClassifierState {
            duplicates: self.duplicates,
            conflicts: self.conflicts,
            ..ClassifierState::default()
        };
        for entry in &self.connections {
            let venue = Venue::from_wire(&entry.venue)
                .ok_or_else(|| format!("unknown carry venue {:?}", entry.venue))?;
            if state
                .connections
                .insert((venue, entry.epoch.clone()), entry.local_counter)
                .is_some()
            {
                return Err(format!(
                    "duplicate connection carry for {}/{:?}",
                    entry.venue, entry.epoch
                ));
            }
        }
        for entry in &self.epochs {
            let venue = Venue::from_wire(&entry.venue)
                .ok_or_else(|| format!("unknown carry venue {:?}", entry.venue))?;
            let stream = Stream::from_wire(&entry.stream)
                .ok_or_else(|| format!("unknown carry stream {:?}", entry.stream))?;
            let key = EpochKey {
                lane: LaneId { venue, stream },
                epoch: entry.epoch.clone(),
            };
            if state
                .epochs
                .insert(
                    key,
                    EpochState {
                        monotonic_key: Some(entry.monotonic_key),
                        ..EpochState::default()
                    },
                )
                .is_some()
            {
                return Err(format!(
                    "duplicate epoch carry for {}/{}/{:?}",
                    entry.venue, entry.stream, entry.epoch
                ));
            }
        }
        Ok(state)
    }
}

/// Which continuity keys were actually observed in the active partition.
#[derive(Clone, Debug, Default)]
pub struct CarryTracker {
    connections: BTreeSet<(Venue, String)>,
    epochs: BTreeSet<EpochKey>,
}

impl CarryTracker {
    pub fn observe_envelope(&mut self, envelope: &EnvelopeView<'_>) {
        self.connections.insert((
            envelope.venue,
            envelope.connection_epoch.as_str().to_owned(),
        ));
        self.epochs.insert(EpochKey::new(
            envelope.venue,
            envelope.stream,
            envelope.connection_epoch.as_str().to_owned(),
        ));
    }

    pub fn observe_fact(&mut self, fact: &ContinuityFact) {
        self.connections
            .insert((fact.key.lane.venue, fact.key.epoch.clone()));
        self.epochs.insert(fact.key.clone());
    }

    fn capture(&self, state: &ClassifierState) -> IngestCarry {
        let connections = self
            .connections
            .iter()
            .filter_map(|(venue, epoch)| {
                state
                    .connections
                    .get(&(*venue, epoch.clone()))
                    .map(|counter| ConnectionCarry {
                        venue: venue.as_str().to_owned(),
                        epoch: epoch.clone(),
                        local_counter: *counter,
                    })
            })
            .collect();
        let epochs = self
            .epochs
            .iter()
            .filter_map(|key| {
                state
                    .epochs
                    .get(key)
                    .and_then(|state| state.monotonic_key)
                    .map(|monotonic_key| EpochCarry {
                        venue: key.lane.venue.as_str().to_owned(),
                        stream: key.lane.stream.as_str().to_owned(),
                        epoch: key.epoch.clone(),
                        monotonic_key,
                    })
            })
            .collect();
        IngestCarry {
            connections,
            epochs,
            duplicates: state.duplicates,
            conflicts: state.conflicts,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActivePartition {
    pub active_version: u64,
    pub partition_date: String,
    pub opened_at_ns: u64,
    pub first_file_order_seq: i64,
    pub parent_receipt_sha256: Option<String>,
    pub initial_carry: IngestCarry,
    pub identity_scope: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SegmentRecord {
    pub path: String,
    pub lane: String,
    pub source_sha256: String,
    pub byte_length: u64,
    pub line_count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PartitionReceipt {
    pub ingest_partition_receipt_version: u64,
    pub partition_date: String,
    pub opened_at_ns: u64,
    pub closed_at_ns: u64,
    pub database_file: String,
    pub database_byte_length: u64,
    pub database_sha256: String,
    pub first_file_order_seq: i64,
    pub last_file_order_seq: Option<i64>,
    pub evidence_rows: u64,
    pub fact_rows: u64,
    pub parent_receipt_sha256: Option<String>,
    pub terminal_carry: IngestCarry,
    pub identity_scope: String,
    pub segments: Vec<SegmentRecord>,
}

#[derive(Clone, Debug)]
pub struct ReceiptRecord {
    pub path: PathBuf,
    pub digest: String,
    pub receipt: PartitionReceipt,
}

#[derive(Clone, Debug, Default)]
pub struct ConsumedIndex(BTreeMap<String, String>);

impl ConsumedIndex {
    fn insert(&mut self, path: &str, sha256: &str) -> Result<(), String> {
        validate_relative_path(path)?;
        validate_sha256(sha256)?;
        if let Some(previous) = self.0.insert(path.to_owned(), sha256.to_owned()) {
            if previous != sha256 {
                return Err(format!(
                    "consumed segment {path:?} has conflicting identities {previous} and {sha256}"
                ));
            }
        }
        Ok(())
    }

    pub fn contains(&self, path: &str, sha256: &str) -> Result<bool, String> {
        match self.0.get(path) {
            None => Ok(false),
            Some(expected) if expected == sha256 => Ok(true),
            Some(expected) => Err(format!(
                "sealed segment {path:?} now has sha256 {sha256}; its retained ingest receipt records {expected}"
            )),
        }
    }

    pub fn remember(&mut self, path: &str, sha256: &str) -> Result<(), String> {
        self.insert(path, sha256)
    }
}

pub struct OpenPartition {
    root: PathBuf,
    marker: ActivePartition,
    pub store: Store,
    pub classifier: Classifier,
    pub tracker: CarryTracker,
    pub consumed: ConsumedIndex,
    receipts: Vec<ReceiptRecord>,
}

impl OpenPartition {
    pub fn open(root: &Path, now_ns: u64) -> Result<Self, String> {
        std::fs::create_dir_all(root)
            .map_err(|error| format!("creating ingest store {}: {error}", root.display()))?;
        let today = date_partition(now_ns);
        relocate_legacy_store(root, &today, now_ns)?;

        let receipts = read_receipts(root)?;
        let mut consumed = ConsumedIndex::default();
        for record in &receipts {
            for segment in &record.receipt.segments {
                consumed.insert(&segment.path, &segment.source_sha256)?;
            }
        }

        let active = recover_active_database(root, &receipts, now_ns, find_active(root)?)?;
        let (directory, marker) = match active {
            Some(active) => active,
            None => create_active(root, &today, now_ns, receipts.last())?,
        };
        validate_active(&directory, &marker)?;
        let (expected_first, expected_parent, expected_carry) = seed_from(receipts.last());
        if marker.first_file_order_seq != expected_first
            || marker.parent_receipt_sha256 != expected_parent
            || marker.initial_carry != expected_carry
        {
            return Err(format!(
                "active ingest marker in {} does not continue the latest receipt",
                directory.display()
            ));
        }
        if marker.partition_date > today {
            return Err(format!(
                "UTC clock moved backwards: active ingest partition is {} but current date is {today}",
                marker.partition_date
            ));
        }
        if directory.join(RECEIPT_FILE).exists() {
            return Err(format!(
                "{} has both an active marker and a close receipt",
                directory.display()
            ));
        }
        if directory.join(DATABASE_FILE).exists() {
            return Err(format!(
                "uncommitted closed database was not recovered in {}",
                directory.display()
            ));
        }

        let store = Store::open_partition(&directory, marker.first_file_order_seq)
            .map_err(|error| format!("opening partition {}: {error}", directory.display()))?;
        for segment in store
            .consumed_segments()
            .map_err(|error| format!("reading consumed segments: {error}"))?
        {
            consumed.insert(&segment.spool_file, &segment.source_sha256)?;
        }

        let mut classifier = Classifier::without_identity_history();
        classifier.restore(marker.initial_carry.restore()?);
        let mut tracker = CarryTracker::default();
        store
            .recover_facts(ContinuityFact::from_canonical_bytes, |fact| {
                tracker.observe_fact(fact.value());
                classifier.apply(&fact);
            })
            .map_err(|error| format!("recovering active partition state: {error}"))?;

        Ok(Self {
            root: root.to_owned(),
            marker,
            store,
            classifier,
            tracker,
            consumed,
            receipts,
        })
    }

    pub fn date(&self) -> &str {
        &self.marker.partition_date
    }

    pub fn directory(&self) -> PathBuf {
        partition_directory(&self.root, &self.marker.partition_date)
    }

    pub fn should_rotate(&self, now_ns: u64) -> Result<bool, String> {
        let today = date_partition(now_ns);
        if today < self.marker.partition_date {
            return Err(format!(
                "UTC clock moved backwards: active ingest partition is {} but current date is {today}",
                self.marker.partition_date
            ));
        }
        Ok(today != self.marker.partition_date)
    }

    pub fn rotate(self, now_ns: u64) -> Result<Self, String> {
        if !self.should_rotate(now_ns)? {
            return Ok(self);
        }
        let root = self.root.clone();
        self.close(now_ns)?;
        Self::open(&root, now_ns)
    }

    pub fn close(self, now_ns: u64) -> Result<ReceiptRecord, String> {
        let directory = self.directory();
        let evidence_rows = self
            .store
            .evidence_count()
            .map_err(|error| format!("counting partition evidence: {error}"))?;
        let fact_rows = self
            .store
            .fact_count()
            .map_err(|error| format!("counting partition facts: {error}"))?;
        let first = self.store.first_evidence_seq();
        let next = self.store.next_evidence_seq();
        let mut segments = self
            .store
            .consumed_segments()
            .map_err(|error| format!("reading consumed segments: {error}"))?
            .into_iter()
            .map(segment_record)
            .collect::<Result<Vec<_>, _>>()?;
        segments.sort_by(|left, right| left.path.cmp(&right.path));
        let terminal_carry = self.tracker.capture(self.classifier.state());
        let receipt = PartitionReceipt {
            ingest_partition_receipt_version: PARTITION_RECEIPT_VERSION,
            partition_date: self.marker.partition_date.clone(),
            opened_at_ns: self.marker.opened_at_ns,
            closed_at_ns: now_ns,
            database_file: DATABASE_FILE.to_owned(),
            database_byte_length: 0,
            database_sha256: String::new(),
            first_file_order_seq: first,
            last_file_order_seq: (next > first).then_some(next - 1),
            evidence_rows,
            fact_rows,
            parent_receipt_sha256: self.marker.parent_receipt_sha256.clone(),
            terminal_carry,
            identity_scope: IDENTITY_SCOPE.to_owned(),
            segments,
        };

        self.store
            .checkpoint_and_close()
            .map_err(|error| format!("closing partition database: {error}"))?;
        let open_database = directory.join(OPEN_DATABASE_FILE);
        let closed_database = directory.join(DATABASE_FILE);
        std::fs::File::open(&open_database)
            .and_then(|file| file.sync_all())
            .map_err(|error| format!("fsyncing closed partition database: {error}"))?;
        if closed_database.exists() {
            return Err(format!(
                "closed partition database already exists: {}",
                closed_database.display()
            ));
        }
        std::fs::rename(&open_database, &closed_database).map_err(|error| {
            format!(
                "renaming {} to {}: {error}",
                open_database.display(),
                closed_database.display()
            )
        })?;
        fsync_directory(&directory)?;

        let mut receipt = receipt;
        receipt.database_byte_length = closed_database
            .metadata()
            .map_err(|error| format!("reading {} metadata: {error}", closed_database.display()))?
            .len();
        receipt.database_sha256 = sha256_file_streaming(&closed_database)?;
        validate_receipt(&directory, &receipt)?;

        // The receipt is the commit marker and is published last. If the process
        // dies before it appears, startup renames the unreceipted database back
        // to `.open`, reconstructs state from its facts, and retries closure.
        remove_durable(&directory.join(ACTIVE_FILE))?;
        write_json_durable(&directory, RECEIPT_FILE, &receipt)?;

        let path = directory.join(RECEIPT_FILE);
        let digest = sha256_file(&path)?;
        Ok(ReceiptRecord {
            path,
            digest,
            receipt,
        })
    }

    pub fn receipt_count(&self) -> usize {
        self.receipts.len()
    }
}

fn segment_record(segment: ConsumedSegment) -> Result<SegmentRecord, String> {
    validate_relative_path(&segment.spool_file)?;
    validate_sha256(&segment.source_sha256)?;
    Ok(SegmentRecord {
        path: segment.spool_file,
        lane: segment.lane,
        source_sha256: segment.source_sha256,
        byte_length: segment.byte_length,
        line_count: segment.line_count,
    })
}

pub fn relative_segment_path(spool_root: &Path, path: &Path) -> Result<String, String> {
    let relative = path.strip_prefix(spool_root).map_err(|_| {
        format!(
            "segment {} is outside spool root {}",
            path.display(),
            spool_root.display()
        )
    })?;
    let encoded = relative
        .components()
        .map(|component| match component {
            Component::Normal(value) => value
                .to_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("non-UTF-8 segment path: {}", path.display())),
            _ => Err(format!("unsafe segment path: {}", path.display())),
        })
        .collect::<Result<Vec<_>, _>>()?
        .join("/");
    validate_relative_path(&encoded)?;
    Ok(encoded)
}

pub fn read_receipts(root: &Path) -> Result<Vec<ReceiptRecord>, String> {
    let mut records = Vec::new();
    for directory in partition_directories(root)? {
        let path = directory.join(RECEIPT_FILE);
        if !path.is_file() {
            continue;
        }
        let encoded =
            std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        let receipt: PartitionReceipt = serde_json::from_slice(&encoded)
            .map_err(|error| format!("invalid {}: {error}", path.display()))?;
        validate_receipt(&directory, &receipt)?;
        records.push(ReceiptRecord {
            path,
            digest: format!("{:x}", Sha256::digest(&encoded)),
            receipt,
        });
    }
    records.sort_by(|left, right| {
        left.receipt
            .partition_date
            .cmp(&right.receipt.partition_date)
    });
    if records
        .first()
        .is_some_and(|record| record.receipt.parent_receipt_sha256.is_some())
    {
        return Err("first ingest partition receipt claims a missing parent".into());
    }
    for pair in records.windows(2) {
        if pair[1].receipt.parent_receipt_sha256.as_deref() != Some(pair[0].digest.as_str()) {
            return Err(format!(
                "ingest partition receipt chain breaks between {} and {}",
                pair[0].path.display(),
                pair[1].path.display()
            ));
        }
        let expected = pair[0]
            .receipt
            .last_file_order_seq
            .map_or(pair[0].receipt.first_file_order_seq, |last| last + 1);
        if pair[1].receipt.first_file_order_seq != expected {
            return Err(format!(
                "file_order sequence breaks between {} and {}",
                pair[0].path.display(),
                pair[1].path.display()
            ));
        }
    }
    Ok(records)
}

fn validate_active(directory: &Path, active: &ActivePartition) -> Result<(), String> {
    if active.active_version != ACTIVE_VERSION {
        return Err(format!(
            "{} has unsupported active_version {}",
            directory.display(),
            active.active_version
        ));
    }
    if directory.file_name().and_then(|name| name.to_str())
        != Some(format!("date={}", active.partition_date).as_str())
    {
        return Err(format!(
            "{} disagrees with partition_date {}",
            directory.display(),
            active.partition_date
        ));
    }
    if active.first_file_order_seq <= 0 || active.identity_scope != IDENTITY_SCOPE {
        return Err(format!("invalid active marker in {}", directory.display()));
    }
    if let Some(digest) = &active.parent_receipt_sha256 {
        validate_sha256(digest)?;
    }
    active.initial_carry.restore()?;
    Ok(())
}

fn validate_receipt(directory: &Path, receipt: &PartitionReceipt) -> Result<(), String> {
    if receipt.ingest_partition_receipt_version != PARTITION_RECEIPT_VERSION {
        return Err(format!(
            "{} has unsupported ingest_partition_receipt_version {}",
            directory.display(),
            receipt.ingest_partition_receipt_version
        ));
    }
    if directory.file_name().and_then(|name| name.to_str())
        != Some(format!("date={}", receipt.partition_date).as_str())
        || receipt.database_file != DATABASE_FILE
        || receipt.database_byte_length == 0
        || receipt.identity_scope != IDENTITY_SCOPE
        || receipt.first_file_order_seq <= 0
        || receipt.closed_at_ns < receipt.opened_at_ns
    {
        return Err(format!(
            "invalid partition receipt in {}",
            directory.display()
        ));
    }
    validate_sha256(&receipt.database_sha256)?;
    let expected_last = if receipt.evidence_rows == 0 {
        None
    } else {
        Some(receipt.first_file_order_seq + receipt.evidence_rows as i64 - 1)
    };
    if receipt.last_file_order_seq != expected_last || receipt.fact_rows > receipt.evidence_rows {
        return Err(format!(
            "{} has inconsistent row counts or sequence bounds",
            directory.display()
        ));
    }
    if let Some(digest) = &receipt.parent_receipt_sha256 {
        validate_sha256(digest)?;
    }
    receipt.terminal_carry.restore()?;
    let mut previous = None;
    for segment in &receipt.segments {
        validate_relative_path(&segment.path)?;
        validate_sha256(&segment.source_sha256)?;
        if previous.as_ref().is_some_and(|path| path >= &segment.path) {
            return Err(format!(
                "{} segment records are not unique and sorted",
                directory.display()
            ));
        }
        previous = Some(segment.path.clone());
    }
    Ok(())
}

fn find_active(root: &Path) -> Result<Option<(PathBuf, ActivePartition)>, String> {
    let mut found = Vec::new();
    for directory in partition_directories(root)? {
        let path = directory.join(ACTIVE_FILE);
        if !path.is_file() {
            continue;
        }
        let encoded =
            std::fs::read(&path).map_err(|error| format!("reading {}: {error}", path.display()))?;
        let marker: ActivePartition = serde_json::from_slice(&encoded)
            .map_err(|error| format!("invalid {}: {error}", path.display()))?;
        found.push((directory, marker));
    }
    if found.len() > 1 {
        return Err("multiple active ingest partitions; refusing to guess which writer won".into());
    }
    Ok(found.pop())
}

fn recover_active_database(
    root: &Path,
    receipts: &[ReceiptRecord],
    now_ns: u64,
    marked: Option<(PathBuf, ActivePartition)>,
) -> Result<Option<(PathBuf, ActivePartition)>, String> {
    if let Some((directory, _)) = &marked {
        if directory.join(RECEIPT_FILE).exists() {
            return Err(format!(
                "{} has both an active marker and a close receipt",
                directory.display()
            ));
        }
    }

    let mut found = partition_directories(root)?
        .into_iter()
        .filter(|directory| {
            !directory.join(RECEIPT_FILE).exists()
                && (directory.join(ACTIVE_FILE).is_file()
                    || directory.join(OPEN_DATABASE_FILE).is_file()
                    || directory.join(DATABASE_FILE).is_file())
        })
        .collect::<Vec<_>>();
    if found.len() > 1 {
        return Err("multiple uncommitted ingest partitions; refusing crash recovery".into());
    }
    let Some(directory) = found.pop() else {
        return Ok(None);
    };

    if let Some((marked_directory, _)) = &marked {
        if marked_directory != &directory {
            return Err("active marker and uncommitted database name different partitions".into());
        }
    }
    let open_database = directory.join(OPEN_DATABASE_FILE);
    let closed_database = directory.join(DATABASE_FILE);
    if open_database.exists() && closed_database.exists() {
        return Err(format!(
            "{} contains both open and closed ingest databases",
            directory.display()
        ));
    }
    if closed_database.exists() {
        std::fs::rename(&closed_database, &open_database).map_err(|error| {
            format!(
                "recovering {} as {}: {error}",
                closed_database.display(),
                open_database.display()
            )
        })?;
        fsync_directory(&directory)?;
    }

    if let Some((_, marker)) = marked {
        return Ok(Some((directory, marker)));
    }
    let partition_date = directory
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("date="))
        .ok_or_else(|| format!("invalid partition directory {}", directory.display()))?
        .to_owned();
    let (first, parent, carry) = seed_from(receipts.last());
    let marker = ActivePartition {
        active_version: ACTIVE_VERSION,
        partition_date,
        opened_at_ns: now_ns,
        first_file_order_seq: first,
        parent_receipt_sha256: parent,
        initial_carry: carry,
        identity_scope: IDENTITY_SCOPE.to_owned(),
    };
    write_json_durable(&directory, ACTIVE_FILE, &marker)?;
    Ok(Some((directory, marker)))
}

fn create_active(
    root: &Path,
    date: &str,
    now_ns: u64,
    previous: Option<&ReceiptRecord>,
) -> Result<(PathBuf, ActivePartition), String> {
    let directory = partition_directory(root, date);
    if directory.join(RECEIPT_FILE).exists() {
        return Err(format!(
            "cannot reopen committed ingest partition {}",
            directory.display()
        ));
    }
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("creating {}: {error}", directory.display()))?;
    fsync_directory(root)?;
    let (first, parent, carry) = seed_from(previous);
    let marker = ActivePartition {
        active_version: ACTIVE_VERSION,
        partition_date: date.to_owned(),
        opened_at_ns: now_ns,
        first_file_order_seq: first,
        parent_receipt_sha256: parent,
        initial_carry: carry,
        identity_scope: IDENTITY_SCOPE.to_owned(),
    };
    write_json_durable(&directory, ACTIVE_FILE, &marker)?;
    Ok((directory, marker))
}

fn seed_from(previous: Option<&ReceiptRecord>) -> (i64, Option<String>, IngestCarry) {
    previous.map_or((1, None, IngestCarry::default()), |record| {
        (
            record
                .receipt
                .last_file_order_seq
                .map_or(record.receipt.first_file_order_seq, |last| last + 1),
            Some(record.digest.clone()),
            record.receipt.terminal_carry.clone(),
        )
    })
}

fn relocate_legacy_store(root: &Path, date: &str, now_ns: u64) -> Result<(), String> {
    let source = root.join(DATABASE_FILE);
    if !source.exists() {
        return Ok(());
    }
    if find_active(root)?.is_some() || !read_receipts(root)?.is_empty() {
        return Err(format!(
            "legacy {} exists beside partitioned state; refusing to merge histories",
            source.display()
        ));
    }
    let directory = partition_directory(root, date);
    std::fs::create_dir_all(&directory)
        .map_err(|error| format!("creating {}: {error}", directory.display()))?;
    for suffix in ["", "-wal", "-shm"] {
        let from = root.join(format!("{DATABASE_FILE}{suffix}"));
        if !from.exists() {
            continue;
        }
        let to = directory.join(format!("{OPEN_DATABASE_FILE}{suffix}"));
        std::fs::rename(&from, &to)
            .map_err(|error| format!("moving {} to {}: {error}", from.display(), to.display()))?;
    }
    fsync_directory(root)?;
    fsync_directory(&directory)?;
    let marker = ActivePartition {
        active_version: ACTIVE_VERSION,
        partition_date: date.to_owned(),
        opened_at_ns: now_ns,
        first_file_order_seq: 1,
        parent_receipt_sha256: None,
        initial_carry: IngestCarry::default(),
        identity_scope: IDENTITY_SCOPE.to_owned(),
    };
    write_json_durable(&directory, ACTIVE_FILE, &marker)
}

pub(crate) fn partition_directories(root: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = std::fs::read_dir(root)
        .map_err(|error| format!("reading ingest store {}: {error}", root.display()))?;
    let mut found = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|error| format!("reading ingest store {}: {error}", root.display()))?
            .path();
        if path.is_dir()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("date="))
        {
            found.push(path);
        }
    }
    found.sort();
    Ok(found)
}

fn partition_directory(root: &Path, date: &str) -> PathBuf {
    root.join(format!("date={date}"))
}

fn validate_relative_path(path: &str) -> Result<(), String> {
    let parsed = Path::new(path);
    if path.is_empty()
        || parsed.is_absolute()
        || parsed.components().any(|component| {
            !matches!(component, Component::Normal(_))
                || component.as_os_str().to_string_lossy().contains('\\')
        })
        || !path.ends_with(".ndjson")
    {
        return Err(format!("invalid consumed segment path {path:?}"));
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!("invalid lowercase sha256 {value:?}"));
    }
    Ok(())
}

fn write_json_durable<T: Serialize>(directory: &Path, name: &str, value: &T) -> Result<(), String> {
    write_json_file_durable(&directory.join(name), value)
}

pub(crate) fn write_json_file_durable<T: Serialize>(
    final_path: &Path,
    value: &T,
) -> Result<(), String> {
    let directory = final_path
        .parent()
        .ok_or_else(|| format!("{} has no containing directory", final_path.display()))?;
    std::fs::create_dir_all(directory)
        .map_err(|error| format!("creating {}: {error}", directory.display()))?;
    let name = final_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid JSON output path {}", final_path.display()))?;
    let mut encoded =
        serde_json::to_vec_pretty(value).map_err(|error| format!("encoding {name}: {error}"))?;
    encoded.push(b'\n');
    let temporary = directory.join(format!("{name}.{}.open", std::process::id()));
    {
        let mut file = std::fs::File::create(&temporary)
            .map_err(|error| format!("creating {}: {error}", temporary.display()))?;
        file.write_all(&encoded)
            .map_err(|error| format!("writing {}: {error}", temporary.display()))?;
        file.sync_all()
            .map_err(|error| format!("fsyncing {}: {error}", temporary.display()))?;
    }
    std::fs::rename(&temporary, final_path)
        .map_err(|error| format!("renaming {}: {error}", temporary.display()))?;
    fsync_directory(directory)
}

pub(crate) fn remove_durable(path: &Path) -> Result<(), String> {
    std::fs::remove_file(path).map_err(|error| format!("removing {}: {error}", path.display()))?;
    fsync_directory(
        path.parent()
            .ok_or_else(|| format!("{} has no containing directory", path.display()))?,
    )
}

pub fn fsync_directory(directory: &Path) -> Result<(), String> {
    std::fs::File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| format!("fsyncing directory {}: {error}", directory.display()))
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes =
        std::fs::read(path).map_err(|error| format!("reading {}: {error}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(crate) fn sha256_file_streaming(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("opening {}: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("reading {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
