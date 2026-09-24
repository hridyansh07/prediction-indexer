use canonical_normalizer::{
    ConfigValue, Normalization, Normalize, NormalizerConfigIdentity, NormalizerDescriptor,
    NormalizerError,
};
use indexer_finalize::{
    CanonicalOutput, CompressionContract, DecodedIdentity, InputSegment, Receipt, StoredIdentity,
    window_directory,
};
use indexer_types::{ContentHash, Sha256};
use prediction_encoder::{encode_stream, encoder_version};
use replay_domain::*;
use replay_materialize::{DerivativeSpec, NormalizationPolicy, build_window};
use replay_normalizers::{CanonicalNormalizerIdentity, VenueNormalizerIdentity};
use replay_tape::PinnedDerivative;
use serde_json::json;
use std::{collections::BTreeMap, fs, io::Cursor, path::Path};
use tempdir::TempDir;

pub struct Row {
    pub lane: &'static str,
    pub time: u64,
    pub epoch: &'static str,
    pub continuity: &'static str,
    pub result: Normalization,
}
pub fn row(lane: &'static str, time: u64, events: Vec<SegmentEvent>) -> Row {
    Row {
        lane,
        time,
        epoch: "e1",
        continuity: "continuous",
        result: Normalization::Events(events),
    }
}
pub fn ignored(lane: &'static str, time: u64) -> Row {
    Row {
        result: Normalization::Ignored {
            reason_code: "heartbeat".into(),
        },
        ..row(lane, time, vec![])
    }
}
pub struct Fixture {
    _source: TempDir,
    _out: TempDir,
    pub pin: PinnedDerivative,
}
impl Fixture {
    pub fn new(
        start: u64,
        end: u64,
        first: i64,
        rows: Vec<Row>,
        amend: impl FnOnce(&mut Receipt),
    ) -> Self {
        let source = TempDir::new("risk-source").unwrap();
        let out = TempDir::new("risk-derivative").unwrap();
        let directory = window_directory(source.path(), start);
        fs::create_dir_all(&directory).unwrap();
        let mut evidence = vec![];
        let mut provenance = vec![];
        let mut lanes = BTreeMap::<&str, u64>::new();
        for (i, row) in rows.iter().enumerate() {
            let seq = first + i as i64;
            let local = lanes.entry(row.lane).or_default();
            *local += 1;
            let payload = i.to_string();
            let id = format!("{}-{seq}", row.lane);
            let envelope = json!({"envelope_version":2,"delivery_index":first as u64 + *local - 1,"record_id":id,"visible_ns":row.time,"monotonic_ns":row.time,"venue":"kalshi","stream":"public_book","connection_epoch":row.epoch,"local_counter":local,"source_cursor":{"type":"unsequenced","counter":local},"kind":"venue_frame","raw_payload":payload});
            evidence.extend_from_slice(format!("{envelope}\n").as_bytes());
            let tie = rows
                .iter()
                .any(|r| r.time == row.time && r.lane != row.lane)
                .then_some(row.time);
            provenance.extend_from_slice(format!("{}\n",json!({"canonical_seq":seq,"lane_id":row.lane,"source_segment_sha256":Sha256::digest(row.lane.as_bytes()).as_hex(),"source_line_number":local,"record_id":id,"content_hash":ContentHash::hash(payload.as_bytes()).to_hex(),"continuity_verdict":row.continuity,"visible_tie_group":tie})).as_bytes());
        }
        let names = lanes.keys().map(|s| s.to_string()).collect::<Vec<_>>();
        let inputs = lanes
            .into_iter()
            .map(|(lane, count)| InputSegment {
                lane: lane.into(),
                data_file: "source.ndjson".into(),
                segment_index: 0,
                line_count: count,
                sha256: Sha256::digest(lane.as_bytes()).as_hex(),
                first_delivery_index: Some(first as u64),
                last_delivery_index: Some(first as u64 + count - 1),
            })
            .collect();
        let mut receipt = Receipt {
            receipt_version: 1,
            window_start_ns: start,
            window_end_ns: end,
            completeness: "complete".into(),
            certified: true,
            expected_lanes: names.clone(),
            present_lanes: names,
            unexpected_lanes: vec![],
            missing_lanes: vec![],
            invalid_lanes: vec![],
            finalization_deadline_seconds: 300,
            deadline_expired: false,
            finalized_at_ns: end,
            inputs,
            evidence: output(&directory, "evidence.ndjson.zst", &evidence),
            provenance: output(&directory, "provenance.ndjson.zst", &provenance),
            first_canonical_seq: (!rows.is_empty()).then_some(first),
            last_canonical_seq: (!rows.is_empty()).then_some(first + rows.len() as i64 - 1),
            carried: Default::default(),
            clock_faults: vec![],
            finalizer_version: 1,
        };
        amend(&mut receipt);
        fs::write(
            directory.join("receipt.json"),
            format!("{}\n", serde_json::to_string_pretty(&receipt).unwrap()),
        )
        .unwrap();
        let mut normalizer = Script {
            descriptor: test_normalizer_identity().descriptor().unwrap(),
            rows: rows.into_iter().map(|r| Some(r.result)).collect(),
        };
        let spec = DerivativeSpec {
            normalized_schema_version: SEGMENT_SCHEMA_VERSION,
            normalizer_bundle_sha256: normalizer.descriptor.bundle_sha256,
            normalizer_config_sha256: normalizer.descriptor.config_sha256,
            policy: NormalizationPolicy {
                policy_sha256: Sha256::digest(b"policy"),
                effective_from_ns: 0,
                effective_until_ns: None,
            },
        };
        let built = build_window(
            source.path(),
            out.path(),
            start,
            end,
            &spec,
            &mut normalizer,
        )
        .unwrap();
        let pin = PinnedDerivative {
            directory: built.derivative.directory,
            pin: built.derivative.pin,
        };
        Self {
            _source: source,
            _out: out,
            pin,
        }
    }
}

pub fn test_normalizer_identity() -> CanonicalNormalizerIdentity {
    let scales = || {
        BTreeMap::from([
            ("price_scale".to_owned(), ConfigValue::Unsigned(2)),
            ("quantity_scale".to_owned(), ConfigValue::Unsigned(0)),
        ])
    };
    CanonicalNormalizerIdentity {
        identity_version: 1,
        venues: vec![
            VenueNormalizerIdentity {
                venue: "kalshi".into(),
                bundle_id: "risk-test-kalshi".into(),
                parser_version: 1,
                config: NormalizerConfigIdentity {
                    schema_version: 2,
                    variables: scales(),
                },
            },
            VenueNormalizerIdentity {
                venue: "limitless".into(),
                bundle_id: "risk-test-limitless".into(),
                parser_version: 1,
                config: NormalizerConfigIdentity {
                    schema_version: 1,
                    variables: scales(),
                },
            },
            VenueNormalizerIdentity {
                venue: "polymarket".into(),
                bundle_id: "risk-test-polymarket".into(),
                parser_version: 1,
                config: NormalizerConfigIdentity {
                    schema_version: 1,
                    variables: BTreeMap::from([
                        (
                            "accept_additive_fields".to_owned(),
                            ConfigValue::Boolean(true),
                        ),
                        ("price_scale".to_owned(), ConfigValue::Unsigned(2)),
                        ("quantity_scale".to_owned(), ConfigValue::Unsigned(0)),
                    ]),
                },
            },
        ],
    }
}
struct Script {
    descriptor: NormalizerDescriptor,
    rows: Vec<Option<Normalization>>,
}
impl Normalize for Script {
    fn descriptor(&self) -> &NormalizerDescriptor {
        &self.descriptor
    }
    fn normalize(
        &mut self,
        source: &indexer_finalize::JoinedCanonicalRecord,
    ) -> Result<Normalization, NormalizerError> {
        let envelope = indexer_types::EnvelopeView::parse(&source.envelope).unwrap();
        Ok(self.rows[envelope.raw_payload.parse::<usize>().unwrap()]
            .take()
            .unwrap())
    }
    fn finish(&mut self) -> Result<(), NormalizerError> {
        Ok(())
    }
}
fn output(directory: &Path, name: &str, bytes: &[u8]) -> CanonicalOutput {
    let r = encode_stream(
        Cursor::new(bytes),
        fs::File::create(directory.join(name)).unwrap(),
        3,
    )
    .unwrap();
    CanonicalOutput {
        file: name.into(),
        content_encoding: "zstd".into(),
        decoded: DecodedIdentity {
            sha256: r.logical.sha256,
            byte_length: r.logical.byte_length,
            line_count: r.logical.line_count,
        },
        stored: StoredIdentity {
            sha256: r.stored.sha256,
            byte_length: r.stored.byte_length,
        },
        compression: CompressionContract {
            algorithm: "zstd".into(),
            level: 3,
            frame_checksum: true,
            dictionary: None,
            frame_count: 1,
            encoder: encoder_version(),
        },
    }
}
pub fn scale(n: u8) -> DecimalScale {
    DecimalScale::new(n).unwrap()
}
pub fn id(name: &str) -> InstrumentId {
    InstrumentId::new(name).unwrap()
}
pub fn key(name: &str) -> BookKey {
    BookKey {
        instrument: id(name),
        orientation: ContractOrientation::Outcome,
    }
}
pub fn qty(atoms: u64) -> PositiveQty {
    PositiveQty::new(Qty::from_atoms(atoms, scale(0)).unwrap()).unwrap()
}
pub fn full(name: &str, bids: &[(i64, u64)], asks: &[(i64, u64)]) -> SegmentEvent {
    let levels = |values: &[(i64, u64)]| {
        values
            .iter()
            .map(|&(p, q)| {
                Level::new(
                    ConditionalMarketPrice::from_atoms(p, scale(2)).unwrap(),
                    qty(q),
                )
            })
            .collect()
    };
    SegmentEvent::Book(BookEvent::Full(
        FullBook::new(
            id(name),
            ContractOrientation::Outcome,
            levels(bids),
            levels(asks),
            None,
            None,
        )
        .unwrap(),
    ))
}
pub fn delta(name: &str, price: i64, change: LevelChange) -> SegmentEvent {
    SegmentEvent::Book(BookEvent::Delta(
        BookDelta::new(
            id(name),
            ContractOrientation::Outcome,
            Side::Bid,
            ConditionalMarketPrice::from_atoms(price, scale(2)).unwrap(),
            change,
            None,
        )
        .unwrap(),
    ))
}
pub fn plan(name: &str, lane: &str) -> replay_risk::BookPlan {
    replay_risk::BookPlan {
        key: key(name),
        lane: LaneId::new(lane).unwrap(),
        venue: name.split_once(':').unwrap().0.into(),
        price_scale: scale(2),
        quantity_scale: scale(0),
    }
}
