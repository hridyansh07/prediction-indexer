#[allow(dead_code)]
#[path = "../../risk/tests/support/mod.rs"]
mod support;
use replay_domain::*;
use replay_risk::{RiskEngine, RiskLimits};
use replay_tape::LowerBoundPolicy;
use replay_transport::{Config, Input, Plan, wire};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use support::*;

#[test]
fn cli_config_io_is_nonretryable() {
    let dir = tempdir::TempDir::new("publish-config").unwrap();
    for path in [dir.path().join("absent.json"), dir.path().to_path_buf()] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_replay-publish"))
            .arg(path)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(20));
    }
}

fn fixture() -> Fixture {
    let trade = |q| {
        SegmentEvent::Trade(TradeEvent::new(
            id("kalshi:A"),
            ContractOrientation::Outcome,
            ConditionalMarketPrice::from_atoms(37, scale(2)).unwrap(),
            qty(q),
            Some(Side::Ask),
        ))
    };
    Fixture::new(
        0,
        100,
        1,
        vec![
            row(
                "x",
                1,
                vec![
                    full("kalshi:A", &[(17, 3), (37, 11)], &[(83, 2)]),
                    full("kalshi:B", &[(21, 6)], &[]),
                ],
            ),
            row(
                "x",
                2,
                vec![
                    delta("kalshi:A", 37, LevelChange::Increase(qty(3))),
                    trade(5),
                    delta("kalshi:A", 37, LevelChange::Decrease(qty(7))),
                    trade(9),
                ],
            ),
            row(
                "y",
                2,
                vec![full("polymarket:T", &[], &[(81, 9007199254740993)])],
            ),
            Row {
                continuity: "duplicate",
                ..row(
                    "x",
                    3,
                    vec![
                        delta("kalshi:A", 37, LevelChange::Increase(qty(99))),
                        trade(7),
                    ],
                )
            },
            row(
                "x",
                4,
                vec![
                    delta("kalshi:A", 37, LevelChange::Decrease(qty(8))),
                    delta("kalshi:B", 21, LevelChange::Set(qty(19))),
                ],
            ),
            row(
                "x",
                5,
                vec![
                    full("kalshi:A", &[], &[]),
                    delta("kalshi:A", 12, LevelChange::Increase(qty(4))),
                ],
            ),
            row(
                "x",
                6,
                vec![SegmentEvent::Control(ControlEvent::MetadataChanged {
                    from: Some("before".into()),
                    to: "after".into(),
                })],
            ),
        ],
        |_| {},
    )
}
fn config(f: &Fixture) -> Config {
    Config {
        run_id: "contract".into(),
        attempt_id: "golden".into(),
        scope: "test".into(),
        normalizer: test_normalizer_identity(),
        inputs: vec![Input {
            directory: f.pin.directory.clone(),
            derivative_address: f.pin.pin.derivative_address.clone(),
            receipt_sha256: f.pin.pin.receipt_sha256,
        }],
        start_ns: "0".into(),
        end_ns: "100".into(),
        lower_bound: "clip".into(),
        plans: [("kalshi:A", "x"), ("kalshi:B", "x"), ("polymarket:T", "y")]
            .into_iter()
            .map(|(name, lane)| Plan {
                instrument: id(name),
                orientation: ContractOrientation::Outcome,
                lane: LaneId::new(lane).unwrap(),
                venue: name.split_once(':').unwrap().0.into(),
                price_scale: "2".into(),
                quantity_scale: "0".into(),
            })
            .collect(),
        groups: vec!["fast".into(), "slow".into()],
        command_timeout_ms: 1000,
        max_entry_bytes: 65536,
        max_queue_bytes: 1_048_576,
    }
}
fn records(f: &Fixture, c: &Config) -> Vec<Value> {
    let mut engine = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![
            plan("kalshi:A", "x"),
            plan("kalshi:B", "x"),
            plan("polymarket:T", "y"),
        ],
        RiskLimits::default(),
    )
    .unwrap();
    let wrap = |seq: u64, kind: &str, body: Value| json!({"version":"1","run_id":c.run_id,"attempt_id":c.attempt_id,"sequence":seq.to_string(),"kind":kind,"body":body});
    let mut output = vec![wrap(0, "initial", c.initial())];
    while let Some(cut) = engine.next_cut().unwrap() {
        output.push(wrap(cut.sequence(), "cut", wire::cut(&cut)));
    }
    let books = wire::books_sha256(&engine);
    let done = engine.finish().unwrap();
    output.push(wrap(
        done.cuts() + 1,
        "terminal",
        wire::terminal(done.cuts(), books),
    ));
    output
}

/// Re-encodes the fixture's events with only the final line changed, rebinding
/// both identities in manifest, receipt and pin so every hash check passes.
fn corrupt_last_event(pin: &mut replay_materialize::PinnedDerivative) {
    use std::io::Read;
    let directory = pin.directory.clone();
    let mut manifest: replay_materialize::DerivativeManifest =
        serde_json::from_slice(&std::fs::read(directory.join("manifest.json")).unwrap()).unwrap();
    let mut receipt: replay_materialize::DerivativeReceipt =
        serde_json::from_slice(&std::fs::read(directory.join("receipt.json")).unwrap()).unwrap();
    let logical = prediction_encoder::LogicalIdentity {
        sha256: manifest.events.logical.sha256.as_hex(),
        byte_length: manifest.events.logical.byte_length,
        line_count: manifest.events.logical.line_count,
    };
    let mut decoder = prediction_encoder::StreamingDecoder::new(
        std::fs::File::open(directory.join("events.ndjson.zst")).unwrap(),
        &logical,
        None,
        Some(logical.byte_length),
    )
    .unwrap();
    let mut text = String::new();
    decoder.read_to_string(&mut text).unwrap();
    decoder.finish().unwrap();
    let start = text[..text.len() - 1].rfind('\n').unwrap() + 1;
    let last = text[start..].replacen("\"event_index\":0", "\"event_index\":1", 1);
    assert_ne!(last, text[start..]);
    text.replace_range(start.., &last);
    let mut stored = Vec::new();
    let result =
        prediction_encoder::encode_stream(std::io::Cursor::new(text), &mut stored, 3).unwrap();
    std::fs::write(directory.join("events.ndjson.zst"), &stored).unwrap();
    manifest.events.logical = replay_materialize::LogicalIdentity {
        sha256: indexer_types::Sha256::from_hex(&result.logical.sha256).unwrap(),
        byte_length: result.logical.byte_length,
        line_count: result.logical.line_count,
    };
    manifest.events.stored = replay_materialize::StoredIdentity {
        sha256: indexer_types::Sha256::digest(&stored),
        byte_length: stored.len() as u64,
    };
    let bytes = format!("{}\n", serde_json::to_string(&manifest).unwrap()).into_bytes();
    receipt.events = manifest.events.clone();
    receipt.manifest.sha256 = indexer_types::Sha256::digest(&bytes);
    receipt.manifest.byte_length = bytes.len() as u64;
    std::fs::write(directory.join("manifest.json"), bytes).unwrap();
    let bytes = format!("{}\n", serde_json::to_string(&receipt).unwrap()).into_bytes();
    pin.pin.receipt_sha256 = indexer_types::Sha256::digest(&bytes);
    std::fs::write(directory.join("receipt.json"), bytes).unwrap();
}

#[test]
fn late_corruption_streams_cuts_but_never_reaches_a_terminal() {
    // Pinned reads verify while they stream, so cuts from a window can precede
    // its failure. The publisher mints its terminal record only from
    // `RiskEngine::finish`, which this attempt can never reach.
    let mut f = fixture();
    corrupt_last_event(&mut f.pin);
    let mut engine = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![
            plan("kalshi:A", "x"),
            plan("kalshi:B", "x"),
            plan("polymarket:T", "y"),
        ],
        RiskLimits::default(),
    )
    .unwrap();
    let mut cuts = 0;
    let error = loop {
        match engine.next_cut() {
            Ok(Some(_)) => cuts += 1,
            Ok(None) => panic!("corrupt window reached clean EOF"),
            Err(error) => break error,
        }
    };
    assert_eq!(error, "normalized events are not in canonical child order");
    assert!(
        cuts > 0,
        "the window status and earlier groups stream first"
    );
    assert_eq!(engine.next_cut().unwrap_err(), "risk attempt is poisoned");
    assert!(engine.finish().is_err());
}

fn invalid_preflight_configs(f: &Fixture) -> (Vec<(&'static str, Config)>, tempdir::TempDir) {
    let mut cases = Vec::new();

    let mut descriptor = config(f);
    descriptor.normalizer.venues[0]
        .bundle_id
        .push_str("-changed");
    cases.push(("bundle", descriptor));

    let mut config_hash = config(f);
    config_hash.normalizer.venues[2].config.variables.insert(
        "accept_additive_fields".into(),
        canonical_normalizer::ConfigValue::Boolean(false),
    );
    cases.push(("config", config_hash));

    let mut missing = config(f);
    missing.plans[0].venue = "unknown".into();
    cases.push(("venue", missing));

    let mut scale = config(f);
    scale.plans[0].price_scale = "3".into();
    cases.push(("scale", scale));

    let mut profile = config(f);
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../materialize/tests/fixtures/profile1");
    let profile_root = tempdir::TempDir::new("transport-profile1").unwrap();
    let directory = profile_root
        .path()
        .join("5b2a4358be376144d898f0c0671b25c23dc2f8de946d16cc049b3b9f92685ea4");
    std::fs::create_dir(&directory).unwrap();
    for name in [
        "receipt.json",
        "manifest.json",
        "events.ndjson.zst",
        "rejects.ndjson.zst",
    ] {
        std::fs::copy(source.join(name), directory.join(name)).unwrap();
    }
    let receipt = std::fs::read(directory.join("receipt.json")).unwrap();
    profile.inputs = vec![Input {
        directory,
        derivative_address: "5b2a4358be376144d898f0c0671b25c23dc2f8de946d16cc049b3b9f92685ea4"
            .into(),
        receipt_sha256: indexer_types::Sha256::digest(&receipt),
    }];
    cases.push(("profile", profile));
    (cases, profile_root)
}

#[test]
fn publisher_preflight_binds_descriptor_config_venues_scales_and_profile() {
    let f = fixture();
    config(&f).validate().unwrap();
    let (cases, _profile_root) = invalid_preflight_configs(&f);
    for (name, invalid) in cases {
        let error = invalid.validate().unwrap_err().to_string();
        assert!(error.contains(name), "{name}: {error}");
    }
}

#[test]
fn golden_wire_from_real_materializer_walker_and_risk() {
    let f = fixture();
    let c = config(&f);
    let values = records(&f, &c);
    let mut bytes = String::new();
    for value in &values {
        bytes.push_str(&serde_json::to_string(value).unwrap());
        bytes.push('\n');
    }
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/contract.ndjson");
    if std::env::var_os("REPLAY_UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(std::fs::read_to_string(path).unwrap(), bytes);
    let operations = &values[3]["body"]["book_transitions"][0]["decision"];
    assert_eq!(operations["kind"], "operations");
    assert!(operations.get("bids").is_none());
    assert_eq!(
        values[3]["body"]["market_events"].as_array().unwrap().len(),
        5
    );
    assert!(values[6]["body"].get("control_events").is_none());
    assert_eq!(
        values[7]["body"]["control_events"][0]["event"],
        json!({"kind":"metadata_changed","from":"before","to":"after"})
    );
}

#[test]
fn metadata_change_is_a_noninvalidating_wire_observation() {
    let f = Fixture::new(
        0,
        100,
        1,
        vec![
            row("x", 1, vec![full("kalshi:A", &[(37, 11)], &[])]),
            row(
                "x",
                2,
                vec![SegmentEvent::Control(ControlEvent::MetadataChanged {
                    from: Some("old".into()),
                    to: "new".into(),
                })],
            ),
        ],
        |_| {},
    );
    let mut engine = RiskEngine::open(
        vec![f.pin.clone()],
        0,
        100,
        LowerBoundPolicy::Clip,
        vec![plan("kalshi:A", "x")],
        RiskLimits::default(),
    )
    .unwrap();
    engine.next_cut().unwrap();
    engine.next_cut().unwrap();
    let cut = engine.next_cut().unwrap().unwrap();
    let encoded = wire::cut(&cut);

    assert!(encoded["book_transitions"].as_array().unwrap().is_empty());
    assert!(encoded["market_events"].as_array().unwrap().is_empty());
    assert_eq!(
        encoded["control_events"][0]["event"],
        json!({"kind":"metadata_changed","from":"old","to":"new"})
    );
    assert_eq!(
        encoded["control_events"][0]["reference"]["address"]["event_index"],
        "0"
    );
}

#[test]
#[ignore = "requires explicitly supplied disposable Redis >=8.2"]
fn invalid_preflight_never_creates_redis_keys() {
    use replay_transport::Publisher;

    let url = std::env::var("REPLAY_REDIS_URL").expect("disposable REPLAY_REDIS_URL required");
    let f = fixture();
    let mut admin = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    let (cases, _profile_root) = invalid_preflight_configs(&f);
    for (index, (name, mut invalid)) in cases.into_iter().enumerate() {
        invalid.attempt_id = format!("preflight-{index}-{}", std::process::id());
        let keys = invalid.keys();
        let _: () = redis::cmd("DEL").arg(&keys).query(&mut admin).unwrap();
        assert!(
            Publisher::open(&url, invalid, RiskLimits::default()).is_err(),
            "{name}"
        );
        let count: u64 = redis::cmd("EXISTS").arg(&keys).query(&mut admin).unwrap();
        assert_eq!(count, 0, "{name}");
    }
}

#[test]
#[ignore = "requires explicitly supplied disposable Redis >=8.2 and Python SDK"]
fn redis_cli_python_end_to_end() {
    let url = std::env::var("REPLAY_REDIS_URL").expect("disposable REPLAY_REDIS_URL required");
    let f = fixture();
    let mut c = config(&f);
    c.attempt_id = format!("e2e-{}", std::process::id());
    let scratch = tempdir::TempDir::new("replay-transport").unwrap();
    let config_path = scratch.path().join("config.json");
    let initial_path = scratch.path().join("initial.json");
    std::fs::write(&config_path, serde_json::to_vec(&c).unwrap()).unwrap();
    std::fs::write(&initial_path, serde_json::to_vec(&c.initial()).unwrap()).unwrap();
    assert!(
        std::process::Command::new(env!("CARGO_BIN_EXE_replay-publish"))
            .arg(&config_path)
            .env("REDIS_URL", &url)
            .status()
            .unwrap()
            .success()
    );
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    assert!(
        std::process::Command::new(root.join(".venv/bin/python"))
            .current_dir(&root)
            .args(["-m", "replay.tests.test_streams_redis", "--existing"])
            .arg(&initial_path)
            .arg(&c.attempt_id)
            .env("REPLAY_REDIS_URL", &url)
            .status()
            .unwrap()
            .success()
    );
}

#[test]
#[ignore = "flushes script cache on explicitly disposable Redis; run serially"]
fn redis_script_cache_fallback_and_ready_path_failure() {
    use replay_transport::{Error, Publisher};
    let url = std::env::var("REPLAY_REDIS_URL").unwrap();
    let mut admin = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    let calls = |admin: &mut redis::Connection| -> u64 {
        let info: String = redis::cmd("INFO").arg("commandstats").query(admin).unwrap();
        info.lines()
            .find_map(|line| line.strip_prefix("cmdstat_eval:calls="))
            .map(|s| s.split(',').next().unwrap().parse().unwrap())
            .unwrap_or(0)
    };
    let f = fixture();
    let mut c = config(&f);
    c.attempt_id = format!("cache-{}", std::process::id());
    let keys = c.keys();
    let _: () = redis::cmd("SCRIPT").arg("FLUSH").query(&mut admin).unwrap();
    let before = calls(&mut admin);
    let mut publisher = Publisher::open(&url, c.clone(), RiskLimits::default()).unwrap();
    assert_eq!(calls(&mut admin), before + 1); // setup loads; initial is cached
    publisher.progress().unwrap();
    assert_eq!(calls(&mut admin), before + 1);
    let _: () = redis::cmd("SCRIPT").arg("FLUSH").query(&mut admin).unwrap();
    publisher.progress().unwrap();
    assert_eq!(calls(&mut admin), before + 2);
    let _: i64 = redis::cmd("XGROUP")
        .arg("DESTROY")
        .arg(&keys[0])
        .arg("slow")
        .query(&mut admin)
        .unwrap();
    // Fixed membership is verified at join/check, not on every publish.
    assert!(publisher.step().unwrap());
    assert!(matches!(publisher.progress(), Err(Error::Protocol(_))));
    assert_eq!(calls(&mut admin), before + 2); // no fallback on an executed error
    assert!(matches!(publisher.step(), Err(Error::Poisoned)));
    let _: () = redis::cmd("DEL").arg(&keys).query(&mut admin).unwrap();

    c.attempt_id = format!("ready-{}", std::process::id());
    let tmp = tempdir::TempDir::new("ready-collision").unwrap();
    let path = tmp.path().join("config.json");
    let ready = tmp.path().join("ready");
    std::fs::write(&path, serde_json::to_vec(&c).unwrap()).unwrap();
    std::fs::write(&ready, b"existing").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_replay-publish"))
        .arg(path)
        .arg(&ready)
        .env("REDIS_URL", &url)
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(20));
    assert_eq!(std::fs::read(ready).unwrap(), b"existing");
    let _: () = redis::cmd("DEL").arg(&c.keys()).query(&mut admin).unwrap();
}

#[test]
#[ignore = "mutates maxmemory and pauses explicitly disposable Redis; run with --test-threads=1"]
fn redis_publisher_resource_timeout_oom_and_no_terminal_after_failure() {
    use replay_transport::{Error, Publisher};
    let url = std::env::var("REPLAY_REDIS_URL").unwrap();
    let f = fixture();
    let mut admin = redis::Client::open(url.as_str())
        .unwrap()
        .get_connection()
        .unwrap();
    for failure in ["queue", "timeout", "oom", "risk"] {
        let mut c = config(&f);
        c.attempt_id = format!("{}-{}", failure, std::process::id());
        c.command_timeout_ms = 50;
        let keys = c.keys();
        let limits = RiskLimits {
            max_levels_per_book: if failure == "risk" { 1 } else { 100_000 },
            ..RiskLimits::default()
        };
        let mut publisher = Publisher::open(&url, c, limits).unwrap();
        if failure == "queue" {
            let _: () = redis::cmd("HSET")
                .arg(&keys[1])
                .arg("limit")
                .arg(1)
                .query(&mut admin)
                .unwrap();
        }
        let settings: BTreeMap<String, String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory")
            .query(&mut admin)
            .unwrap();
        if failure == "timeout" {
            let _: () = redis::cmd("CLIENT")
                .arg("PAUSE")
                .arg(200)
                .arg("ALL")
                .query(&mut admin)
                .unwrap();
        }
        if failure == "oom" {
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg("maxmemory")
                .arg(1)
                .query(&mut admin)
                .unwrap();
        }
        let result = if failure == "risk" {
            assert!(publisher.step().unwrap());
            publisher.step()
        } else {
            publisher.step()
        };
        if failure == "oom" {
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg("maxmemory")
                .arg(&settings["maxmemory"])
                .query(&mut admin)
                .unwrap();
        }
        assert!(
            match failure {
                "oom" | "queue" => matches!(result, Err(Error::Resource)),
                "timeout" => matches!(result, Err(Error::Transport)),
                "risk" => matches!(result, Err(Error::Risk(_))),
                _ => unreachable!(),
            },
            "{failure}: {result:?}"
        );
        assert!(matches!(publisher.step(), Err(Error::Poisoned)));
        std::thread::sleep(std::time::Duration::from_millis(250));
        let terminal: String = redis::cmd("HGET")
            .arg(&keys[1])
            .arg("terminal")
            .query(&mut admin)
            .unwrap();
        assert!(terminal.is_empty());
        let _: () = redis::cmd("DEL").arg(&keys).query(&mut admin).unwrap();
    }
}

#[test]
#[ignore = "requires explicitly supplied disposable Redis >=8.2 and Python SDK"]
fn redis_supervisor_retries_entire_attempt() {
    let url = std::env::var("REPLAY_REDIS_URL").expect("disposable REPLAY_REDIS_URL required");
    let f = fixture();
    let c = config(&f);
    let scratch = tempdir::TempDir::new("replay-supervisor").unwrap();
    let path = scratch.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&c).unwrap()).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    assert!(
        std::process::Command::new(root.join(".venv/bin/python"))
            .current_dir(&root)
            .args(["-m", "replay.tests.test_supervisor", "--real-publisher"])
            .arg(path)
            .arg(env!("CARGO_BIN_EXE_replay-publish"))
            .env("REPLAY_REDIS_URL", url)
            .status()
            .unwrap()
            .success()
    );
}

#[test]
#[ignore = "requires explicitly supplied disposable Redis >=8.2 and Python SDK"]
fn redis_bundle_coverage_acceptance() {
    let url = std::env::var("REPLAY_REDIS_URL").expect("disposable REPLAY_REDIS_URL required");
    let trade = || {
        SegmentEvent::Trade(TradeEvent::new(
            id("polymarket:123"),
            ContractOrientation::Outcome,
            ConditionalMarketPrice::from_atoms(37, scale(2)).unwrap(),
            qty(5),
            Some(Side::Ask),
        ))
    };
    // Tiny canonical contracts, scripted normalization, REAL materialization.
    // The pre-request Full must be clipped, not used as an implicit warm start.
    let a = Fixture::new(
        0,
        40,
        1,
        vec![
            row("primary", 5, vec![full("polymarket:987", &[(21, 6)], &[])]),
            row(
                "primary",
                14,
                vec![full("polymarket:123", &[(17, 3)], &[(83, 2)])],
            ),
            row("primary", 18, vec![trade()]),
            Row {
                continuity: "duplicate",
                ..row("primary", 19, vec![trade()])
            },
            row("primary", 29, vec![full("polymarket:987", &[], &[])]),
        ],
        |_| {},
    );
    let b = Fixture::new(
        40,
        50,
        6,
        vec![row(
            "primary",
            45,
            vec![SegmentEvent::Control(ControlEvent::ConnectionClosed {
                epoch: "e1".into(),
            })],
        )],
        |r| {
            r.certified = false;
            r.clock_faults.push(indexer_finalize::ClockFault {
                window_start_ns: 40,
                lane: "primary".into(),
                previous_visible_ns: 48,
                observed_visible_ns: 45,
            });
        },
    );
    let c = Fixture::new(50, 60, 7, vec![ignored("primary", 55)], |_| {});
    let d = Fixture::new(
        60,
        80,
        8,
        vec![
            row("primary", 61, vec![full("polymarket:123", &[(31, 7)], &[])]),
            row("primary", 67, vec![full("polymarket:987", &[], &[(81, 9)])]),
        ],
        |_| {},
    );
    let mut cfg = config(&a);
    cfg.inputs = [&a, &b, &c, &d]
        .into_iter()
        .map(|f| Input {
            directory: f.pin.directory.clone(),
            derivative_address: f.pin.pin.derivative_address.clone(),
            receipt_sha256: f.pin.pin.receipt_sha256,
        })
        .collect();
    cfg.start_ns = "10".into();
    cfg.end_ns = "80".into();
    cfg.groups = vec!["coverage".into()];
    cfg.plans = ["polymarket:123", "polymarket:987"]
        .into_iter()
        .map(|name| Plan {
            instrument: id(name),
            orientation: ContractOrientation::Outcome,
            lane: LaneId::new("primary").unwrap(),
            venue: "polymarket".into(),
            price_scale: "2".into(),
            quantity_scale: "0".into(),
        })
        .collect();
    let scratch = tempdir::TempDir::new("coverage-acceptance").unwrap();
    let path = scratch.path().join("config.json");
    std::fs::write(&path, serde_json::to_vec(&cfg).unwrap()).unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    assert!(
        std::process::Command::new(root.join(".venv/bin/python"))
            .current_dir(&root)
            .args(["-m", "replay.tests.test_coverage_acceptance"])
            .arg(path)
            .arg(env!("CARGO_BIN_EXE_replay-publish"))
            .env("REPLAY_REDIS_URL", url)
            .status()
            .unwrap()
            .success()
    );
}
