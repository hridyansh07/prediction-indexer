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
    let done = engine.finish().unwrap();
    output.push(wrap(
        done.cuts() + 1,
        "terminal",
        json!({"cuts":done.cuts().to_string()}),
    ));
    output
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
        assert!(result.is_err(), "{failure}");
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
