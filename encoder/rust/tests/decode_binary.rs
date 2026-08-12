use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

const LOGICAL_SHA256: &str = "61d8080cf357099a3dee50a642f0cb20650fe57ca69b5de55ae2b772e32e4dd4";
const STORED_SHA256: &str = "a8ca84b5ff2ab367e16e6918ae9a4d48650f6e1e30d6beb7c5827e9133653896";
const LOGICAL_BYTES: &str = "121232";
const LOGICAL_LINES: &str = "256";
const STORED_BYTES: &str = "4531";

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "prediction-decode-v1-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("create test directory");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures")
}

fn command(input: &Path, output: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_prediction-decode-v1"));
    command.args(["--protocol-version", "1", "--input"]);
    command.arg(input).arg("--output").arg(output).args([
        "--logical-sha256",
        LOGICAL_SHA256,
        "--logical-byte-length",
        LOGICAL_BYTES,
        "--logical-line-count",
        LOGICAL_LINES,
        "--stored-sha256",
        STORED_SHA256,
        "--stored-byte-length",
        STORED_BYTES,
    ]);
    command
}

fn run_fixture(frame: &str) {
    let directory = TestDirectory::new();
    let output_path = directory.0.join("decoded.ndjson");
    let result = command(&fixtures().join(frame), &output_path)
        .output()
        .expect("run decoder");
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(result.stdout.is_empty());
    assert_eq!(
        std::fs::read(output_path).expect("decoded output"),
        std::fs::read(fixtures().join("roundtrip_v1.ndjson")).expect("payload fixture")
    );
}

fn assert_failure_without_output(result: Output, expected_code: i32, output: &Path) {
    assert_eq!(result.status.code(), Some(expected_code));
    assert!(result.stdout.is_empty());
    assert!(!result.stderr.is_empty());
    assert!(result.stderr.len() <= 4096);
    assert!(!output.exists(), "partial output was retained");
}

#[test]
fn decodes_committed_python_and_rust_fixtures() {
    run_fixture("roundtrip_v1.python.ndjson.zst");
    run_fixture("roundtrip_v1.rust.ndjson.zst");
}

#[test]
fn refuses_to_overwrite_an_existing_output() {
    let directory = TestDirectory::new();
    let output = directory.0.join("existing");
    std::fs::write(&output, b"keep me").expect("existing output");
    let result = command(&fixtures().join("roundtrip_v1.rust.ndjson.zst"), &output)
        .output()
        .expect("run decoder");
    assert_eq!(result.status.code(), Some(4));
    assert_eq!(std::fs::read(output).expect("existing output"), b"keep me");
}

#[test]
fn malformed_and_duplicate_arguments_are_invocation_errors() {
    for arguments in [
        vec!["--unknown", "value"],
        vec!["--protocol-version", "2"],
        vec!["--protocol-version", "1", "--protocol-version", "1"],
        vec!["--protocol-version", "1", "--input"],
    ] {
        let result = Command::new(env!("CARGO_BIN_EXE_prediction-decode-v1"))
            .args(arguments)
            .output()
            .expect("run decoder");
        assert_eq!(result.status.code(), Some(2));
        assert!(result.stdout.is_empty());
        assert!(!result.stderr.is_empty());
        assert!(result.stderr.len() <= 4096);
    }
}

#[test]
fn wrong_identity_removes_output() {
    let directory = TestDirectory::new();
    let output = directory.0.join("decoded");
    let mut invocation = command(&fixtures().join("roundtrip_v1.rust.ndjson.zst"), &output);
    invocation.args(["--stored-sha256", &"0".repeat(64)]);
    let result = invocation.output().expect("run decoder");
    // The appended duplicate itself is rejected before any file is created.
    assert_failure_without_output(result, 2, &output);

    let invocation = command(&fixtures().join("roundtrip_v1.rust.ndjson.zst"), &output);
    let arguments = invocation
        .get_args()
        .map(|arg| arg.to_owned())
        .collect::<Vec<_>>();
    let wrong = "0".repeat(64);
    let mut replaced = Command::new(env!("CARGO_BIN_EXE_prediction-decode-v1"));
    let mut iterator = arguments.into_iter();
    while let Some(argument) = iterator.next() {
        if argument == "--logical-sha256" {
            replaced.arg(argument).arg(&wrong);
            iterator.next();
        } else {
            replaced.arg(argument);
        }
    }
    assert_failure_without_output(replaced.output().expect("run decoder"), 3, &output);

    let invocation = command(&fixtures().join("roundtrip_v1.rust.ndjson.zst"), &output);
    let arguments = invocation
        .get_args()
        .map(|arg| arg.to_owned())
        .collect::<Vec<_>>();
    let mut replaced = Command::new(env!("CARGO_BIN_EXE_prediction-decode-v1"));
    let mut iterator = arguments.into_iter();
    while let Some(argument) = iterator.next() {
        if argument == "--stored-sha256" {
            replaced.arg(argument).arg(&wrong);
            iterator.next();
        } else {
            replaced.arg(argument);
        }
    }
    assert_failure_without_output(replaced.output().expect("run decoder"), 3, &output);
}

#[test]
fn truncation_trailing_data_and_decode_limit_remove_output() {
    let directory = TestDirectory::new();
    let original =
        std::fs::read(fixtures().join("roundtrip_v1.rust.ndjson.zst")).expect("fixture frame");
    let cases = [
        (
            "truncated",
            original[..original.len() - 3].to_vec(),
            LOGICAL_BYTES,
        ),
        (
            "trailing",
            [original.as_slice(), b"junk"].concat(),
            LOGICAL_BYTES,
        ),
        ("limit", original.clone(), "64"),
    ];
    for (name, bytes, logical_bytes) in cases {
        let input = directory.0.join(format!("{name}.zst"));
        let output = directory.0.join(format!("{name}.ndjson"));
        std::fs::write(&input, bytes).expect("write hostile frame");
        let base = command(&input, &output);
        let arguments = base
            .get_args()
            .map(|arg| arg.to_owned())
            .collect::<Vec<_>>();
        let mut replaced = Command::new(env!("CARGO_BIN_EXE_prediction-decode-v1"));
        let mut iterator = arguments.into_iter();
        while let Some(argument) = iterator.next() {
            if argument == "--logical-byte-length" {
                replaced.arg(argument).arg(logical_bytes);
                iterator.next();
            } else {
                replaced.arg(argument);
            }
        }
        assert_failure_without_output(replaced.output().expect("run decoder"), 3, &output);
    }
}
