//! §9.1's codec gates, and the Rust half of the cross-language fixture proof.
//!
//! Every rejection test below constructs the *unsafe* object first and asserts
//! that the codec refuses it. A decoder that merely happens to fail on today's
//! corrupt input is not the same as one that cannot be made to succeed on one.

use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};

use prediction_encoder::{
    CodecError, DEFAULT_ZSTD_LEVEL, LogicalIdentity, StoredIdentity, StreamingDecoder,
    StreamingEncoder, decode_stream, encode_stream, encoder_version, logical_identity_of,
};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../fixtures")
}

fn payload() -> Vec<u8> {
    std::fs::read(fixtures().join("roundtrip_v1.ndjson")).expect("payload fixture")
}

fn metadata() -> serde_json::Value {
    let encoded = std::fs::read(fixtures().join("roundtrip_v1.json")).expect("fixture metadata");
    serde_json::from_slice(&encoded).expect("fixture metadata is JSON")
}

fn recorded_logical() -> LogicalIdentity {
    let document = metadata();
    let logical = &document["payload"]["logical"];
    LogicalIdentity {
        sha256: logical["sha256"].as_str().expect("sha256").to_owned(),
        byte_length: logical["byte_length"].as_u64().expect("byte_length"),
        line_count: logical["line_count"].as_u64().expect("line_count"),
    }
}

fn encode(bytes: &[u8]) -> (Vec<u8>, LogicalIdentity, StoredIdentity) {
    let mut frame = Vec::new();
    let result =
        encode_stream(Cursor::new(bytes.to_vec()), &mut frame, DEFAULT_ZSTD_LEVEL).expect("encode");
    (frame, result.logical, result.stored)
}

fn decode(
    frame: &[u8],
    logical: &LogicalIdentity,
    stored: Option<&StoredIdentity>,
    limit: Option<u64>,
) -> Result<Vec<u8>, CodecError> {
    let mut decoded = Vec::new();
    decode_stream(
        Cursor::new(frame.to_vec()),
        &mut decoded,
        logical,
        stored,
        limit,
    )?;
    Ok(decoded)
}

#[test]
fn decodes_the_committed_python_frame_to_exact_bytes() {
    let frame = std::fs::read(fixtures().join("roundtrip_v1.python.ndjson.zst"))
        .expect("python fixture frame");
    let logical = recorded_logical();
    let decoded = decode(&frame, &logical, None, None).expect("decode python frame");
    assert_eq!(decoded, payload());
}

#[test]
fn its_own_frame_matches_the_recorded_stored_identity() {
    let (frame, logical, stored) = encode(&payload());
    let document = metadata();
    let recorded = &document["frames"]["rust"]["stored"];
    assert_eq!(logical, recorded_logical());
    assert_eq!(
        stored.byte_length,
        recorded["byte_length"].as_u64().expect("byte_length")
    );
    assert_eq!(stored.sha256, recorded["sha256"].as_str().expect("sha256"));
    assert_eq!(
        frame,
        std::fs::read(fixtures().join("roundtrip_v1.rust.ndjson.zst")).expect("rust")
    );
}

#[test]
fn empty_ndjson_round_trips_through_a_valid_non_empty_frame() {
    let (frame, logical, stored) = encode(b"");
    assert!(!frame.is_empty());
    assert_eq!(logical.byte_length, 0);
    assert_eq!(logical.line_count, 0);
    // sha256 of zero bytes, per §2.1.
    assert_eq!(
        logical.sha256,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(
        decode(&frame, &logical, Some(&stored), None).expect("decode"),
        Vec::<u8>::new()
    );
}

#[test]
fn incremental_writer_and_reader_preserve_one_logical_stream() {
    let mut encoder = StreamingEncoder::new(Vec::new(), DEFAULT_ZSTD_LEVEL).expect("encoder");
    encoder.write_all(b"{\"one\":1}\n").expect("first record");
    encoder.write_all(b"{\"two\":2}\n").expect("second record");
    let (frame, encoded) = encoder.finish().expect("finish frame");

    let mut decoder = StreamingDecoder::new(
        Cursor::new(frame),
        &encoded.logical,
        Some(&encoded.stored),
        Some(encoded.logical.byte_length),
    )
    .expect("decoder");
    let mut decoded = Vec::new();
    let mut tiny = [0_u8; 3];
    loop {
        let read = decoder.read(&mut tiny).expect("incremental decode");
        if read == 0 {
            break;
        }
        decoded.extend_from_slice(&tiny[..read]);
    }
    let verified = decoder.finish().expect("verify frame");
    assert_eq!(decoded, b"{\"one\":1}\n{\"two\":2}\n");
    assert_eq!(verified.logical, encoded.logical);
    assert_eq!(verified.stored, encoded.stored);
}

#[test]
fn any_level_other_than_the_v1_level_is_refused() {
    // Receipts state `"level": 3` and readers check it, so an encoder that
    // could be pointed elsewhere would describe a frame it did not write.
    for level in [1, 9, 19, -3] {
        let error =
            encode_stream(Cursor::new(b"one\n".to_vec()), Vec::new(), level).expect_err("level");
        assert_eq!(error, CodecError::UnsupportedLevel { level });
    }
    assert!(
        encode_stream(
            Cursor::new(b"one\n".to_vec()),
            Vec::new(),
            DEFAULT_ZSTD_LEVEL
        )
        .is_ok()
    );
}

#[test]
fn a_truncated_frame_is_rejected() {
    let (frame, logical, _) = encode(b"one\ntwo\n");
    let error = decode(&frame[..frame.len() - 3], &logical, None, None).expect_err("truncated");
    assert_eq!(error, CodecError::TruncatedFrame);
}

#[test]
fn trailing_bytes_after_the_frame_are_rejected() {
    let (frame, logical, _) = encode(b"one\ntwo\n");
    let mut appended = frame.clone();
    appended.extend_from_slice(b"junk");
    assert_eq!(
        decode(&appended, &logical, None, None).expect_err("trailing"),
        CodecError::TrailingBytes
    );
}

#[test]
fn concatenated_frames_are_rejected() {
    let (frame, logical, _) = encode(b"one\ntwo\n");
    let mut concatenated = frame.clone();
    concatenated.extend_from_slice(&frame);
    assert_eq!(
        decode(&concatenated, &logical, None, None).expect_err("concatenated"),
        CodecError::TrailingBytes
    );
}

#[test]
fn a_corrupt_frame_checksum_is_rejected() {
    let (frame, logical, _) = encode(b"one\ntwo\n");
    let mut corrupt = frame.clone();
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xFF;
    assert!(matches!(
        decode(&corrupt, &logical, None, None).expect_err("checksum"),
        CodecError::Compression(_) | CodecError::TruncatedFrame
    ));
}

#[test]
fn a_frame_without_a_checksum_is_rejected() {
    // Written with the checksum flag off, which `encode_stream` cannot produce.
    let mut frame = Vec::new();
    {
        let mut encoder =
            zstd::stream::write::Encoder::new(&mut frame, DEFAULT_ZSTD_LEVEL).expect("encoder");
        encoder.include_checksum(false).expect("flag");
        encoder.write_all(b"one\ntwo\n").expect("write");
        encoder.finish().expect("finish");
    }
    let logical = logical_identity_of(Cursor::new(b"one\ntwo\n".to_vec())).expect("identity");
    assert_eq!(
        decode(&frame, &logical, None, None).expect_err("unchecksummed"),
        CodecError::MissingChecksum
    );
}

#[test]
fn a_dictionary_dependent_frame_is_rejected() {
    let samples: Vec<Vec<u8>> = (0..256)
        .map(|index| format!("{{\"k\":{index},\"v\":\"{}\"}}\n", "x".repeat(48)).into_bytes())
        .collect();
    let dictionary = zstd::dict::from_samples(&samples, 4096).expect("train dictionary");
    let mut frame = Vec::new();
    {
        let mut encoder = zstd::stream::write::Encoder::with_dictionary(
            &mut frame,
            DEFAULT_ZSTD_LEVEL,
            &dictionary,
        )
        .expect("encoder");
        encoder.include_checksum(true).expect("flag");
        encoder.write_all(b"one\ntwo\n").expect("write");
        encoder.finish().expect("finish");
    }
    let logical = logical_identity_of(Cursor::new(b"one\ntwo\n".to_vec())).expect("identity");
    assert!(matches!(
        decode(&frame, &logical, None, None).expect_err("dictionary"),
        CodecError::DictionaryRequired(_)
    ));
}

#[test]
fn a_wrong_stored_hash_and_a_wrong_logical_hash_fail_independently() {
    let (frame, logical, stored) = encode(b"one\ntwo\n");

    let wrong_stored = StoredIdentity {
        sha256: "0".repeat(64),
        byte_length: stored.byte_length,
    };
    assert!(matches!(
        decode(&frame, &logical, Some(&wrong_stored), None).expect_err("stored"),
        CodecError::IdentityMismatch(_)
    ));

    let wrong_logical = LogicalIdentity {
        sha256: "0".repeat(64),
        ..logical.clone()
    };
    assert!(matches!(
        decode(&frame, &wrong_logical, Some(&stored), None).expect_err("logical"),
        CodecError::IdentityMismatch(_)
    ));

    let wrong_length = LogicalIdentity {
        byte_length: logical.byte_length + 1,
        ..logical.clone()
    };
    assert!(matches!(
        decode(&frame, &wrong_length, Some(&stored), None).expect_err("length"),
        CodecError::IdentityMismatch(_) | CodecError::LimitExceeded { .. }
    ));

    let wrong_lines = LogicalIdentity {
        line_count: logical.line_count + 1,
        ..logical.clone()
    };
    assert!(matches!(
        decode(&frame, &wrong_lines, Some(&stored), None).expect_err("lines"),
        CodecError::IdentityMismatch(_)
    ));
}

#[test]
fn the_decode_limit_aborts_before_the_first_byte_beyond_it() {
    let (frame, logical, _) = encode(&payload());
    let mut decoded = Vec::new();
    let error = decode_stream(Cursor::new(frame), &mut decoded, &logical, None, Some(64))
        .expect_err("limit");
    assert_eq!(error, CodecError::LimitExceeded { limit: 64 });
    assert!(
        decoded.len() <= 64,
        "sink received {} bytes past the limit",
        decoded.len()
    );
}

#[test]
fn memory_does_not_scale_with_input_size() {
    // 8 MiB through a 1 MiB encode buffer and a 128 KiB decode buffer. The
    // assertion that matters is structural — neither side is handed a `Vec` of
    // the payload — so this exercises the streaming path over a file rather
    // than proving an RSS number, which `scripts/archive_probe.py` measures.
    let line = format!("{{\"filler\":\"{}\"}}\n", "z".repeat(200));
    let mut large = Vec::with_capacity(8 * 1024 * 1024);
    while large.len() < 8 * 1024 * 1024 {
        large.extend_from_slice(line.as_bytes());
    }

    let directory = std::env::temp_dir().join(format!("codec-{}", std::process::id()));
    std::fs::create_dir_all(&directory).expect("temp directory");
    let source = directory.join("large.ndjson");
    let frame = directory.join("large.ndjson.zst");
    let decoded = directory.join("large.decoded.ndjson");
    std::fs::write(&source, &large).expect("write source");

    let result = encode_stream(
        std::fs::File::open(&source).expect("open source"),
        std::fs::File::create(&frame).expect("create frame"),
        DEFAULT_ZSTD_LEVEL,
    )
    .expect("encode");
    decode_stream(
        std::fs::File::open(&frame).expect("open frame"),
        std::fs::File::create(&decoded).expect("create decoded"),
        &result.logical,
        Some(&result.stored),
        None,
    )
    .expect("decode");

    let mut round_tripped = Vec::new();
    std::fs::File::open(&decoded)
        .expect("open decoded")
        .read_to_end(&mut round_tripped)
        .expect("read decoded");
    assert_eq!(round_tripped, large);
    let _ = std::fs::remove_dir_all(&directory);
}

/// Rewrites `roundtrip_v1.rust.ndjson.zst` and its half of the metadata.
///
/// Ignored by default: a fixture a test run can regenerate silently proves
/// nothing about interoperability — the point of committing both frames is that
/// changing one is a reviewable event.
///
/// ```sh
/// cargo test --manifest-path encoder/rust/Cargo.toml -- --ignored regenerate
/// ```
#[test]
#[ignore]
fn regenerate_rust_fixture() {
    let target = fixtures().join("roundtrip_v1.rust.ndjson.zst");
    let result = encode_stream(
        std::fs::File::open(fixtures().join("roundtrip_v1.ndjson")).expect("payload"),
        std::fs::File::create(&target).expect("create frame"),
        DEFAULT_ZSTD_LEVEL,
    )
    .expect("encode");

    let path = fixtures().join("roundtrip_v1.json");
    let mut document = metadata();
    document["frames"]["rust"] = serde_json::json!({
        "file": "roundtrip_v1.rust.ndjson.zst",
        "encoder": encoder_version(),
        "stored": {
            "sha256": result.stored.sha256,
            "byte_length": result.stored.byte_length,
        },
    });
    let mut encoded = serde_json::to_vec_pretty(&document).expect("encode metadata");
    encoded.push(b'\n');
    std::fs::write(&path, encoded).expect("write metadata");
    println!("wrote {} and {}", target.display(), path.display());
}
