//! The one Zstandard boundary shared by every Rust producer and consumer.
//!
//! The mirror of `encoder/compression.py`, field for field and rule for rule.
//! Two implementations exist because the archiver is Python and the finalizer is
//! Rust; they are *not* required to emit identical bytes, and
//! `encoder/ZSTD_MATERIALIZATION_PIPELINE_V1.md` §2.1 says so explicitly. What they
//! are required to do is decode one another's frames to byte-identical NDJSON,
//! which the committed `roundtrip_v1` fixtures prove in both directions without
//! either test suite shelling out to the other toolchain.
//!
//! ```text
//! logical  sha256, byte length and LF count of the decoded NDJSON
//! stored   sha256 and byte length of the complete Zstandard frame
//! ```
//!
//! Streaming only. No API here takes or returns a complete file as one `Vec<u8>`,
//! because the deployment target processes multi-gigabyte windows and a shape
//! that *can* buffer eventually does.

use std::fmt;
use std::io::{Read, Write};

use sha2::{Digest, Sha256};

/// §4.1. Pinned rather than defaulted: receipts record it and readers check it.
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// §4.1's "maximum default stream buffer".
pub const DEFAULT_BUFFER_BYTES: usize = 1024 * 1024;

/// Decode pulls in smaller bites than encode pushes, so a hostile compression
/// ratio bounds transient allocation by this rather than by the frame.
pub const DECODE_INPUT_BYTES: usize = 128 * 1024;

/// The zstd crate this codec is built against, tracked with `Cargo.toml`.
///
/// Recorded in receipts for diagnosis only (§2.1). A reader never branches on
/// it: a V1 frame is a V1 frame whichever library emitted it, and letting a
/// decoder choose itself from a producer string would recreate the compatibility
/// surface this format exists to avoid.
const ZSTD_CRATE_VERSION: &str = "0.13";
pub const CODEC_VERSION: &str = env!("CARGO_PKG_VERSION");

const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CodecError {
    Compression(String),
    Io(String),
    /// The input ended inside the frame.
    TruncatedFrame,
    /// A second frame, or padding, followed the one frame an object may hold.
    TrailingBytes,
    /// The frame names a dictionary. V1 objects are self-contained.
    DictionaryRequired(u32),
    /// The frame carries no content checksum. V1 objects always do.
    MissingChecksum,
    /// Decoding would have produced more bytes than the caller accepts.
    LimitExceeded {
        limit: u64,
    },
    /// Decoded or stored bytes disagree with the identity that was expected.
    IdentityMismatch(String),
    /// A non-empty NDJSON payload whose final record has no terminator.
    MissingTrailingNewline,
    /// A compression level other than the one V1 receipts commit to.
    UnsupportedLevel {
        level: i32,
    },
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compression(detail) => write!(formatter, "Zstandard operation failed: {detail}"),
            Self::Io(detail) => write!(formatter, "stream error: {detail}"),
            Self::TruncatedFrame => formatter.write_str("truncated Zstandard frame"),
            Self::TrailingBytes => {
                formatter.write_str("trailing bytes follow the one expected Zstandard frame")
            }
            Self::DictionaryRequired(id) => write!(
                formatter,
                "frame requires dictionary {id}; V1 frames carry no dictionary"
            ),
            Self::MissingChecksum => {
                formatter.write_str("frame carries no checksum; V1 frames are checksummed")
            }
            Self::LimitExceeded { limit } => write!(
                formatter,
                "decoded output exceeds the {limit}-byte maximum this decode accepts"
            ),
            Self::IdentityMismatch(detail) => formatter.write_str(detail),
            Self::MissingTrailingNewline => {
                formatter.write_str("non-empty NDJSON payload does not end in LF")
            }
            Self::UnsupportedLevel { level } => write!(
                formatter,
                "compression level {level} is not the V1 level {DEFAULT_ZSTD_LEVEL}; \
                 receipts commit to level 3 and readers check it"
            ),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<std::io::Error> for CodecError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

/// What the decoded NDJSON is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalIdentity {
    pub sha256: String,
    pub byte_length: u64,
    pub line_count: u64,
}

/// What the physical frame is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredIdentity {
    pub sha256: String,
    pub byte_length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncodeResult {
    pub logical: LogicalIdentity,
    pub stored: StoredIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeResult {
    pub logical: LogicalIdentity,
    pub stored: StoredIdentity,
}

/// The concrete encoder recorded in a receipt, e.g. `zstd-rs/0.13; libzstd/1.5.7`.
pub fn encoder_version() -> String {
    let raw = zstd::zstd_safe::version_number();
    format!(
        "zstd-rs/{ZSTD_CRATE_VERSION}; libzstd/{}.{}.{}",
        raw / 10_000,
        (raw / 100) % 100,
        raw % 100
    )
}

#[derive(Default)]
struct LogicalCounter {
    digest: Sha256,
    byte_length: u64,
    line_count: u64,
    last_byte: Option<u8>,
}

impl LogicalCounter {
    fn observe(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        self.digest.update(bytes);
        self.byte_length += bytes.len() as u64;
        self.line_count += bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
        self.last_byte = bytes.last().copied();
    }

    fn ends_in_newline(&self) -> bool {
        self.byte_length == 0 || self.last_byte == Some(b'\n')
    }

    fn identity(self) -> LogicalIdentity {
        LogicalIdentity {
            sha256: format!("{:x}", self.digest.finalize()),
            byte_length: self.byte_length,
            line_count: self.line_count,
        }
    }
}

/// Passes the frame through to the real sink while measuring it.
struct HashingWriter<W: Write> {
    inner: W,
    digest: Sha256,
    byte_length: u64,
}

impl<W: Write> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            byte_length: 0,
        }
    }

    fn finish(self) -> (W, StoredIdentity) {
        (
            self.inner,
            StoredIdentity {
                sha256: format!("{:x}", self.digest.finalize()),
                byte_length: self.byte_length,
            },
        )
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.digest.update(&buffer[..written]);
        self.byte_length += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// The longest a Zstandard frame header can be: magic, descriptor, window
/// byte, four dictionary-ID bytes and eight content-size bytes.
const MAX_FRAME_HEADER_BYTES: usize = 18;

/// Counts and hashes every compressed byte the decoder pulls from the source,
/// keeping the first few so the frame header can be inspected after the fact.
struct HashingReader<R: Read> {
    inner: R,
    digest: Sha256,
    byte_length: u64,
    header: Vec<u8>,
}

impl<R: Read> HashingReader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            byte_length: 0,
            header: Vec::with_capacity(MAX_FRAME_HEADER_BYTES),
        }
    }

    /// The frame header, as far as the source has supplied it.
    fn header_bytes(&self) -> &[u8] {
        &self.header
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.digest.update(&buffer[..read]);
        self.byte_length += read as u64;
        if self.header.len() < MAX_FRAME_HEADER_BYTES {
            let wanted = MAX_FRAME_HEADER_BYTES - self.header.len();
            self.header.extend_from_slice(&buffer[..read.min(wanted)]);
        }
        Ok(read)
    }
}

/// An incremental V1 encoder for producers that generate NDJSON a record at a
/// time. It is the stateful form of [`encode_stream`].
///
/// Logical identity is measured over the bytes accepted by `Write`; stored
/// identity is measured over the complete checksummed frame written to `sink`.
/// Call [`finish`](Self::finish) exactly once to close the frame and obtain both
/// identities.
pub struct StreamingEncoder<W: Write> {
    encoder: zstd::stream::write::Encoder<'static, HashingWriter<W>>,
    logical: LogicalCounter,
}

impl<W: Write> StreamingEncoder<W> {
    pub fn new(sink: W, level: i32) -> Result<Self, CodecError> {
        if level != DEFAULT_ZSTD_LEVEL {
            return Err(CodecError::UnsupportedLevel { level });
        }
        let mut encoder = zstd::stream::write::Encoder::new(HashingWriter::new(sink), level)
            .map_err(|error| CodecError::Compression(error.to_string()))?;
        encoder
            .include_checksum(true)
            .map_err(|error| CodecError::Compression(error.to_string()))?;
        Ok(Self {
            encoder,
            logical: LogicalCounter::default(),
        })
    }

    pub fn finish(self) -> Result<(W, EncodeResult), CodecError> {
        if !self.logical.ends_in_newline() {
            return Err(CodecError::MissingTrailingNewline);
        }
        let measured = self
            .encoder
            .finish()
            .map_err(|error| CodecError::Compression(error.to_string()))?;
        let (sink, stored) = measured.finish();
        Ok((
            sink,
            EncodeResult {
                logical: self.logical.identity(),
                stored,
            },
        ))
    }
}

impl<W: Write> Write for StreamingEncoder<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.encoder.write(buffer)?;
        self.logical.observe(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.encoder.flush()
    }
}

/// A bounded incremental decoder for consumers that must inspect canonical
/// evidence without materializing a decoded copy on disk.
///
/// `Read` never returns bytes beyond `max_decoded_bytes`. [`finish`](Self::finish)
/// drains any unread payload, rejects trailing frames/bytes, and verifies both
/// identities. Callers must not trust bytes read before `finish` succeeds.
pub struct StreamingDecoder<R: Read> {
    decoder: Option<zstd::stream::read::Decoder<'static, std::io::BufReader<HashingReader<R>>>>,
    logical: LogicalCounter,
    expected_logical: LogicalIdentity,
    expected_stored: Option<StoredIdentity>,
    limit: u64,
    eof: bool,
    error: Option<CodecError>,
    header_checked: bool,
}

impl<R: Read> StreamingDecoder<R> {
    pub fn new(
        source: R,
        expected_logical: &LogicalIdentity,
        expected_stored: Option<&StoredIdentity>,
        max_decoded_bytes: Option<u64>,
    ) -> Result<Self, CodecError> {
        let hashing = HashingReader::new(source);
        let buffered = std::io::BufReader::with_capacity(DECODE_INPUT_BYTES, hashing);
        let decoder = zstd::stream::read::Decoder::with_buffer(buffered)
            .map_err(|error| CodecError::Compression(error.to_string()))?
            .single_frame();
        Ok(Self {
            decoder: Some(decoder),
            logical: LogicalCounter::default(),
            expected_logical: expected_logical.clone(),
            expected_stored: expected_stored.cloned(),
            limit: max_decoded_bytes.unwrap_or(expected_logical.byte_length),
            eof: false,
            error: None,
            header_checked: false,
        })
    }

    fn remember(&mut self, error: CodecError) -> std::io::Error {
        self.error = Some(error.clone());
        std::io::Error::new(std::io::ErrorKind::InvalidData, error)
    }

    pub fn finish(mut self) -> Result<DecodeResult, CodecError> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        let mut scratch = [0_u8; DECODE_INPUT_BYTES];
        while !self.eof {
            if let Err(error) = self.read(&mut scratch) {
                return Err(self
                    .error
                    .take()
                    .unwrap_or_else(|| CodecError::Io(error.to_string())));
            }
        }

        let decoder = self.decoder.take().expect("decoder exists until finish");
        let mut buffered = decoder.finish();
        let mut remainder = [0_u8; 1];
        if buffered.read(&mut remainder)? != 0 {
            return Err(CodecError::TrailingBytes);
        }
        let hashing = buffered.into_inner();
        let stored = StoredIdentity {
            sha256: format!("{:x}", hashing.digest.finalize()),
            byte_length: hashing.byte_length,
        };
        verify_stored(&stored, self.expected_stored.as_ref())?;

        if !self.logical.ends_in_newline() {
            return Err(CodecError::MissingTrailingNewline);
        }
        let decoded = self.logical.identity();
        verify_logical(&decoded, &self.expected_logical)?;
        Ok(DecodeResult {
            logical: decoded,
            stored,
        })
    }
}

impl<R: Read> Read for StreamingDecoder<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() || self.eof {
            return Ok(0);
        }
        if let Some(error) = self.error.clone() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
        }

        let remaining = self.limit.saturating_sub(self.logical.byte_length);
        if remaining == 0 {
            let mut probe = [0_u8; 1];
            let outcome = self
                .decoder
                .as_mut()
                .expect("decoder exists")
                .read(&mut probe);
            if !self.header_checked {
                let header = self
                    .decoder
                    .as_ref()
                    .expect("decoder exists")
                    .get_ref()
                    .get_ref()
                    .header_bytes();
                if let Err(error) = check_frame_header(header) {
                    return Err(self.remember(error));
                }
                self.header_checked = true;
            }
            return match outcome {
                Ok(0) => {
                    self.eof = true;
                    Ok(0)
                }
                Ok(_) => {
                    let limit = self.limit;
                    Err(self.remember(CodecError::LimitExceeded { limit }))
                }
                Err(error) => {
                    let error = classify_decode_error(&error);
                    Err(self.remember(error))
                }
            };
        }

        let allowed = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let outcome = self
            .decoder
            .as_mut()
            .expect("decoder exists")
            .read(&mut buffer[..allowed]);
        if !self.header_checked {
            let header = self
                .decoder
                .as_ref()
                .expect("decoder exists")
                .get_ref()
                .get_ref()
                .header_bytes();
            if let Err(error) = check_frame_header(header) {
                return Err(self.remember(error));
            }
            self.header_checked = true;
        }
        match outcome {
            Ok(0) => {
                self.eof = true;
                Ok(0)
            }
            Ok(read) => {
                self.logical.observe(&buffer[..read]);
                Ok(read)
            }
            Err(error) => {
                let error = classify_decode_error(&error);
                Err(self.remember(error))
            }
        }
    }
}

/// Compresses `source` into exactly one frame on `sink`, measuring both sides.
///
/// The source is observed before the encoder sees it, so the logical identity
/// describes the caller's bytes rather than anything the codec did to them.
///
/// `level` exists to be checked, not chosen: every V1 receipt states
/// `"level": 3` and readers verify that field, so an encoder pointed at another
/// level would produce receipts describing a frame it did not write.
pub fn encode_stream<R: Read, W: Write>(
    mut source: R,
    sink: W,
    level: i32,
) -> Result<EncodeResult, CodecError> {
    let mut encoder = StreamingEncoder::new(sink, level)?;
    let mut buffer = vec![0_u8; DEFAULT_BUFFER_BYTES];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        encoder.write_all(&buffer[..read])?;
    }
    encoder.finish().map(|(_, result)| result)
}

/// Decodes one frame, or fails. There is no partial success.
///
/// `max_decoded_bytes` defaults to the expected logical length and is a hard
/// ceiling: the sink never receives byte `maximum + 1`.
pub fn decode_stream<R: Read, W: Write>(
    source: R,
    mut sink: W,
    expected_logical: &LogicalIdentity,
    expected_stored: Option<&StoredIdentity>,
    max_decoded_bytes: Option<u64>,
) -> Result<DecodeResult, CodecError> {
    let mut decoder =
        StreamingDecoder::new(source, expected_logical, expected_stored, max_decoded_bytes)?;
    std::io::copy(&mut decoder, &mut sink).map_err(|error| {
        decoder
            .error
            .take()
            .unwrap_or_else(|| CodecError::Io(error.to_string()))
    })?;
    decoder.finish()
}

fn verify_stored(
    stored: &StoredIdentity,
    expected: Option<&StoredIdentity>,
) -> Result<(), CodecError> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if stored.byte_length != expected.byte_length {
        return Err(CodecError::IdentityMismatch(format!(
            "stored byte length {} is not the expected {}",
            stored.byte_length, expected.byte_length
        )));
    }
    if stored.sha256 != expected.sha256 {
        return Err(CodecError::IdentityMismatch(
            "stored sha256 does not match the expected frame".to_owned(),
        ));
    }
    Ok(())
}

fn verify_logical(decoded: &LogicalIdentity, expected: &LogicalIdentity) -> Result<(), CodecError> {
    if decoded.byte_length != expected.byte_length {
        return Err(CodecError::IdentityMismatch(format!(
            "decoded byte length {} is not the expected {}",
            decoded.byte_length, expected.byte_length
        )));
    }
    if decoded.line_count != expected.line_count {
        return Err(CodecError::IdentityMismatch(format!(
            "decoded line count {} is not the expected {}",
            decoded.line_count, expected.line_count
        )));
    }
    if decoded.sha256 != expected.sha256 {
        return Err(CodecError::IdentityMismatch(
            "decoded sha256 does not match the expected payload".to_owned(),
        ));
    }
    Ok(())
}

/// Digest, length and LF count of a stream, without compressing it.
pub fn logical_identity_of<R: Read>(mut source: R) -> Result<LogicalIdentity, CodecError> {
    let mut logical = LogicalCounter::default();
    let mut buffer = vec![0_u8; DEFAULT_BUFFER_BYTES];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        logical.observe(&buffer[..read]);
    }
    Ok(logical.identity())
}

/// Digest and length of an object's bytes exactly as they sit.
pub fn stored_identity_of<R: Read>(mut source: R) -> Result<StoredIdentity, CodecError> {
    let mut digest = Sha256::new();
    let mut byte_length = 0_u64;
    let mut buffer = vec![0_u8; DEFAULT_BUFFER_BYTES];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        byte_length += read as u64;
    }
    Ok(StoredIdentity {
        sha256: format!("{:x}", digest.finalize()),
        byte_length,
    })
}

/// libzstd reports a truncated frame as an ordinary read error, so the
/// distinction has to be made here rather than lost as "decompression failed".
fn classify_decode_error(error: &std::io::Error) -> CodecError {
    let detail = error.to_string();
    if detail.contains("Src size is incorrect") || detail.contains("Unknown frame descriptor") {
        return CodecError::TruncatedFrame;
    }
    if error.kind() == std::io::ErrorKind::UnexpectedEof {
        return CodecError::TruncatedFrame;
    }
    CodecError::Compression(detail)
}

/// Rejects a dictionary-dependent or unchecksummed frame from its header.
///
/// Hand-parsed because libzstd's frame-header struct is behind its experimental
/// API. The layout is stable and specified: magic number, then a descriptor byte
/// whose bit 2 is the content-checksum flag and whose low two bits size the
/// dictionary ID that follows the optional window byte.
fn check_frame_header(header: &[u8]) -> Result<(), CodecError> {
    if header.len() < 5 {
        return Err(CodecError::TruncatedFrame);
    }
    if header[0..4] != ZSTD_MAGIC {
        return Err(CodecError::Compression(
            "input is not a Zstandard frame".to_owned(),
        ));
    }
    let descriptor = header[4];
    if descriptor & 0b0000_0100 == 0 {
        return Err(CodecError::MissingChecksum);
    }
    let dictionary_id_bytes = match descriptor & 0b0000_0011 {
        0 => 0_usize,
        1 => 1,
        2 => 2,
        _ => 4,
    };
    if dictionary_id_bytes == 0 {
        return Ok(());
    }
    let single_segment = descriptor & 0b0010_0000 != 0;
    let start = 5 + usize::from(!single_segment);
    let end = start + dictionary_id_bytes;
    if header.len() < end {
        return Err(CodecError::TruncatedFrame);
    }
    let mut identifier = 0_u32;
    for (index, byte) in header[start..end].iter().enumerate() {
        identifier |= u32::from(*byte) << (8 * index);
    }
    if identifier != 0 {
        return Err(CodecError::DictionaryRequired(identifier));
    }
    Ok(())
}
