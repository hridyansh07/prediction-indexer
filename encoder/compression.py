"""The one Zstandard boundary shared by every Python producer and consumer.

Zstandard-only, streaming-only, NDJSON-only. There is no message framing layer
here and no whole-file API: `ZSTD_MATERIALIZATION_PIPELINE_V1.md` §2.1
requires that no production path hold a complete source, frame, or decoded
object in memory, and the way to guarantee that is to not offer the shape at
all. `encoder.whole_buffer` exists for tests and is built on these functions.

**Two identities, calculated in one pass** (§2.2):

```text
logical  sha256, byte length and LF count of the decoded NDJSON
stored   sha256 and byte length of the complete Zstandard frame
```

Logical identity proves what can be reconstructed; stored identity proves which
physical object was committed. A receipt carrying only one of them proves half
of what it claims, so both are returned together and neither is optional.

**Decoding is adversarial by default** (§4.3 of the phase document). One frame
ended exactly at end-of-input, its checksum verified, its declared identities
matched — anything else raises. The `.zst` suffix is never evidence that this
contract was followed, and neither is a successful decompression.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol

__all__ = [
    "DECODE_INPUT_BYTES",
    "DEFAULT_BUFFER_BYTES",
    "DEFAULT_ZSTD_LEVEL",
    "CodecError",
    "DecodeLimitExceeded",
    "DecodeResult",
    "EncodeResult",
    "IdentityMismatch",
    "LogicalIdentity",
    "StoredIdentity",
    "decode_stream",
    "encode_stream",
    "encoder_version",
    "logical_identity_of",
    "stored_identity_of",
]

#: §4.1. Pinned, not defaulted: a receipt records the level it was written with
#: and a reader checks it, so changing this changes the receipt contract.
DEFAULT_ZSTD_LEVEL = 3

#: §4.1's "maximum default stream buffer". Memory is bounded by this and the
#: codec's own window, never by the size of the file being processed.
DEFAULT_BUFFER_BYTES = 1024 * 1024

#: How much decoded output `decode_stream` pulls at a time. Decode is bounded on
#: the *output* side, which is the only side that can be made to explode: one
#: 128 KiB compressed chunk of run-length blocks can expand to gigabytes, so
#: feeding a push decoder in small bites is not a bound at all. `stream_reader`
#: fills a buffer this size and no more, whatever the ratio.
DECODE_INPUT_BYTES = 128 * 1024

#: The longest a Zstandard frame header can be: magic, descriptor, window byte,
#: four dictionary-ID bytes and eight content-size bytes.
MAX_FRAME_HEADER_BYTES = 18

_ZSTD_MAGIC = b"\x28\xb5\x2f\xfd"
_LF = 0x0A


class CodecError(ValueError):
    """A Zstandard stream could not be produced or trusted."""


class IdentityMismatch(CodecError):
    """Decoded or stored bytes disagree with the identity that was expected."""


class DecodeLimitExceeded(CodecError):
    """A decode produced more bytes than its caller declared it would accept."""


class _Writable(Protocol):
    def write(self, data: Any, /) -> Any: ...


class _Readable(Protocol):
    def read(self, size: int, /) -> bytes: ...


def _zstandard():
    try:
        import zstandard
    except ImportError as error:  # pragma: no cover - packaging error, not a codec branch
        raise RuntimeError(
            "the 'zstandard' package is required; install the project dependencies"
        ) from error
    return zstandard


def encoder_version() -> str:
    """The concrete encoder recorded in a receipt, e.g. `python-zstandard/0.25.0`.

    Recorded for diagnosis only (§2.1). It never selects a decoder: a V1 frame
    is a V1 frame regardless of which library emitted it, and making a reader
    branch on the producer would create exactly the compatibility surface this
    format is defined to avoid.
    """
    return f"python-zstandard/{_zstandard().__version__}"


@dataclass(frozen=True)
class LogicalIdentity:
    """What the decoded NDJSON is: digest, length, and record count."""

    sha256: str
    byte_length: int
    line_count: int

    def as_record(self) -> dict[str, Any]:
        return {
            "sha256": self.sha256,
            "byte_length": self.byte_length,
            "line_count": self.line_count,
        }

    @classmethod
    def from_record(cls, record: Any) -> LogicalIdentity:
        return cls(
            sha256=_require_hex(record, "sha256"),
            byte_length=_require_count(record, "byte_length"),
            line_count=_require_count(record, "line_count"),
        )


@dataclass(frozen=True)
class StoredIdentity:
    """What the physical frame is: digest and length of the complete object."""

    sha256: str
    byte_length: int

    def as_record(self) -> dict[str, Any]:
        return {"sha256": self.sha256, "byte_length": self.byte_length}

    @classmethod
    def from_record(cls, record: Any) -> StoredIdentity:
        return cls(
            sha256=_require_hex(record, "sha256"),
            byte_length=_require_count(record, "byte_length"),
        )


@dataclass(frozen=True)
class EncodeResult:
    logical: LogicalIdentity
    stored: StoredIdentity


@dataclass(frozen=True)
class DecodeResult:
    logical: LogicalIdentity
    stored: StoredIdentity


def _require_hex(record: Any, field: str) -> str:
    if not isinstance(record, dict):
        raise CodecError("identity is not an object")
    value = record.get(field)
    if not isinstance(value, str) or len(value) != 64:
        raise CodecError(f"identity {field} is not a 64-character digest")
    if value != value.lower() or any(character not in "0123456789abcdef" for character in value):
        raise CodecError(f"identity {field} is not lowercase hexadecimal")
    return value


def _require_count(record: Any, field: str) -> int:
    if not isinstance(record, dict):
        raise CodecError("identity is not an object")
    value = record.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise CodecError(f"identity {field} is not a non-negative integer")
    return value


class _LogicalCounter:
    """Digest, byte length and LF count of the bytes as the caller supplied them."""

    __slots__ = ("_digest", "byte_length", "line_count", "last_byte")

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self.byte_length = 0
        self.line_count = 0
        self.last_byte: int | None = None

    def observe(self, chunk: bytes | memoryview) -> None:
        data = chunk if isinstance(chunk, bytes) else bytes(chunk)
        if not data:
            return
        self._digest.update(data)
        self.byte_length += len(data)
        self.line_count += data.count(b"\n")
        self.last_byte = data[-1]

    def identity(self) -> LogicalIdentity:
        return LogicalIdentity(
            sha256=self._digest.hexdigest(),
            byte_length=self.byte_length,
            line_count=self.line_count,
        )

    def ends_in_newline(self) -> bool:
        return self.byte_length == 0 or self.last_byte == _LF


def _write_all(sink: _Writable, data: Any) -> int:
    """Writes every byte, or raises. Never reports more than reached the sink.

    `write` is permitted to consume less than it was given — that is the whole
    point of its return value — and an unbuffered file does exactly that under
    memory or device pressure. Ignoring it produces the worst failure this
    codebase has: a stored identity describing bytes that are not on disk, which
    verifies against itself and fails only later, against the object.

    A sink returning `None` is following the older convention where a short
    write raises instead; there is nothing to loop on, so it is taken at its
    word.
    """
    view = memoryview(data)
    total = view.nbytes
    while view.nbytes:
        written = sink.write(view)
        if written is None:
            break
        if written < 0 or written > view.nbytes:
            raise CodecError(f"sink reported writing {written} of {view.nbytes} bytes")
        if written == 0:
            raise CodecError("sink accepted no bytes and made no progress")
        view = view[written:]
    return total


class _HashingSink:
    """Passes the frame through to the real sink while measuring it."""

    __slots__ = ("_sink", "_digest", "byte_length")

    def __init__(self, sink: _Writable) -> None:
        self._sink = sink
        self._digest = hashlib.sha256()
        self.byte_length = 0

    def write(self, data: Any) -> int:
        view = memoryview(data)
        # Hashed after the write, not before: the identity may only describe
        # bytes that actually reached the sink.
        _write_all(self._sink, view)
        self._digest.update(view)
        self.byte_length += view.nbytes
        return view.nbytes

    def flush(self) -> None:  # python-zstandard flushes its output stream
        flush = getattr(self._sink, "flush", None)
        if flush is not None:
            flush()

    def identity(self) -> StoredIdentity:
        return StoredIdentity(sha256=self._digest.hexdigest(), byte_length=self.byte_length)


def encode_stream(
    source: _Readable | BinaryIO,
    sink: _Writable | BinaryIO,
    *,
    level: int = DEFAULT_ZSTD_LEVEL,
    buffer_bytes: int = DEFAULT_BUFFER_BYTES,
) -> EncodeResult:
    """Compresses `source` into exactly one frame on `sink`, measuring both sides.

    The source is read once and observed *before* the encoder sees it, so the
    logical identity describes the caller's bytes rather than anything the codec
    did to them. That is what lets the archiver compare against a seal written by
    a different process months earlier without ever re-reading the file.

    A non-empty NDJSON payload must end in LF. §2.2 makes the LF count the record
    count, and a payload whose final record has no terminator would make that
    equality quietly false.

    `level` exists to be checked, not chosen. Every V1 receipt states
    `"level": 3`, and readers verify that field, so an encoder that could be
    pointed at another level would write receipts describing a frame it did not
    produce. Changing the level is a format change and belongs in a new receipt
    version, not a keyword argument.
    """
    if buffer_bytes <= 0:
        raise ValueError("buffer_bytes must be positive")
    if buffer_bytes > DEFAULT_BUFFER_BYTES:
        raise ValueError(
            f"buffer_bytes {buffer_bytes} is above the {DEFAULT_BUFFER_BYTES}-byte"
            " streaming ceiling in ZSTD_MATERIALIZATION_PIPELINE_V1 §2.1"
        )
    if int(level) != DEFAULT_ZSTD_LEVEL:
        raise ValueError(
            f"compression level {level} is not the V1 level {DEFAULT_ZSTD_LEVEL}; "
            "receipts commit to level 3 and readers check it"
        )

    zstandard = _zstandard()
    compressor = zstandard.ZstdCompressor(
        level=DEFAULT_ZSTD_LEVEL,
        write_checksum=True,
        write_content_size=False,
        write_dict_id=False,
    )
    logical = _LogicalCounter()
    measured = _HashingSink(sink)

    try:
        with compressor.stream_writer(measured, closefd=False) as writer:
            while True:
                chunk = source.read(buffer_bytes)
                if not chunk:
                    break
                logical.observe(chunk)
                writer.write(chunk)
    except zstandard.ZstdError as error:
        raise CodecError(f"Zstandard compression failed: {error}") from error

    if not logical.ends_in_newline():
        raise CodecError("non-empty NDJSON payload does not end in LF")

    return EncodeResult(logical=logical.identity(), stored=measured.identity())


def decode_stream(
    source: _Readable | BinaryIO,
    sink: _Writable | BinaryIO,
    *,
    expected_logical: LogicalIdentity,
    expected_stored: StoredIdentity | None = None,
    max_decoded_bytes: int | None = None,
    buffer_bytes: int = DECODE_INPUT_BYTES,
) -> DecodeResult:
    """Decodes one frame, or raises. There is no partial success.

    `max_decoded_bytes` defaults to the expected logical length and is a *hard*
    ceiling: the sink never receives byte `maximum + 1`. It is separate from the
    expected length because a caller replaying an untrusted object wants the
    abort to happen at the limit, not after a gigabyte has been written and then
    found to disagree.

    Stored identity is verified once the whole frame has been consumed, which is
    the earliest a streaming reader can know it. A caller who needs the stored
    digest checked *before* any decoded byte exists stages the object first —
    that ordering belongs to the archive verifier (§3.5), not to the codec.
    """
    if buffer_bytes <= 0:
        raise ValueError("buffer_bytes must be positive")
    limit = expected_logical.byte_length if max_decoded_bytes is None else int(max_decoded_bytes)
    if limit < 0:
        raise ValueError("max_decoded_bytes must not be negative")

    zstandard = _zstandard()

    # The header is read and judged before a decoder exists, so a
    # dictionary-dependent or unchecksummed frame is refused rather than
    # diagnosed from whatever libzstd says when it fails on one.
    header = _read_at_most(source, MAX_FRAME_HEADER_BYTES)
    _check_frame_header(header)

    bounded = _FrameBoundedReader(source, prefix=header)
    decompressor = zstandard.ZstdDecompressor()
    logical = _LogicalCounter()

    try:
        with decompressor.stream_reader(bounded, read_across_frames=False) as reader:
            while True:
                # Output-bounded by construction. A push decoder returns
                # everything one input chunk expands to, which on a run-length
                # frame is unbounded; this fills a fixed buffer instead.
                produced = reader.read(buffer_bytes)
                if not produced:
                    break
                if logical.byte_length + len(produced) > limit:
                    raise DecodeLimitExceeded(
                        f"decoded output exceeds the {limit}-byte maximum this decode accepts"
                    )
                # Written before it is counted, and written in full. A decoded
                # identity may only describe bytes the sink actually took.
                _write_all(sink, produced)
                logical.observe(produced)
    except zstandard.ZstdError as error:
        if not bounded.frame_complete:
            raise CodecError(f"truncated Zstandard frame: {error}") from error
        raise CodecError(f"Zstandard decompression failed: {error}") from error

    if not bounded.frame_complete:
        raise CodecError("truncated Zstandard frame")
    if bounded.has_trailing_bytes():
        raise CodecError("trailing bytes follow the one expected Zstandard frame")

    stored = bounded.stored_identity()
    if expected_stored is not None:
        if stored.byte_length != expected_stored.byte_length:
            raise IdentityMismatch(
                f"stored byte length {stored.byte_length} is not the expected "
                f"{expected_stored.byte_length}"
            )
        if stored.sha256 != expected_stored.sha256:
            raise IdentityMismatch("stored sha256 does not match the expected frame")

    decoded = logical.identity()
    if decoded.byte_length != expected_logical.byte_length:
        raise IdentityMismatch(
            f"decoded byte length {decoded.byte_length} is not the expected "
            f"{expected_logical.byte_length}"
        )
    if decoded.line_count != expected_logical.line_count:
        raise IdentityMismatch(
            f"decoded line count {decoded.line_count} is not the expected "
            f"{expected_logical.line_count}"
        )
    if decoded.sha256 != expected_logical.sha256:
        raise IdentityMismatch("decoded sha256 does not match the expected payload")
    if not logical.ends_in_newline():
        raise CodecError("non-empty decoded payload does not end in LF")

    return DecodeResult(logical=decoded, stored=stored)


def _read_at_most(source: _Readable | BinaryIO, count: int) -> bytes:
    """Reads up to `count` bytes, tolerating short reads from any stream."""
    collected = bytearray()
    while len(collected) < count:
        chunk = source.read(count - len(collected))
        if not chunk:
            break
        collected.extend(chunk)
    return bytes(collected)


def _check_frame_header(header: bytes) -> None:
    """Rejects a dictionary-dependent or unchecksummed frame before decoding it."""
    if len(header) < 5:
        raise CodecError("truncated Zstandard frame")
    if header[:4] != _ZSTD_MAGIC:
        raise CodecError("input is not a Zstandard frame")
    descriptor = header[4]
    if descriptor & 0b0000_1000:
        raise CodecError("Zstandard frame header sets a reserved bit")
    if not descriptor & 0b0000_0100:
        raise CodecError("frame carries no checksum; V1 frames are checksummed")
    dictionary_bytes = (0, 1, 2, 4)[descriptor & 0b0000_0011]
    if dictionary_bytes == 0:
        return
    start = 5 + (0 if descriptor & 0b0010_0000 else 1)
    if len(header) < start + dictionary_bytes:
        raise CodecError("truncated Zstandard frame")
    identifier = int.from_bytes(header[start : start + dictionary_bytes], "little")
    if identifier != 0:
        raise CodecError(
            f"frame requires dictionary {identifier}; V1 frames carry no dictionary"
        )


class _FrameBoundedReader:
    """A source that stops at the end of the first frame and notices what follows.

    The decoder needs three answers this wrapper produces in one pass over the
    compressed object: bytes to decode, the stored identity of the frame, and
    the exact byte at which that frame ended. The last one is why the Zstandard
    frame structure is walked here rather than inferred — libzstd's streaming
    readers buffer ahead, so "how much did the source hand over" does not answer
    "where did the frame stop", and a concatenated or padded object would
    otherwise decode as one healthy stream (§4.3).

    Walking the structure is cheap and total: a block header is three bytes and
    its contents are skipped by length, never inspected. Bytes past the frame
    are held aside rather than passed on, so the decoder cannot see them at all.
    """

    _MAGIC, _DESCRIPTOR, _HEADER_REST, _BLOCK_HEADER, _BLOCK_BODY, _CHECKSUM, _DONE = range(7)

    def __init__(self, source: _Readable | BinaryIO, *, prefix: bytes = b"") -> None:
        self._source = source
        self._queue = bytearray()
        self._trailing = bytearray()
        self._digest = hashlib.sha256()
        self._field = bytearray()
        self._state = self._MAGIC
        self._need = 4
        self._has_checksum = False
        self._block_remaining = 0
        self._last_block = False
        self.frame_complete = False
        self.frame_length = 0
        if prefix:
            self._feed(prefix)

    # -- the source protocol libzstd's stream_reader consumes ---------------

    def read(self, size: int = -1) -> bytes:
        """Hands out compressed bytes, never one past the end of the frame."""
        if size is None or size < 0:
            size = DECODE_INPUT_BYTES
        while not self._queue and not self.frame_complete:
            chunk = self._source.read(max(size, DECODE_INPUT_BYTES))
            if not chunk:
                break
            self._feed(chunk)
        if not self._queue or size == 0:
            return b""
        taken = bytes(self._queue[:size])
        del self._queue[: len(taken)]
        return taken

    # -- the frame walk -----------------------------------------------------

    def _feed(self, chunk: bytes) -> None:
        offset = 0
        if not self.frame_complete:
            while offset < len(chunk) and not self.frame_complete:
                offset = self._advance(chunk, offset)
            consumed = chunk[:offset]
            self._digest.update(consumed)
            self.frame_length += len(consumed)
            self._queue.extend(consumed)
        if offset < len(chunk):
            self._trailing.extend(chunk[offset:])

    def _advance(self, chunk: bytes, offset: int) -> int:
        """Consumes as much of `chunk` as the field being read still wants."""
        if self._state == self._BLOCK_BODY:
            step = min(self._block_remaining, len(chunk) - offset)
            self._block_remaining -= step
            if self._block_remaining == 0:
                self._end_of_block()
            return offset + step

        step = min(self._need - len(self._field), len(chunk) - offset)
        self._field.extend(chunk[offset : offset + step])
        offset += step
        if len(self._field) < self._need:
            return offset
        encoded = bytes(self._field)
        self._field.clear()

        if self._state == self._MAGIC:
            if encoded != _ZSTD_MAGIC:
                raise CodecError("input is not a Zstandard frame")
            self._state, self._need = self._DESCRIPTOR, 1
        elif self._state == self._DESCRIPTOR:
            self._read_descriptor(encoded[0])
        elif self._state == self._HEADER_REST:
            self._state, self._need = self._BLOCK_HEADER, 3
        elif self._state == self._BLOCK_HEADER:
            self._read_block_header(encoded)
        elif self._state == self._CHECKSUM:
            self._state = self._DONE
            self.frame_complete = True
        return offset

    def _read_descriptor(self, descriptor: int) -> None:
        if descriptor & 0b0000_1000:
            raise CodecError("Zstandard frame header sets a reserved bit")
        self._has_checksum = bool(descriptor & 0b0000_0100)
        single_segment = bool(descriptor & 0b0010_0000)
        dictionary_bytes = (0, 1, 2, 4)[descriptor & 0b0000_0011]
        # Flag 0 means "no field" unless the frame is a single segment, in which
        # case the content size is one byte. The rest of the table is 2/4/8.
        content_bytes = (1 if single_segment else 0, 2, 4, 8)[descriptor >> 6]
        remaining = (0 if single_segment else 1) + dictionary_bytes + content_bytes
        if remaining:
            self._state, self._need = self._HEADER_REST, remaining
        else:
            self._state, self._need = self._BLOCK_HEADER, 3

    def _read_block_header(self, encoded: bytes) -> None:
        header = int.from_bytes(encoded, "little")
        self._last_block = bool(header & 1)
        block_type = (header >> 1) & 0b11
        if block_type == 3:
            raise CodecError("Zstandard frame contains a reserved block type")
        # An RLE block occupies one stored byte however large it decodes to.
        self._block_remaining = 1 if block_type == 1 else header >> 3
        if self._block_remaining == 0:
            self._end_of_block()
        else:
            self._state = self._BLOCK_BODY

    def _end_of_block(self) -> None:
        if not self._last_block:
            self._state, self._need = self._BLOCK_HEADER, 3
            return
        if self._has_checksum:
            self._state, self._need = self._CHECKSUM, 4
            return
        # Only reachable through a caller that skipped the header check; the
        # frame is structurally over either way.
        self._state = self._DONE
        self.frame_complete = True

    # -- what the decoder asks afterwards -----------------------------------

    def has_trailing_bytes(self) -> bool:
        """True when anything at all follows the one frame this object may hold."""
        if self._trailing:
            return True
        return bool(self._source.read(1))

    def stored_identity(self) -> StoredIdentity:
        """Digest and length of the frame, which for a valid object is all of it."""
        return StoredIdentity(sha256=self._digest.hexdigest(), byte_length=self.frame_length)


def logical_identity_of(
    source: _Readable | BinaryIO,
    *,
    buffer_bytes: int = DEFAULT_BUFFER_BYTES,
) -> LogicalIdentity:
    """Digest, length and LF count of a stream, without compressing it."""
    logical = _LogicalCounter()
    while True:
        chunk = source.read(buffer_bytes)
        if not chunk:
            break
        logical.observe(chunk)
    return logical.identity()


def stored_identity_of(
    source: _Readable | BinaryIO,
    *,
    buffer_bytes: int = DEFAULT_BUFFER_BYTES,
) -> StoredIdentity:
    """Digest and length of an object's bytes exactly as they sit."""
    digest = hashlib.sha256()
    length = 0
    while True:
        chunk = source.read(buffer_bytes)
        if not chunk:
            break
        digest.update(chunk)
        length += len(chunk)
    return StoredIdentity(sha256=digest.hexdigest(), byte_length=length)
