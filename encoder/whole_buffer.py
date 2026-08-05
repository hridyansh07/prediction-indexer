"""Whole-buffer conveniences for tests and probes. **Not a production path.**

`ZSTD_MATERIALIZATION_PIPELINE_V1.md` §2.3 keeps these "only as small
test/convenience wrappers implemented on top of the streaming path, never as
production file APIs", and `PHASE_4_RAW_ARCHIVE_REAPER_V1.md` §4.2 states the
rule as a prohibition: no archiver, finalizer, replay, or deployment path may
call an API whose source or result is one complete `bytes` object.

They live in their own module rather than beside `encode_stream` so that the
prohibition is checkable — `tests/test_no_sbe.py` asserts that nothing outside
tests and probes imports this file. A helper that is merely documented as
test-only becomes a production call site the first time one is convenient.
"""

from __future__ import annotations

import io

from encoder.compression import (
    DECODE_INPUT_BYTES,
    DEFAULT_ZSTD_LEVEL,
    EncodeResult,
    LogicalIdentity,
    StoredIdentity,
    decode_stream,
    encode_stream,
)

__all__ = ["compress_bytes", "decompress_bytes", "encode_identity"]


def compress_bytes(payload: bytes, *, level: int = DEFAULT_ZSTD_LEVEL) -> bytes:
    """One frame over one in-memory payload."""
    sink = io.BytesIO()
    encode_stream(io.BytesIO(payload), sink, level=level)
    return sink.getvalue()


def encode_identity(payload: bytes, *, level: int = DEFAULT_ZSTD_LEVEL) -> EncodeResult:
    """Both identities for an in-memory payload, discarding the frame."""
    return encode_stream(io.BytesIO(payload), io.BytesIO(), level=level)


def decompress_bytes(
    frame: bytes,
    *,
    expected_logical: LogicalIdentity,
    expected_stored: StoredIdentity | None = None,
    max_decoded_bytes: int | None = None,
    buffer_bytes: int = DECODE_INPUT_BYTES,
) -> bytes:
    """Decodes one in-memory frame under the same strict rules as a stream."""
    sink = io.BytesIO()
    decode_stream(
        io.BytesIO(frame),
        sink,
        expected_logical=expected_logical,
        expected_stored=expected_stored,
        max_decoded_bytes=max_decoded_bytes,
        buffer_bytes=buffer_bytes,
    )
    return sink.getvalue()
