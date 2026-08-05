"""The shared Zstandard codec boundary.

One format, one frame per object, two identities, streaming only. See
`encoder/compression.py` for the contract and `encoder/README.md` for why this
package no longer carries a message-encoding layer.
"""

from encoder.compression import (
    DECODE_INPUT_BYTES,
    DEFAULT_BUFFER_BYTES,
    DEFAULT_ZSTD_LEVEL,
    CodecError,
    DecodeLimitExceeded,
    DecodeResult,
    EncodeResult,
    IdentityMismatch,
    LogicalIdentity,
    StoredIdentity,
    decode_stream,
    encode_stream,
    encoder_version,
    logical_identity_of,
    stored_identity_of,
)

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
