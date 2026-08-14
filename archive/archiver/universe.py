"""Per-segment control sidecars and their immutable universe commit records.

The raw archive receipt remains the authority for the complete segment.  This
module derives a smaller, exact view only after that receipt re-verifies:

```
<segment>.control.ndjson.zst  exact envelope lines whose kind is ``control``
<segment>.universe.json      immutable commit record, published last
```

The universe receipt is deliberately per segment.  A connection epoch may span
many segments and UTC dates; consumers fold controls in lane/delivery order and
must not treat a date prefix as an event or subscription boundary.

Failure here never changes raw archive state.  The caller reports this outcome
separately and retries it on a later sweep.
"""

from __future__ import annotations

import hashlib
import io
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from archive.common.receipts import ArchiveReceipt, compression_contract
from archive.common.verify import verify_archive
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    ObjectStore,
    VerificationFailure,
)
from encoder import (
    LogicalIdentity,
    StoredIdentity,
    encode_stream,
    encoder_version,
    stored_identity_of,
)

__all__ = [
    "FAILED",
    "PUBLISHED",
    "SKIPPED",
    "ControlObject",
    "ReceiptIdentity",
    "SegmentUniverseReceipt",
    "UniverseArtifactError",
    "UniversePublication",
    "parse_segment_universe_receipt",
    "publish_segment_universe",
    "read_segment_universe_receipt",
    "segment_universe_keys",
    "verify_segment_universe_receipt",
]

SEGMENT_UNIVERSE_RECEIPT_VERSION = 1
UNIVERSE_PUBLISHER_VERSION = 1
MAX_RECEIPT_BYTES = 1_048_576

PUBLISHED = "published"
SKIPPED = "skipped"
FAILED = "failed"


class UniverseArtifactError(ValueError):
    """A sidecar or universe receipt is malformed or fails verification."""


@dataclass(frozen=True)
class ReceiptIdentity:
    file: str
    sha256: str
    byte_length: int
    verified_at_ns: int


@dataclass(frozen=True)
class ControlObject:
    key: str
    logical: LogicalIdentity
    stored: StoredIdentity
    compression: dict[str, Any]
    first_delivery_index: int | None
    last_delivery_index: int | None


@dataclass(frozen=True)
class SegmentUniverseReceipt:
    key: str
    location: str
    lane_id: str
    window_start_ns: int
    window_end_ns: int
    segment_id: str
    segment_index: int
    source_archive_receipt: ReceiptIdentity
    source_file: str
    source_logical: LogicalIdentity
    data_key: str
    data_stored: StoredIdentity
    seal_key: str
    seal_stored: StoredIdentity
    control: ControlObject
    published_at_ns: int
    document: dict[str, Any]


@dataclass(frozen=True)
class UniversePublication:
    status: str
    receipt_key: str
    archive_receipt_sha256: str
    control_count: int


def segment_universe_keys(receipt: ArchiveReceipt) -> tuple[str, str]:
    """Return ``(control key, universe receipt key)`` beside the raw object."""
    suffix = ".ndjson.zst"
    if not receipt.data_key.endswith(suffix):
        raise UniverseArtifactError(
            f"archive object {receipt.data_key!r} is not a raw NDJSON Zstd segment"
        )
    stem = receipt.data_key[: -len(suffix)]
    return f"{stem}.control.ndjson.zst", f"{stem}.universe.json"


def publish_segment_universe(
    store: ObjectStore,
    archive_receipt: ArchiveReceipt,
    source_path: Path,
    *,
    now_ns: int,
) -> UniversePublication:
    """Publish one exact control sidecar and its receipt, idempotently.

    Only production raw receipts participate.  A conformance receipt is useful
    for testing raw archival but is not proof that the raw object belongs to an
    independently durable universe.
    """
    if not archive_receipt.is_production:
        raise UniverseArtifactError("a segment universe requires a production archive receipt")
    verify_archive(store, archive_receipt)
    source_path = Path(source_path)
    receipt_identity = _file_identity(archive_receipt.path)
    control_key, receipt_key = segment_universe_keys(archive_receipt)

    existing = store.head(receipt_key)
    if existing is not None:
        committed = read_segment_universe_receipt(store, receipt_key)
        _matches_archive(committed, archive_receipt, receipt_identity)
        verify_segment_universe_receipt(store, committed)
        return UniversePublication(
            status=SKIPPED,
            receipt_key=receipt_key,
            archive_receipt_sha256=receipt_identity.sha256,
            control_count=committed.control.logical.line_count,
        )

    with tempfile.TemporaryFile(mode="w+b", dir=source_path.parent) as controls:
        source_logical, first_control, last_control = _extract_controls(
            source_path, controls
        )
        if source_logical != archive_receipt.source:
            raise UniverseArtifactError(
                f"{source_path.name} no longer matches its archive receipt while extracting controls"
            )
        controls.seek(0)
        with tempfile.TemporaryFile(mode="w+b", dir=source_path.parent) as encoded:
            result = encode_stream(controls, encoded)
            encoded.flush()
            encoded.seek(0)
            metadata = store.put_immutable(
                control_key,
                encoded,
                result.stored,
                content_type=NDJSON_CONTENT_TYPE,
                content_encoding=ZSTD_CONTENT_ENCODING,
            )
    if not metadata.matches_request(
        result.stored, NDJSON_CONTENT_TYPE, ZSTD_CONTENT_ENCODING
    ):
        raise VerificationFailure(
            f"control sidecar {control_key} failed identity or metadata verification"
        )

    document = _build_document(
        archive_receipt,
        receipt_identity,
        control_key=control_key,
        control_logical=result.logical,
        control_stored=result.stored,
        first_control_delivery_index=first_control,
        last_control_delivery_index=last_control,
        published_at_ns=now_ns,
    )
    # Prove our own closed reader accepts the document before committing it.
    parsed = parse_segment_universe_receipt(document, key=receipt_key)
    verify_segment_universe_receipt(store, parsed)
    encoded_receipt = _canonical_json(document)
    with io.BytesIO(encoded_receipt) as reader:
        store.put_immutable(
            receipt_key,
            reader,
            stored_identity_of(io.BytesIO(encoded_receipt)),
            content_type=JSON_CONTENT_TYPE,
        )
    committed = read_segment_universe_receipt(store, receipt_key)
    _matches_archive(committed, archive_receipt, receipt_identity)
    return UniversePublication(
        status=PUBLISHED,
        receipt_key=receipt_key,
        archive_receipt_sha256=receipt_identity.sha256,
        control_count=result.logical.line_count,
    )


def read_segment_universe_receipt(
    store: ObjectStore, key: str
) -> SegmentUniverseReceipt:
    metadata = store.head(key)
    if metadata is None:
        raise UniverseArtifactError(f"segment universe receipt is absent: {key}")
    if metadata.byte_length > MAX_RECEIPT_BYTES:
        raise UniverseArtifactError(
            f"segment universe receipt {key} exceeds {MAX_RECEIPT_BYTES} bytes"
        )
    if metadata.content_type != JSON_CONTENT_TYPE or metadata.content_encoding is not None:
        raise UniverseArtifactError(f"segment universe receipt {key} has invalid content metadata")
    payload = _read_exact(store, key, metadata.stored, MAX_RECEIPT_BYTES)
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise UniverseArtifactError(f"invalid segment universe receipt {key}: {error}") from error
    receipt = parse_segment_universe_receipt(document, key=key)
    verify_segment_universe_receipt(store, receipt)
    return receipt


def parse_segment_universe_receipt(
    document: Any, *, key: str
) -> SegmentUniverseReceipt:
    def invalid(detail: str) -> UniverseArtifactError:
        return UniverseArtifactError(f"invalid segment universe receipt {key}: {detail}")

    _exact_object(
        document,
        {
            "segment_universe_receipt_version",
            "location",
            "authoritative",
            "lane_id",
            "window_start_ns",
            "window_end_ns",
            "segment_id",
            "segment_index",
            "source_archive_receipt",
            "source",
            "control",
            "published_at_ns",
            "universe_publisher_version",
        },
        invalid,
        "receipt",
    )
    assert isinstance(document, dict)
    if document["segment_universe_receipt_version"] != SEGMENT_UNIVERSE_RECEIPT_VERSION:
        raise invalid("unsupported receipt version")
    if document["authoritative"] is not True:
        raise invalid("authoritative must be true")
    if document["universe_publisher_version"] != UNIVERSE_PUBLISHER_VERSION:
        raise invalid("unsupported publisher version")
    location = _text(document, "location", invalid)
    lane = _text(document, "lane_id", invalid)
    start = _integer(document, "window_start_ns", invalid)
    end = _integer(document, "window_end_ns", invalid)
    if start >= end:
        raise invalid("window bounds are inverted")
    segment_id = _text(document, "segment_id", invalid)
    segment_index = _integer(document, "segment_index", invalid)

    raw_archive = _closed_section(
        document,
        "source_archive_receipt",
        {"file", "sha256", "byte_length", "verified_at_ns"},
        invalid,
    )
    archive_file = _basename(raw_archive, "file", invalid)
    if not archive_file.endswith(".archive.json"):
        raise invalid("source archive receipt is not a production receipt filename")
    archive_identity = ReceiptIdentity(
        file=archive_file,
        sha256=_digest(raw_archive, "sha256", invalid),
        byte_length=_integer(raw_archive, "byte_length", invalid),
        verified_at_ns=_integer(raw_archive, "verified_at_ns", invalid),
    )

    source = _closed_section(document, "source", {"file", "logical", "data", "seal"}, invalid)
    source_file = _basename(source, "file", invalid)
    logical = _logical(_closed_section(source, "logical", {"sha256", "byte_length", "line_count"}, invalid), invalid)
    data = _closed_section(source, "data", {"key", "stored"}, invalid)
    data_key = _text(data, "key", invalid)
    data_stored = _stored(_closed_section(data, "stored", {"sha256", "byte_length"}, invalid), invalid)
    seal = _closed_section(source, "seal", {"key", "stored"}, invalid)
    seal_key = _text(seal, "key", invalid)
    seal_stored = _stored(_closed_section(seal, "stored", {"sha256", "byte_length"}, invalid), invalid)

    control = _closed_section(
        document,
        "control",
        {
            "key",
            "content_type",
            "content_encoding",
            "logical",
            "stored",
            "compression",
            "first_delivery_index",
            "last_delivery_index",
        },
        invalid,
    )
    control_key = _text(control, "key", invalid)
    if (
        control["content_type"] != NDJSON_CONTENT_TYPE
        or control["content_encoding"] != ZSTD_CONTENT_ENCODING
    ):
        raise invalid("control object has invalid content metadata")
    control_logical = _logical(
        _closed_section(control, "logical", {"sha256", "byte_length", "line_count"}, invalid),
        invalid,
    )
    control_stored = _stored(
        _closed_section(control, "stored", {"sha256", "byte_length"}, invalid), invalid
    )
    _compression(control.get("compression"), invalid)
    first = _optional_integer(control, "first_delivery_index", invalid)
    last = _optional_integer(control, "last_delivery_index", invalid)
    if control_logical.line_count == 0:
        if first is not None or last is not None:
            raise invalid("empty control sidecar carries delivery bounds")
    elif first is None or last is None or first > last:
        raise invalid("non-empty control sidecar has invalid delivery bounds")

    expected_control, expected_receipt = _keys_from_data_key(data_key)
    if key != expected_receipt or control_key != expected_control:
        raise invalid("receipt or control key is not beside the source data object")
    if not source_file.endswith(".ndjson") or not data_key.endswith(
        source_file + ".zst"
    ):
        raise invalid("source filename and data key disagree")

    return SegmentUniverseReceipt(
        key=key,
        location=location,
        lane_id=lane,
        window_start_ns=start,
        window_end_ns=end,
        segment_id=segment_id,
        segment_index=segment_index,
        source_archive_receipt=archive_identity,
        source_file=source_file,
        source_logical=logical,
        data_key=data_key,
        data_stored=data_stored,
        seal_key=seal_key,
        seal_stored=seal_stored,
        control=ControlObject(
            key=control_key,
            logical=control_logical,
            stored=control_stored,
            compression=dict(control["compression"]),
            first_delivery_index=first,
            last_delivery_index=last,
        ),
        published_at_ns=_integer(document, "published_at_ns", invalid),
        document=document,
    )


def verify_segment_universe_receipt(
    store: ObjectStore, receipt: SegmentUniverseReceipt
) -> None:
    if receipt.location != store.store_id:
        raise UniverseArtifactError(
            f"segment universe receipt names {receipt.location!r}, not {store.store_id!r}"
        )
    for key, expected, content_type, content_encoding in (
        (
            receipt.data_key,
            receipt.data_stored,
            NDJSON_CONTENT_TYPE,
            ZSTD_CONTENT_ENCODING,
        ),
        (receipt.seal_key, receipt.seal_stored, JSON_CONTENT_TYPE, None),
        (
            receipt.control.key,
            receipt.control.stored,
            NDJSON_CONTENT_TYPE,
            ZSTD_CONTENT_ENCODING,
        ),
    ):
        metadata = store.head(key)
        if metadata is None or not metadata.matches_request(
            expected, content_type, content_encoding
        ):
            raise UniverseArtifactError(
                f"segment universe object {key} does not match its committed identity"
            )


def _extract_controls(
    source_path: Path, destination: BinaryIO
) -> tuple[LogicalIdentity, int | None, int | None]:
    digest = hashlib.sha256()
    byte_length = 0
    line_count = 0
    first: int | None = None
    last: int | None = None
    with source_path.open("rb") as source:
        for line_number, line in enumerate(source, 1):
            digest.update(line)
            byte_length += len(line)
            line_count += line.count(b"\n")
            if not line.endswith(b"\n") or not line.strip():
                raise UniverseArtifactError(
                    f"{source_path.name}:{line_number} is not one complete NDJSON record"
                )
            try:
                envelope = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise UniverseArtifactError(
                    f"{source_path.name}:{line_number} is invalid envelope JSON: {error}"
                ) from error
            if not isinstance(envelope, dict) or not isinstance(envelope.get("kind"), str):
                raise UniverseArtifactError(
                    f"{source_path.name}:{line_number} has no envelope kind"
                )
            if envelope["kind"] != "control":
                continue
            delivery = envelope.get("delivery_index")
            if not isinstance(delivery, int) or isinstance(delivery, bool) or delivery < 0:
                raise UniverseArtifactError(
                    f"{source_path.name}:{line_number} has invalid delivery_index"
                )
            destination.write(line)
            first = delivery if first is None else first
            last = delivery
    return (
        LogicalIdentity(
            sha256=digest.hexdigest(),
            byte_length=byte_length,
            line_count=line_count,
        ),
        first,
        last,
    )


def _build_document(
    receipt: ArchiveReceipt,
    receipt_identity: StoredIdentity,
    *,
    control_key: str,
    control_logical: LogicalIdentity,
    control_stored: StoredIdentity,
    first_control_delivery_index: int | None,
    last_control_delivery_index: int | None,
    published_at_ns: int,
) -> dict[str, Any]:
    return {
        "segment_universe_receipt_version": SEGMENT_UNIVERSE_RECEIPT_VERSION,
        "location": receipt.location,
        "authoritative": True,
        "lane_id": receipt.lane_id,
        "window_start_ns": receipt.window_start_ns,
        "window_end_ns": receipt.window_end_ns,
        "segment_id": receipt.segment_id,
        "segment_index": receipt.segment_index,
        "source_archive_receipt": {
            "file": receipt.path.name,
            "sha256": receipt_identity.sha256,
            "byte_length": receipt_identity.byte_length,
            "verified_at_ns": receipt.verified_at_ns,
        },
        "source": {
            "file": receipt.source_file,
            "logical": receipt.source.as_record(),
            "data": {
                "key": receipt.data_key,
                "stored": receipt.data_stored.as_record(),
            },
            "seal": {
                "key": receipt.seal_key,
                "stored": receipt.seal_stored.as_record(),
            },
        },
        "control": {
            "key": control_key,
            "content_type": NDJSON_CONTENT_TYPE,
            "content_encoding": ZSTD_CONTENT_ENCODING,
            "logical": control_logical.as_record(),
            "stored": control_stored.as_record(),
            "compression": compression_contract(encoder_version()),
            "first_delivery_index": first_control_delivery_index,
            "last_delivery_index": last_control_delivery_index,
        },
        "published_at_ns": published_at_ns,
        "universe_publisher_version": UNIVERSE_PUBLISHER_VERSION,
    }


def _matches_archive(
    universe: SegmentUniverseReceipt,
    archive: ArchiveReceipt,
    archive_identity: StoredIdentity,
) -> None:
    if (
        universe.location != archive.location
        or universe.lane_id != archive.lane_id
        or universe.window_start_ns != archive.window_start_ns
        or universe.window_end_ns != archive.window_end_ns
        or universe.segment_id != archive.segment_id
        or universe.segment_index != archive.segment_index
        or universe.source_file != archive.source_file
        or universe.source_logical != archive.source
        or universe.data_key != archive.data_key
        or universe.data_stored != archive.data_stored
        or universe.seal_key != archive.seal_key
        or universe.seal_stored != archive.seal_stored
        or universe.source_archive_receipt.file != archive.path.name
        or universe.source_archive_receipt.sha256 != archive_identity.sha256
        or universe.source_archive_receipt.byte_length != archive_identity.byte_length
    ):
        raise UniverseArtifactError(
            f"segment universe receipt {universe.key} does not bind the supplied raw archive receipt"
        )


def _read_exact(
    store: ObjectStore, key: str, expected: StoredIdentity, maximum: int
) -> bytes:
    digest = hashlib.sha256()
    payload = bytearray()
    with store.open(key, max_bytes=min(expected.byte_length, maximum)) as source:
        while chunk := source.read(64 * 1024):
            digest.update(chunk)
            payload.extend(chunk)
    if len(payload) != expected.byte_length or digest.hexdigest() != expected.sha256:
        raise UniverseArtifactError(f"object {key} changed between head and read")
    return bytes(payload)


def _file_identity(path: Path) -> StoredIdentity:
    with Path(path).open("rb") as handle:
        return stored_identity_of(handle)


def _canonical_json(document: dict[str, Any]) -> bytes:
    return (
        json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _keys_from_data_key(data_key: str) -> tuple[str, str]:
    suffix = ".ndjson.zst"
    if not data_key.endswith(suffix):
        raise UniverseArtifactError("source data key has the wrong suffix")
    stem = data_key[: -len(suffix)]
    return f"{stem}.control.ndjson.zst", f"{stem}.universe.json"


def _exact_object(value: Any, expected: set[str], invalid, label: str) -> None:
    if not isinstance(value, dict) or set(value) != expected:
        raise invalid(f"{label} fields are invalid")


def _closed_section(document: dict[str, Any], field: str, expected: set[str], invalid):
    value = document.get(field)
    _exact_object(value, expected, invalid, field)
    return value


def _text(document: dict[str, Any], field: str, invalid) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise invalid(f"{field} is not non-empty text")
    return value


def _basename(document: dict[str, Any], field: str, invalid) -> str:
    value = _text(document, field, invalid)
    if Path(value).name != value or "\\" in value:
        raise invalid(f"{field} is not a bare filename")
    return value


def _integer(document: dict[str, Any], field: str, invalid) -> int:
    value = document.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise invalid(f"{field} is not a non-negative integer")
    return value


def _optional_integer(document: dict[str, Any], field: str, invalid) -> int | None:
    value = document.get(field)
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise invalid(f"{field} is not a non-negative integer or null")
    return value


def _digest(document: dict[str, Any], field: str, invalid) -> str:
    value = _text(document, field, invalid)
    if len(value) != 64 or value != value.lower() or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise invalid(f"{field} is not a SHA-256 digest")
    return value


def _stored(document: dict[str, Any], invalid) -> StoredIdentity:
    return StoredIdentity(
        sha256=_digest(document, "sha256", invalid),
        byte_length=_integer(document, "byte_length", invalid),
    )


def _logical(document: dict[str, Any], invalid) -> LogicalIdentity:
    return LogicalIdentity(
        sha256=_digest(document, "sha256", invalid),
        byte_length=_integer(document, "byte_length", invalid),
        line_count=_integer(document, "line_count", invalid),
    )


def _compression(value: Any, invalid) -> None:
    if not isinstance(value, dict):
        raise invalid("compression is not an object")
    encoder = value.get("encoder")
    if not isinstance(encoder, str) or not encoder:
        raise invalid("compression.encoder is required")
    expected = compression_contract(encoder)
    if set(value) != set(expected) or any(value.get(key) != item for key, item in expected.items()):
        raise invalid("compression contract is invalid")
