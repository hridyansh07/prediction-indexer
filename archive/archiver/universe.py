"""Per-segment control sidecars and their immutable universe commit records.

The raw archive receipt remains the authority for the complete segment.  This
module derives a smaller, exact view only after that receipt re-verifies:

```
<segment>.archive-receipt-mirror.json  exact receipt bytes, no commit/delete authority
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

import base64
import binascii
import hashlib
import io
import json
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from archive.common.receipts import (
    ArchiveReceipt,
    ReceiptError,
    compression_contract,
    parse_archive_receipt,
)
from archive.common.verify import decode_archived_segment, verify_archive
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
    "ArchiveReceiptMirror",
    "ControlObject",
    "MirrorPublication",
    "ReceiptIdentity",
    "SegmentUniverseReceipt",
    "UniverseArtifactError",
    "UniversePublication",
    "archive_receipt_mirror_key",
    "parse_segment_universe_receipt",
    "publish_archive_receipt_mirror",
    "publish_segment_universe",
    "publish_segment_universe_from_archive",
    "read_archive_receipt_mirror",
    "read_segment_universe_receipt",
    "segment_universe_keys",
    "verify_segment_universe_receipt",
]

SEGMENT_UNIVERSE_RECEIPT_VERSION = 1
ARCHIVE_RECEIPT_MIRROR_VERSION = 1
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
    mirror_key: str
    mirror_stored: StoredIdentity


@dataclass(frozen=True)
class ArchiveReceiptMirror:
    key: str
    stored: StoredIdentity
    receipt_identity: StoredIdentity
    receipt: ArchiveReceipt
    document: dict[str, Any]


@dataclass(frozen=True)
class MirrorPublication:
    status: str
    mirror: ArchiveReceiptMirror


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


def archive_receipt_mirror_key(receipt: ArchiveReceipt) -> str:
    """S3 key for a non-authoritative mirror of one committed local receipt."""
    suffix = ".ndjson.zst"
    if not receipt.data_key.endswith(suffix):
        raise UniverseArtifactError(
            f"archive object {receipt.data_key!r} is not a raw NDJSON Zstd segment"
        )
    return receipt.data_key[: -len(suffix)] + ".archive-receipt-mirror.json"


def segment_universe_keys(receipt: ArchiveReceipt) -> tuple[str, str]:
    """Return ``(control key, universe receipt key)`` beside the raw object."""
    suffix = ".ndjson.zst"
    if not receipt.data_key.endswith(suffix):
        raise UniverseArtifactError(
            f"archive object {receipt.data_key!r} is not a raw NDJSON Zstd segment"
        )
    stem = receipt.data_key[: -len(suffix)]
    return f"{stem}.control.ndjson.zst", f"{stem}.universe.json"


def publish_archive_receipt_mirror(
    store: ObjectStore, archive_receipt: ArchiveReceipt
) -> MirrorPublication:
    """Mirror exact committed receipt bytes for remote derivative discovery.

    The wrapper is deliberately not an archive receipt and explicitly carries
    no deletion authority.  Its only role is to attest that the capture-side
    archiver observed a committed production receipt, while preserving the
    exact receipt bytes a remote universe worker must parse and reverify.
    """
    if not archive_receipt.is_production:
        raise UniverseArtifactError("only a production archive receipt can be mirrored")
    verify_archive(store, archive_receipt)
    try:
        receipt_bytes = archive_receipt.path.read_bytes()
    except OSError as error:
        raise UniverseArtifactError(
            f"cannot read archive receipt {archive_receipt.path}: {error}"
        ) from error
    if len(receipt_bytes) > MAX_RECEIPT_BYTES:
        raise UniverseArtifactError(
            f"archive receipt {archive_receipt.path.name} exceeds {MAX_RECEIPT_BYTES} bytes"
        )
    receipt_identity = stored_identity_of(io.BytesIO(receipt_bytes))
    key = archive_receipt_mirror_key(archive_receipt)
    document = {
        "raw_archive_receipt_mirror_version": ARCHIVE_RECEIPT_MIRROR_VERSION,
        "authoritative_commit_marker": False,
        "authorizes_deletion": False,
        "location": archive_receipt.location,
        "source_receipt": {
            "file": archive_receipt.path.name,
            "sha256": receipt_identity.sha256,
            "byte_length": receipt_identity.byte_length,
            "bytes_base64": base64.b64encode(receipt_bytes).decode("ascii"),
        },
        "universe_publisher_version": UNIVERSE_PUBLISHER_VERSION,
    }
    encoded = _canonical_json(document)
    expected = stored_identity_of(io.BytesIO(encoded))
    existing = store.head(key)
    if existing is not None:
        mirror = read_archive_receipt_mirror(store, key)
        _mirror_matches_receipt(mirror, archive_receipt, receipt_identity)
        return MirrorPublication(SKIPPED, mirror)
    with io.BytesIO(encoded) as source:
        store.put_immutable(key, source, expected, content_type=JSON_CONTENT_TYPE)
    mirror = read_archive_receipt_mirror(store, key)
    _mirror_matches_receipt(mirror, archive_receipt, receipt_identity)
    return MirrorPublication(PUBLISHED, mirror)


def read_archive_receipt_mirror(
    store: ObjectStore, key: str
) -> ArchiveReceiptMirror:
    metadata = store.head(key)
    if metadata is None:
        raise UniverseArtifactError(f"archive receipt mirror is absent: {key}")
    if metadata.byte_length > MAX_RECEIPT_BYTES * 2:
        raise UniverseArtifactError(f"archive receipt mirror {key} is too large")
    if metadata.content_type != JSON_CONTENT_TYPE or metadata.content_encoding is not None:
        raise UniverseArtifactError(f"archive receipt mirror {key} has invalid content metadata")
    payload = _read_exact(store, key, metadata.stored, MAX_RECEIPT_BYTES * 2)
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise UniverseArtifactError(f"invalid archive receipt mirror {key}: {error}") from error

    def invalid(detail: str) -> UniverseArtifactError:
        return UniverseArtifactError(f"invalid archive receipt mirror {key}: {detail}")

    _exact_object(
        document,
        {
            "raw_archive_receipt_mirror_version",
            "authoritative_commit_marker",
            "authorizes_deletion",
            "location",
            "source_receipt",
            "universe_publisher_version",
        },
        invalid,
        "mirror",
    )
    assert isinstance(document, dict)
    if document["raw_archive_receipt_mirror_version"] != ARCHIVE_RECEIPT_MIRROR_VERSION:
        raise UniverseArtifactError(f"archive receipt mirror {key} has unsupported version")
    if document["authoritative_commit_marker"] is not False:
        raise UniverseArtifactError(f"archive receipt mirror {key} claims commit authority")
    if document["authorizes_deletion"] is not False:
        raise UniverseArtifactError(f"archive receipt mirror {key} claims deletion authority")
    if document["universe_publisher_version"] != UNIVERSE_PUBLISHER_VERSION:
        raise UniverseArtifactError(f"archive receipt mirror {key} has unsupported publisher")
    if document["location"] != store.store_id:
        raise UniverseArtifactError(
            f"archive receipt mirror names {document['location']!r}, not {store.store_id!r}"
        )
    source = _closed_section(
        document,
        "source_receipt",
        {"file", "sha256", "byte_length", "bytes_base64"},
        invalid,
    )
    receipt_file = _basename(source, "file", invalid)
    receipt_identity = StoredIdentity(
        sha256=_digest(source, "sha256", invalid),
        byte_length=_integer(source, "byte_length", invalid),
    )
    encoded_receipt = _text(source, "bytes_base64", invalid)
    try:
        receipt_bytes = base64.b64decode(encoded_receipt, validate=True)
    except (binascii.Error, ValueError) as error:
        raise UniverseArtifactError(
            f"archive receipt mirror {key} has invalid receipt bytes"
        ) from error
    actual_identity = stored_identity_of(io.BytesIO(receipt_bytes))
    if actual_identity != receipt_identity:
        raise UniverseArtifactError(
            f"archive receipt mirror {key} receipt bytes do not match their identity"
        )
    try:
        receipt_document = json.loads(receipt_bytes)
        receipt = parse_archive_receipt(receipt_document, path=Path(receipt_file))
    except (UnicodeDecodeError, json.JSONDecodeError, ReceiptError) as error:
        raise UniverseArtifactError(
            f"archive receipt mirror {key} contains an invalid receipt: {error}"
        ) from error
    if not receipt.is_production:
        raise UniverseArtifactError(f"archive receipt mirror {key} is not production evidence")
    if receipt.location != store.store_id or key != archive_receipt_mirror_key(receipt):
        raise UniverseArtifactError(
            f"archive receipt mirror {key} does not match its archive location or object key"
        )
    verify_archive(store, receipt)
    return ArchiveReceiptMirror(
        key=key,
        stored=metadata.stored,
        receipt_identity=receipt_identity,
        receipt=receipt,
        document=document,
    )


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
    mirror = publish_archive_receipt_mirror(store, archive_receipt).mirror
    existing = _existing_universe(store, archive_receipt, mirror)
    if existing is not None:
        return existing

    with tempfile.TemporaryFile(mode="w+b", dir=source_path.parent) as controls:
        source_logical, first_control, last_control = _extract_controls(
            source_path, controls
        )
        if source_logical != archive_receipt.source:
            raise UniverseArtifactError(
                f"{source_path.name} no longer matches its archive receipt while extracting controls"
            )
        return _publish_controls(
            store,
            archive_receipt,
            mirror,
            controls,
            first_control_delivery_index=first_control,
            last_control_delivery_index=last_control,
            temp_root=source_path.parent,
            now_ns=now_ns,
        )


def publish_segment_universe_from_archive(
    store: ObjectStore,
    mirror: ArchiveReceiptMirror,
    *,
    now_ns: int,
    temp_root: Path | None = None,
) -> UniversePublication:
    """Stream one S3 archive through the shared strict decoder into controls.

    Only control lines are staged. The decoded raw segment is never retained on
    the universe host, so historical volume does not accumulate on its disk.
    """
    archive_receipt = mirror.receipt
    existing = _existing_universe(store, archive_receipt, mirror)
    if existing is not None:
        return existing
    if temp_root is not None:
        Path(temp_root).mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryFile(mode="w+b", dir=temp_root) as controls:
        extractor = _ControlLineExtractor(controls, archive_receipt.source_file)
        decode_archived_segment(store, archive_receipt, extractor)
        first_control, last_control = extractor.finish()
        return _publish_controls(
            store,
            archive_receipt,
            mirror,
            controls,
            first_control_delivery_index=first_control,
            last_control_delivery_index=last_control,
            temp_root=temp_root,
            now_ns=now_ns,
        )


def _existing_universe(
    store: ObjectStore,
    archive_receipt: ArchiveReceipt,
    mirror: ArchiveReceiptMirror,
) -> UniversePublication | None:
    _, receipt_key = segment_universe_keys(archive_receipt)
    if store.head(receipt_key) is None:
        return None
    committed = read_segment_universe_receipt(store, receipt_key)
    _matches_archive(committed, archive_receipt, mirror)
    return UniversePublication(
        status=SKIPPED,
        receipt_key=receipt_key,
        archive_receipt_sha256=mirror.receipt_identity.sha256,
        control_count=committed.control.logical.line_count,
    )


def _publish_controls(
    store: ObjectStore,
    archive_receipt: ArchiveReceipt,
    mirror: ArchiveReceiptMirror,
    controls: BinaryIO,
    *,
    first_control_delivery_index: int | None,
    last_control_delivery_index: int | None,
    temp_root: Path | None,
    now_ns: int,
) -> UniversePublication:
    control_key, receipt_key = segment_universe_keys(archive_receipt)
    controls.seek(0)
    with tempfile.TemporaryFile(mode="w+b", dir=temp_root) as encoded:
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
        mirror,
        control_key=control_key,
        control_logical=result.logical,
        control_stored=result.stored,
        first_control_delivery_index=first_control_delivery_index,
        last_control_delivery_index=last_control_delivery_index,
        published_at_ns=now_ns,
    )
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
    _matches_archive(committed, archive_receipt, mirror)
    return UniversePublication(
        status=PUBLISHED,
        receipt_key=receipt_key,
        archive_receipt_sha256=mirror.receipt_identity.sha256,
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
        {"file", "sha256", "byte_length", "verified_at_ns", "mirror"},
        invalid,
    )
    archive_file = _basename(raw_archive, "file", invalid)
    if not archive_file.endswith(".archive.json"):
        raise invalid("source archive receipt is not a production receipt filename")
    archive_mirror = _closed_section(
        raw_archive, "mirror", {"key", "stored"}, invalid
    )
    archive_identity = ReceiptIdentity(
        file=archive_file,
        sha256=_digest(raw_archive, "sha256", invalid),
        byte_length=_integer(raw_archive, "byte_length", invalid),
        verified_at_ns=_integer(raw_archive, "verified_at_ns", invalid),
        mirror_key=_text(archive_mirror, "key", invalid),
        mirror_stored=_stored(
            _closed_section(
                archive_mirror, "stored", {"sha256", "byte_length"}, invalid
            ),
            invalid,
        ),
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
    if archive_identity.mirror_key != _mirror_key_from_data_key(data_key):
        raise invalid("archive receipt mirror is not beside the source data object")
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
        (
            receipt.source_archive_receipt.mirror_key,
            receipt.source_archive_receipt.mirror_stored,
            JSON_CONTENT_TYPE,
            None,
        ),
    ):
        metadata = store.head(key)
        if metadata is None or not metadata.matches_request(
            expected, content_type, content_encoding
        ):
            raise UniverseArtifactError(
                f"segment universe object {key} does not match its committed identity"
            )


class _ControlLineExtractor:
    """Binary sink that retains only complete control-envelope lines."""

    def __init__(self, destination: BinaryIO, label: str) -> None:
        self.destination = destination
        self.label = label
        self.pending = bytearray()
        self.line_number = 0
        self.first: int | None = None
        self.last: int | None = None

    def write(self, chunk: bytes) -> int:
        size = len(chunk)
        self.pending.extend(chunk)
        consumed = 0
        while True:
            newline = self.pending.find(b"\n", consumed)
            if newline < 0:
                break
            line = bytes(self.pending[consumed : newline + 1])
            consumed = newline + 1
            self.line_number += 1
            delivery = _control_delivery(line, self.label, self.line_number)
            if delivery is None:
                continue
            self.destination.write(line)
            self.first = delivery if self.first is None else self.first
            self.last = delivery
        if consumed:
            del self.pending[:consumed]
        return size

    def finish(self) -> tuple[int | None, int | None]:
        if self.pending:
            raise UniverseArtifactError(
                f"{self.label}:{self.line_number + 1} is not one complete NDJSON record"
            )
        self.destination.flush()
        return self.first, self.last


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
            delivery = _control_delivery(line, source_path.name, line_number)
            if delivery is None:
                continue
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


def _control_delivery(line: bytes, label: str, line_number: int) -> int | None:
    if not line.endswith(b"\n") or not line.strip():
        raise UniverseArtifactError(
            f"{label}:{line_number} is not one complete NDJSON record"
        )
    try:
        envelope = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise UniverseArtifactError(
            f"{label}:{line_number} is invalid envelope JSON: {error}"
        ) from error
    if not isinstance(envelope, dict) or not isinstance(envelope.get("kind"), str):
        raise UniverseArtifactError(f"{label}:{line_number} has no envelope kind")
    if envelope["kind"] != "control":
        return None
    delivery = envelope.get("delivery_index")
    if not isinstance(delivery, int) or isinstance(delivery, bool) or delivery < 0:
        raise UniverseArtifactError(f"{label}:{line_number} has invalid delivery_index")
    return delivery


def _build_document(
    receipt: ArchiveReceipt,
    mirror: ArchiveReceiptMirror,
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
            "sha256": mirror.receipt_identity.sha256,
            "byte_length": mirror.receipt_identity.byte_length,
            "verified_at_ns": receipt.verified_at_ns,
            "mirror": {
                "key": mirror.key,
                "stored": mirror.stored.as_record(),
            },
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
    mirror: ArchiveReceiptMirror,
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
        or universe.source_archive_receipt.sha256 != mirror.receipt_identity.sha256
        or universe.source_archive_receipt.byte_length != mirror.receipt_identity.byte_length
        or universe.source_archive_receipt.mirror_key != mirror.key
        or universe.source_archive_receipt.mirror_stored != mirror.stored
    ):
        raise UniverseArtifactError(
            f"segment universe receipt {universe.key} does not bind the supplied raw archive receipt"
        )


def _mirror_matches_receipt(
    mirror: ArchiveReceiptMirror,
    receipt: ArchiveReceipt,
    receipt_identity: StoredIdentity,
) -> None:
    mirrored = mirror.receipt
    if (
        mirror.receipt_identity != receipt_identity
        or mirrored.document != receipt.document
        or mirrored.location != receipt.location
        or mirrored.data_key != receipt.data_key
        or mirrored.seal_key != receipt.seal_key
        or mirrored.source != receipt.source
    ):
        raise UniverseArtifactError(
            f"archive receipt mirror {mirror.key} does not match {receipt.path.name}"
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


def _mirror_key_from_data_key(data_key: str) -> str:
    suffix = ".ndjson.zst"
    if not data_key.endswith(suffix):
        raise UniverseArtifactError("source data key has the wrong suffix")
    return data_key[: -len(suffix)] + ".archive-receipt-mirror.json"


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
