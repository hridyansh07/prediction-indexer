"""Archive receipts and the canonical receipts the reaper reads beside them.

Two archive receipt shapes exist, and keeping them apart is a safety property
rather than a schema preference:

```text
<segment>.archive.json        archive_receipt_version: 1        deletion authority
<segment>.archive.local.json  local_archive_receipt_version: 1  proof of control flow only
```

§5.3: "Local conformance metadata must not be serialized so that it can be
mistaken for an S3-verified `archive_receipt_version: 1`." The local receipt
therefore carries a *different* version key, a different filename, a store
identifier instead of a bucket, and no provider-checksum claim at all. Manifest
builders and the reaper ignore it entirely. Which one an archiver writes is
decided by the backend's declared durability class, never by a flag at the call
site.

The production shape is normative and comes from
`ZSTD_MATERIALIZATION_PIPELINE_V1.md` §3.3. Field names, lowercase hex digests,
integer byte counts and UTC Unix nanoseconds are all part of the contract, so
this module both writes and re-validates it rather than trusting the writer.

The canonical receipt reader here mirrors `read_receipt` in
`ingester/crates/finalize/src/canonical.rs`. The reaper needs a *committed*
canonical window, and "the file parses" is not that: an empty object parses, and
a receipt whose evidence file was deleted parses.
"""

from __future__ import annotations

import base64
import json
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from encoder import (
    DEFAULT_ZSTD_LEVEL,
    LogicalIdentity,
    StoredIdentity,
    stored_identity_of,
)

__all__ = [
    "ARCHIVER_VERSION",
    "ARCHIVE_RECEIPT_SUFFIX",
    "ARCHIVE_RECEIPT_VERSION",
    "CANONICAL_RECEIPT_FILE",
    "LOCAL_ARCHIVE_RECEIPT_SUFFIX",
    "LOCAL_ARCHIVE_RECEIPT_VERSION",
    "ArchiveReceipt",
    "CanonicalIndex",
    "CanonicalInput",
    "CanonicalOutput",
    "CanonicalReceipt",
    "ReceiptError",
    "archive_receipt_path",
    "build_archive_receipt",
    "compression_contract",
    "iter_canonical_receipts",
    "parse_archive_receipt",
    "read_archive_receipt",
    "read_canonical_receipt",
]

ARCHIVE_RECEIPT_VERSION = 2
LOCAL_ARCHIVE_RECEIPT_VERSION = 1
ARCHIVER_VERSION = 1

ARCHIVE_RECEIPT_SUFFIX = ".archive.json"
LOCAL_ARCHIVE_RECEIPT_SUFFIX = ".archive.local.json"
CANONICAL_RECEIPT_FILE = "receipt.json"

PRODUCTION = "production"
LOCAL = "local"


class ReceiptError(ValueError):
    """A receipt is malformed, or claims something it does not establish."""


def compression_contract(encoder: str) -> dict[str, Any]:
    """The V1 compression block. Every field is checked on the way back in."""
    return {
        "algorithm": "zstd",
        "level": DEFAULT_ZSTD_LEVEL,
        "frame_checksum": True,
        "dictionary": None,
        "frame_count": 1,
        "encoder": encoder,
    }


@dataclass(frozen=True)
class ArchiveReceipt:
    """A validated archive receipt, in the shape its readers actually use."""

    kind: str
    path: Path
    lane_id: str
    window_start_ns: int
    window_end_ns: int
    segment_id: str
    segment_index: int
    source_file: str
    source: LogicalIdentity
    seal_file: str
    seal_key: str
    seal_stored: StoredIdentity
    data_key: str
    data_stored: StoredIdentity
    provider: str | None
    location: str
    provider_checksum: str | None
    provider_checksum_algorithm: str | None
    seal_provider_checksum: str | None
    seal_provider_checksum_algorithm: str | None
    content_encoding: str
    compression: dict[str, Any]
    verified_at_ns: int
    archiver_version: int
    document: dict[str, Any]

    @property
    def is_production(self) -> bool:
        """Whether this receipt can take part in a deletion decision (§5.3)."""
        return self.kind == PRODUCTION


def archive_receipt_path(data_path: Path, kind: str) -> Path:
    stem = Path(data_path).name
    if stem.endswith(".ndjson"):
        stem = stem[: -len(".ndjson")]
    suffix = ARCHIVE_RECEIPT_SUFFIX if kind == PRODUCTION else LOCAL_ARCHIVE_RECEIPT_SUFFIX
    return Path(data_path).with_name(stem + suffix)


def build_archive_receipt(
    *,
    kind: str,
    lane_id: str,
    window_start_ns: int,
    window_end_ns: int,
    segment_id: str,
    segment_index: int,
    source_file: str,
    source: LogicalIdentity,
    seal_file: str,
    seal_key: str,
    seal_stored: StoredIdentity,
    data_key: str,
    data_stored: StoredIdentity,
    provider: str,
    location: str,
    provider_checksum: str | None,
    provider_checksum_algorithm: str | None,
    seal_provider_checksum: str | None,
    seal_provider_checksum_algorithm: str | None,
    encoder: str,
    verified_at_ns: int,
) -> dict[str, Any]:
    """Serializes whichever receipt the backend's durability class permits."""
    if kind == PRODUCTION:
        if not all(
            (
                provider,
                provider_checksum,
                provider_checksum_algorithm,
                seal_provider_checksum,
                seal_provider_checksum_algorithm,
            )
        ):
            raise ReceiptError("a production archive receipt requires provider identity and checksums")
        return {
            "archive_receipt_version": ARCHIVE_RECEIPT_VERSION,
            "store": {"provider": provider, "location": location},
            "lane_id": lane_id,
            "window_start_ns": window_start_ns,
            "window_end_ns": window_end_ns,
            "segment_id": segment_id,
            "segment_index": segment_index,
            "source": {
                "file": source_file,
                "byte_length": source.byte_length,
                "line_count": source.line_count,
                "sha256": source.sha256,
            },
            "seal": {
                "file": seal_file,
                "byte_length": seal_stored.byte_length,
                "sha256": seal_stored.sha256,
                "key": seal_key,
                "provider_checksum": seal_provider_checksum,
                "provider_checksum_algorithm": seal_provider_checksum_algorithm,
            },
            "object": {
                "key": data_key,
                "byte_length": data_stored.byte_length,
                "sha256": data_stored.sha256,
                "provider_checksum": provider_checksum,
                "provider_checksum_algorithm": provider_checksum_algorithm,
                "content_encoding": "zstd",
            },
            "compression": compression_contract(encoder),
            "verified_at_ns": verified_at_ns,
            "archiver_version": ARCHIVER_VERSION,
        }
    if kind != LOCAL:
        raise ReceiptError(f"unknown archive receipt kind {kind!r}")
    return {
        # Deliberately *not* `archive_receipt_version`. A deployment that later
        # gains an S3 adapter must not find this file and read it as proof that
        # a durable copy exists.
        "local_archive_receipt_version": LOCAL_ARCHIVE_RECEIPT_VERSION,
        "lane_id": lane_id,
        "window_start_ns": window_start_ns,
        "window_end_ns": window_end_ns,
        "segment_id": segment_id,
        "segment_index": segment_index,
        "source": {
            "file": source_file,
            "byte_length": source.byte_length,
            "line_count": source.line_count,
            "sha256": source.sha256,
        },
        "seal": {
            "file": seal_file,
            "byte_length": seal_stored.byte_length,
            "sha256": seal_stored.sha256,
            "store": location,
            "key": seal_key,
        },
        "object": {
            "store": location,
            "key": data_key,
            "byte_length": data_stored.byte_length,
            "sha256": data_stored.sha256,
            "content_encoding": "zstd",
        },
        "compression": compression_contract(encoder),
        "verified_at_ns": verified_at_ns,
        "archiver_version": ARCHIVER_VERSION,
        "durability": "local_conformance",
        "authorizes_deletion": False,
    }


def read_archive_receipt(path: Path) -> ArchiveReceipt:
    path = Path(path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ReceiptError(f"unreadable archive receipt {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise ReceiptError(f"invalid archive receipt {path}: {error}") from error
    return parse_archive_receipt(document, path=path)


def parse_archive_receipt(document: Any, *, path: Path) -> ArchiveReceipt:
    """Validates a receipt's schema and compression contract. Fails closed."""

    def invalid(detail: str) -> ReceiptError:
        return ReceiptError(f"invalid archive receipt {path}: {detail}")

    if not isinstance(document, dict):
        raise invalid("receipt is not an object")

    production = "archive_receipt_version" in document
    local = "local_archive_receipt_version" in document
    if production and local:
        raise invalid("receipt claims to be both a production and a local receipt")
    if not production and not local:
        raise invalid("receipt names no archive receipt version")
    kind = PRODUCTION if production else LOCAL
    version_field = "archive_receipt_version" if production else "local_archive_receipt_version"
    version = document[version_field]
    supported_versions = {1, ARCHIVE_RECEIPT_VERSION} if production else {1}
    if version not in supported_versions:
        raise invalid(f"unsupported {version_field} {document[version_field]!r}")
    # The filename and the version must agree, or a local receipt renamed to
    # `.archive.json` would read as deletion authority.
    expected_suffix = ARCHIVE_RECEIPT_SUFFIX if production else LOCAL_ARCHIVE_RECEIPT_SUFFIX
    if not path.name.endswith(expected_suffix):
        raise invalid(f"a {kind} receipt does not belong at {path.name}")
    if production and version == ARCHIVE_RECEIPT_VERSION:
        _require_exact_keys(
            document,
            {
                "archive_receipt_version",
                "store",
                "lane_id",
                "window_start_ns",
                "window_end_ns",
                "segment_id",
                "segment_index",
                "source",
                "seal",
                "object",
                "compression",
                "verified_at_ns",
                "archiver_version",
            },
            "receipt",
            invalid,
        )

    lane_id = _text(document, "lane_id", invalid)
    segment_id = _text(document, "segment_id", invalid)
    window_start_ns = _integer(document, "window_start_ns", invalid)
    window_end_ns = _integer(document, "window_end_ns", invalid)
    if window_start_ns >= window_end_ns:
        raise invalid("window_start_ns must precede window_end_ns")
    segment_index = _integer(document, "segment_index", invalid)
    # Zero while the archiver is verifying the objects it is about to receipt;
    # a committed receipt always carries the moment that verification returned.
    verified_at_ns = _integer(document, "verified_at_ns", invalid)
    archiver_version = _integer(document, "archiver_version", invalid)
    if archiver_version != ARCHIVER_VERSION:
        raise invalid(f"unsupported archiver_version {archiver_version}")

    source = _section(document, "source", invalid)
    if production and version == ARCHIVE_RECEIPT_VERSION:
        _require_exact_keys(
            source,
            {"file", "byte_length", "line_count", "sha256"},
            "source",
            invalid,
        )
    source_file = _basename(source, "file", invalid)
    if not source_file.endswith(".ndjson"):
        raise invalid(f"source file {source_file!r} is not a segment")
    logical = LogicalIdentity(
        sha256=_digest(source, "sha256", invalid),
        byte_length=_integer(source, "byte_length", invalid),
        line_count=_integer(source, "line_count", invalid),
    )

    seal = _section(document, "seal", invalid)
    if production and version == ARCHIVE_RECEIPT_VERSION:
        _require_exact_keys(
            seal,
            {
                "file",
                "byte_length",
                "sha256",
                "key",
                "provider_checksum",
                "provider_checksum_algorithm",
            },
            "seal",
            invalid,
        )
    seal_file = _basename(seal, "file", invalid)
    if not seal_file.endswith(".seal.json"):
        raise invalid(f"seal file {seal_file!r} is not a sidecar")
    # The reaper resolves both names against the directory the receipt was
    # discovered in (§4 finding 1). Requiring one shared stem is what makes
    # that resolution safe: a receipt whose two files could name unrelated
    # segments could point the reaper's deletion at a file its own checks
    # never examined.
    if source_file[: -len(".ndjson")] != seal_file[: -len(".seal.json")]:
        raise invalid(
            f"source file {source_file!r} and seal file {seal_file!r} do not share one "
            "segment stem"
        )
    seal_stored = StoredIdentity(
        sha256=_digest(seal, "sha256", invalid),
        byte_length=_integer(seal, "byte_length", invalid),
    )
    seal_key = _text(seal, "key", invalid)

    data = _section(document, "object", invalid)
    if production and version == ARCHIVE_RECEIPT_VERSION:
        _require_exact_keys(
            data,
            {
                "key",
                "byte_length",
                "sha256",
                "provider_checksum",
                "provider_checksum_algorithm",
                "content_encoding",
            },
            "object",
            invalid,
        )
    data_key = _text(data, "key", invalid)
    data_stored = StoredIdentity(
        sha256=_digest(data, "sha256", invalid),
        byte_length=_integer(data, "byte_length", invalid),
    )
    content_encoding = _text(data, "content_encoding", invalid)
    if content_encoding != "zstd":
        raise invalid(f"content_encoding {content_encoding!r} is not zstd")

    provider = "local"
    location = ""
    provider_checksum: str | None = None
    provider_checksum_algorithm: str | None = None
    seal_provider_checksum: str | None = None
    seal_provider_checksum_algorithm: str | None = None
    if production:
        if version == 1:
            provider = None
            location = _text(data, "bucket", invalid)
            if _text(seal, "bucket", invalid) != location:
                raise invalid("the seal and data objects name different locations")
            provider_checksum = _text(data, "s3_checksum_sha256", invalid)
            provider_checksum_algorithm = "SHA256"
            seal_provider_checksum = base64.b64encode(bytes.fromhex(seal_stored.sha256)).decode("ascii")
            seal_provider_checksum_algorithm = "SHA256"
            try:
                decoded = base64.b64decode(provider_checksum, validate=True)
            except Exception as error:  # noqa: BLE001 - any decode failure is the same fault
                raise invalid(f"s3_checksum_sha256 is not base64: {error}") from error
            if decoded.hex() != data_stored.sha256:
                raise invalid("s3_checksum_sha256 disagrees with the recorded sha256")
        else:
            store = _section(document, "store", invalid)
            _require_exact_keys(store, {"provider", "location"}, "store", invalid)
            provider = _text(store, "provider", invalid)
            location = _text(store, "location", invalid)
            provider_checksum = _text(data, "provider_checksum", invalid)
            provider_checksum_algorithm = _text(data, "provider_checksum_algorithm", invalid)
            seal_provider_checksum = _text(seal, "provider_checksum", invalid)
            seal_provider_checksum_algorithm = _text(
                seal, "provider_checksum_algorithm", invalid
            )
    else:
        location = _text(data, "store", invalid)
        if _text(seal, "store", invalid) != location:
            raise invalid("the seal and data objects name different locations")
        for forbidden in ("bucket", "s3_checksum_sha256"):
            if forbidden in data or forbidden in seal:
                raise invalid(f"a local conformance receipt must not carry {forbidden!r}")
        if document.get("authorizes_deletion") is not False:
            raise invalid("a local conformance receipt must record that it authorizes nothing")

    compression = _section(document, "compression", invalid)
    if production and version == ARCHIVE_RECEIPT_VERSION:
        _require_exact_keys(
            compression,
            {
                "algorithm",
                "level",
                "frame_checksum",
                "dictionary",
                "frame_count",
                "encoder",
            },
            "compression",
            invalid,
        )
    expected = compression_contract(compression.get("encoder", ""))
    for field, value in expected.items():
        if field == "encoder":
            if not isinstance(compression.get("encoder"), str) or not compression["encoder"]:
                raise invalid("compression.encoder is required")
            continue
        if compression.get(field) != value:
            raise invalid(f"compression.{field} is {compression.get(field)!r}, not {value!r}")

    return ArchiveReceipt(
        kind=kind,
        path=path,
        lane_id=lane_id,
        window_start_ns=window_start_ns,
        window_end_ns=window_end_ns,
        segment_id=segment_id,
        segment_index=segment_index,
        source_file=source_file,
        source=logical,
        seal_file=seal_file,
        seal_key=seal_key,
        seal_stored=seal_stored,
        data_key=data_key,
        data_stored=data_stored,
        provider=provider,
        location=location,
        provider_checksum=provider_checksum,
        provider_checksum_algorithm=provider_checksum_algorithm,
        seal_provider_checksum=seal_provider_checksum,
        seal_provider_checksum_algorithm=seal_provider_checksum_algorithm,
        content_encoding=content_encoding,
        compression=compression,
        verified_at_ns=verified_at_ns,
        archiver_version=archiver_version,
        document=document,
    )


def _require_exact_keys(
    value: dict[str, Any], expected: set[str], label: str, invalid
) -> None:
    if set(value) != expected:
        raise invalid(f"{label} fields are {sorted(value)!r}, expected {sorted(expected)!r}")


# -- canonical receipts --------------------------------------------------------


@dataclass(frozen=True)
class CanonicalInput:
    """One source segment a finalized window consumed."""

    lane: str
    data_file: str
    segment_index: int
    line_count: int
    sha256: str

    @property
    def identity(self) -> tuple[str, str, str, int]:
        """Lane plus digest is the contract's minimum; the file and index make
        an accidental cross-segment match fail loudly instead of authorizing a
        deletion (§7.1)."""
        return (self.lane, self.sha256, self.data_file, self.segment_index)


@dataclass(frozen=True)
class CanonicalOutput:
    """One receipt-bound canonical Zstandard object and both of its identities."""

    file: str
    decoded: LogicalIdentity
    stored: StoredIdentity


@dataclass(frozen=True)
class CanonicalReceipt:
    path: Path
    window_start_ns: int
    window_end_ns: int
    finalized_at_ns: int
    completeness: str
    certified: bool
    inputs: tuple[CanonicalInput, ...]
    evidence: CanonicalOutput
    provenance: CanonicalOutput
    #: False only when a production archive receipt proves the large local
    #: frames were intentionally reaped. ``receipt.json`` remains the canonical
    #: commit marker and restart authority in either state.
    outputs_present: bool
    document: dict[str, Any]


def read_canonical_receipt(path: Path) -> CanonicalReceipt:
    """Reads a committed canonical window, or refuses to call it one.

    The mirror of `canonical.rs::read_receipt`: structural validity is not
    enough, so the objects the receipt names are confirmed to exist at the
    length it recorded. The one valid absence is an archive-reaper tombstone:
    a strict production archive receipt must bind the unchanged canonical
    receipt and both output identities. This keeps sequence/watermark rebuild
    state locally while allowing the large frames to leave disk.
    """
    path = Path(path)

    def invalid(detail: str) -> ReceiptError:
        return ReceiptError(f"invalid canonical receipt {path}: {detail}")

    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ReceiptError(f"unreadable canonical receipt {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise invalid(str(error)) from error
    if not isinstance(document, dict):
        raise invalid("receipt is not an object")
    if document.get("receipt_version") != 1:
        raise invalid(f"unsupported receipt_version {document.get('receipt_version')!r}")

    window_start_ns = _integer(document, "window_start_ns", invalid)
    window_end_ns = _integer(document, "window_end_ns", invalid)
    finalized_at_ns = _integer(document, "finalized_at_ns", invalid)
    if window_start_ns >= window_end_ns:
        raise invalid("window_start_ns does not precede window_end_ns")
    directory = path.parent
    if directory.name != f"window={window_start_ns}":
        raise invalid(f"declares window {window_start_ns} but sits under {directory.name!r}")

    completeness = _text(document, "completeness", invalid)
    if completeness not in ("complete", "incomplete"):
        raise invalid(f"unknown completeness {completeness!r}")
    certified = document.get("certified")
    if not isinstance(certified, bool):
        raise invalid("certified is not a boolean")

    evidence_document = _canonical_output(document, "evidence", "evidence.ndjson.zst", invalid)
    provenance_document = _canonical_output(
        document, "provenance", "provenance.ndjson.zst", invalid
    )
    evidence = _canonical_output_identity(evidence_document, invalid)
    provenance = _canonical_output_identity(provenance_document, invalid)
    evidence_present = _canonical_output_present(directory, evidence, invalid)
    provenance_present = _canonical_output_present(directory, provenance, invalid)
    if provenance_present and not evidence_present:
        # Evidence is removed first, then provenance. This is the sole partial
        # crash state the canonical reaper can produce.
        _validate_canonical_archive_tombstone(
            path, window_start_ns, window_end_ns, evidence, provenance, invalid
        )
    elif not evidence_present and not provenance_present:
        _validate_canonical_archive_tombstone(
            path, window_start_ns, window_end_ns, evidence, provenance, invalid
        )
    elif evidence_present and not provenance_present:
        raise invalid(
            "provenance.ndjson.zst is absent while evidence.ndjson.zst remains; "
            "this is not a state produced by the canonical reaper"
        )
    evidence_decoded = _section(evidence_document, "decoded", invalid)
    provenance_decoded = _section(provenance_document, "decoded", invalid)
    if _integer(evidence_decoded, "line_count", invalid) != _integer(
        provenance_decoded, "line_count", invalid
    ):
        raise invalid("evidence and provenance line counts disagree")
    first = document.get("first_canonical_seq")
    last = document.get("last_canonical_seq")
    evidence_lines = _integer(evidence_decoded, "line_count", invalid)
    if first is None and last is None:
        if evidence_lines != 0:
            raise invalid("a receipt with no sequence range must have no evidence lines")
    elif isinstance(first, int) and isinstance(last, int) and 1 <= first <= last:
        if last - first + 1 != evidence_lines:
            raise invalid("canonical sequence range disagrees with line_count")
    else:
        raise invalid("incoherent canonical sequence range")

    raw_inputs = document.get("inputs")
    if not isinstance(raw_inputs, list):
        raise invalid("inputs is not a list")
    inputs = []
    for entry in raw_inputs:
        if not isinstance(entry, dict):
            raise invalid("an input is not an object")
        inputs.append(
            CanonicalInput(
                lane=_text(entry, "lane", invalid),
                data_file=_text(entry, "data_file", invalid),
                segment_index=_integer(entry, "segment_index", invalid),
                line_count=_integer(entry, "line_count", invalid),
                sha256=_digest(entry, "sha256", invalid),
            )
        )

    return CanonicalReceipt(
        path=path,
        window_start_ns=window_start_ns,
        window_end_ns=window_end_ns,
        finalized_at_ns=finalized_at_ns,
        completeness=completeness,
        certified=certified,
        inputs=tuple(inputs),
        evidence=evidence,
        provenance=provenance,
        outputs_present=evidence_present and provenance_present,
        document=document,
    )


def _canonical_output_identity(document: dict[str, Any], invalid) -> CanonicalOutput:
    decoded = _section(document, "decoded", invalid)
    stored = _section(document, "stored", invalid)
    return CanonicalOutput(
        file=_text(document, "file", invalid),
        decoded=LogicalIdentity(
            sha256=_digest(decoded, "sha256", invalid),
            byte_length=_integer(decoded, "byte_length", invalid),
            line_count=_integer(decoded, "line_count", invalid),
        ),
        stored=StoredIdentity(
            sha256=_digest(stored, "sha256", invalid),
            byte_length=_integer(stored, "byte_length", invalid),
        ),
    )


def _canonical_output(
    document: dict[str, Any],
    field: str,
    expected_name: str,
    invalid,
) -> dict[str, Any]:
    output = _section(document, field, invalid)
    name = _text(output, "file", invalid)
    if name != expected_name:
        raise invalid(f"{name!r} is not the canonical output {expected_name!r}")
    if output.get("content_encoding") != "zstd":
        raise invalid(f"{name} does not declare content_encoding 'zstd'")

    decoded = _section(output, "decoded", invalid)
    _integer(decoded, "byte_length", invalid)
    _integer(decoded, "line_count", invalid)
    _digest(decoded, "sha256", invalid)
    stored = _section(output, "stored", invalid)
    _integer(stored, "byte_length", invalid)
    _digest(stored, "sha256", invalid)

    compression = _section(output, "compression", invalid)
    expected_compression = {
        "algorithm": "zstd",
        "level": DEFAULT_ZSTD_LEVEL,
        "frame_checksum": True,
        "dictionary": None,
        "frame_count": 1,
    }
    for key, expected in expected_compression.items():
        if compression.get(key) != expected or isinstance(compression.get(key), bool) != isinstance(
            expected, bool
        ):
            raise invalid(f"{name} has invalid compression.{key}")
    encoder = compression.get("encoder")
    if not isinstance(encoder, str) or not encoder:
        raise invalid(f"{name} has no compression.encoder")

    return output


def _canonical_output_present(
    directory: Path, output: CanonicalOutput, invalid
) -> bool:
    object_path = directory / output.file
    try:
        metadata = object_path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise invalid(f"{output.file}: {error}") from error
    if not stat.S_ISREG(metadata.st_mode):
        raise invalid(f"{output.file} is not a regular file")
    if metadata.st_size != output.stored.byte_length:
        raise invalid(
            f"{output.file} is {metadata.st_size} bytes, recorded as "
            f"{output.stored.byte_length}"
        )
    return True


def _validate_canonical_archive_tombstone(
    path: Path,
    window_start_ns: int,
    window_end_ns: int,
    evidence: CanonicalOutput,
    provenance: CanonicalOutput,
    invalid,
) -> None:
    """Prove absent outputs were archived, without making S3 a reader concern.

    The destructive reaper performs fresh remote heads immediately before it
    unlinks anything. Readers need only distinguish that intentional state from
    unexplained local loss, so the retained production marker must bind the
    exact local receipt bytes and both output identities.
    """
    # Local import avoids making the raw receipt schema depend on the canonical
    # archiver during ordinary (outputs-present) scans.
    from archive.archiver.canonical import (
        PRODUCTION,
        PRODUCTION_RECEIPT_FILE,
        CanonicalArchiveReceiptError,
        read_canonical_archive_receipt,
    )

    marker_path = path.with_name(PRODUCTION_RECEIPT_FILE)
    try:
        marker_metadata = marker_path.lstat()
    except OSError as error:
        raise invalid(
            "canonical outputs are absent without a regular production archive receipt: "
            f"{error}"
        ) from error
    if not stat.S_ISREG(marker_metadata.st_mode):
        raise invalid("canonical archive receipt is not a regular file")
    try:
        marker = read_canonical_archive_receipt(marker_path)
    except CanonicalArchiveReceiptError as error:
        raise invalid(
            "canonical outputs are absent without a valid production archive receipt: "
            f"{error}"
        ) from error
    if marker.kind != PRODUCTION:
        raise invalid("canonical outputs are absent without production archive authority")
    if (marker.window_start_ns, marker.window_end_ns) != (window_start_ns, window_end_ns):
        raise invalid("canonical archive receipt names another window")
    try:
        with path.open("rb") as handle:
            receipt_identity = stored_identity_of(handle)
    except OSError as error:
        raise invalid(f"cannot identify retained receipt.json: {error}") from error
    if marker.canonical_receipt.stored != receipt_identity:
        raise invalid("canonical archive receipt does not bind the retained receipt.json")
    if marker.evidence.stored != evidence.stored:
        raise invalid("canonical archive receipt does not bind the evidence identity")
    if marker.provenance.stored != provenance.stored:
        raise invalid("canonical archive receipt does not bind the provenance identity")


def iter_canonical_receipts(canonical_root: Path) -> Iterator[Path]:
    """Every `receipt.json` under a canonical root, in window order."""
    root = Path(canonical_root)
    if not root.is_dir():
        raise ReceiptError(f"canonical root {root} is not a directory")
    for date_directory in sorted(root.iterdir()):
        if not date_directory.is_dir() or not date_directory.name.startswith("date="):
            continue
        for window_directory in sorted(date_directory.iterdir()):
            if not window_directory.is_dir() or not window_directory.name.startswith("window="):
                continue
            receipt = window_directory / CANONICAL_RECEIPT_FILE
            if receipt.is_file():
                yield receipt


@dataclass
class CanonicalIndex:
    """Which source segments are provably inside committed canonical evidence.

    Built once per sweep and consulted per segment. Faults are kept rather than
    swallowed: a canonical root holding an unreadable receipt is a condition the
    reaper must report, not one it may skip past on its way to a deletion.
    """

    by_identity: dict[tuple[str, str, str, int], Path]
    faults: list[str]

    @classmethod
    def build(cls, canonical_root: Path) -> CanonicalIndex:
        by_identity: dict[tuple[str, str, str, int], Path] = {}
        faults: list[str] = []
        try:
            paths = iter_canonical_receipts(canonical_root)
            for path in paths:
                try:
                    receipt = read_canonical_receipt(path)
                except ReceiptError as error:
                    faults.append(str(error))
                    continue
                for entry in receipt.inputs:
                    by_identity.setdefault(entry.identity, path)
        except ReceiptError as error:
            faults.append(str(error))
        return cls(by_identity=by_identity, faults=faults)

    def find(self, lane: str, sha256: str, data_file: str, segment_index: int) -> Path | None:
        return self.by_identity.get((lane, sha256, data_file, segment_index))

    def names_digest(self, lane: str, sha256: str) -> bool:
        """Whether any window names this lane and digest at all.

        Used only to tell "never canonicalized" from "canonicalized under a
        different filename or index", which are different operational problems
        and neither of which authorizes deletion.
        """
        return any(
            key[0] == lane and key[1] == sha256 for key in self.by_identity
        )


def _section(document: dict[str, Any], field: str, invalid) -> dict[str, Any]:
    value = document.get(field)
    if not isinstance(value, dict):
        raise invalid(f"{field} is not an object")
    return value


def _text(document: dict[str, Any], field: str, invalid) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise invalid(f"{field} is not a non-empty string")
    return value


def _basename(document: dict[str, Any], field: str, invalid) -> str:
    """A filename with no directory component and no traversal.

    The reaper resolves `source_file` and `seal_file` against the directory a
    receipt was discovered in (`archive/reaper/service.py`). A value carrying `/`, `\\`,
    or `..` would let a malformed or hostile receipt name a path outside that
    directory instead of a sibling of itself.
    """
    value = _text(document, field, invalid)
    if "/" in value or "\\" in value or value in (".", ".."):
        raise invalid(f"{field} {value!r} is not a bare filename")
    return value


def _integer(document: dict[str, Any], field: str, invalid) -> int:
    value = document.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise invalid(f"{field} is not a non-negative integer")
    return value


def _digest(document: dict[str, Any], field: str, invalid) -> str:
    value = document.get(field)
    if not isinstance(value, str) or len(value) != 64:
        raise invalid(f"{field} is not a 64-character digest")
    if value != value.lower() or any(character not in "0123456789abcdef" for character in value):
        raise invalid(f"{field} is not lowercase hexadecimal")
    return value
