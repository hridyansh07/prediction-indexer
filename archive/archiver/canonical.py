"""Receipt-last archival of already compressed canonical windows.

The finalizer owns canonical materialization: it writes the two strict V1
Zstandard frames and publishes ``receipt.json`` last. This sink does not encode
those bytes a second time. It independently decodes both frames against the
receipt, uploads the exact two frames and unchanged receipt under immutable
keys, verifies fresh object-store metadata, and publishes a separate local
archive receipt last.
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from archive.common.durable import confirm_durable, write_json_durable
from archive.common.receipts import (
    LOCAL,
    PRODUCTION,
    CanonicalOutput,
    CanonicalReceipt,
    ReceiptError,
    iter_canonical_receipts,
    read_canonical_receipt,
)
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    IntegrityConflict,
    ObjectMetadata,
    ObjectStore,
    ObjectStoreError,
    provider_checksum_of,
)
from encoder import CodecError, StoredIdentity, decode_stream, stored_identity_of

ARCHIVED = "archived"
SKIPPED = "skipped"
FAILED = "failed"
CONFLICT = "conflict"

KEY_PREFIX = "canonical"
PRODUCTION_RECEIPT_FILE = "canonical_archive_receipt.json"
LOCAL_RECEIPT_FILE = "canonical_archive_receipt.local.json"
RECEIPT_VERSION = 1
ARCHIVER_VERSION = 1

__all__ = [
    "ARCHIVED",
    "CONFLICT",
    "FAILED",
    "SKIPPED",
    "CanonicalArchiveReceipt",
    "CanonicalArchiver",
    "CanonicalObject",
    "WindowOutcome",
    "WindowSweepResult",
    "canonical_object_keys",
    "read_canonical_archive_receipt",
    "verify_canonical_archive",
]


class CanonicalArchiveReceiptError(ValueError):
    pass


class CanonicalArchiveVerificationError(RuntimeError):
    pass


@dataclass(frozen=True)
class CanonicalObject:
    file: str
    key: str
    stored: StoredIdentity
    location: str
    provider_checksum: str | None
    content_type: str
    content_encoding: str | None


@dataclass(frozen=True)
class CanonicalArchiveReceipt:
    kind: str
    path: Path
    window_start_ns: int
    window_end_ns: int
    location: str
    evidence: CanonicalObject
    provenance: CanonicalObject
    canonical_receipt: CanonicalObject
    verified_at_ns: int
    document: dict[str, Any]


@dataclass(frozen=True)
class WindowOutcome:
    window_start_ns: int
    status: str
    detail: str
    receipt_path: Path | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "window_start_ns": self.window_start_ns,
            "status": self.status,
            "detail": self.detail,
            "receipt": str(self.receipt_path) if self.receipt_path else None,
        }


@dataclass
class WindowSweepResult:
    outcomes: list[WindowOutcome] = field(default_factory=list)
    halted: str | None = None

    def count(self, status: str) -> int:
        return sum(outcome.status == status for outcome in self.outcomes)

    @property
    def counts(self) -> dict[str, int]:
        return {
            "discovered": len(self.outcomes),
            "archived": self.count(ARCHIVED),
            "skipped": self.count(SKIPPED),
            "failed": self.count(FAILED),
            "conflicted": self.count(CONFLICT),
        }

    def as_record(self) -> dict[str, Any]:
        return {
            "counts": self.counts,
            "halted": self.halted,
            "windows": [outcome.as_record() for outcome in self.outcomes],
        }


def canonical_object_keys(
    receipt_path: Path, *, prefix: str = KEY_PREFIX
) -> tuple[str, str, str]:
    path = Path(receipt_path)
    window = path.parent.name
    date = path.parent.parent.name
    if path.name != "receipt.json" or not window.startswith("window=") or not date.startswith(
        "date="
    ):
        raise CanonicalArchiveReceiptError(
            f"canonical receipt {path} is not under date=<date>/window=<start>/receipt.json"
        )
    base = f"{prefix}/{date}/{window}"
    return (
        f"{base}/evidence.ndjson.zst",
        f"{base}/provenance.ndjson.zst",
        f"{base}/receipt.json",
    )


class _DiscardWriter:
    def write(self, data: bytes | memoryview) -> int:
        return len(data)


def _verify_local_frame(directory: Path, output: CanonicalOutput) -> None:
    with (directory / output.file).open("rb") as source:
        decode_stream(
            source,
            _DiscardWriter(),
            expected_logical=output.decoded,
            expected_stored=output.stored,
            max_decoded_bytes=output.decoded.byte_length,
        )


class CanonicalArchiver:
    """Publishes committed canonical windows to the configured object store."""

    def __init__(
        self,
        canonical_root: Path | str,
        store: ObjectStore,
        *,
        now_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.canonical_root = Path(canonical_root)
        self.store = store
        self._now_ns = now_ns

    @property
    def receipt_kind(self) -> str:
        return PRODUCTION if self.store.durability.receipt_kind == PRODUCTION else LOCAL

    def sweep(self) -> WindowSweepResult:
        result = WindowSweepResult()
        for receipt_path in iter_canonical_receipts(self.canonical_root):
            outcome = self.archive_window(receipt_path)
            result.outcomes.append(outcome)
            if outcome.status == CONFLICT:
                result.halted = (
                    f"window {outcome.window_start_ns}: {outcome.detail}. The canonical "
                    "archive sweep stops on an immutable-key conflict."
                )
                break
        return result

    def archive_window(self, receipt_path: Path) -> WindowOutcome:
        path = Path(receipt_path)
        start = _window_start_hint(path)
        try:
            source = read_canonical_receipt(path)
            marker = path.with_name(
                PRODUCTION_RECEIPT_FILE if self.receipt_kind == PRODUCTION else LOCAL_RECEIPT_FILE
            )
            if marker.is_file():
                existing = read_canonical_archive_receipt(marker)
                self._verify_existing(source, existing)
                confirm_durable(marker)
                return WindowOutcome(
                    source.window_start_ns,
                    SKIPPED,
                    "an existing canonical archive receipt re-verified against the store",
                    marker,
                )
            return self._archive(source, marker)
        except IntegrityConflict as error:
            return WindowOutcome(start, CONFLICT, str(error))
        except (
            CanonicalArchiveReceiptError,
            CanonicalArchiveVerificationError,
            CodecError,
            ObjectStoreError,
            ReceiptError,
            OSError,
        ) as error:
            return WindowOutcome(start, FAILED, str(error))

    def _verify_existing(
        self, source: CanonicalReceipt, marker: CanonicalArchiveReceipt
    ) -> None:
        if marker.kind != self.receipt_kind:
            raise CanonicalArchiveReceiptError("canonical archive receipt kind changed")
        if marker.location != self.store.store_id:
            raise CanonicalArchiveReceiptError("canonical archive receipt names another store")
        if (marker.window_start_ns, marker.window_end_ns) != (
            source.window_start_ns,
            source.window_end_ns,
        ):
            raise CanonicalArchiveReceiptError("canonical archive receipt names another window")
        expected_keys = canonical_object_keys(source.path)
        if tuple(item.key for item in _objects(marker)) != expected_keys:
            raise CanonicalArchiveReceiptError("canonical archive receipt names unexpected keys")
        receipt_identity = _file_identity(source.path)
        expected = (source.evidence.stored, source.provenance.stored, receipt_identity)
        if tuple(item.stored for item in _objects(marker)) != expected:
            raise CanonicalArchiveReceiptError("canonical source identities changed")
        verify_canonical_archive(self.store, marker)

    def _archive(self, source: CanonicalReceipt, marker: Path) -> WindowOutcome:
        # Decoding is the independent proof that a finalizer-produced `.zst`
        # really carries the exact logical NDJSON its receipt commits to.
        _verify_local_frame(source.path.parent, source.evidence)
        _verify_local_frame(source.path.parent, source.provenance)

        receipt_identity = _file_identity(source.path)
        keys = canonical_object_keys(source.path)
        paths = (
            source.path.parent / source.evidence.file,
            source.path.parent / source.provenance.file,
            source.path,
        )
        identities = (source.evidence.stored, source.provenance.stored, receipt_identity)
        metadata = []
        for path, key, identity, content_type, content_encoding in zip(
            paths,
            keys,
            identities,
            (NDJSON_CONTENT_TYPE, NDJSON_CONTENT_TYPE, JSON_CONTENT_TYPE),
            (ZSTD_CONTENT_ENCODING, ZSTD_CONTENT_ENCODING, None),
            strict=True,
        ):
            with path.open("rb") as reader:
                metadata.append(
                    self.store.put_immutable(
                        key,
                        reader,
                        identity,
                        content_type=content_type,
                        content_encoding=content_encoding,
                    )
                )

        document = _build_receipt(
            kind=self.receipt_kind,
            source=source,
            keys=keys,
            receipt_identity=receipt_identity,
            location=self.store.store_id,
            metadata=tuple(metadata),
            verified_at_ns=0,
        )
        parsed = parse_canonical_archive_receipt(document, path=marker)
        verify_canonical_archive(self.store, parsed)
        document["verified_at_ns"] = self._now_ns()
        write_json_durable(marker, document)
        return WindowOutcome(
            source.window_start_ns,
            ARCHIVED,
            f"published {keys[0]}, {keys[1]}, and {keys[2]}",
            marker,
        )


def _build_receipt(
    *,
    kind: str,
    source: CanonicalReceipt,
    keys: tuple[str, str, str],
    receipt_identity: StoredIdentity,
    location: str,
    metadata: tuple[ObjectMetadata, ObjectMetadata, ObjectMetadata],
    verified_at_ns: int,
) -> dict[str, Any]:
    production = kind == PRODUCTION
    location_field = "bucket" if production else "store"

    def entry(
        file: str,
        key: str,
        identity: StoredIdentity,
        remote: ObjectMetadata,
        *,
        compressed: bool,
    ) -> dict[str, Any]:
        value = {
            "file": file,
            location_field: location,
            "key": key,
            "byte_length": identity.byte_length,
            "sha256": identity.sha256,
            "content_type": NDJSON_CONTENT_TYPE if compressed else JSON_CONTENT_TYPE,
            "content_encoding": ZSTD_CONTENT_ENCODING if compressed else None,
        }
        if production:
            value["s3_checksum_sha256"] = remote.provider_checksum
        return value

    document = {
        (
            "canonical_archive_receipt_version"
            if production
            else "local_canonical_archive_receipt_version"
        ): RECEIPT_VERSION,
        "window_start_ns": source.window_start_ns,
        "window_end_ns": source.window_end_ns,
        "evidence": entry(
            source.evidence.file, keys[0], source.evidence.stored, metadata[0], compressed=True
        ),
        "provenance": entry(
            source.provenance.file,
            keys[1],
            source.provenance.stored,
            metadata[1],
            compressed=True,
        ),
        "canonical_receipt": entry(
            source.path.name, keys[2], receipt_identity, metadata[2], compressed=False
        ),
        "verified_at_ns": verified_at_ns,
        "archiver_version": ARCHIVER_VERSION,
    }
    if not production:
        document.update({"durability": "local_conformance", "authorizes_deletion": False})
    return document


def read_canonical_archive_receipt(path: Path) -> CanonicalArchiveReceipt:
    path = Path(path)
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CanonicalArchiveReceiptError(f"unreadable canonical archive receipt {path}: {error}")
    return parse_canonical_archive_receipt(document, path=path)


def parse_canonical_archive_receipt(
    document: Any, *, path: Path
) -> CanonicalArchiveReceipt:
    def invalid(detail: str) -> CanonicalArchiveReceiptError:
        return CanonicalArchiveReceiptError(f"invalid canonical archive receipt {path}: {detail}")

    if not isinstance(document, dict):
        raise invalid("receipt is not an object")
    production = "canonical_archive_receipt_version" in document
    local = "local_canonical_archive_receipt_version" in document
    if production == local:
        raise invalid("receipt must name exactly one canonical archive version")
    kind = PRODUCTION if production else LOCAL
    version = (
        document["canonical_archive_receipt_version"]
        if production
        else document["local_canonical_archive_receipt_version"]
    )
    if version != RECEIPT_VERSION:
        raise invalid(f"unsupported version {version!r}")
    expected_name = PRODUCTION_RECEIPT_FILE if production else LOCAL_RECEIPT_FILE
    if path.name != expected_name:
        raise invalid(f"a {kind} receipt does not belong at {path.name}")
    common_fields = {
        "window_start_ns",
        "window_end_ns",
        "evidence",
        "provenance",
        "canonical_receipt",
        "verified_at_ns",
        "archiver_version",
    }
    expected_fields = common_fields | (
        {"canonical_archive_receipt_version"}
        if production
        else {
            "local_canonical_archive_receipt_version",
            "durability",
            "authorizes_deletion",
        }
    )
    _require_fields(document, expected_fields, "receipt", invalid)
    start = _integer(document, "window_start_ns", invalid)
    end = _integer(document, "window_end_ns", invalid)
    if start >= end:
        raise invalid("window_start_ns must precede window_end_ns")
    if path.parent.name != f"window={start}":
        raise invalid("window_start_ns disagrees with the receipt path")
    location_field = "bucket" if production else "store"
    evidence = _parse_object(
        document, "evidence", "evidence.ndjson.zst", location_field, production, True, invalid
    )
    provenance = _parse_object(
        document,
        "provenance",
        "provenance.ndjson.zst",
        location_field,
        production,
        True,
        invalid,
    )
    canonical_receipt = _parse_object(
        document, "canonical_receipt", "receipt.json", location_field, production, False, invalid
    )
    if len({item.location for item in (evidence, provenance, canonical_receipt)}) != 1:
        raise invalid("objects name different archive locations")
    expected_keys = canonical_object_keys(path.with_name("receipt.json"))
    if tuple(item.key for item in (evidence, provenance, canonical_receipt)) != expected_keys:
        raise invalid("object keys disagree with the canonical window path")
    if not production:
        if document.get("authorizes_deletion") is not False:
            raise invalid("a local receipt must record that it authorizes nothing")
        if document.get("durability") != "local_conformance":
            raise invalid("a local receipt must declare local_conformance durability")
    if _integer(document, "archiver_version", invalid) != ARCHIVER_VERSION:
        raise invalid("unsupported archiver_version")
    return CanonicalArchiveReceipt(
        kind=kind,
        path=path,
        window_start_ns=start,
        window_end_ns=end,
        location=evidence.location,
        evidence=evidence,
        provenance=provenance,
        canonical_receipt=canonical_receipt,
        verified_at_ns=_integer(document, "verified_at_ns", invalid),
        document=document,
    )


def _parse_object(
    document: dict[str, Any],
    field: str,
    expected_file: str,
    location_field: str,
    production: bool,
    compressed: bool,
    invalid,
) -> CanonicalObject:
    value = document.get(field)
    if not isinstance(value, dict):
        raise invalid(f"{field} is not an object")
    expected_fields = {
        "file",
        location_field,
        "key",
        "byte_length",
        "sha256",
        "content_type",
        "content_encoding",
    }
    if production:
        expected_fields.add("s3_checksum_sha256")
    _require_fields(value, expected_fields, field, invalid)
    file = _text(value, "file", invalid)
    if file != expected_file:
        raise invalid(f"{field}.file is {file!r}, not {expected_file!r}")
    content_type = _text(value, "content_type", invalid)
    expected_type = NDJSON_CONTENT_TYPE if compressed else JSON_CONTENT_TYPE
    if content_type != expected_type:
        raise invalid(f"{field}.content_type is not {expected_type!r}")
    content_encoding = value.get("content_encoding")
    expected_encoding = ZSTD_CONTENT_ENCODING if compressed else None
    if content_encoding != expected_encoding:
        raise invalid(f"{field}.content_encoding is not {expected_encoding!r}")
    stored = StoredIdentity(
        sha256=_digest(value, "sha256", invalid),
        byte_length=_integer(value, "byte_length", invalid),
    )
    checksum = _text(value, "s3_checksum_sha256", invalid) if production else None
    if checksum is not None:
        try:
            decoded = base64.b64decode(checksum, validate=True)
        except Exception as error:  # noqa: BLE001
            raise invalid(f"{field}.s3_checksum_sha256 is invalid: {error}") from error
        if decoded.hex() != stored.sha256:
            raise invalid(f"{field}.s3_checksum_sha256 disagrees with sha256")
    return CanonicalObject(
        file=file,
        key=_text(value, "key", invalid),
        stored=stored,
        location=_text(value, location_field, invalid),
        provider_checksum=checksum,
        content_type=content_type,
        content_encoding=content_encoding,
    )


def verify_canonical_archive(
    store: ObjectStore, receipt: CanonicalArchiveReceipt
) -> None:
    if receipt.location != store.store_id:
        raise CanonicalArchiveVerificationError(
            f"receipt names {receipt.location!r}; configured store is {store.store_id!r}"
        )
    for item in _objects(receipt):
        remote = store.head(item.key)
        if remote is None:
            raise CanonicalArchiveVerificationError(
                f"canonical archive object {item.key} is absent"
            )
        if not remote.matches_request(item.stored, item.content_type, item.content_encoding):
            raise CanonicalArchiveVerificationError(
                f"canonical archive object {item.key} disagrees with its receipt"
            )
        if receipt.kind == PRODUCTION:
            expected = provider_checksum_of(item.stored.sha256)
            if item.provider_checksum != expected or remote.provider_checksum != expected:
                raise CanonicalArchiveVerificationError(
                    f"canonical archive object {item.key} has the wrong provider checksum"
                )
            if remote.provider_checksum_algorithm != "SHA256":
                raise CanonicalArchiveVerificationError(
                    f"canonical archive object {item.key} has no full-object SHA256 proof"
                )


def _objects(receipt: CanonicalArchiveReceipt) -> tuple[CanonicalObject, ...]:
    return (receipt.evidence, receipt.provenance, receipt.canonical_receipt)


def _file_identity(path: Path) -> StoredIdentity:
    with path.open("rb") as handle:
        return stored_identity_of(handle)


def _window_start_hint(path: Path) -> int:
    try:
        return int(path.parent.name.removeprefix("window="))
    except ValueError:
        return 0


def _integer(document: dict[str, Any], field: str, invalid) -> int:
    value = document.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise invalid(f"{field} is not a non-negative integer")
    return value


def _text(document: dict[str, Any], field: str, invalid) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise invalid(f"{field} is not a non-empty string")
    return value


def _digest(document: dict[str, Any], field: str, invalid) -> str:
    value = document.get(field)
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise invalid(f"{field} is not lowercase SHA-256 hexadecimal")
    return value


def _require_fields(
    document: dict[str, Any], expected: set[str], label: str, invalid
) -> None:
    actual = set(document)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        raise invalid(f"{label} fields differ; missing={missing}, unknown={unknown}")
