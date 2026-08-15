"""Incrementally ingest immutable archive evidence into the Event Universe."""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Mapping

from archive.archiver.universe import read_segment_universe_receipt
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
)
from encoder import LogicalIdentity, StoredIdentity, decode_stream
from targeter.v2.selected_bundles import (
    SELECTED_BUNDLE_INDEX_STEM,
    selected_bundle_rows,
)
from universe.selected_bundle_derivative import (
    SelectedBundleArtifact,
    derivative_keys,
    ensure_selected_bundle_derivative,
)
from universe.store import UniverseStore

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_SELECTED_INDEX_BYTES = 64 * 1024 * 1024
MAX_SELECTION_REPORT_BYTES = 128 * 1024 * 1024


class UniverseSyncError(ValueError):
    """A discovered object does not satisfy its immutable evidence contract."""


@dataclass
class SyncResult:
    targeter_ingested: int = 0
    targeter_skipped: int = 0
    controls_ingested: int = 0
    controls_skipped: int = 0
    failures: list[str] = field(default_factory=list)

    def as_record(self) -> dict[str, Any]:
        return {
            "targeter_ingested": self.targeter_ingested,
            "targeter_skipped": self.targeter_skipped,
            "controls_ingested": self.controls_ingested,
            "controls_skipped": self.controls_skipped,
            "failures": list(self.failures),
        }


@dataclass(frozen=True)
class _RunObject:
    file: str
    key: str
    stored: StoredIdentity
    logical: LogicalIdentity | None
    content_type: str
    content_encoding: str | None


@dataclass(frozen=True)
class _RunManifest:
    key: str
    stored: StoredIdentity
    run_id: str
    generated_at: str
    input_complete: bool
    objects: tuple[_RunObject, ...]


class UniverseSync:
    def __init__(
        self,
        database: UniverseStore,
        objects: ObjectStore,
        *,
        temporary_directory: Path | None = None,
    ) -> None:
        self.database = database
        self.objects = objects
        self.temporary_directory = Path(
            temporary_directory or tempfile.gettempdir()
        )
        self.temporary_directory.mkdir(parents=True, exist_ok=True)

    def sync(self) -> SyncResult:
        result = SyncResult()
        self.sync_targeter(result)
        self.sync_controls(result)
        return result

    def sync_targeter(self, result: SyncResult | None = None) -> SyncResult:
        output = result or SyncResult()
        keys = sorted(
            key
            for key in self.objects.list_keys("targeter-v2/runs/")
            if key.endswith("/run_manifest.json")
        )
        for key in keys:
            try:
                manifest = self._read_run_manifest(key)
                item, source_key, source_sha256, source_kind = (
                    self._selected_bundle_artifact(manifest)
                )
                if self.database.source_is_complete(source_key, source_sha256):
                    output.targeter_skipped += 1
                    continue
                status = self.database.ingest_selected_bundle_index(
                    source_key=source_key,
                    source_sha256=source_sha256,
                    source_kind=source_kind,
                    manifest_key=manifest.key,
                    manifest_sha256=manifest.stored.sha256,
                    index_key=item.key,
                    index_sha256=item.stored.sha256,
                    index_byte_length=item.stored.byte_length,
                    run_id=manifest.run_id,
                    generated_at=manifest.generated_at,
                    input_complete=manifest.input_complete,
                    rows=self._read_ndjson(item),
                )
                if status == "ingested":
                    output.targeter_ingested += 1
                else:
                    output.targeter_skipped += 1
            except Exception as error:  # noqa: BLE001 - one source must not hide later sources
                output.failures.append(f"{key}: {type(error).__name__}: {error}")
        return output

    def _selected_bundle_artifact(
        self, manifest: _RunManifest
    ) -> tuple[_RunObject, str, str, str]:
        native = [
            item
            for item in manifest.objects
            if item.file
            in {
                f"{SELECTED_BUNDLE_INDEX_STEM}.ndjson",
                f"{SELECTED_BUNDLE_INDEX_STEM}.ndjson.zst",
            }
        ]
        if len(native) > 1:
            raise UniverseSyncError(
                f"run {manifest.run_id} has multiple selected-bundle indexes"
            )
        if native:
            item = native[0]
            _verify_object_metadata(self.objects, item)
            return item, manifest.key, manifest.stored.sha256, "manifest_index"

        reports = [
            item
            for item in manifest.objects
            if item.file in {"selection_report.json", "selection_report.json.zst"}
        ]
        if len(reports) != 1:
            raise UniverseSyncError(
                f"run {manifest.run_id} must name one selection report"
            )
        report_item = reports[0]
        _, receipt_key = derivative_keys(manifest.key)
        receipt_exists = self.objects.head(receipt_key) is not None
        if receipt_exists:
            report_logical = None
            rows: list[dict[str, Any]] = []
        else:
            _verify_object_metadata(self.objects, report_item)
            report = self._read_json(report_item)
            if report.get("run_id") != manifest.run_id:
                raise UniverseSyncError(
                    f"run manifest {manifest.key} and selection report disagree on run_id"
                )
            report_logical = self._logical_for(report_item)
            rows = selected_bundle_rows(report)
        derivative = ensure_selected_bundle_derivative(
            self.objects,
            manifest_key=manifest.key,
            manifest_stored=manifest.stored,
            report_key=report_item.key,
            report_stored=report_item.stored,
            report_logical=report_logical,
            rows=rows,
            temporary_directory=self.temporary_directory,
        )
        receipt_metadata = self.objects.head(receipt_key)
        if receipt_metadata is None:
            raise VerificationFailure(f"selected-bundle receipt vanished: {receipt_key}")
        return (
            _run_object_from_derivative(derivative),
            receipt_key,
            receipt_metadata.sha256,
            "derived_index",
        )

    def sync_controls(self, result: SyncResult | None = None) -> SyncResult:
        output = result or SyncResult()
        keys = sorted(
            key
            for key in self.objects.list_keys("raw/")
            if key.endswith(".universe.json")
        )
        for key in keys:
            try:
                metadata = self.objects.head(key)
                if metadata is None:
                    raise UniverseSyncError(f"listed segment universe receipt vanished: {key}")
                if self.database.source_is_complete(key, metadata.sha256):
                    output.controls_skipped += 1
                    continue
                receipt = read_segment_universe_receipt(self.objects, key)
                envelopes = self._read_control_sidecar(receipt.control.key, receipt)
                status = self.database.ingest_control_receipt(
                    receipt,
                    envelopes,
                    source_sha256=metadata.sha256,
                )
                if status == "ingested":
                    output.controls_ingested += 1
                else:
                    output.controls_skipped += 1
            except Exception as error:  # noqa: BLE001 - one source must not hide later sources
                output.failures.append(f"{key}: {type(error).__name__}: {error}")
        return output

    def _read_run_manifest(self, key: str) -> _RunManifest:
        metadata = self.objects.head(key)
        if metadata is None:
            raise UniverseSyncError(f"run manifest is absent: {key}")
        if (
            metadata.byte_length > MAX_MANIFEST_BYTES
            or metadata.content_type != JSON_CONTENT_TYPE
            or metadata.content_encoding is not None
        ):
            raise UniverseSyncError(f"run manifest {key} has invalid content metadata")
        document = _read_json_bytes(
            self.objects,
            key,
            metadata,
            maximum=MAX_MANIFEST_BYTES,
        )
        if not isinstance(document, dict) or set(document) != {
            "targeter_run_manifest_version",
            "run_id",
            "generated_at",
            "input_complete",
            "files",
        }:
            raise UniverseSyncError(f"run manifest {key} has invalid fields")
        version = document["targeter_run_manifest_version"]
        if version not in {1, 2}:
            raise UniverseSyncError(f"run manifest {key} has unsupported version")
        run_id = _required_text(document, "run_id", "run manifest")
        expected_run_component = f"run={run_id}"
        if expected_run_component not in key.split("/") or not key.endswith(
            f"/{expected_run_component}/run_manifest.json"
        ):
            raise UniverseSyncError(f"run manifest key does not match run_id {run_id}")
        generated_at = _required_text(document, "generated_at", "run manifest")
        input_complete = document["input_complete"]
        if not isinstance(input_complete, bool):
            raise UniverseSyncError("run manifest input_complete is not boolean")
        files = document["files"]
        if not isinstance(files, list) or not files:
            raise UniverseSyncError("run manifest files must be a non-empty array")
        prefix = key.rsplit("/", 1)[0]
        objects = tuple(self._parse_run_object(prefix, value, version) for value in files)
        names = [item.file for item in objects]
        if len(names) != len(set(names)):
            raise UniverseSyncError(f"run manifest {key} repeats an object")
        indexes = set(names) & {
            f"{SELECTED_BUNDLE_INDEX_STEM}.ndjson",
            f"{SELECTED_BUNDLE_INDEX_STEM}.ndjson.zst",
        }
        reports = set(names) & {
            "selection_report.json",
            "selection_report.json.zst",
        }
        if len(indexes) > 1 or (not indexes and len(reports) != 1):
            raise UniverseSyncError(
                f"run manifest {key} must name one selected-bundle index or selection report"
            )
        return _RunManifest(
            key=key,
            stored=metadata.stored,
            run_id=run_id,
            generated_at=generated_at,
            input_complete=input_complete,
            objects=objects,
        )

    def _parse_run_object(
        self, prefix: str, value: Any, version: int
    ) -> _RunObject:
        if not isinstance(value, dict):
            raise UniverseSyncError("run manifest object is not an object")
        name = _required_text(value, "file", "run manifest object")
        if Path(name).name != name:
            raise UniverseSyncError(f"run manifest object is not a basename: {name!r}")
        key = f"{prefix}/{name}"
        if version == 1:
            if set(value) != {"file", "byte_length", "sha256", "content_type"}:
                raise UniverseSyncError(f"legacy run object {name} has invalid fields")
            content_type = _required_text(value, "content_type", f"run object {name}")
            stored = _stored(value, f"run object {name}")
            return _RunObject(name, key, stored, None, content_type, None)

        compressed_or_ndjson = name.endswith((".ndjson", ".ndjson.zst", ".json.zst"))
        expected = (
            {"file", "content_type", "content_encoding", "decoded", "stored", "compression"}
            if compressed_or_ndjson
            else {"file", "byte_length", "sha256", "content_type", "content_encoding"}
        )
        if set(value) != expected:
            raise UniverseSyncError(f"run object {name} has invalid fields")
        content_type = _required_text(value, "content_type", f"run object {name}")
        encoding = value.get("content_encoding")
        if encoding not in {None, "zstd"}:
            raise UniverseSyncError(f"run object {name} has invalid content encoding")
        if compressed_or_ndjson:
            stored_record = value.get("stored")
            logical_record = value.get("decoded")
            if not isinstance(stored_record, dict) or not isinstance(logical_record, dict):
                raise UniverseSyncError(f"run object {name} has invalid identities")
            stored = _stored(stored_record, f"run object {name} stored")
            logical = _logical(logical_record, f"run object {name} decoded")
            if name.endswith(".zst") != (encoding == "zstd"):
                raise UniverseSyncError(f"run object {name} suffix and encoding disagree")
            compression = value.get("compression")
            if encoding == "zstd":
                if not _valid_compression(compression):
                    raise UniverseSyncError(f"run object {name} has invalid compression")
            elif compression is not None:
                raise UniverseSyncError(f"plain run object {name} claims compression")
        else:
            stored = _stored(value, f"run object {name}")
            logical = None
        return _RunObject(name, key, stored, logical, content_type, encoding)

    def _read_ndjson(self, item: _RunObject) -> list[dict[str, Any]]:
        if item.content_type != NDJSON_CONTENT_TYPE:
            raise UniverseSyncError(f"selected-bundle object {item.key} is not NDJSON")
        decoded_bytes = (
            item.logical.byte_length if item.logical is not None else item.stored.byte_length
        )
        if decoded_bytes > MAX_SELECTED_INDEX_BYTES:
            raise UniverseSyncError(
                f"selected-bundle object {item.key} exceeds "
                f"{MAX_SELECTED_INDEX_BYTES} decoded bytes"
            )
        if item.logical is None:
            if item.content_encoding is not None:
                raise UniverseSyncError(
                    f"legacy selected-bundle object {item.key} is compressed"
                )
            with tempfile.TemporaryFile(
                mode="w+b", dir=self.temporary_directory
            ) as staged:
                _copy_verified(self.objects, item.key, item.stored, staged)
                staged.seek(0)
                return list(_iter_ndjson(staged, item.key))
        with self._decoded(item) as decoded:
            return list(_iter_ndjson(decoded, item.key))

    def _read_json(self, item: _RunObject) -> dict[str, Any]:
        if item.content_type != JSON_CONTENT_TYPE:
            raise UniverseSyncError(f"selection report {item.key} is not JSON")
        decoded_bytes = (
            item.logical.byte_length if item.logical is not None else item.stored.byte_length
        )
        if decoded_bytes > MAX_SELECTION_REPORT_BYTES:
            raise UniverseSyncError(
                f"selection report {item.key} exceeds "
                f"{MAX_SELECTION_REPORT_BYTES} decoded bytes"
            )
        if item.content_encoding == "zstd":
            if item.logical is None:
                raise UniverseSyncError(f"selection report {item.key} lacks decoded identity")
            with self._decoded(item) as decoded:
                try:
                    document = json.load(decoded)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise UniverseSyncError(f"invalid selection report {item.key}: {error}") from error
        else:
            metadata = _verify_object_metadata(self.objects, item)
            document = _read_json_bytes(
                self.objects,
                item.key,
                metadata,
                maximum=MAX_SELECTION_REPORT_BYTES,
            )
        if not isinstance(document, dict):
            raise UniverseSyncError(f"selection report {item.key} is not an object")
        return document

    def _logical_for(self, item: _RunObject) -> LogicalIdentity:
        if item.logical is not None:
            return item.logical
        if item.content_encoding is not None:
            raise UniverseSyncError(f"object {item.key} has no decoded identity")
        with tempfile.TemporaryFile(
            mode="w+b", dir=self.temporary_directory
        ) as staged:
            _copy_verified(self.objects, item.key, item.stored, staged)
            return _logical_identity(staged)

    def _decoded(self, item: _RunObject):
        if item.logical is None:
            raise UniverseSyncError(f"object {item.key} has no decoded identity")
        staged = tempfile.TemporaryFile(mode="w+b", dir=self.temporary_directory)
        try:
            if item.content_encoding == "zstd":
                with self.objects.open(
                    item.key, max_bytes=item.stored.byte_length
                ) as source:
                    decode_stream(
                        source,
                        staged,
                        expected_logical=item.logical,
                        expected_stored=item.stored,
                        max_decoded_bytes=item.logical.byte_length,
                    )
            else:
                _copy_verified(self.objects, item.key, item.stored, staged)
                identity = _logical_identity(staged)
                if identity != item.logical:
                    raise UniverseSyncError(f"plain object {item.key} logical identity drifted")
            staged.seek(0)
            return _ClosingTemporaryFile(staged)
        except Exception:
            staged.close()
            raise

    def _read_control_sidecar(
        self, key: str, receipt: Any
    ) -> Iterator[dict[str, Any]]:
        with tempfile.TemporaryFile(
            mode="w+b", dir=self.temporary_directory
        ) as decoded:
            with self.objects.open(
                key, max_bytes=receipt.control.stored.byte_length
            ) as source:
                decode_stream(
                    source,
                    decoded,
                    expected_logical=receipt.control.logical,
                    expected_stored=receipt.control.stored,
                    max_decoded_bytes=receipt.control.logical.byte_length,
                )
            decoded.seek(0)
            yield from _iter_ndjson(decoded, key)


class _ClosingTemporaryFile:
    def __init__(self, handle: BinaryIO) -> None:
        self.handle = handle

    def __enter__(self) -> BinaryIO:
        return self.handle

    def __exit__(self, *_: object) -> None:
        self.handle.close()


def _run_object_from_derivative(artifact: SelectedBundleArtifact) -> _RunObject:
    return _RunObject(
        file=artifact.file,
        key=artifact.key,
        stored=artifact.stored,
        logical=artifact.logical,
        content_type=artifact.content_type,
        content_encoding=artifact.content_encoding,
    )


def _iter_ndjson(source: BinaryIO, label: str) -> Iterator[dict[str, Any]]:
    for line_number, line in enumerate(source, 1):
        if not line.endswith(b"\n") or not line.strip():
            raise UniverseSyncError(f"{label}:{line_number} is not complete NDJSON")
        try:
            document = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise UniverseSyncError(f"{label}:{line_number} is invalid JSON: {error}") from error
        if not isinstance(document, dict):
            raise UniverseSyncError(f"{label}:{line_number} is not an object")
        yield document


def _verify_object_metadata(store: ObjectStore, item: _RunObject) -> ObjectMetadata:
    metadata = store.head(item.key)
    if metadata is None or not metadata.matches_request(
        item.stored, item.content_type, item.content_encoding
    ):
        raise VerificationFailure(
            f"manifest-committed object {item.key} does not match its identity"
        )
    return metadata


def _read_json_bytes(
    store: ObjectStore,
    key: str,
    metadata: ObjectMetadata,
    *,
    maximum: int,
) -> Any:
    if metadata.byte_length > maximum:
        raise UniverseSyncError(f"JSON object {key} exceeds {maximum} bytes")
    digest = hashlib.sha256()
    payload = bytearray()
    with store.open(key, max_bytes=metadata.byte_length) as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            payload.extend(chunk)
    if len(payload) != metadata.byte_length or digest.hexdigest() != metadata.sha256:
        raise VerificationFailure(f"JSON object {key} changed between head and read")
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise UniverseSyncError(f"invalid JSON object {key}: {error}") from error


def _copy_verified(
    store: ObjectStore, key: str, expected: StoredIdentity, destination: BinaryIO
) -> None:
    digest = hashlib.sha256()
    length = 0
    with store.open(key, max_bytes=expected.byte_length) as source:
        while chunk := source.read(1024 * 1024):
            destination.write(chunk)
            digest.update(chunk)
            length += len(chunk)
    if length != expected.byte_length or digest.hexdigest() != expected.sha256:
        raise VerificationFailure(f"object {key} changed while it was read")


def _logical_identity(source: BinaryIO) -> LogicalIdentity:
    source.seek(0)
    digest = hashlib.sha256()
    length = 0
    lines = 0
    while chunk := source.read(1024 * 1024):
        digest.update(chunk)
        length += len(chunk)
        lines += chunk.count(b"\n")
    source.seek(0)
    return LogicalIdentity(digest.hexdigest(), length, lines)


def _required_text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise UniverseSyncError(f"{label} {field} must be non-empty text")
    return value


def _stored(document: Mapping[str, Any], label: str) -> StoredIdentity:
    return StoredIdentity(
        sha256=_digest(document.get("sha256"), label),
        byte_length=_integer(document.get("byte_length"), label),
    )


def _logical(document: Mapping[str, Any], label: str) -> LogicalIdentity:
    if set(document) != {"sha256", "byte_length", "line_count"}:
        raise UniverseSyncError(f"{label} has invalid identity fields")
    return LogicalIdentity(
        sha256=_digest(document.get("sha256"), label),
        byte_length=_integer(document.get("byte_length"), label),
        line_count=_integer(document.get("line_count"), label),
    )


def _digest(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise UniverseSyncError(f"{label} has invalid sha256")
    return value


def _integer(value: Any, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise UniverseSyncError(f"{label} has invalid byte count")
    return value


def _valid_compression(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and set(value)
        == {
            "algorithm",
            "level",
            "frame_checksum",
            "dictionary",
            "frame_count",
            "encoder",
        }
        and value.get("algorithm") == "zstd"
        and value.get("level") == 3
        and value.get("frame_checksum") is True
        and value.get("dictionary") is None
        and value.get("frame_count") == 1
        and isinstance(value.get("encoder"), str)
        and bool(value["encoder"])
    )
