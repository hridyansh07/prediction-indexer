"""Verify committed Targeter v3 reports and append their selected history."""

from __future__ import annotations

import hashlib
import json
import tempfile
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO, Mapping

from archive.storage.base import (
    JSON_CONTENT_TYPE,
    ObjectMetadata,
    ObjectStore,
    VerificationFailure,
    provider_checksum_of,
)
from encoder import LogicalIdentity, StoredIdentity, decode_stream
from targeter.v2.models import parse_timestamp
from universe.projection import PROJECTION_VERSION, project_selected_bundles
from universe.store import EvidenceConflict, UniverseStore

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_SELECTION_REPORT_BYTES = 128 * 1024 * 1024
INCREMENTAL_CHECKPOINT = "targeter-v3-incremental-date"


class UniverseSyncError(ValueError):
    """A discovered archive object violates the v3 consumer contract."""


@dataclass
class SyncResult:
    discovered: int = 0
    ingested: int = 0
    skipped: int = 0
    incomplete: int = 0
    origin_dependencies_ingested: int = 0
    failures: list[str] = field(default_factory=list)

    def as_record(self) -> dict[str, Any]:
        return {
            "discovered": self.discovered,
            "ingested": self.ingested,
            "skipped": self.skipped,
            "incomplete": self.incomplete,
            "origin_dependencies_ingested": self.origin_dependencies_ingested,
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


@dataclass(frozen=True)
class _VerifiedRun:
    manifest: _RunManifest
    report_item: _RunObject
    report: dict[str, Any]
    projected: tuple[dict[str, Any], ...]


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

    def sync(self, *, now: datetime | None = None) -> SyncResult:
        """Catch up from the incremental floor; bootstrap with the latest run."""
        result = SyncResult()
        latest = self.database.latest_run()
        checkpoint = self.database.checkpoint(INCREMENTAL_CHECKPOINT)
        observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if checkpoint is None and latest is None:
            try:
                keys = self._all_manifest_keys()
            except Exception as error:  # noqa: BLE001 - discovery belongs in result
                result.failures.append(
                    f"targeter-v2/runs/: {type(error).__name__}: {error}"
                )
                return result
            result.discovered = len(keys)
            if not keys:
                return result
            key = max(keys, key=_manifest_run_id)
            self._ingest_direct(key, result)
            if not result.failures:
                self.database.set_checkpoint(
                    INCREMENTAL_CHECKPOINT, _manifest_run_instant(key).date().isoformat()
                )
            return result

        floor = _date(
            checkpoint
            or str(latest["generated_at"]).split("T", 1)[0],
            "incremental checkpoint",
        )
        ceiling = max(floor, observed.date())
        try:
            keys = self._manifest_keys_for_dates(floor, ceiling)
        except Exception as error:  # noqa: BLE001
            result.failures.append(
                f"targeter-v2/runs/: {type(error).__name__}: {error}"
            )
            return result
        result.discovered = len(keys)
        failed_dates: list[date] = []
        for key in keys:
            before = len(result.failures)
            self._ingest_direct(key, result)
            if len(result.failures) != before:
                failed_dates.append(_manifest_run_instant(key).date())
        next_floor = min(failed_dates) if failed_dates else ceiling
        self.database.set_checkpoint(INCREMENTAL_CHECKPOINT, next_floor.isoformat())
        return result

    def sync_range(self, start: datetime, end: datetime) -> SyncResult:
        """Ingest every manifest whose canonical run timestamp is in [start, end)."""
        start = _utc(start, "backfill start")
        end = _utc(end, "backfill end")
        if start >= end:
            raise ValueError("backfill start must be before end")
        result = SyncResult()
        try:
            last_date = (end - timedelta(microseconds=1)).date()
            keys = [
                key
                for key in self._manifest_keys_for_dates(start.date(), last_date)
                if start <= _manifest_run_instant(key) < end
            ]
        except Exception as error:  # noqa: BLE001
            result.failures.append(
                f"targeter-v2/runs/: {type(error).__name__}: {error}"
            )
            return result
        result.discovered = len(keys)
        for key in keys:
            self._ingest_direct(key, result)
        return result

    def audit_run(self, run_id: str) -> dict[str, Any]:
        """Reverify one run and every retained origin against immutable S3 bytes."""
        detail = self.database.run_detail(run_id)
        if detail is None:
            raise UniverseSyncError(f"Targeter run {run_id} is not indexed")
        status = self._ingest_manifest(
            detail["manifest_key"],
            stack=set(),
            force=True,
            dependency=False,
            result=None,
        )
        audit = self.database.audit_run(run_id)
        assert audit is not None
        return {"source_status": status, **audit}

    def _ingest_direct(self, key: str, result: SyncResult) -> None:
        try:
            status = self._ingest_manifest(
                key,
                stack=set(),
                force=False,
                dependency=False,
                result=result,
            )
            if status == "ingested":
                result.ingested += 1
                detail = self.database.run_detail(_manifest_run_id(key))
                if detail is not None and detail["input_complete"] is False:
                    result.incomplete += 1
            else:
                result.skipped += 1
        except Exception as error:  # noqa: BLE001 - preserve every failed source
            result.failures.append(f"{key}: {type(error).__name__}: {error}")

    def _ingest_manifest(
        self,
        key: str,
        *,
        stack: set[str],
        force: bool,
        dependency: bool,
        result: SyncResult | None,
        expected_manifest_sha256: str | None = None,
        expected_report_sha256: str | None = None,
    ) -> str:
        metadata = self.objects.head(key)
        if metadata is None:
            raise UniverseSyncError(f"run manifest is absent: {key}")
        if expected_manifest_sha256 is not None and (
            metadata.sha256 != expected_manifest_sha256
        ):
            raise UniverseSyncError(
                f"run manifest {key} identity disagrees with continuity origin"
            )
        if not force and self.database.known_manifest(key, metadata.sha256):
            return "skipped"
        if key in stack:
            raise UniverseSyncError(f"continuity origin cycle reaches {key}")
        stack.add(key)
        try:
            verified = self._verified_run(
                key,
                expected_manifest_sha256=expected_manifest_sha256,
                expected_report_sha256=expected_report_sha256,
            )
            resolved: list[dict[str, Any]] = []
            for row in verified.projected:
                if row["occurrence_kind"] == "complete":
                    context = _complete_context(row)
                else:
                    context = self._resolve_retained_context(
                        verified,
                        row,
                        stack=stack,
                        force=force,
                        result=result,
                    )
                resolved.append(
                    {
                        "run_id": verified.manifest.run_id,
                        "bundle_id": row["bundle_id"],
                        "occurrence_kind": row["occurrence_kind"],
                        "origin_run_id": row["origin_run_id"],
                        "continuity_selected": row["continuity_selected"],
                        "continuity_disposition": row["continuity_disposition"],
                        "context": context,
                    }
                )
            report_logical = verified.report_item.logical
            status = self.database.ingest_run(
                run_id=verified.manifest.run_id,
                generated_at=verified.manifest.generated_at,
                input_complete=verified.manifest.input_complete,
                report_version=verified.report["report_version"],
                strategy_version=verified.report["strategy_version"],
                manifest_key=verified.manifest.key,
                manifest_sha256=verified.manifest.stored.sha256,
                manifest_byte_length=verified.manifest.stored.byte_length,
                report_key=verified.report_item.key,
                report_sha256=verified.report_item.stored.sha256,
                report_byte_length=verified.report_item.stored.byte_length,
                report_decoded_sha256=(
                    report_logical.sha256
                    if report_logical is not None
                    else verified.report_item.stored.sha256
                ),
                report_decoded_byte_length=(
                    report_logical.byte_length
                    if report_logical is not None
                    else verified.report_item.stored.byte_length
                ),
                occurrences=resolved,
            )
            if dependency and status == "ingested" and result is not None:
                result.origin_dependencies_ingested += 1
            return status
        finally:
            stack.remove(key)

    def _resolve_retained_context(
        self,
        current: _VerifiedRun,
        row: Mapping[str, Any],
        *,
        stack: set[str],
        force: bool,
        result: SyncResult | None,
    ) -> dict[str, Any]:
        bundle_id = _required_text(row, "bundle_id", "retained occurrence")
        origin_run_id = _required_text(row, "origin_run_id", "retained occurrence")
        origin_key = _required_text(
            row, "origin_archive_manifest_key", "retained occurrence"
        )
        origin_manifest_sha256 = _required_text(
            row, "origin_archive_manifest_sha256", "retained occurrence"
        )
        origin_report_sha256 = _required_text(
            row, "origin_report_sha256", "retained occurrence"
        )
        if origin_run_id >= current.manifest.run_id:
            raise UniverseSyncError(
                f"retained bundle {bundle_id} origin is not older than its occurrence"
            )
        if _manifest_run_id(origin_key) != origin_run_id:
            raise UniverseSyncError(
                f"retained bundle {bundle_id} origin key disagrees with origin_run_id"
            )
        context = self.database.origin_context(
            run_id=origin_run_id,
            bundle_id=bundle_id,
            manifest_key=origin_key,
            manifest_sha256=origin_manifest_sha256,
            report_sha256=origin_report_sha256,
        )
        if context is None or force:
            self._ingest_manifest(
                origin_key,
                stack=stack,
                force=force,
                dependency=True,
                result=result,
                expected_manifest_sha256=origin_manifest_sha256,
                expected_report_sha256=origin_report_sha256,
            )
            context = self.database.origin_context(
                run_id=origin_run_id,
                bundle_id=bundle_id,
                manifest_key=origin_key,
                manifest_sha256=origin_manifest_sha256,
                report_sha256=origin_report_sha256,
            )
        if context is None:
            raise UniverseSyncError(
                f"retained bundle {bundle_id} has no complete origin occurrence"
            )
        if (
            row.get("activation_at") != context["activation_at"]
            or row.get("capture_start_at") != context["capture_start_at"]
            or _projected_targets(row.get("targets")) != context["targets"]
        ):
            raise UniverseSyncError(
                f"retained bundle {bundle_id} disagrees with its immutable origin"
            )
        return context

    def _verified_run(
        self,
        key: str,
        *,
        expected_manifest_sha256: str | None,
        expected_report_sha256: str | None,
    ) -> _VerifiedRun:
        manifest = self._read_run_manifest(
            key, expected_sha256=expected_manifest_sha256
        )
        report_items = [
            item
            for item in manifest.objects
            if item.file in {"selection_report.json", "selection_report.json.zst"}
        ]
        if len(report_items) != 1:
            raise UniverseSyncError(
                f"run {manifest.run_id} must commit exactly one selection report"
            )
        report_item = report_items[0]
        if (
            expected_report_sha256 is not None
            and report_item.stored.sha256 != expected_report_sha256
        ):
            raise UniverseSyncError(
                f"run {manifest.run_id} report identity disagrees with continuity origin"
            )
        report = self._read_json(report_item)
        if (
            report.get("report_version") != 3
            or report.get("mode") != "shadow"
            or report.get("run_id") != manifest.run_id
            or report.get("generated_at") != manifest.generated_at
            or report.get("input_complete") != manifest.input_complete
        ):
            raise UniverseSyncError(
                f"run {manifest.run_id} is not a consistent Targeter v3 report"
            )
        strategy_version = report.get("strategy_version")
        if (
            not isinstance(strategy_version, int)
            or isinstance(strategy_version, bool)
            or strategy_version <= 0
        ):
            raise UniverseSyncError(
                f"run {manifest.run_id} has an invalid strategy_version"
            )
        generated = parse_timestamp(manifest.generated_at)
        if generated is None or generated != _manifest_run_instant(key):
            raise UniverseSyncError(
                f"run {manifest.run_id} generated_at disagrees with its run id"
            )
        projected = (
            tuple(project_selected_bundles(report))
            if manifest.input_complete
            else ()
        )
        return _VerifiedRun(manifest, report_item, report, projected)

    def _read_run_manifest(
        self, key: str, *, expected_sha256: str | None
    ) -> _RunManifest:
        metadata = self.objects.head(key)
        if metadata is None:
            raise UniverseSyncError(f"run manifest is absent: {key}")
        if expected_sha256 is not None and metadata.sha256 != expected_sha256:
            raise UniverseSyncError(
                f"run manifest {key} identity disagrees with continuity origin"
            )
        if (
            metadata.byte_length > MAX_MANIFEST_BYTES
            or metadata.content_type != JSON_CONTENT_TYPE
            or metadata.content_encoding is not None
        ):
            raise UniverseSyncError(f"run manifest {key} has invalid content metadata")
        _verify_provider_identity(metadata, key)
        document = _read_json_bytes(
            self.objects, key, metadata, maximum=MAX_MANIFEST_BYTES
        )
        if not isinstance(document, dict) or set(document) != {
            "targeter_run_manifest_version",
            "run_id",
            "generated_at",
            "input_complete",
            "files",
        }:
            raise UniverseSyncError(f"run manifest {key} has invalid fields")
        if document["targeter_run_manifest_version"] != 2:
            raise UniverseSyncError(f"run manifest {key} is not version 2")
        run_id = _required_text(document, "run_id", "run manifest")
        if _manifest_run_id(key) != run_id:
            raise UniverseSyncError(f"run manifest key does not match run_id {run_id}")
        generated_at = _required_text(document, "generated_at", "run manifest")
        input_complete = document["input_complete"]
        if not isinstance(input_complete, bool):
            raise UniverseSyncError("run manifest input_complete must be boolean")
        files = document["files"]
        if not isinstance(files, list) or not files:
            raise UniverseSyncError("run manifest files must be a non-empty array")
        prefix = key.rsplit("/", 1)[0]
        objects = tuple(self._parse_run_object(prefix, value) for value in files)
        names = [item.file for item in objects]
        if len(names) != len(set(names)):
            raise UniverseSyncError(f"run manifest {key} repeats an object")
        return _RunManifest(
            key,
            metadata.stored,
            run_id,
            generated_at,
            input_complete,
            objects,
        )

    def _parse_run_object(self, prefix: str, value: Any) -> _RunObject:
        if not isinstance(value, dict):
            raise UniverseSyncError("run manifest object must be an object")
        name = _required_text(value, "file", "run manifest object")
        if Path(name).name != name:
            raise UniverseSyncError(f"run manifest object is not a basename: {name!r}")
        key = f"{prefix}/{name}"
        rich = name.endswith((".ndjson", ".ndjson.zst", ".json.zst"))
        expected = (
            {
                "file",
                "content_type",
                "content_encoding",
                "decoded",
                "stored",
                "compression",
            }
            if rich
            else {
                "file",
                "byte_length",
                "sha256",
                "content_type",
                "content_encoding",
            }
        )
        if set(value) != expected:
            raise UniverseSyncError(f"run object {name} has invalid fields")
        content_type = _required_text(value, "content_type", f"run object {name}")
        encoding = value.get("content_encoding")
        if encoding not in {None, "zstd"}:
            raise UniverseSyncError(f"run object {name} has invalid content encoding")
        if rich:
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
                    raise UniverseSyncError(
                        f"run object {name} has invalid compression"
                    )
            elif compression is not None:
                raise UniverseSyncError(f"plain run object {name} claims compression")
        else:
            stored = _stored(value, f"run object {name}")
            logical = None
        return _RunObject(name, key, stored, logical, content_type, encoding)

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
                raise UniverseSyncError(
                    f"selection report {item.key} lacks a decoded identity"
                )
            with self._decoded(item) as decoded:
                try:
                    document = json.load(decoded)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise UniverseSyncError(
                        f"invalid selection report {item.key}: {error}"
                    ) from error
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

    def _decoded(self, item: _RunObject) -> "_ClosingTemporaryFile":
        if item.logical is None:
            raise UniverseSyncError(f"object {item.key} has no decoded identity")
        metadata = _verify_object_metadata(self.objects, item)
        _verify_provider_identity(metadata, item.key)
        staged = tempfile.TemporaryFile(mode="w+b", dir=self.temporary_directory)
        try:
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
            staged.seek(0)
            return _ClosingTemporaryFile(staged)
        except Exception:
            staged.close()
            raise

    def _all_manifest_keys(self) -> list[str]:
        keys = [
            key
            for key in self.objects.list_keys("targeter-v2/runs/")
            if key.endswith("/run_manifest.json")
        ]
        for key in keys:
            _manifest_run_id(key)
        return sorted(set(keys), key=_manifest_run_id)

    def _manifest_keys_for_dates(self, start: date, end: date) -> list[str]:
        keys: list[str] = []
        cursor = start
        while cursor <= end:
            prefix = f"targeter-v2/runs/date={cursor.isoformat()}/"
            for key in self.objects.list_keys(prefix):
                if key.endswith("/run_manifest.json"):
                    _manifest_run_id(key)
                    keys.append(key)
            cursor += timedelta(days=1)
        return sorted(set(keys), key=_manifest_run_id)


class _ClosingTemporaryFile:
    def __init__(self, handle: BinaryIO) -> None:
        self.handle = handle

    def __enter__(self) -> BinaryIO:
        return self.handle

    def __exit__(self, *_: object) -> None:
        self.handle.close()


def _complete_context(row: Mapping[str, Any]) -> dict[str, Any]:
    if row.get("projection_version") != PROJECTION_VERSION:
        raise UniverseSyncError("selected occurrence has an unsupported projection version")
    return {
        field: row[field]
        for field in (
            "bundle_id",
            "sport",
            "game",
            "topology",
            "participants",
            "participant_keys",
            "activation_at",
            "capture_start_at",
            "event_refs",
            "markets",
            "relationships",
        )
    } | {"targets": _projected_targets(row.get("targets"))}


def _projected_targets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise UniverseSyncError("selected occurrence targets must be an array")
    fields = {
        "venue",
        "target_id",
        "canonical_class",
        "subscription_ids",
        "activation_at",
        "capture_start_at",
        "source_ref",
    }
    output: list[dict[str, Any]] = []
    for target in value:
        if not isinstance(target, Mapping) or set(target) != fields:
            raise UniverseSyncError("selected occurrence target fields are invalid")
        output.append(
            {
                field: target[field]
                for field in (
                    "venue",
                    "target_id",
                    "canonical_class",
                    "subscription_ids",
                    "source_ref",
                )
            }
        )
    return sorted(output, key=lambda item: (item["venue"], item["target_id"]))


def _manifest_run_id(key: str) -> str:
    parts = key.split("/")
    if (
        len(parts) != 5
        or parts[:2] != ["targeter-v2", "runs"]
        or not parts[2].startswith("date=")
        or not parts[3].startswith("run=")
        or parts[4] != "run_manifest.json"
    ):
        raise UniverseSyncError(f"invalid Targeter run manifest key: {key}")
    run_id = parts[3].removeprefix("run=")
    instant = _run_id_instant(run_id)
    if instant.date().isoformat() != parts[2].removeprefix("date="):
        raise UniverseSyncError(f"Targeter run manifest date disagrees with run id: {key}")
    return run_id


def _manifest_run_instant(key: str) -> datetime:
    return _run_id_instant(_manifest_run_id(key))


def _run_id_instant(run_id: str) -> datetime:
    try:
        instant = datetime.strptime(run_id, "%Y%m%dT%H%M%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise UniverseSyncError(f"invalid Targeter run id: {run_id}") from error
    if instant.strftime("%Y%m%dT%H%M%S.%fZ") != run_id:
        raise UniverseSyncError(f"non-canonical Targeter run id: {run_id}")
    return instant


def _verify_object_metadata(store: ObjectStore, item: _RunObject) -> ObjectMetadata:
    metadata = store.head(item.key)
    if metadata is None or not metadata.matches_request(
        item.stored, item.content_type, item.content_encoding
    ):
        raise VerificationFailure(
            f"manifest-committed object {item.key} does not match its identity"
        )
    _verify_provider_identity(metadata, item.key)
    return metadata


def _verify_provider_identity(metadata: ObjectMetadata, key: str) -> None:
    if (
        metadata.provider_checksum_algorithm != "SHA256"
        or metadata.provider_checksum != provider_checksum_of(metadata.sha256)
    ):
        raise VerificationFailure(f"object {key} lacks its committed SHA256 checksum")


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
        raise UniverseSyncError(f"{label} has an invalid count")
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
        and value["algorithm"] == "zstd"
        and value["level"] == 3
        and value["frame_checksum"] is True
        and value["dictionary"] is None
        and value["frame_count"] == 1
        and isinstance(value["encoder"], str)
        and bool(value["encoder"])
    )


def _date(value: str, label: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise EvidenceConflict(f"{label} is not an ISO date") from error
    if parsed.isoformat() != value:
        raise EvidenceConflict(f"{label} is not canonical")
    return parsed


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)
