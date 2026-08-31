"""Verify committed Targeter v3 reports and append their selected history."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from archive import read_verified_json
from archive.retrieval import ArchivedObject, ArchivedObjectByteStreamer
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ObjectExpectation,
    ObjectStore,
)
from archive.storage.verification import verify_metadata_objects
from encoder import StoredIdentity
from targeter.v2.manifest import (
    RunManifest,
    RunManifestError,
    RunObject,
    manifest_run_id,
    manifest_run_instant,
    parse_run_manifest,
)
from targeter.v2.models import parse_timestamp
from universe.market_projection import project_market_universe
from universe.projection import (
    PROJECTION_VERSION,
    project_bundle_retirements,
    project_selected_bundles,
)
from universe.store import EvidenceConflict, UniverseStore

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_SELECTION_REPORT_BYTES = 128 * 1024 * 1024
MAX_CATALOG_ARTIFACT_BYTES = 256 * 1024 * 1024
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
class _VerifiedRun:
    manifest: RunManifest
    manifest_stored: StoredIdentity
    report_item: RunObject
    report: dict[str, Any]
    market_projection: dict[str, Any]
    projected: tuple[dict[str, Any], ...]
    retirements: tuple[dict[str, Any], ...]


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
            key = max(keys, key=manifest_run_id)
            self._ingest_direct(key, result)
            if not result.failures:
                self.database.set_checkpoint(
                    INCREMENTAL_CHECKPOINT, manifest_run_instant(key).date().isoformat()
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
                failed_dates.append(manifest_run_instant(key).date())
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
                if start <= manifest_run_instant(key) < end
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
        """Reverify one run and every retained origin against immutable store bytes."""
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
                detail = self.database.run_detail(manifest_run_id(key))
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
            resolved_retirements: list[dict[str, Any]] = []
            for row in verified.retirements:
                context = self._resolve_retirement_context(
                    verified,
                    row,
                    stack=stack,
                    force=force,
                    result=result,
                )
                resolved_retirements.append(
                    {
                        "run_id": verified.manifest.run_id,
                        "bundle_id": row["bundle_id"],
                        "origin_run_id": row["origin_run_id"],
                        "disposition": row["disposition"],
                        "terminal_observed": row["terminal_observed"],
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
                manifest_sha256=verified.manifest_stored.sha256,
                manifest_byte_length=verified.manifest_stored.byte_length,
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
                market_projection=verified.market_projection,
                occurrences=resolved,
                retirements=resolved_retirements,
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
        context = self._resolve_origin_context(
            current,
            row,
            label="retained bundle",
            stack=stack,
            force=force,
            result=result,
        )
        bundle_id = _required_text(row, "bundle_id", "retained occurrence")
        if (
            row.get("activation_at") != context["activation_at"]
            or row.get("capture_start_at") != context["capture_start_at"]
            or _projected_targets(row.get("targets")) != context["targets"]
        ):
            raise UniverseSyncError(
                f"retained bundle {bundle_id} disagrees with its immutable origin"
            )
        return context

    def _resolve_retirement_context(
        self,
        current: _VerifiedRun,
        row: Mapping[str, Any],
        *,
        stack: set[str],
        force: bool,
        result: SyncResult | None,
    ) -> dict[str, Any]:
        context = self._resolve_origin_context(
            current,
            row,
            label="retired bundle",
            stack=stack,
            force=force,
            result=result,
        )
        bundle_id = _required_text(row, "bundle_id", "retirement")
        if (
            row.get("activation_at") != context["activation_at"]
            or row.get("capture_start_at") != context["capture_start_at"]
            or _projected_targets(row.get("targets")) != context["targets"]
        ):
            raise UniverseSyncError(
                f"retired bundle {bundle_id} disagrees with its immutable origin"
            )
        return context

    def _resolve_origin_context(
        self,
        current: _VerifiedRun,
        row: Mapping[str, Any],
        *,
        label: str,
        stack: set[str],
        force: bool,
        result: SyncResult | None,
    ) -> dict[str, Any]:
        evidence_label = f"{label} evidence"
        bundle_id = _required_text(row, "bundle_id", evidence_label)
        origin_run_id = _required_text(row, "origin_run_id", evidence_label)
        origin_key = _required_text(
            row, "origin_archive_manifest_key", evidence_label
        )
        origin_manifest_sha256 = _required_text(
            row, "origin_archive_manifest_sha256", evidence_label
        )
        origin_report_sha256 = _required_text(
            row, "origin_report_sha256", evidence_label
        )
        if origin_run_id >= current.manifest.run_id:
            raise UniverseSyncError(
                f"{label} {bundle_id} origin is not older than its occurrence"
            )
        if manifest_run_id(origin_key) != origin_run_id:
            raise UniverseSyncError(
                f"{label} {bundle_id} origin key disagrees with origin_run_id"
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
                f"{label} {bundle_id} has no complete origin occurrence"
            )
        return context

    def _verified_run(
        self,
        key: str,
        *,
        expected_manifest_sha256: str | None,
        expected_report_sha256: str | None,
    ) -> _VerifiedRun:
        manifest, manifest_stored = self._read_run_manifest(
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
        metadata = verify_metadata_objects(
            self.objects,
            (item.expectation() for item in manifest.objects),
        )
        bound_expectations = {
            item.key: replace(
                item.expectation(),
                provider_checksum=observed.provider_checksum,
                provider_checksum_algorithm=observed.provider_checksum_algorithm,
            )
            for item, observed in zip(manifest.objects, metadata, strict=True)
        }
        if (
            expected_report_sha256 is not None
            and report_item.stored.sha256 != expected_report_sha256
        ):
            raise UniverseSyncError(
                f"run {manifest.run_id} report identity disagrees with continuity origin"
            )
        report = self._read_json(report_item, bound_expectations[report_item.key])
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
        if generated is None or generated != manifest_run_instant(key):
            raise UniverseSyncError(
                f"run {manifest.run_id} generated_at disagrees with its run id"
            )
        projected = (
            tuple(project_selected_bundles(report))
            if manifest.input_complete
            else ()
        )
        retirements = (
            tuple(project_bundle_retirements(report))
            if manifest.input_complete
            else ()
        )
        catalog_events: list[Mapping[str, Any]] = []
        catalog_markets: list[Mapping[str, Any]] = []
        rule_templates: list[Mapping[str, Any]] = []
        for item in manifest.objects:
            if item.file.startswith("catalog_") and "_events.ndjson" in item.file:
                catalog_events.extend(
                    self._read_ndjson(item, bound_expectations[item.key])
                )
            elif item.file.startswith("catalog_") and "_markets.ndjson" in item.file:
                catalog_markets.extend(
                    self._read_ndjson(item, bound_expectations[item.key])
                )
            elif item.file.startswith("rule_templates.ndjson"):
                rule_templates.extend(
                    self._read_ndjson(item, bound_expectations[item.key])
                )
        market_projection = project_market_universe(
            report,
            catalog_events=catalog_events,
            catalog_markets=catalog_markets,
            rule_templates=rule_templates,
        )
        return _VerifiedRun(
            manifest,
            manifest_stored,
            report_item,
            report,
            market_projection,
            projected,
            retirements,
        )

    def _read_run_manifest(
        self, key: str, *, expected_sha256: str | None
    ) -> tuple[RunManifest, StoredIdentity]:
        metadata = self.objects.head(key)
        if metadata is None:
            raise UniverseSyncError(f"run manifest is absent: {key}")
        if expected_sha256 is not None and metadata.sha256 != expected_sha256:
            raise UniverseSyncError(
                f"run manifest {key} identity disagrees with continuity origin"
            )
        if metadata.byte_length > MAX_MANIFEST_BYTES:
            raise UniverseSyncError(f"run manifest {key} has invalid content metadata")
        document = read_verified_json(
            self.objects,
            ObjectExpectation(
                key,
                metadata.stored,
                metadata.provider_checksum,
                metadata.provider_checksum_algorithm,
                JSON_CONTENT_TYPE,
                None,
            ),
            max_decoded_bytes=MAX_MANIFEST_BYTES,
        )
        try:
            manifest = parse_run_manifest(document, key=key)
        except RunManifestError as error:
            raise UniverseSyncError(str(error)) from error
        return manifest, metadata.stored

    def _read_json(
        self, item: RunObject, expected: ObjectExpectation | None = None
    ) -> dict[str, Any]:
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
        document = read_verified_json(
            self.objects,
            expected or item.expectation(),
            logical=item.logical,
            max_decoded_bytes=MAX_SELECTION_REPORT_BYTES,
            temp_root=self.temporary_directory,
        )
        if not isinstance(document, dict):
            raise UniverseSyncError(f"selection report {item.key} is not an object")
        return document

    def _read_ndjson(
        self, item: RunObject, expected: ObjectExpectation
    ) -> list[Mapping[str, Any]]:
        if item.content_type != NDJSON_CONTENT_TYPE or item.logical is None:
            raise UniverseSyncError(f"normalized artifact {item.key} is not NDJSON")
        if item.logical.byte_length > MAX_CATALOG_ARTIFACT_BYTES:
            raise UniverseSyncError(
                f"normalized artifact {item.key} exceeds "
                f"{MAX_CATALOG_ARTIFACT_BYTES} decoded bytes"
            )
        streamer = ArchivedObjectByteStreamer(
            self.objects,
            (ArchivedObject(item.file, expected, item.logical),),
            temp_root=self.temporary_directory,
        )
        pending = b""
        rows: list[Mapping[str, Any]] = []
        for chunk in streamer.iter_bytes(item.file):
            pending += chunk
            lines = pending.split(b"\n")
            pending = lines.pop()
            for line in lines:
                rows.append(_ndjson_row(line, item.key))
        if pending:
            raise UniverseSyncError(f"normalized artifact {item.key} lacks final LF")
        if len(rows) != item.logical.line_count:
            raise UniverseSyncError(
                f"normalized artifact {item.key} line count disagrees with manifest"
            )
        return rows

    def _all_manifest_keys(self) -> list[str]:
        keys = [
            key
            for key in self.objects.list_keys("targeter-v2/runs/")
            if key.endswith("/run_manifest.json")
        ]
        for key in keys:
            manifest_run_id(key)
        return sorted(set(keys), key=manifest_run_id)

    def _manifest_keys_for_dates(self, start: date, end: date) -> list[str]:
        keys: list[str] = []
        cursor = start
        while cursor <= end:
            prefix = f"targeter-v2/runs/date={cursor.isoformat()}/"
            for key in self.objects.list_keys(prefix):
                if key.endswith("/run_manifest.json"):
                    manifest_run_id(key)
                    keys.append(key)
            cursor += timedelta(days=1)
        return sorted(set(keys), key=manifest_run_id)


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


def _required_text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise UniverseSyncError(f"{label} {field} must be non-empty text")
    return value


def _ndjson_row(line: bytes, key: str) -> Mapping[str, Any]:
    if not line:
        raise UniverseSyncError(f"normalized artifact {key} contains an empty line")
    try:
        value = json.loads(line)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise UniverseSyncError(f"normalized artifact {key} contains invalid JSON") from error
    if not isinstance(value, dict):
        raise UniverseSyncError(f"normalized artifact {key} row is not an object")
    return value


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
