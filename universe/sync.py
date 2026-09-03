"""Verify committed Targeter v3 reports and append their selected history."""

from __future__ import annotations

import hashlib
import heapq
import json
import tempfile
import time
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from archive import read_verified_json
from archive.retrieval import (
    ArchivedObject,
    ArchivedObjectByteStreamer,
    cleanup_stale_retrieval_directories,
)
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
from targeter.v2.models import isoformat, parse_timestamp
from universe.market_projection import project_market_universe
from universe.projection import (
    PROJECTION_VERSION,
    project_bundle_retirements,
    project_selected_bundles,
)
from universe.store import EvidenceConflict, UniverseStore

MAX_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_SELECTION_REPORT_BYTES = 128 * 1024 * 1024
MAX_CATALOG_BYTES_PER_RUN = 128 * 1024 * 1024
CATALOG_HEADROOM_WARNING_BYTES = 96 * 1024 * 1024
MAX_CATALOG_ROW_BYTES = 4 * 1024 * 1024
MAX_CATALOG_REFERENCES_PER_RUN = 100_000
BOOTSTRAP_RUN_BUDGET = 144
FAILURE_RETRY_BUDGET = 32
MAX_RESULT_DIAGNOSTICS = 100
STALE_RETRIEVAL_SECONDS = 24 * 60 * 60
BACKFILL_BATCH_SIZE = 100
INCREMENTAL_CHECKPOINT = "targeter-v3-incremental-date"


class UniverseSyncError(ValueError):
    """A discovered archive object violates the v3 consumer contract."""


def _logical_size(item: RunObject) -> int:
    if item.logical is None:
        raise UniverseSyncError(f"normalized artifact {item.key} has no logical identity")
    if item.logical.byte_length > MAX_CATALOG_BYTES_PER_RUN:
        raise UniverseSyncError(
            f"normalized artifact {item.key} exceeds "
            f"{MAX_CATALOG_BYTES_PER_RUN} decoded bytes"
        )
    return item.logical.byte_length


@dataclass
class SyncResult:
    discovered: int = 0
    ingested: int = 0
    skipped: int = 0
    incomplete: int = 0
    origin_dependencies_ingested: int = 0
    failure_count: int = 0
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    bootstrap_exhausted: bool = False
    completed: bool = False
    catalog_decoded_bytes: int = 0
    catalog_rows_retained: int = 0
    pending_failures: int = 0

    def add_failure(self, value: str) -> None:
        self.failure_count += 1
        if len(self.failures) < MAX_RESULT_DIAGNOSTICS:
            self.failures.append(value[:4096])

    def add_warning(self, value: str) -> None:
        if len(self.warnings) < MAX_RESULT_DIAGNOSTICS:
            self.warnings.append(value[:4096])

    def as_record(self) -> dict[str, Any]:
        return {
            "discovered": self.discovered,
            "ingested": self.ingested,
            "skipped": self.skipped,
            "incomplete": self.incomplete,
            "origin_dependencies_ingested": self.origin_dependencies_ingested,
            "failure_count": self.failure_count,
            "failures": list(self.failures),
            "warnings": list(self.warnings),
            "bootstrap_exhausted": self.bootstrap_exhausted,
            "completed": self.completed,
            "catalog_decoded_bytes": self.catalog_decoded_bytes,
            "catalog_rows_retained": self.catalog_rows_retained,
            "pending_failures": self.pending_failures,
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
    catalog_decoded_bytes: int
    catalog_rows_retained: int


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
        cleanup_stale_retrieval_directories(
            self.temporary_directory,
            older_than_seconds=STALE_RETRIEVAL_SECONDS,
        )

    def sync(self, *, now: datetime | None = None) -> SyncResult:
        """Catch up from the incremental floor; bootstrap with the latest run."""
        result = SyncResult()
        if self.database.event_identity_backfill_running():
            result.add_failure(
                "canonical event-identity backfill is running; "
                "incremental sync is blocked until it completes"
            )
            return self._finish(result)
        latest = self.database.latest_run()
        checkpoint = self.database.checkpoint(INCREMENTAL_CHECKPOINT)
        observed = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        observed_ns = _timestamp_ns(observed)
        retried = set(
            self.database.due_sync_failures(
                now_ns=observed_ns, limit=FAILURE_RETRY_BUDGET
            )
        )
        for key in sorted(retried):
            self._ingest_direct(key, result, now_ns=observed_ns)
        if checkpoint is None and latest is None:
            try:
                keys = self._bootstrap_manifest_keys(
                    result, now_ns=observed_ns, retried=retried
                )
            except Exception as error:  # noqa: BLE001 - discovery belongs in result
                result.add_failure(
                    f"targeter-v2/runs/: {type(error).__name__}: {error}"
                )
                return self._finish(result)
            if not keys:
                self.database.set_checkpoint(
                    INCREMENTAL_CHECKPOINT, observed.date().isoformat()
                )
                result.completed = True
                return self._finish(result)
            complete_found = False
            walked = 0
            for key in reversed(keys):
                input_complete = self._ingest_direct(
                    key, result, now_ns=observed_ns
                )
                walked += 1
                if input_complete is True:
                    complete_found = True
                    break
                if walked >= BOOTSTRAP_RUN_BUDGET:
                    break
            result.bootstrap_exhausted = not complete_found and walked >= BOOTSTRAP_RUN_BUDGET
            if result.bootstrap_exhausted:
                result.add_failure(
                    f"bootstrap found no complete run within the {BOOTSTRAP_RUN_BUDGET}-run budget; "
                    "run the configured backfill to index older history"
                )
            self.database.set_checkpoint(
                INCREMENTAL_CHECKPOINT,
                manifest_run_instant(keys[-1]).date().isoformat(),
            )
            result.completed = not result.bootstrap_exhausted
            return self._finish(result)

        floor = _date(
            checkpoint
            or str(latest["generated_at"]).split("T", 1)[0],
            "incremental checkpoint",
        )
        ceiling = max(floor, observed.date())
        cursor = floor
        while cursor <= ceiling:
            try:
                discovered = self._manifest_keys_for_dates(cursor, cursor)
            except Exception as error:  # noqa: BLE001
                result.add_failure(
                    f"targeter-v2/runs/date={cursor.isoformat()}/: "
                    f"{type(error).__name__}: {error}"
                )
                return self._finish(result)
            result.discovered += len(discovered)
            deferred = self.database.known_sync_failure_keys(discovered) - retried
            keys = self._valid_manifest_keys(
                discovered,
                result,
                now_ns=observed_ns,
                deferred=deferred | retried,
            )
            for key in keys:
                self._ingest_direct(key, result, now_ns=observed_ns)
            next_floor = min(cursor + timedelta(days=1), ceiling)
            self.database.set_checkpoint(INCREMENTAL_CHECKPOINT, next_floor.isoformat())
            cursor += timedelta(days=1)
        result.completed = True
        return self._finish(result)

    def sync_range(self, start: datetime, end: datetime) -> SyncResult:
        """Ingest every manifest whose canonical run timestamp is in [start, end)."""
        start = _utc(start, "backfill start")
        end = _utc(end, "backfill end")
        if start >= end:
            raise ValueError("backfill start must be before end")
        result = SyncResult()
        now_ns = time.time_ns()
        try:
            last_date = (end - timedelta(microseconds=1)).date()
            discovered = self._manifest_keys_for_dates(start.date(), last_date)
            keys = [
                key
                for key in self._valid_manifest_keys(
                    discovered, result, now_ns=now_ns
                )
                if start <= manifest_run_instant(key) < end
            ]
        except Exception as error:  # noqa: BLE001
            result.add_failure(
                f"targeter-v2/runs/: {type(error).__name__}: {error}"
            )
            return self._finish(result)
        result.discovered = len(discovered)
        for key in keys:
            self._ingest_direct(key, result, now_ns=now_ns)
        result.completed = True
        return self._finish(result)

    def backfill_range(
        self,
        start: datetime,
        end: datetime,
        *,
        batch_size: int = BACKFILL_BATCH_SIZE,
        progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> SyncResult:
        """Checkpoint and batch a full half-open rebuild range."""
        start = _utc(start, "backfill start")
        end = _utc(end, "backfill end")
        if start >= end:
            raise ValueError("backfill start must be before end")
        if batch_size <= 0:
            raise ValueError("backfill batch_size must be positive")
        generated_start = isoformat(start)
        generated_end = isoformat(end)
        assert generated_start is not None and generated_end is not None
        self.database.begin_event_identity_backfill(generated_start, generated_end)
        identity = hashlib.sha256(
            f"{start.isoformat()}\n{end.isoformat()}".encode()
        ).hexdigest()[:16]
        checkpoint_name = f"targeter-v3-backfill-{identity}"
        cursor = self.database.checkpoint(checkpoint_name)
        result = SyncResult()
        now_ns = time.time_ns()
        first_partition, after_last_partition = _manifest_partition_bounds(start, end)
        if cursor == "scan-complete":
            for key in self.database.due_sync_failures(
                now_ns=now_ns,
                limit=FAILURE_RETRY_BUDGET,
                key_start=first_partition,
                key_end=after_last_partition,
            ):
                try:
                    instant = manifest_run_instant(key)
                except Exception:  # malformed listed keys remain visible and retriable
                    self._ingest_direct(
                        key, result, now_ns=now_ns, identity_backfill=True
                    )
                    continue
                if start <= instant < end:
                    self._ingest_direct(
                        key, result, now_ns=now_ns, identity_backfill=True
                    )
            result.completed = (
                self.database.sync_failure_count(
                    key_start=first_partition,
                    key_end=after_last_partition,
                )
                == 0
            )
            if result.completed:
                self.database.complete_event_identity_backfill(
                    generated_start, generated_end
                )
            return self._finish(result)
        last_date = (end - timedelta(microseconds=1)).date()
        pending: list[str] = []
        cursor_run_id = manifest_run_id(cursor) if cursor is not None else None
        day = (
            max(start.date(), manifest_run_instant(cursor).date())
            if cursor is not None
            else start.date()
        )
        while day <= last_date:
            keys = self._manifest_keys_for_dates(day, day)
            result.discovered += len(keys)
            for key in self._valid_manifest_keys(keys, result, now_ns=now_ns):
                instant = manifest_run_instant(key)
                if start <= instant < end and (
                    cursor_run_id is None or manifest_run_id(key) > cursor_run_id
                ):
                    pending.append(key)
                if len(pending) == batch_size:
                    if not self._backfill_batch(
                        pending, result, checkpoint_name, progress, now_ns
                    ):
                        return self._finish(result)
                    cursor = pending[-1]
                    pending = []
            day += timedelta(days=1)
        if pending:
            if not self._backfill_batch(
                pending, result, checkpoint_name, progress, now_ns
            ):
                return self._finish(result)
        self.database.set_checkpoint(checkpoint_name, "scan-complete")
        result.completed = (
            self.database.sync_failure_count(
                key_start=first_partition,
                key_end=after_last_partition,
            )
            == 0
        )
        if result.completed:
            self.database.complete_event_identity_backfill(
                generated_start, generated_end
            )
        return self._finish(result)

    def _backfill_batch(
        self,
        keys: list[str],
        result: SyncResult,
        checkpoint_name: str,
        progress: Callable[[dict[str, Any]], None] | None,
        now_ns: int,
    ) -> bool:
        before_ingested = result.ingested
        before_skipped = result.skipped
        before_failures = result.failure_count
        processed = 0
        last_success: str | None = None
        for key in keys:
            failures = result.failure_count
            self._ingest_direct(
                key, result, now_ns=now_ns, identity_backfill=True
            )
            processed += 1
            if result.failure_count != failures:
                break
            last_success = key
        if last_success is not None:
            self.database.set_checkpoint(checkpoint_name, last_success)
        if progress is not None:
            progress(
                {
                    "type": "backfill_batch",
                    "processed": processed,
                    "through_manifest_key": keys[processed - 1],
                    "ingested": result.ingested - before_ingested,
                    "skipped": result.skipped - before_skipped,
                    "failures": result.failure_count - before_failures,
                }
            )
        return result.failure_count == before_failures

    def _finish(self, result: SyncResult) -> SyncResult:
        result.pending_failures = self.database.sync_failure_count()
        return result

    def audit_run(self, run_id: str) -> dict[str, Any]:
        """Reverify one run and every retained origin against immutable store bytes."""
        source = self.database.run_source(run_id)
        if source is None:
            raise UniverseSyncError(f"Targeter run {run_id} is not indexed")
        status = self._ingest_manifest(
            source["manifest_key"],
            stack=set(),
            force=True,
            dependency=False,
            result=None,
        )
        audit = self.database.audit_run(run_id)
        assert audit is not None
        if not audit["ok"]:
            raise EvidenceConflict(f"Targeter run {run_id} SQL projection failed audit")
        return {"source_status": status[0], **audit}

    def _ingest_direct(
        self,
        key: str,
        result: SyncResult,
        *,
        now_ns: int,
        identity_backfill: bool = False,
    ) -> bool | None:
        try:
            status, input_complete = self._ingest_manifest(
                key,
                stack=set(),
                force=False,
                dependency=False,
                result=result,
                identity_backfill=identity_backfill,
            )
            if status == "ingested":
                result.ingested += 1
                if input_complete is False:
                    result.incomplete += 1
            else:
                result.skipped += 1
            self.database.clear_sync_failure(key)
            return input_complete
        except Exception as error:  # noqa: BLE001 - preserve every failed source
            message = f"{key}: {type(error).__name__}: {error}"
            result.add_failure(message)
            self.database.record_sync_failure(key, message, now_ns=now_ns)
            return None

    def _ingest_manifest(
        self,
        key: str,
        *,
        stack: set[str],
        force: bool,
        dependency: bool,
        result: SyncResult | None,
        identity_backfill: bool = False,
        expected_manifest_sha256: str | None = None,
        expected_report_sha256: str | None = None,
    ) -> tuple[str, bool | None]:
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
            return "skipped", None
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
                        identity_backfill=identity_backfill,
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
                    identity_backfill=identity_backfill,
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
                identity_backfill=identity_backfill,
            )
            if dependency and status == "ingested" and result is not None:
                result.origin_dependencies_ingested += 1
            if result is not None:
                result.catalog_decoded_bytes += verified.catalog_decoded_bytes
                result.catalog_rows_retained += verified.catalog_rows_retained
                if verified.catalog_decoded_bytes >= CATALOG_HEADROOM_WARNING_BYTES:
                    result.add_warning(
                        f"run {verified.manifest.run_id} normalized catalogue bytes "
                        f"{verified.catalog_decoded_bytes} are above the "
                        f"{CATALOG_HEADROOM_WARNING_BYTES}-byte headroom threshold"
                    )
            return status, verified.manifest.input_complete
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
        identity_backfill: bool,
    ) -> dict[str, Any]:
        context = self._resolve_origin_context(
            current,
            row,
            label="retained bundle",
            stack=stack,
            force=force,
            result=result,
            identity_backfill=identity_backfill,
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
        identity_backfill: bool,
    ) -> dict[str, Any]:
        context = self._resolve_origin_context(
            current,
            row,
            label="retired bundle",
            stack=stack,
            force=force,
            result=result,
            identity_backfill=identity_backfill,
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
        identity_backfill: bool,
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
                identity_backfill=identity_backfill,
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
        required_event_refs, required_market_refs = (
            _catalog_references(report) if manifest.input_complete else (set(), set())
        )
        if len(required_event_refs) + len(required_market_refs) > MAX_CATALOG_REFERENCES_PER_RUN:
            raise UniverseSyncError(
                f"run {manifest.run_id} exceeds the "
                f"{MAX_CATALOG_REFERENCES_PER_RUN}-reference projection limit"
            )
        event_venues = {value.split(":", 1)[0] for value in required_event_refs}
        market_venues = {value.split(":", 1)[0] for value in required_market_refs}
        catalog_events: list[Mapping[str, Any]] = []
        catalog_markets: list[Mapping[str, Any]] = []
        rule_templates: list[Mapping[str, Any]] = []
        catalog_items = [
            item
            for item in manifest.objects
            if any(
                item.file.startswith(f"catalog_{venue}_events.ndjson")
                for venue in event_venues
            )
            or any(
                item.file.startswith(f"catalog_{venue}_markets.ndjson")
                for venue in market_venues
            )
            or (required_market_refs and item.file.startswith("rule_templates.ndjson"))
        ]
        catalog_bytes = sum(_logical_size(item) for item in catalog_items)
        if catalog_bytes > MAX_CATALOG_BYTES_PER_RUN:
            raise UniverseSyncError(
                f"normalized artifacts for run {manifest.run_id} exceed "
                f"{MAX_CATALOG_BYTES_PER_RUN} decoded bytes"
            )
        for item in catalog_items:
            if any(
                item.file.startswith(f"catalog_{venue}_events.ndjson")
                for venue in event_venues
            ):
                catalog_events.extend(
                    self._read_ndjson(
                        item,
                        bound_expectations[item.key],
                        keep=lambda row: _raw_event_ref(row) in required_event_refs,
                    )
                )
            elif any(
                item.file.startswith(f"catalog_{venue}_markets.ndjson")
                for venue in market_venues
            ):
                catalog_markets.extend(
                    self._read_ndjson(
                        item,
                        bound_expectations[item.key],
                        keep=lambda row: row.get("target_id") in required_market_refs,
                    )
                )
            elif item.file.startswith("rule_templates.ndjson"):
                rule_templates.extend(
                    self._read_ndjson(
                        item,
                        bound_expectations[item.key],
                        keep=lambda row: row.get("market_id") in required_market_refs,
                    )
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
            catalog_bytes,
            len(catalog_events) + len(catalog_markets) + len(rule_templates),
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
        self,
        item: RunObject,
        expected: ObjectExpectation,
        *,
        keep: Callable[[Mapping[str, Any]], bool],
    ) -> list[Mapping[str, Any]]:
        if item.content_type != NDJSON_CONTENT_TYPE or item.logical is None:
            raise UniverseSyncError(f"normalized artifact {item.key} is not NDJSON")
        if item.logical.byte_length > MAX_CATALOG_BYTES_PER_RUN:
            raise UniverseSyncError(
                f"normalized artifact {item.key} exceeds "
                f"{MAX_CATALOG_BYTES_PER_RUN} decoded bytes"
            )
        streamer = ArchivedObjectByteStreamer(
            self.objects,
            (ArchivedObject(item.file, expected, item.logical),),
            temp_root=self.temporary_directory,
        )
        pending = b""
        rows: list[Mapping[str, Any]] = []
        line_count = 0
        for chunk in streamer.iter_bytes(item.file):
            pending += chunk
            lines = pending.split(b"\n")
            pending = lines.pop()
            for line in lines:
                line_count += 1
                if len(line) > MAX_CATALOG_ROW_BYTES:
                    raise UniverseSyncError(
                        f"normalized artifact {item.key} contains a row above the "
                        f"{MAX_CATALOG_ROW_BYTES}-byte limit"
                    )
                row = _ndjson_row(line, item.key)
                if keep(row):
                    rows.append(row)
            if len(pending) > MAX_CATALOG_ROW_BYTES:
                raise UniverseSyncError(
                    f"normalized artifact {item.key} contains a row above the "
                    f"{MAX_CATALOG_ROW_BYTES}-byte limit"
                )
        if pending:
            raise UniverseSyncError(f"normalized artifact {item.key} lacks final LF")
        if line_count != item.logical.line_count:
            raise UniverseSyncError(
                f"normalized artifact {item.key} line count disagrees with manifest"
            )
        return rows
    def _bootstrap_manifest_keys(
        self,
        result: SyncResult,
        *,
        now_ns: int,
        retried: set[str],
    ) -> list[str]:
        newest: list[tuple[str, str]] = []
        candidates: list[tuple[str, str]] = []

        def retain_newest() -> None:
            known = self.database.known_sync_failure_keys(
                [key for _run_id, key in candidates]
            )
            for item in candidates:
                if item[1] in known or item[1] in retried:
                    continue
                if len(newest) < BOOTSTRAP_RUN_BUDGET:
                    heapq.heappush(newest, item)
                elif item > newest[0]:
                    heapq.heapreplace(newest, item)
            candidates.clear()

        for key in self.objects.list_keys("targeter-v2/runs/"):
            if not key.endswith("/run_manifest.json"):
                continue
            result.discovered += 1
            try:
                run_id = manifest_run_id(key)
                manifest_run_instant(key)
            except Exception as error:  # noqa: BLE001 - isolate malformed listings
                if key in retried or self.database.has_sync_failure(key):
                    continue
                message = f"{key}: {type(error).__name__}: {error}"
                result.add_failure(message)
                self.database.record_sync_failure(key, message, now_ns=now_ns)
                continue
            candidates.append((run_id, key))
            if len(candidates) == 500:
                retain_newest()
        retain_newest()
        return [key for _run_id, key in sorted(newest)]

    def _manifest_keys_for_dates(self, start: date, end: date) -> list[str]:
        keys: list[str] = []
        cursor = start
        while cursor <= end:
            prefix = f"targeter-v2/runs/date={cursor.isoformat()}/"
            for key in self.objects.list_keys(prefix):
                if key.endswith("/run_manifest.json"):
                    keys.append(key)
            cursor += timedelta(days=1)
        return sorted(set(keys))

    def _valid_manifest_keys(
        self,
        keys: list[str],
        result: SyncResult,
        *,
        now_ns: int,
        deferred: set[str] | None = None,
    ) -> list[str]:
        valid: list[str] = []
        for key in keys:
            if deferred is not None and key in deferred:
                continue
            try:
                manifest_run_id(key)
                manifest_run_instant(key)
            except Exception as error:  # noqa: BLE001 - isolate malformed listings
                message = f"{key}: {type(error).__name__}: {error}"
                result.add_failure(message)
                self.database.record_sync_failure(key, message, now_ns=now_ns)
                continue
            valid.append(key)
        return sorted(set(valid), key=manifest_run_id)


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


def _catalog_references(report: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise UniverseSyncError("report.candidates must be an array")
    event_refs: set[str] = set()
    market_refs: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise UniverseSyncError("report candidate must be an object")
        bundle_id = _required_text(candidate, "bundle_id", "candidate")
        for field, destination in (
            ("event_refs", event_refs),
            ("market_ids", market_refs),
        ):
            values = candidate.get(field)
            if not isinstance(values, list) or not all(
                _is_venue_reference(value)
                for value in values
            ):
                raise UniverseSyncError(
                    f"candidate {bundle_id} {field} must contain venue-prefixed text"
                )
            destination.update(values)
    return event_refs, market_refs


def _is_venue_reference(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    venue, separator, identifier = value.partition(":")
    return bool(venue and separator and identifier)


def _raw_event_ref(row: Mapping[str, Any]) -> str | None:
    venue = row.get("venue")
    identifier = row.get("venue_event_id")
    if not isinstance(venue, str) or not isinstance(identifier, str):
        return None
    return f"{venue}:{identifier}"


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


def _timestamp_ns(value: datetime) -> int:
    delta = value.astimezone(timezone.utc) - datetime(
        1970, 1, 1, tzinfo=timezone.utc
    )
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _manifest_partition_bounds(start: datetime, end: datetime) -> tuple[str, str]:
    after_last = (end - timedelta(microseconds=1)).date() + timedelta(days=1)
    return (
        f"targeter-v2/runs/date={start.date().isoformat()}/",
        f"targeter-v2/runs/date={after_last.isoformat()}/",
    )
