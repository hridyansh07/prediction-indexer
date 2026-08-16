"""Atomic publication and audit of Targeter v2 splice subscriptions.

All venue files are materialized inside an immutable run generation.  Its
``manifest.json`` commits those files; only then is one small ``current.json``
pointer replaced.  Every splice reads the same pointer and selects its venue,
so a crash cannot expose a mixture of old and new venue generations.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from archive.common.durable import confirm_durable, write_json_durable
from archive.storage.base import ObjectStore
from encoder import StoredIdentity, stored_identity_of
from targeter.targets import (
    TARGET_GENERATION_POINTER_VERSION,
    TARGET_PUBLICATION_MANIFEST_VERSION,
    Target,
    TargetsError,
    load_targets,
    write_targets,
)
from targeter.coverage import CoverageLedger, created_at_of
from targeter.v2.models import SUPPORTED_VENUES
from targeter.v2.publication_validation import (
    PublicationError,
    catalog_reader,
    catalog_markets as _catalog_markets,
    publication_pointer_path,
    read_publication_pointer,
    selection_report as _selection_report,
    selection_report_object as _selection_report_object,
    targets_from_report as _targets_from_report,
    verify_continuity_base as _verify_continuity_base,
)
from targeter.v2.registry import Strategy
from targeter.v2.run_archive import (
    PRODUCTION_RECEIPT_FILE,
    RunArchiveReceipt,
    read_run_archive_receipt,
    validate_local_run,
    verify_run_archive,
)


@dataclass(frozen=True)
class PublishedGeneration:
    run_id: str
    directory: Path
    manifest_path: Path
    pointer_path: Path
    venue_counts: Mapping[str, int]
    #: Assets whose first sighting this publication recorded, per venue. Zero
    #: across the board is the steady state — a republish of an unchanged target
    #: set sees nothing new.
    newly_seen: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class PublicationAudit:
    run_id: str
    venue_counts: Mapping[str, int]


def _read_json(path: Path, description: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise PublicationError(f"cannot read {description} {path}: {error}") from error
    except json.JSONDecodeError as error:
        raise PublicationError(f"invalid {description} {path}: {error}") from error
    if not isinstance(document, dict):
        raise PublicationError(f"{description} must be a JSON object")
    return document


def _identity(path: Path) -> StoredIdentity:
    with path.open("rb") as handle:
        return stored_identity_of(handle)


def _identity_record(path: Path) -> dict[str, Any]:
    stored = _identity(path)
    return {
        "file": path.name,
        "byte_length": stored.byte_length,
        "sha256": stored.sha256,
    }


def coverage_ledger_path(live_root: Path) -> Path:
    return Path(live_root) / "coverage.json"


def record_coverage(
    live_root: Path,
    targets: Mapping[str, list[Target]],
    catalog_markets: Mapping[str, Mapping[str, Mapping[str, Any]]],
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """First sightings for everything this generation subscribes.

    v1 kept this ledger from its discovery loop (`targeter/run.py:236`) and v2
    did not carry it across, which left `discovery_coverage` in `replay/gate1.py`
    with nothing to read: every subscribed asset reported uncovered. The measure
    is coverage-from-inception (`docs/CAPTURE_SPEC.md` §6.1) — how much of a
    market's life the tape actually contains — and for short-dated markets it is
    the difference between a usable dataset and a misleading one.

    Written from `publish_run` rather than from discovery because publication is
    the point where the subscription set becomes a fact: an asset selected by a
    run that then failed to archive was never watched, and recording it would
    claim coverage the tape does not have.

    `created_at` comes from the archived catalogue record, so the venue's own
    creation time is read from the same immutable boundary the targets were
    derived from rather than re-fetched later. `CanonicalMarket.as_record`
    already normalises it to ISO-8601; `created_at_of`
    still mediates the read so a venue that publishes none leaves the field
    null, which `Sighting.discovery_lag_seconds` reports as unmeasurable
    instead of as a lag of zero.

    The ledger never overwrites a sighting, so this is idempotent under
    republication and cheap in the steady state.
    """
    ledger = CoverageLedger(coverage_ledger_path(live_root))
    # The run's own instant, so a `--now` probe does not stamp sightings with
    # the wall clock and report a discovery lag measured against the wrong run.
    seen_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    fresh: dict[str, int] = {}
    for venue in SUPPORTED_VENUES:
        venue_targets = targets.get(venue) or []
        catalog = catalog_markets.get(venue) or {}
        created: dict[str, str] = {}
        for target in venue_targets:
            record = catalog.get(f"{venue}:{target.market_id}")
            if not isinstance(record, Mapping):
                continue
            value = created_at_of(dict(record))
            if value is not None:
                created[target.asset_id] = value
        fresh[venue] = len(
            ledger.observe(
                venue,
                [target.asset_id for target in venue_targets],
                created_at=created,
                now=seen_at,
            )
        )
    ledger.save()
    return fresh


def publish_run(
    run_directory: Path,
    receipt: RunArchiveReceipt,
    store: ObjectStore,
    *,
    live_root: Path,
    strategy: Strategy,
    now: datetime | None = None,
) -> PublishedGeneration:
    run_directory = Path(run_directory)
    live_root = Path(live_root)
    if not receipt.is_production or not store.durability.independent:
        raise PublicationError("publication requires a verified independent archive")
    verify_run_archive(store, receipt)
    validate_local_run(run_directory, receipt)
    report = _selection_report(run_directory, strategy)
    _verify_continuity_base(report, live_root, strategy)
    catalog_markets = _catalog_markets(run_directory, receipt, report)
    targets = _targets_from_report(report, receipt, strategy, catalog_markets)

    publication_root = live_root / "targeter-v2"
    generation_directory = publication_root / "generations" / receipt.run_id
    manifest_path = generation_directory / "manifest.json"
    pointer_path = publication_pointer_path(live_root)
    generation_directory.mkdir(parents=True, exist_ok=True)

    if manifest_path.exists():
        manifest = _read_json(manifest_path, "target publication manifest")
        _verify_generation_manifest(manifest_path, manifest, receipt, strategy, targets)
        confirm_durable(manifest_path)
    else:
        venue_records: dict[str, dict[str, Any]] = {}
        for venue in SUPPORTED_VENUES:
            target_path = generation_directory / f"targets_{venue}.json"
            write_targets(
                target_path,
                venue=venue,
                targets=targets[venue],
                note=f"Targeter v2 generation {receipt.run_id}",
            )
            loaded = load_targets(target_path, venue=venue)
            # ``write_targets`` uses atomic file replacement.  Before the
            # generation manifest can commit those names, make both the target
            # file and its content-addressed metadata snapshot strictly durable
            # (including their directory entries).
            confirm_durable(target_path)
            if loaded.metadata_path is None:
                raise PublicationError(f"publication target file for {venue} has no metadata snapshot")
            confirm_durable(Path(loaded.metadata_path))
            venue_records[venue] = {
                "target_file": _identity_record(target_path),
                "target_digest": loaded.digest,
                "metadata_digest": loaded.metadata_digest,
                "target_count": len(loaded),
            }

        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        report_object = _selection_report_object(receipt)
        manifest = {
            "target_publication_manifest_version": TARGET_PUBLICATION_MANIFEST_VERSION,
            "run_id": receipt.run_id,
            "published_at": instant.isoformat().replace("+00:00", "Z"),
            "selection_report": {
                "byte_length": report_object.stored.byte_length,
                "sha256": report_object.stored.sha256,
            },
            "archive": {
                "bucket": receipt.location,
                "manifest_key": receipt.manifest.key,
                "manifest_byte_length": receipt.manifest.stored.byte_length,
                "manifest_sha256": receipt.manifest.stored.sha256,
            },
            "minimum_venues": strategy.minimum_venues,
            "venues": venue_records,
        }
        write_json_durable(manifest_path, manifest)
        _verify_generation_manifest(manifest_path, manifest, receipt, strategy, targets)

    manifest_identity = _identity(manifest_path)
    pointer = {
        "target_generation_pointer_version": TARGET_GENERATION_POINTER_VERSION,
        "run_id": receipt.run_id,
        "manifest_path": str(manifest_path.relative_to(publication_root)),
        "manifest": {
            "byte_length": manifest_identity.byte_length,
            "sha256": manifest_identity.sha256,
        },
    }
    write_json_durable(pointer_path, pointer)
    counts = {
        venue: int(manifest["venues"][venue]["target_count"])
        for venue in SUPPORTED_VENUES
    }
    # After the pointer commits: the ledger claims the tape watches these
    # assets, and that only becomes true once a splice can resolve them.
    newly_seen = record_coverage(live_root, targets, catalog_markets, now=now)
    return PublishedGeneration(
        run_id=receipt.run_id,
        directory=generation_directory,
        manifest_path=manifest_path,
        pointer_path=pointer_path,
        venue_counts=dict(sorted(counts.items())),
        newly_seen=dict(sorted(newly_seen.items())),
    )


def _verify_generation_manifest(
    manifest_path: Path,
    manifest: dict[str, Any],
    receipt: RunArchiveReceipt,
    strategy: Strategy,
    expected: Mapping[str, list[Target]],
) -> None:
    if manifest.get("target_publication_manifest_version") != TARGET_PUBLICATION_MANIFEST_VERSION:
        raise PublicationError("unsupported target publication manifest version")
    if manifest.get("run_id") != receipt.run_id:
        raise PublicationError("publication manifest names another run")
    if manifest.get("minimum_venues") != strategy.minimum_venues:
        raise PublicationError("publication manifest minimum_venues drifted")
    report_object = _selection_report_object(receipt)
    if manifest.get("selection_report") != {
        "byte_length": report_object.stored.byte_length,
        "sha256": report_object.stored.sha256,
    }:
        raise PublicationError("publication manifest selection identity is invalid")
    archive = manifest.get("archive")
    if archive != {
        "bucket": receipt.location,
        "manifest_key": receipt.manifest.key,
        "manifest_byte_length": receipt.manifest.stored.byte_length,
        "manifest_sha256": receipt.manifest.stored.sha256,
    }:
        raise PublicationError("publication manifest archive identity is invalid")
    venues = manifest.get("venues")
    if not isinstance(venues, dict) or set(venues) != set(SUPPORTED_VENUES):
        raise PublicationError("publication manifest must name every supported venue")
    for venue in SUPPORTED_VENUES:
        entry = venues[venue]
        if not isinstance(entry, dict):
            raise PublicationError(f"publication manifest venue {venue} is invalid")
        target_file = entry.get("target_file")
        if not isinstance(target_file, dict) or target_file.get("file") != f"targets_{venue}.json":
            raise PublicationError(f"publication manifest target file for {venue} is invalid")
        target_path = manifest_path.parent / target_file["file"]
        if _identity_record(target_path) != target_file:
            raise PublicationError(f"publication target file identity drifted for {venue}")
        try:
            loaded = load_targets(target_path, venue=venue)
        except TargetsError as error:
            raise PublicationError(f"publication target file for {venue} is invalid: {error}") from error
        if (
            entry.get("target_digest") != loaded.digest
            or entry.get("metadata_digest") != loaded.metadata_digest
            or entry.get("target_count") != len(loaded)
        ):
            raise PublicationError(f"publication manifest target metadata drifted for {venue}")
        if loaded.targets != tuple(expected[venue]):
            raise PublicationError(f"publication targets no longer match selection report for {venue}")


def audit_current_publication(
    *,
    live_root: Path,
    output_root: Path,
    store: ObjectStore,
    strategy: Strategy,
) -> PublicationAudit:
    pointer_path = publication_pointer_path(live_root)
    run_id = read_publication_pointer(live_root)
    run_directory = Path(output_root) / run_id
    receipt = read_run_archive_receipt(run_directory / PRODUCTION_RECEIPT_FILE)
    verify_run_archive(store, receipt)
    validate_local_run(run_directory, receipt)
    report = _selection_report(run_directory, strategy)
    expected = _targets_from_report(
        report,
        receipt,
        strategy,
        _catalog_markets(run_directory, receipt, report),
    )
    counts: dict[str, int] = {}
    for venue in SUPPORTED_VENUES:
        try:
            loaded = load_targets(pointer_path, venue=venue)
        except TargetsError as error:
            raise PublicationError(f"current publication is invalid for {venue}: {error}") from error
        if loaded.targets != tuple(expected[venue]):
            raise PublicationError(f"current {venue} targets do not match archived selection")
        counts[venue] = len(loaded)
    return PublicationAudit(run_id=run_id, venue_counts=dict(sorted(counts.items())))
