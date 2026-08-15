"""Atomic publication and audit of Targeter v2 splice subscriptions.

All venue files are materialized inside an immutable run generation.  Its
``manifest.json`` commits those files; only then is one small ``current.json``
pointer replaced.  Every splice reads the same pointer and selects its venue,
so a crash cannot expose a mixture of old and new venue generations.
"""

from __future__ import annotations

import json
import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Mapping

from archive.common.durable import confirm_durable, write_json_durable
from archive.storage.base import ObjectStore
from analysis.storage import decoded_zstd_file
from encoder import CodecError, StoredIdentity, stored_identity_of
from targeter.targets import (
    TARGET_GENERATION_POINTER_VERSION,
    TARGET_PUBLICATION_MANIFEST_VERSION,
    Target,
    TargetsError,
    load_targets,
    write_targets,
)
from targeter.coverage import CoverageLedger, created_at_of
from targeter.v2.continuity import ContinuityError, load_continuity_bundles
from targeter.v2.domain import SUPPORTED_VENUES, parse_timestamp
from targeter.v2.registry import Strategy
from targeter.v2.run_archive import (
    PRODUCTION_RECEIPT_FILE,
    RunArchiveError,
    RunArchiveReceipt,
    parse_run_id_ns,
    read_run_report,
    read_run_archive_receipt,
    validate_local_run,
    verify_run_archive,
)


class PublicationError(ValueError):
    """A run cannot safely become the splice subscription authority."""


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


def _selection_report_object(receipt: RunArchiveReceipt):
    item = next(
        (
            candidate
            for candidate in receipt.objects
            if candidate.file in {"selection_report.json", "selection_report.json.zst"}
        ),
        None,
    )
    if item is None:
        raise PublicationError("run archive receipt does not contain the selection report")
    return item


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


def publication_pointer_path(live_root: Path) -> Path:
    return Path(live_root) / "targeter-v2" / "current.json"


def read_publication_pointer(live_root: Path) -> str:
    """The run_id the live generation pointer currently names.

    Extracted so that every reader of "which run is published" is this one
    function.  The run reaper asks the same question before it deletes, and a
    reaper that answered it differently from the audit could remove the local
    evidence the audit is about to demand.
    """
    pointer = _read_json(
        publication_pointer_path(live_root), "target generation pointer"
    )
    run_id = pointer.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise PublicationError("target generation pointer has no run_id")
    return run_id


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


def _selection_report(run_directory: Path, strategy: Strategy) -> dict[str, Any]:
    try:
        report = read_run_report(run_directory)
    except (RunArchiveError, OSError) as error:
        raise PublicationError(f"cannot read selection report: {error}") from error
    if report.get("run_id") != run_directory.name:
        raise PublicationError("selection report run_id does not match its directory")
    if report.get("report_version") not in {1, 2} or report.get("mode") != "shadow":
        raise PublicationError("publication requires a phase-5 shadow report")
    if report.get("strategy_version") != strategy.version:
        raise PublicationError("selection report and publication strategy versions differ")
    if report.get("input_complete") is not True or report.get("discovery_failures") != {}:
        raise PublicationError("incomplete targeter runs cannot be published")
    catalogs = report.get("catalogs")
    if not isinstance(catalogs, list) or not catalogs or any(
        not isinstance(summary, dict) or summary.get("complete") is not True
        for summary in catalogs
    ):
        raise PublicationError("publication requires complete catalog summaries")
    catalog_venues = [summary.get("venue") for summary in catalogs]
    if (
        len(catalog_venues) != len(SUPPORTED_VENUES)
        or set(catalog_venues) != set(SUPPORTED_VENUES)
    ):
        raise PublicationError("publication requires exactly one catalog for every supported venue")
    selection = report.get("selection")
    if not isinstance(selection, dict) or selection.get("publication_performed") is not False:
        raise PublicationError("selection report has invalid publication state")
    bundle_ids = selection.get("bundle_ids")
    if (
        not isinstance(bundle_ids, list)
        or not all(isinstance(item, str) and item for item in bundle_ids)
        or len(bundle_ids) != len(set(bundle_ids))
        or selection.get("bundle_count") != len(bundle_ids)
    ):
        raise PublicationError("selection report has invalid selected bundle identifiers")
    targets = selection.get("targets")
    if not isinstance(targets, dict) or set(targets) != set(SUPPORTED_VENUES):
        raise PublicationError("selection report targets must name every supported venue")
    return report


def _targets_from_report(
    report: dict[str, Any],
    receipt: RunArchiveReceipt,
    strategy: Strategy,
    catalog_markets: Mapping[str, Mapping[str, dict[str, Any]]],
) -> dict[str, list[Target]]:
    selection = report["selection"]
    bundle_ids = frozenset(selection["bundle_ids"])
    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise PublicationError("selection report candidates must be an array")
    candidates_by_bundle: dict[str, dict[str, Any]] = {}
    for raw_candidate in candidates:
        if not isinstance(raw_candidate, dict):
            raise PublicationError("selection report candidate is not an object")
        bundle_id = raw_candidate.get("bundle_id")
        if not isinstance(bundle_id, str) or not bundle_id or bundle_id in candidates_by_bundle:
            raise PublicationError("selection report has invalid or duplicate candidate bundles")
        candidates_by_bundle[bundle_id] = raw_candidate
    continuity = report.get("continuity", {})
    if not isinstance(continuity, dict):
        raise PublicationError("selection report continuity must be an object")
    raw_continuity_bundles = continuity.get("bundles", [])
    raw_retained_bundle_ids = continuity.get("retained_bundle_ids", [])
    dispositions = continuity.get("dispositions", {})
    if not isinstance(raw_continuity_bundles, list):
        raise PublicationError("selection report continuity bundles must be an array")
    if (
        not isinstance(raw_retained_bundle_ids, list)
        or not all(isinstance(item, str) and item for item in raw_retained_bundle_ids)
        or len(raw_retained_bundle_ids) != len(set(raw_retained_bundle_ids))
        or not isinstance(dispositions, dict)
        or not all(
            isinstance(key, str) and key and isinstance(value, str) and value
            for key, value in dispositions.items()
        )
    ):
        raise PublicationError("selection report continuity state is invalid")
    continuity_by_bundle: dict[str, dict[str, Any]] = {}
    continuity_targets: dict[tuple[str, str], dict[str, Any]] = {}
    for raw_bundle in raw_continuity_bundles:
        if not isinstance(raw_bundle, dict):
            raise PublicationError("selection report continuity bundle is not an object")
        bundle_id = raw_bundle.get("bundle_id")
        base_run_id = raw_bundle.get("base_run_id")
        score = raw_bundle.get("score")
        raw_targets = raw_bundle.get("targets")
        if (
            not isinstance(bundle_id, str)
            or not bundle_id
            or bundle_id in continuity_by_bundle
            or not isinstance(base_run_id, str)
            or not base_run_id
            or isinstance(score, bool)
            or not isinstance(score, (int, float))
            or not math.isfinite(float(score))
            or not isinstance(raw_targets, list)
            or not raw_targets
        ):
            raise PublicationError("selection report has invalid continuity bundle")
        continuity_by_bundle[bundle_id] = raw_bundle
        for raw_target in raw_targets:
            if not isinstance(raw_target, dict):
                raise PublicationError(f"continuity bundle {bundle_id} has an invalid target")
            target_id = raw_target.get("target_id")
            venue = raw_target.get("venue")
            subscription_ids = raw_target.get("subscription_ids")
            if (
                not isinstance(target_id, str)
                or not target_id
                or venue not in SUPPORTED_VENUES
                or not target_id.startswith(str(venue) + ":")
                or not isinstance(subscription_ids, list)
                or not subscription_ids
                or not all(isinstance(item, str) and item for item in subscription_ids)
                or len(subscription_ids) != len(set(subscription_ids))
            ):
                raise PublicationError(f"continuity bundle {bundle_id} has an invalid target id")
            if (bundle_id, target_id) in continuity_targets:
                raise PublicationError(f"continuity bundle {bundle_id} repeats a target")
            continuity_targets[(bundle_id, target_id)] = raw_target
    retained_bundle_ids = frozenset(raw_retained_bundle_ids)
    if not retained_bundle_ids <= continuity_by_bundle.keys():
        raise PublicationError("retained bundle is absent from continuity evidence")
    if set(dispositions) != set(continuity_by_bundle):
        raise PublicationError("continuity dispositions do not account for every prior bundle")
    generated_at = parse_timestamp(report.get("generated_at"))
    if continuity_by_bundle and generated_at is None:
        raise PublicationError("selection report has invalid continuity observation time")
    for bundle_id, bundle in continuity_by_bundle.items():
        activation_at = parse_timestamp(bundle.get("activation_at"))
        probes = [
            target.get("terminal_probe")
            for target in bundle["targets"]
        ]
        if (
            activation_at is None
            or any(
                not isinstance(probe, dict)
                or probe.get("state") not in {"open", "terminal", "unknown"}
                or not isinstance(probe.get("reason"), str)
                or not probe["reason"]
                for probe in probes
            )
        ):
            raise PublicationError(f"continuity bundle {bundle_id} has invalid terminal probes")
        clamped = (
            generated_at is not None
            and generated_at.timestamp() >= (
                activation_at.timestamp() + strategy.terminal_clamp_seconds
            )
        )
        all_terminal = all(probe["state"] == "terminal" for probe in probes)
        disposition = dispositions[bundle_id]
        expected_retirement = (
            "terminal_clamp_elapsed"
            if clamped
            else "all_markets_terminal"
            if all_terminal
            else None
        )
        if expected_retirement is not None and disposition != expected_retirement:
            raise PublicationError(
                f"continuity bundle {bundle_id} terminal disposition disagrees with its probes"
            )
        if expected_retirement is None and disposition in {
            "all_markets_terminal",
            "terminal_clamp_elapsed",
        }:
            raise PublicationError(
                f"continuity bundle {bundle_id} terminal disposition disagrees with its probes"
            )
        if disposition == "retained" and (
            bundle_id not in retained_bundle_ids or bundle_id not in bundle_ids
        ):
            raise PublicationError(f"continuity bundle {bundle_id} retained disposition was not selected")
        if disposition == "held_current_candidate" and bundle_id not in bundle_ids:
            raise PublicationError(f"continuity bundle {bundle_id} held disposition was not selected")
        if disposition not in {
            "retained",
            "held_current_candidate",
            "continuity_budget_trimmed",
            "all_markets_terminal",
            "terminal_clamp_elapsed",
        }:
            raise PublicationError(f"continuity bundle {bundle_id} has an invalid disposition")

    protected = sorted(
        (
            (-float(bundle["score"]), bundle_id, bundle)
            for bundle_id, bundle in continuity_by_bundle.items()
            if dispositions[bundle_id]
            not in {"all_markets_terminal", "terminal_clamp_elapsed"}
        ),
        key=lambda item: (item[0], item[1]),
    )
    budget_used = {venue: 0 for venue in strategy.target_budgets}
    selected_protected_ids: dict[str, set[str]] = {
        venue: set() for venue in strategy.target_budgets
    }
    expected_trimmed: set[str] = set()
    protected_count = 0
    for _negative_score, bundle_id, bundle in protected:
        increments = {
            venue: sum(
                len(target["subscription_ids"])
                for target in bundle["targets"]
                if target.get("venue") == venue
                and target.get("target_id") not in selected_protected_ids[venue]
            )
            for venue in strategy.target_budgets
        }
        if protected_count >= strategy.maximum_bundles or any(
            budget_used[venue] + count > strategy.target_budgets[venue]
            for venue, count in increments.items()
        ):
            expected_trimmed.add(bundle_id)
            continue
        protected_count += 1
        for target in bundle["targets"]:
            venue = target.get("venue")
            target_id = target.get("target_id")
            if venue in selected_protected_ids and target_id not in selected_protected_ids[venue]:
                selected_protected_ids[venue].add(target_id)
                budget_used[venue] += len(target["subscription_ids"])
    actual_trimmed = {
        bundle_id
        for bundle_id, disposition in dispositions.items()
        if disposition == "continuity_budget_trimmed"
    }
    if actual_trimmed != expected_trimmed:
        raise PublicationError("continuity budget trimming disagrees with the protected floor")

    selected_continuity_targets = {
        key: value
        for key, value in continuity_targets.items()
        if key[0] in bundle_ids
    }
    selected_candidate_markets: dict[str, frozenset[str]] = {}
    for bundle_id in bundle_ids:
        candidate = candidates_by_bundle.get(bundle_id)
        retained = (
            continuity_by_bundle.get(bundle_id)
            if bundle_id in retained_bundle_ids
            else None
        )
        if retained is not None:
            eligible_market_ids = [
                target.get("target_id") for target in retained.get("targets", [])
            ]
        elif candidate is not None and candidate.get("eligible") is True:
            eligible_market_ids = candidate.get("eligible_market_ids")
        else:
            raise PublicationError(
                f"selected bundle {bundle_id} is neither eligible nor retained"
            )
        if (
            not isinstance(eligible_market_ids, list)
            or not eligible_market_ids
            or not all(isinstance(item, str) and item for item in eligible_market_ids)
            or len(eligible_market_ids) != len(set(eligible_market_ids))
        ):
            raise PublicationError(f"selected bundle {bundle_id} has invalid eligible markets")
        selected_candidate_markets[bundle_id] = frozenset(eligible_market_ids)
    report_object = _selection_report_object(receipt)
    by_venue: dict[str, list[Target]] = {venue: [] for venue in SUPPORTED_VENUES}
    bundle_venues: dict[str, set[str]] = {bundle_id: set() for bundle_id in bundle_ids}
    selected_market_ids: dict[str, set[str]] = {bundle_id: set() for bundle_id in bundle_ids}
    seen_target_ids: set[str] = set()

    for venue in SUPPORTED_VENUES:
        raw_targets = selection["targets"].get(venue)
        if not isinstance(raw_targets, list):
            raise PublicationError(f"selection targets for {venue} must be an array")
        seen_assets: dict[str, Target] = {}
        for position, raw in enumerate(raw_targets):
            if not isinstance(raw, dict):
                raise PublicationError(f"selection target {venue}[{position}] is not an object")
            bundle_id = _required_text(raw, "bundle_id", venue, position)
            if bundle_id not in bundle_ids:
                raise PublicationError(f"selection target {venue}[{position}] names an unselected bundle")
            target_id = _required_text(raw, "target_id", venue, position)
            if not target_id.startswith(venue + ":"):
                raise PublicationError(f"selection target {target_id!r} belongs to another venue")
            if target_id in seen_target_ids:
                raise PublicationError(f"selection report repeats target {target_id!r}")
            seen_target_ids.add(target_id)
            canonical_class = _required_text(raw, "canonical_class", venue, position)
            subscription_ids = raw.get("subscription_ids")
            if (
                not isinstance(subscription_ids, list)
                or not subscription_ids
                or not all(isinstance(item, str) and item.strip() for item in subscription_ids)
                or len(subscription_ids) != len(set(subscription_ids))
            ):
                raise PublicationError(
                    f"selection target {venue}[{position}] has invalid subscription_ids"
                )
            catalog = catalog_markets[venue].get(target_id)
            retained_target = selected_continuity_targets.get((bundle_id, target_id))
            if retained_target is None:
                if catalog is None:
                    raise PublicationError(f"selection target {target_id!r} is absent from its catalog")
                if (
                    catalog.get("canonical_class") != canonical_class
                    or catalog.get("subscription_ids") != subscription_ids
                    or catalog.get("source_ref") != raw.get("source_ref")
                ):
                    raise PublicationError(
                        f"selection target {target_id!r} disagrees with its archived catalog"
                    )
                candidate = candidates_by_bundle[bundle_id]
                if (
                    raw.get("activation_at") != candidate.get("activation_at")
                    or raw.get("capture_start_at") != candidate.get("capture_start_at")
                ):
                    raise PublicationError(
                        f"selection target {target_id!r} timing disagrees with its candidate"
                    )
            elif (
                retained_target.get("venue") != venue
                or retained_target.get("canonical_class") != canonical_class
                or retained_target.get("subscription_ids") != subscription_ids
                or retained_target.get("source_ref") != raw.get("source_ref")
                or retained_target.get("activation_at") != raw.get("activation_at")
                or retained_target.get("capture_start_at") != raw.get("capture_start_at")
            ):
                raise PublicationError(
                    f"selection target {target_id!r} disagrees with retained continuity evidence"
                )
            bundle_venues[bundle_id].add(venue)
            selected_market_ids[bundle_id].add(target_id)
            continuity_score = raw.get("continuity_score", 0.0)
            if (
                isinstance(continuity_score, bool)
                or not isinstance(continuity_score, (int, float))
                or not math.isfinite(float(continuity_score))
            ):
                raise PublicationError(
                    f"selection target {target_id!r} has invalid continuity score"
                )
            resolution = {
                "version": 2,
                "source": "targeter_v2",
                "run_id": report["run_id"],
                "bundle_id": bundle_id,
                "target_id": target_id,
                "canonical_class": canonical_class,
                "activation_at": raw.get("activation_at"),
                "capture_start_at": raw.get("capture_start_at"),
                "source_ref": raw.get("source_ref"),
                "selection_report_sha256": report_object.stored.sha256,
                "archive_manifest_key": receipt.manifest.key,
                "archive_manifest_sha256": receipt.manifest.stored.sha256,
                "continuity_score": float(continuity_score),
                "continuity_base_run_id": (
                    continuity_by_bundle[bundle_id]["base_run_id"]
                    if bundle_id in continuity_by_bundle
                    else report["run_id"]
                ),
            }
            for asset_id in subscription_ids:
                target = Target(
                    asset_id=asset_id,
                    market_id=target_id.split(":", 1)[1],
                    note=canonical_class,
                    resolution=resolution,
                )
                existing = seen_assets.get(asset_id)
                if existing is not None and existing != target:
                    raise PublicationError(
                        f"subscription id {asset_id!r} has conflicting selection provenance"
                    )
                seen_assets[asset_id] = target
        by_venue[venue] = sorted(seen_assets.values(), key=lambda item: item.asset_id)

    terminal_empty = (
        not bundle_ids
        and not any(by_venue.values())
        and bool(continuity_by_bundle)
        and set(dispositions) == set(continuity_by_bundle)
        and all(
            reason in {"all_markets_terminal", "terminal_clamp_elapsed"}
            for reason in dispositions.values()
        )
    )
    if (not bundle_ids or not any(by_venue.values())) and not terminal_empty:
        raise PublicationError(
            "empty target selections require terminal continuity evidence or explicit human review"
        )
    for bundle_id, venues in bundle_venues.items():
        if len(venues) < strategy.minimum_venues:
            raise PublicationError(
                f"selected bundle {bundle_id} has targets on only {len(venues)} venues"
            )
        if selected_market_ids[bundle_id] != selected_candidate_markets[bundle_id]:
            raise PublicationError(
                f"selected bundle {bundle_id} targets do not match its eligible candidate markets"
            )
    if (
        not terminal_empty
        and len({venue for venue, targets in by_venue.items() if targets}) < strategy.minimum_venues
    ):
        raise PublicationError("publication has fewer than the configured minimum venues")
    return by_venue


def _verify_continuity_base(
    report: Mapping[str, Any], live_root: Path, strategy: Strategy
) -> None:
    continuity = report.get("continuity")
    if isinstance(continuity, dict):
        observed = continuity.get("bundles")
    elif report.get("report_version") == 1:
        observed = []
    else:
        observed = None
    if not isinstance(observed, list):
        raise PublicationError("selection report continuity bundles must be an array")
    pointer = publication_pointer_path(live_root)
    degraded_base_run_id = report.get("continuity_degraded_base_run_id")
    if not pointer.exists():
        if observed or degraded_base_run_id is not None:
            raise PublicationError("selection report claims continuity without a committed generation")
        return
    try:
        expected = load_continuity_bundles(pointer)
    except (ContinuityError, TargetsError) as error:
        current_run_id = read_publication_pointer(live_root)
        generated_at = parse_timestamp(report.get("generated_at"))
        current_run_ns = parse_run_id_ns(current_run_id)
        old_enough = (
            generated_at is not None
            and current_run_ns is not None
            and generated_at.timestamp() - current_run_ns / 1_000_000_000
            >= strategy.continuity_degraded_after_seconds
        )
        if (
            observed
            or degraded_base_run_id != current_run_id
            or not old_enough
        ):
            raise PublicationError(
                "selection report cannot prove its degraded continuity authority"
            ) from error
        return
    if degraded_base_run_id is not None:
        raise PublicationError("selection report has an unsupported continuity degradation marker")

    def without_probe(bundle: Mapping[str, Any]) -> dict[str, Any]:
        record = dict(bundle)
        targets = record.get("targets")
        if isinstance(targets, list):
            record["targets"] = [
                {key: value for key, value in target.items() if key != "terminal_probe"}
                for target in targets
                if isinstance(target, dict)
            ]
        return record

    observed_records = {
        str(bundle.get("bundle_id")): without_probe(bundle)
        for bundle in observed
        if isinstance(bundle, dict)
    }
    expected_records = {
        bundle.bundle_id: without_probe(bundle.as_record()) for bundle in expected
    }
    if observed_records != expected_records:
        raise PublicationError(
            "selection report continuity evidence does not match the committed base generation"
        )


@contextmanager
def catalog_reader(path: Path, item: Any) -> Iterator[BinaryIO]:
    if item.content_encoding == "zstd":
        if item.logical is None:
            raise PublicationError(f"compressed catalog {path.name} has no decoded identity")
        try:
            with decoded_zstd_file(
                path,
                expected_logical=item.logical,
                expected_stored=item.stored,
            ) as handle:
                yield handle
        except (CodecError, OSError) as error:
            raise PublicationError(f"cannot decode archived catalog {path}: {error}") from error
    elif item.content_encoding is None:
        try:
            with path.open("rb") as handle:
                yield handle
        except OSError as error:
            raise PublicationError(f"cannot read archived catalog {path}: {error}") from error
    else:  # receipt parsing should make this impossible
        raise PublicationError(f"catalog {path.name} has unsupported content encoding")


def _catalog_markets(
    run_directory: Path,
    receipt: RunArchiveReceipt,
    report: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    """Read the archived catalogue boundary used to produce subscriptions."""
    catalogs: dict[str, dict[str, dict[str, Any]]] = {}
    suffix = ".ndjson.zst" if report.get("artifact_format") == "zstd" else ".ndjson"
    for venue in SUPPORTED_VENUES:
        name = f"catalog_{venue}_markets{suffix}"
        path = Path(run_directory) / name
        item = next((candidate for candidate in receipt.objects if candidate.file == name), None)
        if item is None:
            raise PublicationError(f"archive receipt has no catalog {name}")
        records: dict[str, dict[str, Any]] = {}
        try:
            with catalog_reader(path, item) as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.endswith(b"\n") or not line.strip():
                        raise PublicationError(
                            f"catalog {path.name}:{line_number} is not one complete NDJSON record"
                        )
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise PublicationError(
                            f"catalog {path.name}:{line_number} is invalid JSON: {error}"
                        ) from error
                    if not isinstance(record, dict) or record.get("venue") != venue:
                        raise PublicationError(
                            f"catalog {path.name}:{line_number} has the wrong venue or shape"
                        )
                    target_id = record.get("target_id")
                    if not isinstance(target_id, str) or not target_id.startswith(venue + ":"):
                        raise PublicationError(
                            f"catalog {path.name}:{line_number} has an invalid target_id"
                        )
                    if target_id in records:
                        raise PublicationError(f"catalog {path.name} repeats target {target_id!r}")
                    records[target_id] = record
        except OSError as error:
            raise PublicationError(f"cannot read archived catalog {path}: {error}") from error
        catalogs[venue] = records
    return catalogs


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
    derived from rather than re-fetched later. `Market.as_record` already
    normalises it to ISO-8601 (`targeter/v2/domain.py:355`); `created_at_of`
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


def _required_text(raw: dict[str, Any], field: str, venue: str, position: int) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value:
        raise PublicationError(f"selection target {venue}[{position}] has no {field}")
    return value


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
