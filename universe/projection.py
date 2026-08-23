"""Deterministic SQL projection of bundles selected by Targeter v3.

The immutable selection report remains authoritative. This module derives only
the selected hot path needed by Event Universe and creates no additional S3
artifact, so it works with every already-archived v3 report.
"""

from __future__ import annotations

import math
from typing import Any, Mapping

from archive.storage.base import normalize_key
from targeter.v2.models import SUPPORTED_VENUES, isoformat, parse_timestamp

PROJECTION_VERSION = 1


class ProjectionError(ValueError):
    """A v3 selection report cannot produce selected-bundle history."""


# Internal helpers retain the old local name to keep this projection change
# mechanical; callers use ProjectionError.
SelectedBundleIndexError = ProjectionError


def project_selected_bundles(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Project selected occurrences from one committed Targeter report."""
    if report.get("report_version") != 3:
        raise ProjectionError("selected-bundle projection requires report version 3")
    if report.get("mode") != "shadow":
        raise ProjectionError("selected-bundle projection requires a shadow report")
    run_id = _text(report, "run_id", "report")
    generated_at = _timestamp(report, "generated_at", "report")
    input_complete = report.get("input_complete")
    if input_complete is not True:
        raise ProjectionError("selected-bundle projection requires complete input")
    strategy_version = _integer(report, "strategy_version", "report")
    if strategy_version == 0:
        raise ProjectionError("report.strategy_version must be positive")

    selection = report.get("selection")
    if not isinstance(selection, Mapping):
        raise SelectedBundleIndexError("report.selection must be an object")
    selected_ids = _text_list(selection.get("bundle_ids"), "selection.bundle_ids")
    if len(selected_ids) != len(set(selected_ids)):
        raise SelectedBundleIndexError("selection.bundle_ids contains duplicates")
    if selection.get("bundle_count") != len(selected_ids):
        raise ProjectionError("selection.bundle_count disagrees with bundle_ids")
    selected = set(selected_ids)

    continuity = report.get("continuity")
    if not isinstance(continuity, Mapping):
        raise SelectedBundleIndexError("report.continuity must be an object")
    retained_ids = set(
        _text_list(
            continuity.get("retained_bundle_ids"),
            "continuity.retained_bundle_ids",
        )
    )
    if not retained_ids <= selected:
        raise SelectedBundleIndexError("continuity retains an unselected bundle")
    raw_dispositions = continuity.get("dispositions")
    if not isinstance(raw_dispositions, Mapping) or not all(
        isinstance(key, str)
        and key
        and isinstance(value, str)
        and value
        for key, value in raw_dispositions.items()
    ):
        raise SelectedBundleIndexError("continuity.dispositions must be a text map")
    dispositions = dict(raw_dispositions)
    raw_continuity_bundles = continuity.get("bundles")
    if not isinstance(raw_continuity_bundles, list):
        raise SelectedBundleIndexError("continuity.bundles must be an array")
    continuity_by_bundle: dict[str, Mapping[str, Any]] = {}
    for value in raw_continuity_bundles:
        if not isinstance(value, Mapping):
            raise SelectedBundleIndexError("continuity bundle must be an object")
        bundle_id = _text(value, "bundle_id", "continuity bundle")
        if bundle_id in continuity_by_bundle:
            raise SelectedBundleIndexError(f"continuity repeats bundle {bundle_id}")
        continuity_by_bundle[bundle_id] = value
    if retained_ids - set(continuity_by_bundle):
        raise SelectedBundleIndexError("retained bundle has no continuity evidence")

    candidates = report.get("candidates")
    if not isinstance(candidates, list):
        raise SelectedBundleIndexError("report.candidates must be an array")
    by_bundle: dict[str, Mapping[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            raise SelectedBundleIndexError("report candidate must be an object")
        bundle_id = _text(candidate, "bundle_id", "candidate")
        if bundle_id in by_bundle:
            raise SelectedBundleIndexError(f"report repeats candidate {bundle_id}")
        by_bundle[bundle_id] = candidate
    missing = selected - set(by_bundle) - retained_ids
    if missing:
        raise SelectedBundleIndexError(
            "selected bundles have neither a candidate nor retained origin: "
            f"{', '.join(sorted(missing))}"
        )

    targets = selection.get("targets")
    if not isinstance(targets, Mapping) or set(targets) != set(SUPPORTED_VENUES):
        raise ProjectionError("selection.targets must name every supported venue")
    targets_by_bundle: dict[str, list[dict[str, Any]]] = {
        bundle_id: [] for bundle_id in selected
    }
    seen_targets: set[str] = set()
    for venue, values in sorted(targets.items()):
        if not isinstance(venue, str) or not venue or not isinstance(values, list):
            raise SelectedBundleIndexError("selection target venue is invalid")
        for position, value in enumerate(values):
            if not isinstance(value, Mapping):
                raise SelectedBundleIndexError(
                    f"selection target {venue}[{position}] must be an object"
                )
            bundle_id = _text(value, "bundle_id", "selection target")
            if bundle_id not in selected:
                raise SelectedBundleIndexError(
                    f"selection target names unselected bundle {bundle_id}"
                )
            target_id = _text(value, "target_id", "selection target")
            if target_id in seen_targets:
                raise SelectedBundleIndexError(
                    f"selection repeats target {target_id}"
                )
            if not target_id.startswith(venue + ":"):
                raise SelectedBundleIndexError(
                    f"selection target {target_id} is grouped under {venue}"
                )
            seen_targets.add(target_id)
            activation = _timestamp(value, "activation_at", "selection target")
            capture_start = _timestamp(value, "capture_start_at", "selection target")
            score = value.get("continuity_score")
            if (
                isinstance(score, bool)
                or not isinstance(score, (int, float))
                or not math.isfinite(float(score))
            ):
                raise SelectedBundleIndexError(
                    f"selection target {target_id} continuity_score is invalid"
                )
            targets_by_bundle[bundle_id].append(
                {
                    "venue": venue,
                    "target_id": target_id,
                    "canonical_class": _text(
                        value, "canonical_class", "selection target"
                    ),
                    "subscription_ids": sorted(
                        _text_list(
                            value.get("subscription_ids"),
                            "selection target subscription_ids",
                        )
                    ),
                    "activation_at": isoformat(activation),
                    "capture_start_at": isoformat(capture_start),
                    "source_ref": _text(value, "source_ref", "selection target"),
                }
            )

    rows: list[dict[str, Any]] = []
    for bundle_id in sorted(selected):
        if bundle_id in retained_ids:
            retained_targets = sorted(
                targets_by_bundle[bundle_id],
                key=lambda item: (item["venue"], item["target_id"]),
            )
            rows.append(
                _retained_row(
                    run_id=run_id,
                    generated_at=generated_at,
                    input_complete=input_complete,
                    strategy_version=strategy_version,
                    bundle_id=bundle_id,
                    evidence=continuity_by_bundle[bundle_id],
                    disposition=dispositions.get(bundle_id),
                    targets=retained_targets,
                )
            )
            continue
        candidate = by_bundle[bundle_id]
        if candidate.get("eligible") is not True:
            raise ProjectionError(f"selected candidate {bundle_id} is not eligible")
        activation = _timestamp(candidate, "activation_at", f"candidate {bundle_id}")
        capture_start = _timestamp(
            candidate, "capture_start_at", f"candidate {bundle_id}"
        )
        if capture_start >= activation:
            raise SelectedBundleIndexError(
                f"candidate {bundle_id} capture_start_at must precede activation"
            )
        bundle_targets = sorted(
            targets_by_bundle[bundle_id],
            key=lambda item: (item["venue"], item["target_id"]),
        )
        if not bundle_targets:
            raise SelectedBundleIndexError(
                f"selected bundle {bundle_id} has no selected targets"
            )
        for target in bundle_targets:
            if (
                target["activation_at"] != isoformat(activation)
                or target["capture_start_at"] != isoformat(capture_start)
            ):
                raise SelectedBundleIndexError(
                    f"selected bundle {bundle_id} target timing disagrees with its candidate"
                )
        selected_market_ids = {item["target_id"] for item in bundle_targets}
        market_ids = _text_list(
            candidate.get("market_ids"), f"candidate {bundle_id} market_ids"
        )
        if not selected_market_ids.issubset(market_ids):
            raise SelectedBundleIndexError(
                f"selected bundle {bundle_id} targets are absent from its sibling markets"
            )
        event_refs = sorted(
            _text_list(
                candidate.get("event_refs"), f"candidate {bundle_id} event_refs"
            )
        )
        participants = _pair(candidate.get("participants"), "participants", bundle_id)
        participant_keys = _pair(
            candidate.get("participant_keys"), "participant_keys", bundle_id
        )
        disposition = dispositions.get(bundle_id)
        if disposition not in {None, "held_current_candidate"}:
            raise SelectedBundleIndexError(
                f"selected candidate {bundle_id} has invalid continuity disposition"
            )
        if disposition is not None and bundle_id not in continuity_by_bundle:
            raise ProjectionError(
                f"held candidate {bundle_id} has no continuity evidence"
            )
        rows.append(
            {
                "projection_version": PROJECTION_VERSION,
                "occurrence_kind": "complete",
                "run_id": run_id,
                "generated_at": isoformat(generated_at),
                "input_complete": input_complete,
                "strategy_version": strategy_version,
                "bundle_id": bundle_id,
                "origin_run_id": run_id,
                "continuity_selected": disposition is not None,
                "continuity_disposition": disposition,
                "sport": _text(candidate, "sport", f"candidate {bundle_id}"),
                "game": _optional_text(candidate.get("game"), "game", bundle_id),
                "topology": _optional_text(
                    candidate.get("topology"), "topology", bundle_id
                ),
                "participants": list(participants),
                "participant_keys": list(participant_keys),
                "activation_at": isoformat(activation),
                "capture_start_at": isoformat(capture_start),
                "event_refs": event_refs,
                "markets": [
                    {
                        "target_id": target_id,
                        "venue": _venue_of(target_id),
                        "selected": target_id in selected_market_ids,
                    }
                    for target_id in sorted(set(market_ids))
                ],
                "targets": bundle_targets,
                "relationships": _relationships(candidate, bundle_id),
            }
        )
    return rows


def _retained_row(
    *,
    run_id: str,
    generated_at: Any,
    input_complete: bool,
    strategy_version: int,
    bundle_id: str,
    evidence: Mapping[str, Any],
    disposition: str | None,
    targets: list[dict[str, Any]],
) -> dict[str, Any]:
    if disposition != "retained":
        raise SelectedBundleIndexError(
            f"retained bundle {bundle_id} has invalid continuity disposition"
        )
    if not targets:
        raise SelectedBundleIndexError(f"retained bundle {bundle_id} has no targets")
    activation = _timestamp(evidence, "activation_at", f"continuity bundle {bundle_id}")
    raw_targets = evidence.get("targets")
    if not isinstance(raw_targets, list) or not raw_targets:
        raise SelectedBundleIndexError(
            f"continuity bundle {bundle_id} targets must be a non-empty array"
        )
    continuity_targets: list[dict[str, Any]] = []
    for target in raw_targets:
        if not isinstance(target, Mapping):
            raise SelectedBundleIndexError(
                f"continuity bundle {bundle_id} target must be an object"
            )
        target_id = _text(target, "target_id", "continuity target")
        continuity_targets.append(
            {
                "venue": _text(target, "venue", "continuity target"),
                "target_id": target_id,
                "canonical_class": _text(
                    target, "canonical_class", "continuity target"
                ),
                "subscription_ids": sorted(
                    _text_list(
                        target.get("subscription_ids"),
                        "continuity target subscription_ids",
                    )
                ),
                "activation_at": isoformat(
                    _timestamp(target, "activation_at", "continuity target")
                ),
                "capture_start_at": isoformat(
                    _timestamp(target, "capture_start_at", "continuity target")
                ),
                "source_ref": _text(target, "source_ref", "continuity target"),
            }
        )
    normalized = sorted(
        continuity_targets, key=lambda item: (item["venue"], item["target_id"])
    )
    if targets != normalized:
        raise SelectedBundleIndexError(
            f"retained bundle {bundle_id} targets disagree with continuity evidence"
        )
    if any(target["activation_at"] != isoformat(activation) for target in targets):
        raise SelectedBundleIndexError(
            f"retained bundle {bundle_id} activation disagrees across targets"
        )
    capture_starts = {target["capture_start_at"] for target in targets}
    if len(capture_starts) != 1:
        raise SelectedBundleIndexError(
            f"retained bundle {bundle_id} capture start disagrees across targets"
        )
    origin_key = _text(
        evidence,
        "origin_archive_manifest_key",
        f"continuity bundle {bundle_id}",
    )
    try:
        normalize_key(origin_key)
    except ValueError as error:
        raise SelectedBundleIndexError(
            f"continuity bundle {bundle_id} origin manifest key is invalid"
        ) from error
    return {
        "projection_version": PROJECTION_VERSION,
        "occurrence_kind": "retained",
        "run_id": run_id,
        "generated_at": isoformat(generated_at),
        "input_complete": input_complete,
        "strategy_version": strategy_version,
        "bundle_id": bundle_id,
        "origin_run_id": _text(
            evidence, "origin_run_id", f"continuity bundle {bundle_id}"
        ),
        "origin_report_sha256": _sha256(
            evidence.get("origin_report_sha256"),
            f"continuity bundle {bundle_id} origin report",
        ),
        "origin_archive_manifest_key": origin_key,
        "origin_archive_manifest_sha256": _sha256(
            evidence.get("origin_archive_manifest_sha256"),
            f"continuity bundle {bundle_id} origin manifest",
        ),
        "continuity_selected": True,
        "continuity_disposition": disposition,
        "activation_at": isoformat(activation),
        "capture_start_at": next(iter(capture_starts)),
        "targets": targets,
    }


def _relationships(
    candidate: Mapping[str, Any], bundle_id: str
) -> list[dict[str, str]]:
    analysis = candidate.get("relationship_analysis")
    if not isinstance(analysis, Mapping):
        raise SelectedBundleIndexError(
            f"candidate {bundle_id} relationship_analysis must be an object"
        )
    values = analysis.get("relationships")
    if not isinstance(values, list):
        raise SelectedBundleIndexError(
            f"candidate {bundle_id} relationships must be an array"
        )
    output: list[dict[str, str]] = []
    fields = (
        "left",
        "right",
        "relationship",
        "scope",
        "left_venue",
        "right_venue",
        "coverage",
    )
    for value in values:
        if not isinstance(value, Mapping) or value.get("bundle_id") != bundle_id:
            raise SelectedBundleIndexError(
                f"candidate {bundle_id} has an invalid relationship"
            )
        output.append(
            {field: _text(value, field, "relationship") for field in fields}
        )
    return sorted(
        output,
        key=lambda item: (
            item["left"],
            item["right"],
            item["relationship"],
            item["scope"],
        ),
    )


def _venue_of(target_id: str) -> str:
    venue, separator, _identifier = target_id.partition(":")
    if not separator or not venue:
        raise SelectedBundleIndexError(
            f"market target_id has no venue prefix: {target_id}"
        )
    return venue


def _pair(value: Any, field: str, bundle_id: str) -> tuple[str, str]:
    values = _text_list(value, f"candidate {bundle_id} {field}")
    if len(values) != 2:
        raise SelectedBundleIndexError(
            f"candidate {bundle_id} {field} must contain two values"
        )
    return values[0], values[1]


def _text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise SelectedBundleIndexError(f"{label}.{field} must be non-empty text")
    return value


def _optional_text(value: Any, field: str, bundle_id: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise SelectedBundleIndexError(
            f"candidate {bundle_id}.{field} must be non-empty text or null"
        )
    return value


def _text_list(value: Any, label: str) -> list[str]:
    if (
        not isinstance(value, list)
        or not all(isinstance(item, str) and item for item in value)
        or len(value) != len(set(value))
    ):
        raise SelectedBundleIndexError(f"{label} must contain unique text values")
    return list(value)


def _integer(document: Mapping[str, Any], field: str, label: str) -> int:
    value = document.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise SelectedBundleIndexError(f"{label}.{field} must be a non-negative integer")
    return value


def _sha256(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise SelectedBundleIndexError(f"{label} sha256 is invalid")
    return value


def _timestamp(document: Mapping[str, Any], field: str, label: str):
    value = parse_timestamp(document.get(field))
    if value is None:
        raise SelectedBundleIndexError(f"{label}.{field} must be a UTC timestamp")
    return value
