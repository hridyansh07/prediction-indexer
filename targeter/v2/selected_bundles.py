"""Compact, deterministic evidence for bundles selected by Targeter v2.

The full selection report remains the diagnostic authority.  This projection
contains only the selected hot path needed by Event Universe: event identity,
planned capture bounds, sibling references, selected targets/assets, and
relationship edges.  It deliberately contains no venue record or report JSON.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Mapping

from targeter.v2.domain import isoformat, parse_timestamp

SELECTED_BUNDLE_INDEX_VERSION = 1
SELECTED_BUNDLE_INDEX_STEM = "selected_bundle_index"

# Reports written before selection_policy was added still identify the closed
# strategy version that produced them.  Keep legacy interpretation explicit;
# never apply today's mutable config to historical evidence.
_LEGACY_POST_START_RETENTION_SECONDS = {1: 21_600, 2: 21_600, 3: 21_600}
_LEGACY_PRE_EVENT_SECONDS = {1: 3_600, 2: 3_600, 3: 3_600}


class SelectedBundleIndexError(ValueError):
    """A selection report cannot produce the compact selected-bundle index."""


def selected_bundle_rows(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Project selected bundles from one committed Targeter report."""
    run_id = _text(report, "run_id", "report")
    generated_at = _timestamp(report, "generated_at", "report")
    input_complete = report.get("input_complete")
    if not isinstance(input_complete, bool):
        raise SelectedBundleIndexError("report.input_complete must be boolean")
    strategy_version = _integer(report, "strategy_version", "report")
    pre_event_seconds, retention_seconds = _selection_policy(
        report, strategy_version
    )

    selection = report.get("selection")
    if not isinstance(selection, Mapping):
        raise SelectedBundleIndexError("report.selection must be an object")
    selected_ids = _text_list(selection.get("bundle_ids"), "selection.bundle_ids")
    if len(selected_ids) != len(set(selected_ids)):
        raise SelectedBundleIndexError("selection.bundle_ids contains duplicates")
    selected = set(selected_ids)

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
    missing = selected - set(by_bundle)
    if missing:
        raise SelectedBundleIndexError(
            f"selected bundles have no candidate: {', '.join(sorted(missing))}"
        )

    targets = selection.get("targets")
    if not isinstance(targets, Mapping):
        raise SelectedBundleIndexError("selection.targets must be an object")
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
                }
            )

    rows: list[dict[str, Any]] = []
    for bundle_id in sorted(selected):
        candidate = by_bundle[bundle_id]
        activation = _timestamp(candidate, "activation_at", f"candidate {bundle_id}")
        capture_start = _timestamp(
            candidate, "capture_start_at", f"candidate {bundle_id}"
        )
        expected_start = activation - timedelta(seconds=pre_event_seconds)
        if capture_start != expected_start:
            raise SelectedBundleIndexError(
                f"candidate {bundle_id} capture_start_at disagrees with its policy"
            )
        bundle_targets = sorted(
            targets_by_bundle[bundle_id],
            key=lambda item: (item["venue"], item["target_id"]),
        )
        if not bundle_targets:
            raise SelectedBundleIndexError(
                f"selected bundle {bundle_id} has no selected targets"
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
        rows.append(
            {
                "selected_bundle_index_version": SELECTED_BUNDLE_INDEX_VERSION,
                "run_id": run_id,
                "generated_at": isoformat(generated_at),
                "input_complete": input_complete,
                "strategy_version": strategy_version,
                "post_start_retention_seconds": retention_seconds,
                "bundle_id": bundle_id,
                "sport": _text(candidate, "sport", f"candidate {bundle_id}"),
                "game": _optional_text(candidate.get("game"), "game", bundle_id),
                "topology": _optional_text(
                    candidate.get("topology"), "topology", bundle_id
                ),
                "participants": list(participants),
                "participant_keys": list(participant_keys),
                "activation_at": isoformat(activation),
                "capture_start_at": isoformat(capture_start),
                "planned_capture_end_at": isoformat(
                    activation + timedelta(seconds=retention_seconds)
                ),
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


def _selection_policy(
    report: Mapping[str, Any], strategy_version: int
) -> tuple[int, int]:
    policy = report.get("selection_policy")
    if policy is None:
        try:
            return (
                _LEGACY_PRE_EVENT_SECONDS[strategy_version],
                _LEGACY_POST_START_RETENTION_SECONDS[strategy_version],
            )
        except KeyError as error:
            raise SelectedBundleIndexError(
                f"report strategy {strategy_version} has no persisted selection policy"
            ) from error
    if not isinstance(policy, Mapping) or set(policy) != {
        "pre_event_seconds",
        "post_start_retention_seconds",
    }:
        raise SelectedBundleIndexError("report.selection_policy fields are invalid")
    return (
        _positive_integer(policy, "pre_event_seconds", "selection_policy"),
        _positive_integer(
            policy, "post_start_retention_seconds", "selection_policy"
        ),
    )


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


def _positive_integer(document: Mapping[str, Any], field: str, label: str) -> int:
    value = _integer(document, field, label)
    if value == 0:
        raise SelectedBundleIndexError(f"{label}.{field} must be positive")
    return value


def _timestamp(document: Mapping[str, Any], field: str, label: str):
    value = parse_timestamp(document.get(field))
    if value is None:
        raise SelectedBundleIndexError(f"{label}.{field} must be a UTC timestamp")
    return value
