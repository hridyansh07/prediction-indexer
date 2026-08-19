"""Cross-run subscription continuity for Targeter v2.

The committed target generation is the only continuity authority. Terminal
observations are deliberately ephemeral: every scheduled run probes the exact
markets in that generation, and a bundle is retained unless every market is
affirmatively terminal or its configured clamp has elapsed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Mapping

from archive.storage.base import normalize_key
from targeter.targets import Target, load_targets
from targeter.v2.models import SUPPORTED_VENUES, isoformat, parse_timestamp


def _required_sha256(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ContinuityError(f"published continuity metadata has invalid {field}")
    return value


class TerminalState(str, Enum):
    OPEN = "open"
    TERMINAL = "terminal"
    UNKNOWN = "unknown"


class ContinuityError(ValueError):
    """A valid generation lacks the metadata needed for continuity."""


@dataclass(frozen=True)
class TerminalProbe:
    state: TerminalState
    reason: str

    def __post_init__(self) -> None:
        if isinstance(self.state, str):
            try:
                object.__setattr__(self, "state", TerminalState(self.state))
            except ValueError as error:
                raise ValueError(f"invalid terminal probe state: {self.state}") from error
        elif not isinstance(self.state, TerminalState):
            raise ValueError(f"invalid terminal probe state: {self.state}")
        if not self.reason:
            raise ValueError("terminal probe reason is required")

    def as_record(self) -> dict[str, str]:
        return {"state": self.state.value, "reason": self.reason}


@dataclass(frozen=True)
class ContinuityTarget:
    target_id: str
    venue: str
    venue_market_id: str
    canonical_class: str
    subscription_ids: tuple[str, ...]
    activation_at: datetime
    capture_start_at: datetime
    source_ref: str
    probe: TerminalProbe = TerminalProbe(TerminalState.UNKNOWN, "not_probed")

    def __post_init__(self) -> None:
        if self.venue not in SUPPORTED_VENUES:
            raise ValueError(f"unsupported continuity venue: {self.venue}")
        if self.target_id != f"{self.venue}:{self.venue_market_id}":
            raise ValueError("continuity target_id does not match venue market id")
        if not self.canonical_class or not self.subscription_ids or not self.source_ref:
            raise ValueError("continuity target metadata is incomplete")
        if len(set(self.subscription_ids)) != len(self.subscription_ids):
            raise ValueError("continuity subscription ids must be unique")
        if self.activation_at.tzinfo is None or self.capture_start_at.tzinfo is None:
            raise ValueError("continuity timestamps must be timezone-aware")

    def as_selection_target(self, bundle_id: str, score: float) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "bundle_id": bundle_id,
            "canonical_class": self.canonical_class,
            "subscription_ids": list(self.subscription_ids),
            "activation_at": isoformat(self.activation_at),
            "capture_start_at": isoformat(self.capture_start_at),
            "source_ref": self.source_ref,
            "continuity_score": score,
        }

    def as_record(self) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "venue": self.venue,
            "venue_market_id": self.venue_market_id,
            "canonical_class": self.canonical_class,
            "subscription_ids": list(self.subscription_ids),
            "activation_at": isoformat(self.activation_at),
            "capture_start_at": isoformat(self.capture_start_at),
            "source_ref": self.source_ref,
            "terminal_probe": self.probe.as_record(),
        }


@dataclass(frozen=True)
class ContinuityOrigin:
    run_id: str
    report_sha256: str
    archive_manifest_key: str
    archive_manifest_sha256: str

    def __post_init__(self) -> None:
        if not self.run_id or not self.archive_manifest_key:
            raise ValueError("continuity origin identity is incomplete")
        normalize_key(self.archive_manifest_key)
        _required_sha256(self.report_sha256, "origin report hash")
        _required_sha256(self.archive_manifest_sha256, "origin manifest hash")

    def as_record(self) -> dict[str, str]:
        return {
            "origin_run_id": self.run_id,
            "origin_report_sha256": self.report_sha256,
            "origin_archive_manifest_key": self.archive_manifest_key,
            "origin_archive_manifest_sha256": self.archive_manifest_sha256,
        }


@dataclass(frozen=True)
class ContinuityBundle:
    base_run_id: str
    bundle_id: str
    activation_at: datetime
    score: float
    targets: tuple[ContinuityTarget, ...]
    origin: ContinuityOrigin | None = None

    def __post_init__(self) -> None:
        if not self.base_run_id or not self.bundle_id or not self.targets:
            raise ValueError("continuity bundle metadata is incomplete")
        if self.activation_at.tzinfo is None:
            raise ValueError("continuity activation_at must be timezone-aware")
        if not math.isfinite(self.score):
            raise ValueError("continuity score must be finite")
        if len({target.target_id for target in self.targets}) != len(self.targets):
            raise ValueError("continuity bundle repeats a target")
        if any(target.activation_at != self.activation_at for target in self.targets):
            raise ValueError("continuity target activation disagrees with its bundle")

    @property
    def all_terminal(self) -> bool:
        return all(target.probe.state is TerminalState.TERMINAL for target in self.targets)

    def with_probes(self, probes: Mapping[str, TerminalProbe]) -> "ContinuityBundle":
        return replace(
            self,
            targets=tuple(
                replace(target, probe=probes.get(target.target_id, target.probe))
                for target in self.targets
            ),
        )

    def as_record(self) -> dict[str, object]:
        return {
            "base_run_id": self.base_run_id,
            "bundle_id": self.bundle_id,
            "activation_at": isoformat(self.activation_at),
            "score": self.score,
            "targets": [target.as_record() for target in self.targets],
            **(self.origin.as_record() if self.origin is not None else {}),
        }


def _required_text(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value:
        raise ContinuityError(f"published target continuity metadata has no {field}")
    return value


def _target_metadata(target: Target, venue: str) -> tuple[str, dict[str, Any]]:
    resolution = target.resolution
    if (
        not isinstance(resolution, dict)
        or resolution.get("source") != "targeter_v2"
        or resolution.get("version") != 3
    ):
        raise ContinuityError(
            f"published {venue} target {target.asset_id!r} has no v3 continuity metadata"
        )
    return _required_text(resolution, "target_id"), resolution


def _origin_from_resolution(resolution: Mapping[str, Any]) -> ContinuityOrigin:
    return ContinuityOrigin(
        run_id=_required_text(resolution, "continuity_origin_run_id"),
        report_sha256=_required_sha256(
            resolution.get("continuity_origin_report_sha256"),
            "continuity_origin_report_sha256",
        ),
        archive_manifest_key=_required_text(
            resolution, "continuity_origin_archive_manifest_key"
        ),
        archive_manifest_sha256=_required_sha256(
            resolution.get("continuity_origin_archive_manifest_sha256"),
            "continuity_origin_archive_manifest_sha256",
        ),
    )


def load_continuity_bundles(pointer_path: Path) -> tuple[ContinuityBundle, ...]:
    """Reconstruct bundle ownership from one validated committed generation."""
    grouped_targets: dict[tuple[str, str], dict[str, Any]] = {}
    bundle_metadata: dict[str, tuple[str, datetime, float, ContinuityOrigin]] = {}
    for venue in SUPPORTED_VENUES:
        target_set = load_targets(pointer_path, venue=venue)
        for target in target_set.targets:
            target_id, resolution = _target_metadata(target, venue)
            bundle_id = _required_text(resolution, "bundle_id")
            run_id = _required_text(resolution, "run_id")
            origin = _origin_from_resolution(resolution)
            activation = parse_timestamp(resolution.get("activation_at"))
            capture_start = parse_timestamp(resolution.get("capture_start_at"))
            if activation is None or capture_start is None:
                raise ContinuityError(f"published target {target_id!r} has invalid continuity timing")
            raw_score = resolution.get("continuity_score", 0.0)
            if isinstance(raw_score, bool) or not isinstance(raw_score, (int, float)):
                raise ContinuityError(f"published target {target_id!r} has invalid continuity score")
            score = float(raw_score)
            metadata = (run_id, activation, score, origin)
            previous_bundle = bundle_metadata.setdefault(bundle_id, metadata)
            if previous_bundle != metadata:
                raise ContinuityError(
                    f"published bundle {bundle_id!r} has inconsistent continuity metadata"
                )

            key = (bundle_id, target_id)
            item = grouped_targets.setdefault(
                key,
                {
                    "venue": venue,
                    "venue_market_id": target_id.split(":", 1)[1],
                    "canonical_class": _required_text(resolution, "canonical_class"),
                    "activation_at": activation,
                    "capture_start_at": capture_start,
                    "source_ref": _required_text(resolution, "source_ref"),
                    "subscription_ids": [],
                },
            )
            if target.asset_id not in item["subscription_ids"]:
                item["subscription_ids"].append(target.asset_id)

    bundles: list[ContinuityBundle] = []
    for bundle_id, (run_id, activation, score, origin) in sorted(bundle_metadata.items()):
        targets = tuple(
            ContinuityTarget(
                target_id=target_id,
                venue=item["venue"],
                venue_market_id=item["venue_market_id"],
                canonical_class=item["canonical_class"],
                subscription_ids=tuple(sorted(item["subscription_ids"])),
                activation_at=item["activation_at"],
                capture_start_at=item["capture_start_at"],
                source_ref=item["source_ref"],
            )
            for (candidate_bundle, target_id), item in sorted(grouped_targets.items())
            if candidate_bundle == bundle_id
        )
        bundles.append(
            ContinuityBundle(
                run_id,
                bundle_id,
                activation,
                score,
                targets,
                origin,
            )
        )
    return tuple(bundles)


def target_ids_by_venue(
    bundles: Iterable[ContinuityBundle],
) -> dict[str, tuple[ContinuityTarget, ...]]:
    return {
        venue: tuple(
            sorted(
                (
                    target
                    for bundle in bundles
                    for target in bundle.targets
                    if target.venue == venue
                ),
                key=lambda target: target.target_id,
            )
        )
        for venue in SUPPORTED_VENUES
    }
