"""Mandatory evidence wrapper for every replay analysis result."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class AnalysisEvidence:
    trust_intervals: tuple[dict[str, Any], ...]
    coverage_percentage: tuple[dict[str, Any], ...]
    polymarket_hash_match: dict[str, Any]
    leg_skew_strata: tuple[dict[str, Any], ...]

    def __post_init__(self) -> None:
        if not self.trust_intervals:
            raise ValueError("analysis evidence requires interval trust")
        if not self.coverage_percentage:
            raise ValueError("analysis evidence requires coverage percentages")
        required_hash_fields = {"status", "matched", "total", "rate_percentage"}
        if set(self.polymarket_hash_match) != required_hash_fields:
            raise ValueError(
                "Polymarket hash evidence must have exactly "
                f"{sorted(required_hash_fields)}"
            )
        if not self.leg_skew_strata:
            raise ValueError("analysis evidence requires explicit leg-skew strata")

    def as_record(self) -> dict[str, Any]:
        return {
            "trust_intervals": list(self.trust_intervals),
            "coverage_percentage": list(self.coverage_percentage),
            "polymarket_hash_match": self.polymarket_hash_match,
            "leg_skew_strata": list(self.leg_skew_strata),
        }


@dataclass(frozen=True)
class AnalysisOutput:
    analysis_kind: str
    payload: Mapping[str, Any]
    evidence: AnalysisEvidence

    def __post_init__(self) -> None:
        if not self.analysis_kind:
            raise ValueError("analysis_kind must not be empty")
        if not isinstance(self.payload, Mapping):
            raise TypeError("analysis payload must be a mapping, never a bare scalar")

    def as_record(self) -> dict[str, Any]:
        return {
            "analysis_kind": self.analysis_kind,
            "payload": dict(self.payload),
            "evidence": self.evidence.as_record(),
        }


def encode_analysis_output(value: object) -> bytes:
    """The only supported serialization boundary for analysis output."""
    if not isinstance(value, AnalysisOutput):
        raise TypeError("bare analysis output is forbidden; use AnalysisOutput")
    return (
        json.dumps(
            value.as_record(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
