"""Venue-independent data models and their canonical serialization helpers."""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence


SUPPORTED_VENUES = ("kalshi", "polymarket", "limitless")

# Series lengths whose outcome space the mask engine can enumerate exhaustively.
SUPPORTED_BEST_OF = frozenset({1, 3, 5, 7, 9})


def format_observation(values: Iterable[int]) -> dict[str, Any]:
    """Describe an observed set of best-of lengths for a report record.

    Every report that states a series format derives it here, so a match
    rejection and a selection candidate can never disagree about whether the
    same observation is supported.
    """
    observed = sorted(set(values))
    best_of = observed[0] if len(observed) == 1 else None
    if not observed:
        status, outcome = "unknown", "not_built_unknown_format"
    elif len(observed) > 1:
        status, outcome = "conflicting", "not_built_format_conflict"
    elif best_of in SUPPORTED_BEST_OF:
        status, outcome = "supported", "exhaustive_normal_path"
    else:
        status, outcome = "unsupported", "not_built_unsupported_format"
    return {
        "format_observed": observed,
        "best_of": best_of,
        "format_status": status,
        "outcome_space_status": outcome,
    }


def parse_timestamp(value: Any) -> datetime | None:
    """Parse ISO-8601 or Unix seconds/milliseconds into an aware UTC instant."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        seconds = float(value) / 1000.0 if value > 10_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


_PARTICIPANT_NOISE = frozenset({"afc", "cf", "fc", "fk", "sc"})


def canonical_text(value: str) -> str:
    """Return a conservative Unicode-aware comparison key.

    Latin accents are folded so common vendor spellings such as ``Kōfu`` and
    ``Kofu`` remain comparable.  Letters, numbers, and combining marks from
    every other script are retained; dropping them can collapse two unrelated
    teams to the same ASCII prefix.
    """
    decomposed = unicodedata.normalize("NFKD", str(value).casefold())
    tokens: list[str] = []
    current: list[str] = []
    previous_base_is_latin = False

    def finish_token() -> None:
        if current:
            tokens.append(unicodedata.normalize("NFC", "".join(current)))
            current.clear()

    for character in decomposed:
        category = unicodedata.category(character)
        if category[0] in {"L", "N"}:
            current.append(character)
            previous_base_is_latin = "LATIN" in unicodedata.name(character, "")
        elif category[0] == "M" and current:
            if not previous_base_is_latin:
                current.append(character)
        else:
            finish_token()
            previous_base_is_latin = False
    finish_token()
    return " ".join(tokens)


def canonical_participant(value: str) -> str:
    tokens = [token for token in canonical_text(value).split() if token not in _PARTICIPANT_NOISE]
    return " ".join(tokens)


def stable_id(prefix: str, value: Mapping[str, Any] | Sequence[Any] | str) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return f"{prefix}_{hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:24]}"


def finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None



@dataclass(frozen=True)
class ClassificationEvidence:
    mapping_id: str
    source_field: str
    observed_value: str

    def __post_init__(self) -> None:
        if not self.mapping_id or not self.source_field or not self.observed_value:
            raise ValueError("classification evidence fields must be non-empty")


@dataclass(frozen=True)
class ActivationEvidence:
    instant: datetime
    source_kind: str
    source_field: str
    primary: bool
    parser_id: str | None = None

    def __post_init__(self) -> None:
        if self.instant.tzinfo is None:
            raise ValueError("activation evidence instant must be timezone-aware")
        if self.source_kind not in {"structured", "rule_template"}:
            raise ValueError("activation evidence source_kind is unsupported")
        if not self.source_field:
            raise ValueError("activation evidence source_field is required")
        if self.source_kind == "rule_template" and not self.parser_id:
            raise ValueError("rule-template activation evidence requires parser_id")


@dataclass(frozen=True)
class TargeterDiagnostic:
    code: str
    venue: str
    source_ref: str
    severity: str
    completeness_effect: bool
    details: Mapping[str, object]

    def __post_init__(self) -> None:
        if not self.code or self.venue not in SUPPORTED_VENUES or not self.source_ref:
            raise ValueError("diagnostic identity is invalid")
        if self.severity not in {"warning", "error"}:
            raise ValueError("diagnostic severity is unsupported")
        if any(not isinstance(key, str) for key in self.details):
            raise ValueError("diagnostic detail keys must be strings")
        try:
            json.dumps(self.details, allow_nan=False, sort_keys=True)
        except (TypeError, ValueError) as error:
            raise ValueError("diagnostic details must be finite JSON") from error


@dataclass(frozen=True)
class CanonicalEvent:
    venue: str
    venue_event_id: str
    sport: str
    league: str | None
    title: str
    participants: tuple[str, str]
    activation_at: datetime
    status: str
    source_ref: str
    format: str | None = None
    fragment_type: str | None = None
    game: str | None = None
    topology: str | None = None
    game_evidence: tuple[ClassificationEvidence, ...] = ()
    activation_evidence: tuple[ActivationEvidence, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.venue not in SUPPORTED_VENUES:
            raise ValueError(f"unsupported venue: {self.venue}")
        if not self.venue_event_id:
            raise ValueError("venue_event_id is required")
        if len(self.participants) != 2 or any(not item.strip() for item in self.participants):
            raise ValueError("sports events need exactly two non-empty participants")
        participant_keys = [canonical_participant(item) for item in self.participants]
        if any(not item for item in participant_keys) or len(set(participant_keys)) != 2:
            raise ValueError("participants must remain distinct after canonicalization")
        if self.activation_at.tzinfo is None:
            raise ValueError("activation_at must be timezone-aware")
        if self.game is not None:
            if not self.topology or not self.game_evidence or not self.activation_evidence:
                raise ValueError(
                    "structured esports events require topology, game evidence, and activation evidence"
                )
            if self.activation_at not in {
                item.instant for item in self.activation_evidence
            }:
                raise ValueError("activation_at must be present in activation evidence")

    @property
    def participant_keys(self) -> tuple[str, str]:
        return tuple(sorted(canonical_participant(item) for item in self.participants))  # type: ignore[return-value]

    def as_record(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "venue_event_id": self.venue_event_id,
            "sport": self.sport,
            "league": self.league,
            "title": self.title,
            "participants": list(self.participants),
            "participant_keys": list(self.participant_keys),
            "activation_at": isoformat(self.activation_at),
            "status": self.status,
            "source_ref": self.source_ref,
            "format": self.format,
            "fragment_type": self.fragment_type,
            "game": self.game,
            "topology": self.topology,
            "game_evidence": [
                {
                    "mapping_id": e.mapping_id,
                    "source_field": e.source_field,
                    "observed_value": e.observed_value
                }
                for e in sorted(
                    self.game_evidence,
                    key=lambda item: (
                        item.mapping_id, item.source_field, item.observed_value
                    ),
                )
            ],
            "activation_evidence": [
                {
                    "instant": isoformat(e.instant),
                    "source_kind": e.source_kind,
                    "source_field": e.source_field,
                    "primary": e.primary,
                    "parser_id": e.parser_id
                }
                for e in sorted(
                    self.activation_evidence,
                    key=lambda item: (
                        item.instant,
                        item.source_kind,
                        item.source_field,
                        item.parser_id or "",
                    ),
                )
            ],
        }


@dataclass(frozen=True)
class CanonicalMarket:
    venue: str
    venue_market_id: str
    venue_event_id: str
    canonical_class: str
    market_type: str
    scope: str
    title: str
    parameters: Mapping[str, Any]
    subscription_ids: tuple[str, ...]
    outcome_labels: tuple[str, ...]
    status: str
    accepting_orders: bool
    rules_text: str | None = None
    rules_hash: str | None = None
    created_at: datetime | None = None
    volume_24h: float | None = None
    volume_total: float | None = None
    volume_total_usd: float | None = None
    liquidity: float | None = None
    source_ref: str = ""
    classification_evidence: ClassificationEvidence | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    def __post_init__(self) -> None:
        if self.venue not in SUPPORTED_VENUES:
            raise ValueError(f"unsupported venue: {self.venue}")
        if not self.venue_market_id or not self.venue_event_id:
            raise ValueError("market and event identifiers are required")
        if not self.canonical_class or not self.market_type or not self.scope:
            raise ValueError("canonical class, market type, and scope are required")
        if not self.subscription_ids:
            raise ValueError("at least one subscription id is required")
        if any(not item.strip() for item in self.subscription_ids) or len(set(self.subscription_ids)) != len(self.subscription_ids):
            raise ValueError("subscription ids must be non-empty and unique")
        if self.created_at is not None and self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")
        for name, value in (
            ("volume_24h", self.volume_24h),
            ("volume_total", self.volume_total),
            ("volume_total_usd", self.volume_total_usd),
            ("liquidity", self.liquidity),
        ):
            if value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(f"{name} must be finite")
        if self.volume_total_usd is not None and self.volume_total_usd < 0:
            raise ValueError("volume_total_usd must be non-negative")

    @property
    def target_id(self) -> str:
        return f"{self.venue}:{self.venue_market_id}"

    def as_record(self) -> dict[str, Any]:
        return {
            "target_id": self.target_id,
            "venue": self.venue,
            "venue_market_id": self.venue_market_id,
            "venue_event_id": self.venue_event_id,
            "canonical_class": self.canonical_class,
            "market_type": self.market_type,
            "scope": self.scope,
            "title": self.title,
            "parameters": dict(self.parameters),
            "subscription_ids": list(self.subscription_ids),
            "outcome_labels": list(self.outcome_labels),
            "status": self.status,
            "accepting_orders": self.accepting_orders,
            "rules_text": self.rules_text,
            "rules_hash": self.rules_hash,
            "created_at": isoformat(self.created_at),
            "volume_24h": self.volume_24h,
            "volume_total": self.volume_total,
            "volume_total_usd": self.volume_total_usd,
            "liquidity": self.liquidity,
            "source_ref": self.source_ref,
            "classification_evidence": {
                "mapping_id": self.classification_evidence.mapping_id,
                "source_field": self.classification_evidence.source_field,
                "observed_value": self.classification_evidence.observed_value
            } if self.classification_evidence else None,
        }


@dataclass(frozen=True)
class CatalogSnapshot:
    venue: str
    events: tuple[CanonicalEvent, ...]
    markets: tuple[CanonicalMarket, ...]
    complete: bool = True
    diagnostics: tuple[str, ...] = ()
    classification_diagnostics: tuple[TargeterDiagnostic, ...] = ()
    requests: int = 0

    def __post_init__(self) -> None:
        if self.venue not in SUPPORTED_VENUES:
            raise ValueError(f"unsupported venue: {self.venue}")
        if any(event.venue != self.venue for event in self.events) or any(
            market.venue != self.venue for market in self.markets
        ):
            raise ValueError("catalog records must belong to the catalog venue")
        event_ids = {event.venue_event_id for event in self.events}
        if len(event_ids) != len(self.events):
            raise ValueError("catalog event identifiers must be unique")
        market_ids = {market.venue_market_id for market in self.markets}
        if len(market_ids) != len(self.markets):
            raise ValueError("catalog market identifiers must be unique")
        dangling = [market.target_id for market in self.markets if market.venue_event_id not in event_ids]
        if dangling:
            raise ValueError(f"markets reference missing events: {', '.join(dangling[:3])}")
        if any(item.completeness_effect for item in self.classification_diagnostics):
            object.__setattr__(self, "complete", False)

    def as_summary(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "complete": self.complete,
            "events": len(self.events),
            "markets": len(self.markets),
            "diagnostics": list(self.diagnostics),
            "classification_diagnostics": [
                {
                    "code": d.code,
                    "venue": d.venue,
                    "source_ref": d.source_ref,
                    "severity": d.severity,
                    "completeness_effect": d.completeness_effect,
                    "details": d.details
                }
                for d in sorted(
                    self.classification_diagnostics,
                    key=lambda item: (
                        item.code,
                        item.source_ref,
                        json.dumps(item.details, sort_keys=True, separators=(",", ":")),
                    ),
                )
            ],
            "requests": self.requests,
        }


@dataclass(frozen=True)
class EventBundle:
    bundle_id: str
    sport: str
    participants: tuple[str, str]
    participant_keys: tuple[str, str]
    activation_at: datetime
    events: tuple[CanonicalEvent, ...]
    markets: tuple[CanonicalMarket, ...]
    confidence: str
    warnings: tuple[str, ...] = ()
    game: str | None = None
    topology: str | None = None
    activation_support: tuple[Mapping[str, object], ...] = ()
    activation_conflicts: tuple[Mapping[str, object], ...] = ()
    participant_key_map: Mapping[str, str] = field(default_factory=dict, compare=False, repr=False)

    def participant_key(self, value: str) -> str:
        key = canonical_participant(value)
        return self.participant_key_map.get(key, key)

    @property
    def venues(self) -> tuple[str, ...]:
        return tuple(sorted({event.venue for event in self.events}))

    @property
    def observed_formats(self) -> tuple[int, ...]:
        """Every distinct numeric series length stated by a bundled event."""
        return tuple(
            sorted(
                {
                    int(event.format)
                    for event in self.events
                    if event.format and str(event.format).isdigit()
                }
            )
        )

    @property
    def best_of(self) -> int | None:
        """The unambiguous series length, or None when absent or conflicting."""
        formats = self.observed_formats
        return formats[0] if len(formats) == 1 else None

    def as_record(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "sport": self.sport,
            "participants": list(self.participants),
            "participant_keys": list(self.participant_keys),
            "activation_at": isoformat(self.activation_at),
            "venues": list(self.venues),
            "game": self.game,
            "topology": self.topology,
            "activation_support": list(self.activation_support),
            "activation_conflicts": list(self.activation_conflicts),
            "confidence": self.confidence,
            "warnings": list(self.warnings),
            "participant_aliases_applied": dict(sorted(self.participant_key_map.items())),
            "event_refs": [f"{event.venue}:{event.venue_event_id}" for event in self.events],
            "market_ids": [market.target_id for market in self.markets],
        }


@dataclass(frozen=True)
class Relationship:
    bundle_id: str
    left: str
    right: str
    relationship: str
    scope: str
    left_venue: str
    right_venue: str
    coverage: str

    @property
    def cross_venue(self) -> bool:
        return self.left_venue != self.right_venue

    def as_record(self) -> dict[str, Any]:
        return {
            "bundle_id": self.bundle_id,
            "left": self.left,
            "right": self.right,
            "relationship": self.relationship,
            "scope": self.scope,
            "left_venue": self.left_venue,
            "right_venue": self.right_venue,
            "cross_venue": self.cross_venue,
            "coverage": self.coverage,
        }
