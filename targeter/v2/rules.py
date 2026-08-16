"""Happy-path rule templates and non-blocking drift evidence.

Market-class configuration is the day-one semantic authority.  This module
does not use prose similarity to invent equivalence at runtime.  It only makes
event-variable-independent, content-addressed fingerprints and detects a small
set of explicit contradictions to a configured normal-settlement scope.
"""

from __future__ import annotations

import re
import unicodedata
import html
from dataclasses import dataclass
from typing import Mapping

from targeter.v2.models import CanonicalEvent, CanonicalMarket, EventBundle, stable_id


NORMALIZER_VERSION = 1
_SPACE = re.compile(r"\s+")
_DATE = re.compile(
    r"\b(?:20\d{2}[-/]\d{1,2}[-/]\d{1,2}|\d{1,2}[-/]\d{1,2}[-/]20\d{2}|"
    r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|"
    r"jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|"
    r"dec(?:ember)?)\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s+20\d{2})?)\b",
    re.IGNORECASE,
)
_TIME = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:am|pm|utc|et|pt)?\b", re.IGNORECASE)


@dataclass(frozen=True)
class RuleTemplate:
    template_id: str
    venue: str
    sport: str
    canonical_class: str
    normalized_text: str
    normalizer_version: int
    review_status: str
    market_id: str

    def as_record(self) -> dict[str, object]:
        return {
            "template_id": self.template_id,
            "venue": self.venue,
            "sport": self.sport,
            "canonical_class": self.canonical_class,
            "normalized_text": self.normalized_text,
            "normalizer_version": self.normalizer_version,
            "review_status": self.review_status,
            "market_id": self.market_id,
        }


@dataclass(frozen=True)
class RuleAssessment:
    templates: tuple[RuleTemplate, ...]
    drift: tuple[dict[str, object], ...]
    contradictions: Mapping[str, tuple[str, ...]]

    def as_record(self) -> dict[str, object]:
        return {
            "templates": [template.as_record() for template in self.templates],
            "drift": list(self.drift),
            "contradictions": {
                key: list(value) for key, value in sorted(self.contradictions.items())
            },
        }


def _replace_literal(text: str, literal: str, replacement: str) -> str:
    if not literal.strip():
        return text
    return re.sub(
        rf"(?<!\w){re.escape(literal.strip())}(?!\w)",
        replacement,
        text,
        flags=re.IGNORECASE,
    )


def normalize_rule_text(
    text: str,
    *,
    event: CanonicalEvent,
    market: CanonicalMarket,
) -> str:
    """Replace event variables while preserving the rule's actual language."""
    normalized = html.unescape(unicodedata.normalize("NFKC", str(text or "")))
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    normalized = _replace_literal(normalized, event.participants[0], "<home>")
    normalized = _replace_literal(normalized, event.participants[1], "<away>")
    normalized = _replace_literal(normalized, event.title, "<event>")
    normalized = _DATE.sub("<event_date>", normalized)
    normalized = _TIME.sub("<event_time>", normalized)
    for field, value in sorted(market.parameters.items()):
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            rendered = str(int(value)) if float(value).is_integer() else str(value)
            normalized = re.sub(
                rf"(?<![\d.]){re.escape(rendered)}(?![\d.])",
                f"<{field}>",
                normalized,
            )
    normalized = _SPACE.sub(" ", normalized).strip().casefold()
    return normalized


def template_for(
    market: CanonicalMarket,
    event: CanonicalEvent,
    *,
    known_template_ids: frozenset[str],
) -> RuleTemplate | None:
    if not market.rules_text:
        return None
    normalized = normalize_rule_text(market.rules_text, event=event, market=market)
    identifier = stable_id(
        "rule",
        {
            "normalizer_version": NORMALIZER_VERSION,
            "venue": market.venue,
            "sport": event.sport,
            "canonical_class": market.canonical_class,
            "normalized_text": normalized,
        },
    )
    return RuleTemplate(
        template_id=identifier,
        venue=market.venue,
        sport=event.sport,
        canonical_class=market.canonical_class,
        normalized_text=normalized,
        normalizer_version=NORMALIZER_VERSION,
        review_status="KNOWN" if identifier in known_template_ids else "UNREVIEWED",
        market_id=market.target_id,
    )


def normal_path_contradictions(market: CanonicalMarket) -> tuple[str, ...]:
    """Return only explicit contradictions that are safe to automate.

    Exceptional clauses such as cancellation, postponement, or a rare venue
    correction intentionally do not appear here; they remain drift evidence.
    """
    text = " ".join(str(market.rules_text or "").casefold().split())
    if not text:
        return ()
    conflicts: list[str] = []
    regulation_scope = market.scope in {"regulation_fulltime", "first_half"}
    def has_unnegated_inclusion(
        inclusions: tuple[str, ...], exclusions: tuple[str, ...]
    ) -> bool:
        scrubbed = text
        for phrase in sorted(exclusions, key=len, reverse=True):
            scrubbed = scrubbed.replace(phrase, " ")
        return any(phrase in scrubbed for phrase in inclusions)

    extra_time_inclusion = has_unnegated_inclusion(
        (
            "including extra time",
            "includes extra time",
            "extra time will count",
            "extra time counts",
            "after extra time",
        ),
        (
            "excluding extra time",
            "does not include extra time",
            "not including extra time",
            "extra time will not count",
            "extra time does not count",
        ),
    )
    penalty_inclusion = has_unnegated_inclusion(
        (
            "penalty shootout will count",
            "penalty shootouts will count",
            "penalty shootout counts",
            "including penalties",
            "includes penalties",
            "penalties will count",
        ),
        (
            "excluding penalties",
            "excluding penalty shootouts",
            "does not include penalties",
            "does not include penalty shootouts",
            "not including penalties",
            "not including penalty shootouts",
            "penalties will not count",
            "penalty shootouts will not count",
        ),
    )
    if regulation_scope and (extra_time_inclusion or penalty_inclusion):
        conflicts.append("rules_include_extra_time_but_class_is_regulation")
    if market.scope == "regulation_fulltime" and any(
        phrase in text
        for phrase in (
            "first half",
            "first 45 minutes",
            "after 45 minutes",
            "at half time",
            "at halftime",
            "halftime result",
        )
    ):
        conflicts.append("rules_are_first_half_but_class_is_fulltime")
    if market.scope == "series" and "single map only" in text and market.market_type == "series_moneyline":
        conflicts.append("rules_are_single_map_but_class_is_series")
    return tuple(conflicts)


def assess_rules(
    bundle: EventBundle,
    *,
    known_template_ids: frozenset[str] = frozenset(),
) -> RuleAssessment:
    events = {(event.venue, event.venue_event_id): event for event in bundle.events}
    templates: list[RuleTemplate] = []
    contradictions: dict[str, tuple[str, ...]] = {}
    for market in bundle.markets:
        event = events[(market.venue, market.venue_event_id)]
        template = template_for(market, event, known_template_ids=known_template_ids)
        if template is not None:
            templates.append(template)
        if conflicts := normal_path_contradictions(market):
            contradictions[market.target_id] = conflicts

    by_product: dict[tuple[str, str], set[str]] = {}
    for template in templates:
        by_product.setdefault((template.venue, template.canonical_class), set()).add(
            template.template_id
        )
    drift = []
    for (venue, canonical_class), identifiers in sorted(by_product.items()):
        if len(identifiers) > 1:
            drift.append(
                {
                    "venue": venue,
                    "canonical_class": canonical_class,
                    "template_ids": sorted(identifiers),
                    "blocking": False,
                    "reason": "multiple_rule_templates_observed",
                }
            )
    return RuleAssessment(
        templates=tuple(sorted(templates, key=lambda item: (item.venue, item.canonical_class, item.market_id))),
        drift=tuple(drift),
        contradictions=contradictions,
    )
