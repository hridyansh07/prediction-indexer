"""Shared adapter protocols and low-level normalization helpers."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Protocol

from targeter.v2.models import (
    ActivationEvidence,
    ClassificationEvidence,
    canonical_participant,
    parse_timestamp,
)
from targeter.v2.parsing import text
from targeter.v2.registry import GameFamily


def _game_evidence(
    family: GameFamily,
    venue: str,
    source_field: str,
    observed_value: object,
) -> ClassificationEvidence:
    return ClassificationEvidence(
        f"{family.id}:{venue}:game:{source_field}",
        source_field,
        str(observed_value),
    )


def _classify_market_by_family(
    venue: str,
    family: GameFamily,
    raw: Mapping[str, Any],
    parent_raw: Mapping[str, Any],
) -> tuple[str, ClassificationEvidence] | tuple[None, None]:
    mappings = family.venue_products.get(venue, ())
    matches: list[tuple[str, str, str]] = []
    for mapping in mappings:
        val = ""
        if venue == "kalshi" and mapping.field == "series_ticker":
            val = str(parent_raw.get("series_ticker") or "")
        elif venue == "polymarket" and mapping.field == "group_title":
            val = str(raw.get("groupItemTitle") or "")
        elif venue == "limitless" and mapping.field == "metadata_market_type":
            raw_metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            parent_metadata = (
                parent_raw.get("metadata")
                if isinstance(parent_raw.get("metadata"), dict)
                else parent_raw
            )
            val = str(
                raw.get("metadata_market_type")
                or raw_metadata.get("marketType")
                or raw_metadata.get("binaryMarketType")
                or parent_metadata.get("marketType")
                or parent_metadata.get("binaryMarketType")
                or ""
            )

        if not val:
            continue
        comparison = val.strip() if venue == "kalshi" else text.normalize_label(val)
        if mapping.values and comparison in mapping.values:
            matches.append((mapping.canonical_class, mapping.field, val))
        if mapping.patterns:
            for pat in mapping.patterns:
                if re.search(pat, text.normalize_label(val), re.IGNORECASE):
                    matches.append((mapping.canonical_class, mapping.field, val))
                    break
    unique = {(canonical_class, field, value) for canonical_class, field, value in matches}
    if len({item[0] for item in unique}) != 1:
        return None, None
    if not unique:
        return None, None
    canonical_class, field, value = sorted(unique)[0]
    return canonical_class, ClassificationEvidence(
        f"{family.id}:{venue}:{canonical_class}:{field}",
        field,
        value,
    )


class JsonClient(Protocol):
    def get_json(
        self,
        base_url: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any: ...


def _data(response: Any) -> Any:
    return response.data if hasattr(response, "data") else response


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return [str(item) for item in parsed] if isinstance(parsed, list) else []
    return []


def _rules_hash(value: str | None) -> str | None:
    return hashlib.sha256(value.encode("utf-8")).hexdigest() if value else None


def _earliest_timestamp(*values: Any) -> datetime | None:
    parsed = [item for value in values for item in (parse_timestamp(value),) if item is not None]
    return min(parsed) if parsed else None


def _structured_activation(
    fields: tuple[tuple[str, tuple[Any, ...]], ...],
) -> tuple[datetime | None, ActivationEvidence | None, tuple[dict[str, object], ...]]:
    conflicts: list[dict[str, object]] = []
    for field, values in fields:
        instants = sorted(
            {
                parsed
                for value in values
                for parsed in (parse_timestamp(value),)
                if parsed is not None
            }
        )
        if len(instants) == 1:
            instant = instants[0]
            return (
                instant,
                ActivationEvidence(
                    instant=instant,
                    source_kind="structured",
                    source_field=field,
                    primary=True,
                ),
                tuple(conflicts),
            )
        if len(instants) > 1:
            conflicts.append(
                {
                    "source_field": field,
                    "instants": [_iso(item) for item in instants],
                }
            )
    return None, None, tuple(conflicts)


def _activation_evidence_sorted(
    values: list[ActivationEvidence],
) -> tuple[ActivationEvidence, ...]:
    return tuple(
        sorted(
            values,
            key=lambda item: (
                item.instant,
                item.source_kind,
                item.source_field,
                item.parser_id or "",
            ),
        )
    )


def _participants_usable(participants: tuple[str, str] | None) -> bool:
    if participants is None:
        return False
    keys = tuple(canonical_participant(item) for item in participants)
    return bool(keys[0] and keys[1] and keys[0] != keys[1])
