"""Reconcile captured venue resolutions without inventing oracle verification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

from replay.catalog import MetadataCatalogue
from replay.events import MarketLifecycle, ReplayEvent


@dataclass(frozen=True)
class ResolutionRecord:
    venue: str
    market_id: str
    winning_outcome: str | None
    winning_index: int | None
    resolution_time: str | int | None
    metadata_found: bool
    outcome_index_consistent: bool | None
    resolution_source: str | None
    resolution_identity_status: str | None
    resolution_identity_conflicts: tuple[str, ...]
    independent_oracle_status: str
    semantic_hash: str
    duplicate_deliveries: int

    def as_record(self) -> dict[str, object]:
        return {
            "venue": self.venue,
            "market_id": self.market_id,
            "winning_outcome": self.winning_outcome,
            "winning_index": self.winning_index,
            "resolution_time": self.resolution_time,
            "metadata_found": self.metadata_found,
            "outcome_index_consistent": self.outcome_index_consistent,
            "resolution_source": self.resolution_source,
            "resolution_identity_status": self.resolution_identity_status,
            "resolution_identity_conflicts": list(
                self.resolution_identity_conflicts
            ),
            "independent_oracle_status": self.independent_oracle_status,
            "semantic_hash": self.semantic_hash,
            "duplicate_deliveries": self.duplicate_deliveries,
        }


@dataclass(frozen=True)
class ResolutionAudit:
    records: tuple[ResolutionRecord, ...]
    raw_resolution_deliveries: int

    def as_record(self) -> dict[str, object]:
        return {
            "raw_resolution_deliveries": self.raw_resolution_deliveries,
            "unique_resolutions": len(self.records),
            "metadata_reconciled": sum(
                record.metadata_found for record in self.records
            ),
            "outcome_index_consistent": sum(
                record.outcome_index_consistent is True
                for record in self.records
            ),
            "independently_oracle_verified": sum(
                record.independent_oracle_status == "VERIFIED"
                for record in self.records
            ),
            "resolution_identity_conflicts": sum(
                record.resolution_identity_status == "CONFLICT"
                for record in self.records
            ),
            "records": [record.as_record() for record in self.records],
        }


def reconcile_resolutions(
    events: Iterable[ReplayEvent], catalogue: MetadataCatalogue
) -> ResolutionAudit:
    grouped: dict[str, list[tuple[MarketLifecycle, dict[str, Any]]]] = {}
    raw_count = 0
    for event in events:
        if not (
            isinstance(event, MarketLifecycle)
            and event.lifecycle == "RESOLVED"
            and event.market_id is not None
        ):
            continue
        raw_count += 1
        parsed = _resolution_fields(event)
        semantic = {
            "venue": event.venue,
            "market_id": event.market_id,
            **parsed,
        }
        digest = hashlib.sha256(
            json.dumps(
                semantic,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        grouped.setdefault(digest, []).append((event, parsed))

    records: list[ResolutionRecord] = []
    for digest, deliveries in sorted(grouped.items()):
        event, parsed = deliveries[0]
        metadata = catalogue.by_asset(event.venue, event.market_id or "")
        winning = parsed["winning_outcome"]
        index = parsed["winning_index"]
        consistent: bool | None = None
        if winning is not None and index is not None:
            expected = {"YES": 0, "UP": 0, "NO": 1, "DOWN": 1}.get(
                winning.upper()
            )
            consistent = expected == index if expected is not None else None
        records.append(
            ResolutionRecord(
                venue=event.venue,
                market_id=event.market_id or "",
                winning_outcome=winning,
                winning_index=index,
                resolution_time=parsed["resolution_time"],
                metadata_found=metadata is not None,
                outcome_index_consistent=consistent,
                resolution_source=(
                    metadata.resolution_source if metadata is not None else None
                ),
                resolution_identity_status=(
                    metadata.resolution_identity_status
                    if metadata is not None
                    else None
                ),
                resolution_identity_conflicts=(
                    metadata.resolution_identity_conflicts
                    if metadata is not None
                    else ()
                ),
                independent_oracle_status=(
                    "METADATA_RESOLUTION_IDENTITY_CONFLICT"
                    if metadata is not None
                    and metadata.resolution_identity_status == "CONFLICT"
                    else "NOT_CAPTURED_FOR_RESOLUTION_SOURCE"
                    if metadata is not None
                    else "MARKET_METADATA_MISSING"
                ),
                semantic_hash=digest,
                duplicate_deliveries=len(deliveries),
            )
        )
    return ResolutionAudit(tuple(records), raw_count)


def _resolution_fields(event: MarketLifecycle) -> dict[str, Any]:
    raw = event.raw
    data = raw.get("data") if isinstance(raw.get("data"), dict) else raw
    winning = (
        data.get("winningOutcome")
        or data.get("winning_outcome")
        or data.get("outcome")
    )
    raw_index = data.get("winningIndex", data.get("winning_index"))
    try:
        index = int(raw_index) if raw_index is not None else None
    except (TypeError, ValueError):
        index = None
    return {
        "winning_outcome": str(winning) if winning is not None else None,
        "winning_index": index,
        "resolution_time": data.get(
            "resolutionDate", data.get("resolution_time")
        ),
    }
