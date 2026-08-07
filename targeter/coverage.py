"""Discovery latency: how long a market existed before we started watching it.

docs/CAPTURE_SPEC.md §6.1 asks for coverage-from-inception to be *a measurable number rather
than an assumption*, and this is where the measurement lives. For short-dated
markets it is the difference between a usable dataset and a misleading one: a
five-minute market found a minute late has lost a fifth of its life, and the
missing fifth is its opening price discovery. No later work recovers it, and
nothing in the captured data reveals the loss — the frames we do have look
perfectly healthy.

Two timestamps per market:

    first_seen_at   when a discovery cycle first returned it. Ours, always known.
    created_at      when the venue says it was created. Only where a venue says.

Their difference bounds our discovery lag from above; where `created_at` is
missing, `first_seen_at` still bounds how much of the tape can be trusted as
complete, because nothing before it was ever subscribed.

The ledger is append-only in effect: a market's first sighting is never
overwritten. Overwriting it would quietly improve every latency number on every
rerun, which is exactly the kind of self-flattering metric that survives review by
being unfalsifiable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from analysis.storage import parse_iso8601, utc_now, write_json

#: Venue fields that carry a creation time, in the order they are trusted.
CREATED_AT_FIELDS = ("createdAt", "created_at", "open_time", "openTime")


@dataclass(frozen=True)
class Sighting:
    asset_id: str
    venue: str
    first_seen_at: str
    created_at: str | None = None

    def discovery_lag_seconds(self) -> float | None:
        """Seconds between the venue creating the market and us first seeing it."""
        created = parse_iso8601(self.created_at)
        seen = parse_iso8601(self.first_seen_at)
        if created is None or seen is None:
            return None
        return max(0.0, (seen - created).total_seconds())

    def as_record(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "venue": self.venue,
            "first_seen_at": self.first_seen_at,
            "created_at": self.created_at,
            "discovery_lag_seconds": self.discovery_lag_seconds(),
        }


class CoverageLedger:
    """First sightings per venue, durable across restarts."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._sightings: dict[tuple[str, str], Sighting] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            document = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            # A corrupt ledger must not stop capture. It is a measurement, not
            # evidence — the tape is the evidence — so it rebuilds from now and
            # the gap shows up as markets whose first sighting looks late.
            return
        for record in document.get("sightings") or []:
            sighting = Sighting(
                asset_id=str(record.get("asset_id") or ""),
                venue=str(record.get("venue") or ""),
                first_seen_at=str(record.get("first_seen_at") or ""),
                created_at=record.get("created_at"),
            )
            if sighting.asset_id and sighting.venue:
                self._sightings[(sighting.venue, sighting.asset_id)] = sighting

    def observe(
        self,
        venue: str,
        asset_ids: Iterable[str],
        *,
        created_at: dict[str, str] | None = None,
        now: str | None = None,
    ) -> list[Sighting]:
        """Records anything not seen before. Returns only the new sightings.

        `now` stamps the first sighting. It defaults to the wall clock, which is
        what a live discovery cycle wants. A caller supplies it when the sighting
        time is not the current time: a deterministic `--now` probe run, or a
        reconstruction from evidence recorded earlier.

        A time *later* than the truth is the damaging direction. It overstates
        the discovery lag, and — because `first_seen_at` also bounds how far back
        the tape can be trusted as subscribed — it makes frames that were in fact
        being captured look like they predate coverage. That discards real
        evidence, which is worse than the flattering error of understating lag.
        """
        now = now or utc_now()
        created_at = created_at or {}
        fresh: list[Sighting] = []
        for asset_id in asset_ids:
            key = (venue, asset_id)
            if key in self._sightings:
                continue
            sighting = Sighting(
                asset_id=asset_id,
                venue=venue,
                first_seen_at=now,
                created_at=created_at.get(asset_id),
            )
            self._sightings[key] = sighting
            fresh.append(sighting)
        return fresh

    def lower_first_sighting(
        self,
        venue: str,
        asset_id: str,
        first_seen_at: str,
        *,
        created_at: str | None = None,
    ) -> bool:
        """Move a sighting earlier. Returns whether anything changed.

        The one revision this ledger permits, and it is deliberately a separate
        method rather than a mode of `observe`. `observe` is append-only because
        a discovery loop re-seeing a known market must not restamp it; that rule
        is what keeps the numbers falsifiable.

        Moving a sighting *earlier* is the opposite operation and is a repair:
        it can only be justified by evidence that the asset was already
        subscribed before the ledger knew about it, and it makes the measured
        discovery lag smaller and the trusted span of tape longer — both toward
        the truth, and both away from the direction that silently discards
        captured frames. A later timestamp is never accepted here.
        """
        key = (venue, asset_id)
        existing = self._sightings.get(key)
        if existing is not None:
            current = parse_iso8601(existing.first_seen_at)
            proposed = parse_iso8601(first_seen_at)
            if current is None or proposed is None or proposed >= current:
                return False
        self._sightings[key] = Sighting(
            asset_id=asset_id,
            venue=venue,
            first_seen_at=first_seen_at,
            created_at=created_at if created_at is not None else (
                existing.created_at if existing is not None else None
            ),
        )
        return existing is not None

    def save(self) -> None:
        write_json(
            self.path,
            {
                "version": 1,
                "updated_at": utc_now(),
                "sightings": [s.as_record() for s in self._sightings.values()],
            },
        )

    def summary(self, venue: str | None = None) -> dict[str, Any]:
        """Latency distribution, reported only over markets that can support one.

        Markets whose venue publishes no creation time are counted separately
        rather than dropped. Averaging over only the measurable ones and calling
        it coverage would understate the lag exactly where it is least known.
        """
        selected = [
            s for s in self._sightings.values() if venue is None or s.venue == venue
        ]
        lags = sorted(
            lag for lag in (s.discovery_lag_seconds() for s in selected) if lag is not None
        )
        report: dict[str, Any] = {
            "markets": len(selected),
            "with_created_at": len(lags),
            "without_created_at": len(selected) - len(lags),
        }
        if lags:
            report["lag_seconds_median"] = lags[len(lags) // 2]
            report["lag_seconds_max"] = lags[-1]
            report["lag_seconds_p90"] = lags[min(len(lags) - 1, int(len(lags) * 0.9))]
        return report

    def __len__(self) -> int:
        return len(self._sightings)


def created_at_of(record: dict[str, Any]) -> str | None:
    """Reads a creation timestamp from a venue record, normalised to ISO-8601.

    Venues spell this differently and some not at all. Returning None rather than
    guessing keeps an unmeasurable market visibly unmeasurable instead of giving
    it a fabricated lag of zero.
    """
    for field_name in CREATED_AT_FIELDS:
        value = record.get(field_name)
        if isinstance(value, str) and value.strip():
            parsed = parse_iso8601(value)
            if parsed is not None:
                return parsed.isoformat()
        elif isinstance(value, (int, float)) and value > 0:
            # Unix seconds or milliseconds, distinguished by magnitude: anything
            # past ~2286 in seconds is far more likely to be milliseconds.
            seconds = float(value) / 1000.0 if value > 10_000_000_000 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat()
    return None
