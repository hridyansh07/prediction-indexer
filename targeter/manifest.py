"""The capture manifest: what to watch, expressed as configuration.

The rule this file exists to enforce is docs/CAPTURE_SPEC.md §3's — **adding a market must
be a config change, never a code change.** One declarative document drives
discovery on every venue, and a new instrument class is a new entry rather than a
new script.

A manifest is a list of *entries*. An entry names one structural idea — a Bitcoin
hourly ladder, a set of macro buckets — and carries a per-venue selector saying how
that idea is spelled at each venue. Market identification differs everywhere, so
the selector has to be per-venue; the entry is what makes the several spellings
one thing.

```json
{
  "version": 1,
  "entries": [
    {
      "id": "btc_hourly_ladder",
      "structure_type": "LADDER",
      "discover_every_seconds": 60,
      "venues": {
        "kalshi":     {"series": ["KXBTCD"], "max_targets": 400},
        "limitless":  {"horizons": ["hourly", "5min"], "underlyings": ["btc"]},
        "polymarket": {"tag": "crypto", "slug_contains": "bitcoin"}
      }
    }
  ]
}
```

`discover_every_seconds` matters more than it looks. Short-dated crypto markets are
created continuously, and a five-minute market found a minute late has lost a fifth
of its life — the fifth containing its opening price discovery, which is not
recoverable at any later time.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from targeter.sources import SOURCES

#: Ω generators the mask layer knows how to build. Recorded on each entry so the
#: analysis side can pick a generator without re-deriving structure from titles.
STRUCTURE_TYPES = ("LADDER", "PARTITION", "SERIES_BO_N", "BRACKET", "BINARY")

DEFAULT_DISCOVER_SECONDS = 60.0


class ManifestError(ValueError):
    pass


@dataclass(frozen=True)
class Entry:
    id: str
    structure_type: str
    venues: dict[str, dict[str, Any]]
    discover_every_seconds: float = DEFAULT_DISCOVER_SECONDS
    enabled: bool = True
    note: str | None = None


@dataclass(frozen=True)
class Manifest:
    entries: tuple[Entry, ...]
    source_path: str
    version: int = 1
    raw: dict[str, Any] = field(default_factory=dict)

    def active(self) -> tuple[Entry, ...]:
        return tuple(entry for entry in self.entries if entry.enabled)

    def venues(self) -> tuple[str, ...]:
        seen: list[str] = []
        for entry in self.active():
            for venue in entry.venues:
                if venue not in seen:
                    seen.append(venue)
        return tuple(seen)

    def entries_for(self, venue: str) -> tuple[Entry, ...]:
        return tuple(entry for entry in self.active() if venue in entry.venues)


def load_manifest(path: Path) -> Manifest:
    """Reads and validates a manifest, refusing anything a cycle could misread.

    Validation is strict about venue names and structure types because both are
    silent failures otherwise: an unknown venue would simply never be discovered,
    and the operator would see an empty targets file with no reason given.
    """
    path = Path(path)
    if not path.exists():
        raise ManifestError(f"manifest not found: {path}")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ManifestError(f"manifest is not valid JSON: {error}") from error
    if not isinstance(document, dict):
        raise ManifestError("manifest must be a JSON object")

    raw_entries = document.get("entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ManifestError("manifest needs a non-empty 'entries' array")

    entries: list[Entry] = []
    seen_ids: set[str] = set()
    for position, raw in enumerate(raw_entries):
        if not isinstance(raw, dict):
            raise ManifestError(f"entry {position} is not an object")

        identifier = raw.get("id")
        if not isinstance(identifier, str) or not identifier.strip():
            raise ManifestError(f"entry {position} has no usable 'id'")
        if identifier in seen_ids:
            raise ManifestError(f"duplicate entry id: {identifier}")
        seen_ids.add(identifier)

        structure = raw.get("structure_type", "BINARY")
        if structure not in STRUCTURE_TYPES:
            raise ManifestError(
                f"entry {identifier!r} has structure_type {structure!r}; "
                f"expected one of {', '.join(STRUCTURE_TYPES)}"
            )

        venues = raw.get("venues")
        if not isinstance(venues, dict) or not venues:
            raise ManifestError(f"entry {identifier!r} has no 'venues' object")
        for venue, selector in venues.items():
            if venue not in SOURCES:
                raise ManifestError(
                    f"entry {identifier!r} names venue {venue!r}, which has no discovery "
                    f"source; known venues: {', '.join(sorted(SOURCES))}"
                )
            if not isinstance(selector, dict):
                raise ManifestError(f"entry {identifier!r} selector for {venue!r} is not an object")

        cadence = raw.get("discover_every_seconds", DEFAULT_DISCOVER_SECONDS)
        try:
            cadence = float(cadence)
        except (TypeError, ValueError) as error:
            raise ManifestError(
                f"entry {identifier!r} has non-numeric discover_every_seconds"
            ) from error
        if cadence <= 0:
            raise ManifestError(f"entry {identifier!r} needs a positive discover_every_seconds")

        entries.append(
            Entry(
                id=identifier,
                structure_type=structure,
                venues={venue: dict(selector) for venue, selector in venues.items()},
                discover_every_seconds=cadence,
                enabled=bool(raw.get("enabled", True)),
                note=raw.get("note"),
            )
        )

    return Manifest(
        entries=tuple(entries),
        source_path=str(path),
        version=int(document.get("version", 1)),
        raw=document,
    )
