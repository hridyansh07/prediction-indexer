#!/usr/bin/env python3
"""The targeter: a long-lived discovery loop driven by the capture manifest.

```bash
python3 targeter/run.py                          # loop forever
python3 targeter/run.py --once                   # one cycle, then exit
python3 targeter/run.py --once --venue kalshi    # one venue
```

Separate from every splice, on purpose. A splice must keep recording while this is
being edited, rerun, or broken; a targeter that could take capture down with it
would make every later coverage claim conditional on this process's uptime. The
interface between them is a file the splice polls, so neither has to be up for the
other to be useful.

**Why a loop and not a script.** Short-dated crypto markets are created
continuously. A five-minute Limitless market discovered a minute late has lost a
fifth of its life, and the missing fifth contains its opening price discovery. A
one-shot targeter run by hand guarantees that loss on every market created after
the run — and nothing in the captured data reveals it, because the frames that did
arrive look perfectly healthy.

Each manifest entry carries its own cadence, so a five-minute ladder can be
rediscovered every 30 seconds while a daily one is checked hourly, without either
paying the other's request cost.

One venue's outage is its own. A failing source is recorded and skipped; the
others still write their targets.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from analysis.storage import utc_now, write_json
from targeter.coverage import CoverageLedger
from targeter.manifest import Manifest, ManifestError, load_manifest
from targeter.sources import SOURCES, Discovery, DiscoveryError
from targeter.targets import Target, load_targets, write_targets

DEFAULT_MANIFEST = PROJECT_ROOT / "configs" / "capture_manifest.json"
DEFAULT_LIVE_DIR = PROJECT_ROOT / "data" / "live"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--live-dir", type=Path, default=DEFAULT_LIVE_DIR)
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit.")
    parser.add_argument(
        "--venue", action="append", default=None,
        help="Repeatable. Restrict to these venues; defaults to every venue in the manifest.",
    )
    parser.add_argument(
        "--tick-seconds", type=float, default=5.0,
        help="How often the loop wakes to see which entries are due.",
    )
    return parser.parse_args()


class Targeter:
    """Runs discovery cycles and keeps each venue's targets file current."""

    def __init__(
        self,
        manifest: Manifest,
        live_dir: Path,
        *,
        venues: tuple[str, ...] | None = None,
        clock=time.monotonic,
    ) -> None:
        self.manifest = manifest
        self.live_dir = Path(live_dir)
        self.venues = venues or manifest.venues()
        self.clock = clock
        self.coverage = CoverageLedger(self.live_dir / "coverage.json")
        self._due_at: dict[str, float] = {}
        # Last successful discovery per (entry, venue).
        #
        # A venue's targets file must always be the union of *every* active entry
        # for that venue, not just the ones whose cadence elapsed this tick.
        # Without this cache, a cycle where only the 60-second entries were due
        # rewrote the Kalshi file without the 120-second entry's ladder — silently
        # unsubscribing 300 live markets until that entry next came round.
        #
        # Cached results are slightly stale for entries that were not
        # rediscovered, which is precisely what their cadence declares acceptable.
        self._last: dict[tuple[str, str], Discovery] = {}
        self.cycles = 0

    def targets_path(self, venue: str) -> Path:
        return self.live_dir / f"targets_{venue}.json"

    def due_entries(self, now: float) -> list[str]:
        """Entry ids whose cadence has elapsed. Everything is due on the first pass."""
        due = []
        for entry in self.manifest.active():
            if not any(venue in self.venues for venue in entry.venues):
                continue
            if now >= self._due_at.get(entry.id, 0.0):
                due.append(entry.id)
        return due

    def run_cycle(self, *, only: list[str] | None = None) -> dict[str, Any]:
        """One pass over the due entries, writing one targets file per venue.

        Every entry for a venue is merged into that venue's single file, because a
        splice holds one connection and one subscription set. Merging here rather
        than making the splice read several manifests keeps the splice ignorant of
        why a market is interesting, which is the whole point of the split.
        """
        now = self.clock()
        entry_ids = only if only is not None else self.due_entries(now)
        entries = [entry for entry in self.manifest.active() if entry.id in set(entry_ids)]

        failures: dict[str, list[dict[str, str]]] = {venue: [] for venue in self.venues}

        # Rediscover only what is due; everything else is served from cache.
        for entry in entries:
            self._due_at[entry.id] = now + entry.discover_every_seconds
            for venue, selector in entry.venues.items():
                if venue not in self.venues:
                    continue
                found = self._discover(venue, selector)
                if found.error:
                    # Recorded per entry, not per venue. A single dict keyed by
                    # venue lets a later entry's success overwrite an earlier
                    # entry's failure, and the venue then looks healthy while its
                    # subscription set is silently missing whatever that entry
                    # would have contributed.
                    failures[venue].append({"entry": entry.id, "error": found.error})
                    continue
                self._last[(entry.id, venue)] = found

        # Assemble each venue's file from every active entry, due or not.
        by_venue: dict[str, list[Target]] = {venue: [] for venue in self.venues}
        rejections: dict[str, list[dict[str, Any]]] = {venue: [] for venue in self.venues}
        groups: dict[str, list[dict[str, Any]]] = {venue: [] for venue in self.venues}
        created: dict[str, dict[str, str]] = {venue: {} for venue in self.venues}
        seen: dict[str, set[str]] = {venue: set() for venue in self.venues}

        for entry in self.manifest.active():
            for venue in entry.venues:
                if venue not in self.venues:
                    continue
                found = self._last.get((entry.id, venue))
                if found is None:
                    # Never successfully discovered — on the first cycle this is
                    # the same as a failure, and the venue's file must not be
                    # written from an incomplete set.
                    if not any(f["entry"] == entry.id for f in failures[venue]):
                        failures[venue].append(
                            {"entry": entry.id, "error": "no successful discovery yet"}
                        )
                    continue
                for target in found.targets:
                    # An asset reachable through two entries is still one
                    # subscription; a duplicate would be refused by the targets
                    # writer and cost the whole file.
                    if target.asset_id in seen[venue]:
                        continue
                    seen[venue].add(target.asset_id)
                    by_venue[venue].append(target)
                created[venue].update(found.created_at)
                for rejection in found.rejections:
                    rejections[venue].append({"entry": entry.id, **rejection})
                for group in found.groups:
                    groups[venue].append({"entry": entry.id, **group})

        report: dict[str, Any] = {"at": utc_now(), "entries": [e.id for e in entries], "venues": {}}
        for venue in self.venues:
            if failures[venue]:
                # Any failed entry means this venue's set is known-incomplete, so
                # the existing file is left alone even when other entries
                # succeeded. Writing the partial set would unsubscribe live
                # markets for a reason unrelated to the venue's listings, and the
                # resulting hole is indistinguishable from a quiet market — the
                # exact confusion this whole split exists to prevent.
                #
                # Stale-but-complete beats fresh-but-truncated. The next cycle
                # retries in seconds.
                report["venues"][venue] = {
                    "targets_file": "unchanged",
                    "reason": "incomplete discovery",
                    "failures": failures[venue],
                    "would_have_written": len(by_venue[venue]),
                }
                continue
            report["venues"][venue] = self._write_venue(
                venue, by_venue[venue], rejections[venue], groups[venue], created[venue]
            )

        self.coverage.save()
        self.cycles += 1
        return report

    def _discover(self, venue: str, selector: dict[str, Any]) -> Discovery:
        source = SOURCES[venue]
        try:
            return source.discover(selector)
        except DiscoveryError as error:
            return Discovery(venue=venue, error=str(error))
        except Exception as error:  # noqa: BLE001 - one venue must not stop the rest
            return Discovery(venue=venue, error=f"{type(error).__name__}: {error}")

    def _write_venue(
        self,
        venue: str,
        targets: list[Target],
        rejections: list[dict[str, Any]],
        groups: list[dict[str, Any]],
        created_at: dict[str, str],
    ) -> dict[str, Any]:
        path = self.targets_path(venue)
        previous = None
        if path.exists():
            try:
                previous = load_targets(path, venue=venue).digest
            except Exception:  # noqa: BLE001 - an unreadable file is simply replaced
                previous = None

        digest = write_targets(
            path, venue=venue, targets=targets,
            note=f"targeter cycle {utc_now()} manifest={Path(self.manifest.source_path).name}",
        )
        fresh = self.coverage.observe(
            venue, [t.asset_id for t in targets], created_at=created_at
        )
        write_json(
            self.live_dir / f"rejected_{venue}.json",
            {"generated_at": utc_now(), "accepted": len(targets),
             "groups": groups, "rejected": rejections},
        )
        return {
            "targets": len(targets),
            "groups": len(groups),
            "rejected": len(rejections),
            "digest": digest,
            "changed": digest != previous,
            "newly_seen": len(fresh),
        }


def main() -> int:
    arguments = parse_args()
    try:
        manifest = load_manifest(arguments.manifest)
    except ManifestError as error:
        print(str(error), file=sys.stderr)
        if not arguments.manifest.exists():
            print(f"\nWrite one at {arguments.manifest}, or pass --manifest.", file=sys.stderr)
        return 2

    requested = tuple(arguments.venue) if arguments.venue else manifest.venues()
    unknown = [venue for venue in requested if venue not in SOURCES]
    if unknown:
        print(f"unknown venue(s): {', '.join(unknown)}", file=sys.stderr)
        return 2

    arguments.live_dir.mkdir(parents=True, exist_ok=True)
    targeter = Targeter(manifest, arguments.live_dir, venues=requested)

    if arguments.once:
        report = targeter.run_cycle(only=[entry.id for entry in manifest.active()])
        print(json.dumps(report, indent=2))
        return 0

    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    for received in (signal.SIGINT, signal.SIGTERM):
        signal.signal(received, stop)

    print(f"targeter watching {len(manifest.active())} entries across "
          f"{', '.join(requested)} (ctrl-c to stop)", file=sys.stderr)
    while not stopping:
        due = targeter.due_entries(time.monotonic())
        if due:
            report = targeter.run_cycle(only=due)
            print(json.dumps(report), flush=True)
        # Short sleeps rather than one long one, so a stop signal is honoured
        # within a tick instead of at the end of the slowest cadence.
        deadline = time.monotonic() + arguments.tick_seconds
        while not stopping and time.monotonic() < deadline:
            time.sleep(0.2)

    targeter.coverage.save()
    print(json.dumps({"stopped_at": utc_now(), "cycles": targeter.cycles,
                      "coverage": targeter.coverage.summary()}, indent=2), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
