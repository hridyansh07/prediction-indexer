#!/usr/bin/env python3
"""Rebuild `live/coverage.json` from the generations already published.

Targeter v2 shipped without the coverage ledger v1 kept, so every asset
subscribed before `record_coverage` existed has no first sighting. Starting the
ledger fresh is not a neutral default: it would stamp assets that have been
subscribed for days with today's date, and `first_seen_at` is what bounds how far
back the tape counts as covered. Frames that were genuinely being captured would
look like they predate coverage, and an analysis honouring the ledger would
discard real evidence.

The evidence to reconstruct from is already on disk and is not a guess.
`live/targeter-v2/generations/<run_id>/targets_<venue>.json` is exactly what the
splices resolved and subscribed, and `<run_id>` is the instant that generation
was produced. Walking generations oldest-first and recording each asset the first
time it appears reproduces what a live ledger would have written, to within one
publish interval.

`created_at` comes from the run directory's archived catalogue where it is still
present. Those are what the run reaper reclaims, so this reads better the earlier
it runs — but a missing catalogue costs only the venue's creation time, never the
sighting itself, and an asset whose creation time is unknown is reported as
unmeasurable rather than as a lag of zero.

    python3 -m scripts.backfill_coverage --live-root data/live \
        --output-root data/targeter-v2-runs

Idempotent, and never moves a sighting later: rerunning it after the live writer
has been recording is safe, and repairs any sighting the live writer stamped too
late.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from targeter.coverage import CoverageLedger, created_at_of  # noqa: E402
from targeter.targets import TargetsError, load_targets  # noqa: E402
from targeter.v2.domain import SUPPORTED_VENUES  # noqa: E402
from targeter.v2.publication import catalog_reader, coverage_ledger_path  # noqa: E402
from targeter.v2.run_archive import (  # noqa: E402
    PRODUCTION_RECEIPT_FILE,
    RUN_ID,
    RunArchiveError,
    read_run_archive_receipt,
)


def generation_directories(live_root: Path) -> list[Path]:
    """Every published generation, oldest first.

    Sorted by name, which is the run id, which is an ISO-8601 basic timestamp —
    so lexical order is chronological order and no parsing is needed to sort.
    """
    root = Path(live_root) / "targeter-v2" / "generations"
    if not root.is_dir():
        return []
    return sorted(
        (item for item in root.iterdir() if item.is_dir() and RUN_ID.fullmatch(item.name)),
        key=lambda item: item.name,
    )


def run_instant(run_id: str) -> str:
    """The ISO-8601 instant a run id names."""
    moment = datetime.strptime(run_id[:-1], "%Y%m%dT%H%M%S.%f").replace(tzinfo=timezone.utc)
    return moment.isoformat()


def catalog_created_at(run_directory: Path, venue: str) -> dict[str, str]:
    """Creation time per subscription id, from a run's archived catalogue.

    Reads through the run's own archive receipt so a compressed catalogue is
    decoded against its committed identities — the same path publication takes,
    rather than a second, unverified decoder.

    Returns empty rather than raising when the run is gone or reaped. The run
    reaper reclaims artifacts once the archive proves them safe, and a reclaimed
    run must not stop the sighting itself from being recorded; it costs only the
    venue's creation time, which is reported as unmeasurable instead of guessed.
    """
    receipt_path = Path(run_directory) / PRODUCTION_RECEIPT_FILE
    if not receipt_path.exists():
        return {}
    try:
        receipt = read_run_archive_receipt(receipt_path)
    except (RunArchiveError, OSError) as error:
        print(f"  {run_directory.name}: unreadable receipt, no created_at: {error}", file=sys.stderr)
        return {}
    if receipt is None:
        return {}

    created: dict[str, str] = {}
    for suffix in (".ndjson", ".ndjson.zst"):
        name = f"catalog_{venue}_markets{suffix}"
        path = run_directory / name
        item = next((entry for entry in receipt.objects if entry.file == name), None)
        if item is None or not path.exists():
            continue
        try:
            with catalog_reader(path, item) as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    if not isinstance(record, dict):
                        continue
                    value = created_at_of(record)
                    if value is None:
                        continue
                    for asset_id in record.get("subscription_ids") or []:
                        if isinstance(asset_id, str) and asset_id:
                            created[asset_id] = value
        except (OSError, ValueError, json.JSONDecodeError) as error:
            print(f"  {name} unreadable, no created_at from it: {error}", file=sys.stderr)
        return created
    return created


def backfill(live_root: Path, output_root: Path) -> dict[str, Any]:
    ledger = CoverageLedger(coverage_ledger_path(live_root))
    generations = generation_directories(live_root)
    recorded: dict[str, int] = {venue: 0 for venue in SUPPORTED_VENUES}
    repaired = 0
    unreadable: list[str] = []

    for generation in generations:
        seen_at = run_instant(generation.name)
        run_directory = Path(output_root) / generation.name
        for venue in SUPPORTED_VENUES:
            target_path = generation / f"targets_{venue}.json"
            if not target_path.exists():
                continue
            try:
                asset_ids = load_targets(target_path, venue=venue).asset_ids()
            except TargetsError as error:
                unreadable.append(f"{generation.name}/{target_path.name}: {error}")
                continue
            if not asset_ids:
                continue
            created = catalog_created_at(run_directory, venue)
            # Generations are walked oldest first, so the first time an asset is
            # seen is already its earliest. `lower_first_sighting` matters on a
            # rerun, and when the live writer has since stamped an asset with a
            # date later than the generation that actually first subscribed it.
            for asset_id in asset_ids:
                if ledger.lower_first_sighting(
                    venue, asset_id, seen_at, created_at=created.get(asset_id)
                ):
                    repaired += 1
            fresh = ledger.observe(venue, asset_ids, created_at=created, now=seen_at)
            recorded[venue] += len(fresh)

    ledger.save()
    return {
        "generations": len(generations),
        "sightings": len(ledger),
        "recorded": dict(sorted(recorded.items())),
        "repaired": repaired,
        "unreadable": unreadable,
        "coverage": ledger.summary(),
        "per_venue": {venue: ledger.summary(venue) for venue in SUPPORTED_VENUES},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-root", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        required=True,
        help="Run directories, read only for the venue creation times their catalogues carry.",
    )
    parser.add_argument("--report", type=Path)
    arguments = parser.parse_args()

    if not Path(arguments.live_root).is_dir():
        parser.error(f"live root is not a directory: {arguments.live_root}")

    summary = backfill(arguments.live_root, arguments.output_root)
    encoded = json.dumps(summary, indent=2, sort_keys=True) + "\n"
    if arguments.report is not None:
        arguments.report.parent.mkdir(parents=True, exist_ok=True)
        arguments.report.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 1 if summary["unreadable"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
