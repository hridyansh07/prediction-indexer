#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from prediction_indexer.storage import utc_now, write_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a completed Oddpool history job and write coverage.json."
    )
    parser.add_argument("--job-directory", type=Path, required=True)
    return parser.parse_args()


def native_fingerprint(row: dict[str, Any]) -> str:
    native = {key: value for key, value in row.items() if key != "_provenance"}
    return hashlib.sha256(
        json.dumps(
            native,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def inspect_target(target: dict[str, Any]) -> dict[str, Any]:
    snapshots_path = Path(target["snapshots_path"])
    row_count = 0
    duplicate_count = 0
    invalid_provenance_count = 0
    minimum_timestamp: int | None = None
    maximum_timestamp: int | None = None
    asset_ids: set[str] = set()
    fingerprints: set[str] = set()

    if snapshots_path.exists():
        with snapshots_path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError(
                        f"{snapshots_path}:{line_number} is not a JSON object"
                    )
                row_count += 1
                fingerprint = native_fingerprint(value)
                if fingerprint in fingerprints:
                    duplicate_count += 1
                fingerprints.add(fingerprint)

                provenance = value.get("_provenance") or {}
                if (
                    provenance.get("source") != "oddpool"
                    or provenance.get("target_id") != target.get("target_id")
                ):
                    invalid_provenance_count += 1
                timestamp = value.get("timestamp")
                if isinstance(timestamp, (int, float)):
                    timestamp_int = int(timestamp)
                    minimum_timestamp = (
                        timestamp_int
                        if minimum_timestamp is None
                        else min(minimum_timestamp, timestamp_int)
                    )
                    maximum_timestamp = (
                        timestamp_int
                        if maximum_timestamp is None
                        else max(maximum_timestamp, timestamp_int)
                    )
                if value.get("asset_id") is not None:
                    asset_ids.add(str(value["asset_id"]))

    expected_count = int(target.get("records_written", 0))
    return {
        "target_id": target.get("target_id"),
        "event_key": target.get("event_key"),
        "venue": target.get("venue"),
        "market_id": target.get("market_id"),
        "complete": target.get("complete"),
        "pages_completed": int(target.get("pages_completed", 0)),
        "expected_row_count": expected_count,
        "row_count": row_count,
        "row_count_matches_checkpoint": row_count == expected_count,
        "duplicate_count": duplicate_count,
        "invalid_provenance_count": invalid_provenance_count,
        "minimum_timestamp": minimum_timestamp,
        "maximum_timestamp": maximum_timestamp,
        "asset_ids": sorted(asset_ids),
        "snapshots_path": str(snapshots_path),
    }


def main() -> int:
    args = parse_args()
    run_path = args.job_directory / "run.json"
    run = json.loads(run_path.read_text(encoding="utf-8"))
    targets = [inspect_target(target) for target in run.get("targets") or []]

    event_coverage: dict[str, dict[str, Any]] = {}
    for target in targets:
        event_key = str(target.get("event_key"))
        event = event_coverage.setdefault(
            event_key,
            {
                "event_key": event_key,
                "kalshi_target_count": 0,
                "kalshi_row_count": 0,
                "polymarket_target_count": 0,
                "polymarket_row_count": 0,
            },
        )
        venue = target.get("venue")
        event[f"{venue}_target_count"] += 1
        event[f"{venue}_row_count"] += target["row_count"]

    events = sorted(event_coverage.values(), key=lambda row: row["event_key"])
    for event in events:
        event["kalshi_archive_available"] = event["kalshi_row_count"] > 0
        event["polymarket_archive_available"] = event["polymarket_row_count"] > 0
        event["cross_venue_archive_available"] = (
            event["kalshi_archive_available"]
            and event["polymarket_archive_available"]
        )

    integrity_ok = all(
        target["complete"]
        and target["row_count_matches_checkpoint"]
        and target["duplicate_count"] == 0
        and target["invalid_provenance_count"] == 0
        for target in targets
    )
    report = {
        "generated_at": utc_now(),
        "job_id": run.get("job_id"),
        "integrity_ok": integrity_ok,
        "target_count": len(targets),
        "complete_target_count": sum(1 for target in targets if target["complete"]),
        "page_count": sum(target["pages_completed"] for target in targets),
        "row_count": sum(target["row_count"] for target in targets),
        "duplicate_count": sum(target["duplicate_count"] for target in targets),
        "invalid_provenance_count": sum(
            target["invalid_provenance_count"] for target in targets
        ),
        "zero_row_targets": [
            {
                "target_id": target["target_id"],
                "event_key": target["event_key"],
                "venue": target["venue"],
                "market_id": target["market_id"],
            }
            for target in targets
            if target["row_count"] == 0
        ],
        "cross_venue_event_count": sum(
            1 for event in events if event["cross_venue_archive_available"]
        ),
        "events": events,
        "targets": targets,
    }
    output_path = args.job_directory / "coverage.json"
    write_json(output_path, report)
    print(
        json.dumps(
            {
                "output": str(output_path),
                "integrity_ok": integrity_ok,
                "target_count": report["target_count"],
                "page_count": report["page_count"],
                "row_count": report["row_count"],
                "cross_venue_event_count": report["cross_venue_event_count"],
                "zero_row_target_count": len(report["zero_row_targets"]),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if integrity_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
