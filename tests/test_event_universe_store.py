from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from archive.storage.base import JSON_CONTENT_TYPE
from archive.retrieval import cleanup_stale_retrieval_directories
from encoder import DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version, stored_identity_of
from targeter.v2.domain import CatalogSnapshot
from targeter.v2.registry import load_strategy
from targeter.v2.run import run_shadow
from targeter.v2.run_archive import archive_run
from tests.test_targeter_v2 import NOW, STRATEGY_PATH, snapshot
from universe.api import UniverseApplication
from universe.backfill import backfill_targeter_history
from universe.config import UniverseConfigError, load_config
from universe.market_projection import project_market_universe
from universe.projection import (
    ProjectionError,
    project_bundle_retirements,
    project_selected_bundles,
)
from universe.store import DetailTooLarge, EvidenceConflict, UniverseStore, file_sha256
from universe.sync import BOOTSTRAP_RUN_BUDGET, UniverseSync

R1 = "20260101T000000.000001Z"
R2 = "20260101T001000.000002Z"
R3 = "20260101T002000.000003Z"
R4 = "20260101T040000.000004Z"
G1 = "2026-01-01T00:00:00.000001Z"
G2 = "2026-01-01T00:10:00.000002Z"
G3 = "2026-01-01T00:20:00.000003Z"
G4 = "2026-01-01T04:00:00.000004Z"


class _Adapter:
    def __init__(self, catalog: CatalogSnapshot) -> None:
        self.venue = catalog.venue
        self.catalog = catalog

    def discover(self, _client, *, now):
        return self.catalog

    def probe_terminal(self, _client, targets):
        return {}


def _candidate(bundle_id: str, activation_at: str) -> dict:
    return {
        "bundle_id": bundle_id,
        "sport": "esports",
        "game": "counter_strike_2",
        "topology": "series",
        "participants": ["Alpha", "Beta"],
        "participant_keys": ["alpha", "beta"],
        "event_refs": ["kalshi:event-a", "polymarket:event-b"],
        "market_ids": [
            "kalshi:series",
            "polymarket:series",
            "polymarket:map-one",
        ],
        "eligible_market_ids": ["kalshi:series", "polymarket:series"],
        "activation_at": activation_at,
        "capture_start_at": "2026-01-01T02:00:00Z",
        "eligible": True,
        "event_status": "ELIGIBLE",
        "score": 10.0,
        "score_components": {"venue_coverage": 1000.0},
        "rejection_reasons": [],
        "market_exclusions": {},
        "admission": {
            "combined_moneyline_volume_usd": 30_000,
            "minimum_moneyline_volume_usd": 25_000,
            "moneyline_volume_usd_by_venue": {"kalshi": 30_000},
            "moneyline_volume_usd_coverage": {},
        },
        "relationship_analysis": {
            "relationships": [
                {
                    "bundle_id": bundle_id,
                    "left": "kalshi:series#claim=0",
                    "right": "polymarket:series#claim=0",
                    "relationship": "IDENTITY",
                    "scope": "series",
                    "left_venue": "kalshi",
                    "right_venue": "polymarket",
                    "cross_venue": True,
                    "coverage": "EXHAUSTIVE",
                }
            ]
        },
    }


def _target(bundle_id: str, venue: str, activation_at: str) -> dict:
    target_id = f"{venue}:series"
    return {
        "target_id": target_id,
        "bundle_id": bundle_id,
        "canonical_class": "esports.series_moneyline",
        "subscription_ids": (
            ["K-SERIES"] if venue == "kalshi" else ["pm-no", "pm-yes"]
        ),
        "activation_at": activation_at,
        "capture_start_at": "2026-01-01T02:00:00Z",
        "source_ref": f"{venue}:event",
        "continuity_score": 10.0,
    }


def _selection_report(
    run_id: str,
    generated_at: str,
    *,
    bundle_id: str = "bundle-1",
    activation_at: str = "2026-01-01T03:00:00Z",
    input_complete: bool = True,
) -> dict:
    report = {
        "report_version": 3,
        "mode": "shadow",
        "run_id": run_id,
        "generated_at": generated_at,
        "input_complete": input_complete,
        "strategy_version": 3,
        "candidates": [_candidate(bundle_id, activation_at)],
        "selection": {
            "bundle_ids": [bundle_id],
            "bundle_count": 1,
            "targets": {
                "kalshi": [_target(bundle_id, "kalshi", activation_at)],
                "polymarket": [_target(bundle_id, "polymarket", activation_at)],
                "limitless": [],
            },
        },
        "continuity": {
            "bundles": [],
            "retained_bundle_ids": [],
            "dispositions": {},
        },
    }
    if bundle_id != "bundle-1":
        suffix = "-" + bundle_id
        candidate = report["candidates"][0]
        candidate["event_refs"] = [value + suffix for value in candidate["event_refs"]]
        candidate["market_ids"] = [value + suffix for value in candidate["market_ids"]]
        candidate["eligible_market_ids"] = [
            value + suffix for value in candidate["eligible_market_ids"]
        ]
        for relation in candidate["relationship_analysis"]["relationships"]:
            relation["left"] = relation["left"].replace("#", suffix + "#")
            relation["right"] = relation["right"].replace("#", suffix + "#")
        for targets in report["selection"]["targets"].values():
            for target in targets:
                target["target_id"] += suffix
    return report


def _empty_report(
    run_id: str, generated_at: str, *, input_complete: bool = True
) -> dict:
    report = _selection_report(
        run_id, generated_at, input_complete=input_complete
    )
    report["candidates"] = []
    report["selection"] = {
        "bundle_ids": [],
        "bundle_count": 0,
        "targets": {"kalshi": [], "polymarket": [], "limitless": []},
    }
    return report


def _retained_report(
    run_id: str,
    generated_at: str,
    origin: dict[str, str | int],
) -> dict:
    report = _selection_report(run_id, generated_at)
    report["candidates"] = []
    targets = [
        {
            **target,
            "venue": venue,
            "venue_market_id": target["target_id"].split(":", 1)[1],
            "terminal_probe": {"state": "unknown", "reason": "probe_failed"},
        }
        for venue, values in report["selection"]["targets"].items()
        for target in values
    ]
    report["continuity"] = {
        "bundles": [
            {
                "base_run_id": origin["run_id"],
                "bundle_id": "bundle-1",
                "activation_at": "2026-01-01T03:00:00Z",
                "score": 10.0,
                "targets": targets,
                "origin_run_id": origin["run_id"],
                "origin_report_sha256": origin["report_sha256"],
                "origin_archive_manifest_key": origin["manifest_key"],
                "origin_archive_manifest_sha256": origin["manifest_sha256"],
            }
        ],
        "retained_bundle_ids": ["bundle-1"],
        "dispositions": {"bundle-1": "retained"},
    }
    return report


def _retirement_report(
    run_id: str,
    generated_at: str,
    origin: dict[str, str | int],
    *,
    disposition: str = "all_markets_terminal",
) -> dict:
    report = _empty_report(run_id, generated_at)
    source = _selection_report(str(origin["run_id"]), G1)
    probe_state = "terminal" if disposition == "all_markets_terminal" else "unknown"
    targets = [
        {
            **target,
            "venue": venue,
            "venue_market_id": target["target_id"].split(":", 1)[1],
            "terminal_probe": {"state": probe_state, "reason": "test_probe"},
        }
        for venue, values in source["selection"]["targets"].items()
        for target in values
    ]
    report["continuity"] = {
        "bundles": [
            {
                "base_run_id": origin["run_id"],
                "bundle_id": "bundle-1",
                "activation_at": "2026-01-01T03:00:00Z",
                "score": 10.0,
                "targets": targets,
                "origin_run_id": origin["run_id"],
                "origin_report_sha256": origin["report_sha256"],
                "origin_archive_manifest_key": origin["manifest_key"],
                "origin_archive_manifest_sha256": origin["manifest_sha256"],
            }
        ],
        "retained_bundle_ids": [],
        "dispositions": {"bundle-1": disposition},
    }
    return report


def _put(
    store: LocalObjectStore,
    key: str,
    payload: bytes,
    *,
    content_type: str = JSON_CONTENT_TYPE,
    content_encoding: str | None = None,
):
    identity = stored_identity_of(io.BytesIO(payload))
    store.put_immutable(
        key,
        io.BytesIO(payload),
        identity,
        content_type=content_type,
        content_encoding=content_encoding,
    )
    return identity


def _compression() -> dict:
    return {
        "algorithm": "zstd",
        "level": DEFAULT_ZSTD_LEVEL,
        "frame_checksum": True,
        "dictionary": None,
        "frame_count": 1,
        "encoder": encoder_version(),
    }


def _catalog_rows(report: dict) -> tuple[list[dict], list[dict]]:
    events: list[dict] = []
    markets: list[dict] = []
    selected_classes = {
        target["target_id"]: target["canonical_class"]
        for targets in report["selection"]["targets"].values()
        for target in targets
    }
    for candidate in report["candidates"]:
        event_by_venue = {
            reference.split(":", 1)[0]: reference.split(":", 1)[1]
            for reference in candidate["event_refs"]
        }
        for venue, event_id in event_by_venue.items():
            events.append(
                {
                    "venue": venue,
                    "venue_event_id": event_id,
                    "sport": candidate["sport"],
                    "league": "test league",
                    "title": "Alpha vs Beta",
                    "participants": candidate["participants"],
                    "participant_keys": candidate["participant_keys"],
                    "activation_at": candidate["activation_at"],
                    "status": "open",
                    "source_ref": f"/{venue}/{event_id}",
                    "format": "3",
                    "fragment_type": None,
                    "game": candidate["game"],
                    "topology": candidate["topology"],
                }
            )
        for target_id in candidate["market_ids"]:
            venue, market_id = target_id.split(":", 1)
            canonical_class = selected_classes.get(
                target_id,
                "esports.map_winner"
                if "map" in market_id
                else "esports.series_moneyline",
            )
            markets.append(
                {
                    "target_id": target_id,
                    "venue": venue,
                    "venue_market_id": market_id,
                    "venue_event_id": event_by_venue[venue],
                    "canonical_class": canonical_class,
                    "market_type": canonical_class.split(".", 1)[1],
                    "scope": "series",
                    "title": market_id,
                    "parameters": {"side": "home"},
                    "subscription_ids": [f"{market_id}-subscription"],
                    "outcome_labels": ["Yes", "No"],
                    "status": "open",
                    "accepting_orders": True,
                    "rules_hash": None,
                    "created_at": "2025-12-31T00:00:00Z",
                    "volume_24h": 100,
                    "volume_total": 500,
                    "volume_total_usd": 30_000,
                    "liquidity": 200,
                    "source_ref": f"/{venue}/{market_id}",
                }
            )
    return events, markets


def _publish_run(
    store: LocalObjectStore,
    report: dict,
    *,
    compressed: bool = False,
    manifest_version: int = 2,
) -> dict[str, str | int]:
    run_id = report["run_id"]
    date = report["generated_at"].split("T", 1)[0]
    prefix = f"targeter-v2/runs/date={date}/run={run_id}"
    logical = (
        json.dumps(report, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode()
    if compressed:
        destination = io.BytesIO()
        encoded = encode_stream(io.BytesIO(logical), destination)
        report_name = "selection_report.json.zst"
        report_payload = destination.getvalue()
        report_identity = _put(
            store,
            f"{prefix}/{report_name}",
            report_payload,
            content_encoding="zstd",
        )
        report_record = {
            "file": report_name,
            "content_type": JSON_CONTENT_TYPE,
            "content_encoding": "zstd",
            "decoded": encoded.logical.as_record(),
            "stored": encoded.stored.as_record(),
            "compression": _compression(),
        }
    else:
        report_name = "selection_report.json"
        report_identity = _put(store, f"{prefix}/{report_name}", logical)
        report_record = {
            "file": report_name,
            "byte_length": report_identity.byte_length,
            "sha256": report_identity.sha256,
            "content_type": JSON_CONTENT_TYPE,
            "content_encoding": None,
        }
    artifact_records = []
    events_by_venue: dict[str, list[dict]] = {}
    markets_by_venue: dict[str, list[dict]] = {}
    selected_classes = {
        target["target_id"]: target["canonical_class"]
        for targets in report["selection"]["targets"].values()
        for target in targets
    }
    for candidate in report["candidates"]:
        event_by_venue = {
            reference.split(":", 1)[0]: reference.split(":", 1)[1]
            for reference in candidate["event_refs"]
        }
        for venue, event_id in event_by_venue.items():
            events_by_venue.setdefault(venue, []).append(
                {
                    "venue": venue,
                    "venue_event_id": event_id,
                    "sport": candidate["sport"],
                    "league": "test league",
                    "title": "Alpha vs Beta",
                    "participants": candidate["participants"],
                    "participant_keys": candidate["participant_keys"],
                    "activation_at": candidate["activation_at"],
                    "status": "open",
                    "source_ref": f"/{venue}/{event_id}",
                    "format": "3",
                    "fragment_type": None,
                    "game": candidate["game"],
                    "topology": candidate["topology"],
                    "game_evidence": [],
                    "activation_evidence": [],
                }
            )
        for target_id in candidate["market_ids"]:
            venue, market_id = target_id.split(":", 1)
            canonical_class = selected_classes.get(
                target_id,
                "esports.map_winner" if "map" in market_id else "esports.series_moneyline",
            )
            market_type = canonical_class.split(".", 1)[1]
            markets_by_venue.setdefault(venue, []).append(
                {
                    "target_id": target_id,
                    "venue": venue,
                    "venue_market_id": market_id,
                    "venue_event_id": event_by_venue[venue],
                    "canonical_class": canonical_class,
                    "market_type": market_type,
                    "scope": "series",
                    "title": market_id,
                    "parameters": {"side": "home"},
                    "subscription_ids": [f"{market_id}-subscription"],
                    "outcome_labels": ["Yes", "No"],
                    "status": "open",
                    "accepting_orders": True,
                    "rules_text": None,
                    "rules_hash": None,
                    "created_at": "2025-12-31T00:00:00Z",
                    "volume_24h": 100,
                    "volume_total": 500,
                    "volume_total_usd": 30_000,
                    "liquidity": 200,
                    "source_ref": f"/{venue}/{market_id}",
                    "classification_evidence": None,
                }
            )
    for venue in sorted(set(events_by_venue) | set(markets_by_venue)):
        for kind, rows in (
            ("events", events_by_venue.get(venue, [])),
            ("markets", markets_by_venue.get(venue, [])),
        ):
            payload = b"".join(
                (json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n").encode()
                for row in rows
            )
            name = f"catalog_{venue}_{kind}.ndjson"
            identity = _put(
                store,
                f"{prefix}/{name}",
                payload,
                content_type="application/x-ndjson",
            )
            artifact_records.append(
                {
                    "file": name,
                    "content_type": "application/x-ndjson",
                    "content_encoding": None,
                    "decoded": {
                        "sha256": identity.sha256,
                        "byte_length": identity.byte_length,
                        "line_count": len(rows),
                    },
                    "stored": identity.as_record(),
                    "compression": None,
                }
            )
    manifest = {
        "targeter_run_manifest_version": manifest_version,
        "run_id": run_id,
        "generated_at": report["generated_at"],
        "input_complete": report["input_complete"],
        "files": [*artifact_records, report_record],
    }
    manifest_payload = (
        json.dumps(manifest, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    manifest_key = f"{prefix}/run_manifest.json"
    manifest_identity = _put(store, manifest_key, manifest_payload)
    return {
        "run_id": run_id,
        "manifest_key": manifest_key,
        "manifest_sha256": manifest_identity.sha256,
        "report_key": f"{prefix}/{report_name}",
        "report_sha256": report_identity.sha256,
    }


class ProjectionTests(unittest.TestCase):
    def test_projects_only_selected_complete_context(self) -> None:
        row = project_selected_bundles(_selection_report(R1, G1))[0]
        self.assertEqual(row["projection_version"], 1)
        self.assertEqual(row["occurrence_kind"], "complete")
        self.assertEqual(row["origin_run_id"], R1)
        self.assertEqual(
            [market["target_id"] for market in row["markets"]],
            ["kalshi:series", "polymarket:map-one", "polymarket:series"],
        )
        self.assertNotIn("planned_capture_end_at", row)

    def test_rejects_pre_v3_or_incomplete_reports(self) -> None:
        report = _selection_report(R1, G1)
        report["report_version"] = 2
        with self.assertRaisesRegex(ProjectionError, "version 3"):
            project_selected_bundles(report)
        report = _selection_report(R1, G1, input_complete=False)
        with self.assertRaisesRegex(ProjectionError, "complete input"):
            project_selected_bundles(report)

    def test_retirement_distinguishes_terminal_observation_from_clamp(self) -> None:
        origin = {
            "run_id": R1,
            "report_sha256": "a" * 64,
            "manifest_key": (
                f"targeter-v2/runs/date=2026-01-01/run={R1}/run_manifest.json"
            ),
            "manifest_sha256": "b" * 64,
        }
        terminal = _retirement_report(R4, G4, origin)
        self.assertTrue(project_bundle_retirements(terminal)[0]["terminal_observed"])

        terminal["continuity"]["bundles"][0]["targets"][0]["terminal_probe"] = {
            "state": "unknown",
            "reason": "probe_failed",
        }
        with self.assertRaisesRegex(ProjectionError, "non-terminal target probe"):
            project_bundle_retirements(terminal)

        clamped = _retirement_report(
            R4, G4, origin, disposition="terminal_clamp_elapsed"
        )
        self.assertFalse(project_bundle_retirements(clamped)[0]["terminal_observed"])

    def test_event_identity_uses_exact_native_reference_set_not_activation(self) -> None:
        first = _selection_report(R1, G1, activation_at="2026-01-01T03:00:00Z")
        shifted = _selection_report(R2, G2, activation_at="2026-01-01T03:05:00Z")
        first_projection = project_market_universe(
            first,
            catalog_events=_catalog_rows(first)[0],
            catalog_markets=_catalog_rows(first)[1],
        )
        shifted_projection = project_market_universe(
            shifted,
            catalog_events=_catalog_rows(shifted)[0],
            catalog_markets=_catalog_rows(shifted)[1],
        )
        self.assertEqual(
            first_projection["events"][0]["event_id"],
            shifted_projection["events"][0]["event_id"],
        )

        changed = _selection_report(R2, G2)
        changed["candidates"][0]["event_refs"].append("limitless:event-c")
        changed_events, changed_markets = _catalog_rows(changed)
        changed_projection = project_market_universe(
            changed,
            catalog_events=changed_events,
            catalog_markets=changed_markets,
        )
        self.assertNotEqual(
            first_projection["events"][0]["event_id"],
            changed_projection["events"][0]["event_id"],
        )

    def test_unreferenced_duplicate_catalogue_rows_do_not_reject_projection(self) -> None:
        report = _selection_report(R1, G1)
        events, markets = _catalog_rows(report)
        unrelated = {**events[0], "venue_event_id": "unrelated"}
        projection = project_market_universe(
            report,
            catalog_events=[*events, unrelated, unrelated],
            catalog_markets=markets,
        )
        self.assertEqual(len(projection["venue_events"]), 2)


class EventUniverseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = UniverseStore(self.root / "universe.sqlite3")
        self.database.initialize()
        self.objects = LocalObjectStore(
            self.root / "objects", store_id="archive", durability=INDEPENDENT
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_bootstrap_indexes_latest_only_then_incremental_appends(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        _publish_run(self.objects, _selection_report(R2, G2))

        first = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc)
        )
        self.assertEqual(first.ingested, 1, first.as_record())
        runs, _more = self.database.list_runs()
        self.assertEqual([run["run_id"] for run in runs], [R2])

        _publish_run(self.objects, _selection_report(R3, G3))
        second = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 1, 0, 21, tzinfo=timezone.utc)
        )
        self.assertEqual(second.ingested, 2, second.as_record())
        runs, _more = self.database.list_runs()
        self.assertEqual([run["run_id"] for run in runs], [R1, R2, R3])
        selections, _more = self.database.list_selections(sort="selected")
        self.assertEqual([row["run_id"] for row in selections], [R1, R2, R3])
        events, _more = self.database.list_events()
        self.assertEqual(events[0]["first_seen_run_id"], R1)
        self.assertEqual(events[0]["last_seen_run_id"], R3)
        event = self.database.event_detail(events[0]["event_id"])
        assert event is not None
        self.assertEqual(event["markets"][0]["first_seen_run_id"], R1)
        self.assertEqual(event["markets"][0]["last_seen_run_id"], R3)

    def test_bootstrap_keeps_latest_incomplete_and_newest_complete_run(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        _publish_run(self.objects, _empty_report(R2, G2, input_complete=False))

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc)
        )

        self.assertEqual(result.ingested, 2, result.as_record())
        runs, _more = self.database.list_runs()
        self.assertEqual([run["run_id"] for run in runs], [R1, R2])
        status = self.database.targeter_status_snapshot()
        self.assertEqual(status["latest_run"]["run_id"], R2)
        self.assertEqual(status["current_complete_run"]["run_id"], R1)

    def test_bootstrap_advances_high_water_past_failed_older_date(self) -> None:
        invalid = _empty_report(R1, G1, input_complete=False)
        invalid["report_version"] = 2
        _publish_run(self.objects, invalid)
        newer_run = "20260102T000000.000001Z"
        _publish_run(
            self.objects,
            _empty_report(newer_run, "2026-01-02T00:00:00.000001Z", input_complete=False),
        )

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 2, 0, 1, tzinfo=timezone.utc)
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(len(result.failures), 1)
        self.assertEqual(
            self.database.checkpoint("targeter-v3-incremental-date"), "2026-01-02"
        )
        self.assertEqual(self.database.sync_failure_count(), 1)

    def test_malformed_manifest_key_isolated_from_valid_discovery(self) -> None:
        _publish_run(self.objects, _empty_report(R1, G1))
        _put(
            self.objects,
            "targeter-v2/runs/date=2026-01-01/run=malformed/run_manifest.json",
            b"{}\n",
        )

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 1, 1, tzinfo=timezone.utc)
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(result.failure_count, 1)
        self.assertEqual(self.database.status()["sync"]["pending_failures"], 1)

    def test_later_runs_progress_while_failed_manifest_retries_are_bounded(self) -> None:
        _publish_run(self.objects, _empty_report(R1, G1))
        self.assertEqual(UniverseSync(self.database, self.objects).sync().failures, [])
        bad_id = "20260101T010000.000000Z"
        bad = _empty_report(bad_id, "2026-01-01T01:00:00Z")
        bad["report_version"] = 2
        _publish_run(self.objects, bad)
        later_id = "20260102T010000.000000Z"
        _publish_run(
            self.objects,
            _empty_report(later_id, "2026-01-02T01:00:00Z"),
        )

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 2, 2, tzinfo=timezone.utc)
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(self.database.checkpoint("targeter-v3-incremental-date"), "2026-01-02")
        self.assertEqual(self.database.sync_failure_count(), 1)
        runs, _more = self.database.list_runs()
        self.assertIn(later_id, {run["run_id"] for run in runs})

    def test_failed_manifest_uses_durable_exponential_backoff(self) -> None:
        bad = _empty_report(R1, G1)
        bad["report_version"] = 2
        _publish_run(self.objects, bad)
        sync = UniverseSync(self.database, self.objects)
        first_at = datetime(2026, 1, 1, 1, tzinfo=timezone.utc)

        first = sync.sync(now=first_at)
        deferred = sync.sync(
            now=datetime(2026, 1, 1, 1, 0, 30, tzinfo=timezone.utc)
        )
        retry = sync.sync(
            now=datetime(2026, 1, 1, 1, 1, 1, tzinfo=timezone.utc)
        )

        self.assertEqual(first.failure_count, 1)
        self.assertEqual(deferred.failure_count, 0)
        self.assertEqual(deferred.pending_failures, 1)
        self.assertEqual(retry.failure_count, 1)
        with sqlite3.connect(self.database.path) as connection:
            attempts, delay_ns = connection.execute(
                """SELECT attempts, next_retry_at_ns - last_failed_at_ns
                   FROM universe_sync_failures"""
            ).fetchone()
        self.assertEqual(attempts, 2)
        self.assertEqual(delay_ns, 120 * 1_000_000_000)

    def test_bootstrap_walkback_has_explicit_budget(self) -> None:
        for minute in range(BOOTSTRAP_RUN_BUDGET + 1):
            run_id = f"20260101T{minute // 60:02d}{minute % 60:02d}00.000000Z"
            generated = f"2026-01-01T{minute // 60:02d}:{minute % 60:02d}:00Z"
            _publish_run(
                self.objects,
                _empty_report(run_id, generated, input_complete=False),
            )

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 2, tzinfo=timezone.utc)
        )

        self.assertTrue(result.bootstrap_exhausted)
        self.assertEqual(result.ingested, BOOTSTRAP_RUN_BUDGET)

    def test_bounded_backfill_uses_report_without_selected_index(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1), compressed=True)
        _publish_run(self.objects, _selection_report(R2, G2), compressed=True)

        result = backfill_targeter_history(
            objects=self.objects,
            database=self.database,
            generated_start=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
            generated_end=datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc),
            temporary_directory=self.root / "tmp",
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(result.failures, [])
        detail = self.database.selection_detail(R1, "bundle-1")
        assert detail is not None
        self.assertTrue(detail["source"]["report_key"].endswith(".json.zst"))
        self.assertFalse(
            any(
                "selected_bundle_index" in key
                for key in self.objects.list_keys("targeter-v2/runs/")
            )
        )

    def test_consumes_actual_archived_targeter_v3_run_without_derivative(self) -> None:
        catalogs = (
            snapshot("kalshi", "kalshi-event", "kalshi-market"),
            snapshot("polymarket", "pm-event", "pm-market"),
            CatalogSnapshot("limitless", (), ()),
        )
        shadow = run_shadow(
            strategy=load_strategy(STRATEGY_PATH),
            output_root=self.root / "runs",
            cache_root=self.root / "cache",
            live_root=self.root / "live",
            now=NOW,
            adapters=[_Adapter(catalog) for catalog in catalogs],
            client=object(),
        )
        archive_run(shadow.directory, self.objects, now=NOW)

        result = UniverseSync(self.database, self.objects).sync()

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(result.failures, [])
        selections, _more = self.database.list_selections()
        self.assertEqual(len(selections), 1)
        self.assertFalse(
            any(
                "selected_bundle_index" in key
                for key in self.objects.list_keys("targeter-v2/runs/")
            )
        )

    def test_backfill_appends_history_and_deduplicates_context(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        _publish_run(self.objects, _selection_report(R2, G2))
        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(result.ingested, 2, result.as_record())
        status = self.database.status()
        self.assertEqual(status["counts"]["selection_occurrences"], 2)
        self.assertEqual(status["counts"]["bundle_contexts"], 1)

        retry = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(retry.ingested, 0)
        self.assertEqual(retry.skipped, 2)

    def test_backfill_batches_checkpoints_and_resumes_after_progress_failure(self) -> None:
        for run_id, generated_at in ((R1, G1), (R2, G2), (R3, G3)):
            _publish_run(self.objects, _empty_report(run_id, generated_at))
        progress: list[dict] = []

        def interrupt(record: dict) -> None:
            progress.append(record)
            raise RuntimeError("simulated scheduler interruption")

        with self.assertRaisesRegex(RuntimeError, "scheduler interruption"):
            backfill_targeter_history(
                objects=self.objects,
                database=self.database,
                generated_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
                generated_end=datetime(2026, 1, 2, tzinfo=timezone.utc),
                batch_size=1,
                progress=interrupt,
            )
        self.assertEqual(progress[0]["processed"], 1)

        resumed: list[dict] = []
        result = backfill_targeter_history(
            objects=self.objects,
            database=self.database,
            generated_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            generated_end=datetime(2026, 1, 2, tzinfo=timezone.utc),
            batch_size=1,
            progress=resumed.append,
        )
        self.assertEqual(result.ingested, 2, result.as_record())
        self.assertTrue(result.completed)
        self.assertEqual([row["processed"] for row in resumed], [1, 1])
        self.assertEqual(self.database.status()["counts"]["targeter_runs"], 3)

    def test_backfill_completion_is_scoped_to_its_requested_date_partitions(self) -> None:
        _publish_run(self.objects, _empty_report(R1, G1))
        self.database.record_sync_failure(
            "targeter-v2/runs/date=2025-12-01/run=malformed/run_manifest.json",
            "outside configured rebuild range",
            now_ns=0,
        )

        result = backfill_targeter_history(
            objects=self.objects,
            database=self.database,
            generated_start=datetime(2026, 1, 1, tzinfo=timezone.utc),
            generated_end=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

        self.assertTrue(result.completed)
        self.assertEqual(result.pending_failures, 1)

    def test_retained_selection_resolves_origin_outside_requested_range(self) -> None:
        origin = _publish_run(self.objects, _selection_report(R1, G1))
        _publish_run(self.objects, _retained_report(R2, G2, origin))

        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, 0, 9, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc),
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(result.origin_dependencies_ingested, 1)
        detail = self.database.selection_detail(R2, "bundle-1")
        assert detail is not None
        self.assertEqual(detail["occurrence_kind"], "retained")
        self.assertEqual(detail["origin"]["run_id"], R1)
        self.assertEqual(detail["origin"]["manifest_key"], origin["manifest_key"])
        self.assertEqual(detail["context"]["event_refs"], [
            "kalshi:event-a",
            "polymarket:event-b",
        ])
        run_detail = self.database.targeter_run_detail(R2)
        assert run_detail is not None
        self.assertEqual(
            [event["event_id"] for event in run_detail["events"]],
            [run_detail["selected_markets"][0]["event_id"]],
        )
        self.assertEqual(self.database.status()["counts"]["bundle_contexts"], 1)

    def test_retained_selection_fails_closed_on_origin_drift(self) -> None:
        origin = _publish_run(self.objects, _selection_report(R1, G1))
        report = _retained_report(R2, G2, origin)
        report["continuity"]["bundles"][0]["origin_report_sha256"] = "f" * 64
        _publish_run(self.objects, report)

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc)
        )

        self.assertEqual(result.ingested, 1)
        self.assertEqual(len(result.failures), 1)
        self.assertIn("report identity disagrees", result.failures[0])
        self.assertEqual(self.database.status()["counts"]["targeter_runs"], 1)

    def test_all_terminal_continuity_records_proven_observed_end(self) -> None:
        origin = _publish_run(self.objects, _selection_report(R1, G1))
        retirement = _publish_run(
            self.objects,
            _retirement_report(R4, G4, origin),
        )

        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, 3, 59, tzinfo=timezone.utc),
            datetime(2026, 1, 1, 4, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(result.origin_dependencies_ingested, 1)
        detail = self.database.selection_detail(R1, "bundle-1")
        assert detail is not None
        self.assertEqual(
            detail["retirement"],
            {
                "retired_at": G4,
                "disposition": "all_markets_terminal",
                "terminal_observed_at": G4,
                "source": {
                    "run_id": R4,
                    "manifest_key": retirement["manifest_key"],
                    "manifest_sha256": retirement["manifest_sha256"],
                    "report_key": retirement["report_key"],
                    "report_sha256": retirement["report_sha256"],
                },
            },
        )
        self.assertNotIn("ended_at", detail)
        selections, _more = self.database.list_selections()
        self.assertEqual(selections[0]["retirement"], detail["retirement"])
        self.assertTrue(self.database.audit_run(R4)["ok"])
        self.assertEqual(
            self.database.status()["counts"]["bundle_retirements"], 1
        )

    def test_incomplete_and_complete_empty_runs_are_visible_without_selections(self) -> None:
        _publish_run(self.objects, _empty_report(R1, G1, input_complete=False))
        _publish_run(self.objects, _empty_report(R2, G2))
        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(result.ingested, 2, result.as_record())
        self.assertEqual(result.incomplete, 1)
        runs, _more = self.database.list_runs()
        self.assertEqual([run["input_complete"] for run in runs], [False, True])
        selections, _more = self.database.list_selections()
        self.assertEqual(selections, [])
        incomplete = self.database.targeter_run_detail(R1)
        assert incomplete is not None
        self.assertEqual(incomplete["counts"]["selected_events"], 0)
        self.assertEqual(incomplete["selected_markets"], [])

    def test_incomplete_candidates_do_not_create_universe_bindings(self) -> None:
        _publish_run(
            self.objects,
            _selection_report(R1, G1, input_complete=False),
        )
        result = UniverseSync(self.database, self.objects).sync()
        self.assertEqual(result.failures, [], result.as_record())
        counts = self.database.status()["counts"]
        for table in (
            "umbrella_events",
            "canonical_markets",
            "venue_markets",
            "relations",
        ):
            self.assertEqual(counts[table], 0, table)
        detail = self.database.targeter_run_detail(R1)
        assert detail is not None
        self.assertEqual(detail["decisions"], [])

    def test_activation_drift_is_recorded_without_changing_event_identity(self) -> None:
        _publish_run(
            self.objects,
            _selection_report(R1, G1, activation_at="2026-01-01T03:00:00Z"),
        )
        _publish_run(
            self.objects,
            _selection_report(R2, G2, activation_at="2026-01-01T03:05:00Z"),
        )
        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(result.failures, [], result.as_record())
        events, _more = self.database.list_events()
        self.assertEqual(len(events), 1)
        detail = self.database.event_detail(events[0]["event_id"])
        assert detail is not None
        self.assertEqual(detail["event"]["activation_at"], "2026-01-01T03:00:00Z")
        self.assertEqual(
            [row["observed_activation_at"] for row in detail["observations"]],
            ["2026-01-01T03:00:00Z", "2026-01-01T03:05:00Z"],
        )

    def test_incomplete_run_cannot_poison_later_activation_observation(self) -> None:
        _publish_run(
            self.objects,
            _selection_report(
                R1,
                G1,
                input_complete=False,
                activation_at="2026-01-01T03:00:00Z",
            ),
        )
        _publish_run(
            self.objects,
            _selection_report(R2, G2, activation_at="2026-01-01T03:05:00Z"),
        )

        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(result.failures, [], result.as_record())
        events, _more = self.database.list_events()
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["activation_at"], "2026-01-01T03:05:00Z")

    def test_changed_exact_reference_set_preserves_native_binding_conflict(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        changed = _selection_report(R2, G2)
        changed["candidates"][0]["event_refs"].append("limitless:event-c")
        _publish_run(self.objects, changed)

        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(result.failure_count, 1)
        self.assertIn("assigned to a different umbrella event", result.failures[0])

    def test_run_history_is_not_truncated_and_cadence_route_is_removed(self) -> None:
        run_ids = []
        for minute in range(6):
            run_id = f"20260101T00{minute:02d}00.000000Z"
            generated_at = f"2026-01-01T00:{minute:02d}:00Z"
            run_ids.append(run_id)
            _publish_run(self.objects, _empty_report(run_id, generated_at))
        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(result.ingested, 6, result.as_record())

        with sqlite3.connect(self.database.path) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM targeter_runs").fetchone()[0],
                6,
            )
        self.assertTrue(UniverseSync(self.database, self.objects).audit_run(run_ids[0])["ok"])
        self.assertEqual(UniverseApplication(self.database).get("/v1/targeter/cadence")[0], 404)

    def test_projects_market_universe_and_bounded_run_decisions(self) -> None:
        report = _selection_report(R1, G1)
        report.update(
            {
                "catalogs": [
                    {
                        "venue": "kalshi",
                        "complete": False,
                        "events": 4,
                        "markets": 8,
                        "requests": 2,
                        "diagnostics": ["partial"],
                        "classification_diagnostics": [
                            {"code": "unknown_product"},
                            {"code": "unknown_product"},
                        ],
                    }
                ],
                "discovery_failures": {"limitless": "timeout"},
                "continuity_diagnostics": ["pointer unavailable"],
                "target_record_diagnostics": {"kalshi": ["missing record"]},
            }
        )
        report["candidates"][0].update(
            {
                "score": 12.5,
                "rejection_reasons": [],
            }
        )
        report["candidates"][0]["admission"][
            "combined_moneyline_volume_usd"
        ] = 30_000
        _publish_run(self.objects, report)
        result = UniverseSync(self.database, self.objects).sync()
        self.assertEqual(result.failures, [])

        run = self.database.targeter_run_detail(R1)
        assert run is not None
        self.assertEqual(run["counts"]["candidates"], 1)
        self.assertEqual(run["counts"]["selected_events"], 1)
        self.assertEqual(
            run["decisions"][0]["admission"]["combined_moneyline_volume_usd"],
            30_000,
        )
        self.assertEqual(run["selected_markets"][0]["continuity_score"], 10.0)
        events, _more = self.database.list_events()
        self.assertEqual(len(events), 1)
        detail = self.database.event_detail(events[0]["event_id"])
        assert detail is not None
        self.assertEqual(len(detail["venue_events"]), 2)
        self.assertEqual(len(detail["relations"]), 1)

    def test_targeter_status_is_compact_and_uses_newest_complete_run(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        _publish_run(self.objects, _empty_report(R2, G2, input_complete=False))
        self.assertEqual(
            UniverseSync(self.database, self.objects)
            .sync_range(
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            )
            .failures,
            [],
        )
        status, payload = UniverseApplication(self.database).get(
            "/v1/targeter/status?limit=5"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            set(payload),
            {
                "status_projection_version",
                "observed_at",
                "freshness",
                "latest_run",
                "current_complete_run",
                "current_complete_summary",
            },
        )
        self.assertEqual(payload["latest_run"]["run_id"], R2)
        self.assertFalse(payload["latest_run"]["input_complete"])
        self.assertEqual(payload["current_complete_run"]["run_id"], R1)
        self.assertTrue(payload["current_complete_run"]["input_complete"])
        self.assertEqual(
            payload["current_complete_summary"],
            {
                "selected_bundles": 1,
                "selected_targets": 2,
                "venues": ["kalshi", "polymarket"],
            },
        )
        self.assertLess(len(json.dumps(payload, separators=(",", ":"))), 2_000)

    def test_targeter_run_detail_returns_normalized_references_not_report_payload(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        self.assertEqual(
            UniverseSync(self.database, self.objects)
            .sync_range(
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            )
            .failures,
            [],
        )
        status, detail = UniverseApplication(self.database).get(
            f"/v1/targeter/runs/{R1}"
        )
        self.assertEqual(status, 200)
        self.assertEqual(detail["run"]["run_id"], R1)
        self.assertEqual(detail["decisions"][0]["bundle_id"], "bundle-1")
        self.assertEqual(len(detail["selected_markets"]), 2)
        self.assertEqual(len(detail["relations"]), 1)
        self.assertNotIn("candidates", detail)
        self.assertNotIn("relationship_analysis", json.dumps(detail))
        self.assertLess(len(json.dumps(detail, separators=(",", ":"))), 20_000)

    def test_targeter_run_detail_prechecks_every_variable_field_before_decode(
        self,
    ) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        self.assertEqual(UniverseSync(self.database, self.objects).sync().failures, [])
        multibyte_json = json.dumps(
            {"combined_moneyline_volume_usd": 30_000, "padding": "界" * 600_000},
            ensure_ascii=False,
            separators=(",", ":"),
        )
        oversized_values = {
            "event_id": "界" * 600_000,
            "bundle_id": "界" * 600_000,
            "score_components_json": multibyte_json,
            "rejection_reasons_json": multibyte_json,
            "allocation_rejection": "x" * 1_750_001,
            "admission_json": multibyte_json,
            "market_exclusions_json": multibyte_json,
            "eligible_market_ids_json": multibyte_json,
        }
        with sqlite3.connect(self.database.path) as connection:
            original = connection.execute(
                "SELECT * FROM candidate_decisions WHERE run_id = ?", (R1,)
            ).fetchone()
            assert original is not None
            fields = [column[0] for column in connection.execute(
                "SELECT name FROM pragma_table_info('candidate_decisions')"
            )]
            original_values = dict(zip(fields, original, strict=True))

            for field, oversized in oversized_values.items():
                with self.subTest(field=field):
                    connection.execute(
                        f"UPDATE candidate_decisions SET {field} = ? WHERE run_id = ?",
                        (oversized, R1),
                    )
                    connection.commit()
                    with mock.patch("universe.store.json.loads") as decode:
                        with self.assertRaisesRegex(DetailTooLarge, "byte limit"):
                            self.database.targeter_run_detail(R1)
                        decode.assert_not_called()
                    connection.execute(
                        f"UPDATE candidate_decisions SET {field} = ? WHERE run_id = ?",
                        (original_values[field], R1),
                    )
                    connection.commit()

    def test_event_detail_enforces_serialized_byte_budget(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        self.assertEqual(UniverseSync(self.database, self.objects).sync().failures, [])
        with sqlite3.connect(self.database.path) as connection:
            connection.execute(
                "UPDATE venue_events SET title = ?",
                ("x" * 1_750_001,),
            )
        event_id = self.database.list_events()[0][0]["event_id"]
        with self.assertRaisesRegex(DetailTooLarge, "byte limit"):
            self.database.event_detail(event_id)

    def test_selection_context_enforces_every_child_row_bound(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        self.assertEqual(UniverseSync(self.database, self.objects).sync().failures, [])
        with sqlite3.connect(self.database.path) as connection:
            context = connection.execute(
                "SELECT context_sha256 FROM bundle_contexts"
            ).fetchone()[0]
            connection.executemany(
                """INSERT INTO context_relationships(
                       context_sha256, relationship_index, left_market,
                       right_market, relationship, scope, left_venue,
                       right_venue, coverage
                   ) VALUES (?, ?, 'left', 'right', 'IDENTITY', 'series',
                             'kalshi', 'polymarket', 'EXHAUSTIVE')""",
                ((context, index) for index in range(1, 1001)),
            )
        with self.assertRaisesRegex(DetailTooLarge, "child-row limit"):
            self.database.selection_detail(R1, "bundle-1")

    def test_list_events_pages_before_indexed_counts_and_counts_market_versions(
        self,
    ) -> None:
        _publish_run(
            self.objects,
            _selection_report(
                R1,
                G1,
                bundle_id="older-event",
                activation_at="2026-01-01T03:00:00Z",
            ),
        )
        _publish_run(
            self.objects,
            _selection_report(
                R2,
                G2,
                bundle_id="newer-event",
                activation_at="2026-01-01T05:00:00Z",
            ),
        )
        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(result.failures, [])

        with sqlite3.connect(self.database.path) as connection:
            newer_id, activation_ns = connection.execute(
                """SELECT event_id, activation_at_ns FROM umbrella_events
                   ORDER BY activation_at_ns DESC LIMIT 1"""
            ).fetchone()
            connection.execute(
                """INSERT INTO canonical_markets(
                       market_id, market_template_version, outcome_space_version,
                       event_id, canonical_class, market_type, scope,
                       parameters_json, first_seen_run_id, last_seen_run_id
                   )
                   SELECT market_id, 2, outcome_space_version, event_id,
                          canonical_class, market_type, scope, parameters_json,
                          first_seen_run_id, last_seen_run_id
                   FROM canonical_markets
                   WHERE event_id = ?
                   ORDER BY market_id LIMIT 1""",
                (newer_id,),
            )

        first, has_more = self.database.list_events(limit=1)
        self.assertTrue(has_more)
        self.assertEqual(first[0]["event_id"], newer_id)
        self.assertEqual(first[0]["market_count"], 3)

        class CapturingConnection:
            def __init__(self, connection):
                self.connection = connection
                self.statement = ""
                self.parameters = ()

            def execute(self, statement, parameters=()):
                self.statement = statement
                self.parameters = parameters
                return self.connection.execute(statement, parameters)

            def close(self):
                pass

        connection = self.database.connect(readonly=True)
        captured = CapturingConnection(connection)
        try:
            with mock.patch.object(self.database, "connect", return_value=captured):
                second, second_has_more = self.database.list_events(
                    after=(activation_ns, newer_id), limit=1
                )
            plan = [
                row[3]
                for row in connection.execute(
                    "EXPLAIN QUERY PLAN " + captured.statement,
                    captured.parameters,
                )
            ]
        finally:
            connection.close()

        self.assertFalse(second_has_more)
        self.assertEqual(len(second), 1)
        self.assertIn("USING INDEX umbrella_events_activation", "\n".join(plan))
        for index in (
            "venue_events_event",
            "canonical_markets_event",
            "selected_market_occurrences_event",
        ):
            self.assertIn(f"USING COVERING INDEX {index}", "\n".join(plan))

    def test_public_list_limits_are_bounded(self) -> None:
        application = UniverseApplication(self.database)
        for path in (
            "/v1/runs?limit=101",
            "/v1/selections?limit=101",
            "/v1/bundles?limit=101",
            "/v1/events?limit=101",
        ):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    application.get(path)

    def test_fresh_database_uses_canonical_schema_v4(self) -> None:
        with sqlite3.connect(self.database.path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 4)
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        self.assertTrue(
            {
                "umbrella_events",
                "venue_events",
                "canonical_markets",
                "venue_markets",
                "candidate_decisions",
                "selected_market_occurrences",
                "relations",
                "relation_members",
            }
            <= tables
        )
        self.assertNotIn("cadence_runs", tables)

    def test_initialize_requires_wiping_pre_market_universe_database(self) -> None:
        legacy = self.root / "legacy.sqlite3"
        with sqlite3.connect(legacy) as connection:
            connection.execute("CREATE TABLE legacy_state (value TEXT)")
            connection.execute("PRAGMA user_version = 1")
        with self.assertRaisesRegex(
            EvidenceConflict, "run backfill from the immutable archive"
        ):
            UniverseStore(legacy).initialize()

    def test_initialize_rejects_v3_schema_with_rebuild_instruction(self) -> None:
        previous = self.root / "schema-v3.sqlite3"
        with sqlite3.connect(previous) as connection:
            connection.execute("CREATE TABLE previous_state (value TEXT)")
            connection.execute("PRAGMA user_version = 3")
        with self.assertRaisesRegex(
            EvidenceConflict, "run backfill from the immutable archive"
        ):
            UniverseStore(previous).initialize()

    def test_initialize_rejects_modified_v4_with_rebuild_instruction(self) -> None:
        with sqlite3.connect(self.database.path) as connection:
            connection.execute("CREATE TABLE unexpected_state (value TEXT)")
        with self.assertRaisesRegex(
            EvidenceConflict, "run backfill from the immutable archive"
        ):
            self.database.initialize()

    def test_market_projection_rejects_non_finite_selection_evidence(self) -> None:
        report = _selection_report(R1, G1)
        report["selection"]["targets"]["kalshi"][0]["continuity_score"] = float("inf")
        _publish_run(self.objects, report)
        result = UniverseSync(self.database, self.objects).sync()
        self.assertEqual(result.ingested, 0)
        self.assertIn("continuity_score is invalid", result.failures[0])

    def test_relation_member_must_belong_to_candidate_markets(self) -> None:
        report = _selection_report(R1, G1)
        relationship = report["candidates"][0]["relationship_analysis"][
            "relationships"
        ][0]
        relationship["left"] = "kalshi:unrelated-market#claim=0"
        _publish_run(self.objects, report)

        result = UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(result.ingested, 0)
        self.assertEqual(len(result.failures), 1)
        self.assertIn("is not a candidate market", result.failures[0])

    def test_market_projection_identity_is_checked_on_reingestion(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        self.assertEqual(UniverseSync(self.database, self.objects).sync().failures, [])
        with sqlite3.connect(self.database.path) as connection:
            connection.execute(
                "UPDATE universe_run_projections SET projection_sha256 = ? WHERE run_id = ?",
                ("f" * 64, R1),
            )
        with self.assertRaisesRegex(EvidenceConflict, "market projection conflicts"):
            UniverseSync(self.database, self.objects).audit_run(R1)

    def test_rejects_legacy_report_and_manifest_versions(self) -> None:
        report = _selection_report(R1, G1)
        report["report_version"] = 2
        _publish_run(self.objects, report)
        report_result = UniverseSync(self.database, self.objects).sync()
        self.assertIn("Targeter v3", report_result.failures[0])

        other = LocalObjectStore(
            self.root / "legacy-manifest",
            store_id="legacy",
            durability=INDEPENDENT,
        )
        _publish_run(other, _selection_report(R2, G2), manifest_version=1)
        manifest_result = UniverseSync(self.database, other).sync()
        self.assertIn("not version 2", manifest_result.failures[0])

    def test_local_and_authoritative_audits_detect_projection_mutation(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        result = UniverseSync(self.database, self.objects).sync()
        self.assertEqual(result.failures, [])
        self.assertTrue(self.database.audit_run(R1)["ok"])
        self.assertTrue(UniverseSync(self.database, self.objects).audit_run(R1)["ok"])

        with sqlite3.connect(self.database.path) as connection:
            connection.execute(
                "UPDATE context_participants SET name = 'Changed' WHERE position = 0"
            )
        audit = self.database.audit_run(R1)
        assert audit is not None
        self.assertFalse(audit["ok"])
        self.assertFalse(audit["contexts_ok"])
        with self.assertRaisesRegex(EvidenceConflict, "failed audit"):
            UniverseSync(self.database, self.objects).audit_run(R1)

    def test_query_filters_event_and_selection_times_independently(self) -> None:
        _publish_run(
            self.objects,
            _selection_report(
                R1,
                G1,
                bundle_id="early-event",
                activation_at="2026-01-01T03:00:00Z",
            ),
        )
        _publish_run(
            self.objects,
            _selection_report(
                R2,
                G2,
                bundle_id="late-event",
                activation_at="2026-01-01T05:00:00Z",
            ),
        )
        UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        app = UniverseApplication(self.database)

        status, event_page = app.get(
            "/v1/selections?activation_start=2026-01-01T04:00:00Z"
            "&activation_end=2026-01-01T06:00:00Z"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            [row["bundle_id"] for row in event_page["selections"]], ["late-event"]
        )
        status, selected_page = app.get(
            "/v1/selections?selected_start=2026-01-01T00:05:00Z"
            "&selected_end=2026-01-01T00:15:00Z&sort=selected"
        )
        self.assertEqual(status, 200)
        self.assertEqual(
            [row["bundle_id"] for row in selected_page["selections"]], ["late-event"]
        )

    def test_api_pagination_history_detail_and_removed_raw_routes(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        _publish_run(self.objects, _selection_report(R2, G2))
        UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        app = UniverseApplication(self.database)
        status, bundles = app.get("/v1/bundles?limit=10")
        self.assertEqual(status, 200)
        self.assertEqual(len(bundles["bundles"]), 1)
        self.assertEqual(bundles["bundles"][0]["bundle_id"], "bundle-1")
        self.assertEqual(bundles["bundles"][0]["latest_run_id"], R2)
        self.assertEqual(bundles["bundles"][0]["occurrence_count"], 2)
        self.assertEqual(bundles["bundles"][0]["participants"], ["Alpha", "Beta"])
        self.assertEqual(bundles["bundles"][0]["venues"], ["kalshi", "polymarket"])
        self.assertEqual(bundles["bundles"][0]["target_count"], 2)
        self.assertEqual(bundles["bundles"][0]["lifecycle"], "active")
        self.assertIsNone(bundles["next_cursor"])
        _status, first = app.get("/v1/bundles/bundle-1/history?limit=1")
        self.assertEqual(len(first["selections"]), 1)
        self.assertIsNotNone(first["next_cursor"])
        _status, second = app.get(
            "/v1/bundles/bundle-1/history?limit=1&cursor=" + first["next_cursor"]
        )
        self.assertEqual(len(second["selections"]), 1)
        self.assertNotEqual(
            first["selections"][0]["run_id"], second["selections"][0]["run_id"]
        )
        status, detail = app.get(f"/v1/runs/{R1}/selections/bundle-1")
        self.assertEqual(status, 200)
        self.assertEqual(detail["origin"]["run_id"], R1)
        self.assertIn("report_key", detail["source"])
        self.assertEqual(app.get("/v1/targeter/cadence")[0], 404)
        status, events = app.get("/v1/events?limit=10")
        self.assertEqual(status, 200)
        self.assertEqual(len(events["events"]), 1)
        event_id = events["events"][0]["event_id"]
        status, event = app.get(f"/v1/events/{event_id}")
        self.assertEqual(status, 200)
        self.assertEqual(len(event["venue_events"]), 2)
        self.assertEqual(len(event["markets"]), 2)
        market_id = event["markets"][0]["market_id"]
        status, market = app.get(f"/v1/markets/{market_id}")
        self.assertEqual(status, 200)
        self.assertEqual(market["market"]["event_id"], event_id)
        relation_id = event["relations"][0]["relation_id"]
        status, relation = app.get(f"/v1/relations/{relation_id}")
        self.assertEqual(status, 200)
        self.assertEqual(relation["observations"][0]["event_id"], event_id)
        self.assertEqual(len(relation["members"]), 2)
        status, relation_types = app.get("/v1/relationship-types")
        self.assertEqual(status, 200)
        self.assertIn(
            "MUTUAL_EXCLUSION",
            {item["type"] for item in relation_types["types"]},
        )
        self.assertEqual(app.get("/v1/segments")[0], 404)
        with sqlite3.connect(self.database.path) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        self.assertFalse(
            {"segment_receipts", "control_records", "connection_epochs"} & tables
        )

    def test_bundle_summary_pagination_is_newest_first(self) -> None:
        _publish_run(
            self.objects,
            _selection_report(R1, G1, bundle_id="bundle-older"),
        )
        _publish_run(
            self.objects,
            _selection_report(R2, G2, bundle_id="bundle-newer"),
        )
        UniverseSync(self.database, self.objects).sync_range(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 2, tzinfo=timezone.utc),
        )
        app = UniverseApplication(self.database)

        status, first = app.get("/v1/bundles?limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(first["bundles"][0]["bundle_id"], "bundle-newer")
        self.assertIsNotNone(first["next_cursor"])

        status, second = app.get(
            "/v1/bundles?limit=1&cursor=" + first["next_cursor"]
        )
        self.assertEqual(status, 200)
        self.assertEqual(second["bundles"][0]["bundle_id"], "bundle-older")
        self.assertIsNone(second["next_cursor"])

    def test_status_staleness_uses_latest_archived_run_time(self) -> None:
        _publish_run(self.objects, _empty_report(R1, G1))
        UniverseSync(self.database, self.objects).sync()
        generated_ns = int(
            datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc).timestamp()
            * 1_000_000_000
        )
        fresh = self.database.status(now_ns=generated_ns + 3_599_000_000_000)
        stale = self.database.status(now_ns=generated_ns + 3_600_000_000_000)
        self.assertFalse(fresh["latest_run"]["stale"])
        self.assertTrue(stale["latest_run"]["stale"])

    def test_backup_is_independently_readable(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        UniverseSync(self.database, self.objects).sync()
        backup = self.database.backup(self.root / "backups" / "universe.sqlite3")
        with sqlite3.connect(backup) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM targeter_runs").fetchone()[0],
                1,
            )
        digest, length = file_sha256(backup)
        self.assertEqual(len(digest), 64)
        self.assertGreater(length, 0)

    def test_stale_retrieval_cleanup_keeps_live_process_directories(self) -> None:
        stale = self.root / "archive-retrieval-999999-dead"
        live = self.root / f"archive-json-{os.getpid()}-live"
        stale.mkdir()
        live.mkdir()
        old = time.time() - 25 * 60 * 60
        os.utime(stale, (old, old))
        os.utime(live, (old, old))

        removed = cleanup_stale_retrieval_directories(
            self.root, older_than_seconds=24 * 60 * 60
        )

        self.assertEqual(removed, 1)
        self.assertFalse(stale.exists())
        self.assertTrue(live.exists())

    def test_config_has_explicit_optional_backfill_range(self) -> None:
        path = self.root / "event-universe.json"
        path.write_text(
            json.dumps(
                {
                    "event_universe_config_version": 1,
                    "database_path": "database.sqlite3",
                    "api": {"host": "127.0.0.1", "port": 8080},
                    "backfill": {
                        "temporary_directory": "tmp",
                        "generated_start": G1,
                        "generated_end": G2,
                    },
                    "backup": {"directory": "backup", "object_prefix": "universe"},
                }
            )
        )
        config = load_config(path)
        self.assertEqual(
            config.backfill.generated_start.isoformat(),
            "2026-01-01T00:00:00.000001+00:00",
        )
        self.assertEqual(
            config.backfill.generated_end.isoformat(),
            "2026-01-01T00:10:00.000002+00:00",
        )
        with mock.patch("universe.config.build_store", return_value=self.objects) as build:
            self.assertIs(config.object_store(), self.objects)
        build.assert_called_once_with((config.database_path.parent,))

        document = json.loads(path.read_text())
        document["backfill"]["extra"] = True
        path.write_text(json.dumps(document))
        with self.assertRaises(UniverseConfigError):
            load_config(path)


if __name__ == "__main__":
    unittest.main()
