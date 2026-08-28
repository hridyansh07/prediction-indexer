from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from archive.storage.base import JSON_CONTENT_TYPE
from encoder import DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version, stored_identity_of
from targeter.v2.domain import CatalogSnapshot
from targeter.v2.registry import load_strategy
from targeter.v2.run import run_shadow
from targeter.v2.run_archive import archive_run
from tests.test_targeter_v2 import NOW, STRATEGY_PATH, snapshot
from universe.api import UniverseApplication
from universe.backfill import backfill_targeter_history
from universe.cadence import CadenceProjectionError, project_cadence_run
from universe.config import UniverseConfigError, load_config
from universe.projection import (
    ProjectionError,
    project_bundle_retirements,
    project_selected_bundles,
)
from universe.store import EvidenceConflict, UniverseStore, file_sha256
from universe.sync import UniverseSync

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
    return {
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
    manifest = {
        "targeter_run_manifest_version": manifest_version,
        "run_id": run_id,
        "generated_at": report["generated_at"],
        "input_complete": report["input_complete"],
        "files": [report_record],
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
        self.assertEqual(self.database.status()["counts"]["bundle_contexts"], 1)

    def test_retained_selection_fails_closed_on_origin_drift(self) -> None:
        origin = _publish_run(self.objects, _selection_report(R1, G1))
        report = _retained_report(R2, G2, origin)
        report["continuity"]["bundles"][0]["origin_report_sha256"] = "f" * 64
        _publish_run(self.objects, report)

        result = UniverseSync(self.database, self.objects).sync(
            now=datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc)
        )

        self.assertEqual(result.ingested, 0)
        self.assertEqual(len(result.failures), 1)
        self.assertIn("report identity disagrees", result.failures[0])
        self.assertEqual(self.database.status()["counts"]["targeter_runs"], 0)

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
        cadence = self.database.cadence_snapshot()
        incomplete = next(run for run in cadence["runs"] if not run["input_complete"])
        self.assertEqual(incomplete["counts"]["selected"], 0)
        self.assertEqual(incomplete["selected_targets"], {})
        self.assertEqual(incomplete["selections"], [])

    def test_cadence_cache_keeps_exactly_the_newest_five_runs(self) -> None:
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

        cadence = self.database.cadence_snapshot(now_ns=1_767_225_600_000_000_000)
        self.assertEqual(
            [run["run_id"] for run in cadence["runs"]], list(reversed(run_ids[1:]))
        )
        with sqlite3.connect(self.database.path) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM cadence_runs").fetchone()[0],
                5,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM targeter_runs").fetchone()[0],
                6,
            )
        self.assertTrue(UniverseSync(self.database, self.objects).audit_run(run_ids[0])["ok"])
        self.assertEqual(
            [run["run_id"] for run in self.database.cadence_snapshot()["runs"]],
            list(reversed(run_ids[1:])),
        )
        self.assertEqual(self.database.missing_cadence_manifest_keys(), [])

    def test_cadence_projects_operational_decision_evidence(self) -> None:
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

        run = self.database.cadence_snapshot()["runs"][0]
        self.assertEqual(run["counts"]["candidates"], 1)
        self.assertEqual(run["counts"]["selected"], 1)
        self.assertEqual(
            run["catalogs"][0]["classification_diagnostics_by_code"],
            {"unknown_product": 2},
        )
        self.assertEqual(run["discovery_failures"], {"limitless": "timeout"})
        self.assertEqual(
            run["candidates"][0]["admission"]["combined_moneyline_volume_usd"],
            30_000,
        )
        self.assertEqual(run["diagnostics"]["continuity"], ["pointer unavailable"])
        self.assertNotIn("payload_json", run)
        self.assertNotIn("payload_sha256", run)
        self.assertEqual(
            run["selected_targets"]["kalshi"][0]["continuity_score"],
            10.0,
        )

    def test_initialize_upgrades_an_existing_v1_database(self) -> None:
        legacy = self.root / "legacy.sqlite3"
        with sqlite3.connect(legacy) as connection:
            connection.executescript(
                (Path(__file__).parents[1] / "universe/schema/v1.sql").read_text()
            )
            connection.execute("PRAGMA user_version = 1")
        upgraded = UniverseStore(legacy)
        upgraded.initialize()
        with sqlite3.connect(legacy) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertIsNotNone(
                connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'cadence_runs'"
                ).fetchone()
            )

    def test_initialize_recovers_complete_v1_schema_with_version_zero(self) -> None:
        legacy = self.root / "interrupted-bootstrap.sqlite3"
        with sqlite3.connect(legacy) as connection:
            connection.executescript(
                (Path(__file__).parents[1] / "universe/schema/v1.sql").read_text()
            )
        UniverseStore(legacy).initialize()
        with sqlite3.connect(legacy) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 2)

    def test_initialize_recovers_existing_valid_cadence_table(self) -> None:
        for interrupted_version in (0, 1):
            with self.subTest(interrupted_version=interrupted_version):
                legacy = self.root / f"interrupted-v2-{interrupted_version}.sqlite3"
                with sqlite3.connect(legacy) as connection:
                    connection.executescript(
                        (Path(__file__).parents[1] / "universe/schema/v1.sql").read_text()
                    )
                    connection.execute(f"PRAGMA user_version = {interrupted_version}")
                    connection.execute(
                        """CREATE TABLE cadence_runs (
                               run_id TEXT PRIMARY KEY REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
                               generated_at_ns INTEGER NOT NULL,
                               projection_version INTEGER NOT NULL CHECK(projection_version = 1),
                               payload_json TEXT NOT NULL,
                               payload_sha256 TEXT NOT NULL
                           ) STRICT"""
                    )
                UniverseStore(legacy).initialize()
                with sqlite3.connect(legacy) as connection:
                    self.assertEqual(
                        connection.execute("PRAGMA user_version").fetchone()[0], 2
                    )
                    self.assertIsNotNone(
                        connection.execute(
                            "SELECT 1 FROM sqlite_master WHERE type = 'index' "
                            "AND name = 'cadence_runs_generated'"
                        ).fetchone()
                    )

    def test_initialize_rejects_incomplete_bootstrap_and_invalid_cadence(self) -> None:
        for version in (0, 1):
            incomplete = self.root / f"incomplete-{version}.sqlite3"
            with sqlite3.connect(incomplete) as connection:
                connection.execute("CREATE TABLE targeter_runs (run_id TEXT PRIMARY KEY)")
                connection.execute(f"PRAGMA user_version = {version}")
            with self.assertRaisesRegex(EvidenceConflict, "invalid Event Universe v1"):
                UniverseStore(incomplete).initialize()

        invalid = self.root / "invalid-cadence.sqlite3"
        with sqlite3.connect(invalid) as connection:
            connection.executescript(
                (Path(__file__).parents[1] / "universe/schema/v1.sql").read_text()
            )
            connection.execute("PRAGMA user_version = 1")
            connection.execute("CREATE TABLE cadence_runs (run_id TEXT PRIMARY KEY)")
        with self.assertRaisesRegex(EvidenceConflict, "invalid schema"):
            UniverseStore(invalid).initialize()

        malformed = self.root / "malformed-cadence.sqlite3"
        with sqlite3.connect(malformed) as connection:
            connection.executescript(
                (Path(__file__).parents[1] / "universe/schema/v1.sql").read_text()
            )
            connection.execute("PRAGMA user_version = 1")
            connection.execute(
                """CREATE TABLE cadence_runs (
                       run_id TEXT PRIMARY KEY,
                       generated_at_ns TEXT,
                       projection_version TEXT,
                       payload_json TEXT,
                       payload_sha256 TEXT
                   )"""
            )
        with self.assertRaisesRegex(EvidenceConflict, "invalid schema"):
            UniverseStore(malformed).initialize()

    def test_cadence_projection_rejects_semantically_invalid_fields(self) -> None:
        mutations = (
            ("eligible string", lambda report: report["candidates"][0].update(eligible="false")),
            (
                "catalog count string",
                lambda report: report.update(
                    catalogs=[
                        {
                            "venue": "kalshi",
                            "complete": True,
                            "events": "1",
                            "markets": 1,
                            "requests": 1,
                            "diagnostics": [],
                        }
                    ]
                ),
            ),
            (
                "invalid continuity score",
                lambda report: report["selection"]["targets"]["kalshi"][0].update(
                    continuity_score=float("inf")
                ),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                report = _selection_report(R1, G1)
                mutate(report)
                with self.assertRaises(CadenceProjectionError):
                    project_cadence_run(report)

    def test_cadence_snapshot_revalidates_cached_payload(self) -> None:
        _publish_run(self.objects, _selection_report(R1, G1))
        self.assertEqual(UniverseSync(self.database, self.objects).sync().failures, [])
        with sqlite3.connect(self.database.path) as connection:
            payload = json.loads(
                connection.execute(
                    "SELECT payload_json FROM cadence_runs WHERE run_id = ?", (R1,)
                ).fetchone()[0]
            )
            payload["candidates"][0]["eligible"] = "false"
            payload["input_complete"] = False
            encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            connection.execute(
                "UPDATE cadence_runs SET payload_json = ?, payload_sha256 = ? WHERE run_id = ?",
                (encoded, hashlib.sha256(encoded.encode()).hexdigest(), R1),
            )
        with self.assertRaisesRegex(CadenceProjectionError, "fields are invalid"):
            self.database.cadence_snapshot()

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
        status, cadence = app.get("/v1/targeter/cadence")
        self.assertEqual(status, 200)
        self.assertEqual(cadence["cadence_projection_version"], 1)
        self.assertEqual(cadence["freshness"]["state"], "late")
        self.assertEqual([run["run_id"] for run in cadence["runs"]], [R2, R1])
        self.assertEqual(cadence["runs"][0]["selections"][0]["bundle_id"], "bundle-1")
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
