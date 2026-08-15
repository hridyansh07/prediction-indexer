from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from archive.archiver import Archiver
from archive.archiver.receipt_mirror import mirror_retained_receipts
from archive.archiver.universe import UniverseArtifactError
from archive.common.verify import decode_archived_segment
from archive.storage import INDEPENDENT, LocalObjectStore
from archive.storage.base import JSON_CONTENT_TYPE, NDJSON_CONTENT_TYPE
from encoder import logical_identity_of, stored_identity_of
from splices.common.segment import Record, SegmentWriter
from targeter.targets import Target, target_digest
from targeter.v2.selected_bundles import selected_bundle_rows
from universe.api import UniverseApplication
from universe.backfill import backfill_segment_universe
from universe.config import UniverseConfigError, load_config
from universe.store import EvidenceConflict, UniverseStore
from universe.sync import UniverseSync


HOUR_NS = 3_600_000_000_000
DAY_ONE = 1_767_222_000_000_000_000  # 2025-12-31T23:00:00Z


def _selection_report(run_id: str, generated_at: str) -> dict:
    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "input_complete": True,
        "strategy_version": 3,
        "selection_policy": {
            "pre_event_seconds": 3600,
            "post_start_retention_seconds": 21600,
        },
        "candidates": [
            {
                "bundle_id": "bundle-1",
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
                "activation_at": "2026-01-01T01:00:00Z",
                "capture_start_at": "2026-01-01T00:00:00Z",
                "relationship_analysis": {
                    "relationships": [
                        {
                            "bundle_id": "bundle-1",
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
            },
            {"bundle_id": "rejected", "activation_at": "2026-01-01T02:00:00Z"},
        ],
        "selection": {
            "bundle_ids": ["bundle-1"],
            "targets": {
                "kalshi": [
                    {
                        "target_id": "kalshi:series",
                        "bundle_id": "bundle-1",
                        "canonical_class": "esports.series_moneyline",
                        "subscription_ids": ["K-SERIES"],
                    }
                ],
                "polymarket": [
                    {
                        "target_id": "polymarket:series",
                        "bundle_id": "bundle-1",
                        "canonical_class": "esports.series_moneyline",
                        "subscription_ids": ["pm-yes", "pm-no"],
                    }
                ]
            },
        },
    }


def _ingest_targeter(database: UniverseStore, run_id: str, generated_at: str) -> str:
    rows = selected_bundle_rows(_selection_report(run_id, generated_at))
    source_sha256 = ("a" if run_id.endswith("1Z") else "b") * 64
    index_key = f"targeter-v2/runs/run={run_id}/selected_bundle_index.ndjson.zst"
    return database.ingest_selected_bundle_index(
        source_key=f"targeter-v2/runs/run={run_id}/run_manifest.json",
        source_sha256=source_sha256,
        source_kind="manifest_index",
        manifest_key=f"targeter-v2/runs/run={run_id}/run_manifest.json",
        manifest_sha256=source_sha256,
        index_key=index_key,
        index_sha256="d" * 64,
        index_byte_length=50,
        run_id=run_id,
        generated_at=generated_at,
        input_complete=True,
        rows=rows,
    )


def _control(
    delivery_index: int,
    visible_ns: int,
    epoch: str,
    local_counter: int,
    event: str,
    detail: dict | None = None,
) -> Record:
    payload = json.dumps(
        {"event": event, **(detail or {})}, separators=(",", ":"), sort_keys=True
    )
    document = {
        "envelope_version": 2,
        "delivery_index": delivery_index,
        "record_id": f"pm-{delivery_index}",
        "visible_ns": visible_ns,
        "monotonic_ns": delivery_index * 1_000_000,
        "venue": "polymarket",
        "stream": "process",
        "connection_epoch": epoch,
        "local_counter": local_counter,
        "source_cursor": None,
        "kind": "control",
        "raw_payload": payload,
    }
    line = (json.dumps(document, separators=(",", ":"), sort_keys=True) + "\n").encode()
    return Record(
        line=line,
        visible_ns=visible_ns,
        delivery_index=delivery_index,
        epoch=epoch,
    )


def _write_segment(
    root: Path,
    *,
    start_ns: int,
    index: int,
    segment_id: str,
    records: list[Record],
) -> Path:
    writer = SegmentWriter(
        root,
        "polymarket",
        start_ns,
        segment_seconds=3600,
        segment_index=index,
        segment_id=segment_id,
    )
    writer.write_batch(records)
    writer.seal("boundary")
    return writer.data_path


def _put(store: LocalObjectStore, key: str, payload: bytes, content_type: str):
    identity = stored_identity_of(io.BytesIO(payload))
    store.put_immutable(
        key,
        io.BytesIO(payload),
        identity,
        content_type=content_type,
    )
    return identity


def _publish_targeter_run(
    store: LocalObjectStore, *, include_selected_index: bool = False
) -> str:
    run_id = "20260101T000000.000001Z"
    prefix = f"targeter-v2/runs/date=2026-01-01/run={run_id}"
    report = _selection_report(run_id, "2026-01-01T00:00:00Z")
    report_payload = (
        json.dumps(report, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    report_identity = _put(
        store,
        f"{prefix}/selection_report.json",
        report_payload,
        JSON_CONTENT_TYPE,
    )
    files = [
        {
            "file": "selection_report.json",
            "byte_length": report_identity.byte_length,
            "sha256": report_identity.sha256,
            "content_type": JSON_CONTENT_TYPE,
            "content_encoding": None,
        }
    ]
    if include_selected_index:
        index_name = "selected_bundle_index.ndjson"
        index_payload = b"".join(
            (
                json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode()
            for row in selected_bundle_rows(report)
        )
        index_identity = _put(
            store,
            f"{prefix}/{index_name}",
            index_payload,
            NDJSON_CONTENT_TYPE,
        )
        files.append(
            {
                "file": index_name,
                "content_type": NDJSON_CONTENT_TYPE,
                "content_encoding": None,
                "decoded": logical_identity_of(io.BytesIO(index_payload)).as_record(),
                "stored": index_identity.as_record(),
                "compression": None,
            }
        )
    manifest = {
        "targeter_run_manifest_version": 2,
        "run_id": run_id,
        "generated_at": "2026-01-01T00:00:00Z",
        "input_complete": True,
        "files": files,
    }
    payload = (json.dumps(manifest, separators=(",", ":"), sort_keys=True) + "\n").encode()
    _put(store, f"{prefix}/run_manifest.json", payload, JSON_CONTENT_TYPE)
    return run_id


class UniverseStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.database = UniverseStore(self.root / "universe.sqlite3")
        self.database.initialize()

    def test_schema_contains_no_catalogue_or_json_payload_tables(self) -> None:
        with sqlite3.connect(self.database.path) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type = 'table'"
                )
            }
            columns = {
                f"{table}.{row[1]}"
                for table in tables
                for row in connection.execute(f"PRAGMA table_info({table})")
            }
        self.assertTrue(
            {
                "catalog_events",
                "catalog_markets",
                "target_records",
                "event_bundles",
            }.isdisjoint(tables)
        )
        self.assertFalse(any("json" in column.lower() for column in columns))

    def test_indexes_sibling_markets_idempotently_and_exposes_selection(self) -> None:
        self.assertEqual(
            _ingest_targeter(
                self.database, "20260101T000000.000001Z", "2026-01-01T00:00:00Z"
            ),
            "ingested",
        )
        self.assertEqual(
            _ingest_targeter(
                self.database, "20260101T000000.000001Z", "2026-01-01T00:00:00Z"
            ),
            "skipped",
        )
        detail = self.database.bundle_detail("bundle-1")
        assert detail is not None
        self.assertEqual(len(detail["events"]), 2)
        self.assertEqual(
            {item["target_id"]: item["selected"] for item in detail["markets"]},
            {
                "kalshi:series": True,
                "polymarket:map-one": False,
                "polymarket:series": True,
            },
        )
        selected = next(
            item for item in detail["markets"] if item["target_id"] == "polymarket:series"
        )
        self.assertEqual(selected["subscription_ids"], ["pm-no", "pm-yes"])
        self.assertEqual(len(detail["subscriptions"]), 2)
        self.assertIsNone(self.database.bundle_detail("rejected"))
        with self.assertRaises(EvidenceConflict):
            self.database.source_is_complete(
                "targeter-v2/runs/run=20260101T000000.000001Z/run_manifest.json",
                "f" * 64,
            )

    def test_repeated_target_digest_is_reported_as_ambiguous(self) -> None:
        _ingest_targeter(
            self.database, "20260101T000000.000001Z", "2026-01-01T00:00:00Z"
        )
        _ingest_targeter(
            self.database, "20260101T010000.000002Z", "2026-01-01T01:00:00Z"
        )
        detail = self.database.bundle_detail("bundle-1")
        assert detail is not None
        self.assertEqual(detail["run"]["run_id"], "20260101T010000.000002Z")
        self.assertTrue(
            all(
                link["historical_link_status"] == "ambiguous"
                and link["candidate_run_count"] == 2
                for link in detail["subscriptions"]
            )
        )

    def test_syncs_committed_targeter_manifest_and_retries_as_a_noop(self) -> None:
        objects = LocalObjectStore(
            self.root / "objects", store_id="archive", durability=INDEPENDENT
        )
        _publish_targeter_run(objects)
        first = UniverseSync(self.database, objects).sync_targeter()
        self.assertEqual(first.targeter_ingested, 1, first.as_record())
        self.assertEqual(first.failures, [])
        subsequent = UniverseSync(self.database, objects)
        with patch.object(
            subsequent,
            "_read_json",
            side_effect=AssertionError("committed derivative must avoid the source report"),
        ):
            second = subsequent.sync_targeter()
        self.assertEqual(second.targeter_skipped, 1, second.as_record())
        detail = self.database.bundle_detail("bundle-1")
        assert detail is not None
        self.assertEqual(detail["source"]["kind"], "derived_index")
        self.assertIsNone(self.database.bundle_detail("rejected"))
        keys = set(objects.list_keys("targeter-v2/runs/"))
        self.assertTrue(any(key.endswith("/selected_bundle_index.ndjson.zst") for key in keys))
        self.assertTrue(any(key.endswith("/selected_bundle_index.receipt.json") for key in keys))

    def test_sync_prefers_manifest_committed_selected_index(self) -> None:
        objects = LocalObjectStore(
            self.root / "native-objects", store_id="archive", durability=INDEPENDENT
        )
        _publish_targeter_run(objects, include_selected_index=True)
        sync = UniverseSync(self.database, objects)
        with patch.object(
            sync,
            "_read_json",
            side_effect=AssertionError("native index must avoid the selection report"),
        ):
            result = sync.sync_targeter()
        self.assertEqual(result.targeter_ingested, 1, result.as_record())
        self.assertEqual(result.failures, [])
        detail = self.database.bundle_detail("bundle-1")
        assert detail is not None
        self.assertEqual(detail["source"]["kind"], "manifest_index")
        self.assertFalse(
            any(
                key.endswith("/selected_bundle_index.receipt.json")
                for key in objects.list_keys("targeter-v2/runs/")
            )
        )

    def test_lazy_projection_fails_closed_on_existing_artifact_conflict(self) -> None:
        objects = LocalObjectStore(
            self.root / "conflict-objects", store_id="archive", durability=INDEPENDENT
        )
        run_id = _publish_targeter_run(objects)
        prefix = f"targeter-v2/runs/date=2026-01-01/run={run_id}"
        _put(
            objects,
            f"{prefix}/selected_bundle_index.ndjson.zst",
            b"{}\n",
            NDJSON_CONTENT_TYPE,
        )
        result = UniverseSync(self.database, objects).sync_targeter()
        self.assertEqual(result.targeter_ingested, 0)
        self.assertEqual(len(result.failures), 1)
        self.assertIn("invalid metadata", result.failures[0])
        self.assertIsNone(self.database.bundle_detail("bundle-1"))
        self.assertIsNone(
            objects.head(f"{prefix}/selected_bundle_index.receipt.json")
        )

    def test_folds_connection_state_across_segments_and_utc_days(self) -> None:
        digest = target_digest("polymarket", (Target(asset_id="asset-a"),))
        spool = self.root / "spool"
        _write_segment(
            spool,
            start_ns=DAY_ONE,
            index=0,
            segment_id="dayone",
            records=[
                _control(
                    1,
                    DAY_ONE + 1,
                    "epoch-one",
                    1,
                    "connection_opened",
                    {"target_digest": digest, "target_count": 1},
                )
            ],
        )
        _write_segment(
            spool,
            start_ns=DAY_ONE + HOUR_NS,
            index=1,
            segment_id="daytwo",
            records=[
                _control(
                    2,
                    DAY_ONE + HOUR_NS + 1,
                    "epoch-one",
                    2,
                    "subscription_sent",
                    {"target_digest": digest, "target_count": 1},
                ),
                _control(
                    3,
                    DAY_ONE + HOUR_NS + 2,
                    "epoch-one",
                    3,
                    "connection_closed",
                ),
                _control(
                    4,
                    DAY_ONE + HOUR_NS + 3,
                    "epoch-two",
                    1,
                    "connection_opened",
                    {"target_digest": digest, "target_count": 1},
                ),
            ],
        )
        objects = LocalObjectStore(
            self.root / "archive", store_id="archive", durability=INDEPENDENT
        )
        archived = Archiver(spool, objects).sweep()
        self.assertEqual(archived.counts["universe_published"], 2, archived.as_record())
        sync = UniverseSync(self.database, objects).sync_controls()
        self.assertEqual(sync.controls_ingested, 2, sync.as_record())
        epochs = self.database.overlapping_epochs(
            start_ns=DAY_ONE, end_ns=DAY_ONE + 2 * HOUR_NS
        )
        self.assertEqual(len(epochs), 2)
        first, second = epochs
        self.assertEqual(first["send_status"], "subscription_send_completed")
        self.assertEqual(first["venue_acceptance_status"], "unknown")
        self.assertEqual(first["close_status"], "closed_observed")
        self.assertEqual(second["predecessor_epoch"], "epoch-one")
        self.assertEqual(second["observed_end_ns"], None)
        segments = self.database.overlapping_segments(
            start_ns=DAY_ONE, end_ns=DAY_ONE + 2 * HOUR_NS
        )
        self.assertEqual(len(segments), 2)
        _ingest_targeter(
            self.database,
            "20260101T000000.000001Z",
            "2026-01-01T00:00:00Z",
        )
        bundle_segments = self.database.segments_for_bundle("bundle-1")
        assert bundle_segments is not None
        self.assertEqual(
            [segment["segment_id"] for segment in bundle_segments], ["daytwo"]
        )

    def test_api_and_consistent_backup_are_queryable(self) -> None:
        _ingest_targeter(
            self.database, "20260101T000000.000001Z", "2026-01-01T00:00:00Z"
        )
        application = UniverseApplication(self.database)
        status, health = application.get("/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(health["schema_version"], 1)
        status, detail = application.get("/v1/bundles/bundle-1")
        self.assertEqual(status, 200)
        self.assertEqual(len(detail["markets"]), 3)
        status, segments = application.get("/v1/bundles/bundle-1/segments")
        self.assertEqual((status, segments["segments"]), (200, []))
        status, missing = application.get("/v1/bundles/absent")
        self.assertEqual((status, missing["error"]), (404, "bundle not found"))

        backup = self.database.backup(self.root / "backups" / "universe.sqlite3")
        with sqlite3.connect(backup) as connection:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM selected_bundles").fetchone()[0], 1
            )

    def test_backfill_reconstructs_reaped_raw_and_is_resumable(self) -> None:
        spool = self.root / "historical-spool"
        source = _write_segment(
            spool,
            start_ns=DAY_ONE,
            index=0,
            segment_id="historical",
            records=[
                _control(
                    1,
                    DAY_ONE + 1,
                    "epoch-historical",
                    1,
                    "connection_opened",
                    {"target_digest": "historical", "target_count": 1},
                )
            ],
        )
        objects = LocalObjectStore(
            self.root / "historical-archive",
            store_id="historical-archive",
            durability=INDEPENDENT,
        )
        with patch(
            "archive.archiver.service.publish_segment_universe",
            side_effect=UniverseArtifactError("defer sidecar"),
        ):
            archived = Archiver(spool, objects).sweep()
        self.assertEqual(archived.counts["archived"], 1)
        self.assertEqual(archived.counts["universe_failed"], 1)
        mirrored = mirror_retained_receipts(spool, objects)
        self.assertEqual(mirrored.published, 1)
        shutil.rmtree(spool)

        temporary_controls = self.root / "remote-temporary-controls"
        with patch(
            "archive.archiver.universe.decode_archived_segment",
            wraps=decode_archived_segment,
        ) as decoder:
            first = backfill_segment_universe(
                objects=objects,
                database=self.database,
                temp_root=temporary_controls,
                now_ns=lambda: DAY_ONE + 99,
            )
        self.assertEqual(first.failures, [])
        self.assertEqual(first.published, 1)
        decoder.assert_called_once()
        self.assertEqual(list(temporary_controls.iterdir()), [])
        second = backfill_segment_universe(
            objects=objects,
            database=self.database,
            temp_root=temporary_controls,
            now_ns=lambda: DAY_ONE + 100,
        )
        self.assertEqual(second.skipped, 1)
        checkpoint = self.database.checkpoint(
            "raw-universe-backfill:historical-archive"
        )
        assert checkpoint is not None
        self.assertTrue(checkpoint.endswith(".archive-receipt-mirror.json"))
        self.assertFalse(source.exists())

    def test_readable_config_expands_environment_without_cli_arguments(self) -> None:
        config_path = self.root / "event-universe.json"
        config_path.write_text(
            json.dumps(
                {
                    "event_universe_config_version": 1,
                    "database_path": "data/universe.sqlite3",
                    "archive": {
                        "bucket": "${TEST_UNIVERSE_BUCKET}",
                        "region": "us-east-1",
                        "expected_owner": "123456789012",
                    },
                    "api": {"host": "127.0.0.1", "port": 8080},
                    "backfill": {"temporary_directory": "data/tmp"},
                    "backup": {
                        "directory": "data/backups",
                        "object_prefix": "event-universe/backups",
                    },
                }
            ),
            encoding="utf-8",
        )
        with patch.dict(os.environ, {"TEST_UNIVERSE_BUCKET": "archive-bucket"}):
            config = load_config(config_path)
        self.assertEqual(config.archive.bucket, "archive-bucket")
        self.assertEqual(config.database_path, self.root / "data/universe.sqlite3")
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(
            UniverseConfigError
        ):
            load_config(config_path)


if __name__ == "__main__":
    unittest.main()
