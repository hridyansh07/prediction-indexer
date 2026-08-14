from __future__ import annotations

import io
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from archive.archiver import Archiver
from archive.archiver.universe import UniverseArtifactError
from archive.storage import INDEPENDENT, LocalObjectStore
from archive.storage.base import JSON_CONTENT_TYPE, NDJSON_CONTENT_TYPE
from encoder import logical_identity_of, stored_identity_of
from splices.common.segment import Record, SegmentWriter
from targeter.targets import Target, target_digest
from universe.api import UniverseApplication
from universe.backfill import backfill_segment_universe, receipt_inventory
from universe.store import EvidenceConflict, UniverseStore
from universe.sync import UniverseSync


HOUR_NS = 3_600_000_000_000
DAY_ONE = 1_767_222_000_000_000_000  # 2025-12-31T23:00:00Z


def _targeter_records(run_id: str, generated_at: str):
    events = [
        {
            "venue": "polymarket",
            "venue_event_id": "event-1",
            "title": "Alpha vs Beta",
            "sport": "esports",
            "game": "counter_strike_2",
            "activation_at": "2026-01-01T01:00:00Z",
        }
    ]
    markets = [
        {
            "venue": "polymarket",
            "venue_event_id": "event-1",
            "venue_market_id": "moneyline",
            "target_id": "polymarket:moneyline",
            "canonical_class": "series_moneyline",
            "market_type": "moneyline",
            "scope": "series",
            "title": "Match winner",
            "subscription_ids": ["asset-a"],
        },
        {
            "venue": "polymarket",
            "venue_event_id": "event-1",
            "venue_market_id": "map-one",
            "target_id": "polymarket:map-one",
            "canonical_class": "map_moneyline",
            "market_type": "moneyline",
            "scope": "map_1",
            "title": "Map one winner",
            "subscription_ids": ["asset-b"],
        },
    ]
    report = {
        "run_id": run_id,
        "generated_at": generated_at,
        "candidates": [
            {
                "bundle_id": "bundle-1",
                "event_refs": ["polymarket:event-1"],
                "market_ids": ["polymarket:moneyline", "polymarket:map-one"],
                "activation_at": "2026-01-01T01:00:00Z",
                "game": "counter_strike_2",
                "confidence": "high",
            }
        ],
        "selection": {
            "bundle_ids": ["bundle-1"],
            "targets": {
                "polymarket": [
                    {
                        "target_id": "polymarket:moneyline",
                        "bundle_id": "bundle-1",
                        "subscription_ids": ["asset-a"],
                    }
                ]
            },
        },
    }
    return events, markets, report


def _record_sha256(record: dict) -> str:
    payload = json.dumps(
        record,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def _ingest_targeter(database: UniverseStore, run_id: str, generated_at: str) -> str:
    events, markets, report = _targeter_records(run_id, generated_at)
    return database.ingest_targeter_run(
        source_key=f"targeter-v2/runs/run={run_id}/run_manifest.json",
        source_sha256=("a" if run_id.endswith("1") else "b") * 64,
        run_id=run_id,
        generated_at=generated_at,
        input_complete=True,
        events=events,
        markets=markets,
        report=report,
        target_records=[
            {
                "version": 1,
                "run_id": run_id,
                "venue": "polymarket",
                "target_id": "polymarket:moneyline",
                "subscription_ids": ["asset-a"],
                "observed_at": generated_at,
                "record_sha256": _record_sha256(
                    {"conditionId": "moneyline", "question": "Match winner"}
                ),
                "record": {"conditionId": "moneyline", "question": "Match winner"},
            }
        ],
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


def _publish_targeter_run(store: LocalObjectStore) -> str:
    run_id = "20260101T000000.000001Z"
    prefix = f"targeter-v2/runs/date=2026-01-01/run={run_id}"
    events, markets, report = _targeter_records(run_id, "2026-01-01T00:00:00Z")
    rows = {
        "catalog_polymarket_events.ndjson": events,
        "catalog_polymarket_markets.ndjson": markets,
        "rule_templates.ndjson": [],
        "rule_drift.ndjson": [],
        "target_records_polymarket.ndjson": [
            {
                "version": 1,
                "run_id": run_id,
                "venue": "polymarket",
                "target_id": "polymarket:moneyline",
                "subscription_ids": ["asset-a"],
                "observed_at": "2026-01-01T00:00:00Z",
                "record_sha256": _record_sha256(
                    {"conditionId": "moneyline", "question": "Match winner"}
                ),
                "record": {"conditionId": "moneyline", "question": "Match winner"},
            }
        ],
    }
    files = []
    for name, records in rows.items():
        payload = b"".join(
            (json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n").encode()
            for record in records
        )
        stored = _put(store, f"{prefix}/{name}", payload, NDJSON_CONTENT_TYPE)
        logical = logical_identity_of(io.BytesIO(payload))
        files.append(
            {
                "file": name,
                "content_type": NDJSON_CONTENT_TYPE,
                "content_encoding": None,
                "decoded": logical.as_record(),
                "stored": stored.as_record(),
                "compression": None,
            }
        )
    report_payload = (
        json.dumps(report, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode()
    report_identity = _put(
        store,
        f"{prefix}/selection_report.json",
        report_payload,
        JSON_CONTENT_TYPE,
    )
    files.append(
        {
            "file": "selection_report.json",
            "byte_length": report_identity.byte_length,
            "sha256": report_identity.sha256,
            "content_type": JSON_CONTENT_TYPE,
            "content_encoding": None,
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
        selected = next(item for item in detail["markets"] if item["selected"])
        self.assertEqual(
            selected["target_record"]["record"]["conditionId"], "moneyline"
        )
        self.assertEqual(len(detail["events"]), 1)
        self.assertEqual(
            {item["target_id"]: item["selected"] for item in detail["markets"]},
            {"polymarket:map-one": False, "polymarket:moneyline": True},
        )
        self.assertEqual(detail["subscriptions"][0]["selection_status"], "selected")
        with self.assertRaises(EvidenceConflict):
            self.database.ingest_targeter_run(
                source_key="targeter-v2/runs/run=20260101T000000.000001Z/run_manifest.json",
                source_sha256="f" * 64,
                run_id="20260101T000000.000001Z",
                generated_at="2026-01-01T00:00:00Z",
                input_complete=True,
                events=[],
                markets=[],
                report={},
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
        link = detail["subscriptions"][0]
        self.assertEqual(link["historical_link_status"], "ambiguous")
        self.assertEqual(link["candidate_run_count"], 2)

    def test_syncs_committed_targeter_manifest_and_retries_as_a_noop(self) -> None:
        objects = LocalObjectStore(
            self.root / "objects", store_id="archive", durability=INDEPENDENT
        )
        _publish_targeter_run(objects)
        first = UniverseSync(self.database, objects).sync_targeter()
        self.assertEqual(first.targeter_ingested, 1, first.as_record())
        self.assertEqual(first.failures, [])
        second = UniverseSync(self.database, objects).sync_targeter()
        self.assertEqual(second.targeter_skipped, 1, second.as_record())
        self.assertIsNotNone(self.database.bundle_detail("bundle-1"))

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
        self.assertEqual(len(detail["markets"]), 2)
        status, missing = application.get("/v1/bundles/absent")
        self.assertEqual((status, missing["error"]), (404, "bundle not found"))

        backup = self.database.backup(self.root / "backups" / "universe.sqlite3")
        with sqlite3.connect(backup) as connection:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM event_bundles").fetchone()[0], 1
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
        source.unlink()

        inventory = receipt_inventory([spool])
        first = backfill_segment_universe(
            receipt_paths=inventory,
            objects=objects,
            database=self.database,
            temp_root=self.root,
            now_ns=lambda: DAY_ONE + 99,
        )
        self.assertEqual(first.failures, [])
        self.assertEqual((first.published, first.reconstructed), (1, 1))
        second = backfill_segment_universe(
            receipt_paths=inventory,
            objects=objects,
            database=self.database,
            temp_root=self.root,
            now_ns=lambda: DAY_ONE + 100,
        )
        self.assertEqual((second.skipped, second.reconstructed), (1, 1))
        self.assertEqual(
            self.database.checkpoint("raw-universe-backfill:historical-archive"),
            str(inventory[0]),
        )


if __name__ == "__main__":
    unittest.main()
