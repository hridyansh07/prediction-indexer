"""Execution terms from small, committed offline target-record artifacts."""

import copy
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from archive.storage import INDEPENDENT, LocalObjectStore
from encoder import encode_stream
from replay.catalog import canonical_sha256, project_record, projection_id, projection_sha256
from tests import test_event_universe_store as fixtures
from universe.ingest.sync import UniverseSync
from universe.store import EvidenceConflict, UniverseStore
from universe.tools import rebuild_diff


def target_row(report, venue="polymarket", *, record=None):
    target = report["selection"]["targets"][venue][0]
    record = {"secondsDelay": 1} if record is None else record
    return {
        "version": 1,
        "run_id": report["run_id"],
        "venue": venue,
        "target_id": target["target_id"],
        "subscription_ids": target["subscription_ids"],
        "observed_at": report["generated_at"],
        "provenance": "captured",
        "projection_id": projection_id(venue),
        "projection_sha256": projection_sha256(venue, record),
        "record_sha256": canonical_sha256(record),
        "record": record,
    }


def publish_terms(store, report, rows, *, compressed=False, raw_payload=None):
    """Extend the shared manifest fixture before its immutable publication."""
    original = fixtures._put

    def put(store, key, payload, **kwargs):
        if key.endswith("/run_manifest.json"):
            manifest = json.loads(payload)
            for venue, records in sorted(rows.items()):
                logical = (
                    raw_payload if raw_payload is not None else
                    b"".join((json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n").encode() for r in records)
                )
                name = f"target_records_{venue}.ndjson"
                if compressed:
                    destination = io.BytesIO()
                    encoded = encode_stream(io.BytesIO(logical), destination)
                    name += ".zst"
                    artifact = destination.getvalue()
                    decoded = encoded.logical.as_record()
                else:
                    artifact = logical
                    decoded = {
                        "sha256": "0" * 64,  # replaced with the exact byte identity
                        "byte_length": len(logical),
                        "line_count": logical.count(b"\n"),
                    }
                identity = original(store, key.rsplit("/", 1)[0] + "/" + name, artifact,
                    content_type="application/x-ndjson", content_encoding="zstd" if compressed else None)
                if not compressed:
                    decoded["sha256"] = identity.sha256
                manifest["files"].append({
                    "file": name,
                    "content_type": "application/x-ndjson",
                    "content_encoding": "zstd" if compressed else None,
                    "decoded": decoded,
                    "stored": identity.as_record(),
                    "compression": fixtures._compression() if compressed else None,
                })
            payload = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
        return original(store, key, payload, **kwargs)

    with patch.object(fixtures, "_put", side_effect=put):
        return fixtures._publish_run(store, report)


class ExecutionTermsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.db = UniverseStore(root / "universe.sqlite")
        self.db.initialize()
        self.objects = LocalObjectStore(root / "archive", store_id="t", durability=INDEPENDENT)
        self.sync = UniverseSync(self.db, self.objects)

    def ingest(self):
        return self.sync.sync_range(
            datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)
        )

    def terms(self, mid="polymarket:series", bundle="bundle-1"):
        return next(m for m in self.db.bundle_outcomes(bundle)["markets"] if m["market_id"] == mid)["execution_terms"]

    def test_missing_artifact_and_missing_row_are_unavailable(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        fixtures._publish_run(self.objects, report)
        self.assertEqual(self.ingest().ingested, 1)
        self.assertEqual(self.db.bundle_outcomes("bundle-1")["version"], 2)
        self.assertEqual(self.terms(), {"status": "unavailable", "changes": []})
        second = fixtures._selection_report(fixtures.R2, fixtures.G2)
        publish_terms(self.objects, second, {"polymarket": []})
        self.assertEqual(self.ingest().ingested, 1)
        self.assertEqual(self.terms(), {"status": "unavailable", "changes": []})

    def test_compressed_terms_and_null_sources_are_observed(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        publish_terms(self.objects, report, {
            "polymarket": [target_row(report)],
            "kalshi": [target_row(report, "kalshi", record={"secondsDelay": 9})],
        }, compressed=True)
        result = self.ingest()
        self.assertEqual(result.ingested, 1, result.as_record())
        self.assertEqual(self.terms(), {"status": "observed", "changes": [{"from": fixtures.G1, "taker_delay_ms": 1000}]})
        self.assertEqual(self.terms("kalshi:series"), {"status": "observed", "changes": [{"from": fixtures.G1, "taker_delay_ms": None}]})
        self.assertEqual(self.terms("polymarket:map-one"), {"status": "unavailable", "changes": []})
        with closing(self.db.connect(readonly=True)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 7)
            self.assertIn("STRICT", c.execute("SELECT sql FROM sqlite_master WHERE name='market_execution_terms'").fetchone()[0])
        self.assertTrue(self.sync.audit_run(fixtures.R1)["ok"])

    def test_invalid_delay_values_stay_observed_null(self):
        for value in ({}, {"secondsDelay": None}, {"secondsDelay": -1}, {"secondsDelay": True}, {"secondsDelay": 1.0}, {"secondsDelay": "3"}):
            with self.subTest(record=value):
                report = fixtures._selection_report(fixtures.R1, fixtures.G1)
                row = target_row(report, record=value)
                from universe.derive.execution_terms import target_record_terms
                self.assertIsNone(target_record_terms(row, run_id=fixtures.R1, venue="polymarket")["taker_delay_ms"])

    def test_zero_is_known_and_limitless_has_no_declared_delay(self):
        from universe.derive.execution_terms import target_record_terms
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        row = target_row(report, record={"secondsDelay": 0})
        self.assertEqual(target_record_terms(row, run_id=fixtures.R1, venue="polymarket")["taker_delay_ms"], 0)
        row.update(venue="limitless", target_id="limitless:market", projection_id="limitless.v1",
                   projection_sha256=projection_sha256("limitless", row["record"]))
        self.assertIsNone(target_record_terms(row, run_id=fixtures.R1, venue="limitless")["taker_delay_ms"])

    def test_change_points_use_selection_runs_and_collapse_in_generated_order(self):
        specs = [
            (fixtures.R1, fixtures.G1, 1),
            (fixtures.R2, fixtures.G2, 1),
            (fixtures.R3, fixtures.G3, None),
            (fixtures.R4, fixtures.G4, 3),
        ]
        for run, generated, delay in reversed(specs):
            report = fixtures._selection_report(run, generated)
            publish_terms(self.objects, report, {"polymarket": [target_row(report, record={"secondsDelay": delay})]})
        self.assertEqual(self.ingest().ingested, 4)
        self.assertEqual(self.terms(), {"status": "observed", "changes": [
            {"from": fixtures.G1, "taker_delay_ms": 1000},
            {"from": fixtures.G3, "taker_delay_ms": None},
            {"from": fixtures.G4, "taker_delay_ms": 3000},
        ]})

    def test_malformed_target_record_fails_whole_run(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        row = target_row(report)
        row["unknown"] = 1
        publish_terms(self.objects, report, {"polymarket": [row]})
        result = self.ingest()
        self.assertEqual(result.failure_count, 1, result.as_record())
        self.assertEqual(result.ingested, 0)
        self.assertIsNone(self.db.latest_run())
        self.assertIn("target record", result.failures[0])

    def test_record_validation_rejects_identity_fields_hashes_and_duplicates(self):
        from universe.derive.execution_terms import ExecutionTermsError, target_record_terms
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        original = target_row(report)
        for field, value in [
            ("version", True), ("run_id", fixtures.R2), ("venue", "kalshi"),
            ("target_id", "kalshi:series"), ("observed_at", "yesterday"),
            ("record_sha256", "f" * 64), ("projection_sha256", "f" * 64),
            ("projection_id", "polymarket.v99"), ("subscription_ids", ["a", "a"]),
            ("record", []), ("provenance", "invented"),
        ]:
            with self.subTest(field=field):
                row = copy.deepcopy(original)
                row[field] = value
                with self.assertRaises(ExecutionTermsError):
                    target_record_terms(row, run_id=fixtures.R1, venue="polymarket")

    def test_legacy_v1_projection_hash_is_verified_without_rewriting(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        row = target_row(report)
        row["projection_id"] = "polymarket.v1"
        projected = project_record("polymarket", row["record"])
        projected.pop("secondsDelay", None)
        row["projection_sha256"] = canonical_sha256(projected)
        publish_terms(self.objects, report, {"polymarket": [row]})
        self.assertEqual(self.ingest().ingested, 1)
        self.assertEqual(self.terms()["changes"][0]["taker_delay_ms"], 1000)

    def test_archive_audit_detects_changed_terms_projection(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        publish_terms(self.objects, report, {"polymarket": [target_row(report)]})
        self.assertEqual(self.ingest().ingested, 1)
        with closing(self.db.connect()) as c, c:
            c.execute("UPDATE market_execution_terms SET taker_delay_ms=999")
        self.assertFalse(self.db.audit_run(fixtures.R1)["ok"])
        with self.assertRaisesRegex(EvidenceConflict, "execution terms"):
            self.sync.audit_run(fixtures.R1)

    def test_change_points_fail_closed_when_observation_times_reverse(self):
        first = fixtures._selection_report(fixtures.R1, fixtures.G1)
        second = fixtures._selection_report(fixtures.R2, fixtures.G2)
        row = target_row(second, record={"secondsDelay": 3})
        row["observed_at"] = "2025-12-31T23:59:59Z"
        publish_terms(self.objects, first, {"polymarket": [target_row(first)]})
        publish_terms(self.objects, second, {"polymarket": [row]})
        self.assertEqual(self.ingest().ingested, 2)
        with self.assertRaisesRegex(EvidenceConflict, "strictly time ordered"):
            self.db.bundle_outcomes("bundle-1")

    def test_fractional_and_nanosecond_observations_preserve_exact_source_times(self):
        specs = [
            (fixtures.R1, fixtures.G1, 1, "2026-01-01T00:00:00.000000001Z"),
            (fixtures.R2, fixtures.G2, 1, "2026-01-01T00:00:00.000000002Z"),
            (fixtures.R3, fixtures.G3, None, "2026-01-01T00:00:00.000000003Z"),
            (fixtures.R4, fixtures.G4, 3, "2026-01-01T00:00:00.5Z"),
        ]
        for run, generated, delay, observed in specs:
            report = fixtures._selection_report(run, generated)
            row = target_row(report, record={"secondsDelay": delay})
            row["observed_at"] = observed
            publish_terms(self.objects, report, {"polymarket": [row]})
        result = self.ingest()
        self.assertEqual(result.ingested, 4, result.as_record())
        self.assertEqual(self.terms()["changes"], [
            {"from": specs[0][3], "taker_delay_ms": 1000},
            {"from": specs[2][3], "taker_delay_ms": None},
            {"from": specs[3][3], "taker_delay_ms": 3000},
        ])
        from replay.outcome_model import validate_document
        validate_document(self.db.bundle_outcomes("bundle-1"), "bundle-1")
        with closing(self.db.connect(readonly=True)) as c:
            self.assertEqual([row[0] for row in c.execute("SELECT observed_at FROM market_execution_terms ORDER BY run_id")], [item[3] for item in specs])

    def test_equal_instants_with_different_fraction_renderings_fail_closed(self):
        for run, generated, delay, observed in [
            (fixtures.R1, fixtures.G1, 1, "2026-01-01T00:00:00.5Z"),
            (fixtures.R2, fixtures.G2, 3, "2026-01-01T00:00:00.500000000Z"),
        ]:
            report = fixtures._selection_report(run, generated)
            row = target_row(report, record={"secondsDelay": delay})
            row["observed_at"] = observed
            publish_terms(self.objects, report, {"polymarket": [row]})
        self.assertEqual(self.ingest().ingested, 2)
        with self.assertRaisesRegex(EvidenceConflict, "strictly time ordered"):
            self.db.bundle_outcomes("bundle-1")

    def test_bundle_history_excludes_another_bundles_terms(self):
        first = fixtures._selection_report(fixtures.R1, fixtures.G1)
        second = fixtures._selection_report(fixtures.R2, fixtures.G2)
        second["selection"]["bundle_ids"] = ["bundle-2"]
        second["candidates"][0]["bundle_id"] = "bundle-2"
        for relation in second["candidates"][0]["relationship_analysis"]["relationships"]:
            relation["bundle_id"] = "bundle-2"
        for targets in second["selection"]["targets"].values():
            for target in targets:
                target["bundle_id"] = "bundle-2"
        publish_terms(self.objects, first, {"polymarket": [target_row(first)]})
        publish_terms(self.objects, second, {"polymarket": [target_row(second, record={"secondsDelay": 3})]})
        self.assertEqual(self.ingest().ingested, 2)
        self.assertEqual(self.terms()["changes"], [{"from": fixtures.G1, "taker_delay_ms": 1000}])
        self.assertEqual(self.terms(bundle="bundle-2")["changes"], [{"from": fixtures.G2, "taker_delay_ms": 3000}])

    def test_retained_run_uses_own_terms_and_never_copies_origin_observation(self):
        first = fixtures._selection_report(fixtures.R1, fixtures.G1)
        origin = publish_terms(self.objects, first, {"polymarket": [target_row(first)]})
        retained = fixtures._retained_report(fixtures.R2, fixtures.G2, origin)
        publish_terms(self.objects, retained, {"polymarket": [target_row(retained, record={"secondsDelay": 3})]})
        self.assertEqual(self.ingest().ingested, 2)
        self.assertEqual(self.terms()["changes"], [
            {"from": fixtures.G1, "taker_delay_ms": 1000},
            {"from": fixtures.G2, "taker_delay_ms": 3000},
        ])
        next_run = fixtures._retained_report(fixtures.R3, fixtures.G3, origin)
        fixtures._publish_run(self.objects, next_run)
        self.assertEqual(self.ingest().ingested, 1)
        with closing(self.db.connect(readonly=True)) as c:
            self.assertEqual(c.execute("SELECT COUNT(*) FROM market_execution_terms WHERE run_id=?", (fixtures.R3,)).fetchone()[0], 0)

    def test_artifact_framing_subscriptions_and_order_are_strict(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        row = target_row(report)
        mismatched = {**row, "subscription_ids": ["wrong-token"]}
        cases = [
            ([row, row], None),
            ([mismatched], None),
            ([row], b"{}"),
            ([row], b"\n"),
            ([row], b"{\"version\":1,\"version\":1}\n"),
            ([row], b"{\"value\":NaN}\n"),
        ]
        for i, (rows, raw) in enumerate(cases):
            with self.subTest(case=i):
                root = Path(self.temp.name) / str(i)
                db = UniverseStore(root / "universe.sqlite")
                db.initialize()
                objects = LocalObjectStore(root / "archive", store_id="t", durability=INDEPENDENT)
                publish_terms(objects, report, {"polymarket": rows}, raw_payload=raw)
                result = UniverseSync(db, objects).sync_range(datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC))
                self.assertEqual(result.failure_count, 1, result.as_record())
                self.assertIsNone(db.latest_run())

    def test_terms_growth_obeys_outcomes_response_budget(self):
        from universe.store import DetailTooLarge
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        publish_terms(self.objects, report, {"polymarket": [target_row(report)]})
        self.assertEqual(self.ingest().ingested, 1)
        with patch("universe.store.limits.EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES", 50):
            with self.assertRaises(DetailTooLarge):
                self.db.bundle_outcomes("bundle-1")

    def test_target_artifact_bytes_are_verified_before_ingestion(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        source = publish_terms(self.objects, report, {"polymarket": [target_row(report)]}, compressed=True)
        key = source["manifest_key"].removesuffix("run_manifest.json") + "target_records_polymarket.ndjson.zst"
        # Corrupt disposable conformance-store evidence, leaving its original
        # declared identity intact. No retained/live archive is touched.
        artifact = self.objects._path_for(key)
        contents = artifact.read_bytes()
        artifact.write_bytes(contents[:-1])
        result = self.ingest()
        self.assertEqual(result.failure_count, 1, result.as_record())
        self.assertEqual(result.ingested, 0)
        self.assertIsNone(self.db.latest_run())

    def test_rebuild_diff_reports_observed_and_unavailable_markets(self):
        report = fixtures._selection_report(fixtures.R1, fixtures.G1)
        publish_terms(self.objects, report, {"polymarket": [target_row(report)]})
        self.assertEqual(self.ingest().ingested, 1)
        old = Path(self.temp.name) / "old.sqlite"
        with closing(self.db.connect(readonly=True)) as source, closing(sqlite3.connect(old)) as target:
            source.backup(target)
        with closing(sqlite3.connect(old)) as c, c:
            c.execute("DROP TABLE market_execution_terms")
        with closing(rebuild_diff._open(old)) as before, closing(rebuild_diff._open(self.db.path)) as after:
            report = rebuild_diff.diff(before, after)
        self.assertEqual(report["execution_terms"], {
            "rows_by_venue": {"polymarket": 1},
            "observed": ["polymarket:series"],
            "unavailable": ["kalshi:series", "polymarket:map-one"],
        })


if __name__ == "__main__":
    unittest.main()
