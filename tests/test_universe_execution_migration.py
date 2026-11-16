"""m2 equivalence and crash boundaries, using only disposable local evidence."""

import contextlib
import hashlib
import json
import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from universe.api import UniverseApplication
from tests import test_event_universe_store as fixtures
from tests.test_universe_execution_terms import publish_terms, target_row
from tests.test_universe_totals_direction import _soccer_totals, _thundertalk
from universe.ingest.migrations import migrate
from universe.ingest.sync import SyncResult, UniverseSync
from universe.store import EvidenceConflict, SCHEMA_PATH, UniverseStore
from universe.tools import rebuild_diff


def historical_schema(version):
    sql = SCHEMA_PATH.read_text()
    sql = re.sub(r"CREATE TABLE market_execution_terms \(.*?\) STRICT;", "", sql, flags=re.S)
    for index in ("market_execution_terms_market", "selected_market_occurrences_bundle"):
        sql = re.sub(rf"CREATE INDEX {index}\s+.*?;", "", sql, flags=re.S)
    if version == 6:
        for table in ("universe_migrations", "universe_run_projection_corrections"):
            sql = re.sub(rf"CREATE TABLE {table} \(.*?\) STRICT;", "", sql, flags=re.S)
        sql = sql.replace("CHECK(projection_version IN (3, 4))", "CHECK(projection_version = 3)")
    return sql


def three_venue_fixture(run_id=fixtures.R1, generated=fixtures.G1):
    report = fixtures._selection_report(run_id, generated)
    candidate = report["candidates"][0]
    candidate["event_refs"].append("limitless:event-c")
    candidate["market_ids"].append("limitless:series")
    candidate["eligible_market_ids"].append("limitless:series")
    report["selection"]["targets"]["limitless"] = [fixtures._target("bundle-1", "limitless", candidate["activation_at"])]
    relations = candidate["relationship_analysis"]["relationships"]
    relations.extend([
        {**relations[0], "right": "limitless:series#claim=0", "right_venue": "limitless"},
        {**relations[0], "left": "limitless:series#claim=0", "left_venue": "limitless"},
    ])
    return report, *fixtures._catalog_rows(report)


class ExecutionMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source, self.dest = self.root / "source.sqlite", self.root / "dest.sqlite"
        self.objects = LocalObjectStore(self.root / "archive", store_id="test", durability=INDEPENDENT)
        self.runs = []

    def seed(self, specs, version=7):
        with closing(sqlite3.connect(self.source)) as c:
            c.executescript(historical_schema(version))
            c.execute(f"PRAGMA user_version = {version}")
        db = UniverseStore(self.source)
        for fixture, terms, compressed in specs:
            report, events, markets = fixture
            source = publish_terms(self.objects, report, terms, compressed=compressed, catalog_rows=(events, markets))
            self.runs.append(source)
            # Reproduce old writers: no execution rows or bundle hash entries,
            # and v6 retains its original market projection v3.
            with contextlib.ExitStack() as stack:
                stack.enter_context(mock.patch.object(UniverseSync, "_read_execution_terms", return_value=()))
                if version == 6:
                    for module in ("universe.derive.market_projection", "universe.store.records", "universe.store.ingest_tx"):
                        stack.enter_context(mock.patch(module + ".MARKET_PROJECTION_VERSION", 3))
                    stack.enter_context(mock.patch("universe.derive.market_projection._corrected_totals_parameters",
                                                  side_effect=lambda source, parameters, **kwargs: parameters))
                result = SyncResult()
                status, _ = UniverseSync(db, self.objects)._ingest_manifest(
                    source["manifest_key"], stack=set(), force=False, dependency=False, result=result)
                self.assertEqual(status, "ingested", result.as_record())

    def upgrade(self, **kwargs):
        return migrate(self.source, self.dest, to=8, code_revision="test-revision", objects=self.objects, **kwargs)

    def fresh(self):
        db = UniverseStore(self.root / "fresh.sqlite")
        db.initialize()
        for source in self.runs:
            result = SyncResult()
            status, _ = UniverseSync(db, self.objects)._ingest_manifest(
                source["manifest_key"], stack=set(), force=False, dependency=False, result=result)
            self.assertEqual(status, "ingested", result.as_record())
        return db

    def assert_equivalent(self, fresh):
        with closing(rebuild_diff._open(self.dest)) as before, closing(rebuild_diff._open(fresh.path)) as after:
            self.assertEqual(rebuild_diff._table_differences(before, after, equivalence=True), {})
            self.assertEqual(UniverseStore._schema_objects(before), UniverseStore._expected_schema())
            self.assertEqual(before.execute("PRAGMA foreign_key_check").fetchall(), [])
        db = UniverseStore(self.dest)
        db.initialize()
        self.assertEqual(db.bundle_outcomes("bundle-1"), fresh.bundle_outcomes("bundle-1"))
        status, document = UniverseApplication(db).get("/v1/bundles/bundle-1/outcomes")
        self.assertEqual(status, 200)
        from replay.outcome_model import validate_document
        validate_document(document, "bundle-1")
        for run in self.runs:
            self.assertTrue(db.audit_run(run["run_id"])["ok"])

    def test_fresh_schema_is_v8_and_m1_v7_has_no_execution_objects(self):
        db = UniverseStore(self.root / "empty.sqlite")
        db.initialize()
        with closing(db.connect(readonly=True)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 8)
        fixture = _thundertalk()
        self.seed([(fixture, {"polymarket": [target_row(fixture[0])]}, False)], version=6)
        migrate(self.source, self.dest, to=7, code_revision="test-revision")
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 7)
            expected = sqlite3.connect(":memory:")
            self.addCleanup(expected.close)
            expected.executescript(historical_schema(7))
            self.assertEqual(UniverseStore._schema_objects(c), UniverseStore._schema_objects(expected))
        with self.assertRaisesRegex(EvidenceConflict, "unsupported Event Universe schema version 7"):
            UniverseStore(self.dest).initialize()
        v7 = self.dest
        self.dest = self.root / "m1-upgraded.sqlite"
        summary = migrate(v7, self.dest, to=8, code_revision="test-revision", objects=self.objects)[0]
        self.assertEqual(summary["migration_id"], "m2-execution-terms")
        self.assertEqual(summary["rows_written"], 1)
        self.assert_equivalent(self.fresh())

    def test_v6_to_v8_equals_fresh_ingestion_all_venues_missing_artifact_and_legacy_run(self):
        first = three_venue_fixture()
        second = three_venue_fixture(fixtures.R2, fixtures.G2)
        third = three_venue_fixture(fixtures.R3, fixtures.G3)
        self.seed([
            (first, {v: [target_row(first[0], v)] for v in ("kalshi", "limitless", "polymarket")}, True),
            (second, {"polymarket": [target_row(second[0], record={"secondsDelay": 3})],
                      "kalshi": [target_row(second[0], "kalshi")]}, False),
            (third, {}, False),
        ], version=6)
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        # Only manifests and target records may be touched, even for metadata.
        head, opened = self.objects.head, self.objects.open_verified
        def allowed(key):
            self.assertTrue(key.endswith("run_manifest.json") or "/target_records_" in key, key)
        def checked_head(key):
            allowed(key)
            return head(key)
        def checked_open(expectation):
            allowed(expectation.key)
            return opened(expectation)
        with mock.patch.object(self.objects, "head", side_effect=checked_head), mock.patch.object(
            self.objects, "open_verified", side_effect=checked_open
        ):
            summaries = self.upgrade()
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), before)
        self.assertEqual([s["migration_id"] for s in summaries], ["m1-totals-direction", "m2-execution-terms"])
        m2 = summaries[-1]
        self.assertEqual(m2["rows_written"], 5)
        self.assertEqual(m2["runs_lacking_artifact"], {"kalshi": 1, "limitless": 2, "polymarket": 1})
        self.assertEqual(m2["by_venue"], {
            "kalshi": {"observed": 2, "null": 2}, "limitless": {"observed": 1, "null": 1},
            "polymarket": {"observed": 2, "null": 0},
        })
        self.assert_equivalent(self.fresh())
        doc = UniverseStore(self.dest).bundle_outcomes("bundle-1")
        self.assertEqual(doc["version"], 2)
        terms = {m["market_id"]: m["execution_terms"] for m in doc["markets"]}
        self.assertEqual(terms["limitless:series"], {"status": "observed", "changes": [
            {"from": fixtures.G1, "taker_delay_ms": None}]})

    def test_totals_m1_then_m2_equals_fresh_v8(self):
        for fixture in (_thundertalk(), _soccer_totals()):
            with self.subTest(venue=fixture[2][0]["venue"]), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                self.source, self.dest = self.root / "source.sqlite", self.root / "dest.sqlite"
                self.objects = LocalObjectStore(self.root / "archive", store_id="t", durability=INDEPENDENT)
                self.runs = []
                self.seed([(fixture, {"polymarket": [target_row(fixture[0])]}, False)], version=6)
                self.upgrade()
                self.assert_equivalent(self.fresh())

    def test_null_zero_absence_and_fk_selected_membership(self):
        specs = []
        values = [{}, {"secondsDelay": None}, {"secondsDelay": -1}, {"secondsDelay": True},
                  {"secondsDelay": 1.0}, {"secondsDelay": "3"}, {"secondsDelay": 0}, {"secondsDelay": 2}]
        for i, record in enumerate(values):
            fixture = three_venue_fixture(f"20260101T00{i:02d}00.000000Z", f"2026-01-01T00:{i:02d}:00Z")
            specs.append((fixture, {"polymarket": [target_row(fixture[0], record=record)]}, False))
        self.seed(specs)
        self.upgrade()
        with closing(UniverseStore(self.dest).connect()) as c:
            self.assertEqual([r[0] for r in c.execute("SELECT taker_delay_ms FROM market_execution_terms ORDER BY run_id")],
                             [None, None, None, None, None, None, 0, 2000])
            self.assertEqual(c.execute("SELECT COUNT(*) FROM market_execution_terms WHERE venue != 'polymarket'").fetchone()[0], 0)
            with self.assertRaises(sqlite3.IntegrityError):
                c.execute("INSERT INTO market_execution_terms VALUES (?, 'polymarket', 'not-selected', 1000, ?)",
                          (self.runs[0]["run_id"], fixtures.G1))

    def test_malformed_identity_and_subscription_mismatches_name_artifact_and_abort(self):
        for field, value in (("record_sha256", "f" * 64), ("run_id", fixtures.R2),
                             ("subscription_ids", ["wrong"]), ("target_id", "polymarket:not-selected"),
                             ("unknown", 1)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                self.root = Path(directory)
                self.source, self.dest = self.root / "source.sqlite", self.root / "dest.sqlite"
                self.objects = LocalObjectStore(self.root / "archive", store_id="t", durability=INDEPENDENT)
                self.runs = []
                fixture = three_venue_fixture()
                row = target_row(fixture[0])
                row[field] = value
                self.seed([(fixture, {"polymarket": [row]}, False)])
                with self.assertRaisesRegex(ValueError, "target_records_polymarket.ndjson"):
                    self.upgrade()
                with closing(sqlite3.connect(self.dest)) as c:
                    self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 7)
                    self.assertFalse(c.execute("SELECT 1 FROM sqlite_master WHERE name='market_execution_terms'").fetchone())

    def test_resume_reuses_verified_runs_and_final_transaction_rolls_back(self):
        first, second = three_venue_fixture(), three_venue_fixture(fixtures.R2, fixtures.G2)
        self.seed([(f, {"polymarket": [target_row(f[0])]}, True) for f in (first, second)])
        opened = self.objects.open_verified
        def interrupted(expectation):
            if fixtures.R2 in expectation.key:
                raise KeyboardInterrupt("interrupted")
            return opened(expectation)
        with mock.patch.object(self.objects, "open_verified", side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt):
                self.upgrade()
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 7)
        with self.assertRaisesRegex(EvidenceConflict, "already exists"):
            self.upgrade()
        with self.assertRaisesRegex(EvidenceConflict, "resume.*identity"):
            migrate(self.source, self.dest, to=8, code_revision="other-code", objects=self.objects, resume=True)
        def no_refetch(expectation):
            self.assertNotIn(fixtures.R1, expectation.key)
            return opened(expectation)
        connect = sqlite3.connect
        class FailVersion(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql == "PRAGMA user_version = 8":
                    raise sqlite3.OperationalError("version failure")
                return super().execute(sql, *args, **kwargs)
        def failing_connect(database, *args, **kwargs):
            if Path(str(database)) == self.dest:
                kwargs["factory"] = FailVersion
            return connect(database, *args, **kwargs)
        with mock.patch.object(self.objects, "open_verified", side_effect=no_refetch), mock.patch(
            "sqlite3.connect", side_effect=failing_connect
        ):
            with self.assertRaisesRegex(sqlite3.OperationalError, "version failure"):
                self.upgrade(resume=True)
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertFalse(c.execute("SELECT 1 FROM sqlite_master WHERE name='market_execution_terms'").fetchone())
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 7)
        with mock.patch.object(self.objects, "open_verified", side_effect=AssertionError("refetch")):
            summary = self.upgrade(resume=True)[0]
        self.assertEqual(summary["rows_written"], 2)
        self.assertEqual(summary["runs_processed"], 2)
        self.assertEqual(summary["runs_lacking_artifact"], {"kalshi": 2, "limitless": 2, "polymarket": 0})
        self.assert_equivalent(self.fresh())

    def test_decoded_identity_mismatch_names_compressed_artifact(self):
        fixture = three_venue_fixture()
        put = fixtures._put
        def mismatched(store, key, payload, **kwargs):
            if key.endswith("run_manifest.json"):
                document = json.loads(payload)
                for item in document["files"]:
                    if item["file"] == "target_records_polymarket.ndjson.zst":
                        item["decoded"]["sha256"] = "f" * 64
                payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
            return put(store, key, payload, **kwargs)
        with mock.patch.object(fixtures, "_put", side_effect=mismatched):
            self.seed([(fixture, {"polymarket": [target_row(fixture[0])]}, True)])
        with self.assertRaisesRegex(EvidenceConflict, "target_records_polymarket.ndjson.zst"):
            self.upgrade()

    def test_command_uses_archive_environment_without_loading_universe_config(self):
        fixture = three_venue_fixture()
        self.seed([(fixture, {"polymarket": [target_row(fixture[0])]}, False)])
        from universe.__main__ import main
        import io
        output = io.StringIO()
        with mock.patch.dict("os.environ", {
            "UNIVERSE_CODE_REVISION": "c" * 40, "ARCHIVE_BACKEND": "local",
            "ARCHIVE_ROOT": str(self.objects.root), "ARCHIVE_GCS_BUCKET": "", "ARCHIVE_DURABILITY": "conformance",
        }), mock.patch("universe.commands.load_config", side_effect=AssertionError("config read")), contextlib.redirect_stdout(output):
            self.assertEqual(main(["migrate", "--to", "8", str(self.source), str(self.dest)]), 0)
        self.assertEqual(json.loads(output.getvalue())["rows_written"], 1)


if __name__ == "__main__":
    unittest.main()
