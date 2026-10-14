"""Offline migration contracts on small, hand-authored Targeter shapes."""

import contextlib
import copy
import hashlib
import io
import json
import re
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from tests.test_event_universe_store import G1, G2, G3, R1, R2, R3, _publish_run
from tests.test_universe_totals_direction import (
    KALSHI_TOTAL, LIMITLESS_TOTAL, PM_TOTAL, _soccer_totals, _thundertalk,
)
from universe.derive import market_projection
from universe.derive.projection import project_selected_bundles
from universe.ingest.sync import UniverseSync, _complete_context
from universe.store import EvidenceConflict, SCHEMA_PATH, UniverseStore


def _v6_schema():
    sql = SCHEMA_PATH.read_text()
    for table in ("universe_migrations", "universe_run_projection_corrections"):
        sql = re.sub(rf"CREATE TABLE {table} \(.*?\) STRICT;", "", sql, flags=re.S)
    sql = sql.replace("CHECK(projection_version IN (3, 4))", "CHECK(projection_version = 3)")
    return sql.replace("CHECK(projection_version = 4)", "CHECK(projection_version = 3)")


def _ingest(db, fixture, *, v3=False):
    report, events, markets = fixture
    with contextlib.ExitStack() as stack:
        if v3:
            for module in ("universe.derive.market_projection", "universe.store.records",
                           "universe.store.ingest_tx"):
                stack.enter_context(mock.patch(module + ".MARKET_PROJECTION_VERSION", 3))
            stack.enter_context(mock.patch(
                "universe.derive.market_projection._corrected_totals_parameters",
                side_effect=lambda source, parameters, **kwargs: parameters,
            ))
        projection = market_projection.project_market_universe(
            report, catalog_events=events, catalog_markets=markets,
        )
        # v6 has no correction table; an empty correction list never writes it.
        return db.ingest_run(
            run_id=report["run_id"], generated_at=report["generated_at"],
            input_complete=True, report_version=3, strategy_version=3,
            manifest_key=f"targeter-v2/runs/date=2026-01-01/run={report['run_id']}/run_manifest.json",
            manifest_sha256="a" * 64, manifest_byte_length=1,
            report_key=f"runs/{report['run_id']}/selection_report.json",
            report_sha256="b" * 64, report_byte_length=1,
            report_decoded_sha256="b" * 64, report_decoded_byte_length=1,
            market_projection=projection, occurrences=[
                {**{key: row[key] for key in (
                    "run_id", "bundle_id", "occurrence_kind", "origin_run_id",
                    "continuity_selected", "continuity_disposition",
                )}, "context": _complete_context(row)}
                for row in project_selected_bundles(report)
            ],
            retirements=[],
        )


def _rows(path, table):
    with closing(sqlite3.connect(path)) as c:
        c.row_factory = sqlite3.Row
        return [dict(row) for row in c.execute(f"SELECT * FROM {table} ORDER BY 1, 2")]


class MigrationsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source.sqlite3"
        self.dest = self.root / "dest.sqlite3"
        with closing(sqlite3.connect(self.source)) as c:
            c.executescript(_v6_schema())
            c.execute("PRAGMA user_version = 6")
        self.fixture = _thundertalk()
        _ingest(UniverseStore(self.source), self.fixture, v3=True)

    def migrate(self, to=7, **kwargs):
        from universe.ingest.migrations import migrate
        return migrate(self.source, self.dest, to=to, code_revision="test-revision", **kwargs)

    def unselected_source(self, fixture):
        fixture = copy.deepcopy(fixture)
        fixture[0]["selection"] = {
            "bundle_ids": [], "bundle_count": 0,
            "targets": {"kalshi": [], "polymarket": [], "limitless": []},
        }
        fixture[0]["candidates"][0]["eligible"] = False
        self.source.unlink()
        with closing(sqlite3.connect(self.source)) as c:
            c.executescript(_v6_schema())
            c.execute("PRAGMA user_version = 6")
        _ingest(UniverseStore(self.source), fixture, v3=True)
        self.assertEqual(_rows(self.source, "selection_occurrences"), [])
        return fixture

    def test_unselected_candidate_uses_umbrella_participants_and_rewrites_claims(self):
        from targeter.v2.relationships import derive_bundle_relationships
        from universe.claims.claim_projection import rebuild_bundle
        fixture = copy.deepcopy(self.fixture)
        candidate = fixture[0]["candidates"][0]
        # A different totals line gives the old under claim relations, but
        # does not share its identity. Preserving edges must keep its endpoint
        # even after the corrected market no longer references that claim.
        other = copy.deepcopy(next(r for r in fixture[2] if r["target_id"] == PM_TOTAL))
        other.update(target_id="polymarket:other-line", venue_market_id="other-line",
                     title="Games Total: O/U 4.5", parameters={"line": 4.5},
                     subscription_ids=["other-over", "other-under"])
        fixture[2].append(other)
        candidate["market_ids"].append(other["target_id"])
        candidate["eligible_market_ids"] = candidate["market_ids"]
        candidate["market_exclusions"] = {PM_TOTAL: ["unsupported_semantics"]}
        with mock.patch("universe.derive.market_projection._corrected_totals_parameters",
                        side_effect=lambda source, parameters, **kwargs: parameters):
            projection = market_projection.project_market_universe(fixture[0], catalog_events=fixture[1], catalog_markets=fixture[2])
        bundle = rebuild_bundle(projection["events"][0], projection["venue_events"], projection["venue_markets"])
        candidate["relationship_analysis"]["relationships"] = [r.as_record() for r in
            derive_bundle_relationships(bundle, excluded_market_ids=frozenset({PM_TOTAL})).relationships]
        fixture = self.unselected_source(fixture)
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        old_relations = _rows(self.source, "claim_relations")
        old_claims = _rows(self.source, "market_claims")
        old_claim = next(r["claim_id"] for r in old_claims
                         if f"{r['venue']}:{r['venue_market_id']}" == KALSHI_TOTAL)
        self.assertEqual(sum(r["claim_id"] == old_claim for r in old_claims), 1)
        self.assertTrue(any(old_claim in (r["left_claim_id"], r["right_claim_id"]) for r in old_relations))
        rebuilt = UniverseStore(self.root / "rebuilt.sqlite3")
        rebuilt.initialize()
        _ingest(rebuilt, fixture)
        summary = self.migrate()[0]
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), before)
        for table in ("venue_markets", "canonical_markets", "market_claims",
                      "universe_run_projection_corrections"):
            self.assertEqual(_rows(self.dest, table), _rows(rebuilt.path, table), table)
        corrected = next(r for r in _rows(self.dest, "market_claims")
                         if f"{r['venue']}:{r['venue_market_id']}" == KALSHI_TOTAL)
        claim = next(r for r in _rows(self.dest, "claim_classes") if r["claim_id"] == corrected["claim_id"])
        self.assertEqual(claim["outcome_key_count"], 18)
        self.assertFalse(any(f"{r['venue']}:{r['venue_market_id']}" == PM_TOTAL
                             for r in _rows(self.dest, "market_claims")))
        self.assertEqual(_rows(self.dest, "claim_relations"), old_relations)
        self.assertIn(old_claim, [r["claim_id"] for r in _rows(self.dest, "claim_classes")])
        self.assertEqual(summary["participants_source"], {"context": 0, "umbrella": 1})
        self.assertEqual(summary["correction_membership_fallback"], [])
        self.assertEqual(summary["relations_not_rederived"], [_rows(self.source, "umbrella_events")[0]["event_id"]])
        self.assertEqual(summary["markets_changed"], {"kalshi": 1, "limitless": 0})
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(UniverseStore._schema_objects(c), UniverseStore._expected_schema())

    def test_contextless_membership_fallback_is_bounded_to_market_span(self):
        fixture = self.unselected_source(self.fixture)
        # Totals absent from both eligible IDs and exclusions have no exact
        # membership record. Candidate runs within their seen span are fallback.
        second = copy.deepcopy(fixture)
        second[0].update(run_id=R2, generated_at=G2)
        second[0]["candidates"][0].update(eligible_market_ids=None, market_exclusions=None)
        _ingest(UniverseStore(self.source), second, v3=True)
        third = copy.deepcopy(fixture)
        third[0].update(run_id=R3, generated_at=G3)
        third[0]["candidates"][0]["market_ids"].remove(KALSHI_TOTAL)
        relations = third[0]["candidates"][0]["relationship_analysis"]["relationships"]
        relations[:] = [r for r in relations if not r["left"].startswith(KALSHI_TOTAL)
                        and not r["right"].startswith(KALSHI_TOTAL)]
        third[2][:] = [r for r in third[2] if r["target_id"] != KALSHI_TOTAL]
        _ingest(UniverseStore(self.source), third, v3=True)
        earlier = copy.deepcopy(third)
        earlier[0].update(run_id="20260101T000000.000000Z", generated_at="2026-01-01T00:00:00Z")
        _ingest(UniverseStore(self.source), earlier, v3=True)
        summary = self.migrate()[0]
        self.assertEqual(summary["correction_membership_fallback"], [KALSHI_TOTAL])
        self.assertEqual([r["run_id"] for r in _rows(self.dest, "universe_run_projection_corrections")], [R1, R2])
        claim = next(r for r in _rows(self.dest, "market_claims") if r["venue"] == "kalshi" and r["venue_market_id"] == KALSHI_TOTAL.split(":", 1)[1])
        self.assertEqual((claim["first_seen_run_id"], claim["last_seen_run_id"]), (R1, R2))

    def test_context_and_umbrella_participants_disagreement_rolls_back(self):
        with closing(sqlite3.connect(self.source)) as c:
            # The title still derives over with either list: disagreement must
            # abort even when the corrected parameters happen to agree.
            c.execute("UPDATE umbrella_events SET participants_json = ?", (json.dumps(["IG", "Different Team"]),))
            c.commit()
        before = hashlib.sha256(self.source.read_bytes()).hexdigest()
        with self.assertRaisesRegex(EvidenceConflict, "participants.*disagree.*" + KALSHI_TOTAL):
            self.migrate()
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).hexdigest(), before)
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 6)

    def test_m1_matches_rebuild_schema_data_and_served_claims_without_writing_source(self):
        # An unselected total is nevertheless a candidate market in its context.
        before = self.source.read_bytes(), self.source.stat().st_mtime_ns
        projections = _rows(self.source, "universe_run_projections")
        rebuilt = UniverseStore(self.root / "rebuilt.sqlite3")
        rebuilt.initialize()
        _ingest(rebuilt, self.fixture)
        summary = self.migrate()[0]
        self.assertEqual((self.source.read_bytes(), self.source.stat().st_mtime_ns), before)
        self.assertEqual(hashlib.sha256(self.source.read_bytes()).digest(), hashlib.sha256(before[0]).digest())
        migrated_projections = _rows(self.dest, "universe_run_projections")
        self.assertEqual([(r["projection_version"], r["projection_sha256"]) for r in migrated_projections],
                         [(r["projection_version"], r["projection_sha256"]) for r in projections])
        self.assertEqual(migrated_projections[0]["projection_row_count"], projections[0]["projection_row_count"] + 1)
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(UniverseStore._schema_objects(c), UniverseStore._expected_schema())
            self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(c.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        for table in ("canonical_markets", "venue_markets", "selected_market_occurrences",
                      "claim_classes", "market_claims", "claim_relations",
                      "universe_run_projection_corrections"):
            self.assertEqual(_rows(self.dest, table), _rows(rebuilt.path, table), table)
        migrated = UniverseStore(self.dest)
        migrated.initialize()
        self.assertEqual(migrated.bundle_outcomes("bundle-1"), rebuilt.bundle_outcomes("bundle-1"))
        self.assertEqual(summary["markets_changed"], {"kalshi": 1, "limitless": 0})
        self.assertEqual(summary["participants_source"], {"context": 1, "umbrella": 0})
        self.assertEqual(summary["relations_not_rederived"], [])
        self.assertEqual(summary["changed"], [{"market": KALSHI_TOTAL, "report_value": "under", "projected_value": "over"}])
        record = _rows(self.dest, "universe_migrations")[0]
        self.assertEqual(record["migration_id"], "m1-totals-direction")
        self.assertEqual(record["from_user_version"], 6)
        self.assertEqual(record["to_user_version"], 7)
        self.assertEqual(record["code_revision"], "test-revision")
        self.assertEqual(json.loads(record["inputs_json"]), {})
        self.assertEqual(json.loads(record["summary_json"]), summary)
        self.assertEqual(migrated.status()["migrations"], ["m1-totals-direction"])
        # Bundle projection verification is independent of the old market hash.
        self.assertEqual(migrated.run_detail(R1)["run_id"], R1)

    def test_limitless_three_or_more_and_correct_controls(self):
        for fixture, changed in ((_soccer_totals(), True),
                                 (_soccer_totals(stored_direction="over"), False),
                                 (_thundertalk(stored_direction="over"), False)):
            with self.subTest(changed=changed, fixture=fixture[2][0]["venue"]):
                self.source.unlink()
                with closing(sqlite3.connect(self.source)) as c:
                    c.executescript(_v6_schema())
                    c.execute("PRAGMA user_version = 6")
                _ingest(UniverseStore(self.source), fixture, v3=True)
                self.dest.unlink(missing_ok=True)
                summary = self.migrate()[0]
                self.assertEqual(len(summary["changed"]), int(changed))
                if changed:
                    row = next(r for r in _rows(self.dest, "venue_markets") if r["venue"] == "limitless")
                    self.assertEqual(json.loads(row["parameters_json"]), {"direction": "over", "line": 2.5, "published_line": 3})

    def test_polymarket_overtake_and_wrong_reported_direction_are_untouched(self):
        with closing(sqlite3.connect(self.source)) as c:
            c.execute("UPDATE venue_markets SET title = 'Overtake Over 3.5 maps', parameters_json = ? WHERE venue = 'polymarket' AND venue_market_id = ?",
                      (json.dumps({"line": 3.5, "direction": "under"}), PM_TOTAL.split(":", 1)[1]))
            c.commit()
        old = [r for r in _rows(self.source, "venue_markets") if r["venue"] == "polymarket"]
        self.migrate()
        self.assertEqual([r for r in _rows(self.dest, "venue_markets") if r["venue"] == "polymarket"], old)

    def test_abort_after_a_correction_rolls_back_all_schema_and_data(self):
        # The first market can be changed before the second fails reconstruction.
        with closing(sqlite3.connect(self.source)) as c:
            c.execute("INSERT INTO umbrella_events SELECT event_id || '-missing', identity_version, identity_activation_date, identity_ordinal + 1, sport, game, topology, activation_at, activation_at_ns, '[]', participant_keys_json, first_seen_run_id, last_seen_run_id FROM umbrella_events")
            c.execute("INSERT INTO venue_events SELECT venue, 'missing-event', event_id || '-missing', title, league, status, source_ref, format, fragment_type, first_seen_run_id, last_seen_run_id FROM venue_events WHERE venue = 'kalshi'")
            c.execute("INSERT INTO venue_markets SELECT venue, 'zz-missing', 'missing-event', event_id || '-missing', market_id, market_template_version, outcome_space_version, canonical_class, market_type, scope, title, parameters_json, subscription_ids_json, outcome_labels_json, status, accepting_orders, rules_hash, rule_template_id, source_ref, created_at, volume_24h, volume_total, volume_total_usd, liquidity, first_seen_run_id, last_seen_run_id FROM venue_markets WHERE venue_market_id = ?", (KALSHI_TOTAL.split(":", 1)[1],))
            c.commit()
        before = _rows(self.source, "venue_markets")
        with self.assertRaisesRegex(EvidenceConflict, "kalshi:zz-missing"):
            self.migrate()
        self.assertEqual(_rows(self.dest, "venue_markets"), before)
        with closing(sqlite3.connect(self.dest)) as c, closing(sqlite3.connect(self.source)) as s:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 6)
            self.assertEqual(UniverseStore._schema_objects(c), UniverseStore._schema_objects(s))

    def test_m1_rejects_mixed_projection_versions_without_advancing_dest(self):
        with closing(sqlite3.connect(self.source)) as c:
            c.execute("PRAGMA ignore_check_constraints = ON")
            c.execute("UPDATE universe_run_projections SET projection_version = 4")
            c.commit()
        with self.assertRaisesRegex(EvidenceConflict, "every.*projection.*3"):
            self.migrate()
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 6)

    def test_missing_participants_names_market_and_aborts(self):
        with closing(sqlite3.connect(self.source)) as c:
            c.execute("DELETE FROM context_participants")
            c.execute("UPDATE umbrella_events SET participants_json = '[]'")
            c.commit()
        with self.assertRaisesRegex(EvidenceConflict, "participants.*" + KALSHI_TOTAL):
            self.migrate()

    def test_no_downward_old_versions_same_file_or_overwrite(self):
        for version, to in ((5, 7), (7, 6), (6, 6), (6, 9)):
            with self.subTest(version=version, to=to):
                with closing(sqlite3.connect(self.source)) as c:
                    c.execute(f"PRAGMA user_version = {version}")
                with self.assertRaises(EvidenceConflict):
                    self.migrate(to)
                self.assertFalse(self.dest.exists())
        with closing(sqlite3.connect(self.source)) as c:
            c.execute("PRAGMA user_version = 6")
        from universe.ingest.migrations import migrate
        with self.assertRaises(EvidenceConflict):
            migrate(self.source, self.source, to=7, code_revision="test")
        self.dest.write_bytes(b"owned by someone else")
        with self.assertRaises(EvidenceConflict):
            self.migrate()
        self.assertEqual(self.dest.read_bytes(), b"owned by someone else")

    def test_v3_requires_m1_and_reingestion_has_exact_conflict(self):
        self.migrate()
        db = UniverseStore(self.dest)
        with self.assertRaisesRegex(EvidenceConflict, "^run " + R1 + " was migrated from projection v3; rebuild to re-ingest it$"):
            _ingest(db, self.fixture)
        with closing(sqlite3.connect(self.dest)) as c:
            c.execute("DELETE FROM universe_migrations")
            c.commit()
        with self.assertRaisesRegex(EvidenceConflict, "projection v3.*m1-totals-direction"):
            db.initialize()

    def test_new_runs_still_write_v4_and_stale_message_names_both_paths(self):
        with self.assertRaisesRegex(EvidenceConflict, "migrate.*backfill"):
            UniverseStore(self.source).initialize()
        self.migrate()
        fixture = copy.deepcopy(self.fixture)
        fixture[0].update(run_id=R2, generated_at=G2)
        _ingest(UniverseStore(self.dest), fixture)
        self.assertEqual([r["projection_version"] for r in _rows(self.dest, "universe_run_projections")], [3, 4])

    def test_command_prints_one_stored_json_summary_and_never_loads_config(self):
        from universe.__main__ import main
        output = io.StringIO()
        with mock.patch.dict("os.environ", {"UNIVERSE_CODE_REVISION": "c" * 40}), mock.patch(
            "universe.commands.load_config", side_effect=AssertionError("config access")
        ), contextlib.redirect_stdout(output):
            self.assertEqual(main(["migrate", "--to", "7", str(self.source), str(self.dest)]), 0)
        summary = json.loads(output.getvalue())
        self.assertEqual(summary, json.loads(_rows(self.dest, "universe_migrations")[0]["summary_json"]))

    def test_copy_reads_committed_wal_without_opening_source_for_writing(self):
        with closing(sqlite3.connect(self.source)) as writer:
            writer.execute("PRAGMA journal_mode = WAL")
            writer.execute("UPDATE venue_markets SET volume_total = 923.0")
            writer.commit()
            before = self.source.read_bytes(), self.source.with_name(self.source.name + "-wal").read_bytes()
            connect = sqlite3.connect
            source_opens = []

            def checked_connect(database, *args, **kwargs):
                if str(self.source) in str(database):
                    self.assertIn("mode=ro", str(database))
                    self.assertTrue(kwargs.get("uri"))
                    source_opens.append(database)
                return connect(database, *args, **kwargs)

            with mock.patch("sqlite3.connect", side_effect=checked_connect):
                self.migrate()
            self.assertTrue(source_opens)
            self.assertEqual((self.source.read_bytes(), self.source.with_name(self.source.name + "-wal").read_bytes()), before)
            self.assertTrue(all(row["volume_total"] == 923.0 for row in _rows(self.dest, "venue_markets")))

    def test_failed_version_bump_rolls_back_and_is_the_last_step_statement(self):
        before = _rows(self.source, "venue_markets")
        connect = sqlite3.connect

        class FailVersion(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql == "PRAGMA user_version = 7":
                    raise sqlite3.OperationalError("injected version failure")
                return super().execute(sql, *args, **kwargs)

        def failing_connect(database, *args, **kwargs):
            if Path(str(database)) == self.dest:
                kwargs["factory"] = FailVersion
            return connect(database, *args, **kwargs)

        with mock.patch("sqlite3.connect", side_effect=failing_connect):
            with self.assertRaisesRegex(sqlite3.OperationalError, "injected version failure"):
                self.migrate()
        self.assertEqual(_rows(self.dest, "venue_markets"), before)
        with closing(connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 6)
            self.assertFalse(c.execute("SELECT 1 FROM sqlite_master WHERE name = 'universe_migrations'").fetchone())

    def test_schema_precondition_and_strict_constraints(self):
        self.migrate()
        with closing(sqlite3.connect(self.dest)) as c:
            for version in (2, 5):
                with self.assertRaises(sqlite3.IntegrityError):
                    c.execute("UPDATE universe_run_projections SET projection_version = ?", (version,))
            for column in ("inputs_json", "summary_json"):
                with self.assertRaises(sqlite3.IntegrityError):
                    c.execute(f"UPDATE universe_migrations SET {column} = 'broken'")
            self.assertEqual(c.execute("PRAGMA table_list('universe_migrations')").fetchone()[-1], 1)
        with closing(sqlite3.connect(self.source)) as c:
            c.execute("CREATE TABLE unexpected(value TEXT)")
        self.dest.unlink()
        with self.assertRaisesRegex(EvidenceConflict, "exact Event Universe v6 schema"):
            self.migrate()

    def test_known_v3_manifest_skips_only_matching_identity(self):
        self.migrate()
        db = UniverseStore(self.dest)
        run = _rows(self.dest, "targeter_runs")[0]
        self.assertTrue(db.known_manifest(run["manifest_key"], run["manifest_sha256"]))
        with self.assertRaises(EvidenceConflict) as conflict:
            db.known_manifest(run["manifest_key"], run["manifest_sha256"], reingest=True)
        self.assertEqual(str(conflict.exception),
                         f"run {run['run_id']} was migrated from projection v3; rebuild to re-ingest it")
        for reingest in (False, True):
            with self.subTest(reingest=reingest):
                with self.assertRaisesRegex(EvidenceConflict, "immutable manifest .* changed identity"):
                    db.known_manifest(run["manifest_key"], "c" * 64, reingest=reingest)

    def test_incremental_sync_skips_migrated_day_but_audit_and_backfill_conflict(self):
        objects = LocalObjectStore(self.root / "objects", store_id="archive", durability=INDEPENDENT)
        sources = []
        for run_id, generated in ((R1, G1), (R2, G2)):
            fixture = copy.deepcopy(self.fixture)
            fixture[0].update(run_id=run_id, generated_at=generated)
            if run_id == R2:
                _ingest(UniverseStore(self.source), fixture, v3=True)
            sources.append(_publish_run(objects, fixture[0], catalog_rows=fixture[1:]))
        with closing(sqlite3.connect(self.source)) as c:
            for source in sources:
                c.execute("UPDATE targeter_runs SET manifest_sha256 = ? WHERE run_id = ?",
                          (source["manifest_sha256"], source["run_id"]))
            c.execute("INSERT INTO event_identity_lineage VALUES (1, '2026-01-01T00:00:00Z', '2026-01-01T00:11:00Z', 'complete')")
            c.commit()
        self.migrate()
        db = UniverseStore(self.dest)
        sync = UniverseSync(db, objects)
        with mock.patch.object(objects, "open_verified", side_effect=AssertionError("unexpected verified object read")):
            result = sync.sync(now=datetime(2026, 1, 1, 1, tzinfo=timezone.utc))
        self.assertEqual((result.discovered, result.skipped, result.ingested), (2, 2, 0))
        self.assertEqual((result.failure_count, result.pending_failures, result.failures), (0, 0, []))
        conflict = "^run " + R1 + " was migrated from projection v3; rebuild to re-ingest it$"
        with self.assertRaisesRegex(EvidenceConflict, conflict):
            sync.audit_run(R1)
        with mock.patch.object(objects, "open_verified", side_effect=AssertionError("unexpected verified object read")):
            backfill = sync.backfill_range(datetime(2026, 1, 1, tzinfo=timezone.utc),
                                           datetime(2026, 1, 1, 0, 11, tzinfo=timezone.utc))
        self.assertEqual((backfill.skipped, backfill.failure_count), (0, 2))
        self.assertEqual(backfill.failures, [
            f"{source['manifest_key']}: EvidenceConflict: run {source['run_id']} "
            "was migrated from projection v3; rebuild to re-ingest it"
            for source in sources
        ])

    def test_m2_stub_preserves_m1_and_explains_missing_contract(self):
        # The base and delegating conversation lack the authoritative §1.2.
        output = []
        with self.assertRaisesRegex(EvidenceConflict, "m2.*EXECUTION_DELAY_SDK_V1.*1.2"):
            self.migrate(8, progress=output.append)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]["migration_id"], "m1-totals-direction")
        with closing(sqlite3.connect(self.dest)) as c:
            self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 7)
        self.assertEqual([r["migration_id"] for r in _rows(self.dest, "universe_migrations")], ["m1-totals-direction"])
        UniverseStore(self.dest).initialize()

    def test_summary_counts_changed_claim_rows_not_insert_attempts(self):
        old = {table: {tuple(row.items()) for row in _rows(self.source, table)}
               for table in ("market_claims", "claim_relations")}
        summary = self.migrate()[0]
        for table, field in (("market_claims", "claims_rewritten"), ("claim_relations", "relations_rewritten")):
            new = {tuple(row.items()) for row in _rows(self.dest, table)}
            self.assertEqual(summary[field], len(old[table] ^ new), field)

    def test_repeated_candidate_runs_preserve_claim_spans_and_exact_corrections(self):
        second = copy.deepcopy(self.fixture)
        second[0].update(run_id=R2, generated_at=G2)
        _ingest(UniverseStore(self.source), second, v3=True)
        rebuilt = UniverseStore(self.root / "rebuilt.sqlite3")
        rebuilt.initialize()
        _ingest(rebuilt, self.fixture)
        _ingest(rebuilt, second)
        self.migrate()
        for table in ("canonical_markets", "claim_classes", "market_claims", "claim_relations",
                      "universe_run_projection_corrections"):
            self.assertEqual(_rows(self.dest, table), _rows(rebuilt.path, table), table)

    def test_corrected_canonical_market_merges_an_existing_later_span(self):
        from targeter.v2.relationships import derive_bundle_relationships
        from universe.claims.claim_projection import rebuild_bundle
        second = copy.deepcopy(self.fixture)
        report, events, markets = second
        report.update(run_id=R2, generated_at=G2)
        other = copy.deepcopy(next(row for row in markets if row["target_id"] == KALSHI_TOTAL))
        other.update(target_id="kalshi:already-correct", venue_market_id="already-correct", parameters={"line": 3.5, "direction": "over"})
        markets.append(other)
        report["candidates"][0]["market_ids"].append(other["target_id"])
        with mock.patch("universe.derive.market_projection._corrected_totals_parameters", side_effect=lambda source, parameters, **kwargs: parameters):
            projection = market_projection.project_market_universe(report, catalog_events=events, catalog_markets=markets)
        bundle = rebuild_bundle(projection["events"][0], projection["venue_events"], projection["venue_markets"])
        report["candidates"][0]["relationship_analysis"]["relationships"] = [row.as_record() for row in derive_bundle_relationships(bundle).relationships]
        _ingest(UniverseStore(self.source), second, v3=True)
        rebuilt = UniverseStore(self.root / "rebuilt.sqlite3")
        rebuilt.initialize()
        _ingest(rebuilt, self.fixture)
        _ingest(rebuilt, second)
        self.migrate()
        for table in ("canonical_markets", "claim_classes", "market_claims", "claim_relations"):
            self.assertEqual(_rows(self.dest, table), _rows(rebuilt.path, table), table)
        # A2's existing over and A1's corrected over now share a canonical row:
        # one extra correction minus one canonical row, not blindly +1.
        self.assertEqual([row["projection_row_count"] for row in _rows(self.dest, "universe_run_projections")],
                         [row["projection_row_count"] for row in _rows(rebuilt.path, "universe_run_projections")])

    def test_command_records_revision_in_an_image_without_git(self):
        from universe.__main__ import main
        revision = "c" * 40
        output = io.StringIO()
        with mock.patch.dict("os.environ", {"UNIVERSE_CODE_REVISION": revision}), mock.patch(
            "subprocess.run", side_effect=FileNotFoundError("git is absent")
        ), contextlib.redirect_stdout(output):
            self.assertEqual(main(["migrate", "--to", "7", str(self.source), str(self.dest)]), 0)
        self.assertEqual(_rows(self.dest, "universe_migrations")[0]["code_revision"], revision)

    def test_shared_old_claim_and_canonical_spans_drop_the_corrected_market(self):
        from targeter.v2.relationships import derive_bundle_relationships
        from universe.claims.claim_projection import rebuild_bundle
        # Without the Polymarket total, the wrong subset's early span belongs
        # only to the corrected market; the later valid under must survive.
        self.fixture = copy.deepcopy(self.fixture)
        report, _, markets = self.fixture
        markets[:] = [row for row in markets if row["target_id"] != PM_TOTAL]
        candidate = report["candidates"][0]
        candidate["market_ids"].remove(PM_TOTAL)
        relations = candidate["relationship_analysis"]["relationships"]
        relations[:] = [row for row in relations if not row["left"].startswith(PM_TOTAL) and not row["right"].startswith(PM_TOTAL)]
        self.source.unlink()
        with closing(sqlite3.connect(self.source)) as c:
            c.executescript(_v6_schema())
            c.execute("PRAGMA user_version = 6")
        _ingest(UniverseStore(self.source), self.fixture, v3=True)
        second = copy.deepcopy(self.fixture)
        report, events, markets = second
        report.update(run_id=R2, generated_at=G2)
        other = copy.deepcopy(next(row for row in markets if row["target_id"] == KALSHI_TOTAL))
        other.update(target_id="kalshi:correct-under", venue_market_id="correct-under", title="IG vs ThunderTalk: Under 3.5 maps")
        markets.append(other)
        report["candidates"][0]["market_ids"].append(other["target_id"])
        with mock.patch("universe.derive.market_projection._corrected_totals_parameters", side_effect=lambda source, parameters, **kwargs: parameters):
            projection = market_projection.project_market_universe(report, catalog_events=events, catalog_markets=markets)
        bundle = rebuild_bundle(projection["events"][0], projection["venue_events"], projection["venue_markets"])
        report["candidates"][0]["relationship_analysis"]["relationships"] = [row.as_record() for row in derive_bundle_relationships(bundle).relationships]
        _ingest(UniverseStore(self.source), second, v3=True)
        rebuilt = UniverseStore(self.root / "rebuilt.sqlite3")
        rebuilt.initialize()
        _ingest(rebuilt, self.fixture)
        _ingest(rebuilt, second)
        self.migrate()
        for table in ("claim_classes", "canonical_markets", "market_claims", "claim_relations"):
            self.assertEqual(_rows(self.dest, table), _rows(rebuilt.path, table), table)


if __name__ == "__main__":
    unittest.main()
