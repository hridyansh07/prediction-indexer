"""The read-only rebuild diff between a v6/v3 file and a v7/v4 file."""

import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from archive.storage import INDEPENDENT, LocalObjectStore
from tests.test_event_universe_store import _publish_run
from tests.test_universe_totals_direction import KALSHI_TOTAL, _thundertalk
from universe.ingest.sync import UniverseSync
from universe.store import UniverseStore
from universe.tools import rebuild_diff

NATIVE = KALSHI_TOTAL.split(":", 1)[1]
OLD_CLAIM = "f" * 64


class RebuildDiffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.new = root / "new.sqlite"
        store = UniverseStore(self.new)
        store.initialize()
        objects = LocalObjectStore(root / "archive", store_id="t", durability=INDEPENDENT)
        report, events, markets = _thundertalk()
        _publish_run(objects, report, catalog_rows=(events, markets))
        result = UniverseSync(store, objects).sync(
            now=datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        )
        self.assertEqual(result.ingested, 1)
        # A v3-shaped file: the wrong direction, its old IDs and no corrections table.
        self.old = root / "old.sqlite"
        with closing(sqlite3.connect(self.new)) as source, closing(
            sqlite3.connect(self.old)
        ) as target:
            source.backup(target)
        with closing(sqlite3.connect(self.old)) as c:
            c.execute("PRAGMA foreign_keys = OFF")
            c.execute("PRAGMA ignore_check_constraints = ON")
            c.execute(
                "UPDATE venue_markets SET parameters_json = ?, market_id = 'market:old' "
                "WHERE venue_market_id = ?",
                (json.dumps({"direction": "under", "line": 3.5}), NATIVE),
            )
            c.execute(
                """INSERT INTO claim_classes SELECT ?, space_shape_id, scope, coverage,
                   2, claim_identity_version, first_seen_run_id, last_seen_run_id
                   FROM claim_classes LIMIT 1""",
                (OLD_CLAIM,),
            )
            c.execute(
                "UPDATE market_claims SET claim_id = ? WHERE venue_market_id = ?",
                (OLD_CLAIM, NATIVE),
            )
            c.execute("UPDATE universe_run_projections SET projection_version = 3")
            c.execute("DROP TABLE universe_run_projection_corrections")
            c.execute("PRAGMA user_version = 6")
            c.commit()

    def _stat(self, path):
        info = os.stat(path)
        return path.read_bytes(), info.st_mtime_ns

    def test_lists_exactly_the_corrected_market_and_its_claim(self):
        before = (self._stat(self.old), self._stat(self.new))
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(rebuild_diff.main([str(self.old), str(self.new)]), 0)
        report = json.loads(out.getvalue())
        self.assertEqual((self._stat(self.old), self._stat(self.new)), before)

        self.assertEqual(report["old"], {"user_version": 6, "projection_versions": [3]})
        self.assertEqual(report["new"], {"user_version": 8, "projection_versions": [4]})
        changed = report["venue_markets"]["changed"]
        self.assertEqual(list(changed), [KALSHI_TOTAL])
        self.assertEqual(changed[KALSHI_TOTAL]["parameters"]["old"]["direction"], "under")
        self.assertEqual(changed[KALSHI_TOTAL]["parameters"]["new"]["direction"], "over")
        self.assertEqual(changed[KALSHI_TOTAL]["market_id"]["old"], "market:old")
        self.assertEqual(report["venue_markets"]["only_in_old"], [])
        self.assertEqual(report["venue_markets"]["only_in_new"], [])
        claims = report["market_claims"]["changed"]
        self.assertEqual(len(claims), 1)
        (name,) = claims
        self.assertTrue(name.startswith(KALSHI_TOTAL + "#"))
        self.assertEqual(list(claims[name]["old"]), [OLD_CLAIM])
        self.assertEqual(report["bundles_affected"], ["bundle-1"])
        self.assertEqual(
            [(c["venue_market_id"], c["report_value"], c["projected_value"])
             for c in report["new_corrections"]],
            [(NATIVE, "under", "over")],
        )

    def test_opening_read_only_refuses_writes(self):
        with closing(rebuild_diff._open(self.new)) as c:
            with self.assertRaises(sqlite3.OperationalError):
                c.execute("DELETE FROM targeter_runs")

    def _same_files(self):
        with closing(sqlite3.connect(self.new)) as source, closing(sqlite3.connect(self.old)) as target:
            source.backup(target)
        with closing(sqlite3.connect(self.old)) as c:
            c.execute("PRAGMA ignore_check_constraints = ON")
            c.execute("UPDATE universe_run_projections SET projection_version = 3, projection_sha256 = ?", ("d" * 64,))
            c.execute("UPDATE targeter_runs SET indexed_at_ns = 42")
            c.execute("INSERT INTO universe_migrations VALUES ('m1-totals-direction', 6, 7, ?, '{}', '{}', 1)", ("c" * 40,))
            c.commit()

    def _equivalence(self, *extra):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = rebuild_diff.main([
                "--equivalence", "--generated-start", "2026-01-01T00:00:00Z",
                "--generated-end", "2026-01-02T00:00:00Z", *extra,
                str(self.old), str(self.new),
            ])
        return result, json.loads(out.getvalue())

    def test_equivalence_compares_every_table_with_only_spec_exclusions(self):
        self._same_files()
        before = self._stat(self.old), self._stat(self.new)
        status, report = self._equivalence()
        self.assertEqual(status, 0)
        self.assertTrue(report["equivalence"]["equal"])
        self.assertEqual(report["equivalence"]["runs_in_range"], 1)
        self.assertEqual(report["universe_migrations"]["old"][0]["migration_id"], "m1-totals-direction")
        self.assertEqual(report["universe_migrations"]["new"], [])
        self.assertEqual((self._stat(self.old), self._stat(self.new)), before)
        with closing(sqlite3.connect(self.new)) as c:
            c.execute("UPDATE targeter_runs SET strategy_version = 9")
            c.commit()
        status, report = self._equivalence()
        self.assertEqual(status, 1)
        self.assertFalse(report["equivalence"]["equal"])
        self.assertEqual(report["table_differences"]["targeter_runs"]["only_in_old"], 1)
        self.assertEqual(report["table_differences"]["targeter_runs"]["only_in_new"], 1)

    def test_equivalence_fallback_is_explicit_and_confined_to_corrections(self):
        self._same_files()
        with closing(sqlite3.connect(self.old)) as c:
            c.execute("DELETE FROM universe_run_projection_corrections")
            c.commit()
        self.assertEqual(self._equivalence()[0], 1)
        status, report = self._equivalence("--accept-correction-membership-fallback")
        self.assertEqual(status, 0)
        self.assertEqual(report["equivalence"]["accepted_differences"], ["universe_run_projection_corrections"])
        self.assertEqual(report["table_differences"]["universe_run_projection_corrections"]["only_in_new"], 1)
        with closing(sqlite3.connect(self.old)) as c:
            c.execute("UPDATE venue_markets SET volume_total = 987")
            c.commit()
        self.assertEqual(self._equivalence("--accept-correction-membership-fallback")[0], 1)

    def test_equivalence_checks_half_open_bounds_and_rejects_empty_range(self):
        self._same_files()
        for start, end in (("2026-01-02T00:00:00Z", "2026-01-01T00:00:00Z"),
                           ("2027-01-01T00:00:00Z", "2027-01-02T00:00:00Z")):
            with self.subTest(start=start):
                out, error = io.StringIO(), io.StringIO()
                with contextlib.redirect_stdout(out), contextlib.redirect_stderr(error):
                    result = rebuild_diff.main([
                        "--equivalence", "--generated-start", start, "--generated-end", end,
                        str(self.old), str(self.new),
                    ])
                self.assertNotEqual(result, 0)

    def test_reports_execution_table_nulls_and_detects_an_uncompared_table(self):
        self._same_files()
        with closing(sqlite3.connect(self.new)) as c:
            c.execute("CREATE TABLE execution_terms (run_id TEXT, venue TEXT, venue_market_id TEXT, taker_delay_ms INTEGER, observed_at TEXT, PRIMARY KEY(run_id, venue, venue_market_id)) STRICT")
            c.executemany("INSERT INTO execution_terms VALUES ('r', 'polymarket', ?, ?, ?)",
                          [("m1", 0, "2026-01-01T00:00:00Z"), ("m2", 2000, "2026-01-01T00:00:00Z"), ("m3", None, None)])
            c.commit()
        status, report = self._equivalence()
        self.assertEqual(status, 1)
        terms = report["execution_term_observations"]["new"]["execution_terms"]
        self.assertEqual(terms["by_venue"]["polymarket"], {"rows": 3, "observed": 3, "null": 1})
        self.assertEqual(terms["by_venue"]["kalshi"]["rows"], 0)
        self.assertEqual(terms["by_venue"]["limitless"]["rows"], 0)
        self.assertEqual(report["table_differences"]["execution_terms"]["only_in_new"], 3)


if __name__ == "__main__":
    unittest.main()
