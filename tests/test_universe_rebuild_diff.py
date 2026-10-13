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
        self.assertEqual(report["new"], {"user_version": 7, "projection_versions": [4]})
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


if __name__ == "__main__":
    unittest.main()
