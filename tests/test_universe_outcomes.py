"""Offline outcome endpoint contracts using disposable SQLite evidence."""

import copy
import json
import sqlite3
import tempfile
import unittest
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from archive.storage import INDEPENDENT, LocalObjectStore
from tests.test_event_universe_store import (
    G1,
    R1,
    _catalog_rows,
    _publish_run,
    _selection_report,
)
from universe.api import UniverseApplication
from universe.store import UniverseStore
from universe.ingest.sync import UniverseSync


class UniverseOutcomesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.db = UniverseStore(root / "universe.db")
        self.db.initialize()
        self.store = LocalObjectStore(
            root / "archive", store_id="test", durability=INDEPENDENT
        )
        report = _selection_report(R1, G1)
        self.events, self.markets = _catalog_rows(report)
        # Same claim on both venues, with meaningful PM tokens in positional order.
        for m in self.markets:
            m["subscription_ids"] = (
                [m["venue_market_id"]] if m["venue"] == "kalshi" else ["123", "987"]
            )
            m["outcome_labels"] = (
                ["Alpha"] if m["venue"] == "kalshi" else ["Alpha", "Beta"]
            )
            if m["market_type"] == "map_winner":
                m["parameters"]["map_index"] = 2
        base = next(m for m in self.markets if m["venue"] == "kalshi")
        for native, kind, side, label in [
            ("series-away", "series_moneyline", "away", "Beta"),
            ("map2-home", "map_winner", "home", "Alpha"),
            ("map2-away", "map_winner", "away", "Beta"),
        ]:
            market = copy.deepcopy(base)
            market.update(
                target_id="kalshi:" + native,
                venue_market_id=native,
                canonical_class="esports." + kind,
                market_type=kind,
                subscription_ids=[native],
                outcome_labels=[label],
                parameters={
                    "side": side,
                    **({"map_index": 2} if kind == "map_winner" else {}),
                },
            )
            self.markets.append(market)
            report["candidates"][0]["market_ids"].append(market["target_id"])
        report["candidates"][0]["relationship_analysis"]["relationships"] = []
        # Let the report record the actual hand-authored bundle's relationships.
        from targeter.v2.relationships import derive_bundle_relationships
        from universe.claims.claim_projection import rebuild_bundle
        from universe.derive.market_projection import project_market_universe

        p = project_market_universe(
            report, catalog_events=self.events, catalog_markets=self.markets
        )
        b = rebuild_bundle(p["events"][0], p["venue_events"], p["venue_markets"])
        report["candidates"][0]["relationship_analysis"]["relationships"] = [
            r.as_record() for r in derive_bundle_relationships(b).relationships
        ]
        _publish_run(self.store, report, catalog_rows=(self.events, self.markets))
        result = UniverseSync(self.db, self.store).sync(
            now=datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
        )
        self.assertEqual(result.ingested, 1, result.as_record())

    def test_series_model_and_shared_ingestion_ids(self):
        doc = self.db.bundle_outcomes("bundle-1")
        self.assertEqual(doc["status"], "complete")
        self.assertEqual(
            doc["spaces"][0]["outcome_keys"],
            ["seq:AA", "seq:AHA", "seq:AHH", "seq:HAA", "seq:HAH", "seq:HH"],
        )
        markets = {m["market_id"]: m for m in doc["markets"]}
        self.assertEqual(
            markets["kalshi:series"]["claims"][0]["claim_id"],
            markets["polymarket:series"]["claims"][0]["claim_id"],
        )
        self.assertEqual(
            markets["polymarket:series"]["tokens"][1]["subscription_id"], "987"
        )
        claims = {c["claim_id"]: set(c["outcome_keys"]) for c in doc["claims"]}

        def keys(mid):
            return claims[markets[mid]["claims"][0]["claim_id"]]

        self.assertEqual(keys("kalshi:series"), {"seq:AHH", "seq:HAH", "seq:HH"})
        self.assertEqual(keys("kalshi:series-away"), {"seq:AA", "seq:AHA", "seq:HAA"})
        self.assertEqual(keys("kalshi:map2-home"), {"seq:AHA", "seq:AHH", "seq:HH"})
        self.assertEqual(keys("kalshi:map2-away"), {"seq:AA", "seq:HAA", "seq:HAH"})
        self.assertFalse(keys("kalshi:series") & keys("kalshi:series-away"))
        self.assertEqual(
            keys("kalshi:series") | keys("kalshi:series-away"),
            set(doc["spaces"][0]["outcome_keys"]),
        )
        with closing(sqlite3.connect(self.db.path)) as c, c:
            recorded = c.execute(
                "SELECT venue, venue_market_id, claim_key, claim_id FROM market_claims"
            ).fetchall()
        for venue, native, key, cid in recorded:
            self.assertIn(
                {"claim_key": key, "claim_id": cid},
                markets[venue + ":" + native]["claims"],
            )
        self.assertEqual(
            json.dumps(doc, sort_keys=True),
            json.dumps(self.db.bundle_outcomes("bundle-1"), sort_keys=True),
        )
        status, api_doc = UniverseApplication(self.db).get(
            "/v1/bundles/bundle-1/outcomes"
        )
        self.assertEqual(status, 200)
        self.assertEqual(doc, api_doc)

    def test_void_and_claim_conflict(self):
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute(
                "UPDATE venue_markets SET status='CANCELLED' WHERE venue='kalshi'"
            )
        doc = self.db.bundle_outcomes("bundle-1")
        self.assertEqual(
            next(m for m in doc["markets"] if m["market_id"] == "kalshi:series")[
                "mask_status"
            ],
            "VOID_UNSUPPORTED",
        )
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute("UPDATE venue_markets SET status='open' WHERE venue='kalshi'")
            c.execute(
                "UPDATE venue_markets SET parameters_json=? WHERE venue='kalshi' AND market_type='series_moneyline'",
                (json.dumps({"side": "away"}),),
            )
        self.assertEqual(
            next(
                m
                for m in self.db.bundle_outcomes("bundle-1")["markets"]
                if m["market_id"] == "kalshi:series"
            )["mask_status"],
            "CLAIM_CONFLICT",
        )

    def test_rejected_format_diagnostics_and_unreconstructed(self):
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute(
                "UPDATE venue_markets SET parameters_json=? WHERE market_type='map_winner'",
                (json.dumps({"map_index": 4, "side": "home"}),),
            )
        doc = self.db.bundle_outcomes("bundle-1")
        row = next(m for m in doc["markets"] if m["market_type"] == "map_winner")
        self.assertEqual(
            (row["mask_status"], row["reason"]),
            ("REJECTED", "product_outside_series_format"),
        )
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute("UPDATE venue_events SET format='5' WHERE venue='polymarket'")
        doc = self.db.bundle_outcomes("bundle-1")
        self.assertEqual(
            doc["diagnostics"], ["series_scope_missing_unambiguous_best_of_format"]
        )
        self.assertTrue(all(m["mask_status"] == "NO_SPACE" for m in doc["markets"]))
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute("UPDATE umbrella_events SET participants_json='[]'")
        doc = self.db.bundle_outcomes("bundle-1")
        self.assertEqual(doc["status"], "unreconstructed")
        self.assertEqual(doc["spaces"], [])

    def test_unknown_bundle(self):
        self.assertEqual(
            UniverseApplication(self.db).get("/v1/bundles/absent/outcomes"),
            (404, {"error": "bundle not found"}),
        )

    def test_bo5_and_yes_no_alignment(self):
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute("UPDATE venue_events SET format='5'")
            c.execute("DELETE FROM market_claims")
            c.execute(
                "UPDATE venue_markets SET outcome_labels_json=? WHERE venue='polymarket' AND market_type='series_moneyline'",
                (json.dumps(["No", "Yes"]),),
            )
        doc = self.db.bundle_outcomes("bundle-1")
        self.assertEqual(len(doc["spaces"][0]["outcome_keys"]), 20)
        pm = next(m for m in doc["markets"] if m["market_id"] == "polymarket:series")
        self.assertEqual(
            pm["tokens"],
            [
                {"subscription_id": "123", "claim_key": "claim=0", "negated": True},
                {"subscription_id": "987", "claim_key": "claim=0", "negated": False},
            ],
        )
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.execute(
                "UPDATE venue_markets SET outcome_labels_json=? WHERE venue='polymarket' AND market_type='series_moneyline'",
                (json.dumps(["Alpha"]),),
            )
        pm = next(
            m
            for m in self.db.bundle_outcomes("bundle-1")["markets"]
            if m["market_id"] == "polymarket:series"
        )
        self.assertEqual(
            (pm["mask_status"], pm["reason"], pm["claims"], pm["tokens"]),
            ("UNSUPPORTED", "token_alignment", [], []),
        )

    def test_ambiguous_bundle_is_409(self):
        def copy_row(c, table, **changes):
            row = dict(c.execute(f"SELECT * FROM {table} LIMIT 1").fetchone())
            row.update(changes)
            c.execute(
                f"INSERT INTO {table} ({','.join(row)}) VALUES ({','.join('?' for _ in row)})",
                tuple(row.values()),
            )

        # A second complete selection of the same bundle, decided against a
        # different umbrella event, makes the bundle ambiguous.
        with closing(sqlite3.connect(self.db.path)) as c, c:
            c.row_factory = sqlite3.Row
            other_event = "event:d1:" + "f" * 64
            other_run = "20260101T000500.000000Z"
            copy_row(c, "umbrella_events", event_id=other_event, identity_ordinal=1)
            run = dict(c.execute("SELECT * FROM targeter_runs").fetchone())
            copy_row(
                c,
                "targeter_runs",
                run_id=other_run,
                generated_at_ns=run["generated_at_ns"] + 1,
                manifest_key=run["manifest_key"] + "-other",
                report_key=run["report_key"] + "-other",
            )
            copy_row(
                c,
                "selection_occurrences",
                run_id=other_run,
                origin_run_id=other_run,
                continuity_selected=0,
                continuity_disposition=None,
            )
            copy_row(c, "candidate_decisions", run_id=other_run, event_id=other_event)
        self.assertEqual(
            UniverseApplication(self.db).get("/v1/bundles/bundle-1/outcomes"),
            (409, {"error": "bundle maps to multiple events"}),
        )

    def test_bundle_lookup_never_reads_event_observations(self):
        # event_observations has one row per candidate per run (449k in
        # production) and no bundle_id index. Scanning it on a cold page cache
        # took ~51 s on the 2 GB Universe VM, past Caddy's 30 s upstream
        # timeout, so every first request after idle returned 504.
        statements = []
        connect = self.db.connect

        def traced(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(statements.append)
            return connection

        with patch.object(self.db, "connect", side_effect=traced):
            self.assertIsNotNone(self.db.bundle_outcomes("bundle-1"))
            self.assertIsNone(self.db.bundle_outcomes("absent"))
        self.assertTrue(statements)
        self.assertFalse(
            [s for s in statements if "event_observations" in s],
            "the outcomes lookup must resolve bundles through selections",
        )

    def test_read_bounds_and_independent_validator(self):
        from replay.outcome_model import validate_document

        validate_document(self.db.bundle_outcomes("bundle-1"), "bundle-1")
        from universe.store import DetailTooLarge

        with (
            patch("universe.store.limits.DETAIL_ROW_LIMIT", 1),
            self.assertRaises(DetailTooLarge),
        ):
            self.db.bundle_outcomes("bundle-1")
        with (
            patch("universe.store.limits.EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES", 100),
            self.assertRaises(DetailTooLarge),
        ):
            self.db.bundle_outcomes("bundle-1")

    def test_all_markets_and_partial_condition_fail_closed(self):
        with closing(sqlite3.connect(self.db.path)) as c, c:
            # No observation claim is required for an excluded semantic product.
            c.execute("DELETE FROM market_claims")
            c.execute(
                "UPDATE venue_markets SET outcome_labels_json=? WHERE market_type='series_moneyline' AND venue='polymarket'",
                (json.dumps(["Alpha", "Unknown"]),),
            )
        row = next(
            m
            for m in self.db.bundle_outcomes("bundle-1")["markets"]
            if m["market_id"] == "polymarket:series"
        )
        self.assertEqual(row["mask_status"], "REJECTED")
        self.assertEqual(row["claims"], [])
        self.assertEqual(row["tokens"], [])
