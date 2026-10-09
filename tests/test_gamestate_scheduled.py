import tempfile
import unittest
from pathlib import Path
from gamestate import kalshi
from tests.test_kalshi_game_state import records as archived
from archive.storage.local import LocalObjectStore

EVENT = "event:d1:" + "a" * 64


class ScheduledTests(unittest.TestCase):
    def test_job_temporary_storage_fits_the_adapter_raw_bound(self):
        compose = Path("compose.universe.yaml").read_text()
        service = compose.split("  event-universe-game-state:\n", 1)[1].split("\n  replay-redis:", 1)[0]
        self.assertIn("/tmp:size=512m", service)

    def test_ledger_reopen_preserves_attempts_and_rejects_schema_tamper(self):
        import sqlite3
        from gamestate.run_scheduled import Ledger
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "ledger.sqlite3"
            with Ledger(path) as ledger:
                ledger.append(EVENT, "bundle-a", 0, "fetch_failed", None, None, "fetch_failed")
            with Ledger(path) as ledger:
                self.assertFalse(ledger.due(EVENT, 0))
            with sqlite3.connect(path) as connection:
                connection.execute("DROP TRIGGER attempts_no_delete")
            with self.assertRaisesRegex(ValueError, "ledger_schema"):
                Ledger(path)
            with sqlite3.connect(path) as connection:
                self.assertIsNone(connection.execute("SELECT sql FROM sqlite_master WHERE name='attempts_no_delete'").fetchone())
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 1)

    def test_event_identity_is_the_archive_namespace_and_strict_receipt_binding(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = LocalObjectStore(Path(temporary))
            m, rows = archived()
            prefix = kalshi.archive_fetch(store, m, ["bundle-a"], rows, 1000000000,
                                         "complete", event_id=EVENT)
            self.assertIn("/event=" + EVENT.split(":")[-1] + "/", prefix)
            receipt, records = kalshi.read_records(store, prefix)
            self.assertEqual(receipt["event_id"], EVENT)
            self.assertEqual(receipt["receipt_version"], 2)
            list(records)
            from gamestate.timeline import latest
            self.assertEqual(latest(store, EVENT)["state"], "incomplete")
            self.assertEqual(latest(store, EVENT, require_timeline=False)["state"], "ok")
            timeline = kalshi.regenerate(store, prefix)
            self.assertEqual(timeline["event_id"], EVENT)
            self.assertEqual(latest(store, EVENT)["timeline"], timeline)
            second = kalshi.archive_fetch(store, m, ["bundle-a"], rows, 2000000000,
                                          "incomplete", event_id=EVENT)
            self.assertNotEqual(prefix, second)
            self.assertEqual(latest(store, EVENT)["state"], "ok")
            receipt_path = Path(temporary) / prefix / "receipt.json"
            tampered = kalshi.loads(receipt_path.read_bytes())
            tampered["event_id"] = "event:d1:" + "b" * 64
            receipt_path.write_bytes(kalshi.dumps(tampered))
            with self.assertRaisesRegex(ValueError, "event_identity"):
                kalshi.read_receipt(store, prefix)

    def test_closed_config_and_scheduled_failure_are_visible(self):
        from gamestate.run_scheduled import Ledger, config, execute
        from tests.test_kalshi_game_state import client
        document = kalshi.loads(Path("configs/gamestate.json").read_bytes())
        for changes in ({"unknown": 1}, {"version": True}, {"max_bundles_per_run": 0}):
            with self.assertRaises(ValueError):
                config({**document, **changes})
        replies = [{"bundles": [{"bundle_id": "bundle-a", "lifecycle": "retired"}], "next_cursor": None},
                   {"selections": [{"retirement": {"retired_at": "2026-10-01T00:00:00Z"}}], "next_cursor": None},
                   {"version": 1, "bundle_id": "bundle-a", "event_id": EVENT}]
        discovery = client(replies)
        clients = iter((discovery, client([])))
        report = {"bundles": [{"reason": "fetch_failed", "milestone_id": None}], "fetches": [], "failures": []}
        with tempfile.TemporaryDirectory() as temporary:
            with Ledger(Path(temporary) / "ledger.sqlite3") as ledger:
                result = execute(document, LocalObjectStore(Path(temporary) / "archive"), ledger,
                                 lambda: next(clients), now_ns=kalshi.timestamp("2026-10-01T02:00:00Z"),
                                 pull=lambda *args, **kwargs: report)
                self.assertEqual(result, {"version": 1, "eligible": 1, "attempted": 1, "failed": 1})
                self.assertFalse(ledger.due(EVENT, kalshi.timestamp("2026-10-01T02:59:59Z")))

    def test_backoff_is_event_scoped_and_append_only(self):
        from gamestate.run_scheduled import Ledger
        with tempfile.TemporaryDirectory() as temporary:
            with Ledger(Path(temporary) / "ledger.sqlite3") as ledger:
                for attempt in range(5):
                    at = attempt * 24 * 3600 * 10**9
                    self.assertTrue(ledger.due(EVENT, at))
                    ledger.append(EVENT, "bundle-a", at, "fetch_failed", None, None, "fetch_failed")
                    self.assertFalse(ledger.due(EVENT, at + 10**9))
                self.assertFalse(ledger.due(EVENT, 10 * 24 * 3600 * 10**9))
                other = "event:d1:" + "b" * 64
                self.assertTrue(ledger.due(other, 0))
                ledger.append(other, "bundle-b", 0, "no_kalshi_events", None, None, None)
                self.assertFalse(ledger.due(other, 100 * 24 * 3600 * 10**9))
                import sqlite3
                with self.assertRaises(sqlite3.IntegrityError):
                    ledger.connection.execute("DELETE FROM fetch_attempts")

    def test_settle_boundary_and_missing_retirement_are_not_guessed(self):
        from gamestate.run_scheduled import eligible
        self.assertTrue(eligible([{"retirement": {"retired_at": "2026-10-01T00:00:00Z"}}],
                                 kalshi.timestamp("2026-10-01T02:00:00Z"), 7200))
        self.assertFalse(eligible([{"retirement": {"retired_at": "2026-10-01T00:00:00Z"}}],
                                  kalshi.timestamp("2026-10-01T01:59:59Z"), 7200))
        with self.assertRaises(ValueError):
            eligible([{"retirement": None}], 0, 7200)
