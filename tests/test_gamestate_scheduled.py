import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from gamestate import kalshi
from tests.test_kalshi_game_state import client, milestone, records as archived
from archive.storage.local import LocalObjectStore

EVENT = "event:d1:" + "a" * 64
OTHER = "event:d1:" + "b" * 64
RETIRED_AT = "2026-10-01T00:00:00Z"
SETTLED = kalshi.timestamp("2026-10-01T02:00:00Z")
HOUR = 3600 * 10**9


def document():
    return kalshi.loads(Path("configs/gamestate.json").read_bytes())


class Router:
    """One scripted Universe + Kalshi, routed by URL so no test depends on client order."""

    def __init__(self, events, *, live=None, broken=(), venues=None, active=()):
        self.events = events  # bundle id -> event id
        self.broken = set(broken)  # bundles whose /outcomes is a 404
        self.venues = venues or {}  # bundle id -> venues (default kalshi + polymarket)
        self.active = set(active)  # bundles not retired yet
        self.calls = []
        _, rows = archived()
        self.kalshi = [kalshi.loads(kalshi.body_bytes(r)) for r in rows]
        if live is not None:
            self.kalshi[1] = {"live_data": {"milestone_id": "match-id", "details": live}}

    def __call__(self, url):
        self.calls.append(url)
        path = urlsplit(url).path
        if url.startswith(kalshi.KALSHI):
            if path.endswith("/milestones"):
                body = {"milestones": [milestone()]}
            elif "/live_data/" in path:
                body = self.kalshi[1]
            else:
                body = self.kalshi[2] if path.endswith("/MAP1") else self.kalshi[3]
            return 200, {"Content-Type": "application/json"}, json.dumps(body).encode()
        parts = path.strip("/").split("/")
        if parts == ["v1", "selections"]:
            # The server's venue filter; each bundle appears in two occurrences.
            venue = parse_qs(urlsplit(url).query).get("venue", [None])[0]
            rows = [{"bundle_id": b, "run_id": run, "activation_at": "2026-09-27T10:00:00Z"}
                    for b in sorted(self.events) for run in ("r1", "r2")
                    if venue is None or venue in self.venues.get(b, ["kalshi", "polymarket"])]
            body = {"selections": rows, "sort": "activation", "next_cursor": None}
        elif parts[-1] == "history":
            retirement = None if parts[2] in self.active else {"retired_at": RETIRED_AT}
            body = {"selections": [{"run_id": "run", "bundle_id": parts[2],
                                    "retirement": retirement}], "next_cursor": None}
        elif parts[-1] == "outcomes":
            if parts[2] in self.broken:
                return 404, {"Content-Type": "application/json"}, b'{"error":"not_found"}'
            body = {"version": 1, "bundle_id": parts[2], "event_id": self.events[parts[2]]}
        else:  # /v1/runs/<run>/selections/<bundle>
            body = {"bundle_id": parts[-1], "run_id": parts[2], "context": {"event_refs": ["kalshi:SERIES"]}}
        return 200, {"Content-Type": "application/json"}, json.dumps(body).encode()

    def count(self, fragment):
        return sum(fragment in url for url in self.calls)

    def universe(self):
        return [urlsplit(u).path for u in self.calls if not u.startswith(kalshi.KALSHI)]


class Factory:
    def __init__(self, router):
        self.router, self.clients = router, []

    def __call__(self, **options):
        records = options.get("records", [])
        made = kalshi.Client(self.router, sleep=lambda _: None, records=records)
        self.clients.append(made)
        return made


def scheduled(store, ledger, router, now, **options):
    return kalshi_execute(document(), store, ledger, Factory(router), now_ns=now, **options)


def kalshi_execute(*args, **kwargs):
    from gamestate.run_scheduled import execute
    return execute(*args, **kwargs)


class ScheduledTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.store = LocalObjectStore(self.root / "archive")

    def ledger(self):
        from gamestate.run_scheduled import Ledger
        ledger = Ledger(self.root / "ledger.sqlite3")
        self.addCleanup(ledger.connection.close)
        return ledger

    def test_job_temporary_storage_fits_the_adapter_raw_bound(self):
        compose = Path("compose.universe.yaml").read_text()
        service = compose.split("  event-universe-game-state:\n", 1)[1].split("\n  replay-redis:", 1)[0]
        self.assertIn("/tmp:size=512m", service)

    def test_permanently_bad_live_data_stops_after_the_retry_budget(self):
        # Kalshi answers live_data without the period arrays: each pull archives an incomplete fetch.
        router = Router({"bundle-a": EVENT}, live={})
        ledger = self.ledger()
        for run in range(12):
            scheduled(self.store, ledger, router, SETTLED + run * 24 * HOUR)
        self.assertEqual(router.count("/live_data/"), 5)
        outcomes = [r[0] for r in ledger.connection.execute("SELECT outcome FROM fetch_attempts")]
        self.assertEqual(outcomes, ["incomplete"] * 5)

    def test_complete_but_disqualified_report_is_final(self):
        # Defensive: a complete receipt whose timeline is disqualified is final, not a refetch.
        report = {"bundles": [{"reason": None, "milestone_id": "match-id"}], "milestones_skipped": 0,
                  "fetches": [{"milestone_id": "match-id", "prefix": "p", "status": "complete",
                               "errors": [], "disqualified": True}], "failures": []}
        pulls = []
        ledger = self.ledger()
        router = Router({"bundle-a": EVENT})
        for run in range(3):
            kalshi_execute(document(), self.store, ledger, Factory(router), now_ns=SETTLED + run * 24 * HOUR,
                           pull=lambda *a, **k: pulls.append(1) or report)
        self.assertEqual(len(pulls), 1)
        self.assertEqual(ledger.connection.execute("SELECT outcome FROM fetch_attempts").fetchall(),
                         [("complete_inconsistent",)])

    def test_one_bad_bundle_does_not_block_later_bundles(self):
        router = Router({"bundle-a": EVENT, "bundle-b": OTHER}, broken={"bundle-a"})
        ledger = self.ledger()
        result = scheduled(self.store, ledger, router, SETTLED)
        self.assertEqual(router.count("/live_data/"), 1)
        rows = ledger.connection.execute(
            "SELECT bundle_id, event_id, outcome FROM fetch_attempts ORDER BY bundle_id").fetchall()
        self.assertEqual(rows, [("bundle-a", None, "unavailable"), ("bundle-b", OTHER, "complete")])
        self.assertEqual(result["failed"], 1)
        # The unavailable bundle backs off like a failed pull instead of failing every run.
        self.assertFalse(ledger.due(SETTLED + HOUR - 1, bundle_id="bundle-a"))
        self.assertTrue(ledger.due(SETTLED + HOUR, bundle_id="bundle-a"))

    def test_universe_discovery_responses_are_never_recorded(self):
        router = Router({f"bundle-{i}": "event:d1:" + f"{i:064x}" for i in range(3)})
        factory = Factory(router)
        kalshi_execute(document(), self.store, self.ledger(), factory, now_ns=SETTLED)
        recorded = [r["url"] for c in factory.clients if isinstance(c.records, list) for r in c.records]
        self.assertTrue(recorded)
        self.assertTrue(all(url.startswith(kalshi.KALSHI) for url in recorded))

    def test_finished_bundles_cost_no_universe_or_archive_calls(self):
        router = Router({"bundle-a": EVENT, "bundle-b": OTHER})
        ledger = self.ledger()
        scheduled(self.store, ledger, router, SETTLED)
        router.calls.clear()
        listed = []
        original = self.store.list_keys
        self.store.list_keys = lambda prefix: listed.append(prefix) or original(prefix)
        scheduled(self.store, ledger, router, SETTLED + 24 * HOUR)
        self.assertEqual(router.universe(), ["/v1/selections"])
        self.assertEqual(router.count(kalshi.KALSHI), 0)
        self.assertEqual(listed, [])

    def test_bundles_without_a_kalshi_market_cost_no_calls(self):
        # Polymarket/Limitless-only bundles need another source; they are skipped, not failed.
        router = Router({"bundle-a": EVENT, "bundle-b": OTHER}, venues={"bundle-a": ["limitless", "polymarket"]})
        ledger = self.ledger()
        result = scheduled(self.store, ledger, router, SETTLED)
        self.assertNotIn("/v1/bundles/bundle-a/history", router.universe())
        self.assertIn("venue=kalshi", router.calls[0])
        self.assertEqual(ledger.connection.execute("SELECT bundle_id FROM fetch_attempts").fetchall(), [("bundle-b",)])
        self.assertEqual(result["failed"], 0)

    def test_unretired_bundles_wait_without_a_ledger_row(self):
        router = Router({"bundle-a": EVENT, "bundle-b": OTHER}, active={"bundle-a"})
        ledger = self.ledger()
        result = scheduled(self.store, ledger, router, SETTLED)
        self.assertEqual(ledger.connection.execute("SELECT bundle_id FROM fetch_attempts").fetchall(), [("bundle-b",)])
        self.assertEqual(result["failed"], 0)
        self.assertIsNone(ledger.event_for("bundle-a"))

    def test_backfill_passes_canonical_activation_bounds(self):
        router = Router({"bundle-a": EVENT})
        start, end = kalshi.timestamp("2026-08-01T00:00:00Z"), kalshi.timestamp("2026-10-07T00:00:00Z")
        scheduled(self.store, self.ledger(), router, SETTLED, activation=(start, end))
        query = parse_qs(urlsplit(router.calls[0]).query)
        self.assertEqual((query["activation_start"], query["activation_end"]),
                         (["2026-08-01T00:00:00Z"], ["2026-10-07T00:00:00Z"]))

    def test_shared_event_is_pulled_once(self):
        router = Router({"bundle-a": EVENT, "bundle-b": EVENT})
        ledger = self.ledger()
        scheduled(self.store, ledger, router, SETTLED)
        self.assertEqual(router.count("/live_data/"), 1)
        mapped = ledger.connection.execute("SELECT bundle_id, event_id FROM bundle_events ORDER BY bundle_id").fetchall()
        self.assertEqual(mapped, [("bundle-a", EVENT), ("bundle-b", EVENT)])

    def test_lost_ledger_trusts_the_archive_instead_of_refetching_live_data(self):
        router = Router({"bundle-a": EVENT})
        scheduled(self.store, self.ledger(), router, SETTLED)
        (self.root / "ledger.sqlite3").unlink()
        for suffix in ("-wal", "-shm"):
            Path(str(self.root / "ledger.sqlite3") + suffix).unlink(missing_ok=True)
        router.calls.clear()
        from gamestate.run_scheduled import Ledger
        with Ledger(self.root / "ledger.sqlite3") as fresh:
            scheduled(self.store, fresh, router, SETTLED + HOUR)
            self.assertEqual(router.count("/live_data/"), 0)
            self.assertEqual(fresh.connection.execute("SELECT outcome FROM fetch_attempts").fetchall(), [("complete",)])

    def test_ledger_reopen_preserves_attempts_and_rejects_schema_tamper(self):
        from gamestate.run_scheduled import Ledger
        path = self.root / "ledger.sqlite3"
        with Ledger(path) as ledger:
            ledger.append(EVENT, "bundle-a", 0, "fetch_failed", None, None, "fetch_failed")
            ledger.record_event("bundle-a", EVENT, 0)
        with Ledger(path) as ledger:
            self.assertFalse(ledger.due(0, event_id=EVENT))
            self.assertEqual(ledger.event_for("bundle-a"), EVENT)
            with self.assertRaisesRegex(ValueError, "bundle_event_changed"):
                ledger.record_event("bundle-a", OTHER, 1)
        with sqlite3.connect(path) as connection:
            connection.execute("DROP TRIGGER attempts_no_delete")
        with self.assertRaisesRegex(ValueError, "ledger_schema"):
            Ledger(path)
        with sqlite3.connect(path) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM fetch_attempts").fetchone()[0], 1)

    def test_event_identity_is_the_archive_namespace_and_strict_receipt_binding(self):
        store = self.store
        m, rows = archived()
        prefix = kalshi.archive_fetch(store, m, ["bundle-a"], rows, 1000000000, "complete", event_id=EVENT)
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
        second = kalshi.archive_fetch(store, m, ["bundle-a"], rows, 2000000000, "incomplete", event_id=EVENT)
        self.assertNotEqual(prefix, second)
        self.assertEqual(latest(store, EVENT)["state"], "ok")
        receipt_path = self.root / "archive" / prefix / "receipt.json"
        tampered = kalshi.loads(receipt_path.read_bytes())
        tampered["event_id"] = OTHER
        receipt_path.write_bytes(kalshi.dumps(tampered))
        with self.assertRaisesRegex(ValueError, "event_identity"):
            kalshi.read_receipt(store, prefix)

    def test_latest_skips_an_unreadable_fetch_and_counts_it(self):
        from gamestate.timeline import latest
        m, rows = archived()
        good = kalshi.archive_fetch(self.store, m, ["bundle-a"], rows, 1000000000, "complete", event_id=EVENT)
        bad = kalshi.archive_fetch(self.store, m, ["bundle-a"], rows, 2000000000, "complete", event_id=EVENT)
        (self.root / "archive" / bad / "receipt.json").write_bytes(b"{}")
        result = latest(self.store, EVENT, require_timeline=False)
        self.assertEqual(result["state"], "ok")
        self.assertEqual(result["rejected_fetches"], 1)
        self.assertIn(good + "/receipt.json", result["inputs"])

    def test_legacy_lookup_ignores_event_keyed_fetches(self):
        m, rows = archived()
        kalshi.archive_fetch(self.store, m, ["bundle-a"], rows, 1000000000, "complete", event_id=EVENT)
        self.assertTrue(kalshi.existing_complete(self.store, m["id"], event_id=EVENT))
        self.assertFalse(kalshi.existing_complete(self.store, m["id"]))

    def test_closed_config(self):
        from gamestate.run_scheduled import config
        for changes in ({"unknown": 1}, {"version": True}, {"max_bundles_per_run": 0}):
            with self.assertRaises(ValueError):
                config({**document(), **changes})

    def test_backoff_is_scoped_and_append_only(self):
        ledger = self.ledger()
        for attempt in range(5):
            at = attempt * 24 * HOUR
            self.assertTrue(ledger.due(at, event_id=EVENT))
            ledger.append(EVENT, "bundle-a", at, "fetch_failed", None, None, "fetch_failed")
            self.assertFalse(ledger.due(at + 10**9, event_id=EVENT))
        self.assertFalse(ledger.due(10 * 24 * HOUR, event_id=EVENT))
        self.assertTrue(ledger.due(0, event_id=OTHER))
        ledger.append(OTHER, "bundle-b", 0, "no_kalshi_events", None, None, None)
        self.assertFalse(ledger.due(100 * 24 * HOUR, event_id=OTHER))
        ledger.append(None, "bundle-c", 0, "unavailable", None, None, "http_status")
        self.assertTrue(ledger.due(0, event_id=EVENT.replace("a", "c")))
        self.assertFalse(ledger.due(0, bundle_id="bundle-c"))
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.connection.execute("DELETE FROM fetch_attempts")
        ledger.record_event("bundle-a", EVENT, 0)
        for statement in ("UPDATE bundle_events SET event_id='x'", "DELETE FROM bundle_events"):
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.connection.execute(statement)

    def test_settle_boundary_and_missing_retirement_are_not_guessed(self):
        from gamestate.run_scheduled import eligible
        self.assertTrue(eligible([{"retirement": {"retired_at": RETIRED_AT}}], SETTLED, 7200))
        self.assertFalse(eligible([{"retirement": {"retired_at": RETIRED_AT}}], SETTLED - 10**9, 7200))
        self.assertFalse(eligible([{"retirement": None}], 10**20, 7200))  # not retired yet


if __name__ == "__main__":
    unittest.main()
