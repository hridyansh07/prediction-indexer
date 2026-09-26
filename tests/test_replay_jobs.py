from __future__ import annotations

import hashlib
import json
import random
import sqlite3
import tempfile
import threading
import unittest
from dataclasses import replace
from pathlib import Path

from replay.jobs import contracts as c
from universe.auth import Principal
from universe.replay_jobs import (
    ReplayJobError,
    ReplayJobStore,
)


ROOT = Path(__file__).resolve().parents[1]
ADDRESS = "0x" + "11" * 20
OTHER = "0x" + "22" * 20
SECOND = 1_000_000_000


def runner_config() -> c.RunnerConfig:
    return c.parse_runner_config((ROOT / "configs/replay_runner.json").read_bytes())


def request_bytes(bundle_id: str = "bundle-1", *, pretty: bool = False) -> bytes:
    document = {
        "replay_request_version": 1,
        "bundle_id": bundle_id,
        "probe_markets": None,
        "interval": None,
        "strategy": {"name": "bundle_coverage", "config": {}},
        "limits": "small",
    }
    return json.dumps(
        document,
        sort_keys=not pretty,
        separators=None if pretty else (",", ":"),
    ).encode()


class Limits:
    max_active_jobs_total = 100
    max_active_jobs_per_submitter = 4
    max_queued_jobs_total = 64


class ReplayJobStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "jobs.sqlite3"
        suffixes = iter(f"{value:016x}" for value in range(1000))
        self.store = ReplayJobStore(self.path, Limits(), suffix=lambda: next(suffixes))
        self.store.initialize()
        self.config = runner_config()
        self.principal = Principal(ADDRESS, "member")

    def tearDown(self) -> None:
        self.temp.cleanup()

    def submit(
        self,
        key: str = "key-1",
        *,
        raw: bytes | None = None,
        principal: Principal | None = None,
        now: int = 10 * SECOND,
        bundle_exists: bool = True,
    ):
        raw = raw or request_bytes()
        request = c.parse_request(raw, self.config)
        return self.store.submit(
            raw,
            request,
            principal or self.principal,
            key,
            now,
            bundle_exists=bundle_exists,
        )

    def test_spec_requires_durable_submitter_scoped_idempotency(self) -> None:
        spec = (ROOT / "docs/REPLAY_JOBS_V1.md").read_text(encoding="utf-8")
        self.assertIn("Idempotency-Key", spec)
        self.assertIn("scoped to\n  the authenticated lowercase submitter", spec)
        self.assertNotIn("Submission is at least once", spec)
        self.assertNotIn("There is no idempotency key", spec)

    def test_initialize_coexists_with_auth_and_rejects_owned_tampering(self) -> None:
        from tests.test_replay_auth import auth_config
        from universe.auth import AuthStore

        auth = AuthStore(self.path, auth_config())
        auth.initialize()
        auth.add_member(ADDRESS, "member", auth.config.admin_address)
        self.store.initialize()
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM allowlist").fetchone()[0], 1)
            connection.execute("DROP TRIGGER job_events_no_delete")
        with self.assertRaisesRegex(ValueError, "invalid replay jobs schema"):
            self.store.initialize()

    def test_metadata_hashes_normalized_owned_schema_not_sql_file_bytes(self) -> None:
        with sqlite3.connect(self.path) as connection:
            stored = connection.execute(
                "SELECT schema_sha256 FROM replay_job_components "
                "WHERE component='replay_jobs'"
            ).fetchone()[0]
            actual = self.store._schema_digest(connection)
        raw = hashlib.sha256(
            (ROOT / "universe/schema/replay_jobs.sql").read_bytes()
        ).hexdigest()
        self.assertEqual(stored, actual)
        self.assertNotEqual(stored, raw)

    def test_nonempty_w0_table_requires_migration(self) -> None:
        other = Path(self.temp.name) / "legacy.sqlite3"
        with sqlite3.connect(other) as connection:
            connection.executescript(c.JOBS_SCHEMA_SQL)
            row = c.new_job(
                c.job_id(10 * SECOND, "0123456789abcdef"),
                submitted_by=ADDRESS,
                request_sha256="0" * 64,
                now_ns=10 * SECOND,
            )
            record = row.as_record() | {"request_json": b"{}"}
            connection.execute(
                f"INSERT INTO jobs({','.join(c.JOB_COLUMNS)}) VALUES ({','.join('?' for _ in c.JOB_COLUMNS)})",
                [record[name] for name in c.JOB_COLUMNS],
            )
        with self.assertRaisesRegex(ValueError, "migration required"):
            ReplayJobStore(other, Limits()).initialize()

    def test_submit_is_atomic_and_preserves_exact_first_bytes(self) -> None:
        raw = request_bytes(pretty=True)
        first = self.submit(raw=raw)
        semantic_replay = self.submit(raw=request_bytes(), now=20 * SECOND)
        self.assertTrue(first.created)
        self.assertFalse(semantic_replay.created)
        self.assertEqual(semantic_replay.row.job_id, first.row.job_id)
        row, stored = self.store.get_job(first.row.job_id)
        self.assertEqual(row, first.row)
        self.assertEqual(stored, raw)
        events, more = self.store.list_events(first.row.job_id, limit=100)
        self.assertFalse(more)
        self.assertEqual([event["event_type"] for event in events], ["submitted"])
        with sqlite3.connect(self.path) as connection:
            dump = "\n".join(connection.iterdump())
        self.assertNotIn("key-1", json.dumps(self.store.job_record(row, stored)))
        self.assertIn("key-1", dump)  # durable mapping, but never an API/job record field

    def test_conflicting_hash_and_distinct_keys(self) -> None:
        first = self.submit()
        with self.assertRaisesRegex(ReplayJobError, "idempotency key") as caught:
            self.submit(raw=request_bytes("bundle-2"), bundle_exists=True)
        self.assertEqual(caught.exception.status, 409)
        second = self.submit("key-2")
        self.assertNotEqual(second.row.job_id, first.row.job_id)

    def test_same_key_race_creates_one_job_and_event(self) -> None:
        barrier = threading.Barrier(2)
        results = []

        def worker() -> None:
            barrier.wait()
            results.append(self.submit())

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(result.created for result in results), [False, True])
        self.assertEqual(len({result.row.job_id for result in results}), 1)
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM jobs").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM job_events").fetchone()[0], 1)

    def test_job_id_collision_retries_without_overwrite(self) -> None:
        collision = "0000000000000000"
        store = ReplayJobStore(
            self.path,
            Limits(),
            suffix=iter((collision, collision, "0000000000000001")).__next__,
        )
        first = store.submit(
            request_bytes(), c.parse_request(request_bytes(), self.config), self.principal,
            "collision-1", 30 * SECOND, bundle_exists=True,
        )
        second = store.submit(
            request_bytes(), c.parse_request(request_bytes(), self.config), self.principal,
            "collision-2", 30 * SECOND, bundle_exists=True,
        )
        self.assertNotEqual(first.row.job_id, second.row.job_id)

    def test_unknown_bundle_only_rejects_new_mapping(self) -> None:
        first = self.submit()
        replayed = self.submit(bundle_exists=False)
        self.assertEqual(replayed.row.job_id, first.row.job_id)
        with self.assertRaises(ReplayJobError) as caught:
            self.submit("new-key", bundle_exists=False)
        self.assertEqual(caught.exception.status, 404)

    def test_quota_boundaries_and_replay_bypass(self) -> None:
        class Tight:
            max_active_jobs_total = 2
            max_active_jobs_per_submitter = 1
            max_queued_jobs_total = 2

        store = ReplayJobStore(self.path, Tight(), suffix=lambda: "abcdefabcdefabcd")
        first = self.submit()
        replay = store.submit(
            request_bytes(), c.parse_request(request_bytes(), self.config),
            Principal(ADDRESS, "admin"), "key-1", 20 * SECOND, bundle_exists=False,
        )
        self.assertFalse(replay.created)
        with self.assertRaises(ReplayJobError) as caught:
            store.submit(
                request_bytes(), c.parse_request(request_bytes(), self.config),
                Principal(ADDRESS, "admin"), "another", 20 * SECOND, bundle_exists=True,
            )
        self.assertEqual(caught.exception.status, 429)
        other = store.submit(
            request_bytes(), c.parse_request(request_bytes(), self.config),
            Principal(OTHER, "member"), "other", 20 * SECOND, bundle_exists=True,
        )
        self.assertTrue(other.created)
        with self.assertRaises(ReplayJobError) as caught:
            store.submit(
                request_bytes(), c.parse_request(request_bytes(), self.config),
                Principal("0x" + "33" * 20, "member"), "third", 20 * SECOND,
                bundle_exists=True,
            )
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(len(self.store.list_jobs(limit=100)[0]), 2)
        self.assertNotEqual(first.row.job_id, other.row.job_id)

    def test_terminal_frees_quota_but_archive_blocked_does_not(self) -> None:
        class One:
            max_active_jobs_total = 1
            max_active_jobs_per_submitter = 1
            max_queued_jobs_total = 1

        store = ReplayJobStore(self.path, One())
        first = self.submit().row
        row = store.claim_next(self.config.orchestration, 20 * SECOND).row
        for stage, now in zip(c.WORK_STAGES[1:], range(30, 70, 10)):
            after = c.advance(row, stage, now * SECOND)
            row = store.save(row, after)
        archiving = c.succeed(row, 80 * SECOND)
        row = store.save(row, archiving)
        terminal = c.finish(row, 90 * SECOND)
        store.save(row, terminal)
        replacement = store.submit(
            request_bytes(), c.parse_request(request_bytes(), self.config),
            self.principal, "replacement", 100 * SECOND, bundle_exists=True,
        )
        self.assertTrue(replacement.created)
        self.assertNotEqual(first.job_id, replacement.row.job_id)

        cancelled = c.cancel(replacement.row, 110 * SECOND)
        store.cancel_job(replacement.row.job_id, self.principal, 110 * SECOND)
        blocked = c.block_archive(cancelled, "archive_conflict", 120 * SECOND)
        store.save(cancelled, blocked)
        with self.assertRaises(ReplayJobError) as caught:
            store.submit(
                request_bytes(), c.parse_request(request_bytes(), self.config),
                self.principal, "blocked-still-active", 130 * SECOND,
                bundle_exists=True,
            )
        self.assertEqual(caught.exception.status, 429)

    def test_cancel_permissions_status_and_single_event(self) -> None:
        created = self.submit()
        with self.assertRaises(ReplayJobError) as caught:
            self.store.cancel_job(created.row.job_id, Principal(OTHER, "member"), 20 * SECOND)
        self.assertEqual(caught.exception.status, 403)
        cancelled = self.store.cancel_job(
            created.row.job_id, Principal(OTHER, "admin"), 20 * SECOND
        )
        self.assertEqual(cancelled.pending_outcome, c.CANCELLED)
        with self.assertRaises(ReplayJobError) as caught:
            self.store.cancel_job(created.row.job_id, self.principal, 30 * SECOND)
        self.assertEqual(caught.exception.status, 409)
        events, _ = self.store.list_events(created.row.job_id, limit=100)
        self.assertEqual([event["event_type"] for event in events], ["submitted", "cancelled"])

    def test_claim_modes_crash_resume_and_cancelled_initialize(self) -> None:
        created = self.submit()
        claimed = self.store.claim_next(self.config.orchestration, 20 * SECOND)
        self.assertEqual(claimed.mode, "initialize")
        resumed = self.store.claim_next(
            self.config.orchestration,
            claimed.row.next_attempt_at_ns,
        )
        self.assertEqual(resumed.mode, "resume")
        self.assertEqual(resumed.row.stage_attempts, 2)

        cancelled = self.submit("cancelled", now=30 * SECOND)
        self.store.cancel_job(cancelled.row.job_id, self.principal, 31 * SECOND)
        claimed_cancel = self.store.claim_next(self.config.orchestration, 40 * SECOND)
        self.assertEqual(claimed_cancel.row.job_id, cancelled.row.job_id)
        self.assertEqual(claimed_cancel.mode, "initialize")

    def test_claim_exact_boundary_and_priority(self) -> None:
        first = self.submit(now=10 * SECOND)
        claim = self.store.claim_next(self.config.orchestration, 20 * SECOND)
        second = self.submit("second", now=30 * SECOND)
        selected = self.store.claim_next(
            self.config.orchestration, claim.row.next_attempt_at_ns - 1
        )
        self.assertEqual(selected.row.job_id, second.row.job_id)
        selected = self.store.claim_next(
            self.config.orchestration, claim.row.next_attempt_at_ns
        )
        self.assertEqual(selected.row.job_id, first.row.job_id)

    def test_randomized_sql_claim_choice_matches_reference(self) -> None:
        orchestration = self.config.orchestration
        raw = request_bytes()
        digest = c.request_sha256(c.parse_request(raw, self.config))
        rng = random.Random(7719)
        for seed in range(20):
            path = Path(self.temp.name) / f"random-{seed}.sqlite3"
            store = ReplayJobStore(path, Limits())
            store.initialize()
            now = 1_000 * SECOND
            rows = []
            for index in range(12):
                created = (index + 1) * SECOND
                row = c.new_job(
                    c.job_id(created, f"{index:016x}"),
                    submitted_by=ADDRESS,
                    request_sha256=digest,
                    now_ns=created,
                )
                kind = rng.randrange(5)
                if kind == 1:
                    row = c.claim(row, orchestration, created + 1)
                elif kind == 2:
                    row = c.claim(row, orchestration, created + 1)
                    row = c.retry_later(
                        row, "resource_exhausted", "wait", orchestration, now
                    )
                elif kind == 3:
                    row = c.cancel(row, created + 1)
                elif kind == 4:
                    row = c.block_archive(
                        c.cancel(row, created + 1), "archive_conflict", created + 2
                    )
                rows.append(row)
            with sqlite3.connect(path) as connection:
                for row in rows:
                    record = row.as_record() | {"request_json": raw}
                    connection.execute(
                        f"INSERT INTO jobs({','.join(c.JOB_COLUMNS)}) "
                        f"VALUES ({','.join('?' for _ in c.JOB_COLUMNS)})",
                        [record[name] for name in c.JOB_COLUMNS],
                    )
            expected = c.select_next(rows, now)
            actual = store.claim_next(orchestration, now)
            self.assertEqual(
                None if actual is None else actual.row.job_id,
                None if expected is None else expected.job_id,
            )

    def test_concurrent_claim_returns_one_owner(self) -> None:
        created = self.submit().row
        barrier = threading.Barrier(2)
        claims = []

        def claim() -> None:
            barrier.wait()
            claims.append(self.store.claim_next(self.config.orchestration, 20 * SECOND))

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        owned = [result for result in claims if result is not None]
        self.assertEqual(len(owned), 1)
        self.assertEqual(owned[0].row.job_id, created.job_id)

    def test_save_cas_is_exact_and_emits_one_transition_event(self) -> None:
        created = self.submit()
        claimed = self.store.claim_next(self.config.orchestration, 20 * SECOND)
        after = c.advance(claimed.row, "bundle", 30 * SECOND)
        saved = self.store.save(claimed.row, after)
        self.assertEqual(saved, after)
        with self.assertRaises(ReplayJobError) as caught:
            self.store.save(claimed.row, after)
        self.assertEqual(caught.exception.status, 409)
        corrupt_before = replace(after, reason_detail="corrupt")
        corrupt_after = replace(after, updated_at_ns=40 * SECOND)
        with self.assertRaises(ReplayJobError):
            self.store.save(corrupt_before, corrupt_after)
        events, _ = self.store.list_events(created.row.job_id, limit=100)
        self.assertEqual(events[-1]["event_type"], "stage_advanced")

    def test_save_records_each_legal_transition_once_and_rejects_stage_skip(self) -> None:
        self.submit()
        row = self.store.claim_next(self.config.orchestration, 20 * SECOND).row
        with self.assertRaises(ReplayJobError):
            self.store.save(row, replace(row, stage="read", updated_at_ns=21 * SECOND))
        retried = c.retry_later(
            row, "resource_exhausted", "busy", self.config.orchestration, 30 * SECOND
        )
        row = self.store.save(row, retried)
        row = self.store.claim_next(
            self.config.orchestration, row.next_attempt_at_ns
        ).row
        for stage, now in zip(c.WORK_STAGES[1:], range(100, 140, 10)):
            after = c.advance(row, stage, now * SECOND)
            row = self.store.save(row, after)
        after = c.fail(row, "tool_failure", "failed", 150 * SECOND)
        row = self.store.save(row, after)
        after = c.lose_local_state(row, "missing", 160 * SECOND)
        row = self.store.save(row, after)
        after = c.block_archive(row, "archive_conflict", 170 * SECOND)
        row = self.store.save(row, after)
        after = c.resume_blocked(row, 180 * SECOND)
        row = self.store.save(row, after)
        after = c.finish(row, 190 * SECOND)
        self.store.save(row, after)
        events, _ = self.store.list_events(after.job_id, limit=100)
        event_types = [event["event_type"] for event in events]
        for expected in (
            "retry_scheduled",
            "stage_advanced",
            "outcome_pending",
            "outcome_replaced",
            "archive_blocked",
            "archive_resumed",
            "finished",
        ):
            self.assertIn(expected, event_types)

    def test_events_are_append_only(self) -> None:
        self.submit()
        with sqlite3.connect(self.path) as connection:
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("DELETE FROM job_events")
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("UPDATE job_events SET event_type='finished'")

    def test_list_order_pagination_status_and_serialization(self) -> None:
        older = self.submit("older", now=10 * SECOND).row
        newer = self.submit("newer", now=20 * SECOND).row
        rows, more = self.store.list_jobs(limit=1)
        self.assertEqual(rows, [newer])
        self.assertTrue(more)
        rows, more = self.store.list_jobs(limit=1, after=(newer.created_at_ns, newer.job_id))
        self.assertEqual(rows, [older])
        self.assertFalse(more)
        record = self.store.job_record(older, request_bytes())
        self.assertEqual(record["created_at_ns"], str(older.created_at_ns))
        self.assertIn("request", record)
        self.assertNotIn("request_json", record)
        self.assertNotIn("idempotency_key", record)


if __name__ == "__main__":
    unittest.main()
