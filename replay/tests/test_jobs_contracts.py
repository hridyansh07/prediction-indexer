import hashlib
import json
import sqlite3
import unittest
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from archive.archiver.canonical import canonical_object_keys
from replay.jobs import contracts as c
from replay.tests.test_supervisor import normalizer

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "replay_runner.json"
JOB = "20260924T120000Z-0123456789abcdef"
ADDRESS = "0x" + "ab" * 20
SECOND = 1_000_000_000
HOUR = 3600 * SECOND
HALF = HOUR // 2
ORCH = c.Orchestration(max_stage_attempts=3, max_job_seconds=1000, retry_backoff_seconds=60)


def raw(value):
    return json.dumps(value).encode()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def runner_config():
    return c.parse_runner_config(CONFIG_PATH.read_bytes())


def request(**changes):
    value = {
        "replay_request_version": 1,
        "bundle_id": "bundle_0123456789abcdef01234567",
        "probe_markets": None,
        "interval": None,
        "strategy": {"name": "bundle_coverage", "config": {}},
        "limits": "small",
    }
    value.update(changes)
    return value


def producer(**changes):
    value = {
        "normalizer": c.freeze(normalizer()),
        "normalized_schema_version": 3,
        "materializer_version": 2,
        "materialization_policy_sha256": "e" * 64,
    }
    value.update(changes)
    return c.Producer(**value)


def windows(starts=(4 * HALF, 5 * HALF)):
    return tuple(
        c.BundleWindow(
            window_start_ns=start,
            window_end_ns=start + HALF,
            canonical_receipt_sha256=f"{i}" * 64,
            derivative_address=f"{i + 2}" * 64,
            receipt_sha256=f"{i + 4}" * 64,
        )
        for i, start in enumerate(starts)
    )


def bundle_receipt(**changes):
    value = {
        "bundle_id": "bundle_x",
        "start_ns": 4 * HALF,
        "end_ns": 6 * HALF,
        "canonical_window_seconds": 1800,
        "producer": producer(),
        "windows": windows(),
    }
    value.update(changes)
    return c.BundleReceipt(**value)


def queued(now=10 * SECOND):
    return c.new_job(JOB, submitted_by=ADDRESS, request_sha256="a" * 64, now_ns=now)


def running(stage="resolve", now=10 * SECOND):
    row = c.claim(queued(now), ORCH, now)
    for next_stage in c.WORK_STAGES[1 : c.WORK_STAGES.index(stage) + 1]:
        row = c.advance(row, next_stage, now)
    return row


def history(*times):
    return [
        c.HistoryEntry(
            run_id=f"run{i}",
            generated_at_ns=t,
            manifest_key=f"targeter-v2/runs/run{i}/run_manifest.json",
            manifest_sha256=f"{i % 10}" * 64,
            report_key=f"targeter-v2/runs/run{i}/selection_report.json.zst",
            report_sha256=f"{(i + 5) % 10}" * 64,
        )
        for i, t in enumerate(times)
    ]


def resolved(**changes):
    bundle, job, occurrences = c.resolve_occurrences(history(100, 200), 300, None)
    value = {
        "job_id": JOB,
        "request_sha256": "a" * 64,
        "bundle_id": "bundle_x",
        "canonical_window_seconds": 1800,
        "bundle_interval": bundle,
        "job_interval": job,
        "occurrences": occurrences,
    }
    value.update(changes)
    return c.ResolvedJob(**value)


def job_result(**changes):
    value = {
        "job_id": JOB,
        "strategy": "bundle_coverage",
        "strategy_semantic_sha256": "1" * 64,
        "snapshot_sha256": "2" * 64,
        "bundle_receipt_sha256": "3" * 64,
        "supervisor_identity": "4" * 64,
        "attempt_id": "5" * 32,
        "attempts_started": 1,
    }
    value.update(changes)
    return c.JobResult(**value)


def objects(*relative):
    return tuple(
        c.JobObject(key=c.job_object_key(JOB, r), sha256=hashlib.sha256(r.encode()).hexdigest(), byte_length=len(r))
        for r in sorted(relative)
    )


def job_receipt(outcome="succeeded", code=None, depth=5, **changes):
    chain = ("resolved_sha256", "bundle_receipt_sha256", "snapshot_sha256", "supervisor_identity", "strategy_semantic_sha256")
    value = {
        "job_id": JOB,
        "request_sha256": "a" * 64,
        "submitted_by": ADDRESS,
        "image_revision": "0123abc",
        "final_outcome": outcome,
        "reason_code": code,
        "reason_detail": None,
        **{name: (f"{i + 1}" * 64 if i < depth else None) for i, name in enumerate(chain)},
        "objects": objects("request.json", "resolved.json", "run/SUCCESS.json")[: max(depth, 1)],
        "created_at_ns": 1,
        "finished_at_ns": 2,
    }
    value.update(changes)
    return c.JobReceipt(**value)


class RequestTest(unittest.TestCase):
    def test_minimal_request_parses(self):
        parsed = c.parse_request(raw(request()), runner_config())
        self.assertEqual(parsed.bundle_id, "bundle_0123456789abcdef01234567")
        self.assertIsNone(parsed.interval)
        self.assertIsNone(parsed.probe_markets)
        self.assertEqual(len(parsed.sha256), 64)

    def test_interval_and_probe_parse(self):
        parsed = c.parse_request(
            raw(request(interval={"start_ns": "10", "end_ns": "20"}, probe_markets=["kalshi:A", "polymarket:1"])),
            runner_config(),
        )
        self.assertEqual(parsed.interval, (10, 20))
        self.assertEqual(parsed.probe_markets, ("kalshi:A", "polymarket:1"))

    def test_hash_ignores_whitespace_and_key_order(self):
        config = runner_config()
        self.assertEqual(
            c.parse_request(canonical(request()), config).sha256,
            c.parse_request(json.dumps(request(), indent=2).encode(), config).sha256,
        )

    def test_rejections(self):
        config = runner_config()
        cases = {
            "unknown field": raw({**request(), "extra": 1}),
            "missing field": raw({k: v for k, v in request().items() if k != "limits"}),
            "duplicate key": b'{"bundle_id":"a","bundle_id":"b"}',
            "bad version": raw(request(replay_request_version=2)),
            "bool version": raw(request(replay_request_version=True)),
            "traversing bundle": raw(request(bundle_id="..")),
            "slash bundle": raw(request(bundle_id="a/b")),
            "empty probe": raw(request(probe_markets=[])),
            "unsorted probe": raw(request(probe_markets=["polymarket:1", "kalshi:A"])),
            "duplicate probe": raw(request(probe_markets=["kalshi:A", "kalshi:A"])),
            "unqualified probe": raw(request(probe_markets=["A"])),
            "numeric interval": raw(request(interval={"start_ns": 1, "end_ns": 2})),
            "leading zero": raw(request(interval={"start_ns": "01", "end_ns": "2"})),
            "empty interval": raw(request(interval={"start_ns": "5", "end_ns": "5"})),
            "unknown strategy": raw(request(strategy={"name": "x", "config": {}})),
            "factory as name": raw(request(strategy={"name": "os:system", "config": {}})),
            "runner-owned key": raw(request(strategy={"name": "bundle_coverage", "config": {"snapshot_directory": "/tmp"}})),
            "unknown config key": raw(request(strategy={"name": "bundle_coverage", "config": {"x": 1}})),
            "unknown preset": raw(request(limits="huge")),
            "oversize": b" " * (c.MAX_REQUEST_BYTES + 1),
            "not json": b"{",
            "nan": b'{"replay_request_version": NaN}',
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_request(body, config)

    def test_parsed_request_is_immutable(self):
        parsed = c.parse_request(raw(request()), runner_config())
        with self.assertRaises(TypeError):
            parsed.document["bundle_id"] = "other"


class TransitionTest(unittest.TestCase):
    """Review items 1, 2, 3: every accepted job archives before it is terminal."""

    def test_success_path_archives_before_terminal(self):
        row = running("read")
        row = c.succeed(row, 20 * SECOND)
        self.assertEqual((row.status, row.pending_outcome, row.stage), ("archiving", "succeeded", "archive"))
        row = c.finish(row, 30 * SECOND)
        self.assertEqual(row.status, "succeeded")
        self.assertEqual(row.archive_receipt_key, c.job_receipt_key(JOB))

    def test_every_work_outcome_goes_through_archiving(self):
        for code, outcome in c.OUTCOME_OF_CODE.items():
            if code == "cancelled":
                continue
            with self.subTest(code):
                row = c.fail(running("bundle"), code, "detail", 20 * SECOND)
                self.assertEqual((row.status, row.pending_outcome, row.reason_code), ("archiving", outcome, code))
                self.assertEqual(c.finish(row, 30 * SECOND).status, outcome)

    def test_cancel_goes_through_archiving(self):
        row = c.cancel(queued(), 20 * SECOND)
        self.assertEqual((row.status, row.pending_outcome, row.stage_attempts), ("archiving", "cancelled", 0))
        self.assertEqual(c.finish(c.claim(row, ORCH, 20 * SECOND), 30 * SECOND).status, "cancelled")
        with self.assertRaises(c.ContractError):
            c.cancel(running(), 20 * SECOND)

    def test_terminal_status_cannot_bypass_archiving(self):
        for status in c.TERMINAL:
            with self.subTest(status), self.assertRaises(c.ContractError):
                replace(running(), status=status)
        with self.assertRaises(c.ContractError):
            c.finish(running("read"), 1)
        with self.assertRaises(c.ContractError):
            c.fail(queued(), "tool_failure", None, 1)
        with self.assertRaises(c.ContractError):
            c.fail(running(), "cancelled", None, 1)
        with self.assertRaises(c.ContractError):
            c.succeed(running("run"), 1)

    def test_every_terminal_row_requires_archive_receipt(self):
        done = c.finish(c.succeed(running("read"), 20 * SECOND), 30 * SECOND)
        for field, value in (("archive_receipt_key", None), ("finished_at_ns", None)):
            with self.subTest(field), self.assertRaises(c.ContractError):
                replace(done, **{field: value})
        with self.assertRaises(c.ContractError):
            replace(done, archive_receipt_key="replay/jobs/other/job_receipt.json")

    def test_pending_outcome_only_while_archiving(self):
        with self.assertRaises(c.ContractError):
            replace(running(), pending_outcome="failed")
        row = c.fail(running(), "tool_failure", None, 20 * SECOND)
        with self.assertRaises(c.ContractError):
            replace(row, pending_outcome=None)
        with self.assertRaises(c.ContractError):
            replace(row, reason_code="bundle_not_retired")  # outcome mismatch

    def test_stage_advance_is_ordered(self):
        row = running()
        with self.assertRaises(c.ContractError):
            c.advance(row, "prepare", 1)
        with self.assertRaises(c.ContractError):
            c.advance(running("read"), "archive", 1)


class SchedulingTest(unittest.TestCase):
    """Review item 2: circuit breaker, backoff, and no starvation."""

    def test_claim_counts_attempt_and_schedules_backoff(self):
        row = c.claim(queued(), ORCH, 10 * SECOND)
        self.assertEqual((row.status, row.stage, row.stage_attempts), ("running", "resolve", 1))
        self.assertEqual(row.next_attempt_at_ns, 70 * SECOND)
        self.assertFalse(c.claimable(row, 69 * SECOND))
        self.assertTrue(c.claimable(row, 70 * SECOND))
        with self.assertRaises(c.ContractError):
            c.claim(row, ORCH, 69 * SECOND)

    def test_advance_resets_attempts(self):
        row = c.claim(c.claim(queued(), ORCH, 0), ORCH, 60 * SECOND)
        self.assertEqual(row.stage_attempts, 2)
        self.assertEqual(c.advance(row, "bundle", 61 * SECOND).stage_attempts, 1)

    def test_retry_later_requires_retryable_code(self):
        row = c.retry_later(running(), "universe_unavailable", "503", ORCH, 20 * SECOND)
        self.assertEqual((row.status, row.reason_code, row.next_attempt_at_ns), ("running", "universe_unavailable", 80 * SECOND))
        for code in ("tool_failure", "integrity_failure", "cancelled"):
            with self.subTest(code), self.assertRaises(c.ContractError):
                c.retry_later(running(), code, None, ORCH, 20 * SECOND)

    def test_work_stage_exhaustion_moves_to_archived_failure(self):
        row = running()
        now = 10 * SECOND
        while row.status == "running":
            now += 60 * SECOND
            row = c.claim(row, ORCH, now)
        self.assertEqual((row.status, row.pending_outcome, row.reason_code), ("archiving", "failed", "stage_attempts_exhausted"))

    def test_job_deadline(self):
        row = running()
        row = c.claim(row, ORCH, 10 * SECOND + 1000 * SECOND)
        self.assertEqual(row.reason_code, "job_deadline_exceeded")
        self.assertEqual(row.pending_outcome, "failed")

    def test_archive_exhaustion_blocks_and_resumes_manually(self):
        row = c.fail(running(), "tool_failure", None, 10 * SECOND)
        now = 10 * SECOND
        while row.status == "archiving":
            now += 60 * SECOND
            row = c.claim(row, ORCH, now)
        self.assertEqual((row.status, row.blocked_reason_code, row.pending_outcome), ("archive_blocked", "stage_attempts_exhausted", "failed"))
        self.assertFalse(c.claimable(row, now + HOUR))
        resumed = c.resume_blocked(row, now + HOUR)
        self.assertEqual((resumed.status, resumed.stage_attempts), ("archiving", 0))
        self.assertTrue(c.claimable(resumed, now + HOUR))

    def test_blocked_or_waiting_jobs_do_not_starve_queued(self):
        waiting = c.retry_later(running(), "universe_unavailable", None, ORCH, 10 * SECOND)
        blocked = c.block_archive(c.fail(running(), "tool_failure", None, 10 * SECOND), "archive_unavailable", 10 * SECOND)
        fresh = replace(queued(20 * SECOND), job_id="20260924T120000Z-fedcba9876543210")
        self.assertIs(c.select_next([waiting, blocked, fresh], 30 * SECOND), fresh)
        self.assertIs(c.select_next([waiting, blocked, fresh], 70 * SECOND), waiting)
        self.assertIsNone(c.select_next([blocked], 70 * SECOND))

    def test_select_next_orders_by_age(self):
        older = queued(1)
        newer = replace(queued(2), job_id="20260924T120000Z-fedcba9876543210")
        self.assertIs(c.select_next([newer, older], 5), older)


class LocalStateTest(unittest.TestCase):
    """Review item 3."""

    def test_running_job_with_lost_state_fails_through_archiving(self):
        row = c.lose_local_state(running("run"), "job directory missing", 20 * SECOND)
        self.assertEqual((row.status, row.pending_outcome, row.reason_code), ("archiving", "failed", "local_state_lost"))
        self.assertEqual(row.job_id, JOB)  # never re-queued under a fresh identity

    def test_archiving_job_with_lost_state_fails(self):
        row = c.succeed(running("read"), 20 * SECOND)
        lost = c.lose_local_state(row, None, 30 * SECOND)
        self.assertEqual((lost.status, lost.pending_outcome, lost.reason_code), ("archiving", "failed", "local_state_lost"))

    def test_lost_state_never_restarts_work(self):
        row = c.lose_local_state(running("run"), None, 20 * SECOND)
        with self.assertRaises(c.ContractError):
            c.advance(row, "read", 30 * SECOND)
        with self.assertRaises(c.ContractError):
            c.lose_local_state(queued(), None, 1)


class JobsTableTest(unittest.TestCase):
    """Review items 3, 7: relational constraints mirror JobRow."""

    def insert(self, connection, row):
        record = row.as_record()
        record["request_json"] = b"{}"
        columns = ", ".join(c.JOB_COLUMNS)
        connection.execute(
            f"INSERT INTO jobs({columns}) VALUES ({', '.join('?' * len(c.JOB_COLUMNS))})",
            [record[name] for name in c.JOB_COLUMNS],
        )

    def rows(self):
        base = running("read")
        archiving = c.succeed(base, 20 * SECOND)
        return {
            "queued": queued(),
            "running": base,
            "retrying": c.retry_later(running(), "archive_unavailable", "s3", ORCH, 20 * SECOND),
            "archiving": archiving,
            "blocked": c.block_archive(archiving, "archive_conflict", 30 * SECOND),
            "succeeded": c.finish(archiving, 40 * SECOND),
            "failed": c.finish(c.fail(base, "tool_failure", "x", 20 * SECOND), 40 * SECOND),
            "cancelled": c.finish(c.cancel(queued(), 20 * SECOND), 40 * SECOND),
        }

    def test_every_valid_row_inserts(self):
        for name, row in self.rows().items():
            with self.subTest(name), closing(sqlite3.connect(":memory:")) as connection:
                connection.executescript(c.JOBS_SCHEMA_SQL)
                connection.executescript(c.JOBS_SCHEMA_SQL)  # idempotent
                self.insert(connection, row)

    def test_database_rejects_invalid_rows(self):
        rows = self.rows()
        updates = {
            "terminal without receipt": "UPDATE jobs SET archive_receipt_key = NULL WHERE status = 'succeeded'",
            "terminal without finish": "UPDATE jobs SET finished_at_ns = NULL WHERE status = 'failed'",
            "wrong receipt key": "UPDATE jobs SET archive_receipt_key = 'x' WHERE status = 'succeeded'",
            "pending outside archiving": "UPDATE jobs SET pending_outcome = 'failed' WHERE status = 'running'",
            "archiving without pending": "UPDATE jobs SET pending_outcome = NULL WHERE status = 'archiving'",
            "blocked without code": "UPDATE jobs SET blocked_reason_code = NULL WHERE status = 'archive_blocked'",
            "nonterminal claims completion": "UPDATE jobs SET finished_at_ns = 1 WHERE status = 'running'",
            "queued with stage": "UPDATE jobs SET stage = 'resolve' WHERE status = 'queued'",
            "failed without code": "UPDATE jobs SET reason_code = NULL WHERE status = 'failed'",
            "succeeded with code": "UPDATE jobs SET reason_code = 'tool_failure' WHERE status = 'succeeded'",
            "scheduled terminal": "UPDATE jobs SET next_attempt_at_ns = 1 WHERE status = 'succeeded'",
            "bad hash": "UPDATE jobs SET request_sha256 = 'A' || substr(request_sha256, 2) WHERE status = 'queued'",
            "bad address": "UPDATE jobs SET submitted_by = '0xZZ' WHERE status = 'queued'",
            "unknown code": "UPDATE jobs SET reason_code = 'oops' WHERE status = 'failed'",
        }
        for name, statement in updates.items():
            with self.subTest(name), closing(sqlite3.connect(":memory:")) as connection:
                connection.executescript(c.JOBS_SCHEMA_SQL)
                for index, row in enumerate(rows.values()):
                    self.insert(connection, replace(row, job_id=f"20260924T120000Z-{index:016x}", archive_receipt_key=None if row.archive_receipt_key is None else c.job_receipt_key(f"20260924T120000Z-{index:016x}")))
                with self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(statement)


class IdentifierTest(unittest.TestCase):
    def test_job_id(self):
        value = c.job_id(1_785_412_800_123_456_789, "0123456789abcdef")
        self.assertEqual(value, "20260730T120000Z-0123456789abcdef")
        for bad in ("20260730T120000Z-0123", "x", "20260730T120000Z-0123456789ABCDEF"):
            with self.subTest(bad), self.assertRaises(c.ContractError):
                c.check_job_id(bad)

    def test_date_partition_matches_finalizer_vectors(self):
        self.assertEqual(c.date_partition(0), "1970-01-01")
        self.assertEqual(c.date_partition(1_785_412_800_000_000_000), "2026-07-30")
        self.assertEqual(c.date_partition(1_709_164_800_000_000_000), "2024-02-29")

    def test_canonical_keys_match_archiver(self):
        for start in (0, 1_785_412_800_000_000_000, 1_709_164_800_000_000_000 - HALF):
            with self.subTest(start):
                receipt = Path(f"canonical/date={c.date_partition(start)}/window={start}/receipt.json")
                self.assertEqual(c.canonical_window_keys(start), canonical_object_keys(receipt))

    def test_object_keys_reject_unnormalized_paths(self):
        """Review item 7."""
        self.assertEqual(c.job_object_key(JOB, "run/SUCCESS.json"), f"replay/jobs/{JOB}/run/SUCCESS.json")
        for bad in ("../x", "a//b", "a/./b", "a\\b", "a\x00b", "/abs", "", "job_receipt.json"):
            with self.subTest(repr(bad)), self.assertRaises(c.ContractError):
                c.job_object_key(JOB, bad)
        for call in (
            lambda: c.derivative_key("A" * 64, "receipt.json"),
            lambda: c.derivative_key("a" * 64, "other.json"),
            lambda: c.bundle_receipt_key("a/b", "0" * 64),
        ):
            with self.assertRaises(c.ContractError):
                call()

    def test_window_bounds(self):
        self.assertEqual(c.window_bounds(HALF + 1, 2 * HALF + 1, 1800), (HALF, 3 * HALF, (HALF, 2 * HALF)))
        for seconds in (0, 7, 1800.0):
            with self.subTest(seconds), self.assertRaises(c.ContractError):
                c.window_bounds(0, 1, seconds)

    def test_reason_detail_is_bounded_and_redacted(self):
        self.assertEqual(c.reason_detail("redis://user:secret@host:6379/0 down"), "redis://***@host:6379/0 down")
        self.assertEqual(len(c.reason_detail("x" * 5000)), c.MAX_REASON_DETAIL)
        self.assertEqual(c.reason_detail("a\nb\x00c"), "a b c")
        long = c.reason_detail("y" * 5000)
        self.assertEqual(c.reason_detail(long), long)


class ProducerTest(unittest.TestCase):
    """Review item 4: every producer field independently changes identity."""

    def test_each_field_changes_identity(self):
        base = producer()
        other_normalizer = normalizer()
        other_normalizer["venues"][0]["parser_version"] = 2
        variants = {
            "normalizer": producer(normalizer=c.freeze(other_normalizer)),
            "schema": producer(normalized_schema_version=4),
            "materializer": producer(materializer_version=3),
            "policy": producer(materialization_policy_sha256="f" * 64),
        }
        for name, variant in variants.items():
            with self.subTest(name):
                self.assertNotEqual(variant, base)
                self.assertNotEqual(variant.document(), base.document())
        self.assertEqual(producer(), base)

    def test_describe_output_round_trip(self):
        body = canonical(producer().document())
        self.assertEqual(c.parse_producer(body), producer())
        for bad in (
            json.dumps(producer().document(), indent=1).encode(),
            canonical({**producer().document(), "producer_identity_version": 2}),
            canonical({**producer().document(), "normalizer": {"identity_version": 2}}),
        ):
            with self.assertRaises(c.ContractError):
                c.parse_producer(bad)


class BundleReceiptTest(unittest.TestCase):
    def test_round_trip_is_byte_exact(self):
        body = c.bundle_receipt_bytes(bundle_receipt())
        self.assertEqual(c.parse_bundle_receipt(body), bundle_receipt())
        self.assertEqual(c.bundle_receipt_bytes(c.parse_bundle_receipt(body)), body)

    def test_independent_builds_publish_identical_bytes(self):
        self.assertEqual(c.bundle_receipt_bytes(bundle_receipt()), c.bundle_receipt_bytes(bundle_receipt()))
        self.assertNotIn(b"built_by_job", c.bundle_receipt_bytes(bundle_receipt()))

    def test_generation_identity_binds_interval_sources_producer_and_pins(self):
        base = bundle_receipt()
        identity = c.bundle_generation_sha256(base)
        variants = (
            bundle_receipt(bundle_id="bundle_y"),
            bundle_receipt(start_ns=3 * HALF, windows=windows((3 * HALF, 4 * HALF, 5 * HALF))),
            bundle_receipt(producer=producer(materializer_version=3)),
            bundle_receipt(windows=(replace(base.windows[0], canonical_receipt_sha256="f" * 64), base.windows[1])),
            bundle_receipt(windows=(replace(base.windows[0], receipt_sha256="f" * 64), base.windows[1])),
        )
        for variant in variants:
            with self.subTest(variant=variant):
                self.assertNotEqual(c.bundle_generation_sha256(variant), identity)
        self.assertEqual(
            c.bundle_receipt_key(base.bundle_id, identity),
            f"replay/bundles/{base.bundle_id}/generations/{identity}/bundle_receipt.json",
        )

    def test_direct_construction_of_invalid_receipt_fails(self):
        """Review item 7."""
        good = windows()
        cases = {
            "uppercase hash": lambda: replace(good[0], receipt_sha256="A" * 64),
            "empty window": lambda: replace(good[0], window_end_ns=good[0].window_start_ns),
            "unaligned": lambda: bundle_receipt(start_ns=4 * HALF + 1),
            "gap": lambda: bundle_receipt(end_ns=7 * HALF),
            "reordered": lambda: bundle_receipt(windows=good[::-1]),
            "list windows": lambda: bundle_receipt(windows=list(good)),
            "repeated address": lambda: bundle_receipt(windows=(good[0], replace(good[1], derivative_address=good[0].derivative_address))),
            "bad producer": lambda: producer(materializer_version=0),
        }
        for name, build in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                build()

    def test_serialization_revalidates_bypassed_fields(self):
        receipt = bundle_receipt()
        object.__setattr__(receipt.windows[0], "receipt_sha256", "not-a-hash")
        with self.assertRaises(c.ContractError):
            c.bundle_receipt_bytes(receipt)

    def test_single_field_tampering(self):
        document = json.loads(c.bundle_receipt_bytes(bundle_receipt()))
        tampered = [
            {**document, "extra": 1},
            {**document, "replay_bundle_receipt_version": 2},
            {**document, "bundle_id": "../x"},
            {**document, "canonical_window_seconds": 7},
            {**document, "producer": {**document["producer"], "materializer_version": "2"}},
            {**document, "windows": [{**document["windows"][0], "receipt_sha256": "A" * 64}, document["windows"][1]]},
            {**document, "interval": {**document["interval"], "end_ns": str(7 * HALF)}},
        ]
        for index, value in enumerate(tampered):
            with self.subTest(index), self.assertRaises(c.ContractError):
                c.parse_bundle_receipt(canonical(value))
        with self.assertRaises(c.ContractError):
            c.parse_bundle_receipt(json.dumps(document, indent=1).encode())


class HistoryTest(unittest.TestCase):
    """Review item 8."""

    def test_occurrences_partition_the_bundle(self):
        bundle, job, occurrences = c.resolve_occurrences(history(200, 100), 300, None)
        self.assertEqual(bundle, (100, 300))
        self.assertEqual(job, bundle)
        self.assertEqual([(o.run_id, o.start_ns, o.end_ns) for o in occurrences], [("run1", 100, 200), ("run0", 200, 300)])

    def test_job_interval_clips_occurrences(self):
        _, job, occurrences = c.resolve_occurrences(history(100, 200, 250), 300, (150, 260))
        self.assertEqual(job, (150, 260))
        self.assertEqual([(o.start_ns, o.end_ns) for o in occurrences], [(150, 200), (200, 250), (250, 260)])

    def test_equal_timestamps_fail_deterministically(self):
        for order in ((100, 100), (100, 200, 200)):
            with self.subTest(order), self.assertRaises(c.ContractError) as caught:
                c.resolve_occurrences(history(*order), 300, None)
            self.assertEqual(caught.exception.code, "bundle_history_invalid")
        messages = set()
        for entries in (history(100, 100), list(reversed(history(100, 100)))):
            with self.assertRaises(c.ContractError) as caught:
                c.resolve_occurrences(entries, 300, None)
            messages.add(str(caught.exception))
        self.assertEqual(len(messages), 1)

    def test_invalid_history(self):
        for args, code in (
            ((history(), 300, None), "bundle_history_invalid"),
            ((history(100, 200), 200, None), "bundle_history_invalid"),
            ((history(100, 200), 300, (50, 150)), "interval_out_of_range"),
            ((history(100, 200), 300, (150, 350)), "interval_out_of_range"),
        ):
            with self.subTest(args[1:]), self.assertRaises(c.ContractError) as caught:
                c.resolve_occurrences(*args)
            self.assertEqual(caught.exception.code, code)


class StageReceiptTest(unittest.TestCase):
    """Review items 5, 9, 10."""

    def test_resolved_job_round_trip_and_tampering(self):
        body = c.resolved_job_bytes(resolved())
        self.assertEqual(c.parse_resolved_job(body), resolved())
        document = json.loads(body)
        self.assertEqual(document["window_interval"], {"start_ns": "0", "end_ns": str(HALF)})
        tampered = [
            {**document, "extra": 1},
            {**document, "window_interval": {"start_ns": "0", "end_ns": str(HOUR)}},
            {**document, "job_interval": {"start_ns": "100", "end_ns": "299"}},
            {**document, "occurrences": document["occurrences"][:1]},
            {**document, "occurrences": [{**document["occurrences"][0], "source": {**document["occurrences"][0]["source"], "report_key": "a//b"}}, document["occurrences"][1]]},
            {**document, "request_sha256": "A" * 64},
        ]
        for index, value in enumerate(tampered):
            with self.subTest(index), self.assertRaises(c.ContractError):
                c.parse_resolved_job(canonical(value))

    def test_job_result_round_trip_and_tampering(self):
        body = c.job_result_bytes(job_result())
        self.assertEqual(c.parse_job_result(body), job_result())
        document = json.loads(body)
        for name, value in (("attempt_id", "x"), ("attempts_started", 0), ("strategy", "a/b"), ("snapshot_sha256", None), ("extra", 1)):
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_job_result(canonical({**document, name: value}))

    def test_job_receipt_round_trip_and_tampering(self):
        body = c.job_receipt_bytes(job_receipt())
        self.assertEqual(c.parse_job_receipt(body), job_receipt())
        document = json.loads(body)
        tampered = {
            "extra field": {**document, "extra": 1},
            "outcome": {**document, "final_outcome": "running"},
            "code on success": {**document, "reason_code": "tool_failure"},
            "missing identity": {**document, "snapshot_sha256": None},
            "foreign object": {**document, "objects": [{"key": "replay/jobs/other/x", "sha256": "0" * 64, "byte_length": 1}]},
            "unsorted objects": {**document, "objects": document["objects"][::-1]},
            "receipt listed": {**document, "objects": [{"key": c.job_receipt_key(JOB), "sha256": "0" * 64, "byte_length": 1}]},
            "numeric time": {**document, "created_at_ns": 1},
            "finished first": {**document, "created_at_ns": "3"},
            "bad revision": {**document, "image_revision": ""},
        }
        for name, value in tampered.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_job_receipt(canonical(value))

    def test_partial_receipts_use_null_identities(self):
        cases = {
            "cancelled": job_receipt("cancelled", "cancelled", depth=0),
            "not_ready": job_receipt("not_ready", "canonical_not_archived", depth=1),
            "stale": job_receipt("stale_bundle_cache", "stale_bundle_cache", depth=1),
            "failed early": job_receipt("failed", "universe_unavailable", depth=0),
            "failed run": job_receipt("failed", "supervisor_failed", depth=3),
            "exhausted": job_receipt("exhausted", "supervisor_exhausted", depth=4),
        }
        for name, receipt in cases.items():
            with self.subTest(name):
                body = c.job_receipt_bytes(receipt)
                self.assertEqual(c.parse_job_receipt(body), receipt)

    def test_partial_receipt_rules(self):
        with self.assertRaises(c.ContractError):
            job_receipt(depth=3)  # succeeded needs every identity
        with self.assertRaises(c.ContractError):
            job_receipt("failed", "tool_failure", depth=2, resolved_sha256=None)  # gap in chain
        with self.assertRaises(c.ContractError):
            job_receipt("not_ready", "tool_failure", depth=1)  # code/outcome mismatch
        with self.assertRaises(c.ContractError):
            job_receipt("failed", "archive_unavailable", depth=1)  # block code is not an outcome

    def test_receipt_size_bound(self):
        many = tuple(
            c.JobObject(key=c.job_object_key(JOB, f"run/{i:05}"), sha256="0" * 64, byte_length=1)
            for i in range(c.MAX_JOB_OBJECTS + 1)
        )
        with self.assertRaises(c.ContractError):
            job_receipt(objects=many)


class RunnerConfigTest(unittest.TestCase):
    def test_shipped_config_parses(self):
        """Review item 12."""
        config = runner_config()
        self.assertEqual(config.canonical_window_seconds, 1800)
        self.assertEqual(set(config.authorities), {"kalshi", "limitless", "polymarket"})
        self.assertEqual(config.orchestration, c.Orchestration(20, 86400, 60))
        self.assertIn("small", config.limits)

    def test_rejections(self):
        base = json.loads(CONFIG_PATH.read_bytes())
        small = base["limits"]["small"]
        orchestration = base["orchestration"]

        def with_(**changes):
            return raw({**base, **changes})

        cases = {
            "unknown field": with_(extra=1),
            "missing orchestration": raw({k: v for k, v in base.items() if k != "orchestration"}),
            "zero attempts": with_(orchestration={**orchestration, "max_stage_attempts": 0}),
            "float backoff": with_(orchestration={**orchestration, "retry_backoff_seconds": 1.5}),
            "run beyond job deadline": with_(orchestration={**orchestration, "max_job_seconds": 900}),
            "credential url": with_(universe_base_url="http://u:p@host"),
            "relative url": with_(universe_base_url="event-universe:8080"),
            "window seconds": with_(canonical_window_seconds=7),
            "missing venue": with_(authorities={"kalshi": "kalshi", "limitless": "limitless"}),
            "bad factory": with_(strategies={"x": {"factory": "nope", "reader": "a:b", "config_schema": "bundle_coverage_v1"}}),
            "unknown schema": with_(strategies={"x": {"factory": "a:b", "reader": "a:b", "config_schema": "nope"}}),
            "limits order": with_(limits={"small": {**small, "stall_seconds": 1000}}),
            "float attempts": with_(limits={"small": {**small, "attempts": 3.0}}),
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_runner_config(body)


if __name__ == "__main__":
    unittest.main()
