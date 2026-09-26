"""Durable SQLite control plane for Replay jobs."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Literal

from replay.jobs import contracts as jobs
from universe.auth import Principal


SCHEMA_PATH = Path(__file__).with_name("schema") / "replay_jobs.sql"
SCHEMA_VERSION = 1
IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9._~-]{1,128}\Z")
ACTIVE_STATUSES = tuple(sorted(jobs.STATUSES - jobs.TERMINAL))


@dataclass(frozen=True)
class SubmissionResult:
    row: jobs.JobRow
    created: bool


@dataclass(frozen=True)
class ClaimResult:
    row: jobs.JobRow
    request_bytes: bytes
    mode: Literal["initialize", "resume"]


class ReplayJobError(Exception):
    """An HTTP-safe control-plane failure."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


class ReplayJobStore:
    def __init__(
        self,
        path: Path,
        limits: Any,
        *,
        suffix: Callable[[], str] | None = None,
    ) -> None:
        self.path = Path(path)
        self.limits = limits
        self._suffix = suffix or (lambda: secrets.token_hex(8))

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        return connection

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        schema_bytes = SCHEMA_PATH.read_bytes()
        digest = hashlib.sha256(schema_bytes).hexdigest()
        with closing(self.connect()) as connection:
            try:
                has_metadata = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' "
                    "AND name='replay_job_components'"
                ).fetchone() is not None
                if has_metadata:
                    self._validate_schema(connection)
                    metadata = connection.execute(
                        "SELECT schema_version,schema_sha256 FROM replay_job_components "
                        "WHERE component='replay_jobs'"
                    ).fetchone()
                    if metadata is None or (int(metadata[0]), str(metadata[1])) != (
                        SCHEMA_VERSION,
                        digest,
                    ):
                        raise ValueError("database contains an invalid replay jobs schema")
                    return
                has_jobs = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='jobs'"
                ).fetchone() is not None
                count = (
                    int(connection.execute("SELECT count(*) FROM jobs").fetchone()[0])
                    if has_jobs
                    else 0
                )
                if count:
                    raise ValueError(
                        "replay jobs migration required: existing W0 jobs have no event history"
                    )
                connection.executescript(jobs.JOBS_SCHEMA_SQL)
                connection.executescript(schema_bytes.decode("utf-8"))
                connection.execute(
                    "INSERT INTO replay_job_components VALUES ('replay_jobs',?,?)",
                    (SCHEMA_VERSION, digest),
                )
                connection.commit()
            except sqlite3.DatabaseError as error:
                raise ValueError("database contains an invalid replay jobs schema") from error
            self._validate_schema(connection)

    @staticmethod
    def _schema_objects(connection: sqlite3.Connection) -> dict[tuple[str, str], str]:
        return {
            (str(row[0]), str(row[1])): " ".join(str(row[2]).split())
            for row in connection.execute(
                "SELECT type,name,sql FROM sqlite_master "
                "WHERE type IN ('table','index','trigger') AND sql IS NOT NULL "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }

    @classmethod
    def _expected_objects(cls) -> dict[tuple[str, str], str]:
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.executescript(jobs.JOBS_SCHEMA_SQL)
            connection.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
            return cls._schema_objects(connection)

    @classmethod
    def _validate_schema(cls, connection: sqlite3.Connection) -> None:
        expected = cls._expected_objects()
        actual = cls._schema_objects(connection)
        if any(actual.get(key) != value for key, value in expected.items()):
            raise ValueError("database contains an invalid replay jobs schema")

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def lookup_submission(
        self, submitted_by: str, idempotency_key: str, request_sha256: str
    ) -> SubmissionResult | None:
        self._validate_key(idempotency_key)
        with closing(self.connect()) as connection:
            return self._lookup_submission(
                connection, submitted_by.lower(), idempotency_key, request_sha256
            )

    def submit(
        self,
        request_bytes: bytes,
        request: jobs.Request,
        principal: Principal,
        idempotency_key: str,
        now_ns: int,
        *,
        bundle_exists: bool,
    ) -> SubmissionResult:
        self._validate_key(idempotency_key)
        request_hash = jobs.request_sha256(request)
        submitted_by = principal.address.lower()
        with self._write() as connection:
            existing = self._lookup_submission(
                connection, submitted_by, idempotency_key, request_hash
            )
            if existing is not None:
                return existing
            if not bundle_exists:
                raise ReplayJobError(404, "bundle not found")
            self._check_quotas(connection, submitted_by)
            row = self._insert_job(
                connection, request_bytes, submitted_by, request_hash, now_ns
            )
            connection.execute(
                "INSERT INTO job_submissions"
                "(submitted_by,idempotency_key,request_sha256,job_id,created_at_ns) "
                "VALUES (?,?,?,?,?)",
                (submitted_by, idempotency_key, request_hash, row.job_id, now_ns),
            )
            self._append_event(connection, row, "submitted", now_ns)
            return SubmissionResult(row, True)

    def get_job(self, job_id: str) -> tuple[jobs.JobRow, bytes] | None:
        jobs.check_job_id(job_id)
        with closing(self.connect()) as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return None if row is None else self._row_and_request(row)

    def list_jobs(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
        after: tuple[int, str] | None = None,
    ) -> tuple[list[jobs.JobRow], bool]:
        self._limit(limit)
        if status is not None and status not in jobs.STATUSES:
            raise ValueError("unknown job status")
        predicates: list[str] = []
        parameters: list[Any] = []
        if status is not None:
            predicates.append("status=?")
            parameters.append(status)
        if after is not None:
            predicates.append("(created_at_ns,job_id) < (?,?)")
            parameters.extend(after)
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM jobs" + where
                + " ORDER BY created_at_ns DESC,job_id DESC LIMIT ?",
                (*parameters, limit + 1),
            ).fetchall()
        return [self._job_row(row) for row in rows[:limit]], len(rows) > limit

    def list_events(
        self,
        job_id: str,
        *,
        limit: int = 100,
        after_event_id: int | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        jobs.check_job_id(job_id)
        self._limit(limit)
        if after_event_id is not None and (
            isinstance(after_event_id, bool)
            or not isinstance(after_event_id, int)
            or after_event_id < 0
        ):
            raise ValueError("event cursor must be a nonnegative integer")
        with closing(self.connect()) as connection:
            rows = connection.execute(
                "SELECT * FROM job_events WHERE job_id=? AND event_id>? "
                "ORDER BY event_id LIMIT ?",
                (job_id, after_event_id or 0, limit + 1),
            ).fetchall()
        return [self._event_record(row) for row in rows[:limit]], len(rows) > limit

    def cancel_job(
        self, job_id: str, principal: Principal, now_ns: int
    ) -> jobs.JobRow:
        jobs.check_job_id(job_id)
        with self._write() as connection:
            stored = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
            if stored is None:
                raise ReplayJobError(404, "job not found")
            before = self._job_row(stored)
            if principal.role != "admin" and principal.address.lower() != before.submitted_by:
                raise ReplayJobError(403, "forbidden")
            if before.status != jobs.QUEUED:
                raise ReplayJobError(409, "only queued jobs can be cancelled")
            after = jobs.cancel(before, now_ns)
            self._update(connection, before, after)
            self._append_event(connection, after, "cancelled", now_ns)
            return after

    def claim_next(self, orchestration: jobs.Orchestration, now_ns: int) -> ClaimResult | None:
        with self._write() as connection:
            stored = connection.execute(
                "SELECT * FROM jobs WHERE status IN ('running','archiving') "
                "AND (next_attempt_at_ns IS NULL OR next_attempt_at_ns<=?) "
                "ORDER BY created_at_ns,job_id LIMIT 1",
                (now_ns,),
            ).fetchone()
            if stored is None:
                stored = connection.execute(
                    "SELECT * FROM jobs WHERE status='queued' "
                    "ORDER BY created_at_ns,job_id LIMIT 1"
                ).fetchone()
            if stored is None:
                return None
            before, request_bytes = self._row_and_request(stored)
            initialize = before.status == jobs.QUEUED or (
                before.status == jobs.ARCHIVING
                and before.pending_outcome == jobs.CANCELLED
                and before.started_at_ns is None
                and before.stage_attempts == 0
            )
            mode: Literal["initialize", "resume"] = (
                "initialize" if initialize else "resume"
            )
            after = jobs.claim(before, orchestration, now_ns)
            self._update(connection, before, after)
            event_type = self._claim_event(before, after, mode)
            self._append_event(connection, after, event_type, now_ns)
            return ClaimResult(after, request_bytes, mode)

    def save(self, before: jobs.JobRow, after: jobs.JobRow) -> jobs.JobRow:
        if before.job_id != after.job_id:
            raise ReplayJobError(409, "job compare-and-set failed")
        event_type = self._transition_event(before, after)
        with self._write() as connection:
            stored = connection.execute(
                "SELECT * FROM jobs WHERE job_id=?", (before.job_id,)
            ).fetchone()
            if stored is None or self._job_row(stored) != before:
                raise ReplayJobError(409, "job compare-and-set failed")
            self._update(connection, before, after)
            self._append_event(connection, after, event_type, after.updated_at_ns)
        return after

    @staticmethod
    def job_record(row: jobs.JobRow, request_bytes: bytes | None = None) -> dict[str, Any]:
        record: dict[str, Any] = {}
        ns_fields = {
            "created_at_ns", "next_attempt_at_ns", "started_at_ns", "updated_at_ns",
            "finished_at_ns", "blocked_at_ns",
        }
        for name, value in row.as_record().items():
            record[name] = str(value) if name in ns_fields and value is not None else value
        if request_bytes is not None:
            record["request"] = json.loads(request_bytes)
        return record

    @staticmethod
    def _validate_key(value: str) -> None:
        if not isinstance(value, str) or IDEMPOTENCY_KEY_RE.fullmatch(value) is None:
            raise ReplayJobError(400, "invalid Idempotency-Key")

    def _lookup_submission(
        self,
        connection: sqlite3.Connection,
        submitted_by: str,
        idempotency_key: str,
        request_sha256: str,
    ) -> SubmissionResult | None:
        row = connection.execute(
            "SELECT submission.request_sha256,jobs.* FROM job_submissions submission "
            "JOIN jobs USING(job_id) WHERE submission.submitted_by=? "
            "AND submission.idempotency_key=?",
            (submitted_by, idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if str(row["request_sha256"]) != request_sha256:
            raise ReplayJobError(409, "idempotency key was used for a different request")
        return SubmissionResult(self._job_row(row), False)

    def _check_quotas(self, connection: sqlite3.Connection, submitted_by: str) -> None:
        placeholders = ",".join("?" for _ in ACTIVE_STATUSES)
        active_by_user = int(
            connection.execute(
                f"SELECT count(*) FROM jobs WHERE submitted_by=? AND status IN ({placeholders})",
                (submitted_by, *ACTIVE_STATUSES),
            ).fetchone()[0]
        )
        if active_by_user >= self.limits.max_active_jobs_per_submitter:
            raise ReplayJobError(429, "submitter active job quota exhausted")
        active, queued = connection.execute(
            f"SELECT count(*),sum(status='queued') FROM jobs WHERE status IN ({placeholders})",
            ACTIVE_STATUSES,
        ).fetchone()
        if int(active) >= self.limits.max_active_jobs_total:
            raise ReplayJobError(503, "global active job quota exhausted")
        if int(queued or 0) >= self.limits.max_queued_jobs_total:
            raise ReplayJobError(503, "global queued job quota exhausted")

    def _insert_job(
        self,
        connection: sqlite3.Connection,
        request_bytes: bytes,
        submitted_by: str,
        request_sha256: str,
        now_ns: int,
    ) -> jobs.JobRow:
        for _ in range(128):
            candidate = jobs.new_job(
                jobs.job_id(now_ns, self._suffix()),
                submitted_by=submitted_by,
                request_sha256=request_sha256,
                now_ns=now_ns,
            )
            record = candidate.as_record() | {"request_json": request_bytes}
            try:
                connection.execute(
                    f"INSERT INTO jobs({','.join(jobs.JOB_COLUMNS)}) "
                    f"VALUES ({','.join('?' for _ in jobs.JOB_COLUMNS)})",
                    [record[name] for name in jobs.JOB_COLUMNS],
                )
                return candidate
            except sqlite3.IntegrityError:
                collision = connection.execute(
                    "SELECT 1 FROM jobs WHERE job_id=?", (candidate.job_id,)
                ).fetchone()
                if collision is None:
                    raise
        raise ReplayJobError(503, "could not allocate a replay job id")

    @staticmethod
    def _update(
        connection: sqlite3.Connection, before: jobs.JobRow, after: jobs.JobRow
    ) -> None:
        columns = tuple(name for name in jobs.JOB_COLUMNS if name != "request_json")
        values = after.as_record()
        cursor = connection.execute(
            "UPDATE jobs SET " + ",".join(f"{name}=?" for name in columns)
            + " WHERE job_id=? AND status=? AND stage IS ? AND stage_attempts=? "
            "AND updated_at_ns=?",
            [values[name] for name in columns]
            + [before.job_id, before.status, before.stage, before.stage_attempts, before.updated_at_ns],
        )
        if cursor.rowcount != 1:
            raise ReplayJobError(409, "job compare-and-set failed")

    @staticmethod
    def _append_event(
        connection: sqlite3.Connection,
        row: jobs.JobRow,
        event_type: str,
        now_ns: int,
    ) -> None:
        connection.execute(
            "INSERT INTO job_events(job_id,event_type,created_at_ns,status,stage,"
            "stage_attempts,pending_outcome,reason_code) VALUES (?,?,?,?,?,?,?,?)",
            (
                row.job_id, event_type, now_ns, row.status, row.stage,
                row.stage_attempts, row.pending_outcome, row.reason_code,
            ),
        )

    @staticmethod
    def _claim_event(
        before: jobs.JobRow,
        after: jobs.JobRow,
        mode: Literal["initialize", "resume"],
    ) -> str:
        if after.status == jobs.ARCHIVE_BLOCKED:
            return "archive_blocked"
        if before.status == jobs.RUNNING and after.status == jobs.ARCHIVING:
            return "outcome_pending"
        return "claimed_initialize" if mode == "initialize" else "claimed_resume"

    @staticmethod
    def _transition_event(before: jobs.JobRow, after: jobs.JobRow) -> str:
        immutable = ("job_id", "created_at_ns", "submitted_by", "request_sha256", "started_at_ns")
        if any(getattr(before, name) != getattr(after, name) for name in immutable):
            raise ReplayJobError(409, "invalid job transition")
        if before.status == jobs.RUNNING and after.status == jobs.RUNNING:
            if before.stage != after.stage:
                try:
                    expected = jobs.advance(before, after.stage, after.updated_at_ns)
                except jobs.ContractError as error:
                    raise ReplayJobError(409, "invalid job transition") from error
                if after == expected:
                    return "stage_advanced"
            if before.stage_attempts == after.stage_attempts:
                changed = {
                    name
                    for name in before.as_record()
                    if getattr(before, name) != getattr(after, name)
                }
                if changed <= {
                    "reason_code",
                    "reason_detail",
                    "next_attempt_at_ns",
                    "updated_at_ns",
                }:
                    return "retry_scheduled"
        if before.status == jobs.RUNNING and after.status == jobs.ARCHIVING:
            if after.pending_outcome == jobs.SUCCEEDED:
                expected = jobs.succeed(before, after.updated_at_ns)
            elif after.reason_code in jobs.OUTCOME_OF_CODE:
                try:
                    expected = jobs.fail(
                        before,
                        after.reason_code,
                        after.reason_detail,
                        after.updated_at_ns,
                    )
                except jobs.ContractError as error:
                    raise ReplayJobError(409, "invalid job transition") from error
            else:
                expected = None
            if after == expected:
                return "outcome_pending"
        if before.status == jobs.ARCHIVING and after.status == jobs.ARCHIVING:
            if (
                before.pending_outcome != after.pending_outcome
                or before.reason_code != after.reason_code
            ):
                expected = jobs.lose_local_state(
                    before, after.reason_detail, after.updated_at_ns
                )
                if after == expected:
                    return "outcome_replaced"
            if before.stage_attempts == after.stage_attempts:
                changed = {
                    name
                    for name in before.as_record()
                    if getattr(before, name) != getattr(after, name)
                }
                if changed <= {"reason_detail", "next_attempt_at_ns", "updated_at_ns"}:
                    return "retry_scheduled"
        if before.status == jobs.ARCHIVING and after.status == jobs.ARCHIVE_BLOCKED:
            if after.blocked_reason_code is not None and after == jobs.block_archive(
                before, after.blocked_reason_code, after.updated_at_ns
            ):
                return "archive_blocked"
        if before.status == jobs.ARCHIVE_BLOCKED and after.status == jobs.ARCHIVING:
            if after == jobs.resume_blocked(before, after.updated_at_ns):
                return "archive_resumed"
        if before.status == jobs.ARCHIVING and after.status in jobs.TERMINAL:
            if after == jobs.finish(before, after.updated_at_ns):
                return "finished"
        raise ReplayJobError(409, "invalid job transition")

    @staticmethod
    def _job_row(row: sqlite3.Row) -> jobs.JobRow:
        return jobs.JobRow(**{name: row[name] for name in jobs.JOB_COLUMNS if name != "request_json"})

    @classmethod
    def _row_and_request(cls, row: sqlite3.Row) -> tuple[jobs.JobRow, bytes]:
        raw = row["request_json"]
        if not isinstance(raw, bytes):
            raise ValueError("database contains invalid replay request bytes")
        return cls._job_row(row), raw

    @staticmethod
    def _event_record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": str(row["event_id"]),
            "job_id": str(row["job_id"]),
            "event_type": str(row["event_type"]),
            "created_at_ns": str(row["created_at_ns"]),
            "status": str(row["status"]),
            "stage": row["stage"],
            "stage_attempts": int(row["stage_attempts"]),
            "pending_outcome": row["pending_outcome"],
            "reason_code": row["reason_code"],
        }

    @staticmethod
    def _limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
