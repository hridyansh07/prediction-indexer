"""Closed shared contracts for Replay jobs V1 (``docs/REPLAY_JOBS_V1.md`` §3).

Pure: no filesystem, network, clock, or randomness; callers pass time and
random suffixes in. The Universe server and the runner both import this module
and neither redefines these shapes. A change here is a W0 contract amendment,
not a local edit by one workstream.
"""

import hashlib
import re
from dataclasses import dataclass, replace
from datetime import date, timedelta
from types import MappingProxyType
from urllib.parse import urlsplit

from archive.storage.base import ObjectKeyError, normalize_key
from replay.preparation import encoded
from replay.streams.protocol import ProtocolError, decode, freeze, obj, uint
from replay.supervisor import normalizer_descriptor

REQUEST_VERSION = 1
BUNDLE_RECEIPT_VERSION = 1
PRODUCER_IDENTITY_VERSION = 1
RESOLVED_JOB_VERSION = 1
JOB_RESULT_VERSION = 1
JOB_RECEIPT_VERSION = 1
RUNNER_CONFIG_VERSION = 1

MAX_REQUEST_BYTES = 64 * 1024
MAX_BUNDLE_RECEIPT_BYTES = 1024 * 1024
MAX_RESOLVED_JOB_BYTES = 1024 * 1024
MAX_JOB_RESULT_BYTES = 64 * 1024
MAX_JOB_RECEIPT_BYTES = 4 * 1024 * 1024
MAX_RUNNER_CONFIG_BYTES = 1024 * 1024
MAX_PROBE_MARKETS = 4096
MAX_BUNDLE_WINDOWS = 4096
MAX_OCCURRENCES = 128
MAX_JOB_OBJECTS = 4096
MAX_REASON_DETAIL = 1024
MAX_TEXT = 1024

_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_HEX32 = re.compile(r"[0-9a-f]{32}")
_ADDRESS = re.compile(r"0x[0-9a-f]{40}")
_JOB_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{16}")
_FACTORY = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*")
_NS_PER_DAY = 86_400_000_000_000


class ContractError(ValueError):
    """Input that does not satisfy a closed Replay jobs contract.

    ``code`` is set only where the failure maps to a job reason code (§3.7).
    """

    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


def _require(ok, message, code=None):
    if not ok:
        raise ContractError(message, code)


def _closed(value, fields, where):
    try:
        return obj(value, fields)
    except ProtocolError:
        raise ContractError(f"{where} must have exactly the fields: {fields}") from None


def _uint(value, where):
    try:
        return uint(value)
    except ProtocolError:
        raise ContractError(f"{where} must be a canonical unsigned decimal string") from None


def _identifier(value, where):
    _require(
        type(value) is str
        and _IDENTIFIER.fullmatch(value) is not None
        and value not in (".", ".."),
        f"{where} must match [A-Za-z0-9_.-]{{1,128}}",
    )
    return value


def _hex64(value, where):
    _require(
        type(value) is str and _HEX64.fullmatch(value) is not None,
        f"{where} must be 64 lowercase hex characters",
    )
    return value


def _optional(check, value, where):
    return None if value is None else check(value, where)


def _ns(value, where):
    _require(type(value) is int and 0 <= value < 2**64, f"{where} must be u64 ns")
    return value


def _nonnegative_int(value, where):
    _require(type(value) is int and value >= 0, f"{where} must be a nonnegative integer")
    return value


def _positive_int(value, where):
    _require(type(value) is int and value > 0, f"{where} must be a positive integer")
    return value


def _text(value, where, limit=MAX_TEXT):
    _require(
        type(value) is str
        and 0 < len(value) <= limit
        and all(ord(ch) >= 32 and ord(ch) != 127 for ch in value),
        f"{where} must be 1-{limit} printable characters",
    )
    return value


def _decode(raw, limit, where):
    _require(type(raw) is bytes, f"{where} must be bytes")
    _require(len(raw) <= limit, f"{where} exceeds {limit} bytes")
    try:
        return decode(raw, limit)
    except ProtocolError:
        raise ContractError(f"{where} is not strict JSON") from None


def _canonical(raw, document, where):
    _require(encoded(document) == raw, f"{where} is not in canonical serialization")


def _plain(value):
    if isinstance(value, MappingProxyType):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_plain(v) for v in value]
    return value


def _interval(value, where):
    _closed(value, "start_ns end_ns", where)
    start = _uint(value["start_ns"], f"{where}.start_ns")
    end = _uint(value["end_ns"], f"{where}.end_ns")
    _require(start < end, f"{where} must satisfy start_ns < end_ns")
    return start, end


def _interval_document(interval):
    return {"start_ns": str(interval[0]), "end_ns": str(interval[1])}


def _check_interval(interval, where):
    _require(
        type(interval) is tuple
        and len(interval) == 2
        and all(type(n) is int and 0 <= n < 2**64 for n in interval)
        and interval[0] < interval[1],
        f"{where} must be a (start_ns, end_ns) tuple with start < end",
    )
    return interval


# --- Status, stage, and reason codes (§3.2, §3.7) ---------------------------

QUEUED = "queued"
RUNNING = "running"
ARCHIVING = "archiving"
ARCHIVE_BLOCKED = "archive_blocked"
SUCCEEDED = "succeeded"
FAILED = "failed"
EXHAUSTED = "exhausted"
NOT_READY = "not_ready"
STALE_BUNDLE_CACHE = "stale_bundle_cache"
CANCELLED = "cancelled"

TERMINAL = frozenset(
    {SUCCEEDED, FAILED, EXHAUSTED, NOT_READY, STALE_BUNDLE_CACHE, CANCELLED}
)
STATUSES = frozenset({QUEUED, RUNNING, ARCHIVING, ARCHIVE_BLOCKED}) | TERMINAL
#: Claimable again after a crash or retryable failure, subject to backoff.
RESUMABLE = frozenset({RUNNING, ARCHIVING})
#: Every terminal outcome passes through ``archiving`` and has a job receipt.
ARCHIVED_OUTCOMES = TERMINAL

STAGES = ("resolve", "bundle", "prepare", "run", "read", "archive")
WORK_STAGES = STAGES[:-1]

#: Closed reason vocabulary. Behavior depends only on these codes, never on
#: ``reason_detail``. Each outcome code names the terminal status it produces.
OUTCOME_OF_CODE = MappingProxyType(
    {
        "cancelled": CANCELLED,
        "bundle_not_retired": NOT_READY,
        "canonical_not_archived": NOT_READY,
        "stale_bundle_cache": STALE_BUNDLE_CACHE,
        "supervisor_exhausted": EXHAUSTED,
        "bundle_history_invalid": FAILED,
        "interval_out_of_range": FAILED,
        "local_state_lost": FAILED,
        "universe_unavailable": FAILED,
        "integrity_failure": FAILED,
        "resource_exhausted": FAILED,
        "tool_failure": FAILED,
        "supervisor_failed": FAILED,
        "result_invalid": FAILED,
        "stage_attempts_exhausted": FAILED,
        "job_deadline_exceeded": FAILED,
        "internal_failure": FAILED,
    }
)
#: Codes that block archival: the row becomes ``archive_blocked``.
ARCHIVE_BLOCK_CODES = frozenset(
    {"archive_unavailable", "archive_conflict", "stage_attempts_exhausted"}
)
REASON_CODES = frozenset(OUTCOME_OF_CODE) | ARCHIVE_BLOCK_CODES
#: A failure with one of these codes stays in its stage and is retried after
#: backoff until the stage attempt budget is spent.
RETRYABLE_CODES = frozenset(
    {"universe_unavailable", "archive_unavailable", "resource_exhausted"}
)

_URL_USERINFO = re.compile(r"([A-Za-z][A-Za-z0-9+.-]*://)[^/@\s]*@")


def reason_detail(text):
    """Bounded, printable, userinfo-redacted human detail. Never parsed."""
    _require(type(text) is str, "reason detail must be a string")
    redacted = _URL_USERINFO.sub(r"\1***@", text)
    printable = "".join(ch if ch.isprintable() else " " for ch in redacted).strip()
    if len(printable) > MAX_REASON_DETAIL:
        printable = printable[: MAX_REASON_DETAIL - 1] + "…"
    return printable or None


def check_reason_code(code):
    _require(code in REASON_CODES, f"unknown reason code {code!r}")
    return code


def check_stage(stage):
    _require(stage in STAGES, f"unknown job stage {stage!r}")
    return stage


# --- Job row and transitions (§3.3, §3.8) -----------------------------------


@dataclass(frozen=True)
class JobRow:
    """One ``jobs`` row minus ``request_json``. Construction enforces §3.3."""

    job_id: str
    created_at_ns: int
    submitted_by: str
    request_sha256: str
    status: str
    stage: str | None
    stage_attempts: int
    next_attempt_at_ns: int | None
    started_at_ns: int | None
    updated_at_ns: int
    finished_at_ns: int | None
    pending_outcome: str | None
    reason_code: str | None
    reason_detail: str | None
    blocked_reason_code: str | None
    blocked_at_ns: int | None
    archive_receipt_key: str | None

    def __post_init__(self):
        check_job_id(self.job_id)
        _ns(self.created_at_ns, "created_at_ns")
        _require(
            type(self.submitted_by) is str and _ADDRESS.fullmatch(self.submitted_by),
            "submitted_by must be a lowercase 0x address",
        )
        _hex64(self.request_sha256, "request_sha256")
        _require(self.status in STATUSES, f"unknown job status {self.status!r}")
        _require(self.stage is None or self.stage in STAGES, "unknown job stage")
        _nonnegative_int(self.stage_attempts, "stage_attempts")
        for name in ("next_attempt_at_ns", "started_at_ns", "finished_at_ns", "blocked_at_ns"):
            _optional(_ns, getattr(self, name), name)
        _ns(self.updated_at_ns, "updated_at_ns")
        _require(
            self.reason_code is None or self.reason_code in REASON_CODES,
            "reason_code must be a known reason code",
        )
        _require(
            self.reason_detail is None
            or (type(self.reason_detail) is str and self.reason_detail == reason_detail(self.reason_detail)),
            "reason_detail must be sanitized by reason_detail()",
        )
        status = self.status
        blocked = status == ARCHIVE_BLOCKED
        _require(
            (status in (ARCHIVING, ARCHIVE_BLOCKED)) == (self.pending_outcome is not None),
            "pending_outcome is set exactly while archiving or archive_blocked",
        )
        _require(
            self.pending_outcome is None or self.pending_outcome in TERMINAL,
            "pending_outcome must be a terminal status",
        )
        _require(
            blocked
            == (self.blocked_reason_code is not None and self.blocked_at_ns is not None)
            and (blocked or (self.blocked_reason_code is None and self.blocked_at_ns is None)),
            "blocked fields are set exactly while archive_blocked",
        )
        _require(
            self.blocked_reason_code is None or self.blocked_reason_code in ARCHIVE_BLOCK_CODES,
            "blocked_reason_code must be an archive block code",
        )
        terminal = status in TERMINAL
        _require(
            terminal == (self.finished_at_ns is not None)
            and terminal == (self.archive_receipt_key is not None),
            "terminal rows, and only they, have finished_at_ns and archive_receipt_key",
        )
        _require(
            self.archive_receipt_key is None
            or self.archive_receipt_key == job_receipt_key(self.job_id),
            "archive_receipt_key must be this job's receipt key",
        )
        _require(
            self.next_attempt_at_ns is None or status in RESUMABLE,
            "only resumable rows are scheduled",
        )
        if status == QUEUED:
            _require(
                self.stage is None
                and self.stage_attempts == 0
                and self.started_at_ns is None
                and self.reason_code is None,
                "queued rows have no stage, attempts, start, or reason",
            )
        elif status == RUNNING:
            _require(
                self.stage in WORK_STAGES
                and self.stage_attempts >= 1
                and self.started_at_ns is not None,
                "running rows have a work stage, an attempt, and a start",
            )
            _require(
                self.reason_code is None or self.reason_code in RETRYABLE_CODES,
                "a running row may only record its last retryable reason",
            )
        else:
            _require(self.stage == "archive", "archiving and terminal rows are in stage archive")
            outcome = status if terminal else self.pending_outcome
            if outcome == SUCCEEDED:
                _require(self.reason_code is None, "succeeded has no reason code")
            else:
                _require(
                    OUTCOME_OF_CODE.get(self.reason_code) == outcome,
                    f"{outcome} requires a matching reason code",
                )
            if terminal:
                _require(self.stage_attempts == 0, "terminal rows hold no attempt")

    def as_record(self):
        return {name: getattr(self, name) for name in JOB_COLUMNS if name != "request_json"}


def new_job(job_id_value, *, submitted_by, request_sha256, now_ns):
    return JobRow(
        job_id=job_id_value,
        created_at_ns=now_ns,
        submitted_by=submitted_by,
        request_sha256=request_sha256,
        status=QUEUED,
        stage=None,
        stage_attempts=0,
        next_attempt_at_ns=None,
        started_at_ns=None,
        updated_at_ns=now_ns,
        finished_at_ns=None,
        pending_outcome=None,
        reason_code=None,
        reason_detail=None,
        blocked_reason_code=None,
        blocked_at_ns=None,
        archive_receipt_key=None,
    )


def claimable(row, now_ns):
    return row.status in RESUMABLE and (
        row.next_attempt_at_ns is None or row.next_attempt_at_ns <= now_ns
    )


def select_next(rows, now_ns):
    """Reference claim order: oldest claimable resumable row, else oldest queued.

    A resumable row waiting on backoff, and every ``archive_blocked`` row, is
    skipped, so neither prevents queued work from starting.
    """
    order = lambda row: (row.created_at_ns, row.job_id)
    resumable = sorted((r for r in rows if claimable(r, now_ns)), key=order)
    if resumable:
        return resumable[0]
    queued = sorted((r for r in rows if r.status == QUEUED), key=order)
    return queued[0] if queued else None


def claim(row, orchestration, now_ns):
    """Start or resume ``row``. The attempt is counted before any work runs, so a
    process that dies mid-stage still spends budget."""
    backoff = orchestration.retry_backoff_seconds * 1_000_000_000
    if row.status == QUEUED:
        return replace(
            row,
            status=RUNNING,
            stage=STAGES[0],
            stage_attempts=1,
            started_at_ns=now_ns,
            next_attempt_at_ns=now_ns + backoff,
            updated_at_ns=now_ns,
        )
    _require(claimable(row, now_ns), f"job {row.job_id} is not claimable now")
    if row.status == RUNNING:
        deadline = row.started_at_ns + orchestration.max_job_seconds * 1_000_000_000
        if now_ns >= deadline:
            return fail(row, "job_deadline_exceeded", None, now_ns)
        if row.stage_attempts >= orchestration.max_stage_attempts:
            return fail(
                row,
                "stage_attempts_exhausted",
                f"stage {row.stage} started {row.stage_attempts} times",
                now_ns,
            )
    elif row.stage_attempts >= orchestration.max_stage_attempts:
        return block_archive(row, "stage_attempts_exhausted", now_ns)
    return replace(
        row,
        stage_attempts=row.stage_attempts + 1,
        next_attempt_at_ns=now_ns + backoff,
        updated_at_ns=now_ns,
    )


def advance(row, stage, now_ns):
    """Commit the current work stage and start the next; the running attempt counts."""
    _require(row.status == RUNNING, "only running jobs advance")
    _require(
        stage in WORK_STAGES and STAGES.index(stage) == STAGES.index(row.stage) + 1,
        f"stage {stage!r} does not follow {row.stage!r}",
    )
    return replace(row, stage=stage, stage_attempts=1, reason_code=None, reason_detail=None, updated_at_ns=now_ns)


def retry_later(row, code, detail, orchestration, now_ns):
    """A retryable failure: keep the stage, wait for backoff."""
    _require(row.status in RESUMABLE, "only resumable jobs retry")
    _require(code in RETRYABLE_CODES, f"{code!r} is not retryable")
    backoff = orchestration.retry_backoff_seconds * 1_000_000_000
    if row.status == ARCHIVING:
        # The archiving row keeps its pending outcome's code; the retry cause is detail.
        return replace(
            row,
            next_attempt_at_ns=now_ns + backoff,
            reason_detail=reason_detail(f"{code}: {detail or ''}"),
            updated_at_ns=now_ns,
        )
    return replace(
        row,
        reason_code=code,
        reason_detail=reason_detail(detail) if detail else None,
        next_attempt_at_ns=now_ns + backoff,
        updated_at_ns=now_ns,
    )


def _enter_archiving(row, outcome, code, detail, now_ns):
    return replace(
        row,
        status=ARCHIVING,
        stage="archive",
        stage_attempts=0 if row.status == QUEUED else 1,
        pending_outcome=outcome,
        reason_code=code,
        reason_detail=reason_detail(detail) if detail else None,
        next_attempt_at_ns=None,
        updated_at_ns=now_ns,
    )


def succeed(row, now_ns):
    _require(row.status == RUNNING and row.stage == "read", "only a read-complete job succeeds")
    return _enter_archiving(row, SUCCEEDED, None, None, now_ns)


def fail(row, code, detail, now_ns):
    """End the work stages with an outcome code; archival follows."""
    _require(row.status == RUNNING, "only running jobs end their work stages")
    _require(code in OUTCOME_OF_CODE and code != "cancelled", f"{code!r} is not a work outcome")
    return _enter_archiving(row, OUTCOME_OF_CODE[code], code, detail, now_ns)


def cancel(row, now_ns):
    """Only a queued job can be cancelled in V1; the runner archives it."""
    _require(row.status == QUEUED, "only queued jobs can be cancelled")
    return _enter_archiving(row, CANCELLED, "cancelled", None, now_ns)


def lose_local_state(row, detail, now_ns):
    """The job directory is missing or invalid. Never rebuild it under this ID."""
    _require(row.status in RESUMABLE, "only resumable jobs can lose local state")
    if row.status == RUNNING:
        return fail(row, "local_state_lost", detail, now_ns)
    return replace(
        row,
        pending_outcome=FAILED,
        reason_code="local_state_lost",
        reason_detail=reason_detail(detail) if detail else None,
        updated_at_ns=now_ns,
    )


def block_archive(row, code, now_ns):
    _require(row.status == ARCHIVING, "only archiving jobs block")
    _require(code in ARCHIVE_BLOCK_CODES, f"{code!r} is not an archive block code")
    return replace(
        row,
        status=ARCHIVE_BLOCKED,
        blocked_reason_code=code,
        blocked_at_ns=now_ns,
        next_attempt_at_ns=None,
        updated_at_ns=now_ns,
    )


def resume_blocked(row, now_ns):
    """Manual operator action after fixing the archive backend."""
    _require(row.status == ARCHIVE_BLOCKED, "only blocked jobs resume")
    return replace(
        row,
        status=ARCHIVING,
        stage_attempts=0,
        blocked_reason_code=None,
        blocked_at_ns=None,
        updated_at_ns=now_ns,
    )


def finish(row, now_ns):
    """The job receipt is durably archived; the pending outcome becomes final."""
    _require(row.status == ARCHIVING, "only archiving jobs finish")
    return replace(
        row,
        status=row.pending_outcome,
        pending_outcome=None,
        stage_attempts=0,
        next_attempt_at_ns=None,
        finished_at_ns=now_ns,
        archive_receipt_key=job_receipt_key(row.job_id),
        updated_at_ns=now_ns,
    )


# --- Jobs table (§3.3) ------------------------------------------------------

JOB_COLUMNS = (
    "job_id",
    "created_at_ns",
    "submitted_by",
    "request_json",
    "request_sha256",
    "status",
    "stage",
    "stage_attempts",
    "next_attempt_at_ns",
    "started_at_ns",
    "updated_at_ns",
    "finished_at_ns",
    "pending_outcome",
    "reason_code",
    "reason_detail",
    "blocked_reason_code",
    "blocked_at_ns",
    "archive_receipt_key",
)


def _sql_set(values):
    return "(" + ", ".join(f"'{v}'" for v in sorted(values)) + ")"


_TERMINAL_SQL = _sql_set(TERMINAL)
_JOB_ID_GLOB = "[0-9]" * 8 + "T" + "[0-9]" * 6 + "Z-" + "[0-9a-f]" * 16

JOBS_SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY CHECK(job_id GLOB '{_JOB_ID_GLOB}'),
    created_at_ns INTEGER NOT NULL CHECK(created_at_ns >= 0),
    submitted_by TEXT NOT NULL CHECK(
        length(submitted_by) = 42 AND substr(submitted_by, 1, 2) = '0x'
        AND substr(submitted_by, 3) NOT GLOB '*[^0-9a-f]*'
    ),
    request_json BLOB NOT NULL,
    request_sha256 TEXT NOT NULL CHECK(
        length(request_sha256) = 64 AND request_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    status TEXT NOT NULL CHECK(status IN {_sql_set(STATUSES)}),
    stage TEXT CHECK(stage IS NULL OR stage IN {_sql_set(STAGES)}),
    stage_attempts INTEGER NOT NULL CHECK(stage_attempts >= 0),
    next_attempt_at_ns INTEGER,
    started_at_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    finished_at_ns INTEGER,
    pending_outcome TEXT CHECK(pending_outcome IS NULL OR pending_outcome IN {_TERMINAL_SQL}),
    reason_code TEXT CHECK(reason_code IS NULL OR reason_code IN {_sql_set(REASON_CODES)}),
    reason_detail TEXT CHECK(reason_detail IS NULL OR length(reason_detail) <= {MAX_REASON_DETAIL}),
    blocked_reason_code TEXT CHECK(
        blocked_reason_code IS NULL OR blocked_reason_code IN {_sql_set(ARCHIVE_BLOCK_CODES)}
    ),
    blocked_at_ns INTEGER,
    archive_receipt_key TEXT,
    CHECK((status IN ('archiving', 'archive_blocked')) = (pending_outcome IS NOT NULL)),
    CHECK((status = 'archive_blocked') = (blocked_reason_code IS NOT NULL)),
    CHECK((status = 'archive_blocked') = (blocked_at_ns IS NOT NULL)),
    CHECK((status IN {_TERMINAL_SQL}) = (finished_at_ns IS NOT NULL)),
    CHECK((status IN {_TERMINAL_SQL}) = (archive_receipt_key IS NOT NULL)),
    CHECK(archive_receipt_key IS NULL
          OR archive_receipt_key = 'replay/jobs/' || job_id || '/job_receipt.json'),
    CHECK(next_attempt_at_ns IS NULL OR status IN ('running', 'archiving')),
    CHECK(status <> 'queued' OR (stage IS NULL AND stage_attempts = 0
                                 AND started_at_ns IS NULL AND reason_code IS NULL)),
    CHECK(status <> 'running' OR (stage IS NOT NULL AND stage <> 'archive'
                                  AND stage_attempts >= 1 AND started_at_ns IS NOT NULL)),
    CHECK(status IN ('queued', 'running') OR stage = 'archive'),
    CHECK(status <> 'succeeded' AND coalesce(pending_outcome, '') <> 'succeeded'
          OR reason_code IS NULL),
    CHECK(status NOT IN ('failed', 'exhausted', 'not_ready', 'stale_bundle_cache', 'cancelled')
          OR reason_code IS NOT NULL)
) STRICT;
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, created_at_ns, job_id);
"""


def job_id(now_ns, suffix_hex):
    """Deterministic ID from caller-supplied time and 64 bits of randomness."""
    _ns(now_ns, "job time")
    _require(
        type(suffix_hex) is str and re.fullmatch(r"[0-9a-f]{16}", suffix_hex) is not None,
        "job suffix must be 16 lowercase hex characters",
    )
    seconds = now_ns // 1_000_000_000
    day = date(1970, 1, 1) + timedelta(days=seconds // 86_400)
    remainder = seconds % 86_400
    clock = f"{remainder // 3600:02}{remainder % 3600 // 60:02}{remainder % 60:02}"
    return f"{day:%Y%m%d}T{clock}Z-{suffix_hex}"


def check_job_id(value):
    _require(
        type(value) is str and _JOB_ID.fullmatch(value) is not None,
        "job id must be <yyyymmddTHHMMSSZ>-<16 hex>",
    )
    return value


# --- Object keys (§3.4) -----------------------------------------------------


def _key(value):
    try:
        return normalize_key(value)
    except ObjectKeyError as error:
        raise ContractError(str(error)) from None


def date_partition(window_start_ns):
    """UTC date of a window start, matching the finalizer's ``date_partition``."""
    _ns(window_start_ns, "window start")
    return f"{date(1970, 1, 1) + timedelta(days=window_start_ns // _NS_PER_DAY):%Y-%m-%d}"


def canonical_window_keys(window_start_ns):
    """``(evidence, provenance, receipt)`` keys of one archived canonical window."""
    base = f"canonical/date={date_partition(window_start_ns)}/window={window_start_ns}"
    return (
        f"{base}/evidence.ndjson.zst",
        f"{base}/provenance.ndjson.zst",
        f"{base}/receipt.json",
    )


DERIVATIVE_FILES = (
    "events.ndjson.zst",
    "rejects.ndjson.zst",
    "sources.ndjson.zst",
    "manifest.json",
    "receipt.json",
)


def derivative_key(derivative_address, file):
    _hex64(derivative_address, "derivative address")
    _require(file in DERIVATIVE_FILES, f"unknown derivative file {file!r}")
    return f"replay/derivatives/{derivative_address}/{file}"


def bundle_receipt_key(bundle_id, generation_sha256):
    bundle = _identifier(bundle_id, "bundle id")
    generation = _hex64(generation_sha256, "bundle generation sha256")
    return f"replay/bundles/{bundle}/generations/{generation}/bundle_receipt.json"


def job_object_key(job_id_value, relative):
    check_job_id(job_id_value)
    _require(type(relative) is str, "job object path must be a string")
    key = _key(f"replay/jobs/{job_id_value}/{relative}")
    _require(
        key == f"replay/jobs/{job_id_value}/{relative}",
        "job object path must already be normalized",
    )
    _require(relative != "job_receipt.json", "job_receipt.json is reserved")
    return key


def job_receipt_key(job_id_value):
    return f"replay/jobs/{check_job_id(job_id_value)}/job_receipt.json"


def _window_period(window_seconds):
    _require(
        type(window_seconds) is int
        and window_seconds > 0
        and 86_400 % window_seconds == 0,
        "canonical_window_seconds must be a positive divisor of 86400",
    )
    return window_seconds * 1_000_000_000


def window_bounds(start_ns, end_ns, window_seconds):
    """Aligned ``[first_start, last_end)`` and every window start covering a range."""
    _require(start_ns < end_ns, "interval must satisfy start_ns < end_ns")
    period = _window_period(window_seconds)
    first = start_ns // period * period
    last = -(-end_ns // period) * period
    return first, last, tuple(range(first, last, period))


# --- Request (§3.1) ---------------------------------------------------------


@dataclass(frozen=True)
class Request:
    bundle_id: str
    probe_markets: tuple[str, ...] | None
    interval: tuple[int, int] | None
    strategy: str
    strategy_config: MappingProxyType
    limits: str
    document: MappingProxyType

    @property
    def sha256(self):
        return request_sha256(self)


def parse_request(raw, config):
    """Strictly parse submitted request bytes against a parsed runner config."""
    value = _decode(raw, MAX_REQUEST_BYTES, "request")
    _closed(
        value,
        "replay_request_version bundle_id probe_markets interval strategy limits",
        "request",
    )
    _require(
        type(value["replay_request_version"]) is int
        and value["replay_request_version"] == REQUEST_VERSION,
        f"replay_request_version must be {REQUEST_VERSION}",
    )
    bundle_id = _identifier(value["bundle_id"], "bundle_id")

    probe = value["probe_markets"]
    if probe is not None:
        _require(
            type(probe) is list and 0 < len(probe) <= MAX_PROBE_MARKETS,
            f"probe_markets must be null or a list of 1-{MAX_PROBE_MARKETS} ids",
        )
        for market in probe:
            _require(
                type(market) is str
                and re.fullmatch(r"[a-z]+:[^\s]+", market) is not None,
                "probe_markets entries must be venue:native-id",
            )
        _require(probe == sorted(set(probe)), "probe_markets must be sorted and unique")
        probe = tuple(probe)

    interval = value["interval"]
    if interval is not None:
        interval = _interval(interval, "interval")

    strategy = _closed(value["strategy"], "name config", "strategy")
    name = strategy["name"]
    _require(
        type(name) is str and name in config.strategies,
        f"strategy.name must be one of: {', '.join(sorted(config.strategies))}",
    )
    strategy_config = _check_strategy_config(
        config.strategies[name].config_schema, strategy["config"]
    )

    limits = value["limits"]
    _require(
        type(limits) is str and limits in config.limits,
        f"limits must be one of: {', '.join(sorted(config.limits))}",
    )
    return Request(
        bundle_id=bundle_id,
        probe_markets=probe,
        interval=interval,
        strategy=name,
        strategy_config=freeze(strategy_config),
        limits=limits,
        document=freeze(value),
    )


def request_sha256(request):
    return hashlib.sha256(encoded(_plain(request.document))).hexdigest()


# --- Strategy config schemas ------------------------------------------------


@dataclass(frozen=True)
class StrategyConfigSchema:
    request_keys: frozenset
    runner_keys: frozenset


#: Registry schemas a runner config may name. ``request_keys`` may be supplied
#: by a request; ``runner_keys`` are filled by the runner and rejected from one.
STRATEGY_CONFIG_SCHEMAS = MappingProxyType(
    {
        "bundle_coverage_v1": StrategyConfigSchema(
            request_keys=frozenset(),
            runner_keys=frozenset({"version", "snapshot_directory", "snapshot_sha256"}),
        ),
    }
)


def _check_strategy_config(schema_name, value):
    schema = STRATEGY_CONFIG_SCHEMAS[schema_name]
    _require(type(value) is dict, "strategy.config must be an object")
    owned = sorted(set(value) & schema.runner_keys)
    _require(not owned, f"strategy.config may not set runner-owned keys: {owned}")
    unknown = sorted(set(value) - schema.request_keys)
    _require(not unknown, f"strategy.config has unknown keys: {unknown}")
    return value


# --- Producer descriptor (§3.5) ---------------------------------------------


@dataclass(frozen=True)
class Producer:
    """Everything that decides derivative bytes apart from the canonical input."""

    normalizer: MappingProxyType
    normalized_schema_version: int
    materializer_version: int
    materialization_policy_sha256: str

    def __post_init__(self):
        try:
            normalizer_descriptor(_plain(self.normalizer))
        except ProtocolError:
            raise ContractError("producer normalizer is not a valid identity") from None
        _positive_int(self.normalized_schema_version, "normalized_schema_version")
        _positive_int(self.materializer_version, "materializer_version")
        _hex64(self.materialization_policy_sha256, "materialization_policy_sha256")

    def document(self):
        return {
            "producer_identity_version": PRODUCER_IDENTITY_VERSION,
            "normalizer": _plain(self.normalizer),
            "normalized_schema_version": self.normalized_schema_version,
            "materializer_version": self.materializer_version,
            "materialization_policy_sha256": self.materialization_policy_sha256,
        }


def producer_from_document(value, where="producer"):
    _closed(
        value,
        "producer_identity_version normalizer normalized_schema_version "
        "materializer_version materialization_policy_sha256",
        where,
    )
    _require(
        type(value["producer_identity_version"]) is int
        and value["producer_identity_version"] == PRODUCER_IDENTITY_VERSION,
        f"producer_identity_version must be {PRODUCER_IDENTITY_VERSION}",
    )
    return Producer(
        normalizer=freeze(value["normalizer"]),
        normalized_schema_version=value["normalized_schema_version"],
        materializer_version=value["materializer_version"],
        materialization_policy_sha256=value["materialization_policy_sha256"],
    )


def parse_producer(raw):
    """Parse ``materialize_range --describe`` output: one canonical producer object."""
    value = _decode(raw, MAX_BUNDLE_RECEIPT_BYTES, "producer")
    producer = producer_from_document(value)
    _canonical(raw, producer.document(), "producer")
    return producer


# --- Bundle receipt (§3.5) --------------------------------------------------


@dataclass(frozen=True)
class BundleWindow:
    window_start_ns: int
    window_end_ns: int
    canonical_receipt_sha256: str
    derivative_address: str
    receipt_sha256: str

    def __post_init__(self):
        _ns(self.window_start_ns, "window_start_ns")
        _ns(self.window_end_ns, "window_end_ns")
        _require(self.window_start_ns < self.window_end_ns, "window must be nonempty")
        _hex64(self.canonical_receipt_sha256, "canonical_receipt_sha256")
        _hex64(self.derivative_address, "derivative_address")
        _hex64(self.receipt_sha256, "receipt_sha256")


@dataclass(frozen=True)
class BundleReceipt:
    """Deterministic in its content, so independent builds publish identical bytes."""

    bundle_id: str
    start_ns: int
    end_ns: int
    canonical_window_seconds: int
    producer: Producer
    windows: tuple[BundleWindow, ...]

    def __post_init__(self):
        _identifier(self.bundle_id, "bundle receipt bundle_id")
        _require(isinstance(self.producer, Producer), "bundle receipt producer must be a Producer")
        _require(
            type(self.windows) is tuple
            and 0 < len(self.windows) <= MAX_BUNDLE_WINDOWS
            and all(isinstance(w, BundleWindow) for w in self.windows),
            f"bundle receipt windows must be a tuple of 1-{MAX_BUNDLE_WINDOWS} windows",
        )
        first, last, starts = window_bounds(
            self.start_ns, self.end_ns, self.canonical_window_seconds
        )
        _require(
            (first, last) == (self.start_ns, self.end_ns),
            "bundle receipt interval must be aligned to canonical windows",
        )
        period = _window_period(self.canonical_window_seconds)
        _require(
            tuple(w.window_start_ns for w in self.windows) == starts
            and all(w.window_end_ns == w.window_start_ns + period for w in self.windows),
            "bundle receipt windows must be ordered, adjacent, and exactly cover its interval",
        )
        addresses = [w.derivative_address for w in self.windows]
        _require(len(set(addresses)) == len(addresses), "bundle receipt repeats a derivative address")


def parse_bundle_receipt(raw):
    value = _decode(raw, MAX_BUNDLE_RECEIPT_BYTES, "bundle receipt")
    _closed(
        value,
        "replay_bundle_receipt_version bundle_id interval canonical_window_seconds "
        "producer windows",
        "bundle receipt",
    )
    _require(
        type(value["replay_bundle_receipt_version"]) is int
        and value["replay_bundle_receipt_version"] == BUNDLE_RECEIPT_VERSION,
        f"replay_bundle_receipt_version must be {BUNDLE_RECEIPT_VERSION}",
    )
    start, end = _interval(value["interval"], "bundle receipt interval")
    windows = value["windows"]
    _require(type(windows) is list, "bundle receipt windows must be a list")
    receipt = BundleReceipt(
        bundle_id=value["bundle_id"],
        start_ns=start,
        end_ns=end,
        canonical_window_seconds=value["canonical_window_seconds"],
        producer=producer_from_document(value["producer"], "bundle receipt producer"),
        windows=tuple(_bundle_window(window) for window in windows),
    )
    _canonical(raw, _bundle_receipt_document(receipt), "bundle receipt")
    return receipt


def _bundle_window(value):
    _closed(
        value,
        "window_start_ns window_end_ns canonical_receipt_sha256 "
        "derivative_address receipt_sha256",
        "bundle receipt window",
    )
    return BundleWindow(
        window_start_ns=_uint(value["window_start_ns"], "window_start_ns"),
        window_end_ns=_uint(value["window_end_ns"], "window_end_ns"),
        canonical_receipt_sha256=value["canonical_receipt_sha256"],
        derivative_address=value["derivative_address"],
        receipt_sha256=value["receipt_sha256"],
    )


def _bundle_receipt_document(receipt):
    return {
        "replay_bundle_receipt_version": BUNDLE_RECEIPT_VERSION,
        "bundle_id": receipt.bundle_id,
        "interval": _interval_document((receipt.start_ns, receipt.end_ns)),
        "canonical_window_seconds": receipt.canonical_window_seconds,
        "producer": receipt.producer.document(),
        "windows": [
            {
                "window_start_ns": str(w.window_start_ns),
                "window_end_ns": str(w.window_end_ns),
                "canonical_receipt_sha256": w.canonical_receipt_sha256,
                "derivative_address": w.derivative_address,
                "receipt_sha256": w.receipt_sha256,
            }
            for w in receipt.windows
        ],
    }


def bundle_receipt_bytes(receipt):
    """Canonical serialization; the exact bytes uploaded, hashed, and copied to
    the job's ``bundle.json``."""
    _require(isinstance(receipt, BundleReceipt), "expected a BundleReceipt")
    # Re-run construction checks in case a caller bypassed them.
    rebuilt = replace(receipt, windows=tuple(replace(w) for w in receipt.windows))
    return encoded(_bundle_receipt_document(rebuilt))


def bundle_generation_sha256(receipt):
    """Complete immutable generation identity for one bundle receipt."""
    body = bundle_receipt_bytes(receipt)
    digest = hashlib.sha256()
    digest.update(b"prediction-indexer/replay-bundle-generation/v1\0")
    digest.update(body)
    return digest.hexdigest()


# --- Bundle history resolution (§3.9) ---------------------------------------


@dataclass(frozen=True)
class Occurrence:
    run_id: str
    start_ns: int
    end_ns: int
    manifest_key: str
    manifest_sha256: str
    report_key: str
    report_sha256: str

    def __post_init__(self):
        _text(self.run_id, "occurrence run_id", 128)
        _check_interval((self.start_ns, self.end_ns), "occurrence interval")
        for name in ("manifest_key", "report_key"):
            _require(_key(getattr(self, name)) == getattr(self, name), f"{name} must be normalized")
        _hex64(self.manifest_sha256, "manifest_sha256")
        _hex64(self.report_sha256, "report_sha256")

    def document(self):
        return {
            "run_id": self.run_id,
            "start_ns": str(self.start_ns),
            "end_ns": str(self.end_ns),
            "source": {
                "manifest_key": self.manifest_key,
                "manifest_sha256": self.manifest_sha256,
                "report_key": self.report_key,
                "report_sha256": self.report_sha256,
            },
        }


@dataclass(frozen=True)
class HistoryEntry:
    """One selection occurrence as Universe reports it for a bundle."""

    run_id: str
    generated_at_ns: int
    manifest_key: str
    manifest_sha256: str
    report_key: str
    report_sha256: str


def resolve_occurrences(history, retired_at_ns, job_interval):
    """Partition the bundle's life into caller-declared occurrence intervals.

    Orders by ``(generated_at_ns, run_id)``. Equal timestamps would create a
    zero-length occurrence and fail closed as ``bundle_history_invalid``.
    Returns ``(bundle_interval, job_interval, occurrences clipped to it)``.
    """
    ordered = sorted(history, key=lambda e: (e.generated_at_ns, e.run_id))
    _require(bool(ordered), "bundle has no selection occurrences", "bundle_history_invalid")
    for previous, current in zip(ordered, ordered[1:]):
        _require(
            previous.generated_at_ns < current.generated_at_ns,
            f"runs {previous.run_id} and {current.run_id} share generated_at_ns",
            "bundle_history_invalid",
        )
    _require(
        retired_at_ns > ordered[-1].generated_at_ns,
        "bundle retirement does not follow its last occurrence",
        "bundle_history_invalid",
    )
    bundle = (ordered[0].generated_at_ns, retired_at_ns)
    job = bundle if job_interval is None else job_interval
    _require(
        bundle[0] <= job[0] < job[1] <= bundle[1],
        "requested interval is outside the bundle interval",
        "interval_out_of_range",
    )
    ends = [e.generated_at_ns for e in ordered[1:]] + [retired_at_ns]
    occurrences = []
    for entry, end in zip(ordered, ends):
        start, stop = max(entry.generated_at_ns, job[0]), min(end, job[1])
        if start < stop:
            occurrences.append(
                Occurrence(
                    run_id=entry.run_id,
                    start_ns=start,
                    end_ns=stop,
                    manifest_key=entry.manifest_key,
                    manifest_sha256=entry.manifest_sha256,
                    report_key=entry.report_key,
                    report_sha256=entry.report_sha256,
                )
            )
    return bundle, job, tuple(occurrences)


# --- Resolved job (§3.6, ``resolved.json``) ---------------------------------


@dataclass(frozen=True)
class ResolvedJob:
    job_id: str
    request_sha256: str
    bundle_id: str
    canonical_window_seconds: int
    bundle_interval: tuple[int, int]
    job_interval: tuple[int, int]
    occurrences: tuple[Occurrence, ...]

    def __post_init__(self):
        check_job_id(self.job_id)
        _hex64(self.request_sha256, "request_sha256")
        _identifier(self.bundle_id, "bundle_id")
        _window_period(self.canonical_window_seconds)
        bundle = _check_interval(self.bundle_interval, "bundle_interval")
        job = _check_interval(self.job_interval, "job_interval")
        _require(bundle[0] <= job[0] and job[1] <= bundle[1], "job_interval must lie in bundle_interval")
        _require(
            type(self.occurrences) is tuple
            and 0 < len(self.occurrences) <= MAX_OCCURRENCES
            and all(isinstance(o, Occurrence) for o in self.occurrences),
            f"occurrences must be a tuple of 1-{MAX_OCCURRENCES}",
        )
        _require(
            self.occurrences[0].start_ns == job[0]
            and self.occurrences[-1].end_ns == job[1]
            and all(a.end_ns == b.start_ns for a, b in zip(self.occurrences, self.occurrences[1:])),
            "occurrences must partition job_interval in order",
        )
        runs = [o.run_id for o in self.occurrences]
        _require(len(set(runs)) == len(runs), "occurrences repeat a run")

    @property
    def window_interval(self):
        first, last, _ = window_bounds(*self.bundle_interval, self.canonical_window_seconds)
        return first, last

    def document(self):
        return {
            "replay_resolved_job_version": RESOLVED_JOB_VERSION,
            "job_id": self.job_id,
            "request_sha256": self.request_sha256,
            "bundle_id": self.bundle_id,
            "canonical_window_seconds": self.canonical_window_seconds,
            "bundle_interval": _interval_document(self.bundle_interval),
            "window_interval": _interval_document(self.window_interval),
            "job_interval": _interval_document(self.job_interval),
            "occurrences": [o.document() for o in self.occurrences],
        }


def resolved_job_bytes(resolved):
    _require(isinstance(resolved, ResolvedJob), "expected a ResolvedJob")
    return encoded(replace(resolved).document())


def parse_resolved_job(raw):
    value = _decode(raw, MAX_RESOLVED_JOB_BYTES, "resolved job")
    _closed(
        value,
        "replay_resolved_job_version job_id request_sha256 bundle_id "
        "canonical_window_seconds bundle_interval window_interval job_interval occurrences",
        "resolved job",
    )
    _require(
        type(value["replay_resolved_job_version"]) is int
        and value["replay_resolved_job_version"] == RESOLVED_JOB_VERSION,
        f"replay_resolved_job_version must be {RESOLVED_JOB_VERSION}",
    )
    _require(type(value["occurrences"]) is list, "occurrences must be a list")
    occurrences = []
    for item in value["occurrences"]:
        _closed(item, "run_id start_ns end_ns source", "occurrence")
        source = _closed(
            item["source"], "manifest_key manifest_sha256 report_key report_sha256", "occurrence source"
        )
        occurrences.append(
            Occurrence(
                run_id=item["run_id"],
                start_ns=_uint(item["start_ns"], "occurrence start_ns"),
                end_ns=_uint(item["end_ns"], "occurrence end_ns"),
                **source,
            )
        )
    resolved = ResolvedJob(
        job_id=value["job_id"],
        request_sha256=value["request_sha256"],
        bundle_id=value["bundle_id"],
        canonical_window_seconds=value["canonical_window_seconds"],
        bundle_interval=_interval(value["bundle_interval"], "bundle_interval"),
        job_interval=_interval(value["job_interval"], "job_interval"),
        occurrences=tuple(occurrences),
    )
    _canonical(raw, resolved.document(), "resolved job")
    return resolved


# --- Job result (§3.6, ``result.json``) -------------------------------------


@dataclass(frozen=True)
class JobResult:
    job_id: str
    strategy: str
    strategy_semantic_sha256: str
    snapshot_sha256: str
    bundle_receipt_sha256: str
    supervisor_identity: str
    attempt_id: str
    attempts_started: int

    def __post_init__(self):
        check_job_id(self.job_id)
        _identifier(self.strategy, "strategy")
        for name in (
            "strategy_semantic_sha256",
            "snapshot_sha256",
            "bundle_receipt_sha256",
            "supervisor_identity",
        ):
            _hex64(getattr(self, name), name)
        _require(
            type(self.attempt_id) is str and _HEX32.fullmatch(self.attempt_id) is not None,
            "attempt_id must be 32 lowercase hex characters",
        )
        _positive_int(self.attempts_started, "attempts_started")

    def document(self):
        return {
            "replay_job_result_version": JOB_RESULT_VERSION,
            **{name: getattr(self, name) for name in _RESULT_FIELDS},
        }


_RESULT_FIELDS = (
    "job_id",
    "strategy",
    "strategy_semantic_sha256",
    "snapshot_sha256",
    "bundle_receipt_sha256",
    "supervisor_identity",
    "attempt_id",
    "attempts_started",
)


def job_result_bytes(result):
    _require(isinstance(result, JobResult), "expected a JobResult")
    return encoded(replace(result).document())


def parse_job_result(raw):
    value = _decode(raw, MAX_JOB_RESULT_BYTES, "job result")
    _closed(value, "replay_job_result_version " + " ".join(_RESULT_FIELDS), "job result")
    _require(
        type(value["replay_job_result_version"]) is int
        and value["replay_job_result_version"] == JOB_RESULT_VERSION,
        f"replay_job_result_version must be {JOB_RESULT_VERSION}",
    )
    result = JobResult(**{name: value[name] for name in _RESULT_FIELDS})
    _canonical(raw, result.document(), "job result")
    return result


# --- Job receipt (§3.6, archived last) --------------------------------------


@dataclass(frozen=True)
class JobObject:
    key: str
    sha256: str
    byte_length: int

    def __post_init__(self):
        _require(type(self.key) is str and _key(self.key) == self.key, "object key must be normalized")
        _hex64(self.sha256, "object sha256")
        _nonnegative_int(self.byte_length, "object byte_length")


@dataclass(frozen=True)
class JobReceipt:
    job_id: str
    request_sha256: str
    submitted_by: str
    image_revision: str
    final_outcome: str
    reason_code: str | None
    reason_detail: str | None
    resolved_sha256: str | None
    bundle_receipt_sha256: str | None
    snapshot_sha256: str | None
    supervisor_identity: str | None
    strategy_semantic_sha256: str | None
    objects: tuple[JobObject, ...]
    created_at_ns: int
    finished_at_ns: int

    def __post_init__(self):
        check_job_id(self.job_id)
        _hex64(self.request_sha256, "request_sha256")
        _require(
            type(self.submitted_by) is str and _ADDRESS.fullmatch(self.submitted_by),
            "submitted_by must be a lowercase 0x address",
        )
        _text(self.image_revision, "image_revision", 128)
        _require(self.final_outcome in TERMINAL, "final_outcome must be terminal")
        if self.final_outcome == SUCCEEDED:
            _require(self.reason_code is None, "succeeded has no reason code")
        else:
            _require(
                OUTCOME_OF_CODE.get(self.reason_code) == self.final_outcome,
                f"{self.final_outcome} requires a matching reason code",
            )
        _require(
            self.reason_detail is None or self.reason_detail == reason_detail(self.reason_detail),
            "reason_detail must be sanitized by reason_detail()",
        )
        chain = (
            "resolved_sha256",
            "bundle_receipt_sha256",
            "snapshot_sha256",
            "supervisor_identity",
            "strategy_semantic_sha256",
        )
        present = [getattr(self, name) is not None for name in chain]
        for name in chain:
            _optional(_hex64, getattr(self, name), name)
        _require(
            present == sorted(present, reverse=True),
            "identities are recorded in stage order; a later one needs every earlier one",
        )
        if self.final_outcome == SUCCEEDED:
            _require(all(present), "succeeded requires every stage identity")
        _require(
            type(self.objects) is tuple
            and len(self.objects) <= MAX_JOB_OBJECTS
            and all(isinstance(o, JobObject) for o in self.objects),
            f"objects must be a tuple of at most {MAX_JOB_OBJECTS}",
        )
        keys = [o.key for o in self.objects]
        _require(keys == sorted(set(keys)), "objects must be sorted by key and unique")
        prefix = f"replay/jobs/{self.job_id}/"
        _require(
            all(k.startswith(prefix) and k != job_receipt_key(self.job_id) for k in keys),
            "objects must be this job's non-receipt objects",
        )
        _ns(self.created_at_ns, "created_at_ns")
        _ns(self.finished_at_ns, "finished_at_ns")
        _require(self.created_at_ns <= self.finished_at_ns, "finished before created")

    def document(self):
        return {
            "replay_job_receipt_version": JOB_RECEIPT_VERSION,
            "job_id": self.job_id,
            "request_sha256": self.request_sha256,
            "submitted_by": self.submitted_by,
            "image_revision": self.image_revision,
            "final_outcome": self.final_outcome,
            "reason_code": self.reason_code,
            "reason_detail": self.reason_detail,
            "resolved_sha256": self.resolved_sha256,
            "bundle_receipt_sha256": self.bundle_receipt_sha256,
            "snapshot_sha256": self.snapshot_sha256,
            "supervisor_identity": self.supervisor_identity,
            "strategy_semantic_sha256": self.strategy_semantic_sha256,
            "objects": [
                {"key": o.key, "sha256": o.sha256, "byte_length": o.byte_length}
                for o in self.objects
            ],
            "created_at_ns": str(self.created_at_ns),
            "finished_at_ns": str(self.finished_at_ns),
        }


def job_receipt_bytes(receipt):
    _require(isinstance(receipt, JobReceipt), "expected a JobReceipt")
    rebuilt = replace(receipt, objects=tuple(replace(o) for o in receipt.objects))
    data = encoded(rebuilt.document())
    _require(len(data) <= MAX_JOB_RECEIPT_BYTES, f"job receipt exceeds {MAX_JOB_RECEIPT_BYTES} bytes")
    return data


def parse_job_receipt(raw):
    value = _decode(raw, MAX_JOB_RECEIPT_BYTES, "job receipt")
    _closed(
        value,
        "replay_job_receipt_version job_id request_sha256 submitted_by image_revision "
        "final_outcome reason_code reason_detail resolved_sha256 bundle_receipt_sha256 "
        "snapshot_sha256 supervisor_identity strategy_semantic_sha256 objects "
        "created_at_ns finished_at_ns",
        "job receipt",
    )
    _require(
        type(value["replay_job_receipt_version"]) is int
        and value["replay_job_receipt_version"] == JOB_RECEIPT_VERSION,
        f"replay_job_receipt_version must be {JOB_RECEIPT_VERSION}",
    )
    _require(type(value["objects"]) is list, "objects must be a list")
    objects = tuple(
        JobObject(**_closed(item, "key sha256 byte_length", "job object")) for item in value["objects"]
    )
    receipt = JobReceipt(
        **{
            name: value[name]
            for name in (
                "job_id",
                "request_sha256",
                "submitted_by",
                "image_revision",
                "final_outcome",
                "reason_code",
                "reason_detail",
                "resolved_sha256",
                "bundle_receipt_sha256",
                "snapshot_sha256",
                "supervisor_identity",
                "strategy_semantic_sha256",
            )
        },
        objects=objects,
        created_at_ns=_uint(value["created_at_ns"], "created_at_ns"),
        finished_at_ns=_uint(value["finished_at_ns"], "finished_at_ns"),
    )
    _canonical(raw, receipt.document(), "job receipt")
    return receipt


# --- Runner configuration (§3.10) -------------------------------------------

VENUES = ("kalshi", "limitless", "polymarket")

_LIMIT_FIELDS = (
    "max_entry_bytes max_queue_bytes command_timeout_ms attempts no_progress "
    "progress_margin stall_seconds attempt_seconds run_seconds poll_seconds stop_seconds"
)


@dataclass(frozen=True)
class StrategyEntry:
    factory: str
    reader: str
    config_schema: str


@dataclass(frozen=True)
class Orchestration:
    max_stage_attempts: int
    max_job_seconds: int
    retry_backoff_seconds: int


@dataclass(frozen=True)
class RunnerConfig:
    universe_base_url: str
    scope: str
    canonical_window_seconds: int
    authorities: MappingProxyType
    strategies: MappingProxyType
    limits: MappingProxyType
    orchestration: Orchestration


def parse_runner_config(raw):
    value = _decode(raw, MAX_RUNNER_CONFIG_BYTES, "runner config")
    _closed(
        value,
        "replay_runner_config_version universe_base_url scope "
        "canonical_window_seconds authorities strategies limits orchestration",
        "runner config",
    )
    _require(
        type(value["replay_runner_config_version"]) is int
        and value["replay_runner_config_version"] == RUNNER_CONFIG_VERSION,
        f"replay_runner_config_version must be {RUNNER_CONFIG_VERSION}",
    )
    url = value["universe_base_url"]
    parts = urlsplit(url) if type(url) is str else None
    _require(
        parts is not None
        and parts.scheme in ("http", "https")
        and bool(parts.netloc)
        and not parts.query
        and not parts.fragment
        and not parts.username
        and not parts.password,
        "universe_base_url must be an http(s) URL without credentials or query",
    )
    _window_period(value["canonical_window_seconds"])

    authorities = _closed(value["authorities"], " ".join(VENUES), "authorities")
    for venue, lane in authorities.items():
        _identifier(lane, f"authorities.{venue}")

    strategies = value["strategies"]
    _require(type(strategies) is dict and bool(strategies), "strategies must be a nonempty object")
    entries = {}
    for name, entry in strategies.items():
        _identifier(name, "strategy name")
        _closed(entry, "factory reader config_schema", f"strategies.{name}")
        for field in ("factory", "reader"):
            _require(
                type(entry[field]) is str and _FACTORY.fullmatch(entry[field]) is not None,
                f"strategies.{name}.{field} must be module:function",
            )
        _require(
            entry["config_schema"] in STRATEGY_CONFIG_SCHEMAS,
            f"strategies.{name}.config_schema is unknown",
        )
        entries[name] = StrategyEntry(**entry)

    orchestration = _closed(
        value["orchestration"],
        "max_stage_attempts max_job_seconds retry_backoff_seconds",
        "orchestration",
    )
    for field, number in orchestration.items():
        _positive_int(number, f"orchestration.{field}")
    orchestration = Orchestration(**orchestration)

    presets = value["limits"]
    _require(type(presets) is dict and bool(presets), "limits must be a nonempty object")
    for name, preset in presets.items():
        _identifier(name, "limits preset name")
        _check_limits(_closed(preset, _LIMIT_FIELDS, f"limits.{name}"), name)
        _require(
            preset["run_seconds"] < orchestration.max_job_seconds,
            f"limits.{name}.run_seconds must be below orchestration.max_job_seconds",
        )

    return RunnerConfig(
        universe_base_url=url,
        scope=_identifier(value["scope"], "scope"),
        canonical_window_seconds=value["canonical_window_seconds"],
        authorities=MappingProxyType(dict(authorities)),
        strategies=MappingProxyType(entries),
        limits=freeze(presets),
        orchestration=orchestration,
    )


def _check_limits(preset, name):
    """Early mirror of ``replay.supervisor.validate``; the supervisor stays authoritative."""
    where = f"limits.{name}"
    for field in ("max_entry_bytes", "max_queue_bytes", "command_timeout_ms"):
        _positive_int(preset[field], f"{where}.{field}")
    _require(
        2 <= preset["command_timeout_ms"] <= 60_000,
        f"{where}.command_timeout_ms must be 2-60000",
    )
    _require(
        preset["max_entry_bytes"] <= preset["max_queue_bytes"] <= 1_000_000_000,
        f"{where} requires max_entry_bytes <= max_queue_bytes <= 1000000000",
    )
    for field in ("attempts", "no_progress", "progress_margin"):
        _positive_int(preset[field], f"{where}.{field}")
    for field in ("stall_seconds", "attempt_seconds", "run_seconds", "poll_seconds", "stop_seconds"):
        number = preset[field]
        _require(
            type(number) in (int, float) and number > 0 and number != float("inf"),
            f"{where}.{field} must be a positive finite number",
        )
    _require(
        preset["poll_seconds"]
        < preset["stall_seconds"]
        <= preset["attempt_seconds"]
        <= preset["run_seconds"],
        f"{where} requires poll < stall <= attempt <= run seconds",
    )
