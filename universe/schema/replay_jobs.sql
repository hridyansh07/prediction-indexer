CREATE TABLE IF NOT EXISTS replay_job_components (
    component TEXT PRIMARY KEY CHECK(component = 'replay_jobs'),
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    schema_sha256 TEXT NOT NULL CHECK(
        typeof(schema_sha256) = 'text' AND length(schema_sha256) = 64
        AND schema_sha256 = lower(schema_sha256)
        AND schema_sha256 NOT GLOB '*[^0-9a-f]*'
    )
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS job_submissions (
    submitted_by TEXT NOT NULL CHECK(
        typeof(submitted_by) = 'text' AND length(submitted_by) = 42
        AND submitted_by = lower(submitted_by) AND substr(submitted_by, 1, 2) = '0x'
        AND substr(submitted_by, 3) NOT GLOB '*[^0-9a-f]*'
    ),
    idempotency_key TEXT NOT NULL CHECK(
        typeof(idempotency_key) = 'text' AND length(idempotency_key) BETWEEN 1 AND 128
        AND idempotency_key NOT GLOB '*[^A-Za-z0-9._~-]*'
    ),
    request_sha256 TEXT NOT NULL CHECK(
        typeof(request_sha256) = 'text' AND length(request_sha256) = 64
        AND request_sha256 = lower(request_sha256)
        AND request_sha256 NOT GLOB '*[^0-9a-f]*'
    ),
    job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
    created_at_ns INTEGER NOT NULL CHECK(
        typeof(created_at_ns) = 'integer' AND created_at_ns >= 0
    ),
    PRIMARY KEY (submitted_by, idempotency_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS job_events (
    event_id INTEGER PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    event_type TEXT NOT NULL CHECK(event_type IN (
        'submitted', 'cancelled', 'claimed_initialize', 'claimed_resume',
        'stage_advanced', 'retry_scheduled', 'outcome_pending',
        'outcome_replaced', 'archive_blocked', 'archive_resumed', 'finished'
    )),
    created_at_ns INTEGER NOT NULL CHECK(
        typeof(created_at_ns) = 'integer' AND created_at_ns >= 0
    ),
    status TEXT NOT NULL CHECK(status IN (
        'queued', 'running', 'archiving', 'archive_blocked', 'succeeded',
        'failed', 'exhausted', 'not_ready', 'stale_bundle_cache', 'cancelled'
    )),
    stage TEXT CHECK(stage IS NULL OR stage IN (
        'resolve', 'bundle', 'prepare', 'run', 'read', 'archive'
    )),
    stage_attempts INTEGER NOT NULL CHECK(
        typeof(stage_attempts) = 'integer' AND stage_attempts >= 0
    ),
    pending_outcome TEXT CHECK(pending_outcome IS NULL OR pending_outcome IN (
        'succeeded', 'failed', 'exhausted', 'not_ready',
        'stale_bundle_cache', 'cancelled'
    )),
    reason_code TEXT,
    CHECK((status IN ('archiving', 'archive_blocked')) = (pending_outcome IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS replay_jobs_events_job
ON job_events(job_id, event_id);

CREATE TRIGGER IF NOT EXISTS job_events_no_update
BEFORE UPDATE ON job_events
BEGIN
    SELECT RAISE(ABORT, 'job events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS job_events_no_delete
BEFORE DELETE ON job_events
BEGIN
    SELECT RAISE(ABORT, 'job events are append-only');
END;
