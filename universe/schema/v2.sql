CREATE TABLE cadence_runs (
    run_id TEXT PRIMARY KEY REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    generated_at_ns INTEGER NOT NULL,
    projection_version INTEGER NOT NULL CHECK(projection_version = 1),
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL
) STRICT;
CREATE INDEX cadence_runs_generated
    ON cadence_runs(generated_at_ns DESC, run_id DESC);
