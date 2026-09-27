from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path

from archive.storage.factory import build_store
from replay.jobs import contracts as jobs
from replay.jobs.stages import StageFailure, verify_published_receipt
from replay.ops.backup import (
    backup_jobs_database,
    restore_jobs_database,
    verify_backup,
)
from replay.ops.preflight import run_preflight
from universe.config import load_config
from universe.replay_jobs import ReplayJobStore


def _root() -> Path:
    value = os.environ.get("REPLAY_DATA_ROOT", "")
    if not value:
        raise RuntimeError("REPLAY_DATA_ROOT is required")
    return Path(value)


def _config():
    return load_config(Path(os.environ.get("EVENT_UNIVERSE_CONFIG", "/etc/prediction-indexer/event_universe.json")))


def _store(root: Path):
    return build_store((root,))


def _jobs(root: Path):
    config = _config()
    if config.replay.database_path != root / "jobs.sqlite3":
        raise RuntimeError("configured Replay database is outside REPLAY_DATA_ROOT")
    store = ReplayJobStore(config.replay.database_path, config.replay.jobs)
    store.initialize()
    return store


def _audit(root: Path) -> dict[str, object]:
    database = root / "jobs.sqlite3"
    with closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        integrity = connection.execute("PRAGMA integrity_check").fetchall()
        statuses = {
            str(row["status"]): int(row["count"])
            for row in connection.execute(
                "SELECT status,count(*) AS count FROM jobs GROUP BY status ORDER BY status"
            )
        }
        oldest = [
            dict(row)
            for row in connection.execute(
                "SELECT job_id,status,stage,reason_code,blocked_reason_code,stage_attempts,"
                "created_at_ns,updated_at_ns FROM jobs WHERE status NOT IN "
                "('succeeded','failed','exhausted','not_ready','stale_bundle_cache','cancelled') "
                "ORDER BY created_at_ns LIMIT 100"
            )
        ]
    now = time.time_ns()
    for row in oldest:
        row["age_seconds"] = max(0, (now - int(row["created_at_ns"])) // 1_000_000_000)
    return {
        "status": "ok" if integrity == [("ok",)] else "failed",
        "integrity_check": [row[0] for row in integrity],
        "statuses": statuses,
        "active_jobs": oldest,
        "runner_lock_present": (root / "runner.lock").exists(),
    }


def _verify_job(root: Path, job_id: str) -> dict[str, object]:
    job_store = _jobs(root)
    found = job_store.get_job(job_id)
    if found is None:
        raise RuntimeError("job not found")
    row, _ = found
    if row.status not in jobs.TERMINAL and row.status not in {jobs.ARCHIVING, jobs.ARCHIVE_BLOCKED}:
        raise RuntimeError("job has no frozen archive receipt")
    try:
        receipt = verify_published_receipt(_store(root), job_id)
    except StageFailure as error:
        raise RuntimeError(f"strict job receipt verification failed: {error.code}") from error
    outcome = row.status if row.status in jobs.TERMINAL else row.pending_outcome
    if (
        receipt.request_sha256 != row.request_sha256
        or receipt.submitted_by != row.submitted_by
        or receipt.final_outcome != outcome
        or receipt.reason_code != row.reason_code
        or receipt.reason_detail != row.reason_detail
        or receipt.created_at_ns != row.created_at_ns
        or (row.finished_at_ns is not None and receipt.finished_at_ns != row.finished_at_ns)
    ):
        raise RuntimeError("strict job receipt verification failed: row_binding")
    return {
        "job_id": job_id,
        "receipt_verified": True,
        "finished_at_ns": str(receipt.finished_at_ns),
        "image_revision": receipt.image_revision,
    }


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments:
        raise SystemExit(
            "usage: python -m replay.ops preflight|backup|verify-backup|restore-backup|audit|resume-blocked|verify-job-receipt ..."
        )
    command, *parameters = arguments
    root = _root()
    if command == "preflight" and len(parameters) == 1:
        result = run_preflight(Path(parameters[0]))
    elif command == "backup" and not parameters:
        receipt = backup_jobs_database(
            root / "jobs.sqlite3",
            root / ".backup",
            _store(root),
            prefix=os.environ.get("REPLAY_BACKUP_PREFIX", "replay/jobs-db-backups"),
            created_at_ns=time.time_ns(),
        )
        result = receipt.document() | {"receipt_key": receipt.receipt_key}
    elif command == "verify-backup" and len(parameters) == 1:
        receipt = verify_backup(_store(root), parameters[0])
        result = receipt.document() | {"receipt_key": receipt.receipt_key, "verified": True}
    elif command == "restore-backup" and len(parameters) == 2:
        path = restore_jobs_database(_store(root), parameters[0], Path(parameters[1]))
        result = {"restored": str(path), "integrity_check": "ok"}
    elif command == "audit" and not parameters:
        result = _audit(root)
    elif command == "resume-blocked" and len(parameters) == 1:
        job_store = _jobs(root)
        found = job_store.get_job(parameters[0])
        if found is None:
            raise RuntimeError("job not found")
        before, _ = found
        after = job_store.save(before, jobs.resume_blocked(before, time.time_ns()))
        result = {"job_id": after.job_id, "status": after.status, "stage": after.stage}
    elif command == "verify-job-receipt" and len(parameters) == 1:
        result = _verify_job(root, parameters[0])
    else:
        raise SystemExit("invalid replay operations command or arguments")
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
