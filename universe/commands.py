"""The four configured Event Universe commands; each reads only the JSON config.

Run as ``python -m universe {serve,sync,backfill,backup}``. There are no options:
configuration lives in ``configs/event_universe.json``.
"""

import json
from datetime import datetime, timezone

from encoder import StoredIdentity
from replay.jobs.contracts import parse_runner_config
from universe.api import serve as serve_api
from universe.api.rate_limit import RateLimiter
from universe.config import load_config
from universe.ingest.backfill import backfill_targeter_history
from universe.ingest.sync import UniverseSync
from universe.jobs.auth import AuthStore
from universe.jobs.store import ReplayJobStore
from universe.store import SQLITE_CONTENT_TYPE, UniverseStore, file_sha256


def serve() -> None:
    """Run the Event Universe read server from its JSON configuration."""
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    auth = AuthStore(config.replay.database_path, config.replay.auth)
    auth.initialize()
    runner_config = parse_runner_config(
        config.replay.jobs.runner_config_path.read_bytes()
    )
    replay_jobs = ReplayJobStore(config.replay.database_path, config.replay.jobs)
    replay_jobs.initialize()
    rate_limiter = RateLimiter(config.replay.rate_limit, auth)
    serve_api(
        database,
        auth,
        config.api.host,
        config.api.port,
        replay_jobs,
        runner_config,
        rate_limiter,
    )


def sync() -> int:
    """Run one incremental Event Universe ingestion from its JSON configuration."""
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    result = UniverseSync(
        database,
        config.object_store(),
        temporary_directory=config.temporary_directory,
    ).sync()
    print(json.dumps(result.as_record(), ensure_ascii=False, sort_keys=True))
    return 1 if result.failure_count or result.pending_failures else 0


def backfill() -> int:
    """Backfill one configured Targeter v3 generated-time range from ObjectStore."""
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    if config.backfill.generated_start is None or config.backfill.generated_end is None:
        raise RuntimeError(
            "backfill.generated_start and backfill.generated_end must be set in "
            "the Event Universe JSON config"
        )
    result = backfill_targeter_history(
        objects=config.object_store(),
        database=database,
        generated_start=config.backfill.generated_start,
        generated_end=config.backfill.generated_end,
        temporary_directory=config.temporary_directory,
        progress=lambda record: print(
            json.dumps(record, ensure_ascii=False, sort_keys=True), flush=True
        ),
    )
    print(
        json.dumps(
            {"type": "backfill_summary", **result.as_record()},
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 1 if result.failure_count or result.pending_failures else 0


def backup() -> None:
    """Create and upload one consistent Event Universe SQLite backup."""
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    name = f"event-universe-{timestamp}.sqlite3"
    path = database.backup(config.backup.directory / name)
    sha256, byte_length = file_sha256(path)
    store = config.object_store()
    key = f"{config.backup.object_prefix}/{name}"
    with path.open("rb") as source:
        metadata = store.put_immutable(
            key,
            source,
            StoredIdentity(sha256=sha256, byte_length=byte_length),
            content_type=SQLITE_CONTENT_TYPE,
        )
    if metadata.sha256 != sha256 or metadata.byte_length != byte_length:
        raise RuntimeError("published backup failed identity verification")
    print(
        json.dumps(
            {
                "path": str(path),
                "object_key": key,
                "sha256": sha256,
                "byte_length": byte_length,
            },
            sort_keys=True,
        )
    )
