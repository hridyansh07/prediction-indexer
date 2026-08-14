#!/usr/bin/env python3
"""Create and upload one consistent Event Universe SQLite backup."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from encoder import StoredIdentity  # noqa: E402
from universe.config import load_config  # noqa: E402
from universe.store import (  # noqa: E402
    SQLITE_CONTENT_TYPE,
    UniverseStore,
    file_sha256,
)


def main() -> None:
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


if __name__ == "__main__":
    main()
