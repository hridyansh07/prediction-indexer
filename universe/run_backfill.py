#!/usr/bin/env python3
"""Backfill one configured Targeter v3 generated-time range from ObjectStore."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from universe.backfill import backfill_targeter_history  # noqa: E402
from universe.config import load_config  # noqa: E402
from universe.store import UniverseStore  # noqa: E402


def main() -> int:
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
    )
    print(json.dumps(result.as_record(), ensure_ascii=False, sort_keys=True))
    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
