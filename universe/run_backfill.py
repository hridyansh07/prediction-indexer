#!/usr/bin/env python3
"""Run one remote historical control backfill from its JSON configuration."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from universe.backfill import backfill_segment_universe  # noqa: E402
from universe.config import load_config  # noqa: E402
from universe.store import UniverseStore  # noqa: E402


def main() -> int:
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    result = backfill_segment_universe(
        objects=config.object_store(),
        database=database,
        temp_root=config.temporary_directory,
    )
    print(json.dumps(result.as_record(), ensure_ascii=False, sort_keys=True))
    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
