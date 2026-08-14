#!/usr/bin/env python3
"""Run one incremental Event Universe ingestion from its JSON configuration."""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from universe.config import load_config  # noqa: E402
from universe.store import UniverseStore  # noqa: E402
from universe.sync import UniverseSync  # noqa: E402


def main() -> int:
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    result = UniverseSync(database, config.object_store()).sync()
    print(json.dumps(result.as_record(), ensure_ascii=False, sort_keys=True))
    return 1 if result.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
