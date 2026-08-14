#!/usr/bin/env python3
"""Run the Event Universe read server from its JSON configuration."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from universe.api import serve  # noqa: E402
from universe.config import load_config  # noqa: E402
from universe.store import UniverseStore  # noqa: E402


def main() -> None:
    config = load_config()
    database = UniverseStore(config.database_path)
    database.initialize()
    serve(database, config.api.host, config.api.port)


if __name__ == "__main__":
    main()
