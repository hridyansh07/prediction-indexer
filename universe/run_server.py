#!/usr/bin/env python3
"""Run the Event Universe read server from its JSON configuration."""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from universe.api import serve  # noqa: E402
from universe.auth import AuthStore  # noqa: E402
from universe.config import load_config  # noqa: E402
from universe.replay_jobs import ReplayJobStore  # noqa: E402
from universe.store import UniverseStore  # noqa: E402
from replay.jobs.contracts import parse_runner_config  # noqa: E402


def main() -> None:
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
    serve(
        database,
        auth,
        config.api.host,
        config.api.port,
        replay_jobs,
        runner_config,
    )


if __name__ == "__main__":
    main()
