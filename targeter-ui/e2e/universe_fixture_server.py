"""A local Event Universe API over a throwaway fixture store, for the opt-in research e2e.

It builds the same deterministic store as tests/generate_event_universe_contract.py,
serves it on 127.0.0.1:PORT, and deletes everything on exit. No network, no real data.

Usage: .venv/bin/python targeter-ui/e2e/universe_fixture_server.py PORT
"""

from __future__ import annotations

import dataclasses
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from replay.jobs.contracts import parse_runner_config
from tests.test_event_universe_store import G1, R1, _publish_run, _selection_report
from universe.api.server import build_server
from universe.config import load_config
from universe.ingest.sync import UniverseSync
from universe.jobs.auth import AuthStore
from universe.jobs.store import ReplayJobStore
from universe.store import UniverseStore

ROOT = Path(__file__).resolve().parents[2]
FIXED_NOW_NS = 1_767_226_200_000_000_000


def main(port):
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        database = UniverseStore(root / "universe.sqlite3")
        database.initialize()
        objects = LocalObjectStore(root / "objects", store_id="archive", durability=INDEPENDENT)
        _publish_run(objects, _selection_report(R1, G1))
        with mock.patch("universe.store.ingest_tx.time.time_ns", return_value=FIXED_NOW_NS):
            result = UniverseSync(database, objects).sync_range(
                datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2026, 1, 2, tzinfo=timezone.utc))
        if result.failures:
            raise SystemExit(f"fixture sync failed: {result.failures}")
        config = load_config(ROOT / "configs" / "event_universe.json")
        replay = dataclasses.replace(config.replay, database_path=root / "jobs.sqlite3")
        auth = AuthStore(replay.database_path, replay.auth)
        auth.initialize()
        jobs = ReplayJobStore(replay.database_path, replay.jobs)
        jobs.initialize()
        runner = parse_runner_config((ROOT / "configs" / "replay_runner.json").read_bytes())
        server = build_server(database, auth, "127.0.0.1", port, jobs, runner, None)
        print(f"universe fixture listening on {port}", flush=True)
        try:
            server.serve_forever()
        finally:
            server.server_close()


if __name__ == "__main__":
    main(int(sys.argv[1]))
