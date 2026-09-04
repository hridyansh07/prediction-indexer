"""Emit deterministic responses from the current Event Universe application."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from archive.storage import INDEPENDENT, LocalObjectStore
from tests.test_event_universe_store import R1, G1, _publish_run, _selection_report
from universe.api import UniverseApplication
from universe.store import SCHEMA_VERSION, UniverseStore
from universe.sync import UniverseSync

FIXED_NOW_NS = 1_767_226_200_000_000_000


def generate_contract() -> dict:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        database = UniverseStore(root / "universe.sqlite3")
        database.initialize()
        objects = LocalObjectStore(
            root / "objects", store_id="archive", durability=INDEPENDENT
        )
        _publish_run(objects, _selection_report(R1, G1))
        with mock.patch("universe.store.time.time_ns", return_value=FIXED_NOW_NS):
            result = UniverseSync(database, objects).sync_range(
                datetime(2026, 1, 1, tzinfo=timezone.utc),
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            )
            if result.failures:
                raise RuntimeError(f"fixture sync failed: {result.failures}")
            application = UniverseApplication(database)
            event_page = _response(application, "/v1/events?limit=100")
            event_id = event_page["events"][0]["event_id"]
            cases = [
                _case("health_ok", "/healthz", _response(application, "/healthz")),
                _case(
                    "targeter_status",
                    "/v1/targeter/status?limit=5",
                    _response(application, "/v1/targeter/status?limit=5"),
                ),
                _case(
                    "runs",
                    "/v1/runs?limit=100",
                    _response(application, "/v1/runs?limit=100"),
                ),
                _case(
                    "selections",
                    "/v1/selections?limit=100",
                    _response(application, "/v1/selections?limit=100"),
                ),
                _case(
                    "bundles",
                    "/v1/bundles?limit=100",
                    _response(application, "/v1/bundles?limit=100"),
                ),
                _case("events", "/v1/events?limit=100", event_page),
                _case(
                    "event_detail",
                    f"/v1/events/{event_id}",
                    _response(application, f"/v1/events/{event_id}"),
                ),
                _case(
                    "targeter_run",
                    f"/v1/targeter/runs/{R1}",
                    _response(application, f"/v1/targeter/runs/{R1}"),
                ),
            ]
            database.record_sync_failure(
                "targeter-v2/runs/date=2026-01-01/run=bad/run_manifest.json",
                "contract fixture",
                now_ns=FIXED_NOW_NS,
            )
            cases.append(
                _case(
                    "health_degraded",
                    "/healthz",
                    _response(application, "/healthz"),
                )
            )
    return {
        "fixture_version": 1,
        "schema_version": SCHEMA_VERSION,
        "cases": cases,
    }


def _response(application: UniverseApplication, path: str) -> dict:
    status, body = application.get(path)
    if status != 200:
        raise RuntimeError(f"fixture request {path} returned {status}")
    return body


def _case(name: str, path: str, body: dict) -> dict:
    return {"name": name, "path": path, "body": body}


if __name__ == "__main__":
    print(json.dumps(generate_contract(), separators=(",", ":"), sort_keys=True))
