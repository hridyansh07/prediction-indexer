from __future__ import annotations

import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path

from archive.storage import INDEPENDENT, LocalObjectStore
from eth_account import Account
from eth_account.messages import encode_defunct

from replay.jobs import contracts as c
from tests.test_event_universe_store import G1, R1, _publish_run, _selection_report
from tests.test_replay_auth import ADMIN, MEMBER, auth_config, siwe_message
from tests.test_replay_jobs import ROOT, Limits, request_bytes, runner_config
from universe.api import build_server
from universe.auth import AuthStore, Principal
from universe.replay_jobs import ReplayJobStore
from universe.store import UniverseStore
from universe.sync import UniverseSync


class ReplayJobsHTTPAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.universe = UniverseStore(root / "universe.sqlite3")
        self.universe.initialize()
        objects = LocalObjectStore(
            root / "objects", store_id="test", durability=INDEPENDENT
        )
        _publish_run(objects, _selection_report(R1, G1))
        result = UniverseSync(self.universe, objects).sync(
            now=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)
        )
        self.assertEqual(result.ingested, 1)

        self.jobs_path = root / "jobs.sqlite3"
        self.auth = AuthStore(self.jobs_path, auth_config())
        self.auth.initialize()
        self.auth.add_member(MEMBER.address, "member", ADMIN.address)
        self.jobs = ReplayJobStore(self.jobs_path, Limits())
        self.jobs.initialize()
        self.runner = runner_config()
        self.server = build_server(
            self.universe,
            self.auth,
            "127.0.0.1",
            0,
            self.jobs,
            self.runner,
        )
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict, dict[str, str]]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        payload = json.loads(response.read())
        result = response.status, payload, dict(response.getheaders())
        connection.close()
        return result

    def token(self) -> str:
        nonce = self.auth.create_nonce()["nonce"]
        message = siwe_message(MEMBER.address, nonce)
        signature = Account.sign_message(
            encode_defunct(text=message), MEMBER.key
        ).signature.hex()
        return self.auth.verify_siwe(message, signature)["token"]

    def test_idempotency_header_is_exactly_one_and_valid(self) -> None:
        token = self.token()
        base = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        for key in (None, "bad key", "x" * 129):
            headers = dict(base)
            if key is not None:
                headers["Idempotency-Key"] = key
            status, _payload, _ = self.request(
                "POST", "/v1/replay/jobs", request_bytes(), headers
            )
            self.assertEqual(status, 400)

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        body = request_bytes()
        connection.putrequest("POST", "/v1/replay/jobs")
        connection.putheader("Authorization", f"Bearer {token}")
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(len(body)))
        connection.putheader("Idempotency-Key", "one")
        connection.putheader("Idempotency-Key", "two")
        connection.endheaders(body)
        response = connection.getresponse()
        self.assertEqual(response.status, 400)
        response.read()
        connection.close()

    def test_bad_requests_return_actionable_safe_errors(self) -> None:
        headers = {
            "Authorization": f"Bearer {self.token()}",
            "Content-Type": "application/json",
            "Idempotency-Key": "diagnostic-errors",
        }
        malformed_cases = (
            (b"{", "invalid JSON at line 1 column 2"),
            (b'{"x":1,"x":2}', "duplicate JSON key: x"),
            (b"[]", "JSON body must be an object"),
        )
        for body, message in malformed_cases:
            with self.subTest(message=message):
                status, payload, _ = self.request(
                    "POST", "/v1/replay/jobs", body, headers
                )
                self.assertEqual(status, 400)
                self.assertEqual(payload, {"error": message})

        document = json.loads(request_bytes())
        document["unexpected"] = True
        status, payload, _ = self.request(
            "POST", "/v1/replay/jobs", json.dumps(document).encode(), headers
        )
        self.assertEqual(status, 400)
        self.assertIn("request must have exactly the fields", payload["error"])

    def test_sign_in_submit_replay_read_cancel_claim_and_restart(self) -> None:
        token = self.token()
        raw = request_bytes(pretty=True)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Idempotency-Key": "acceptance-key",
        }
        status, submitted, response_headers = self.request(
            "POST", "/v1/replay/jobs", raw, headers
        )
        self.assertEqual(status, 201)
        self.assertFalse(submitted["replayed"])
        self.assertEqual(response_headers["Cache-Control"], "no-store")
        job_id = submitted["job_id"]

        status, replayed, _ = self.request(
            "POST", "/v1/replay/jobs", request_bytes(), headers
        )
        self.assertEqual(
            (status, replayed["job_id"], replayed["replayed"]),
            (200, job_id, True),
        )
        status, listing, _ = self.request("GET", "/v1/replay/jobs?limit=1")
        self.assertEqual((status, listing["jobs"][0]["job_id"]), (200, job_id))
        self.assertNotIn("request", listing["jobs"][0])
        status, detail, _ = self.request("GET", f"/v1/replay/jobs/{job_id}")
        self.assertEqual(detail["request"]["bundle_id"], "bundle-1")
        self.assertNotIn("request_json", detail)
        self.assertNotIn("idempotency_key", detail)
        status, events, _ = self.request(
            "GET", f"/v1/replay/jobs/{job_id}/events"
        )
        self.assertEqual(
            [event["event_type"] for event in events["events"]], ["submitted"]
        )

        status, cancelled, _ = self.request(
            "POST", f"/v1/replay/jobs/{job_id}/cancel", b"{}", headers
        )
        self.assertEqual((status, cancelled["pending_outcome"]), (200, "cancelled"))
        status, first_events, _ = self.request(
            "GET", f"/v1/replay/jobs/{job_id}/events?limit=1"
        )
        self.assertEqual(status, 200)
        self.assertIsNotNone(first_events["next_cursor"])
        status, second_events, _ = self.request(
            "GET",
            f"/v1/replay/jobs/{job_id}/events?limit=1&cursor="
            + first_events["next_cursor"],
        )
        self.assertEqual(second_events["events"][0]["event_type"], "cancelled")
        claim = self.jobs.claim_next(
            self.runner.orchestration, int(cancelled["updated_at_ns"]) + 1
        )
        self.assertIsNotNone(claim)
        assert claim is not None
        self.assertEqual((claim.row.job_id, claim.mode), (job_id, "initialize"))
        self.assertEqual(claim.request_bytes, raw)

        restarted_auth = AuthStore(self.jobs_path, auth_config())
        restarted_auth.initialize()
        restarted_jobs = ReplayJobStore(self.jobs_path, Limits())
        restarted_jobs.initialize()
        self.assertEqual(
            restarted_auth.authenticate({"Authorization": f"Bearer {token}"}),
            Principal(MEMBER.address.lower(), "member"),
        )
        durable = restarted_jobs.lookup_submission(
            MEMBER.address.lower(),
            "acceptance-key",
            c.request_sha256(c.parse_request(raw, self.runner)),
        )
        self.assertIsNotNone(durable)
        with sqlite3.connect(self.jobs_path) as connection:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0], "ok"
            )

    def restart_with_runner(self, runner: c.RunnerConfig) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.server = build_server(
            self.universe, self.auth, "127.0.0.1", 0, self.jobs, runner
        )
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.port = self.server.server_address[1]

    def test_job_detail_survives_runner_registry_changes(self) -> None:
        """A request accepted under one registry stays readable after the registry
        drops its preset or strategy; detail must not re-validate history."""
        raw = request_bytes()
        headers = {
            "Authorization": f"Bearer {self.token()}",
            "Content-Type": "application/json",
            "Idempotency-Key": "registry-change",
        }
        status, submitted, _ = self.request("POST", "/v1/replay/jobs", raw, headers)
        self.assertEqual(status, 201)
        job_id = submitted["job_id"]
        shipped = json.loads((ROOT / "configs/replay_runner.json").read_bytes())
        renamed_preset = {**shipped, "limits": {"standard": shipped["limits"]["small"]}}
        renamed_strategy = {
            **shipped,
            "strategies": {"coverage_v2": shipped["strategies"]["bundle_coverage"]},
        }
        for name, document in (
            ("preset removed", renamed_preset),
            ("strategy removed", renamed_strategy),
        ):
            with self.subTest(name):
                self.restart_with_runner(
                    c.parse_runner_config(json.dumps(document).encode())
                )
                status, detail, _ = self.request("GET", f"/v1/replay/jobs/{job_id}")
                self.assertEqual(status, 200, detail)
                self.assertEqual(detail["request"], json.loads(raw))


if __name__ == "__main__":
    unittest.main()
