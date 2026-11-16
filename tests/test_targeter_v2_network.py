from __future__ import annotations

import gzip
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

from analysis.durable_http import HttpRequestError
from replay.catalog import project_record, projection_sha256
from targeter.v2.adapters.clients import durable_client
from targeter.v2.continuity import ContinuityBundle, ContinuityTarget, TerminalProbe
from targeter.v2.models import CatalogSnapshot, SUPPORTED_VENUES
from targeter.v2.registry import load_strategy
from targeter.v2.run import _continuity_for_run, run_shadow
from targeter.v2.run_archive import read_run_report
from tests.test_targeter_v2 import snapshot

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 3, 12, tzinfo=timezone.utc)


class _Client:
    def __init__(self):
        self.network_requests = 0
        self.cache_hits = 0
        self.closed = False

    def get_json(self, *_args, **_kwargs):
        self.network_requests += 1
        return {}

    def close(self):
        self.closed = True


class DiscoveryConcurrencyTests(unittest.TestCase):
    def test_parallel_target_records_use_polymarket_v2_delay_projection(self) -> None:
        catalogs = {
            venue: snapshot(venue, f"{venue}-event", f"{venue}-market")
            for venue in SUPPORTED_VENUES
        }
        polymarket = catalogs["polymarket"]
        record = {**polymarket.markets[0].raw, "secondsDelay": 3}
        catalogs["polymarket"] = replace(
            polymarket, markets=(replace(polymarket.markets[0], raw=record),)
        )

        class Adapter:
            def __init__(self, venue):
                self.venue = venue

            def discover(self, _client, *, now):
                return catalogs[self.venue]

        with tempfile.TemporaryDirectory() as directory, patch(
            "targeter.v2.run.durable_client", side_effect=lambda *_args, **_kwargs: _Client()
        ):
            root = Path(directory)
            result = run_shadow(
                strategy=load_strategy(ROOT / "configs/targeter_v2.json"), now=NOW,
                output_root=root / "runs", cache_root=root / "cache",
                adapters=tuple(Adapter(venue) for venue in SUPPORTED_VENUES),
                artifact_format="ndjson",
            )
            rows = [
                json.loads(line)
                for line in (result.directory / "target_records_polymarket.ndjson").read_text().splitlines()
            ]
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["projection_id"], "polymarket.v2")
            self.assertEqual(project_record("polymarket", row["record"])["secondsDelay"], 3)
            self.assertEqual(row["projection_sha256"], projection_sha256("polymarket", record))
            from universe.derive.execution_terms import target_record_terms

            terms = target_record_terms(row, run_id=result.run_id, venue="polymarket")
            self.assertEqual(terms["taker_delay_ms"], 3000)
            changed_delay = {**record, "secondsDelay": 4}
            self.assertNotEqual(row["projection_sha256"], projection_sha256("polymarket", changed_delay))
            limitless_rows = [
                json.loads(line)
                for line in (result.directory / "target_records_limitless.ndjson").read_text().splitlines()
            ]
            self.assertEqual(limitless_rows[0]["projection_id"], "limitless.v1")

    def test_venues_overlap_with_isolated_clients_and_deterministic_reports(self) -> None:
        barrier = threading.Barrier(3, timeout=2)
        completed = {venue: threading.Event() for venue in SUPPORTED_VENUES}
        clients = []

        def factory(*_args, **_kwargs):
            client = _Client()
            clients.append(client)
            return client

        class Adapter:
            def __init__(self, venue, concurrent):
                self.venue = venue
                self.concurrent = concurrent

            def discover(self, client, *, now):
                if self.concurrent:
                    barrier.wait()
                    index = SUPPORTED_VENUES.index(self.venue)
                    if index + 1 < len(SUPPORTED_VENUES):
                        if not completed[SUPPORTED_VENUES[index + 1]].wait(2):
                            raise AssertionError("venue did not finish concurrently")
                before = client.network_requests
                client.get_json("https://example.test", f"/{self.venue}")
                completed[self.venue].set()
                return replace(
                    snapshot(self.venue, f"{self.venue}-event", f"{self.venue}-market"),
                    requests=client.network_requests - before,
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            strategy = load_strategy(ROOT / "configs/targeter_v2.json")
            baseline = run_shadow(
                strategy=strategy, now=NOW,
                output_root=root / "serial", cache_root=root / "cache",
                adapters=tuple(Adapter(venue, False) for venue in SUPPORTED_VENUES),
                client=_Client(),
            )
            with patch("targeter.v2.run.durable_client", side_effect=factory):
                concurrent = run_shadow(
                    strategy=strategy, now=NOW,
                    output_root=root / "concurrent", cache_root=root / "cache",
                    adapters=tuple(Adapter(venue, True) for venue in SUPPORTED_VENUES),
                )
            self.assertTrue(concurrent.input_complete)
            self.assertTrue(concurrent.selection.selected)
            self.assertEqual(len(clients), 3)
            self.assertTrue(all(client.closed for client in clients))
            self.assertEqual([client.network_requests for client in clients], [1, 1, 1])
            expected = {p.name: p.read_bytes() for p in baseline.directory.iterdir()}
            actual = {p.name: p.read_bytes() for p in concurrent.directory.iterdir()}
            self.assertEqual(actual, expected)

    def test_parallel_vendor_failure_preserves_other_catalogues_and_closes_clients(self):
        clients = []

        def factory(*_args, **_kwargs):
            client = _Client()
            clients.append(client)
            return client

        class Adapter:
            def __init__(self, venue):
                self.venue = venue

            def discover(self, _client, *, now):
                if self.venue == "polymarket":
                    raise RuntimeError("catalog unavailable")
                return CatalogSnapshot(self.venue, (), ())

        with tempfile.TemporaryDirectory() as directory, patch(
            "targeter.v2.run.durable_client", side_effect=factory
        ):
            root = Path(directory)
            result = run_shadow(
                strategy=load_strategy(ROOT / "configs/targeter_v2.json"), now=NOW,
                output_root=root / "runs", cache_root=root / "cache",
                adapters=tuple(Adapter(venue) for venue in SUPPORTED_VENUES),
            )
            report = read_run_report(result.directory)
            self.assertFalse(report["input_complete"])
            self.assertEqual(report["discovery_failures"], {
                "polymarket": "RuntimeError: catalog unavailable",
            })
            self.assertEqual([c["venue"] for c in report["catalogs"]], ["kalshi", "limitless"])
            self.assertTrue(all(client.closed for client in clients))

    def test_continuity_probes_overlap_without_changing_terminal_evidence(self):
        barrier = threading.Barrier(3, timeout=2)
        bundle = ContinuityBundle(
            base_run_id="20260803T110000.000000Z", bundle_id="bundle",
            activation_at=NOW, score=1,
            targets=tuple(
                ContinuityTarget(
                    target_id=f"{venue}:market", venue=venue, venue_market_id="market",
                    canonical_class="soccer.moneyline_3way",
                    subscription_ids=(f"{venue}-subscription",),
                    activation_at=NOW, capture_start_at=NOW,
                    source_ref="/markets/market",
                )
                for venue in SUPPORTED_VENUES
            ),
        )

        class Adapter:
            def __init__(self, venue, concurrent):
                self.venue, self.concurrent = venue, concurrent

            def probe_terminal(self, _client, targets):
                if self.concurrent:
                    barrier.wait()
                key = next(iter(targets))
                state = {"kalshi": "terminal", "polymarket": "open", "limitless": "unknown"}[self.venue]
                return {key: TerminalProbe(state, "contract-shaped probe")}

        with tempfile.TemporaryDirectory() as directory, patch(
            "targeter.v2.run.load_continuity_bundles", return_value=(bundle,)
        ):
            root = Path(directory)
            pointer = root / "targeter-v2/current.json"
            pointer.parent.mkdir()
            pointer.write_text("{}")
            strategy = load_strategy(ROOT / "configs/targeter_v2.json")
            baseline = _continuity_for_run(
                live_root=root, strategy=strategy, now=NOW, client=object(),
                adapters=tuple(Adapter(venue, False) for venue in SUPPORTED_VENUES),
            )
            with ThreadPoolExecutor(max_workers=3) as executor:
                concurrent = _continuity_for_run(
                    live_root=root, strategy=strategy, now=NOW, client=None,
                    clients={venue: _Client() for venue in SUPPORTED_VENUES},
                    executor=executor,
                    adapters=tuple(Adapter(venue, True) for venue in SUPPORTED_VENUES),
                )
            self.assertEqual(concurrent, baseline)


class TargeterTransportTests(unittest.TestCase):
    def test_catalogue_requests_negotiate_gzip_and_reuse_the_connection(self) -> None:
        observed: list[tuple[str, int]] = []
        body = json.dumps({"events": [{"id": "small-contract-shape"}]}).encode()
        failures = []

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                accepts = self.headers.get("Accept-Encoding", "")
                observed.append((accepts, self.client_address[1]))
                compressed = "gzip" in accepts
                status, content = 200, body
                if self.path == "/missing":
                    status, content = 404, b"missing"
                elif self.path == "/retry" and not failures:
                    failures.append(503)
                    status, content = 503, b"unavailable"
                payload = gzip.compress(content) if compressed else content
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                if compressed:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                client = durable_client(
                    Path(directory), force_refresh=True, persist_responses=False
                )
                try:
                    base = f"http://127.0.0.1:{server.server_port}"
                    first = client.get_json(base, "/events?page=1")
                    second = client.get_json(base, "/events?page=2")
                    self.assertEqual(first.data, json.loads(body))
                    self.assertEqual(second.data, first.data)
                    self.assertEqual(client.network_requests, 2)
                    with patch("analysis.durable_http.time.sleep") as sleep:
                        retried = client.get_json(base, "/retry")
                    self.assertEqual(retried.data, first.data)
                    self.assertEqual(client.transient_retry_attempts, 1)
                    sleep.assert_called_once_with(5.0)
                    with self.assertRaisesRegex(HttpRequestError, "HTTP 404.*: missing"):
                        client.get_json(base, "/missing")
                    self.assertEqual(client.transient_retry_attempts, 1)
                finally:
                    close = getattr(client, "close", None)
                    if close is not None:
                        close()
            self.assertTrue(all("gzip" in encoding for encoding, _ in observed))
            self.assertEqual(len({port for _, port in observed}), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
