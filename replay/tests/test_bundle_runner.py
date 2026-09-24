import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import Mock, patch

from encoder import DEFAULT_ZSTD_LEVEL, encode_stream, encoder_version
from replay.streams import ProtocolError
from replay.tests.test_supervisor import metadata_pin, normalizer
from scripts import replay_bundle

URL = os.environ.get("REPLAY_REDIS_URL")
ROOT = Path(__file__).resolve().parents[2]
MATERIALIZER = ROOT / "engine/target/debug/examples/materialize_range"
PUBLISHER = ROOT / "engine/target/debug/replay-publish"


def _content_hash(payload):
    data = payload.encode()
    return hashlib.sha256(
        b"indexer.content.v1" + len(data).to_bytes(8, "big") + data
    ).hexdigest()


def _output(path, logical):
    with path.open("wb") as sink:
        result = encode_stream(io.BytesIO(logical), sink, level=DEFAULT_ZSTD_LEVEL)
    return {
        "file": path.name,
        "content_encoding": "zstd",
        "decoded": result.logical.as_record(),
        "stored": result.stored.as_record(),
        "compression": {
            "algorithm": "zstd",
            "level": DEFAULT_ZSTD_LEVEL,
            "frame_checksum": True,
            "dictionary": None,
            "frame_count": 1,
            "encoder": encoder_version(),
        },
    }


def write_two_venue_canonical(root):
    directory = root / "date=1970-01-01" / "window=0"
    directory.mkdir(parents=True)
    kalshi = (
        ROOT
        / "engine/crates/replay-normalizers/tests/fixtures/kalshi/orderbook_snapshot.json"
    ).read_text().strip()
    polymarket = (
        ROOT
        / "engine/crates/replay-normalizers/tests/fixtures/polymarket/book.json"
    ).read_text().strip()
    rows = [
        (
            "kalshi",
            kalshi,
            {"type": "update_range", "first": 2, "last": 2, "previous_last": 1},
            "continuous",
        ),
        (
            "polymarket",
            polymarket,
            {"type": "unsequenced", "counter": 1},
            "unsequenced_venue",
        ),
    ]
    evidence, provenance = bytearray(), bytearray()
    for sequence, (lane, payload, cursor, continuity) in enumerate(rows, 1):
        envelope = {
            "envelope_version": 2,
            "delivery_index": 1,
            "record_id": f"{lane}-1",
            "visible_ns": sequence,
            "monotonic_ns": sequence,
            "venue": lane,
            "stream": "public_book",
            "connection_epoch": f"{lane}-epoch",
            "local_counter": 1,
            "source_cursor": cursor,
            "kind": "venue_frame",
            "raw_payload": payload,
        }
        evidence.extend((json.dumps(envelope, separators=(",", ":")) + "\n").encode())
        provenance.extend(
            (
                json.dumps(
                    {
                        "canonical_seq": sequence,
                        "lane_id": lane,
                        "source_segment_sha256": hashlib.sha256(lane.encode()).hexdigest(),
                        "source_line_number": 1,
                        "record_id": f"{lane}-1",
                        "content_hash": _content_hash(payload),
                        "continuity_verdict": continuity,
                        "visible_tie_group": None,
                    },
                    separators=(",", ":"),
                )
                + "\n"
            ).encode()
        )
    receipt = {
        "receipt_version": 1,
        "window_start_ns": 0,
        "window_end_ns": 100,
        "completeness": "complete",
        "certified": True,
        "expected_lanes": ["kalshi", "polymarket"],
        "present_lanes": ["kalshi", "polymarket"],
        "unexpected_lanes": [],
        "missing_lanes": [],
        "invalid_lanes": [],
        "finalization_deadline_seconds": 300,
        "deadline_expired": False,
        "finalized_at_ns": 100,
        "inputs": [
            {
                "lane": lane,
                "data_file": f"{lane}.ndjson",
                "segment_index": 0,
                "line_count": 1,
                "sha256": hashlib.sha256(lane.encode()).hexdigest(),
                "first_delivery_index": 1,
                "last_delivery_index": 1,
            }
            for lane in ("kalshi", "polymarket")
        ],
        "evidence": _output(directory / "evidence.ndjson.zst", evidence),
        "provenance": _output(directory / "provenance.ndjson.zst", provenance),
        "first_canonical_seq": 1,
        "last_canonical_seq": 2,
        "carried": {
            "ordering": {"connections": [], "epochs": []},
            "lane_visible_ns": {},
        },
        "clock_faults": [],
        "finalizer_version": 1,
    }
    (directory / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


class BundleRunnerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.canonical = self.root / "canonical"
        self.derivatives = self.root / "derivatives"
        self.canonical.mkdir()
        self.derivatives.mkdir()
        self.pin = metadata_pin()
        target = self.derivatives / "window=0" / self.pin["derivative_address"]
        target.mkdir(parents=True)
        for name in ("receipt.json", "manifest.json"):
            (target / name).write_bytes((Path(self.pin["directory"]) / name).read_bytes())
        self.pin["directory"] = str(target)
        self.request = {
            "version": 1,
            "run_id": "bundle-test",
            "interval": {"start_ns": "0", "end_ns": "100", "lower_bound": "clip"},
            "capture": {
                "canonical_root": str(self.canonical),
                "derivative_root": str(self.derivatives),
            },
            "plans": [
                {
                    "instrument": "kalshi:A",
                    "orientation": "outcome",
                    "lane": "kalshi",
                    "venue": "kalshi",
                },
                {
                    "instrument": "polymarket:T",
                    "orientation": "outcome",
                    "lane": "polymarket",
                    "venue": "polymarket",
                },
            ],
            "strategy": {
                "group": "coverage",
                "factory": "replay.tests.test_supervisor:strategy",
                "revision": "test-v1",
                "config": {},
            },
            "runtime": {
                "materializer": "/bin/materialize_range",
                "publisher": "/bin/replay-publish",
                "python": "/usr/bin/python3",
                "scope": "test",
                "max_entry_bytes": "65536",
                "max_queue_bytes": "1048576",
                "command_timeout_ms": 500,
                "limits": {
                    "attempts": 2,
                    "no_progress": 1,
                    "progress_margin": 1,
                    "stall_seconds": 2,
                    "attempt_seconds": 5,
                    "run_seconds": 20,
                    "poll_seconds": 0.01,
                    "stop_seconds": 0.2,
                },
            },
        }

    def tearDown(self):
        self.tmp.cleanup()

    def helper_result(self):
        return {
            "version": 1,
            "normalizer": normalizer(),
            "derivatives": [
                {
                    "window_start_ns": 0,
                    "window_end_ns": 100,
                    "derivative_address": self.pin["derivative_address"],
                    "receipt_sha256": self.pin["receipt_sha256"],
                }
            ],
        }

    def test_closed_request_derives_scales_binds_and_reruns(self):
        process = Mock(
            returncode=0,
            stdout=replay_bundle._canonical(self.helper_result()),
            stderr=b"",
        )
        completion = {
            "version": 1,
            "identity": "a" * 64,
            "attempt": "b" * 32,
            "terminal": 2,
            "outputs": {"coverage": "output"},
        }
        workdir = self.root / "work"
        with (
            patch.object(replay_bundle.subprocess, "run", return_value=process),
            patch.object(replay_bundle.supervisor, "run", return_value=completion) as run,
            patch.object(replay_bundle.supervisor, "read_success", return_value=completion),
        ):
            first = replay_bundle.execute(self.request, workdir, "redis://secret@unused")
            second = replay_bundle.execute(self.request, workdir, "redis://secret@unused")
        self.assertEqual(first, second)
        transport = run.call_args.args[0]["transport"]
        self.assertEqual(transport["plans"][0]["price_scale"], "2")
        self.assertEqual(transport["plans"][0]["quantity_scale"], "0")
        self.assertNotIn("redis", (workdir / "request.json").read_text().lower())
        self.assertEqual(
            json.loads((workdir / "result.json").read_bytes()), first
        )

        changed = copy.deepcopy(self.request)
        changed["interval"]["end_ns"] = "99"
        with self.assertRaisesRegex(ProtocolError, "immutable request"):
            replay_bundle.execute(changed, workdir, "redis://secret@unused")

    def test_request_rejects_caller_scales_and_error_redacts_redis_url(self):
        invalid = copy.deepcopy(self.request)
        invalid["plans"][0]["price_scale"] = "2"
        with self.assertRaises(ProtocolError):
            replay_bundle.validate_request(invalid)

        path = self.root / "request.json"
        path.write_bytes(replay_bundle._canonical(self.request))
        stderr = io.StringIO()
        with (
            patch.dict(os.environ, {"REDIS_URL": "redis://user:top-secret@unused"}),
            patch.object(
                replay_bundle.subprocess,
                "run",
                return_value=Mock(returncode=20, stdout=b"", stderr=b"top-secret"),
            ),
            redirect_stderr(stderr),
        ):
            self.assertEqual(replay_bundle.main([str(path), str(self.root / "failed")]), 20)
        self.assertNotIn("top-secret", stderr.getvalue())
        self.assertFalse((self.root / "failed/result.json").exists())


@unittest.skipUnless(
    URL and MATERIALIZER.exists() and PUBLISHER.exists(),
    "requires disposable REPLAY_REDIS_URL >=8.2 and prebuilt Stage-B binaries",
)
class BundleRunnerRedisAcceptance(unittest.TestCase):
    def test_two_venue_materialize_supervise_complete_and_noop_rerun(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            canonical, derivatives, work = (
                root / "canonical",
                root / "derivatives",
                root / "work",
            )
            write_two_venue_canonical(canonical)
            token = "5615282760875985231868508008056959876238536896643315063916840237042205273721"
            books = [
                ["kalshi:FED-23DEC-T3.00", "outcome"],
                ["kalshi:FED-23DEC-T3.00", "complement"],
                [f"polymarket:{token}", "outcome"],
            ]
            request = {
                "version": 1,
                "run_id": f"bundle-e2e-{os.getpid()}",
                "interval": {"start_ns": "0", "end_ns": "100", "lower_bound": "clip"},
                "capture": {
                    "canonical_root": str(canonical),
                    "derivative_root": str(derivatives),
                },
                "plans": [
                    {
                        "instrument": instrument,
                        "orientation": orientation,
                        "lane": "kalshi" if instrument.startswith("kalshi:") else "polymarket",
                        "venue": instrument.split(":", 1)[0],
                    }
                    for instrument, orientation in books
                ],
                "strategy": {
                    "group": "coverage",
                    "factory": "replay.tests.test_supervisor:strategy",
                    "revision": "test-v1",
                    "config": {"required_usable_books": books},
                },
                "runtime": {
                    "materializer": str(MATERIALIZER),
                    "publisher": str(PUBLISHER),
                    "python": sys.executable,
                    "scope": "test",
                    "max_entry_bytes": "1048576",
                    "max_queue_bytes": "67108864",
                    "command_timeout_ms": 1000,
                    "limits": {
                        "attempts": 2,
                        "no_progress": 1,
                        "progress_margin": 1,
                        "stall_seconds": 5,
                        "attempt_seconds": 15,
                        "run_seconds": 30,
                        "poll_seconds": 0.02,
                        "stop_seconds": 1,
                    },
                },
            }
            request_path = root / "request.json"
            request_path.write_bytes(replay_bundle._canonical(request))
            environment = {**os.environ, "REDIS_URL": URL}
            command = [sys.executable, "scripts/replay_bundle.py", str(request_path), str(work)]
            first = subprocess.run(
                command,
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=40,
            )
            diagnostics = {
                str(path.relative_to(work)): path.read_text()
                for pattern in ("run/**/result.json", "run/**/books.json")
                for path in work.glob(pattern)
            }
            self.assertEqual(first.returncode, 0, f"{first.stderr}\n{diagnostics}")
            result = json.loads(first.stdout)
            self.assertEqual(len(result["pins"]), 1)
            receipt_path = Path(result["pins"][0]["directory"]) / "receipt.json"
            receipt_before = receipt_path.read_bytes()
            success = replay_bundle.supervisor.read_success(work / "run")
            output_directory = work / "run" / success["outputs"]["coverage"]
            output = output_directory / "sequences.txt"
            self.assertTrue(output.read_text().startswith("0\n"))
            self.assertEqual(
                set(json.loads((output_directory / "books.json").read_text()).values()),
                {"usable"},
            )

            second = subprocess.run(
                command,
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=40,
            )
            self.assertEqual(second.returncode, 0, second.stderr)
            self.assertEqual(json.loads(second.stdout), result)
            self.assertEqual(receipt_path.read_bytes(), receipt_before)

            changed = copy.deepcopy(request)
            changed["interval"]["end_ns"] = "99"
            changed_path = root / "changed.json"
            changed_path.write_bytes(replay_bundle._canonical(changed))
            rejected = subprocess.run(
                [sys.executable, "scripts/replay_bundle.py", str(changed_path), str(work)],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(rejected.returncode, 20)
            self.assertNotIn(URL, rejected.stderr)


if __name__ == "__main__":
    unittest.main()
