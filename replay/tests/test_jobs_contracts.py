import json
import sqlite3
import unittest
from contextlib import closing
from pathlib import Path

from archive.archiver.canonical import canonical_object_keys
from replay.jobs import contracts as c
from replay.tests.test_supervisor import normalizer

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "replay_runner.json"
JOB = "20260924T120000Z-0123456789abcdef"
HOUR = 3_600_000_000_000
HALF = HOUR // 2


def raw(value):
    return json.dumps(value).encode()


def runner_config():
    return c.parse_runner_config(CONFIG_PATH.read_bytes())


def request(**changes):
    value = {
        "replay_request_version": 1,
        "bundle_id": "bundle_0123456789abcdef01234567",
        "probe_markets": None,
        "interval": None,
        "strategy": {"name": "bundle_coverage", "config": {}},
        "limits": "small",
    }
    value.update(changes)
    return value


def receipt_document(**changes):
    value = {
        "replay_bundle_receipt_version": 1,
        "bundle_id": "bundle_0123456789abcdef01234567",
        "interval": {"start_ns": str(4 * HALF), "end_ns": str(6 * HALF)},
        "canonical_window_seconds": 1800,
        "normalizer": normalizer(),
        "windows": [
            {
                "window_start_ns": str(start),
                "window_end_ns": str(start + HALF),
                "canonical_receipt_sha256": f"{index}" * 64,
                "derivative_address": f"{index + 2}" * 64,
                "receipt_sha256": f"{index + 4}" * 64,
            }
            for index, start in enumerate((4 * HALF, 5 * HALF))
        ],
        "built_by_job": JOB,
    }
    value.update(changes)
    return value


def canonical(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode()


class RequestTest(unittest.TestCase):
    def test_minimal_request_parses(self):
        parsed = c.parse_request(raw(request()), runner_config())
        self.assertEqual(parsed.bundle_id, "bundle_0123456789abcdef01234567")
        self.assertIsNone(parsed.interval)
        self.assertIsNone(parsed.probe_markets)
        self.assertEqual(parsed.strategy, "bundle_coverage")
        self.assertEqual(len(parsed.sha256), 64)

    def test_interval_and_probe_parse(self):
        parsed = c.parse_request(
            raw(
                request(
                    interval={"start_ns": "10", "end_ns": "20"},
                    probe_markets=["kalshi:A", "polymarket:1"],
                )
            ),
            runner_config(),
        )
        self.assertEqual(parsed.interval, (10, 20))
        self.assertEqual(parsed.probe_markets, ("kalshi:A", "polymarket:1"))

    def test_hash_ignores_whitespace_and_key_order(self):
        config = runner_config()
        compact = c.parse_request(canonical(request()), config)
        spaced = c.parse_request(json.dumps(request(), indent=2).encode(), config)
        self.assertEqual(compact.sha256, spaced.sha256)

    def test_rejections(self):
        config = runner_config()
        cases = {
            "unknown field": raw({**request(), "extra": 1}),
            "missing field": raw({k: v for k, v in request().items() if k != "limits"}),
            "duplicate key": b'{"bundle_id":"a","bundle_id":"b"}',
            "bad version": raw(request(replay_request_version=2)),
            "bool version": raw(request(replay_request_version=True)),
            "traversing bundle": raw(request(bundle_id="..")),
            "slash bundle": raw(request(bundle_id="a/b")),
            "empty probe": raw(request(probe_markets=[])),
            "unsorted probe": raw(request(probe_markets=["polymarket:1", "kalshi:A"])),
            "duplicate probe": raw(request(probe_markets=["kalshi:A", "kalshi:A"])),
            "unqualified probe": raw(request(probe_markets=["A"])),
            "numeric interval": raw(request(interval={"start_ns": 1, "end_ns": 2})),
            "leading zero": raw(request(interval={"start_ns": "01", "end_ns": "2"})),
            "empty interval": raw(request(interval={"start_ns": "5", "end_ns": "5"})),
            "unknown strategy": raw(request(strategy={"name": "x", "config": {}})),
            "factory as name": raw(
                request(strategy={"name": "os:system", "config": {}})
            ),
            "runner-owned key": raw(
                request(
                    strategy={
                        "name": "bundle_coverage",
                        "config": {"snapshot_directory": "/tmp"},
                    }
                )
            ),
            "unknown config key": raw(
                request(strategy={"name": "bundle_coverage", "config": {"x": 1}})
            ),
            "unknown preset": raw(request(limits="huge")),
            "oversize": b" " * (c.MAX_REQUEST_BYTES + 1),
            "not json": b"{",
            "nan": b'{"replay_request_version": NaN}',
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_request(body, config)

    def test_runner_owned_key_error_names_the_key(self):
        body = raw(
            request(
                strategy={"name": "bundle_coverage", "config": {"snapshot_sha256": "x"}}
            )
        )
        with self.assertRaisesRegex(c.ContractError, "runner-owned keys.*snapshot_sha256"):
            c.parse_request(body, runner_config())

    def test_parsed_request_is_immutable(self):
        parsed = c.parse_request(raw(request()), runner_config())
        with self.assertRaises(TypeError):
            parsed.document["bundle_id"] = "other"


class StatusTest(unittest.TestCase):
    def test_every_allowed_transition(self):
        allowed = {
            ("queued", "running"),
            ("queued", "cancelled"),
            ("running", "archiving"),
            ("running", "not_ready"),
            ("running", "stale_bundle_cache"),
            ("archiving", "succeeded"),
            ("archiving", "failed"),
            ("archiving", "exhausted"),
        }
        for current in c.STATUSES:
            for new in c.STATUSES:
                with self.subTest(current=current, new=new):
                    if (current, new) in allowed:
                        c.check_transition(current, new)
                    else:
                        with self.assertRaises(c.ContractError):
                            c.check_transition(current, new)

    def test_terminal_and_resumable_partition(self):
        self.assertEqual(c.TERMINAL | c.RESUMABLE | {"queued"}, c.STATUSES)
        self.assertFalse(c.TERMINAL & c.RESUMABLE)
        for status in c.TERMINAL:
            self.assertEqual(c.ALLOWED_TRANSITIONS[status], frozenset())

    def test_unknown_status_and_stage(self):
        with self.assertRaises(c.ContractError):
            c.check_transition("queued", "paused")
        with self.assertRaises(c.ContractError):
            c.check_stage("compile")
        self.assertEqual(c.check_stage("archive"), "archive")


class JobsTableTest(unittest.TestCase):
    def test_schema_applies_and_constrains(self):
        with closing(sqlite3.connect(":memory:")) as connection:
            connection.executescript(c.JOBS_SCHEMA_SQL)
            connection.executescript(c.JOBS_SCHEMA_SQL)  # idempotent
            row = (JOB, 1, "0xabc", b"{}", "a" * 64, "queued", None, 1)
            connection.execute(
                "INSERT INTO jobs(job_id, created_at_ns, submitted_by, request_json,"
                " request_sha256, status, stage, updated_at_ns)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            for column, value in (("status", "paused"), ("stage", "compile")):
                with self.subTest(column), self.assertRaises(sqlite3.IntegrityError):
                    connection.execute(f"UPDATE jobs SET {column} = ?", (value,))


class IdentifierTest(unittest.TestCase):
    def test_job_id(self):
        # 2026-07-30T12:00:00Z, shared with the finalizer's partition test.
        value = c.job_id(1_785_412_800_123_456_789, "0123456789abcdef")
        self.assertEqual(value, "20260730T120000Z-0123456789abcdef")
        self.assertEqual(c.check_job_id(value), value)
        for bad in ("20260730T120000Z-0123", "x", "20260730T120000Z-0123456789ABCDEF"):
            with self.subTest(bad), self.assertRaises(c.ContractError):
                c.check_job_id(bad)
        with self.assertRaises(c.ContractError):
            c.job_id(0, "XYZ")

    def test_date_partition_matches_finalizer_vectors(self):
        # Vectors from ingester/crates/finalize/src/window.rs.
        self.assertEqual(c.date_partition(0), "1970-01-01")
        self.assertEqual(c.date_partition(1_785_412_800_000_000_000), "2026-07-30")
        self.assertEqual(c.date_partition(1_709_164_800_000_000_000), "2024-02-29")

    def test_canonical_keys_match_archiver(self):
        for start in (0, 1_785_412_800_000_000_000, 1_709_164_800_000_000_000 - HALF):
            with self.subTest(start):
                receipt = Path(
                    f"canonical/date={c.date_partition(start)}/window={start}/receipt.json"
                )
                self.assertEqual(
                    c.canonical_window_keys(start), canonical_object_keys(receipt)
                )

    def test_object_keys(self):
        address = "a" * 64
        self.assertEqual(
            c.derivative_key(address, "receipt.json"),
            f"replay/derivatives/{address}/receipt.json",
        )
        self.assertEqual(
            c.bundle_receipt_key("bundle_x"), "replay/bundles/bundle_x/bundle_receipt.json"
        )
        self.assertEqual(
            c.job_object_key(JOB, "run/SUCCESS.json"), f"replay/jobs/{JOB}/run/SUCCESS.json"
        )
        self.assertEqual(c.job_receipt_key(JOB), f"replay/jobs/{JOB}/job_receipt.json")
        for call in (
            lambda: c.derivative_key("A" * 64, "receipt.json"),
            lambda: c.derivative_key(address, "other.json"),
            lambda: c.bundle_receipt_key("a/b"),
            lambda: c.job_object_key(JOB, "../x"),
            lambda: c.job_object_key(JOB, "a//b"),
            lambda: c.job_object_key(JOB, "job_receipt.json"),
        ):
            with self.assertRaises(c.ContractError):
                call()

    def test_window_bounds(self):
        self.assertEqual(
            c.window_bounds(HALF + 1, 2 * HALF + 1, 1800),
            (HALF, 3 * HALF, (HALF, 2 * HALF)),
        )
        self.assertEqual(c.window_bounds(HALF, 2 * HALF, 1800), (HALF, 2 * HALF, (HALF,)))
        for seconds in (0, 7, 1800.0):
            with self.subTest(seconds), self.assertRaises(c.ContractError):
                c.window_bounds(0, 1, seconds)
        with self.assertRaises(c.ContractError):
            c.window_bounds(5, 5, 1800)


class BundleReceiptTest(unittest.TestCase):
    def test_round_trip_is_byte_exact(self):
        body = canonical(receipt_document())
        receipt = c.parse_bundle_receipt(body)
        self.assertEqual(c.bundle_receipt_bytes(receipt), body)
        self.assertEqual(receipt.start_ns, 4 * HALF)
        self.assertEqual(len(receipt.windows), 2)
        self.assertEqual(receipt.normalizer_document(), normalizer())

    def test_rejections(self):
        windows = receipt_document()["windows"]
        cases = {
            "not canonical": json.dumps(receipt_document(), indent=1).encode(),
            "unknown field": canonical({**receipt_document(), "x": 1}),
            "bad version": canonical(receipt_document(replay_bundle_receipt_version=2)),
            "unaligned interval": canonical(
                receipt_document(
                    interval={"start_ns": str(4 * HALF + 1), "end_ns": str(6 * HALF)}
                )
            ),
            "gap": canonical(
                receipt_document(
                    interval={"start_ns": str(4 * HALF), "end_ns": str(7 * HALF)}
                )
            ),
            "reordered": canonical(receipt_document(windows=windows[::-1])),
            "empty windows": canonical(receipt_document(windows=[])),
            "repeated address": canonical(
                receipt_document(
                    windows=[
                        windows[0],
                        {**windows[1], "derivative_address": windows[0]["derivative_address"]},
                    ]
                )
            ),
            "uppercase hex": canonical(
                receipt_document(windows=[{**windows[0], "receipt_sha256": "A" * 64}, windows[1]])
            ),
            "bad normalizer": canonical(receipt_document(normalizer={"identity_version": 2})),
            "bad job": canonical(receipt_document(built_by_job="job-1")),
            "bad window seconds": canonical(receipt_document(canonical_window_seconds=7)),
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_bundle_receipt(body)


class RunnerConfigTest(unittest.TestCase):
    def test_shipped_config_parses(self):
        config = runner_config()
        self.assertEqual(config.canonical_window_seconds, 1800)
        self.assertEqual(set(config.authorities), {"kalshi", "limitless", "polymarket"})
        self.assertEqual(
            config.strategies["bundle_coverage"].factory, "replay.bundle_coverage:build"
        )
        self.assertIn("small", config.limits)

    def test_rejections(self):
        base = json.loads(CONFIG_PATH.read_bytes())
        small = base["limits"]["small"]

        def with_(**changes):
            return raw({**base, **changes})

        cases = {
            "unknown field": with_(extra=1),
            "credential url": with_(universe_base_url="http://u:p@host"),
            "relative url": with_(universe_base_url="event-universe:8080"),
            "window seconds": with_(canonical_window_seconds=7),
            "missing venue": with_(authorities={"kalshi": "kalshi", "limitless": "limitless"}),
            "bad lane": with_(authorities={**base["authorities"], "kalshi": "a/b"}),
            "bad factory": with_(
                strategies={"x": {"factory": "not a factory", "reader": "a:b", "config_schema": "bundle_coverage_v1"}}
            ),
            "unknown schema": with_(
                strategies={"x": {"factory": "a:b", "reader": "a:b", "config_schema": "nope"}}
            ),
            "no strategies": with_(strategies={}),
            "limits order": with_(limits={"small": {**small, "stall_seconds": 1000}}),
            "timeout range": with_(limits={"small": {**small, "command_timeout_ms": 1}}),
            "queue below entry": with_(limits={"small": {**small, "max_queue_bytes": 1}}),
            "float attempts": with_(limits={"small": {**small, "attempts": 3.0}}),
            "missing limit": with_(
                limits={"small": {k: v for k, v in small.items() if k != "stop_seconds"}}
            ),
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_runner_config(body)


if __name__ == "__main__":
    unittest.main()
