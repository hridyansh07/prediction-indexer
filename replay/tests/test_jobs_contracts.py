import hashlib
import json
import unittest
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

from archive.archiver.canonical import canonical_object_keys
from replay.jobs import contracts as c
from replay.tests.test_supervisor import normalizer

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / "configs" / "replay_runner.json"
JOB = "20260924T120000Z-0123456789abcdef"
ADDRESS = "0x" + "ab" * 20
SECOND = 1_000_000_000
HOUR = 3600 * SECOND
HALF = HOUR // 2


def raw(value):
    return json.dumps(value).encode()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def runner_config():
    return c.parse_runner_config(CONFIG_PATH.read_bytes())


def retired_config():
    config = runner_config()
    entry = config.strategies["bundle_coverage"]
    return replace(
        config,
        strategies=MappingProxyType(
            {"bundle_coverage": replace(entry, status=c.STRATEGY_RETIRED)}
        ),
    )


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


def producer(**changes):
    value = {
        "normalizer": c.freeze(normalizer()),
        "normalized_schema_version": 3,
        "materializer_version": 2,
        "materialization_policy_sha256": "e" * 64,
    }
    value.update(changes)
    return c.Producer(**value)


def windows(starts=(4 * HALF, 5 * HALF)):
    return tuple(
        c.BundleWindow(
            window_start_ns=start,
            window_end_ns=start + HALF,
            canonical_receipt_sha256=f"{i}" * 64,
            derivative_address=f"{i + 2}" * 64,
            receipt_sha256=f"{i + 4}" * 64,
        )
        for i, start in enumerate(starts)
    )


def bundle_receipt(**changes):
    value = {
        "bundle_id": "bundle_x",
        "start_ns": 4 * HALF,
        "end_ns": 6 * HALF,
        "canonical_window_seconds": 1800,
        "producer": producer(),
        "windows": windows(),
    }
    value.update(changes)
    return c.BundleReceipt(**value)


def history(*times):
    return [
        c.HistoryEntry(
            run_id=f"run{i}",
            generated_at_ns=t,
            manifest_key=f"targeter-v2/runs/run{i}/run_manifest.json",
            manifest_sha256=f"{i % 10}" * 64,
            report_key=f"targeter-v2/runs/run{i}/selection_report.json.zst",
            report_sha256=f"{(i + 5) % 10}" * 64,
        )
        for i, t in enumerate(times)
    ]


def resolved(**changes):
    bundle, job, occurrences = c.resolve_occurrences(history(100, 200), 300, None)
    value = {
        "job_id": JOB,
        "request_sha256": "a" * 64,
        "bundle_id": "bundle_x",
        "canonical_window_seconds": 1800,
        "bundle_interval": bundle,
        "job_interval": job,
        "occurrences": occurrences,
    }
    value.update(changes)
    return c.ResolvedJob(**value)


def job_result(**changes):
    value = {
        "job_id": JOB,
        "strategy": "bundle_coverage",
        "strategy_semantic_sha256": "1" * 64,
        "snapshot_sha256": "2" * 64,
        "bundle_receipt_sha256": "3" * 64,
        "supervisor_identity": "4" * 64,
        "attempt_id": "5" * 32,
        "attempts_started": 1,
    }
    value.update(changes)
    return c.JobResult(**value)


def objects(*relative):
    return tuple(
        c.JobObject(key=c.job_object_key(JOB, r), sha256=hashlib.sha256(r.encode()).hexdigest(), byte_length=len(r))
        for r in sorted(relative)
    )


def job_receipt(outcome="succeeded", code=None, depth=5, **changes):
    chain = ("resolved_sha256", "bundle_receipt_sha256", "snapshot_sha256", "supervisor_identity", "strategy_semantic_sha256")
    value = {
        "job_id": JOB,
        "request_sha256": "a" * 64,
        "submitted_by": ADDRESS,
        "image_revision": "0123abc",
        "final_outcome": outcome,
        "reason_code": code,
        "reason_detail": None,
        **{name: (f"{i + 1}" * 64 if i < depth else None) for i, name in enumerate(chain)},
        "objects": objects("request.json", "resolved.json", "run/SUCCESS.json")[: max(depth, 1)],
        "created_at_ns": 1,
        "finished_at_ns": 2,
    }
    value.update(changes)
    return c.JobReceipt(**value)


class RequestTest(unittest.TestCase):
    def test_minimal_request_parses(self):
        parsed = c.parse_request(raw(request()), runner_config())
        self.assertEqual(parsed.bundle_id, "bundle_0123456789abcdef01234567")
        self.assertIsNone(parsed.interval)
        self.assertIsNone(parsed.probe_markets)
        self.assertEqual(len(parsed.sha256), 64)

    def test_interval_and_probe_parse(self):
        parsed = c.parse_request(
            raw(request(interval={"start_ns": "10", "end_ns": "20"}, probe_markets=["kalshi:A", "polymarket:1"])),
            runner_config(),
        )
        self.assertEqual(parsed.interval, (10, 20))
        self.assertEqual(parsed.probe_markets, ("kalshi:A", "polymarket:1"))

    def test_hash_ignores_whitespace_and_key_order(self):
        config = runner_config()
        self.assertEqual(
            c.parse_request(canonical(request()), config).sha256,
            c.parse_request(json.dumps(request(), indent=2).encode(), config).sha256,
        )

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
            "factory as name": raw(request(strategy={"name": "os:system", "config": {}})),
            "runner-owned key": raw(request(strategy={"name": "bundle_coverage", "config": {"snapshot_directory": "/tmp"}})),
            "unknown config key": raw(request(strategy={"name": "bundle_coverage", "config": {"x": 1}})),
            "unknown preset": raw(request(limits="huge")),
            "oversize": b" " * (c.MAX_REQUEST_BYTES + 1),
            "not json": b"{",
            "nan": b'{"replay_request_version": NaN}',
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_request(body, config)

    def test_retired_strategy_refuses_new_requests_but_parses_accepted_ones(self):
        config = retired_config()
        with self.assertRaisesRegex(c.ContractError, "retired and accepts no new jobs"):
            c.parse_request(raw(request()), config)
        parsed = c.parse_request(raw(request()), config, accept_retired=True)
        self.assertEqual(parsed.strategy, "bundle_coverage")
        self.assertEqual(
            parsed.sha256, c.parse_request(raw(request()), runner_config()).sha256
        )

    def test_unknown_strategy_error_lists_only_active_strategies(self):
        config = runner_config()
        coverage = config.strategies["bundle_coverage"]
        config = replace(
            config,
            strategies=MappingProxyType(
                {
                    "bundle_coverage": coverage,
                    "old_coverage": replace(coverage, status=c.STRATEGY_RETIRED),
                }
            ),
        )
        with self.assertRaises(c.ContractError) as caught:
            c.parse_request(raw(request(strategy={"name": "x", "config": {}})), config)
        self.assertEqual(str(caught.exception), "strategy.name must be one of: bundle_coverage")

    def test_parsed_request_is_immutable(self):
        parsed = c.parse_request(raw(request()), runner_config())
        with self.assertRaises(TypeError):
            parsed.document["bundle_id"] = "other"


class IdentifierTest(unittest.TestCase):
    def test_job_id(self):
        value = c.job_id(1_785_412_800_123_456_789, "0123456789abcdef")
        self.assertEqual(value, "20260730T120000Z-0123456789abcdef")
        for bad in ("20260730T120000Z-0123", "x", "20260730T120000Z-0123456789ABCDEF"):
            with self.subTest(bad), self.assertRaises(c.ContractError):
                c.check_job_id(bad)

    def test_date_partition_matches_finalizer_vectors(self):
        self.assertEqual(c.date_partition(0), "1970-01-01")
        self.assertEqual(c.date_partition(1_785_412_800_000_000_000), "2026-07-30")
        self.assertEqual(c.date_partition(1_709_164_800_000_000_000), "2024-02-29")

    def test_canonical_keys_match_archiver(self):
        for start in (0, 1_785_412_800_000_000_000, 1_709_164_800_000_000_000 - HALF):
            with self.subTest(start):
                receipt = Path(f"canonical/date={c.date_partition(start)}/window={start}/receipt.json")
                self.assertEqual(c.canonical_window_keys(start), canonical_object_keys(receipt))

    def test_object_keys_reject_unnormalized_paths(self):
        """Review item 7."""
        self.assertEqual(c.job_object_key(JOB, "run/SUCCESS.json"), f"replay/jobs/{JOB}/run/SUCCESS.json")
        for bad in ("../x", "a//b", "a/./b", "a\\b", "a\x00b", "/abs", "", "job_receipt.json"):
            with self.subTest(repr(bad)), self.assertRaises(c.ContractError):
                c.job_object_key(JOB, bad)
        for call in (
            lambda: c.derivative_key("A" * 64, "receipt.json"),
            lambda: c.derivative_key("a" * 64, "other.json"),
            lambda: c.bundle_receipt_key("a/b"),
        ):
            with self.assertRaises(c.ContractError):
                call()

    def test_window_bounds(self):
        self.assertEqual(c.window_bounds(HALF + 1, 2 * HALF + 1, 1800), (HALF, 3 * HALF, (HALF, 2 * HALF)))
        for seconds in (0, 7, 1800.0):
            with self.subTest(seconds), self.assertRaises(c.ContractError):
                c.window_bounds(0, 1, seconds)

    def test_reason_detail_is_bounded_and_redacted(self):
        self.assertEqual(c.reason_detail("redis://user:secret@host:6379/0 down"), "redis://***@host:6379/0 down")
        self.assertEqual(len(c.reason_detail("x" * 5000)), c.MAX_REASON_DETAIL)
        self.assertEqual(c.reason_detail("a\nb\x00c"), "a b c")
        long = c.reason_detail("y" * 5000)
        self.assertEqual(c.reason_detail(long), long)


class ProducerTest(unittest.TestCase):
    """Review item 4: every producer field independently changes identity."""

    def test_each_field_changes_identity(self):
        base = producer()
        other_normalizer = normalizer()
        other_normalizer["venues"][0]["parser_version"] = 2
        variants = {
            "normalizer": producer(normalizer=c.freeze(other_normalizer)),
            "schema": producer(normalized_schema_version=4),
            "materializer": producer(materializer_version=3),
            "policy": producer(materialization_policy_sha256="f" * 64),
        }
        for name, variant in variants.items():
            with self.subTest(name):
                self.assertNotEqual(variant, base)
                self.assertNotEqual(variant.document(), base.document())
        self.assertEqual(producer(), base)

    def test_describe_output_round_trip(self):
        body = canonical(producer().document())
        self.assertEqual(c.parse_producer(body), producer())
        for bad in (
            json.dumps(producer().document(), indent=1).encode(),
            canonical({**producer().document(), "producer_identity_version": 2}),
            canonical({**producer().document(), "normalizer": {"identity_version": 2}}),
        ):
            with self.assertRaises(c.ContractError):
                c.parse_producer(bad)


class BundleReceiptTest(unittest.TestCase):
    def test_round_trip_is_byte_exact(self):
        body = c.bundle_receipt_bytes(bundle_receipt())
        self.assertEqual(c.parse_bundle_receipt(body), bundle_receipt())
        self.assertEqual(c.bundle_receipt_bytes(c.parse_bundle_receipt(body)), body)

    def test_independent_builds_publish_identical_bytes(self):
        self.assertEqual(c.bundle_receipt_bytes(bundle_receipt()), c.bundle_receipt_bytes(bundle_receipt()))
        self.assertNotIn(b"built_by_job", c.bundle_receipt_bytes(bundle_receipt()))

    def test_bundle_has_one_deterministic_receipt_key(self):
        self.assertEqual(
            c.bundle_receipt_key("bundle_x"),
            "replay/bundles/bundle_x/bundle_receipt.json",
        )

    def test_direct_construction_of_invalid_receipt_fails(self):
        """Review item 7."""
        good = windows()
        cases = {
            "uppercase hash": lambda: replace(good[0], receipt_sha256="A" * 64),
            "empty window": lambda: replace(good[0], window_end_ns=good[0].window_start_ns),
            "unaligned": lambda: bundle_receipt(start_ns=4 * HALF + 1),
            "gap": lambda: bundle_receipt(end_ns=7 * HALF),
            "reordered": lambda: bundle_receipt(windows=good[::-1]),
            "list windows": lambda: bundle_receipt(windows=list(good)),
            "repeated address": lambda: bundle_receipt(windows=(good[0], replace(good[1], derivative_address=good[0].derivative_address))),
            "bad producer": lambda: producer(materializer_version=0),
        }
        for name, build in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                build()

    def test_serialization_revalidates_bypassed_fields(self):
        receipt = bundle_receipt()
        object.__setattr__(receipt.windows[0], "receipt_sha256", "not-a-hash")
        with self.assertRaises(c.ContractError):
            c.bundle_receipt_bytes(receipt)

    def test_single_field_tampering(self):
        document = json.loads(c.bundle_receipt_bytes(bundle_receipt()))
        tampered = [
            {**document, "extra": 1},
            {**document, "replay_bundle_receipt_version": 2},
            {**document, "bundle_id": "../x"},
            {**document, "canonical_window_seconds": 7},
            {**document, "producer": {**document["producer"], "materializer_version": "2"}},
            {**document, "windows": [{**document["windows"][0], "receipt_sha256": "A" * 64}, document["windows"][1]]},
            {**document, "interval": {**document["interval"], "end_ns": str(7 * HALF)}},
        ]
        for index, value in enumerate(tampered):
            with self.subTest(index), self.assertRaises(c.ContractError):
                c.parse_bundle_receipt(canonical(value))
        with self.assertRaises(c.ContractError):
            c.parse_bundle_receipt(json.dumps(document, indent=1).encode())


class HistoryTest(unittest.TestCase):
    """Review item 8."""

    def test_occurrences_partition_the_bundle(self):
        bundle, job, occurrences = c.resolve_occurrences(history(200, 100), 300, None)
        self.assertEqual(bundle, (100, 300))
        self.assertEqual(job, bundle)
        self.assertEqual([(o.run_id, o.start_ns, o.end_ns) for o in occurrences], [("run1", 100, 200), ("run0", 200, 300)])

    def test_job_interval_clips_occurrences(self):
        _, job, occurrences = c.resolve_occurrences(history(100, 200, 250), 300, (150, 260))
        self.assertEqual(job, (150, 260))
        self.assertEqual([(o.start_ns, o.end_ns) for o in occurrences], [(150, 200), (200, 250), (250, 260)])

    def test_equal_timestamps_fail_deterministically(self):
        for order in ((100, 100), (100, 200, 200)):
            with self.subTest(order), self.assertRaises(c.ContractError) as caught:
                c.resolve_occurrences(history(*order), 300, None)
            self.assertEqual(caught.exception.code, "bundle_history_invalid")
        messages = set()
        for entries in (history(100, 100), list(reversed(history(100, 100)))):
            with self.assertRaises(c.ContractError) as caught:
                c.resolve_occurrences(entries, 300, None)
            messages.add(str(caught.exception))
        self.assertEqual(len(messages), 1)

    def test_invalid_history(self):
        for args, code in (
            ((history(), 300, None), "bundle_history_invalid"),
            ((history(100, 200), 200, None), "bundle_history_invalid"),
            ((history(100, 200), 300, (50, 150)), "interval_out_of_range"),
            ((history(100, 200), 300, (150, 350)), "interval_out_of_range"),
        ):
            with self.subTest(args[1:]), self.assertRaises(c.ContractError) as caught:
                c.resolve_occurrences(*args)
            self.assertEqual(caught.exception.code, code)


class StageReceiptTest(unittest.TestCase):
    """Review items 5, 9, 10."""

    def test_resolved_job_round_trip_and_tampering(self):
        body = c.resolved_job_bytes(resolved())
        self.assertEqual(c.parse_resolved_job(body), resolved())
        document = json.loads(body)
        self.assertEqual(document["window_interval"], {"start_ns": "0", "end_ns": str(HALF)})
        tampered = [
            {**document, "extra": 1},
            {**document, "window_interval": {"start_ns": "0", "end_ns": str(HOUR)}},
            {**document, "job_interval": {"start_ns": "100", "end_ns": "299"}},
            {**document, "occurrences": document["occurrences"][:1]},
            {**document, "occurrences": [{**document["occurrences"][0], "source": {**document["occurrences"][0]["source"], "report_key": "a//b"}}, document["occurrences"][1]]},
            {**document, "request_sha256": "A" * 64},
        ]
        for index, value in enumerate(tampered):
            with self.subTest(index), self.assertRaises(c.ContractError):
                c.parse_resolved_job(canonical(value))

    def test_job_result_round_trip_and_tampering(self):
        body = c.job_result_bytes(job_result())
        self.assertEqual(c.parse_job_result(body), job_result())
        document = json.loads(body)
        for name, value in (("attempt_id", "x"), ("attempts_started", 0), ("strategy", "a/b"), ("snapshot_sha256", None), ("extra", 1)):
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_job_result(canonical({**document, name: value}))

    def test_job_receipt_round_trip_and_tampering(self):
        body = c.job_receipt_bytes(job_receipt())
        self.assertEqual(c.parse_job_receipt(body), job_receipt())
        document = json.loads(body)
        tampered = {
            "extra field": {**document, "extra": 1},
            "outcome": {**document, "final_outcome": "running"},
            "code on success": {**document, "reason_code": "tool_failure"},
            "missing identity": {**document, "snapshot_sha256": None},
            "foreign object": {**document, "objects": [{"key": "replay/jobs/other/x", "sha256": "0" * 64, "byte_length": 1}]},
            "unsorted objects": {**document, "objects": document["objects"][::-1]},
            "receipt listed": {**document, "objects": [{"key": c.job_receipt_key(JOB), "sha256": "0" * 64, "byte_length": 1}]},
            "numeric time": {**document, "created_at_ns": 1},
            "finished first": {**document, "created_at_ns": "3"},
            "bad revision": {**document, "image_revision": ""},
        }
        for name, value in tampered.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_job_receipt(canonical(value))

    def test_partial_receipts_use_null_identities(self):
        cases = {
            "cancelled": job_receipt("cancelled", "cancelled", depth=0),
            "not_ready": job_receipt("not_ready", "canonical_not_archived", depth=1),
            "stale": job_receipt("stale_bundle_cache", "stale_bundle_cache", depth=1),
            "failed early": job_receipt("failed", "universe_unavailable", depth=0),
            "failed run": job_receipt("failed", "supervisor_failed", depth=3),
            "exhausted": job_receipt("exhausted", "supervisor_exhausted", depth=4),
        }
        for name, receipt in cases.items():
            with self.subTest(name):
                body = c.job_receipt_bytes(receipt)
                self.assertEqual(c.parse_job_receipt(body), receipt)

    def test_partial_receipt_rules(self):
        with self.assertRaises(c.ContractError):
            job_receipt(depth=3)  # succeeded needs every identity
        with self.assertRaises(c.ContractError):
            job_receipt("failed", "tool_failure", depth=2, resolved_sha256=None)  # gap in chain
        with self.assertRaises(c.ContractError):
            job_receipt("not_ready", "tool_failure", depth=1)  # code/outcome mismatch
        with self.assertRaises(c.ContractError):
            job_receipt("failed", "archive_unavailable", depth=1)  # block code is not an outcome

    def test_receipt_size_bound(self):
        many = tuple(
            c.JobObject(key=c.job_object_key(JOB, f"run/{i:05}"), sha256="0" * 64, byte_length=1)
            for i in range(c.MAX_JOB_OBJECTS + 1)
        )
        with self.assertRaises(c.ContractError):
            job_receipt(objects=many)


class RunnerConfigTest(unittest.TestCase):
    def test_shipped_config_parses(self):
        """Review item 12."""
        config = runner_config()
        self.assertEqual(config.canonical_window_seconds, 1800)
        self.assertEqual(set(config.authorities), {"kalshi", "limitless", "polymarket"})
        self.assertEqual(config.orchestration, c.Orchestration(20, 86400, 60))
        self.assertIn("small", config.limits)
        self.assertEqual(config.limits["small"]["publish_batch_entries"], 1024)
        # Dense corpus bundles publish single entries above 1 MiB (a deep book snapshot).
        self.assertEqual(config.limits["small"]["max_entry_bytes"], 8 * 1024 * 1024)
        self.assertEqual(
            config.strategies["bundle_coverage"],
            c.StrategyEntry(
                factory="replay.strategies.bundle_coverage:build",
                reader="replay.strategies.bundle_coverage:read_completed",
                config_schema="bundle_coverage_v1",
                label="Bundle coverage",
                description="Evaluates historical coverage for the selected bundle.",
                status=c.STRATEGY_ACTIVE,
            ),
        )

    def test_rejections(self):
        base = json.loads(CONFIG_PATH.read_bytes())
        small = base["limits"]["small"]
        orchestration = base["orchestration"]
        coverage = base["strategies"]["bundle_coverage"]

        def with_(**changes):
            return raw({**base, **changes})

        def entry(**changes):
            return with_(strategies={"x": {**coverage, **changes}})

        def without(field):
            return with_(strategies={"x": {k: v for k, v in coverage.items() if k != field}})

        cases = {
            "unknown field": with_(extra=1),
            "missing orchestration": raw({k: v for k, v in base.items() if k != "orchestration"}),
            "zero attempts": with_(orchestration={**orchestration, "max_stage_attempts": 0}),
            "float backoff": with_(orchestration={**orchestration, "retry_backoff_seconds": 1.5}),
            "run beyond job deadline": with_(orchestration={**orchestration, "max_job_seconds": 900}),
            "credential url": with_(universe_base_url="http://u:p@host"),
            "relative url": with_(universe_base_url="event-universe:8080"),
            "window seconds": with_(canonical_window_seconds=7),
            "missing venue": with_(authorities={"kalshi": "kalshi", "limitless": "limitless"}),
            "version 1": with_(replay_runner_config_version=1),
            "version 2 without batch size": with_(replay_runner_config_version=2),
            "missing batch size": with_(
                limits={"small": {k: v for k, v in small.items() if k != "publish_batch_entries"}}
            ),
            "zero batch size": with_(limits={"small": {**small, "publish_batch_entries": 0}}),
            "batch size above 1024": with_(
                limits={"small": {**small, "publish_batch_entries": 1025}}
            ),
            "boolean batch size": with_(limits={"small": {**small, "publish_batch_entries": True}}),
            "float batch size": with_(limits={"small": {**small, "publish_batch_entries": 100.0}}),
            "no strategies": with_(strategies={}),
            "bad factory": entry(factory="nope"),
            "unknown schema": entry(config_schema="nope"),
            "missing label": without("label"),
            "missing description": without("description"),
            "missing status": without("status"),
            "unknown entry field": entry(module="x"),
            "empty label": entry(label=""),
            "long label": entry(label="x" * (c.MAX_STRATEGY_LABEL + 1)),
            "control label": entry(label="a\nb"),
            "numeric label": entry(label=1),
            "long description": entry(description="x" * (c.MAX_STRATEGY_DESCRIPTION + 1)),
            "unknown status": entry(status="removed"),
            "status case": entry(status="Active"),
            "limits order": with_(limits={"small": {**small, "stall_seconds": 1000}}),
            "float attempts": with_(limits={"small": {**small, "attempts": 3.0}}),
        }
        for name, body in cases.items():
            with self.subTest(name), self.assertRaises(c.ContractError):
                c.parse_runner_config(body)


if __name__ == "__main__":
    unittest.main()
