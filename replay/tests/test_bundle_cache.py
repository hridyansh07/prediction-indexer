import io
import shutil
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from archive.archiver.canonical import CanonicalArchiver
from archive.storage import LocalObjectStore, ObjectStoreError
from encoder import StoredIdentity
from replay.jobs import bundle
from replay.jobs.bundle import BundleFailure, BundleReady, NotReady, StaleCache, ensure_bundle
from tests.archive_fixtures import BASE_NS, WINDOW_SECONDS, write_canonical_receipt


ROOT = Path(__file__).resolve().parents[2]
MATERIALIZER = ROOT / "engine/target/debug/examples/materialize_range"


class RecordingStore:
    def __init__(self, inner, fail_at=None):
        self.inner = inner
        self.fail_at = fail_at
        self.puts = []
        self.opens = []

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def open_verified(self, expected):
        self.opens.append(expected.key)
        return self.inner.open_verified(expected)

    def put_immutable(self, key, reader, expected_identity, **kwargs):
        self.puts.append(key)
        if self.fail_at == len(self.puts):
            raise ObjectStoreError("injected upload interruption")
        return self.inner.put_immutable(key, reader, expected_identity, **kwargs)


class BundleCacheTest(unittest.TestCase):
    def setUp(self):
        if not MATERIALIZER.is_file():
            self.skipTest("build the materialize_range example first")
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = LocalObjectStore(self.root / "objects")
        self.work = self.root / "work"
        self.derivatives = self.root / "derivatives"

    def tearDown(self):
        self.temporary.cleanup()

    def archive_window(self, **changes):
        canonical = self.root / "source"
        receipt = write_canonical_receipt(canonical, evidence_lines=0, **changes)
        outcome = CanonicalArchiver(canonical, self.store).archive_window(receipt)
        self.assertEqual(outcome.status, "archived")

    def ensure(self, materializer=MATERIALIZER):
        return ensure_bundle(
            "bundle-1",
            (BASE_NS, BASE_NS + WINDOW_SECONDS * 1_000_000_000),
            store=self.store,
            work_root=self.work,
            derivatives_root=self.derivatives,
            materializer=materializer,
            window_seconds=WINDOW_SECONDS,
        )

    def helper(self, body):
        path = self.root / f"helper-{len(tuple(self.root.glob('helper-*')))}"
        path.write_text(f"#!/usr/bin/env python3\n{body}\n", encoding="utf-8")
        path.chmod(0o755)
        return path

    def test_subprocess_stdout_stderr_and_timeout_are_hard_bounded(self):
        cases = (
            ("import sys; sys.stdout.write('x' * 17)", "MAX_SUBPROCESS_STDOUT", 16, "byte budget"),
            ("import sys; sys.stderr.write('x' * 17)", "MAX_SUBPROCESS_STDERR", 16, "byte budget"),
            ("import time; time.sleep(2)", "MAX_SUBPROCESS_SECONDS", 0.01, "timed out"),
        )
        for body, setting, limit, detail in cases:
            with self.subTest(setting=setting), mock.patch.object(bundle, setting, limit):
                with self.assertRaises(BundleFailure) as caught:
                    bundle._run_tool(self.helper(body), (), b"", self.work)
                self.assertEqual(caught.exception.code, "tool_failure")
                self.assertIn(detail, caught.exception.detail)

    def test_missing_receipt_is_not_ready_without_publication(self):
        result = self.ensure()
        self.assertEqual(result, NotReady("canonical_not_archived", f"canonical receipt for window {BASE_NS} is absent"))
        self.assertFalse(any(key.startswith("replay/") for key in self.store.list_keys("replay/")))

    def test_cold_then_warm_returns_byte_equal_receipt_without_materialization(self):
        self.archive_window()
        first = self.ensure()
        self.assertIsInstance(first, BundleReady)
        self.assertEqual(len(first.pins), 1)
        generation_keys = tuple(self.store.list_keys("replay/bundles/bundle-1/"))
        self.assertEqual(len(generation_keys), 1)

        shutil.rmtree(self.derivatives)
        log = self.root / "arguments.log"
        wrapper = self.root / "materializer"
        wrapper.write_text(
            f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> {log}\nexec {MATERIALIZER} \"$@\"\n",
            encoding="utf-8",
        )
        wrapper.chmod(0o755)
        second = self.ensure(wrapper)
        self.assertIsInstance(second, BundleReady)
        self.assertEqual(second.receipt_bytes, first.receipt_bytes)
        arguments = log.read_text(encoding="utf-8").splitlines()
        self.assertIn("--describe", arguments)
        self.assertIn("--inspect-pin", arguments)
        self.assertNotIn("", arguments)

    def test_cold_publication_order_and_retry_at_each_commit_boundary(self):
        for fail_at in range(1, 7):
            with self.subTest(fail_at=fail_at):
                root = self.root / str(fail_at)
                inner = LocalObjectStore(root / "objects")
                source = root / "source"
                receipt = write_canonical_receipt(source, evidence_lines=0)
                self.assertEqual(CanonicalArchiver(source, inner).archive_window(receipt).status, "archived")
                store = RecordingStore(inner, fail_at=fail_at)
                with self.assertRaises(BundleFailure) as caught:
                    ensure_bundle(
                        "bundle-1", (BASE_NS, BASE_NS + WINDOW_SECONDS * 1_000_000_000),
                        store=store, work_root=root / "work", derivatives_root=root / "derivatives",
                        materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS,
                    )
                self.assertEqual(caught.exception.code, "archive_unavailable")
                store.fail_at = None
                ready = ensure_bundle(
                    "bundle-1", (BASE_NS, BASE_NS + WINDOW_SECONDS * 1_000_000_000),
                    store=store, work_root=root / "work", derivatives_root=root / "derivatives",
                    materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS,
                )
                self.assertIsInstance(ready, BundleReady)
                replay_puts = [key for key in store.puts if key.startswith("replay/")]
                self.assertTrue(replay_puts[-1].endswith("/bundle_receipt.json"))
                receipt_index = max(i for i, key in enumerate(replay_puts) if key.endswith("/receipt.json"))
                self.assertTrue(all(not key.endswith("/receipt.json") for key in replay_puts[receipt_index + 1 : -1]))

    def test_independent_concurrent_builders_publish_one_semantic_generation(self):
        self.archive_window(
            completeness="incomplete",
            certified=False,
            expected_lanes=["kalshi"],
            missing_lanes=[{"lane": "kalshi", "reason": "lane_missing", "detail": None}],
        )

        def build(index):
            return ensure_bundle(
                "bundle-1", (BASE_NS, BASE_NS + WINDOW_SECONDS * 1_000_000_000),
                store=self.store, work_root=self.root / f"work-{index}",
                derivatives_root=self.root / f"derivatives-{index}",
                materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(pool.map(build, (1, 2)))
        self.assertTrue(all(isinstance(result, BundleReady) for result in results))
        self.assertEqual(results[0].receipt_bytes, results[1].receipt_bytes)
        self.assertEqual(len(tuple(self.store.list_keys("replay/bundles/bundle-1/"))), 1)

    def test_all_receipts_are_checked_before_any_download(self):
        self.archive_window()
        store = RecordingStore(self.store)
        result = ensure_bundle(
            "bundle-1", (BASE_NS, BASE_NS + 2 * WINDOW_SECONDS * 1_000_000_000),
            store=store, work_root=self.work, derivatives_root=self.derivatives,
            materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS,
        )
        self.assertIsInstance(result, NotReady)
        self.assertEqual(store.opens, [])

    def test_symlink_root_and_immutable_conflict_fail_closed(self):
        self.archive_window()
        outside = self.root / "outside"
        outside.mkdir()
        linked = self.root / "linked"
        linked.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(BundleFailure) as caught:
            ensure_bundle(
                "bundle-1", (BASE_NS, BASE_NS + WINDOW_SECONDS * 1_000_000_000),
                store=self.store, work_root=linked, derivatives_root=self.derivatives,
                materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS,
            )
        self.assertEqual(caught.exception.code, "integrity_failure")

        class ConflictStore(RecordingStore):
            def put_immutable(self, key, reader, expected_identity, **kwargs):
                if key.startswith("replay/derivatives/") and not self.puts:
                    self.puts.append(key)
                    bad = b"conflict"
                    self.inner.put_immutable(
                        key, io.BytesIO(bad), StoredIdentity("fa9e1d22205ad852b0dc9509ec4e31644e88742c4dfce93c08f011fee1cd8a1a", len(bad)),
                        **kwargs,
                    )
                return super().put_immutable(key, reader, expected_identity, **kwargs)

        conflict_root = self.root / "conflict"
        conflict = ConflictStore(self.store)
        with self.assertRaises(BundleFailure) as caught:
            ensure_bundle(
                "bundle-conflict", (BASE_NS, BASE_NS + WINDOW_SECONDS * 1_000_000_000),
                store=conflict, work_root=conflict_root / "work", derivatives_root=conflict_root / "derivatives",
                materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS,
            )
        self.assertEqual(caught.exception.code, "integrity_failure")

    def test_full_producer_mismatch_is_stale(self):
        self.archive_window()
        first = self.ensure()
        self.assertIsInstance(first, BundleReady)
        wrapper = self.root / "changed-materializer"
        wrapper.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, subprocess, sys\n"
            f"real={str(MATERIALIZER)!r}\n"
            "if sys.argv[1:] == ['--describe']:\n"
            " p=json.loads(subprocess.check_output([real,'--describe']))\n"
            " p['materializer_version'] += 1\n"
            " print(json.dumps(p,sort_keys=True,separators=(',',':')))\n"
            "else:\n"
            " os.execv(real,[real,*sys.argv[1:]])\n",
            encoding="utf-8",
        )
        wrapper.chmod(0o755)
        stale = self.ensure(wrapper)
        self.assertIsInstance(stale, StaleCache)
        self.assertEqual(stale.cached_producer, first.receipt.producer)
        self.assertNotEqual(stale.current_producer, first.receipt.producer)


if __name__ == "__main__":
    unittest.main()
