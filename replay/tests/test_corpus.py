"""Corpus runner: offline, with fakes for Universe, GCS, the materializer and Docker."""

import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from archive.storage.base import ObjectStoreError
from archive.storage.local import LocalObjectStore
from encoder import StoredIdentity
from replay.corpus import results, spec as specs
from replay.corpus.__main__ import corpus_key
from replay.corpus.ledger import Ledger
from replay.corpus.overlay import OverlayStore, VerifyingSink
from replay.corpus.pipeline import Pipeline, StageError, preflight_groups, retired_bundles, run_corpus


def put(store, key, data):
    store.put_immutable(key, io.BytesIO(data), StoredIdentity(hashlib.sha256(data).hexdigest(), len(data)),
                        content_type="application/json")


def spec_document(root, **changes):
    value = {
        "version": 1, "universe_base_url": "http://127.0.0.1:1", "runner_config": str(root / "runner.json"),
        "canonical_store": str(root / "canonical"), "work_root": str(root / "work"),
        "results_root": str(root / "results"), "ledger": str(root / "ledger.sqlite3"),
        "materializer": str(root / "materializer"),
        "image": {"build_context": str(root), "dockerfile": "docker/replay-runner.Dockerfile"},
        "redis": {"image": "redis:8.2-alpine", "maxmemory": "256mb"},
        "limits": {"attempt_seconds": 100, "run_seconds": 200, "stall_seconds": 50},
        "fees": {"template_catalog": str(root / "catalog"), "sources": str(root / "sources"),
                 "kalshi_member_class": "DIRECT", "limitless_buy_bps": 300, "limitless_sell_bps": 150},
        "game_state": {"prepare": True}, "bundles": {"only": None, "exclude": None},
        "groups": [{"name": "complement", "factory": "replay.strategies.same_venue_complement:build",
                    "reader": "replay.strategies.same_venue_complement:read_completed",
                    "config": {"snapshot_directory": "{context}", "fees": "@corpus:fees",
                               "policy": {"game": {"input": {"path": "{game_state}"}}}},
                    "checks": [], "compare": {"rows": "rows", "key": ["venue"]}}],
    }
    value.update(changes)
    return value


class OverlayTests(unittest.TestCase):
    def test_reads_base_writes_layer_and_never_rewrites_a_base_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            base, layer = LocalObjectStore(Path(tmp) / "base"), LocalObjectStore(Path(tmp) / "layer")
            put(base, "canonical/w/receipt.json", b"{}")
            overlay = OverlayStore(base, layer)
            self.assertEqual(overlay.head("canonical/w/receipt.json").byte_length, 2)
            put(overlay, "replay/bundles/b/bundle_receipt.json", b"[1]")
            self.assertIsNotNone(layer.head("replay/bundles/b/bundle_receipt.json"))
            self.assertIsNone(base.head("replay/bundles/b/bundle_receipt.json"))
            with self.assertRaisesRegex(ObjectStoreError, "base key"):
                put(overlay, "canonical/w/receipt.json", b"{}")
            with overlay.open("replay/bundles/b/bundle_receipt.json") as reader:
                self.assertEqual(reader.read(), b"[1]")
            self.assertEqual(list(overlay.list_keys("canonical/")), ["canonical/w/receipt.json"])
            self.assertEqual(list(overlay.list_keys("replay/")), ["replay/bundles/b/bundle_receipt.json"])


class SinkAndPreflightTests(unittest.TestCase):
    def test_sink_verifies_writes_and_keeps_nothing(self):
        sink = VerifyingSink()
        data = b"derivative bytes"
        meta = sink.put_immutable("replay/derivatives/x", io.BytesIO(data),
                                  StoredIdentity(hashlib.sha256(data).hexdigest(), len(data)))
        self.assertEqual(meta.byte_length, len(data))
        self.assertIsNone(sink.head("replay/derivatives/x"))
        with self.assertRaisesRegex(ObjectStoreError, "expected identity"):
            sink.put_immutable("k", io.BytesIO(b"other"), StoredIdentity(hashlib.sha256(data).hexdigest(), len(data)))
        with self.assertRaises(ObjectStoreError):
            sink.open("replay/derivatives/x")

    def test_preflight_excludes_refusing_groups_and_keeps_the_rest(self):
        from replay.economic_sdk import factory

        class Fits:
            def __init__(self, config):
                self.config = config

        class TooBig(Fits):
            pass

        module = SimpleNamespace(fits=factory(Fits), too_big=factory(TooBig), plain=lambda context: None)
        seen = []

        def fake_preflight(strategy):
            seen.append(strategy.config)
            if isinstance(strategy, TooBig):
                raise ValueError("entity metadata preflight: entities.json requires 16009633 bytes; limit 8388608")

        groups = [{"name": "a", "factory": "m:fits", "config": {"path": "{context}"}},
                  {"name": "b", "factory": "m:too_big", "config": {}},
                  {"name": "c", "factory": "m:plain", "config": {}}]
        with patch("replay.economic_sdk.entity_tables.preflight", side_effect=fake_preflight), \
             patch("importlib.import_module", return_value=module):
            runnable, excluded = preflight_groups(groups, lambda c: {k: v.replace("{context}", "/ctx") for k, v in c.items()})
        self.assertEqual([g["name"] for g in runnable], ["a", "c"])   # non-SDK groups pass through
        self.assertEqual(excluded[0]["group"], "b")
        self.assertIn("16009633", excluded[0]["reason"])
        self.assertEqual(seen[0], {"path": "/ctx"})   # tokens resolved to host paths


class LedgerTests(unittest.TestCase):
    def test_claim_finish_reclaim_and_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(Path(tmp) / "l.sqlite3")
            ledger.start("k", "b1")
            ledger.start("k", "b1")   # a crashed running row is reclaimed
            self.assertEqual(ledger.state("k", "b1")["attempts"], 2)
            ledger.finish("k", "b1", "done", seconds=3.0, bench_exit=0)
            with self.assertRaisesRegex(RuntimeError, "not running"):
                ledger.finish("k", "b1", "failed", seconds=1.0)
            with self.assertRaisesRegex(ValueError, "final"):
                ledger.finish("k", "b1", "running", seconds=1.0)
            self.assertEqual(ledger.summary("k"), {"done": {"bundles": 1, "seconds": 3.0}})
            self.assertIsNone(ledger.state("other", "b1"))
            ledger.set_meta("image_id:k", "sha256:1"); ledger.set_meta("image_id:k", "sha256:2")
            self.assertEqual(ledger.get_meta("image_id:k"), "sha256:2")


class ResultsTests(unittest.TestCase):
    def test_commit_copies_hashes_writes_receipt_last_and_reader_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            bench, dest = Path(tmp) / "bench", Path(tmp) / "results" / "b1"
            (bench / "run/x/output").mkdir(parents=True); (bench / "in/derivatives").mkdir(parents=True)
            (bench / "result.json").write_text('{"status":"SUCCESS"}')
            (bench / "run/x/output/rows.ndjson").write_text("a\n")
            (bench / "in/derivatives/big").write_text("mounted input")
            sha = results.commit(bench, dest, corpus_key="k", bundle_id="b1", status="SUCCESS", bench_exit=0,
                                 game_state={"state": "ok", "reason": None})
            receipt = results.verify(dest)
            self.assertEqual(hashlib.sha256((dest / results.RECEIPT).read_bytes()).hexdigest(), sha)
            self.assertEqual([f["path"] for f in receipt["files"]], ["result.json", "run/x/output/rows.ndjson"])
            with self.assertRaises(FileExistsError):
                results.commit(bench, dest, corpus_key="k", bundle_id="b1", status="SUCCESS", bench_exit=0,
                               game_state=None)
            (dest / "run/x/output/rows.ndjson").write_text("b\n")
            with self.assertRaisesRegex(ValueError, "mismatch"):
                results.verify(dest)
            (dest / "run/x/output/rows.ndjson").write_text("a\n")
            (dest / "extra.txt").write_text("x")
            with self.assertRaisesRegex(ValueError, "unlisted"):
                results.verify(dest)
            (dest / "extra.txt").unlink()
            (dest / "._result.json").write_text("macOS AppleDouble")   # ignored, as on exFAT
            results.verify(dest)


class SpecTests(unittest.TestCase):
    def test_closed_schema_and_corpus_tokens(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("runner.json", "materializer"):
                (root / name).write_text("{}")
            (root / "canonical").mkdir()
            (root / "s.json").write_text(json.dumps(spec_document(root)))
            spec = specs.load(root / "s.json")
            (root / "bad.json").write_text(json.dumps({**spec_document(root), "extra": 1}))
            with self.assertRaisesRegex(ValueError, "corpus spec"):
                specs.load(root / "bad.json")
            replaced = specs.substitute(spec["groups"][0]["config"], {"@corpus:fees": {"x": 1}, "@corpus:valuation": {}})
            self.assertEqual(replaced["fees"], {"x": 1})
            self.assertEqual(replaced["snapshot_directory"], "{context}")   # bench tokens pass through
            with self.assertRaisesRegex(ValueError, "unknown corpus token"):
                specs.substitute({"a": "@corpus:nope"}, {})

    def test_corpus_key_binds_groups_source_and_tools(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / "sources").mkdir(); (root / "sources/a.html").write_text("fee page")
            spec = spec_document(root)
            args = dict(source={"commit": "abc", "dirty": False}, materializer_describe=b"v6", runner_config=b"{}")
            base = corpus_key(spec, **args)
            self.assertEqual(base, corpus_key(spec, **args))
            changed = spec_document(root); changed["groups"][0]["config"]["policy"] = {}
            self.assertNotEqual(base, corpus_key(changed, **args))
            self.assertNotEqual(base, corpus_key(spec, **{**args, "materializer_describe": b"v7"}))
            self.assertNotEqual(base, corpus_key(spec, **{**args, "source": {"commit": "abd", "dirty": False}}))
            self.assertEqual(base, corpus_key(spec_document(root, work_root="/elsewhere"), **args))


class FakeOpener:
    def __init__(self, pages):
        self.pages, self.urls = pages, []

    def open(self, url, timeout):
        self.urls.append(url)
        return io.BytesIO(json.dumps(self.pages.pop(0)).encode())


class PipelineTests(unittest.TestCase):
    def test_retired_bundles_pages_filters_and_orders_by_capture(self):
        row = lambda b, start, life: {"bundle_id": b, "capture_start_at": start, "lifecycle": life}
        opener = FakeOpener([{"bundles": [row("b3", "2026-09-02", "retired"), row("b9", "2026-09-01", "active")],
                              "next_cursor": "c/1"},
                             {"bundles": [row("b1", "2026-09-01", "retired")], "next_cursor": None}])
        self.assertEqual(retired_bundles("http://u", opener=opener), ["b1", "b3"])
        self.assertTrue(opener.urls[1].endswith("&cursor=c/1"))

    def setUp(self):
        patcher = patch("replay.corpus.pipeline.preflight_groups", side_effect=lambda groups, resolve: (groups, []))
        patcher.start()
        self.addCleanup(patcher.stop)

    def harness(self, tmp, *, bench_status="SUCCESS", fail_stage=None):
        root = Path(tmp)
        spec = spec_document(root)
        calls = {}

        def ensure(bundle_id, interval, *, store, work_root, derivatives_root, materializer, window_seconds):
            if fail_stage == "materialize":
                raise RuntimeError("materializer crashed")
            calls["store"] = store
            (derivatives_root / "addr").mkdir()
            return SimpleNamespace(receipt="receipt", pins=("pin",))

        def prepare(root_dir, request, resolved, receipt, pins, config):
            (root_dir / "context").mkdir()
            (root_dir / "context/receipt.json").write_text('{"snapshot_sha256": "' + "a" * 64 + '"}')
            return {"plans": [], "config": {"start_ns": "1"}}

        def game(context, out, *, store):
            out.mkdir(); (out / "game_state.json").write_text('{"state":"unavailable","reason":"no_fetch"}')

        def supervisor(job_id, request, config, receipt, pins, snapshot, context, **kw):
            host = Path(spec["work_root"]) / "b1" / "derivatives" / "addr"
            return {"transport": {"inputs": [{"directory": str(host)}]}, "publisher": "x", "python": "y"}

        def bench(spec_path, out, *, keep_image, label):
            calls["bench_spec"] = json.loads(Path(spec_path).read_bytes())
            out.mkdir(); (out / "image.id").write_text("sha256:img\n")
            (out / "result.json").write_text(json.dumps({"status": bench_status}))
            return 0 if bench_status == "SUCCESS" else 2

        history = SimpleNamespace(resolve=lambda job_id, request, config: SimpleNamespace(window_interval=(0, 1800)))
        config = SimpleNamespace(canonical_window_seconds=1800)
        pipeline = Pipeline(spec, corpus_key="f" * 64, config=config, canonical=LocalObjectStore(root / "canonical"),
                            history=history, game_store="gcs", image_id=None, ensure_bundle=ensure,
                            prepare_stage=prepare, supervisor_config=supervisor, prepare_game_state=game,
                            run_bench=bench)
        return pipeline, calls, root

    def test_bundle_runs_end_to_end_with_pinned_inputs_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, calls, root = self.harness(tmp)
            with patch("replay.corpus.pipeline.parse_request", create=True), \
                 patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {"fees": 1}, {"val": 1})):
                state, code, receipt, excluded = pipeline.run("b1")
            self.assertEqual((state, code, excluded), ("done", 0, []))
            self.assertIsInstance(calls["store"], OverlayStore)
            self.assertIsInstance(calls["store"].layer, VerifyingSink)
            spec = calls["bench_spec"]
            self.assertIsNone(spec["image"]["reuse_id"])
            self.assertEqual(pipeline.image_id, "sha256:img")   # later bundles reuse the built image
            self.assertEqual(spec["groups"][0]["config"]["fees"], {"fees": 1})
            self.assertEqual(spec["groups"][0]["config"]["policy"]["game"]["input"]["path"], "{game_state}")
            self.assertTrue(spec["game_state_path"].endswith("game_state/game_state.json"))
            self.assertEqual(spec["mounts"], [{"host": str(root / "work/b1/derivatives"),
                                               "container": "/bench/in/derivatives"}])
            committed = results.verify(pipeline.results_dir("b1"))
            self.assertEqual(committed["game_state"], {"state": "unavailable", "reason": "no_fetch"})
            self.assertFalse((root / "work/b1").exists())

    def test_corpus_skips_finished_records_failures_and_retries_on_request(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, _, root = self.harness(tmp, fail_stage="materialize")
            ledger = Ledger(root / "l.sqlite3")
            with patch("replay.jobs.contracts.parse_request", return_value="request"):
                processed, skipped = run_corpus(pipeline, ledger, ["b1"], log=lambda m: None)
                self.assertEqual((processed, skipped), (1, 0))
                row = ledger.state(pipeline.corpus_key, "b1")
                self.assertEqual((row["state"], row["stage"]), ("failed", "materialize"))
                self.assertIn("materializer crashed", row["detail"])
                self.assertFalse((root / "work/b1").exists())
                self.assertEqual(run_corpus(pipeline, ledger, ["b1"], log=lambda m: None), (0, 1))
                ok, _, _ = self.harness(tmp)
                with patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                    self.assertEqual(run_corpus(ok, ledger, ["b1"], retry_failed=True, log=lambda m: None), (1, 0))
                self.assertEqual(ledger.state(ok.corpus_key, "b1")["state"], "done")
                self.assertEqual(ledger.state(ok.corpus_key, "b1")["attempts"], 2)
                self.assertEqual(run_corpus(ok, ledger, ["b1"], retry_failed=True, log=lambda m: None), (0, 1))

    def test_required_game_state_skip_is_its_own_final_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, _, root = self.harness(tmp, bench_status="GAME_STATE_UNAVAILABLE")
            ledger = Ledger(root / "l.sqlite3")
            with patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                run_corpus(pipeline, ledger, ["b1"], log=lambda m: None)
            self.assertEqual(ledger.state(pipeline.corpus_key, "b1")["state"], "game_state_unavailable")

    def test_stage_error_names_the_stage(self):
        error = StageError("bench", ValueError("x"))
        self.assertEqual((error.stage, str(error)), ("bench", "bench: ValueError: x"))


if __name__ == "__main__":
    unittest.main()
