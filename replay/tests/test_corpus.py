"""Corpus runner: offline, with fakes for Universe, GCS, the materializer and Docker."""

import hashlib
import io
import json
import tempfile
import unittest
from copy import deepcopy
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
from replay.corpus.pipeline import (OUTCOME_ATTEMPTS, OUTCOME_RETRY_SECONDS, Pipeline, StageError,
                                    preflight_groups, retired_bundles, run_corpus)
from replay.outcome_model import UNAVAILABLE

UNIVERSE_OUTCOMES = {"provider": "universe", "document": {}}


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

    def test_source_is_provenance_only_warns_on_commit_change_and_is_best_effort(self):
        with tempfile.TemporaryDirectory() as tmp:
            ledger = Ledger(Path(tmp) / "l.sqlite3")
            ledger.set_meta("image_id:k", "sha256:img")
            first = {"commit": "abc", "dirty": False}
            second = {"commit": "def", "dirty": True}
            err = io.StringIO()
            with patch("sys.stderr", err):
                ledger.record_source("k", first)
                ledger.record_source("k", first)
                self.assertEqual(err.getvalue(), "")
                self.assertEqual(json.loads(ledger.get_meta("source:k")), first)
                ledger.record_source("k", second)
            self.assertIn("abc -> def", err.getvalue())
            self.assertEqual(json.loads(ledger.get_meta("source:k")), second)
            self.assertEqual(ledger.get_meta("image_id:k"), "sha256:img")
            self.assertEqual(ledger.summary("k"), {})
            with patch.object(ledger, "set_meta", side_effect=OSError("disk full")), patch("sys.stderr", err):
                ledger.record_source("k", second)  # diagnostics must not fail the run
            self.assertIn("could not record source", err.getvalue())


class ResultsTests(unittest.TestCase):
    def test_commit_copies_hashes_writes_receipt_last_and_reader_rejects_tampering(self):
        with tempfile.TemporaryDirectory() as tmp:
            bench, dest = Path(tmp) / "bench", Path(tmp) / "results" / "b1"
            (bench / "run/x/output").mkdir(parents=True); (bench / "in/derivatives").mkdir(parents=True)
            (bench / "result.json").write_text('{"status":"SUCCESS"}')
            (bench / "run/x/output/rows.ndjson").write_text("a\n")
            (bench / "run/x/publisher.stderr.txt").write_bytes(b"diagnostic\xff\n")
            (bench / "in/derivatives/big").write_text("mounted input")
            sha = results.commit(bench, dest, corpus_key="k", bundle_id="b1", status="SUCCESS", bench_exit=0,
                                 game_state={"state": "ok", "reason": None})
            receipt = results.verify(dest)
            self.assertEqual(hashlib.sha256((dest / results.RECEIPT).read_bytes()).hexdigest(), sha)
            self.assertEqual([f["path"] for f in receipt["files"]],
                             ["result.json", "run/x/output/rows.ndjson", "run/x/publisher.stderr.txt"])
            self.assertEqual((dest / "run/x/publisher.stderr.txt").read_bytes(), b"diagnostic\xff\n")
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

    def test_commit_keeps_named_inputs_and_refuses_one_that_shadows_an_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            bench, context = Path(tmp) / "bench", Path(tmp) / "context.json"
            bench.mkdir(); (bench / "result.json").write_text("{}"); context.write_text("{}")
            receipt = results.commit(bench, Path(tmp) / "r1", corpus_key="k", bundle_id="b1", status="SUCCESS",
                                     bench_exit=0, game_state=None, inputs=[("context/context.json", context)])
            self.assertTrue(receipt)
            self.assertEqual([f["path"] for f in results.verify(Path(tmp) / "r1")["files"]],
                             ["context/context.json", "result.json"])
            with self.assertRaisesRegex(FileExistsError, "collides"):
                results.commit(bench, Path(tmp) / "r2", corpus_key="k", bundle_id="b1", status="SUCCESS",
                               bench_exit=0, game_state=None, inputs=[("result.json", context)])
            self.assertFalse((Path(tmp) / "r2").exists())

    def test_deep_prerequisites_and_readonly_original_image_runner(self):
        from replay.corpus import deep
        from replay.bench.docker import DockerError
        from replay.tests.test_bundle_coverage import Harness

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bench = root / "bench"
            bench.mkdir()
            h = Harness(bench)
            h.window(); h.finish()
            image, fees, address = "sha256:" + "a" * 64, "b" * 64, "d" * 64
            source = root / "cache/derivatives" / address
            source.mkdir(parents=True)
            receipt = {}
            for kind in ("manifest", "events", "rejects", "sources"):
                name = "manifest.json" if kind == "manifest" else kind + ".ndjson.zst"
                payload = b"{}" if kind == "manifest" else kind.encode()
                (source / name).write_bytes(payload)
                identity = {"sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload)}
                receipt[kind] = {"file": name, **identity} if kind == "manifest" else {"file": name, "stored": identity}
            raw = json.dumps(receipt).encode()
            (source / "receipt.json").write_bytes(raw)
            groups = [{"name": "a", "reader": "replay.strategies.bundle_coverage:read_completed"},
                      {"name": "b", "reader": "replay.strategies.market_profile:read_completed"}]
            (bench / "image.id").write_text(image)
            (bench / "game_state").mkdir()
            (bench / "game_state/game_state.json").write_bytes(b"{}")
            (bench / "result.json").write_text(json.dumps({"image": {"id": image},
                "context": {"snapshot_sha256": h.sha}, "fee_catalog_identity": fees,
                "game_state_sha256": hashlib.sha256(b"{}").hexdigest()}))
            (bench / "spec.resolved.json").write_text(json.dumps({"groups": groups}))
            (bench / "run").mkdir()
            (bench / "run/run.json").write_text(json.dumps({"strategies": {"a": {}, "b": {}},
                "transport": {"inputs": [{"derivative_address": address,
                    "directory": "/bench/in/private-derivatives/" + address,
                    "receipt_sha256": hashlib.sha256(raw).hexdigest()}]}, "limits": {"run_seconds": 100}}))
            dest = root / "results/b1"
            results.commit(bench, dest, corpus_key="k", bundle_id="b1", status="SUCCESS", bench_exit=0,
                           game_state=None)
            spec = {"fees": {"template_catalog": str(root / "fees")},
                    "derivative_cache": {"root": str(root / "cache")}}
            reports = [{"group": "a", "status": "PASS"}, {"group": "b", "status": "PASS"}]
            calls = []

            class Docker:
                def run(self, args, **kwargs):
                    if args[0] != "image":
                        raise AssertionError("only an injected runner may launch")
                    return image

            def runner(args, *, timeout):
                calls.append(args)
                self.assertEqual(timeout, 160)
                mounts = [args[i + 1] for i, arg in enumerate(args) if arg == "--mount"]
                self.assertTrue(all(m.endswith(",readonly") for m in mounts))
                self.assertTrue(any("dst=/bench/run," in m for m in mounts))
                self.assertTrue(any("dst=/bench/context," in m for m in mounts))
                self.assertTrue(any("dst=/bench/game_state.json," in m for m in mounts))
                self.assertTrue(any("dst=/bench/fees/" + fees + "," in m for m in mounts))
                self.assertTrue(any("dst=/bench/in/private-derivatives/" + address + "," in m for m in mounts))
                self.assertFalse(any("dst=/bench," in m for m in mounts))
                self.assertEqual(args[args.index("--pull") + 1], "never")
                self.assertEqual(args[args.index("--network") + 1], "none")
                self.assertEqual(json.loads(args[-1]), groups)
                self.assertEqual(args[args.index("-c") - 1], image)
                compile(args[args.index("-c") + 1], "deep-entry", "exec")
                return json.dumps(reports)

            catalog_loader = lambda _: SimpleNamespace(identity=fees)
            self.assertEqual(deep.audit(dest, spec, docker=Docker(), runner=runner,
                                        catalog_loader=catalog_loader), reports)
            (bench / "image.id").unlink()   # run_bench omits this when reusing an image
            reused = root / "results/b2"
            results.commit(bench, reused, corpus_key="k", bundle_id="b2", status="SUCCESS", bench_exit=0,
                           game_state=None)
            self.assertEqual(deep.audit(reused, spec, docker=Docker(), runner=runner,
                                        catalog_loader=catalog_loader), reports)
            # Missing environment cases do not launch anything or modify evidence.
            bad_image = SimpleNamespace(run=lambda *a, **kw: (_ for _ in ()).throw(DockerError("absent")))
            with self.assertRaisesRegex(deep.MissingPrerequisite, "deep: missing prerequisite image " + image):
                deep.audit(dest, spec, docker=bad_image, runner=runner, catalog_loader=catalog_loader)
            with self.assertRaisesRegex(deep.MissingPrerequisite, "deep: missing prerequisite fees " + fees):
                deep.audit(dest, spec, docker=Docker(), runner=runner,
                           catalog_loader=lambda _: SimpleNamespace(identity="c" * 64))
            (source / "manifest.json").unlink()
            with self.assertRaisesRegex(deep.MissingPrerequisite, "deep: missing prerequisite derivative " + address):
                deep.audit(dest, spec, docker=Docker(), runner=runner, catalog_loader=catalog_loader)
            self.assertEqual(len(calls), 2)
            results.verify(dest)

    def test_deep_calls_configured_readers_unchanged_and_continues_after_a_group_failure(self):
        from replay.corpus.deep import _script
        seen = []

        def importer(reference):
            def reader(directory, group):
                seen.append((reference, directory, group))
                if group == "a":
                    raise ValueError("bad semantics")
                return {"receipt": {}, "manifest": {}, "summary": {}}
            return reader

        groups = [{"name": "a", "reader": "original:a"}, {"name": "b", "reader": "original:b"}]
        out = io.StringIO()
        # Execute the exact source sent into recorded images, not another reader.
        with patch("replay.strategies.load_entrypoint", side_effect=importer), \
             patch("sys.argv", ["-c", json.dumps(groups)]), patch("sys.stdout", out):
            exec(compile(_script(), "deep-entry", "exec"), {})
        self.assertEqual(seen, [("original:a", Path("/bench/run"), "a"),
                                ("original:b", Path("/bench/run"), "b")])
        reports = json.loads(out.getvalue())
        self.assertEqual([r["status"] for r in reports], ["FAIL", "PASS"])
        self.assertEqual(reports[0]["error"], "ValueError: bad semantics")

    def test_deep_subset_sampling_and_each_bundle_report_are_isolated(self):
        from replay.corpus.__main__ import verify_deep
        from replay.corpus.deep import MissingPrerequisite

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bench = root / "bench"
            bench.mkdir(); (bench / "result.json").write_text("{}")
            for name in ("a", "b", "c", "d", "e"):
                results.commit(bench, root / "results" / name, corpus_key="k", bundle_id=name,
                               status="SUCCESS", bench_exit=0, game_state=None)
            (root / "results/uncommitted").mkdir()
            seen = []

            def audit(directory, spec):
                seen.append(directory.name)
                if directory.name in ("a", "b", "c"):
                    raise MissingPrerequisite({"a": "image", "b": "derivative", "c": "fees"}[directory.name], "pin")
                if directory.name == "d":
                    raise ValueError("invalid result")
                return [{"group": "g", "status": "PASS"}]

            out = io.StringIO()
            with patch("sys.stdout", out):
                self.assertEqual(verify_deep(root / "results", {}, auditor=audit), 1)
            self.assertEqual(seen, ["a", "b", "c", "d", "e"])
            for name, kind in (("a", "image"), ("b", "derivative"), ("c", "fees")):
                self.assertIn(f"{name} deep: missing prerequisite {kind} pin", out.getvalue())
            self.assertIn("e deep: g PASS", out.getvalue())
            self.assertIn("deep verified 1, failed 1, missing prerequisites 3", out.getvalue())
            seen.clear()
            with patch("sys.stdout", io.StringIO()):
                self.assertEqual(verify_deep(root / "results", {}, bundles=["b", "a", "b"], auditor=audit), 2)
            self.assertEqual(seen, ["a", "b"])
            seen.clear()
            with patch("sys.stdout", io.StringIO()):
                verify_deep(root / "results", {}, sample=2, auditor=audit)
            # random.Random(0) over a sorted five-element population chooses d/e.
            self.assertEqual(seen, ["d", "e"])
            seen.clear()
            with patch("sys.stdout", io.StringIO()):
                verify_deep(root / "results", {}, sample=2, seed=0, auditor=audit)
            self.assertEqual(seen, ["d", "e"])
            seen.clear()
            with patch("sys.stdout", io.StringIO()):
                verify_deep(root / "results", {}, sample=2, seed=1, auditor=audit)
            self.assertEqual(seen, ["a", "b"])

    def test_deep_cli_routes_subsets_without_writes_and_plain_verify_stays_rehash_only(self):
        from replay.corpus import __main__ as entry

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("runner.json", "materializer"):
                (root / name).write_text("{}")
            (root / "canonical").mkdir()
            cache = root / "absent-cache"
            spec = spec_document(root, derivative_cache={"root": str(cache), "max_bytes": 1000})
            path = root / "spec.json"
            path.write_text(json.dumps(spec))
            bench = root / "bench"
            bench.mkdir(); (bench / "result.json").write_text("{}")
            key = "a" * 64
            for name in ("a", "b", "c"):
                results.commit(bench, root / "results" / key[:16] / name, corpus_key=key, bundle_id=name,
                               status="SUCCESS", bench_exit=0, game_state=None)
            seen = []

            def auditor(directory, spec):
                seen.append(directory.name)
                return [{"group": "g", "status": "PASS"}]

            with patch.object(entry, "_context", return_value=(key, b"{}", {"commit": "abc", "dirty": False})), \
                 patch("replay.corpus.deep.audit", side_effect=auditor), patch("sys.stdout", io.StringIO()):
                self.assertEqual(entry.main(["verify", str(path), "--deep", "--bundle", "b", "--bundle", "c"]), 0)
                self.assertEqual(seen, ["b", "c"])
                self.assertFalse(cache.exists())
                self.assertFalse(Path(spec["ledger"]).exists())
                seen.clear()
                self.assertEqual(entry.main(["verify", str(path), "--deep", "--sample", "1", "--seed", "1"]), 0)
                self.assertEqual(seen, ["a"])
                seen.clear()
                self.assertEqual(entry.main(["verify", str(path)]), 0)
                self.assertEqual(seen, [])


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

    def test_corpus_key_binds_declared_versions_groups_game_and_materializer_only(self):
        import importlib
        from replay import semantics
        from replay.tests.test_strategy_packages import CASES

        spec = spec_document(Path("/not-read"))
        spec["groups"] = [dict(spec["groups"][0], name=name, factory="replay.strategies." + name + ":build")
                          for name, *_ in CASES]
        original = deepcopy(spec)
        args = {"materializer_describe": b"v6"}
        base = corpus_key(spec, **args)
        identity = {"corpus_key_version": 2, "strategies": {
            g["name"]: [g["factory"], importlib.import_module(g["factory"].split(":")[0]).STRATEGY_VERSION]
            for g in spec["groups"]}, "replay_semantics": semantics.REPLAY_SEMANTICS_VERSION,
            "materializer": hashlib.sha256(b"v6").hexdigest(), "groups": spec["groups"],
            "game_state": spec["game_state"]}
        self.assertEqual(base, hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
        for field, value in (("universe_base_url", "http://other"), ("limits", {"attempt_seconds": 1}),
                             ("fees", {"template_catalog": "/absent/catalog", "sources": "/absent/sources"}),
                             ("bundles", {"only": ["b"], "exclude": ["c"]}), ("work_root", "/other/work"),
                             ("results_root", "/other/results"), ("runner_config", "/absent/runner"),
                             ("image", {"build_context": "/other", "dockerfile": "other"}),
                             ("redis", {"image": "other"}),
                             ("derivative_cache", {"root": "/cache", "max_bytes": 100})):
            with self.subTest(invariant=field):
                self.assertEqual(base, corpus_key({**spec, field: value}, **args))
        for group in spec["groups"]:
            module = importlib.import_module(group["factory"].split(":")[0])
            with self.subTest(version=group["name"]), patch.object(module, "STRATEGY_VERSION", module.STRATEGY_VERSION + 1):
                self.assertNotEqual(base, corpus_key(spec, **args))
        with patch.object(semantics, "REPLAY_SEMANTICS_VERSION", semantics.REPLAY_SEMANTICS_VERSION + 1):
            self.assertNotEqual(base, corpus_key(spec, **args))
        changed = deepcopy(spec)
        changed["groups"][0]["config"]["policy"] = {}
        self.assertNotEqual(base, corpus_key(changed, **args))
        self.assertNotEqual(base, corpus_key(spec, materializer_describe=b"v7"))
        self.assertNotEqual(base, corpus_key({**spec, "game_state": {"prepare": False}}, **args))
        self.assertEqual(spec, original)

    def test_corpus_key_resolves_historical_factory_and_names_both_references_on_missing_version(self):
        from replay import semantics
        from replay.strategies import same_venue_complement as package

        spec = spec_document(Path("/not-read"))
        spec["groups"][0]["factory"] = "replay.same_venue_complement:build"
        identity = {"corpus_key_version": 2, "strategies": {"complement": [
            "replay.strategies.same_venue_complement:build", package.STRATEGY_VERSION]},
            "replay_semantics": semantics.REPLAY_SEMANTICS_VERSION,
            "materializer": hashlib.sha256(b"v6").hexdigest(), "groups": spec["groups"],
            "game_state": spec["game_state"]}
        self.assertEqual(corpus_key(spec, materializer_describe=b"v6"), hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest())
        with patch.object(package, "STRATEGY_VERSION", None):
            with self.assertRaises(ValueError) as caught:
                corpus_key(spec, materializer_describe=b"v6")
        for text in ("STRATEGY_VERSION", "replay.same_venue_complement:build",
                     "replay.strategies.same_venue_complement:build"):
            self.assertIn(text, str(caught.exception))
        spec["groups"][0]["factory"] = "replay.tests.test_corpus:spec_document"
        with self.assertRaisesRegex(ValueError, "STRATEGY_VERSION"):
            corpus_key(spec, materializer_describe=b"v6")

    def test_context_source_url_runner_and_fee_bytes_are_not_key_material_and_dirty_gate_stays(self):
        from replay.corpus.__main__ import _context

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runner = root / "runner.json"
            runner.write_text('{"universe_base_url": "http://old"}')
            sources = root / "sources"
            sources.mkdir()
            fee = sources / "fee.html"
            fee.write_text("old fee")
            spec = spec_document(root)
            with patch("replay.bench.docker.git_source", return_value=("abc", False)), \
                 patch("replay.corpus.__main__.subprocess.run", return_value=SimpleNamespace(stdout=b"v6")):
                base, _, source = _context(spec, False)
            self.assertEqual(source, {"commit": "abc", "dirty": False})
            spec["universe_base_url"] = "http://new"
            runner.write_text('{"universe_base_url": "http://ignored", "timeout": 99}')
            fee.write_text("new fee")
            with patch("replay.bench.docker.git_source", return_value=("def", True)), \
                 patch("replay.corpus.__main__.subprocess.run", return_value=SimpleNamespace(stdout=b"v6")):
                with self.assertRaisesRegex(SystemExit, "image build context has uncommitted changes"):
                    _context(spec, False)
                key, raw, source = _context(spec, True)
            self.assertEqual(key, base)
            self.assertEqual(json.loads(raw), {"universe_base_url": "http://new", "timeout": 99})
            self.assertEqual(source, {"commit": "def", "dirty": True})

    def test_derivative_cache_is_optional_closed_writable_and_disjoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("runner.json", "materializer"):
                (root / name).write_text("{}")
            (root / "canonical").mkdir()
            path = root / "s.json"
            cache = {"root": str(root / "cache"), "max_bytes": 1000}
            path.write_text(json.dumps(spec_document(root, derivative_cache=cache)))
            self.assertEqual(specs.load(path)["derivative_cache"], cache)
            bad = [None, {**cache, "extra": 1}, {"root": "relative", "max_bytes": 1000},
                   *({**cache, "max_bytes": n} for n in (True, 0, -1, 1.5))]
            for name in ("canonical", "work", "results"):
                bad.extend(({**cache, "root": str(root / name)}, {**cache, "root": str(root / name / "cache")}))
            bad.append({**cache, "root": str(root)})
            alias = root / "alias"
            alias.symlink_to(root / "canonical", target_is_directory=True)
            bad.append({**cache, "root": str(alias / "cache")})
            (root / "file").write_text("not a directory")
            bad.append({**cache, "root": str(root / "file")})
            for value in bad:
                with self.subTest(cache=value):
                    path.write_text(json.dumps(spec_document(root, derivative_cache=value)))
                    with self.assertRaisesRegex(ValueError, "derivative_cache"):
                        specs.load(path)


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
        # This orchestration fixture mocks preparation, fees and game evidence;
        # it cannot declare real SDK timers. Retention itself has SDK tests.
        retention = patch("replay.economic_sdk.retention.for_groups", return_value={
            "retain_boundaries_ns": ["1"], "retain_shift_offsets_ns": []})
        retention.start()
        self.addCleanup(retention.stop)

    def harness(self, tmp, *, bench_status="SUCCESS", fail_stage=None, outcomes=None, cache=None):
        root = Path(tmp)
        spec = spec_document(root)
        if cache is not None:
            spec["derivative_cache"] = cache
        calls = {"prepare": 0}
        outcomes = list(outcomes or [UNIVERSE_OUTCOMES])

        def ensure(bundle_id, interval, *, store, work_root, derivatives_root, materializer, window_seconds):
            if fail_stage == "materialize":
                raise RuntimeError("materializer crashed")
            calls["store"] = store
            (derivatives_root / "addr").mkdir()
            return SimpleNamespace(receipt="receipt", pins=("pin",))

        def prepare(root_dir, request, resolved, receipt, pins, config):
            calls["prepare"] += 1
            calls["pins"] = pins
            (root_dir / "context").mkdir()
            (root_dir / "context/receipt.json").write_text('{"snapshot_sha256": "' + "a" * 64 + '"}')
            (root_dir / "context/context.json").write_text('{"context_version": 2}')
            recorded = outcomes.pop(0) if len(outcomes) > 1 else outcomes[0]
            return {"plans": [], "config": {"start_ns": "1"}, "outcomes": recorded}

        def game(context, out, *, store):
            out.mkdir(); (out / "game_state.json").write_text('{"state":"unavailable","reason":"no_fetch"}')

        def supervisor(job_id, request, config, receipt, pins, snapshot, context, **kw):
            host = Path(spec["work_root"]) / "b1" / "derivatives" / "addr"
            return {"transport": {"inputs": [{"directory": str(host)}]}, "publisher": "x", "python": "y"}

        def bench(spec_path, out, *, keep_image, label):
            calls["bench_spec"] = json.loads(Path(spec_path).read_bytes())
            calls["run_config"] = json.loads(Path(calls["bench_spec"]["base_run_config"]).read_bytes())
            out.mkdir(); (out / "image.id").write_text("sha256:img\n")
            (out / "result.json").write_text(json.dumps({"status": bench_status}))
            return 0 if bench_status == "SUCCESS" else 2

        history = SimpleNamespace(resolve=lambda job_id, request, config: SimpleNamespace(window_interval=(0, 1800)))
        config = SimpleNamespace(canonical_window_seconds=1800)
        pipeline = Pipeline(spec, corpus_key="f" * 64, config=config, canonical=LocalObjectStore(root / "canonical"),
                            history=history, game_store="gcs", image_id=None, ensure_bundle=ensure,
                            prepare_stage=prepare, supervisor_config=supervisor, prepare_game_state=game,
                            run_bench=bench, sleep=lambda seconds: calls.setdefault("slept", []).append(seconds))
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
            # The prepared context and game state are deleted with the work directory, so
            # the result keeps them: they label the books and events downstream.
            kept = {f["path"] for f in committed["files"]}
            self.assertLessEqual({"context/context.json", "context/receipt.json", "game_state/game_state.json"}, kept)
            result = pipeline.results_dir("b1")
            self.assertEqual((result / "context/context.json").read_text(), '{"context_version": 2}')
            self.assertEqual((result / "game_state/game_state.json").read_text(),
                             '{"state":"unavailable","reason":"no_fetch"}')
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

    def test_unavailable_outcomes_fail_the_bundle_instead_of_committing_it(self):
        # A Universe timeout records UNAVAILABLE outcomes; cross, multi and the covers then
        # admit nothing, so committing the bundle as done would be a silent false negative.
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, calls, root = self.harness(tmp, outcomes=[UNAVAILABLE])
            ledger = Ledger(root / "l.sqlite3")
            with patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                run_corpus(pipeline, ledger, ["b1"], log=lambda m: None)
            row = ledger.state(pipeline.corpus_key, "b1")
            self.assertEqual((row["state"], row["stage"]), ("failed", "outcomes"))
            self.assertIn("universe_outcomes_unavailable", row["detail"])
            self.assertEqual(calls["prepare"], OUTCOME_ATTEMPTS)
            self.assertEqual(calls["slept"], [5, 15])
            self.assertNotIn("bench_spec", calls)
            self.assertFalse(pipeline.results_dir("b1").exists())

    def test_transient_unavailable_outcomes_are_retried_with_a_fresh_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, calls, root = self.harness(tmp, outcomes=[UNAVAILABLE, UNIVERSE_OUTCOMES])
            with patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                state, _, _, _ = pipeline.run("b1")
            self.assertEqual(state, "done")
            self.assertEqual(calls["prepare"], 2)   # the first context was removed, not reused
            self.assertEqual(calls["slept"], [OUTCOME_RETRY_SECONDS[0]])

    def test_mixed_cache_mounts_ledger_diagnostics_and_lease_cleanup(self):
        from archive.archiver.canonical import CanonicalArchiver
        from replay.jobs.bundle import ensure_bundle
        from replay.tests.test_bundle_cache import MATERIALIZER
        from tests.archive_fixtures import WINDOW_SECONDS, write_canonical_receipt
        if not MATERIALIZER.is_file():
            self.skipTest("build the materialize_range example first")
        width = WINDOW_SECONDS * 1_000_000_000
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            pipeline, calls, _ = self.harness(tmp, cache={"root": str(root / "cache"), "max_bytes": 100_000_000})
            for start in (0, width):
                receipt = write_canonical_receipt(root / "source", window_start_ns=start, evidence_lines=0)
                self.assertEqual(CanonicalArchiver(root / "source", pipeline.canonical).archive_window(receipt).status,
                                 "archived")
            cache = pipeline.derivative_cache
            with cache.lease() as lease:
                seed = ensure_bundle("seed", (0, width), store=OverlayStore(pipeline.canonical, VerifyingSink()),
                                     work_root=root / "seed", derivatives_root=root / "seed-private",
                                     materializer=MATERIALIZER, window_seconds=WINDOW_SECONDS, derivative_cache=lease)
            cache.max_bytes = cache.total_bytes() + 1000
            pipeline.ensure_bundle = ensure_bundle
            pipeline.spec["materializer"] = str(MATERIALIZER)
            pipeline.history.resolve = lambda *args: SimpleNamespace(window_interval=(0, 2 * width))
            pipeline.supervisor_config = lambda *args, **kw: {
                "transport": {"inputs": [{"directory": str(p.directory)} for p in args[4]]}}
            old_bench = pipeline.run_bench

            def bench(*args, **kwargs):
                # No Docker/bench is run: prove the cache lease still spans this boundary.
                leased = [json.loads(p.read_bytes()) for p in cache.leases.glob("*.json")]
                self.assertEqual(len(leased), 1)
                self.assertIn(seed.pins[0].address, leased[0]["addresses"])
                for pin in calls["pins"]:
                    self.assertTrue((pin.directory / "receipt.json").is_file())
                return old_bench(*args, **kwargs)

            pipeline.run_bench = bench
            ledger = Ledger(root / "l.sqlite3")
            with patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                self.assertEqual(run_corpus(pipeline, ledger, ["b1"], log=lambda m: None), (1, 0))
            self.assertEqual(ledger.state(pipeline.corpus_key, "b1")["state"], "done")
            detail = json.loads(ledger.state(pipeline.corpus_key, "b1")["detail"])["derivative_cache"]
            self.assertEqual(detail, {"hits": 1, "misses": 1,
                             "cache_bypass": [{"window_start_ns": width, "reason": "live_leases"}]})
            self.assertEqual(calls["bench_spec"]["mounts"], [
                {"host": str(cache.derivatives), "container": "/bench/in/derivatives"},
                {"host": str(root / "work/b1/derivatives"), "container": "/bench/in/private-derivatives"}])
            directories = [p["directory"] for p in calls["run_config"]["transport"]["inputs"]]
            self.assertEqual(directories, ["/bench/in/derivatives/" + p.address if index == 0
                                           else "/bench/in/private-derivatives/" + p.address
                                           for index, p in enumerate(calls["pins"])])
            self.assertFalse(list(cache.leases.iterdir()))
            self.assertFalse((root / "work/b1").exists())
            self.assertTrue(seed.pins[0].directory.exists())
            # Failure also releases its lease and retains diagnostics in the ledger.
            pipeline.prepare_stage = lambda *args: (_ for _ in ()).throw(RuntimeError("prepare failed"))
            with patch("replay.jobs.contracts.parse_request", return_value="request"):
                run_corpus(pipeline, ledger, ["b2"], log=lambda m: None)
            self.assertEqual(ledger.state(pipeline.corpus_key, "b2")["stage"], "prepare")
            self.assertIn("derivative_cache", json.loads(ledger.state(pipeline.corpus_key, "b2")["detail"]))
            self.assertFalse(list(cache.leases.iterdir()))

    def test_required_game_state_skip_is_its_own_final_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, _, root = self.harness(tmp, bench_status="GAME_STATE_UNAVAILABLE")
            ledger = Ledger(root / "l.sqlite3")
            with patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                run_corpus(pipeline, ledger, ["b1"], log=lambda m: None)
            self.assertEqual(ledger.state(pipeline.corpus_key, "b1")["state"], "game_state_unavailable")

    def test_interrupt_stops_the_corpus_and_leaves_the_bundle_running(self):
        with tempfile.TemporaryDirectory() as tmp:
            pipeline, calls, root = self.harness(tmp)
            started = []

            def interrupted(spec_path, out, *, keep_image, label):
                started.append(spec_path)
                out.mkdir(); (out / "result.json").write_text(json.dumps({"status": "FAILED"}))
                raise KeyboardInterrupt

            pipeline.run_bench = interrupted
            ledger = Ledger(root / "l.sqlite3")
            with patch("replay.jobs.contracts.parse_request", return_value="request"), \
                 patch("replay.corpus.pipeline.fee_inputs", return_value=(root / "cat", {}, {})):
                with self.assertRaises(KeyboardInterrupt) as caught:
                    run_corpus(pipeline, ledger, ["b1", "b2"], log=lambda m: None)
            self.assertEqual(caught.exception.bundle_id, "b1")
            row = ledger.state(pipeline.corpus_key, "b1")
            self.assertEqual((row["state"], row["attempts"], row["bench_exit"], row["receipt_sha256"]),
                             ("running", 1, None, None))
            self.assertFalse(pipeline.results_dir("b1").exists())
            self.assertIsNone(ledger.state(pipeline.corpus_key, "b2"))
            self.assertEqual(len(started), 1)
            self.assertFalse((root / "work/b1").exists())   # scratch is rebuilt from nothing on the redo

    def test_main_exits_130_naming_the_bundle_left_running(self):
        from replay.corpus import __main__ as entry
        interrupt = KeyboardInterrupt()
        interrupt.bundle_id = "b1"
        err = io.StringIO()
        with patch.object(entry, "_main", side_effect=interrupt), patch("sys.stderr", err):
            self.assertEqual(entry.main(["run", "spec.json"]), 130)
        self.assertEqual(len(err.getvalue().strip().splitlines()), 1)
        self.assertIn("b1", err.getvalue())
        self.assertIn("running", err.getvalue())

    def test_stage_error_names_the_stage(self):
        error = StageError("bench", ValueError("x"))
        self.assertEqual((error.stage, str(error)), ("bench", "bench: ValueError: x"))


if __name__ == "__main__":
    unittest.main()
