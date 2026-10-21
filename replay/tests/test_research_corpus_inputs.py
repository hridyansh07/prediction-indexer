"""Approved corpus-input regressions, using offline runtime output and receipts."""

import hashlib
import io
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr
from pathlib import Path

import pyarrow.parquet as pq

from encoder import encode_stream
from replay.research import build as builder
from replay.research.inputs import discover
from replay.research.jsonio import ResearchError, read_json
from replay.tests.research_fixtures import ATTEMPT, corpus_result, write_json
from replay.tests.test_research_build import CONFIG, CONFIG_PATH, OK
from replay.tests import test_strategy_execution_realism as execution


class CorpusInputsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.keys = [self.root / (c * 16) for c in "ab"]
        for key in self.keys:
            key.mkdir()

    def build(self, keys=None, out="out", **kwargs):
        target = self.root / out
        version, failures = builder.build(keys or self.keys[:1], target, CONFIG,
            config_bytes=CONFIG_PATH.read_bytes(), log=lambda _: None, **kwargs)
        self.assertEqual(failures, [])
        return target / version

    def test_committed_wins_in_both_orders_and_reports_uncommitted(self):
        corpus_result(self.keys[0], OK, commit=False)
        corpus_result(self.keys[1], OK)
        for keys in (self.keys, self.keys[::-1]):
            with self.subTest(order=keys):
                events, superseded, inputs = discover(keys, CONFIG)
                self.assertEqual(events[0].root, self.keys[1] / OK)
                self.assertEqual(superseded[0]["state"], "superseded_uncommitted")
                self.assertEqual(superseded[0]["key_prefix"], "a" * 16)
                self.assertEqual([i["bundles"] for i in inputs], [1, 1])
        output = self.build()
        rows = pq.read_table(output / "events.parquet").to_pylist()
        self.assertEqual(rows[0]["key_prefix"], "a" * 16)
        self.assertIsNone(rows[0]["corpus_key"])  # No verified full key exists in this directory.
        output = self.build(self.keys, out="chosen")
        self.assertIn("superseded_uncommitted", (output / "viability.md").read_text())

    def test_duplicate_commits_require_explicit_preference(self):
        for key in self.keys:
            corpus_result(key, OK)
        with self.assertRaisesRegex(ResearchError, "multiple committed"):
            discover(self.keys, CONFIG)
        for keys in (self.keys, self.keys[::-1]):
            events, superseded, _ = discover(keys, CONFIG, prefer_later=True)
            self.assertEqual(events[0].root, keys[-1] / OK)
            self.assertEqual(superseded[0]["key_prefix"], keys[0].name)

    def test_invalid_receipt_cannot_hide_committed_result(self):
        for key in self.keys:
            corpus_result(key, OK)
        (self.keys[1] / OK / "result.json").write_bytes(b"{}\n")
        events, superseded, _ = discover(self.keys, CONFIG)
        self.assertEqual(events[0].root, self.keys[0] / OK)
        self.assertEqual(superseded[0]["state"], "superseded_uncommitted")

    def test_appledouble_rebuild_is_reused_but_other_extras_fail(self):
        corpus_result(self.keys[0], OK)
        output = self.build()
        for directory in (self.keys[0], output, output / "events" / OK):
            (directory / "._episodes.ndjson.zst").write_bytes(b"finder")
            (directory / ".DS_Store").write_bytes(b"finder")
        self.assertEqual(self.build(), output)
        (output / "unexpected").write_bytes(b"not finder")
        with self.assertRaisesRegex(ResearchError, "file set"):
            self.build()

    def test_runtime_zstd_economics_matches_plain_build(self):
        harnesses = execution.IntegratedTests()
        harnesses.setUp()
        self.addCleanup(harnesses.tmp.cleanup)
        self.addCleanup(harnesses.doCleanups)
        results = []
        for compressed, key in zip((False, True), self.keys):
            h = harnesses.harness("cross", **({"storage": {"version": 1, "economic": "zstd"}}
                                              if compressed else {}))
            h.finish()
            suffix = ".zst" if compressed else ""
            for name in ("episodes.ndjson", "probes.ndjson", "entities.json", "reasons.json"):
                self.assertTrue((h.output / (name + suffix)).is_file(), name)

            def install(staging):
                output = staging / "run" / ATTEMPT / "cross" / "output"
                shutil.rmtree(output)
                shutil.copytree(h.output, output)

            corpus_result(key, OK, before_commit=install)
            output = self.build([key], out="compressed" if compressed else "plain")
            results.append((pq.read_table(output / "episodes.parquet").to_pylist(),
                            pq.read_table(output / "probes.parquet").to_pylist(),
                            read_json(output / "events" / OK / "overlays" / "cross.json")))
            from scripts.research_independent_check import main as independent_check
            with redirect_stdout(io.StringIO()) as report:
                self.assertEqual(independent_check([str(output.parent), str(key)]), 0)
            self.assertIn("ALL CHECKS PASSED", report.getvalue())
        self.assertTrue(results[0][0])
        self.assertTrue(results[0][1])
        # Storage changes the semantic manifest digest, not economic rows.
        for result in results:
            result[2].pop("semantic_sha256")
        self.assertEqual(results[0], results[1])

    def test_real_profile_layout3_without_levels(self):
        from replay.tests.test_market_profile import Harness
        from replay.tests.test_market_profile_v2 import exercise
        from replay.tests.test_market_volatility import policy
        root = self.root / "profile-fixture"
        root.mkdir()
        profile = policy()
        profile["groups"] = sorted(profile["groups"] + ["availability"])
        h = Harness(root, policy=profile,
                    storage={"version": 1, "profile": "zstd"}, mixed=True)
        exercise(h)
        h.finish()
        self.assertEqual(read_json(h.profile_output / "manifest.json")["version"], 3)
        self.assertFalse((h.profile_output / "levels.ndjson.zst").exists())

        def install(staging):
            output = staging / "run" / ATTEMPT / "profile" / "output"
            shutil.rmtree(output)
            shutil.copytree(h.profile_output, output)

        corpus_result(self.keys[0], OK, groups=("profile",), before_commit=install)
        output = self.build()
        pack = read_json(output / "events" / OK / "manifest.json")
        self.assertTrue(pack["tiles"]["raw"])
        self.assertEqual(pack["execution_realism"]["volatility"], "ok")

    def test_compressed_availability_matches_plain_and_cli_resolves_root(self):
        corpus_result(self.keys[0], OK)
        plain = self.build()

        def compress_availability(staging):
            output = staging / "run" / ATTEMPT / "profile" / "output"
            source = output / "availability.ndjson"
            with source.open("rb") as handle, (output / "availability.ndjson.zst").open("wb") as sink:
                result = encode_stream(handle, sink)
            manifest = read_json(output / "manifest.json")
            logical = manifest["files"].pop("availability.ndjson")
            manifest["files"]["availability.ndjson.zst"] = {"logical": logical, "stored": {
                "sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}
            source.unlink()
            write_json(output / "manifest.json", manifest)

        corpus_result(self.keys[1], OK, before_commit=compress_availability)
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        from replay.research.__main__ import main
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(main(["verify", str(alias / self.keys[1].name)]), 0)
            self.assertEqual(main(["build", str(alias / self.keys[1].name), "--out", str(self.root / "cli")]), 0)
        pointer = read_json(self.root / "cli" / "current.json")
        compressed = self.root / "cli" / pointer["version"]
        self.assertEqual(read_json(plain / "events" / OK / "availability.json"),
                         read_json(compressed / "events" / OK / "availability.json"))


class DeclaredFilesTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def stored(self, data=b'{"scopes":[]}\n', name="entities.json"):
        sink = io.BytesIO()
        result = encode_stream(io.BytesIO(data), sink)
        path = self.root / (name + ".zst")
        path.write_bytes(sink.getvalue())
        return {"files": {path.name: {
            "logical": {"sha256": result.logical.sha256, "byte_length": len(data), "records": data.count(b"\n")},
            "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}}}

    def test_corruption_and_inventory_errors(self):
        from replay.research.files import Files
        for case, message in (("stored", "stored.*sha256"), ("logical", "decoded.*sha256"),
                              ("truncated", "truncat"), ("trailing", "trailing"),
                              ("concatenated", "trailing|concatenat"), ("checksum", "checksum"),
                              ("both", "both forms declared"), ("undeclared", "undeclared evidence"),
                              ("missing", "declared file missing")):
            with self.subTest(case=case):
                manifest = self.stored()
                entry = manifest["files"]["entities.json.zst"]
                path = self.root / "entities.json.zst"
                if case in ("stored", "logical"):
                    entry[case]["sha256"] = "0" * 64
                elif case == "truncated":
                    path.write_bytes(path.read_bytes()[:-1])
                elif case == "trailing":
                    path.write_bytes(path.read_bytes() + b"extra")
                elif case == "concatenated":
                    path.write_bytes(path.read_bytes() * 2)
                elif case == "checksum":
                    raw = bytearray(path.read_bytes())
                    raw[-1] ^= 1
                    path.write_bytes(raw)
                elif case == "both":
                    manifest["files"]["entities.json"] = entry["logical"]
                elif case == "undeclared":
                    manifest["files"].clear()
                elif case == "missing":
                    path.unlink()
                with self.assertRaisesRegex(ValueError, message):
                    Files(self.root, manifest).document("entities.json", 256 * 1024 * 1024)

    def test_entity_document_bound_and_100mb_under_256mib(self):
        from replay.economic_sdk.bounds import MAX_ENTITY_TABLE
        from replay.research.files import Files
        data = b'{"padding":"' + b"x" * 100_000_000 + b'"}\n'
        manifest = self.stored(data)
        with self.assertRaisesRegex(ValueError, "size|limit|bound"):
            Files(self.root, manifest).document("entities.json", len(data) - 1)
        document = Files(self.root, manifest).document("entities.json", MAX_ENTITY_TABLE)
        self.assertEqual(len(document["padding"]), 100_000_000)

    def test_plain_identity_records_and_line_bound(self):
        from replay.research.files import Files
        data = b'{"x":1}\n'
        (self.root / "episodes.ndjson").write_bytes(data)
        entry = {"sha256": hashlib.sha256(data).hexdigest(), "byte_length": len(data), "records": 1}
        manifest = {"files": {"episodes.ndjson": entry}}
        self.assertEqual(list(Files(self.root, manifest).lines("episodes.ndjson")), [{"x": 1}])
        entry["records"] = 2
        with self.assertRaisesRegex(ValueError, "record count mismatch"):
            list(Files(self.root, manifest).lines("episodes.ndjson"))
        entry["records"] = 1
        with self.assertRaisesRegex(ValueError, "line bound"):
            list(Files(self.root, manifest).lines("episodes.ndjson", max_line=3))

    def test_symlinked_evidence_parent_is_rejected(self):
        from replay.research.files import Files
        manifest = self.stored()
        alias = self.root / "alias"
        alias.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "regular output required"):
            Files(alias, manifest).document("entities.json", 1024)

    def test_reasons_metadata_limit_and_summary_binding(self):
        from replay.economic_sdk.bounds import MAX_METADATA
        from replay.research.files import Files
        manifest = self.stored(b'{"reasons":[]}\n', name="reasons.json")
        self.assertEqual(Files(self.root, manifest).document("reasons.json", MAX_METADATA), {"reasons": []})
        with self.assertRaisesRegex(ValueError, "size limit"):
            Files(self.root, manifest).document("reasons.json", 1)
        write_json(self.root / "summary.json", {"x": 1})
        manifest["summary_sha256"] = hashlib.sha256(b'{"x":1}').hexdigest()
        self.assertEqual(Files(self.root, manifest).document("summary.json", MAX_METADATA), {"x": 1})
        write_json(self.root / "summary.json", {"x": 2})
        with self.assertRaisesRegex(ValueError, "summary.json: logical sha256 mismatch"):
            Files(self.root, manifest).document("summary.json", MAX_METADATA)


if __name__ == "__main__":
    unittest.main()
