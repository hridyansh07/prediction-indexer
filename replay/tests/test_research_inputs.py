import json
import tempfile
import unittest
from pathlib import Path

from replay.research import config as research_config
from replay.research.inputs import discover
from replay.research.jsonio import ResearchError
from replay.tests.research_fixtures import ATTEMPT, corpus_result

CONFIG = research_config.load(Path(__file__).resolve().parents[2] / "configs" / "research.json")
B1, B2, B3 = "bundle_" + "1" * 24, "bundle_" + "2" * 24, "bundle_" + "3" * 24


class InputsTest(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.key = self.root / "results" / "aaaaaaaaaaaaaaaa"
        self.key.mkdir(parents=True)

    def tearDown(self):
        self._tmp.cleanup()

    def one(self, key_dirs=None):
        events, superseded, inputs = discover(key_dirs or [self.key], CONFIG)
        return events, superseded, inputs

    def test_ok_result_lens_states(self):
        corpus_result(self.key, B1, excluded=("multi",))
        (event,), _, inputs = self.one()
        self.assertEqual(event.state, "ok")
        self.assertEqual(event.attempt, ATTEMPT)
        self.assertEqual(event.context_state, "ok")
        self.assertEqual(event.lenses["profile"]["state"], "ok")
        self.assertEqual(event.lenses["cross"]["state"], "ok")
        self.assertEqual(event.lenses["cross"]["semantic_sha256"], "c" * 64)
        self.assertEqual(event.lenses["multi"], {"label": "Same-venue multi-market", "state": "excluded",
                                                 "reason": "entities.json over 8 MiB"})
        self.assertEqual(event.lenses["complement"]["state"], "not_run")
        self.assertEqual(event.unmapped_groups, ["coverage", ])
        self.assertEqual(inputs, [{"key_prefix": "aaaaaaaaaaaaaaaa", "corpus_key": "a" * 16 + "0" * 48,
                                   "bundles": 1}])
        self.assertTrue(all(c["pass"] for c in event.checks), event.checks)

    def test_failed_group_takes_its_failure_json_chain(self):
        corpus_result(self.key, B1, status="FAILED", failed_group="cross")
        (event,), _, _ = self.one()
        self.assertEqual(event.lenses["cross"]["error"],
                         "RuntimeError: detached state budget <- MemoryError: 128 MiB")
        self.assertEqual(event.lenses["profile"]["error"], "SupervisorError: group failed")
        self.assertIn({"bundle_id": B1, "check": "bench_status", "pass": False, "message": "FAILED"}, event.checks)

    def test_failed_check_fails_only_that_lens(self):
        corpus_result(self.key, B1, status="CHECKS_FAILED", checks={"cross": {"m:fees": False, "m:ok": True}})
        (event,), _, _ = self.one()
        self.assertEqual(event.lenses["cross"], {"label": "Cross-venue complete set", "state": "failed",
                                                 "error": "checks failed: m:fees"})
        self.assertEqual(event.lenses["profile"]["state"], "ok")

    def test_uncommitted_and_invalid_receipts_stay_visible(self):
        corpus_result(self.key, B1, commit=False)
        corpus_result(self.key, B2)
        (self.key / B2 / "result.json").write_bytes(b"{}\n")
        events, _, _ = self.one()
        self.assertEqual([(e.bundle_id, e.state) for e in events], [(B1, "uncommitted"), (B2, "receipt_invalid")])
        self.assertIn("mismatch", events[1].message)
        self.assertEqual(events[0].lenses["cross"]["state"], "failed")

    def test_context_binding_and_missing_context(self):
        corpus_result(self.key, B1, tamper_context=True)
        corpus_result(self.key, B2, context=False)
        bad, missing = self.one()[0]
        self.assertEqual((bad.state, bad.context_state), ("context_invalid", "invalid"))
        self.assertEqual((missing.state, missing.context_state, missing.context), ("ok", "missing", None))

    def test_attempt_choice(self):
        corpus_result(self.key, B1, success_marker=False)
        corpus_result(self.key, B2, success_marker=False, attempts=(ATTEMPT, "b" * 32))
        single, ambiguous = self.one()[0]
        self.assertEqual(single.attempt, ATTEMPT)
        self.assertIsNone(ambiguous.attempt)
        self.assertEqual(ambiguous.lenses["cross"]["error"], f"ambiguous attempts: {ATTEMPT}, {'b' * 32}")

    def test_last_key_directory_wins_and_the_rest_are_superseded(self):
        later = self.root / "results-state" / "bbbbbbbbbbbbbbbb"
        later.mkdir(parents=True)
        corpus_result(self.key, B1, status="FAILED")
        corpus_result(self.key, B3)
        corpus_result(later, B1)
        events, superseded, inputs = self.one([self.key, later])
        self.assertEqual([(e.bundle_id, e.key_prefix) for e in events],
                         [(B1, "bbbbbbbbbbbbbbbb"), (B3, "aaaaaaaaaaaaaaaa")])
        self.assertEqual(superseded, [{"bundle_id": B1, "key_prefix": "aaaaaaaaaaaaaaaa",
                                       "corpus_key": "a" * 16 + "0" * 48, "by": "bbbbbbbbbbbbbbbb"}])
        self.assertEqual([i["bundles"] for i in inputs], [2, 1])

    def test_key_directory_must_prefix_its_corpus_key(self):
        corpus_result(self.key, B1, corpus_key="c" * 64)
        with self.assertRaises(ResearchError):
            self.one()


class ConfigTest(unittest.TestCase):
    def config(self):
        return json.loads((Path(__file__).resolve().parents[2] / "configs" / "research.json").read_bytes())

    def test_closed_config(self):
        value = self.config()
        value["publish"] = {}
        with self.assertRaises(ResearchError):
            research_config.validate(value)
        value = self.config()
        value["lenses"]["cross"]["threshold"] = 1
        with self.assertRaises(ResearchError):
            research_config.validate(value)
        value = self.config()
        value["lenses"]["multi"]["group"] = "cross"
        with self.assertRaises(ResearchError):
            research_config.validate(value)
        value = self.config()
        value["game_time_windows_status"] = "measured"
        with self.assertRaises(ResearchError):
            research_config.validate(value)
        value = self.config()
        del value["lenses"]["profile"]
        with self.assertRaises(ResearchError):
            research_config.validate(value)


if __name__ == "__main__":
    unittest.main()
