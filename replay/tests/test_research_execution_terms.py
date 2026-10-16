"""Build research packs from committed, identity-valid outcomes-v2 contexts."""

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from replay.corpus import results
from replay.game_state import context_header
from replay.preparation import encoded, prepare
from replay.research import build as builder
from replay.research.config import load
from replay.tests.research_fixtures import corpus_result, write_json
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_execution_terms import execution_document, replace_outcomes
from replay.tests.test_preparation_outcomes import document

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "research.json"
CONFIG = load(CONFIG_PATH)
BUNDLE = "bundle_" + "1" * 24


class ResearchExecutionTermsTest(unittest.TestCase):
    def corpus(self, root, *, version, committed_game=False):
        context = root / "context"
        evidence = detail()
        evidence["bundle_id"] = evidence["context"]["bundle_id"] = BUNDLE
        policy = config(evidence)
        policy["bundle_id"] = BUNDLE
        outcomes = document()
        outcomes["bundle_id"] = BUNDLE
        snapshot = prepare(policy, context, universe=lambda *_: evidence, outcomes=lambda _: outcomes)
        if version == 2:
            outcomes = execution_document()
            outcomes["bundle_id"] = BUNDLE
            pin = replace_outcomes(context, outcomes)
        else:
            pin = hashlib.sha256((context / "context.json").read_bytes()).hexdigest()
        stage_key = root / "stage"
        stage_key.mkdir()
        staging = corpus_result(stage_key, BUNDLE, context=False, commit=False)
        shutil.copytree(context, staging / "context")
        result = json.loads((staging / "result.json").read_bytes())
        result["context"]["snapshot_sha256"] = pin
        if committed_game:
            header, _ = context_header(snapshot)
            game = {"version": 1, **header, "state": "unavailable", "reason": "no_source", "source": None,
                    "competitors": {side: {"label": None, "participant": None} for side in ("home", "away")},
                    "scheduled_start_ns": None, "segment_kind": None, "segments": [], "match": None}
            payload = encoded(game)
            path = staging / "game_state" / "game_state.json"
            path.parent.mkdir()
            path.write_bytes(payload)
            result["game_state_sha256"] = hashlib.sha256(payload).hexdigest()
        write_json(staging / "result.json", result)
        key = root / "results" / "aaaaaaaaaaaaaaaa"
        results.commit(staging, key / BUNDLE, corpus_key="a" * 16 + "0" * 48, bundle_id=BUNDLE,
                       status="SUCCESS", bench_exit=0, game_state=None)
        results.verify(key / BUNDLE)
        return key

    def assert_build(self, *, committed_game):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for version in (1, 2):
                with self.subTest(version=version, committed_game=committed_game):
                    case = root / str(version)
                    case.mkdir()
                    key = self.corpus(case, version=version, committed_game=committed_game)
                    archive = case / "archive"
                    archive.mkdir()
                    out = case / "out"
                    built, failures = builder.build([key], out, CONFIG, config_bytes=CONFIG_PATH.read_bytes(),
                                                    game_state_root=None if committed_game else archive,
                                                    log=lambda _: None)
                    self.assertEqual(failures, [])
                    row = pq.read_table(out / built / "events.parquet").to_pylist()[0]
                    self.assertEqual((row["state"], row["context"], row["pack"]), ("ok", "ok", "ok"))
                    game = json.loads((out / built / "events" / BUNDLE / "game.json").read_bytes())
                    self.assertEqual((game["state"], game["reason"]),
                                     ("no_source", "no_source" if committed_game else "no_fetch"))

    def test_v2_build_with_local_game_archive(self):
        self.assert_build(committed_game=False)

    def test_v2_build_with_committed_game_state(self):
        self.assert_build(committed_game=True)
