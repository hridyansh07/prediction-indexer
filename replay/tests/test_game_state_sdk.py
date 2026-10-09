"""Pinned game inputs and quiet-book release; only hand-authored offline shapes."""

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from replay.preparation import encoded, prepare
from replay.streams.protocol import ProtocolError
from replay.tests.test_preparation import config, detail
from replay.tests.test_preparation_outcomes import document
from replay.tests.economic_scenarios import v2_policy
from replay.tests.test_same_venue_complement import Harness


def snapshot():
    return {"outcomes": {"provider": "universe", "document": document()},
            "evidence": [{"detail": detail()}], "config": {"start_ns": "10", "end_ns": "40"}}


def game_file():
    return {"version": 1, "event_id": document()["event_id"], "state": "ok", "reason": None,
            "source": {"name": "kalshi", "prefix": "gamestate/source=kalshi/event=" + "a" * 64 + "/fetch=test",
                       "timeline_sha256": "b" * 64},
            "sport": "esports", "game": "counter_strike_2", "market_types": ["series_moneyline"],
            "competitors": {"home": {"label": "Alpha", "participant": 0},
                            "away": {"label": "Beta", "participant": 1}},
            "scheduled_start_ns": 8, "segment_kind": "map",
            "segments": [{"index": 1, "start_ns": 12, "start_estimated": True,
                          "end_ns": 20, "settled_ns": 22, "winner": "home",
                          "details": {"unknown_stat": {"value": 3}}}],
            "match": {"end_ns": 32, "winner": "home", "score": {"home": 1, "away": 0}}}


def policy(path, sha, **changes):
    result = {"required": False, "input": {"path": str(path), "sha256": sha},
              "windows": {name: {"after_ms": 0} for name in ("segment_start", "segment_end", "match_end")},
              "segment_start": "estimated", "priors": {"counter_strike_2": {
                  "segment_duration_ns": "8", "between_segments_ns": "4", "settlement_delay_ns": "2"}}}
    result.update(changes)
    return result


class GameInputTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def write(self, value=None):
        payload = encoded(value or game_file()) + b"\n"
        path = self.root / "game_state.json"
        path.write_bytes(payload)
        return path, hashlib.sha256(payload).hexdigest()

    def test_loader_pin_closed_schema_identity_and_segment_order(self):
        from replay.game_state import load
        path, sha = self.write()
        self.assertEqual(load(path, sha, snapshot())["segments"][0]["details"]["unknown_stat"]["value"], 3)
        with self.assertRaisesRegex(ProtocolError, "SHA-256"):
            load(path, "0" * 64, snapshot())
        for mutation in (lambda d: d.update(extra=1), lambda d: d.update(version=2),
                         lambda d: d.update(event_id="event:d1:" + "c" * 64),
                         lambda d: d["segments"].append(dict(d["segments"][0])),
                         lambda d: d["segments"][0].update(start_ns=21)):
            bad = game_file(); mutation(bad)
            path, sha = self.write(bad)
            with self.assertRaises(ProtocolError):
                load(path, sha, snapshot())

    def test_loader_requires_nonnull_source_identity(self):
        from replay.game_state import load
        for key in ("name", "prefix"):
            value = game_file(); value["source"][key] = None
            path, sha = self.write(value)
            with self.subTest(key=key), self.assertRaisesRegex(ProtocolError, "source identity"):
                load(path, sha, snapshot())

    def test_loader_rejects_overflowed_float_in_opaque_details(self):
        from replay.game_state import load
        payload = encoded(game_file()).replace(b'"value":3', b'"value":1e309') + b"\n"
        self.assertIn(b"1e309", payload)
        path = self.root / "game_state.json"; path.write_bytes(payload)
        with self.assertRaisesRegex(ProtocolError, "nonfinite"):
            load(path, hashlib.sha256(payload).hexdigest(), snapshot())

    def test_release_is_frozen_no_lookahead_and_drops_applied_facts(self):
        from replay.game_state import load
        from replay.economic_sdk.game import Timeline, participant_score
        path, sha = self.write()
        queue = Timeline(load(path, sha, snapshot()), policy(path, sha), 10)
        queue.advance(19)
        before = queue.view
        self.assertEqual(before.phase, "in_segment")
        self.assertIsNone(before.last_segment)
        count = len(queue.facts)
        queue.advance(20)
        self.assertEqual(len(queue.facts), count - 1)
        self.assertEqual(queue.view.last_segment.details["unknown_stat"]["value"], 3)
        self.assertEqual(participant_score(queue.view, 0), 1)
        self.assertIsNone(before.last_segment)
        with self.assertRaises(TypeError):
            queue.view.last_segment.details["unknown_stat"]["value"] = 5
        queue.advance(32)
        self.assertEqual(queue.view.phase, "finished")
        self.assertEqual(len(queue.facts), 0)

    def test_at_end_mode_and_generic_match_only(self):
        from replay.game_state import load
        from replay.economic_sdk.game import Timeline
        path, sha = self.write()
        queue = Timeline(load(path, sha, snapshot()), policy(path, sha, segment_start="at_segment_end"), 10)
        queue.advance(19)
        self.assertEqual(queue.view.phase, "pre_match")
        queue.advance(20)
        self.assertEqual(queue.view.phase, "between_segments")
        value = game_file(); value.update(segments=[], segment_kind=None)
        path, sha = self.write(value)
        queue = Timeline(load(path, sha, snapshot()), policy(path, sha), 10)
        queue.advance(32)
        self.assertEqual(queue.view.score, {"home": 1, "away": 0})
        self.assertEqual(queue.view.phase, "finished")

    def test_same_instant_match_finishes_after_final_segment(self):
        from replay.game_state import load
        from replay.economic_sdk.game import Timeline
        value = game_file(); value["match"]["end_ns"] = 20
        path, sha = self.write(value)
        queue = Timeline(load(path, sha, snapshot()), policy(path, sha), 10)
        queue.advance(20)
        self.assertEqual(queue.view.phase, "finished")
        self.assertEqual(queue.view.score, {"home": 1, "away": 0})
        self.assertEqual(queue.view.last_segment.index, 1)

    def test_delayed_windows_one_ns_before_release_and_missing_priors(self):
        from replay.game_state import load
        from replay.economic_sdk.game import (Timeline, elapsed_in_phase,
                                             expected_segment_end, expected_settlement)
        value = game_file()
        value["scheduled_start_ns"] *= 1_000_000
        for segment in value["segments"]:
            for key in ("start_ns", "end_ns", "settled_ns"):
                segment[key] *= 1_000_000
        value["match"]["end_ns"] *= 1_000_000
        path, sha = self.write(value)
        p = policy(path, sha, windows={"segment_start": {"after_ms": 1},
                                      "segment_end": {"after_ms": 2}, "match_end": {"after_ms": 3}})
        queue = Timeline(load(path, sha, snapshot()), p, 10_000_000)
        queue.advance(12_999_999)
        self.assertEqual(queue.view.phase, "pre_match")
        queue.advance(13_000_000)
        self.assertEqual(queue.view.phase_since_ns, 13_000_000)
        self.assertEqual(elapsed_in_phase(queue.view, 13_000_003), 3)
        self.assertIsNone(expected_segment_end(queue.view, {}))
        self.assertIsNone(expected_settlement(queue.view, {}))
        self.assertEqual(expected_segment_end(queue.view, p["priors"]), 13_000_008)
        queue.advance(21_999_999)
        self.assertIsNone(queue.view.last_segment)
        self.assertEqual(queue.view.score["home"], 0)
        queue.advance(22_000_000)
        self.assertEqual(queue.view.last_segment.winner, "home")
        self.assertEqual(queue.view.last_segment.details["unknown_stat"]["value"], 3)
        self.assertEqual(queue.view.settled, 1)
        queue.advance(34_999_999)
        self.assertEqual(queue.view.phase, "between_segments")
        queue.advance(35_000_000)
        self.assertEqual(queue.view.phase, "finished")

    def test_preparation_unavailable_and_header_from_context(self):
        from replay.prepare_game_state import prepare_game_state
        context = self.root / "context"
        prepare(config(), context, universe=lambda *_: detail(), outcomes=lambda _: document())
        with patch("gamestate.timeline.latest", return_value={"state": "no_source", "inputs": {}}):
            sha = prepare_game_state(context, self.root / "prepared", store=object())
        payload = (self.root / "prepared/game_state.json").read_bytes()
        self.assertEqual(hashlib.sha256(payload).hexdigest(), sha)
        value = json.loads(payload)
        self.assertEqual((value["state"], value["reason"]), ("unavailable", "no_fetch"))
        self.assertEqual(value["game"], detail()["game"])
        self.assertEqual(value["market_types"], ["series_moneyline"])

    def archive(self, *, labels=(" Alpha ", "BETA"), status="complete", label_conflict=False):
        from archive.storage.local import LocalObjectStore
        from gamestate import kalshi
        from tests.test_kalshi_game_state import records, rewrite
        store = LocalObjectStore(self.root / "archive")
        milestone, rows = records("lol")
        milestone["end_date"] = "2026-09-27T13:00:00Z"
        rows = rewrite(rows, 0, {"milestones": [milestone]})
        for index in (2, 3):
            doc = kalshi.loads(kalshi.body_bytes(rows[index]))
            home = doc["event"]["markets"][0]
            home["yes_sub_title"] = labels[0] if index == 2 or not label_conflict else "Different"
            doc["event"]["markets"].append({**home, "ticker": home["ticker"] + "-A", "result": "no",
                                            "custom_strike": {"esports_competitor": "a"}, "yes_sub_title": labels[1]})
            rows = rewrite(rows, index, doc)
        prefix = kalshi.archive_fetch(store, milestone, ["bundle-1"], rows, 1_800_000_000_000000000,
                                      status, event_id=document()["event_id"])
        kalshi.regenerate(store, prefix)
        return store, prefix

    def test_preparation_verified_archive_alignment_and_priors(self):
        from replay.prepare_game_state import prepare_game_state
        from replay.game_state import load
        from gamestate.priors import statistics
        context = self.root / "context"
        prepare(config(), context, universe=lambda *_: detail(), outcomes=lambda _: document())
        store, prefix = self.archive()
        before = list(store.list_keys("gamestate/"))
        with patch("socket.create_connection", side_effect=AssertionError("network")):
            sha = prepare_game_state(context, self.root / "prepared", store=store)
            from replay.preparation import load_snapshot
            game = load(self.root / "prepared/game_state.json", sha, load_snapshot(context))
            priors = statistics(store)
        self.assertEqual(game["state"], "ok")
        self.assertEqual(game["source"]["prefix"], prefix)
        self.assertEqual(game["competitors"]["home"]["participant"], 0)
        self.assertEqual(game["competitors"]["away"]["participant"], 1)
        self.assertEqual(game["game"], "counter_strike_2")  # vendor says lol
        self.assertEqual(game["segments"][0]["winner"], "home")
        self.assertEqual(game["segments"][0]["details"]["home"]["kills"], 27)
        self.assertEqual(priors["games"]["league_of_legends"]["segment_duration_ns"],
                         {"count": 1, "median": "1234000000000", "p10": "1234000000000", "p90": "1234000000000"})
        self.assertEqual(before, list(store.list_keys("gamestate/")))
        with self.assertRaisesRegex(ProtocolError, "already exists"):
            prepare_game_state(context, self.root / "prepared", store=store)

    def test_preparation_no_source_incomplete_and_conflicting_names(self):
        from replay.prepare_game_state import prepare_game_state
        from replay.game_state import align
        self.assertEqual(align({"home": "STRASSE", "away": " Beta "}, ["Straße", "beta"])["home"]["participant"], 0)
        self.assertTrue(all(row["participant"] is None for row in
                            align({"home": "Alpha", "away": "alpha"}, ["Alpha", "Beta"]).values()))
        self.assertIsNone(align({"home": "Alfa", "away": "Beta"}, ["Alpha", "Beta"])["home"]["participant"])
        context = self.root / "context"
        doc = document(); doc["markets"] = [m for m in doc["markets"] if m["venue"] != "kalshi"]
        prepare(config(), context, universe=lambda *_: detail(), outcomes=lambda _: doc)
        with patch("gamestate.timeline.latest", side_effect=AssertionError("no source read")):
            prepare_game_state(context, self.root / "no-source", store=object())
        self.assertEqual(json.loads((self.root / "no-source/game_state.json").read_bytes())["reason"], "no_source")
        context2 = self.root / "context2"
        prepare(config(), context2, universe=lambda *_: detail(), outcomes=lambda _: document())
        with patch("gamestate.timeline.latest", return_value={"state": "incomplete"}):
            prepare_game_state(context2, self.root / "incomplete", store=object())
        self.assertEqual(json.loads((self.root / "incomplete/game_state.json").read_bytes())["reason"], "incomplete")
        store, _ = self.archive(label_conflict=True)
        prepare_game_state(context2, self.root / "conflict", store=store)
        self.assertIsNone(json.loads((self.root / "conflict/game_state.json").read_bytes())["competitors"]["home"]["participant"])

    def harness(self, value=None, *, scopes=False, **game_changes):
        path, sha = self.write(value)
        from replay.preparation import prepare as real_prepare
        with patch("replay.tests.test_bundle_coverage.prepare",
                   side_effect=lambda c, p, **kw: real_prepare(c, p, **kw, outcomes=lambda _: document())):
            result = Harness(self.root, scopes=scopes, policy=v2_policy(game=policy(path, sha, **game_changes)))
        for writer in result.strategy.writers.values():
            self.addCleanup(writer.stream.close)
        return result

    def test_required_unavailable_opens_no_outputs_and_reads_no_books(self):
        from replay.economic_sdk.game import GameStateUnavailable
        value = game_file(); value.update(state="unavailable", reason="no_fetch", source=None,
                                         scheduled_start_ns=None, segment_kind=None, segments=[], match=None)
        with self.assertRaisesRegex(GameStateUnavailable, "game_state_unavailable:no_fetch"):
            self.harness(value, required=True)
        self.assertEqual(list((self.root / "output").iterdir()), [])

    def test_nonrequired_unavailable_runs_with_frozen_header(self):
        value = game_file(); value.update(state="unavailable", reason="incomplete", source=None,
                                         scheduled_start_ns=None, segment_kind=None, segments=[], match=None)
        h = self.harness(value)
        self.assertEqual(h.strategy.game.view.phase, "unavailable")
        self.assertEqual(h.strategy.game.view.reason, "incomplete")
        self.assertEqual(h.strategy.game.view.revision, 0)
        h.window(); h.quote(12, 0); h.quote(12, 1)
        h.finish()
        self.assertTrue(all(row["open"]["game"]["phase"] == "unavailable" for row in h.records("episodes.ndjson")))

    def test_scope_then_release_then_same_instant_books(self):
        from replay.economic_sdk import GameRequirement, Observation, Requirements
        from replay.strategies.same_venue_complement.strategy import SameVenueComplement
        baskets = SameVenueComplement.baskets
        requirements = SameVenueComplement.requirements
        seen = []
        def read_game(strategy, *args):
            return tuple(replace(b, inputs=b.inputs + ((("game", None),),)) for b in baskets(strategy, *args))
        def game_requirement(strategy, *args):
            return Requirements(requirements(strategy, *args).books, game=GameRequirement())
        def evaluate(strategy, entity, views, context):
            seen.append((context.time, context.scope, context.game.phase,
                         context.game.revision, tuple(v.last_change for v in views)))
            return Observation("UNUSABLE", context_free=True)
        value = game_file(); value["segments"][0].update(end_ns=23, settled_ns=25)
        with patch.object(SameVenueComplement, "baskets", read_game), \
             patch.object(SameVenueComplement, "requirements", game_requirement), \
             patch.object(SameVenueComplement, "evaluate", evaluate):
            h = self.harness(value, scopes=True); h.window(); h.quote(12, 0); h.quote(12, 1)
            seen.clear(); h.quote(23, 0, ask=480); h.group(24)
            at = [row for row in seen if row[0] == 23]
            self.assertTrue(at)
            self.assertTrue(all(row[1:4] == (1, "between_segments", 3) for row in at))
            self.assertTrue(any(23 in row[4] for row in at))
            h.finish()

    def bench_spec(self, value=None):
        from replay.tests.test_bench import run_spec
        spec = run_spec(self.root)
        path, sha = self.write(value)
        spec["game_state_path"] = str(path)
        gp = policy(path, sha, required=True)
        gp["input"] = {"path": "{game_state}", "sha256": "{game_state_sha256}"}
        spec["groups"][0]["config"]["policy"] = {"game": gp}
        return spec, sha

    def test_bench_tokens_pin_and_readonly_mount(self):
        from replay.tests.test_bench import FakeDocker, resolved
        from replay.bench.common import write_json
        from replay.bench.docker import run_bench
        from replay.bench.specs import validate_resolved
        spec, sha = self.bench_spec()
        actual = resolved(spec)
        self.assertEqual(actual["runtime"]["game_state_sha256"], sha)
        self.assertEqual(actual["groups"][0]["config"]["policy"]["game"]["input"],
                         {"path": "/bench/game_state.json", "sha256": sha})
        bad = deepcopy(actual); bad["groups"][0]["config"]["policy"]["game"]["input"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ProtocolError, "policy pin"):
            validate_resolved(bad)
        file = self.root / "spec.json"; write_json(file, spec)
        docker = FakeDocker()
        self.assertEqual(run_bench(file, self.root / "bench", docker=docker,
                                  source_reader=lambda _: ("a" * 40, True)), 0)
        mounts = [args[i + 1] for args, _ in docker.calls if args[0] == "create"
                  for i, arg in enumerate(args) if arg == "--mount"]
        self.assertIn("type=bind,src=" + str(Path(spec["game_state_path"]).resolve()) + ",dst=/bench/game_state.json,readonly", mounts)

    def test_bench_required_skip_and_optional_group_continue(self):
        from replay.tests.test_bench import resolved
        from replay.bench.inside import execute, validate_result
        from replay.bench.common import read_json
        value = game_file(); value.update(state="unavailable", reason="no_fetch", source=None,
                                         scheduled_start_ns=None, segment_kind=None, segments=[], match=None)
        spec, sha = self.bench_spec(value)
        for mixed in (False, True):
            case = deepcopy(spec)
            if mixed:
                optional = deepcopy(case["groups"][0]); optional["name"] = "optional"
                optional["config"]["policy"]["game"]["required"] = False
                case["groups"].append(optional)
            output = self.root / ("mixed" if mixed else "all-required"); output.mkdir()
            (output / "game_state.json").write_bytes((self.root / "game_state.json").read_bytes())
            reader = Mock(return_value={"receipt": {"semantic_sha256": "f" * 64}})
            importer = lambda ref: reader if ref.endswith(":read_completed") else Mock()
            supervise = Mock(return_value={"outputs": {"optional": "attempt/optional/output"}})
            code, result = execute(resolved(case), output, redis_url="redis://synthetic", importer=importer,
                                   snapshot_loader=Mock(return_value=snapshot()), supervisor_run=supervise)
            self.assertEqual((code, result["status"]), (2, "GAME_STATE_UNAVAILABLE"))
            self.assertEqual(result["groups"][0]["unavailable"],
                             {"status": "game_state_unavailable", "reason": "no_fetch", "sha256": sha})
            self.assertIsNone(result["groups"][0]["receipt"])
            self.assertEqual(list((output / "groups/g/work").iterdir()), [])
            validate_result(read_json(output / "result.json"))
            bad = deepcopy(result); bad["groups"][0]["unavailable"]["sha256"] = "0" * 64
            with self.assertRaisesRegex(ProtocolError, "unavailable game pin"):
                validate_result(bad)
            bad = deepcopy(result); bad["status"] = "SUCCESS"
            with self.assertRaisesRegex(ProtocolError, "unavailable game status"):
                validate_result(bad)
            if mixed:
                self.assertEqual(supervise.call_args.args[0]["transport"]["groups"], ["optional"])
                self.assertEqual(reader.call_count, 1)
            else:
                supervise.assert_not_called(); reader.assert_not_called()

    def test_bench_refuses_policy_pin_mismatch_before_docker(self):
        from replay.tests.test_bench import resolved
        spec, _ = self.bench_spec()
        spec["groups"][0]["config"]["policy"]["game"]["input"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ProtocolError, "policy pin"):
            resolved(spec)

    def test_runtime_releases_only_game_readers_and_timers_on_quiet_books(self):
        from replay.economic_sdk import GameRequirement, Observation, Requirements
        from replay.strategies.same_venue_complement.strategy import SameVenueComplement
        original_baskets = SameVenueComplement.baskets
        original_requirements = SameVenueComplement.requirements
        seen = []

        def baskets(strategy, *args):
            return tuple(replace(b, inputs=b.inputs + ((("game", None),),))
                         if b.descriptor["direction"] == "long" else b
                         for b in original_baskets(strategy, *args))

        def requirements(strategy, *args):
            result = original_requirements(strategy, *args)
            return Requirements(result.books, game=GameRequirement(timers=(("segment_start", 5),)))

        def evaluate(strategy, entity, views, context):
            seen.append((entity.descriptor["direction"], context.time, context.game.phase, context.game.revision))
            return Observation("UNUSABLE", context_free=True)

        with patch.object(SameVenueComplement, "baskets", baskets), \
             patch.object(SameVenueComplement, "requirements", requirements), \
             patch.object(SameVenueComplement, "evaluate", evaluate):
            h = self.harness(); h.window(); seen.clear(); h.group(19)
            self.assertEqual({(d, t) for d, t, _, _ in seen}, {("long", 12), ("long", 17)})
            h.group(20)
            self.assertIn(("long", 20, "between_segments", 3), seen)
            h.finish()

    def test_episode_annotation_manifest_and_independent_reader_rejection(self):
        from replay.strategies.same_venue_complement.output import validate_content
        h = self.harness(); h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(23, 0, ask=480)
        result = h.finish()
        manifest = result["manifest"]
        self.assertEqual(manifest["game_state"]["sha256"], h.complement_config["policy"]["game"]["input"]["sha256"])
        episodes = [json.loads(line) for line in (h.output / "episodes.ndjson").read_bytes().splitlines()]
        self.assertTrue(episodes)
        self.assertTrue(all("game" in row["open"] and "game" in row["at_max"] for row in episodes))
        long = next(row for row in episodes if row["at_max"]["game"]["revision"] > row["open"]["game"]["revision"])
        self.assertEqual(long["open"]["game"]["phase"], "in_segment")
        self.assertEqual(long["at_max"]["game"]["phase"], "between_segments")
        bad = deepcopy(manifest); bad["game_state"]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ProtocolError, "binding"):
            validate_content(h.output, h.snapshot, bad)
        bad = deepcopy(manifest); bad.pop("game_state")
        with self.assertRaisesRegex(ProtocolError, "presence"):
            validate_content(h.output, h.snapshot, bad)

    def test_independent_reader_rejects_rehashed_invalid_game_annotations(self):
        from replay.strategies.same_venue_complement.output import validate_content
        h = self.harness(); h.window(); h.quote(12, 0); h.quote(12, 1)
        manifest = h.finish()["manifest"]
        original = h.records("episodes.ndjson")
        self.assertTrue(original)
        for field, game in (("open", {"phase": "in_segment", "segment": None,
                                    "score": {"home": 0, "away": 0}, "revision": 2}),
                            ("at_max", {"phase": "unavailable", "segment": None,
                                      "score": {"home": 1, "away": 0}, "revision": 3})):
            rows = deepcopy(original); rows[0][field]["game"] = game
            payload = b"".join(encoded(row) + b"\n" for row in rows)
            (h.output / "episodes.ndjson").write_bytes(payload)
            bad = deepcopy(manifest); bad.pop("summary_sha256", None)
            bad["files"]["episodes.ndjson"] = {"sha256": hashlib.sha256(payload).hexdigest(),
                                                "byte_length": len(payload), "records": len(rows)}
            with self.subTest(field=field), self.assertRaisesRegex(ProtocolError, "episode game"):
                validate_content(h.output, h.snapshot, bad)

    def test_fixture_stage_reuses_verified_bytes_without_archive(self):
        from replay.bench.game_state import prepare_fixture_stage
        context = self.root / "context"
        prepare(config(), context, universe=lambda *_: detail(), outcomes=lambda _: document())
        store, _ = self.archive()
        root = self.root / "game"
        pin = prepare_fixture_stage(context, root, store=store)
        payload = (root / "game_state.json").read_bytes()
        with patch("replay.bench.game_state.build_store", side_effect=AssertionError("archive reopened")):
            self.assertEqual(prepare_fixture_stage(context, root), pin)
        self.assertEqual((root / "game_state.json").read_bytes(), payload)
        incomplete = self.root / "crashed"; incomplete.mkdir()
        with self.assertRaises(FileNotFoundError):
            prepare_fixture_stage(context, incomplete, store=store)

    def test_policy_decisions_change_identity_but_mount_path_does_not(self):
        from replay.strategies.same_venue_complement.contract import experiment_identity
        path, sha = self.write()
        base = v2_policy(game=policy(path, sha))
        identity = experiment_identity("c" * 64, base, {})
        for mutation in (lambda p: p.update(required=True), lambda p: p.update(segment_start="at_segment_end"),
                         lambda p: p["input"].update(sha256="d" * 64),
                         lambda p: p["windows"]["segment_end"].update(after_ms=1),
                         lambda p: p["priors"]["counter_strike_2"].update(segment_duration_ns="9")):
            changed = deepcopy(base); mutation(changed["game"])
            self.assertNotEqual(identity, experiment_identity("c" * 64, changed, {}))
        changed = deepcopy(base); changed["game"]["input"]["path"] = "/mounted/game_state.json"
        self.assertEqual(identity, experiment_identity("c" * 64, changed, {}))


if __name__ == "__main__":
    unittest.main()
