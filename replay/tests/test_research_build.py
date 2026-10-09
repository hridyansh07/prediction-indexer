"""Real SDK outputs with synthetic completion evidence; no retained or live data."""
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from archive.storage.local import LocalObjectStore
from gamestate import kalshi
from replay.research.build import audit, build
from replay.research.query import query, download_table
from replay.research.io import digest, document, encoded, identity, write
from replay.research.verify.runs import verify
from replay.strategies.market_profile.strategy import MarketProfile
from replay.streams.protocol import Cut, freeze
from replay.tests.test_cross_venue_arbitrage import EDGE, Harness
from replay.tests.economic_scenarios import PROFILE_POLICY
from tests.test_kalshi_game_state import records as game_records

EVENT = "event:d1:" + "a" * 64


def acceptance(root):
    """Same immutable context/tape for profile and cross-venue, genuine fee fills."""
    work = root / "source"
    work.mkdir()
    h = Harness(work, fills=(EDGE,), fees="collateral")
    profile_root = work / "profile"
    profile_root.mkdir()
    policy = {**PROFILE_POLICY, "version": 2, "groups": sorted(set(PROFILE_POLICY["groups"]) | {"transitions", "availability"})}
    profile_config = {**h.cfg, "policy": policy}
    profile = MarketProfile(freeze({**h.context, "config": profile_config, "output_directory": str(profile_root)}))
    profile(Cut(0, "initial", freeze(h.initial), h.decoder.books))
    send = h.send
    def tee(kind, body):
        cut = send(kind, body)
        profile(cut)
        return cut
    h.send = tee
    h.populate()
    h.finish()
    profile.finish()
    run_paths = {}
    for lens, group, output, cfg, factory in (
        ("profile", "profile", profile_root, profile_config, "market_profile"),
        ("cross_venue", "cross", h.output, h.cross_config, "cross_venue_arbitrage")):
        bench = root / lens
        run = bench / "run"
        attempt = "a" * 32
        destination = run / attempt / group / "output"
        shutil.copytree(output, destination)
        # Hand-authored complete supervisor/bench contract, not a running process.
        config = {"version": 1, "limits": {"state_bytes": 128 * 1024**2}, "publisher": "unused", "python": "python",
                  "transport": {"groups": [group], "run_id": "synthetic"},
                  "strategies": {group: {"factory": "replay.strategies." + factory + ":build", "revision": "synthetic", "config": cfg}}}
        sha = digest(config)
        terminal = h.seq
        content = {"version": 1, "semantic_sha256": digest(document(destination / "manifest.json")),
                   "run_id": "synthetic", "attempt_id": attempt, "group": group, "identity": sha, "terminal": terminal}
        (destination / "content_receipt.json").write_bytes(encoded(content))
        write(run / "run.json", config)
        write(run / "SUCCESS.json", {"version": 1, "identity": sha, "attempt": attempt, "terminal": terminal,
                                      "outputs": {group: attempt + "/" + group + "/output"}})
        write(run / attempt / "result.json", {"version": 1, "identity": sha, "attempt": attempt, "outcome": "success",
                                               "fatal": False, "progress": terminal, "terminal": terminal,
                                               "participants": {"publisher": 0, group: 0}})
        write(run / attempt / group / "complete.json", {"version": 1, "identity": sha, "attempt": attempt, "group": group, "terminal": terminal})
        write(bench / "result.json", {"version": 1, "label": lens, "status": "SUCCESS", "started_at": "synthetic",
                                     "run_seconds": 0, "image": {"source_commit": "synthetic"}, "error": None,
                                     "context": {"snapshot_sha256": h.sha}, "fee_catalog_identity": None,
                                     "groups": [{"name": group, "receipt": content, "checks": {"synthetic": True}}]})
        run_paths[lens] = str(bench)
    h.close()
    config_path, runs_path = root / "research.json", root / "runs.json"
    write(config_path, {"version": 1, "lenses": {"profile": {"group": "profile", "adapter": "profile"},
          "cross_venue": {"group": "cross", "adapter": "sdk_episodes"}, "complement": {"group": "complement", "adapter": "sdk_episodes"}},
          "game_time_windows": {k: {"before_ms": 2000, "after_ms": 5000} for k in
          ("kalshi_market_close", "close_minus_duration", "kalshi_milestone_end")}, "archive": None})
    write(runs_path, {"runs_version": 1, "events": [{"bundle_id": "bundle-1", "context": str(work / "context"), "runs": run_paths}]})
    store = LocalObjectStore(root / "game-archive")
    mapping, records = game_records()
    prefix = kalshi.archive_fetch(store, mapping, ["bundle-1"], records, 10**9, "complete", event_id=EVENT)
    kalshi.regenerate(store, prefix)
    return config_path, runs_path, store


class ResearchBuildTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config, self.runs, self.store = acceptance(self.root)
        self.verification = self.root / "verify.json"

    def build(self, name="built"):
        return build(self.config, self.runs, self.verification, self.root / name, store=self.store)

    def test_full_episode_binding_game_windows_determinism_and_query(self):
        report = verify(self.config, self.runs, self.verification, store=self.store)
        self.assertEqual(report["events"][0]["lenses"]["cross_venue"]["state"], "ok", str(report))
        result = self.build()
        self.assertEqual(audit(self.root / "built"), result)
        self.assertEqual(self.build("rebuilt"), result)
        pack = document(self.root / "built" / "packs" / EVENT / "manifest.json")
        self.assertEqual(pack["lenses"]["complement"], {"state": "not_run"})
        self.assertEqual(pack["game_state"]["state"], "ok")
        overlay = document(self.root / "built" / "packs" / EVENT / "overlays/cross_venue.json")
        q = query(self.root / "built", "episodes", event_id=EVENT, lens="cross_venue")
        self.assertEqual(len(q["rows"]), len(overlay["items"]))
        self.assertTrue(q["rows"])
        self.assertTrue(all("label" not in r and "classification" not in r for r in q["rows"]))
        self.assertEqual([r["episode_id"] for r in q["rows"]], [i["id"] for i in overlay["items"]])
        for item in overlay["items"]:
            self.assertEqual(item["start_ns"], item["detail"]["episode"]["start_ns"])
        with self.assertRaisesRegex(ValueError, "query"):
            query(self.root / "built", "episodes", limit=501)
        with self.assertRaisesRegex(ValueError, "query"):
            query(self.root / "built", "episodes", lens="' OR 1=1")

    def test_missing_profile_prevents_pack_not_verified_episode_records(self):
        runs = document(self.runs)
        del runs["events"][0]["runs"]["profile"]
        self.runs.write_bytes(encoded(runs))
        verify(self.config, self.runs, self.verification, store=self.store)
        self.build()
        events = query(self.root / "built", "events")["rows"]
        self.assertIsNone(events[0]["pack"])
        self.assertEqual(events[0]["pack_error"], "profile not_run")
        episodes = query(self.root / "built", "episodes", lens="cross_venue")["rows"]
        self.assertEqual(len(episodes), 3)
        self.assertEqual(next(l for l in events[0]["lenses"] if l["lens"] == "cross_venue")["episode_count"], 3)

    def test_rebound_unknown_manifest_version_is_not_verified(self):
        runs = document(self.runs)
        bench = Path(runs["events"][0]["runs"]["profile"])
        success = document(bench / "run/SUCCESS.json")
        output = bench / "run" / success["outputs"]["profile"]
        manifest = document(output / "manifest.json")
        manifest["version"] = 999
        (output / "manifest.json").write_bytes(encoded(manifest))
        receipt = document(output / "content_receipt.json")
        receipt["semantic_sha256"] = digest(manifest)
        (output / "content_receipt.json").write_bytes(encoded(receipt))
        result = document(bench / "result.json")
        result["groups"][0]["receipt"] = receipt
        (bench / "result.json").write_bytes(encoded(result))
        report = verify(self.config, self.runs, self.verification, store=self.store)
        lens = report["events"][0]["lenses"]["profile"]
        self.assertEqual(lens["state"], "failed")
        self.assertIn("manifest version", lens["error"])

    def test_verify_tamper_and_crash_never_commit_build_receipt(self):
        verify(self.config, self.runs, self.verification, store=self.store)
        bad = document(self.verification)
        bad["events"][0]["game_state"] = "no_source"
        self.verification.write_bytes(encoded(bad))
        with self.assertRaisesRegex(ValueError, "binding"):
            self.build()
        self.assertFalse((self.root / "built" / "receipt.json").exists())
        self.verification.unlink()
        verify(self.config, self.runs, self.verification, store=self.store)
        with patch("replay.research.build.build_charts", side_effect=OSError("crash")):
            with self.assertRaisesRegex(OSError, "crash"):
                self.build("crashed")
        self.assertFalse((self.root / "crashed" / "receipt.json").exists())

    def test_archive_input_tamper_during_build_never_commits(self):
        from archive.storage.base import VerificationFailure
        from replay.research.records import game_document
        report = verify(self.config, self.runs, self.verification, store=self.store)
        key = next(k for k in report["archive_inputs"][EVENT] if k.endswith("/timeline.v2.json"))
        def tamper(*args):
            result = game_document(*args)
            (self.root / "game-archive" / key).write_bytes(b"{}")
            return result
        with patch("replay.research.build.game_document", side_effect=tamper):
            with self.assertRaises(VerificationFailure):
                self.build()
        self.assertFalse((self.root / "built/receipt.json").exists())

    def test_complete_download_before_query_and_identity_tamper_rejected(self):
        verify(self.config, self.runs, self.verification, store=self.store)
        receipt = self.build()
        table = self.root / "built/episodes.parquet"
        target = LocalObjectStore(self.root / "table-store")
        from encoder import StoredIdentity
        expected = receipt["files"]["episodes.parquet"]
        with table.open("rb") as stream:
            target.put_immutable("episodes.parquet", stream, StoredIdentity(**expected),
                                 content_type="application/vnd.apache.parquet")
        download = self.root / "download.parquet"
        download_table(target, "episodes.parquet", expected, download)
        self.assertEqual(identity(download), expected)
        table.write_bytes(table.read_bytes()[:-1] + b"x")
        with self.assertRaisesRegex(ValueError, "identity"):
            query(self.root / "built", "episodes")
