import json
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from replay.research import build as builder
from replay.research.config import load
from replay.research.jsonio import ResearchError, file_identity, read_json
from replay.tests.research_fixtures import KALSHI, corpus_result

CONFIG_PATH = Path(__file__).resolve().parents[2] / "configs" / "research.json"
CONFIG = load(CONFIG_PATH)
OK, BARE, NOPROF, LOOSE = ("bundle_" + c * 24 for c in "1234")


class BuildTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls._tmp.name)
        cls.key = cls.root / "results" / "aaaaaaaaaaaaaaaa"
        cls.key.mkdir(parents=True)
        corpus_result(cls.key, OK)
        corpus_result(cls.key, BARE, context=False)
        corpus_result(cls.key, NOPROF, excluded=("profile",), groups=("cross",))
        corpus_result(cls.key, LOOSE, commit=False)
        cls.out = cls.root / "out"
        cls.version, cls.failures = cls.build(cls.out)
        cls.base = cls.out / cls.version

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    @classmethod
    def build(cls, out):
        return builder.build([cls.key], out, CONFIG, config_bytes=CONFIG_PATH.read_bytes(), log=lambda line: None)

    def test_pointer_and_manifest_identities(self):
        self.assertEqual(self.failures, [])
        pointer = read_json(self.out / "current.json")
        self.assertEqual(pointer["version"], self.version)
        manifest_path = self.out / pointer["manifest_key"]
        self.assertEqual(file_identity(manifest_path), {"sha256": pointer["manifest_sha256"],
                                                        "byte_length": pointer["manifest_byte_length"]})
        manifest = read_json(manifest_path)
        for name, ident in manifest["files"].items():
            self.assertEqual(file_identity(self.base / name), ident, name)
        self.assertEqual(sorted(manifest["events"]), sorted([BARE, OK]))
        for bundle, entry in manifest["events"].items():
            pack_manifest = self.base / entry["pack"]
            self.assertEqual(file_identity(pack_manifest), {k: entry[k] for k in ("sha256", "byte_length")})
            pack = read_json(pack_manifest)
            for name, ident in pack["files"].items():
                self.assertEqual(file_identity(pack_manifest.parent / name), ident, name)
        self.assertEqual(len(manifest["version"]), 16)

    def test_no_local_path_is_published(self):
        needle = str(self.root).encode()
        for path in self.base.rglob("*"):
            if path.is_file():
                self.assertNotIn(needle, path.read_bytes(), path)

    def test_pack_with_context_has_labels_and_twins(self):
        pack = read_json(self.base / "events" / OK / "manifest.json")
        self.assertEqual((pack["title"], pack["game"], pack["context"]), ("Cloud9 vs LYON", "league_of_legends", "ok"))
        self.assertEqual([b["label"] for b in pack["books"]],
                         ["series_moneyline · Cloud9", "series_moneyline · Cloud9", "series_moneyline · LYON"])
        self.assertEqual(pack["books"][0]["claim_id"], pack["books"][1]["claim_id"])
        self.assertNotEqual(pack["books"][1]["claim_id"], pack["books"][2]["claim_id"])
        self.assertEqual(pack["lenses"]["cross"]["state"], "ok")
        self.assertEqual(pack["lenses"]["cross"]["episodes"], 4)
        self.assertEqual(pack["lenses"]["complement"], {"label": "Same-venue complement", "state": "not_run"})
        self.assertEqual(pack["unmapped_groups"], ["coverage"])
        self.assertEqual(pack["game_state"]["state"], "no_source")
        self.assertEqual(pack["tiles"]["1s"], ["0", "10", "20"])
        availability = read_json(self.base / "events" / OK / "availability.json")
        self.assertEqual([len(b["intervals"]) for b in availability["books"]], [2, 0, 0])
        self.assertEqual(availability["books"][0]["intervals"][1]["reason"], "connection_closed")

    def test_degrade_path_without_context(self):
        pack = read_json(self.base / "events" / BARE / "manifest.json")
        self.assertEqual((pack["title"], pack["game"], pack["context"], pack["event_id"]), (BARE, None, "missing", None))
        self.assertEqual(pack["books"][0]["label"], KALSHI)
        self.assertTrue(all(b["claim_id"] is None for b in pack["books"]))

    def test_events_table_lists_every_result(self):
        rows = {r["bundle_id"]: r for r in pq.read_table(self.base / "events.parquet").to_pylist()}
        self.assertEqual({b: (r["state"], r["pack"]) for b, r in rows.items()},
                         {OK: ("ok", "ok"), BARE: ("ok", "ok"), NOPROF: ("ok", "no_profile"),
                          LOOSE: ("uncommitted", None)})
        self.assertEqual(rows[NOPROF]["profile_state"], "excluded")
        self.assertEqual(rows[OK]["cross_episodes"], 4)
        self.assertEqual(rows[OK]["cross_headline"], 3)
        self.assertEqual(rows[OK]["cross_max_net"], 3.05)
        self.assertEqual(rows[OK]["capture_start_ns"], 1_791_000_000 * 10 ** 9)

    def test_episodes_table_and_summary(self):
        table = pq.read_table(self.base / "episodes.parquet")
        self.assertEqual(table.schema.names, [f.name for f in builder.tables.EPISODE_SCHEMA])
        self.assertEqual(table.num_rows, 8)
        summary = read_json(self.base / "summary.json")
        cross = summary["lenses"]["cross"]
        self.assertEqual(cross["states"], {"ok": 3, "failed": 1, "excluded": 0, "not_run": 0})
        self.assertEqual(cross["all"]["events"], 2)
        self.assertEqual(cross["all"]["episodes"], 6)
        self.assertEqual(cross["all"]["dedup_episodes"], 4)
        self.assertEqual(cross["min_100ms"]["episodes"], 4)
        self.assertAlmostEqual(cross["all"]["net_total"], 2 * 3.14)
        self.assertEqual(summary["events"], {"ok": 3, "uncommitted": 1, "receipt_invalid": 0, "context_invalid": 0})
        report = (self.base / "viability.md").read_text()
        self.assertIn(self.version, report)
        self.assertIn(f"`{LOOSE}`: state `uncommitted`", report)
        self.assertIn("excluded: entities.json over 8 MiB", report)

    def test_events_index_matches_the_events_table(self):
        index = read_json(self.base / "events.json")
        table = pq.read_table(self.base / "events.parquet").to_pylist()
        self.assertEqual([e["bundle_id"] for e in index["events"]], [r["bundle_id"] for r in table])
        self.assertEqual(index["events"][0]["capture_start_ns"], str(table[0]["capture_start_ns"]))
        self.assertEqual([lens["lens"] for lens in index["lenses"]], list(CONFIG["lenses"]))
        self.assertIn("events.json", read_json(self.base / "manifest.json")["files"])

    def test_verify_json(self):
        checks = read_json(self.base / "verify.json")["checks"]
        self.assertIn({"bundle_id": LOOSE, "check": "corpus_receipt", "pass": False,
                       "message": "no corpus_receipt.json"}, checks)
        self.assertIn({"bundle_id": BARE, "check": "context_binding", "pass": True, "message": "missing"}, checks)

    def test_build_is_deterministic_and_reuses_an_identical_version(self):
        other = self.root / "out2"
        version, _ = self.build(other)
        self.assertEqual(version, self.version)
        mine = {p.relative_to(self.base): p.read_bytes() for p in self.base.rglob("*") if p.is_file()}
        theirs = {p.relative_to(other / version): p.read_bytes() for p in (other / version).rglob("*") if p.is_file()}
        self.assertEqual(mine, theirs)
        self.assertEqual(self.build(other)[0], version)
        self.assertEqual(sorted(p.name for p in other.iterdir()), sorted(["current.json", version]))

    def test_refuses_to_rewrite_a_differing_version(self):
        other = self.root / "out3"
        version, _ = self.build(other)
        overview = other / version / "events" / OK / "overview.json"
        overview.write_bytes(overview.read_bytes().replace(b"4100", b"4101"))
        with self.assertRaises(ResearchError):
            self.build(other)
        self.assertEqual(read_json(other / "current.json")["version"], version)


class PackFailureTest(unittest.TestCase):
    def test_a_pack_that_cannot_build_is_recorded_not_dropped(self):
        with tempfile.TemporaryDirectory() as directory:
            key = Path(directory) / "aaaaaaaaaaaaaaaa"
            key.mkdir()
            corpus_result(key, OK)
            # A committed result cannot be edited without breaking its receipt, so the
            # pack step is made to fail directly.
            build_root = Path(directory) / "out"
            original = builder.series.profile_tables
            builder.series.profile_tables = lambda output: (_ for _ in ()).throw(ResearchError("no transitions"))
            try:
                version, failures = builder.build([key], build_root, CONFIG, config_bytes=b"{}", log=lambda l: None)
            finally:
                builder.series.profile_tables = original
            self.assertEqual(failures, [(OK, "ResearchError: no transitions")])
            rows = pq.read_table(build_root / version / "events.parquet").to_pylist()
            self.assertEqual((rows[0]["pack"], rows[0]["pack_error"]), ("build_failed", "ResearchError: no transitions"))
            self.assertFalse((build_root / version / "events").exists())
            manifest = json.loads((build_root / version / "manifest.json").read_bytes())
            self.assertEqual(manifest["events"], {})


if __name__ == "__main__":
    unittest.main()
