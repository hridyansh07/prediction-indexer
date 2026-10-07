"""Package entry points through bench and real completed readers, fully offline."""

import importlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from replay import supervisor
from replay.bench.inside import execute, import_callable
from replay.bench.specs import supervisor_config
from replay.jobs.stages import _import_callable
from replay.preparation import encoded
from replay.strategies import canonical_reference, load_entrypoint
from replay.tests.test_bench import resolved, run_spec
from replay.tests.test_bundle_coverage import Harness as CoverageHarness
from replay.tests.test_cross_venue_arbitrage import Harness as CrossVenueHarness
from replay.tests.test_implication_cover import Harness as ImplicationHarness
from replay.tests.test_market_profile import Harness as ProfileHarness
from replay.tests.test_same_venue_complement import Harness as ComplementHarness
from replay.tests.test_same_venue_multi_market import Harness as MultiMarketHarness
from replay.tests.test_supervisor import config as supervisor_config_fixture, metadata_pin


CASES = (
    ("bundle_coverage", CoverageHarness, None, "replay.coverage_output"),
    ("same_venue_complement", ComplementHarness, "complement_config", "replay.complement_output"),
    ("cross_venue_arbitrage", CrossVenueHarness, "cross_config", "replay.cross_venue_output"),
    ("same_venue_multi_market", MultiMarketHarness, "multi_config", "replay.same_venue_multi_market_output"),
    ("same_venue_implication_cover", ImplicationHarness, "cross_config", "replay.same_venue_implication_cover"),
    ("cross_venue_implication_cover", ImplicationHarness, "cross_config", "replay.cross_venue_implication_cover"),
    ("market_profile", ProfileHarness, None, "replay.market_profile"),
)


class StrategyPackageTests(unittest.TestCase):
    def test_all_shipped_factories_and_readers_resolve_in_bench_and_jobs(self):
        for name, _, _, old_reader in CASES:
            package = importlib.import_module("replay.strategies." + name)
            for attribute, old_module in (("build", "replay." + name), ("read_completed", old_reader)):
                expected = getattr(package, attribute)
                for reference in (old_module + ":" + attribute,
                                  "replay.strategies." + name + ":" + attribute):
                    with self.subTest(reference=reference):
                        self.assertIs(load_entrypoint(reference), expected)
                        self.assertIs(import_callable(reference), expected)
                        self.assertIs(_import_callable(reference), expected)
        # Caller-owned strategy/check modules still use ordinary Python imports.
        self.assertIs(load_entrypoint("replay.tests.test_bench:context"),
                      import_callable("replay.tests.test_bench:context"))
        self.assertNotEqual(canonical_reference("replay.same_venue_implication_cover:build"),
                            canonical_reference("replay.cross_venue_implication_cover:build"))

    def test_bench_reads_every_package_and_historical_factory_without_rewriting_configs(self):
        for name, harness_class, config_attribute, old_reader in CASES:
            for historical in (False, True):
                with self.subTest(strategy=name, historical=historical), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    pin = metadata_pin()
                    kwargs = {"pin": pin}
                    if harness_class in (CrossVenueHarness, ImplicationHarness):
                        kwargs["native"] = False
                    if harness_class is ImplicationHarness:
                        kwargs["mode"] = "same_venue" if name.startswith("same_venue") else "cross_venue"
                    h = harness_class(root, **kwargs)
                    subject = h.profile if name == "market_profile" else h.strategy
                    config = getattr(h, config_attribute) if config_attribute else h.cfg
                    if name == "market_profile":
                        config = {**h.cfg, "policy": h.profile.policy}
                    factory = ("replay." if historical else "replay.strategies.") + name + ":build"
                    reader = (old_reader if historical else "replay.strategies." + name) + ":read_completed"
                    spec = run_spec(root, root / "context")
                    spec["groups"] = [{"name": "coverage", "factory": factory, "revision": "synthetic-test",
                                       "config": config, "reader": reader, "checks": [],
                                       "compare": {"rows": "rows", "key": ["id"]}}]
                    spec = resolved(spec)
                    spec["runtime"]["run_id"] = h.context["run_id"]
                    base = supervisor_config_fixture()
                    base["transport"].update(plans=h.initial["plans"], start_ns="10", end_ns="40", inputs=[pin])
                    spec["runtime"]["base_run_config"] = base
                    expected = supervisor_config(spec)
                    original = encoded(expected)
                    snapshot_bytes = (root / "context/context.json").read_bytes()

                    def complete(config, run_directory, redis_url):
                        # Only process launch is replaced. Real Decoder, strategy output,
                        # SUCCESS reader and strategy content readers run below.
                        self.assertEqual(encoded(config), original)
                        identity = supervisor.identity(config)
                        subject.binding["identity"] = identity
                        h.window()
                        h.finish()
                        attempt = h.context["attempt_id"]
                        participant = run_directory / attempt / "coverage"
                        participant.mkdir(parents=True)
                        output = h.profile_output if name == "market_profile" else h.output
                        output.rename(participant / "output")
                        terminal = h.seq
                        supervisor.write_json_durable(run_directory / "run.json", config)
                        supervisor.write_json_durable(participant / "complete.json", {
                            "version": 1, "identity": identity, "attempt": attempt,
                            "group": "coverage", "terminal": terminal})
                        supervisor.write_json_durable(participant.parent / "result.json", {
                            "version": 1, "identity": identity, "attempt": attempt,
                            "outcome": "success", "fatal": False, "progress": terminal,
                            "terminal": terminal, "participants": {"publisher": 0, "coverage": 0}})
                        supervisor.write_json_durable(run_directory / "SUCCESS.json", {
                            "version": 1, "identity": identity, "attempt": attempt, "terminal": terminal,
                            "outputs": {"coverage": attempt + "/coverage/output"}})
                        return supervisor.read_success(run_directory)

                    try:
                        with patch.object(supervisor, "_strict_metadata_preflight"):
                            code, result = execute(spec, root, redis_url="redis://unused",
                                                   supervisor_run=complete)
                        self.assertEqual((code, result["status"]), (0, "SUCCESS"), result["error"])
                        self.assertEqual(result["groups"][0]["factory"], factory)
                        self.assertEqual(supervisor.read(root / "run/run.json")["strategies"]["coverage"]["factory"], factory)
                        self.assertEqual((root / "context/context.json").read_bytes(), snapshot_bytes)
                        self.assertTrue((root / "groups/coverage/summary.json").is_file())
                        self.assertEqual(result["groups"][0]["receipt"]["identity"], supervisor.identity(expected))
                    finally:
                        writers = getattr(subject, "writers", {})
                        for writer in writers.values():
                            writer.stream.close()


if __name__ == "__main__":
    unittest.main()
