"""Package entry points through bench and real completed readers, fully offline."""

from copy import deepcopy
import importlib
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from replay import supervisor
from replay.bench.inside import execute, import_callable
from replay.bench.specs import supervisor_config
from replay.economic_sdk.completed_reader import read_completed
from replay.jobs.stages import _import_callable
from replay.preparation import digest, encoded
from replay.strategies import canonical_reference, load_entrypoint
from replay.streams.protocol import ProtocolError
from replay.tests.economic_scenarios import PROFILE_POLICY, v2_policy
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
                    if not historical and name == "same_venue_complement":
                        kwargs["policy"] = v2_policy(controls=[{"kind": "time_shift", "shift_ns": ["5"]}],
                            controls_episodes=True, controls_slices=True, audit_intervals=True, profile=PROFILE_POLICY)
                    if not historical and name == "market_profile":
                        from replay.tests.test_market_volatility import policy
                        kwargs["policy"] = policy()
                        kwargs["policy"]["groups"] = sorted(kwargs["policy"]["groups"] + ["levels", "availability"])
                        kwargs["storage"] = {"version": 1, "profile": "zstd"}
                    h = harness_class(root, **kwargs)
                    subject = h.profile if name == "market_profile" else h.strategy
                    config = getattr(h, config_attribute) if config_attribute else h.cfg
                    if name == "market_profile":
                        config = {**h.cfg, "policy": h.profile.policy}
                        if h.profile.storage is not None:
                            config["storage"] = h.profile.storage
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
                        run = root / "run"
                        package_reader = load_entrypoint(reader)
                        with patch.object(supervisor, "_strict_metadata_preflight"):
                            original_result = package_reader(run, "coverage")
                            self.assertEqual(read_completed(run, "coverage", reader=package_reader, deep=True), original_result)
                            bound = read_completed(run, "coverage", reader=package_reader)
                            self.assertEqual(encoded(bound), encoded(original_result))
                            for key in ("receipt", "summary"):
                                self.assertEqual((root / f"groups/coverage/{key}.json").read_bytes(),
                                                 encoded(original_result[key]) + b"\n")
                            output = run / ("a" * 32) / "coverage/output"
                            saved = {p: p.read_bytes() for p in output.rglob("*") if p.is_file()}
                            for path, raw in saved.items():
                                with self.subTest(file=path.relative_to(output).as_posix()):
                                    # Same length changes must fail, not just truncation.
                                    path.write_bytes(bytes([raw[0] ^ 1]) + raw[1:] if raw else b"x")
                                    try:
                                        with self.assertRaises(ProtocolError):
                                            read_completed(run, "coverage", reader=package_reader)
                                    finally:
                                        path.write_bytes(raw)
                            self.assertEqual({p: p.read_bytes() for p in saved}, saved)
                            # Canonical semantic JSON alone would miss this byte edit.
                            path = output / "summary.json"
                            raw = path.read_bytes()
                            path.write_bytes(raw.replace(b" ", b"\t", 1))
                            try:
                                with self.assertRaisesRegex(ProtocolError, "output metadata encoding mismatch"):
                                    read_completed(run, "coverage", reader=package_reader)
                            finally:
                                path.write_bytes(raw)
                            if not historical:
                                path = output / "content_receipt.json"
                                raw = path.read_bytes()
                                for field, value in (("version", 0), ("semantic_sha256", "0" * 64),
                                                     ("terminal", original_result["receipt"]["terminal"] + 1)):
                                    with self.subTest(binding=field):
                                        supervisor.write_json_durable(path, {**original_result["receipt"], field: value})
                                        try:
                                            with self.assertRaises(ProtocolError) as direct:
                                                package_reader(run, "coverage")
                                            with self.assertRaises(ProtocolError) as deep:
                                                read_completed(run, "coverage", reader=package_reader, deep=True)
                                            with self.assertRaises(ProtocolError) as bound_error:
                                                read_completed(run, "coverage", reader=package_reader)
                                            self.assertEqual(str(deep.exception), str(direct.exception))
                                            self.assertEqual(str(bound_error.exception), str(direct.exception))
                                        finally:
                                            path.write_bytes(raw)
                                files = original_result["manifest"].get("files", {"intervals.ndjson": {}})
                                path = next(output / n for n in files if saved[output / n])
                                raw = saved[path]
                                for mutation, message in ((raw + b"x", "output file length mismatch"),
                                                          (bytes([raw[0] ^ 1]) + raw[1:], "output file SHA-256 mismatch")):
                                    path.write_bytes(mutation)
                                    try:
                                        with self.assertRaisesRegex(ProtocolError, message):
                                            read_completed(run, "coverage", reader=package_reader)
                                    finally:
                                        path.write_bytes(raw)
                            if name == "market_profile" and not historical:
                                from replay.strategies.market_profile.strategy import profile_identity
                                # The physical frame is verified by the generic byte-tamper loop above.
                                self.assertIn("profile.ndjson.zst", original_result["manifest"]["files"])
                                mismatch = deepcopy(expected)
                                mismatch["strategies"]["coverage"]["config"].pop("storage")
                                new_identity = supervisor.identity(mismatch)
                                bindings = [run / "SUCCESS.json", run / ("a" * 32) / "result.json",
                                            output.parent / "complete.json", output / "content_receipt.json"]
                                originals = {p: p.read_bytes() for p in [run / "run.json", *bindings]}
                                supervisor.write_json_durable(run / "run.json", mismatch)
                                for path in bindings:
                                    supervisor.write_json_durable(path, {**supervisor.read(path), "identity": new_identity})
                                try:
                                    # Every supervisor and content binding is valid; only storage intent differs.
                                    with self.assertRaisesRegex(ProtocolError, "configured storage"):
                                        package_reader(run, "coverage")
                                    with self.assertRaisesRegex(ProtocolError, "configured storage"):
                                        read_completed(run, "coverage", reader=package_reader)
                                finally:
                                    for path, raw in originals.items():
                                        path.write_bytes(raw)
                                path = output / "content_receipt.json"
                                supervisor.write_json_durable(path, {**original_result["receipt"],
                                    "terminal": float(original_result["receipt"]["terminal"])})
                                try:
                                    # Python numeric equality must not hide an edited receipt.
                                    with self.assertRaisesRegex(ProtocolError, "supervisor/content binding"):
                                        read_completed(run, "coverage", reader=package_reader)
                                finally:
                                    path.write_bytes(saved[path])
                                manifest = deepcopy(original_result["manifest"])
                                manifest["policy"]["bucket_ns"] = str(int(manifest["policy"]["bucket_ns"]) + 1)
                                manifest["experiment_sha256"] = profile_identity(h.sha, manifest["policy"])
                                receipt = {**original_result["receipt"], "semantic_sha256": digest(manifest)}
                                supervisor.write_json_durable(output / "manifest.json", manifest)
                                supervisor.write_json_durable(output / "content_receipt.json", receipt)
                                try:
                                    # All digests bind correctly; only the configured policy differs.
                                    with self.assertRaisesRegex(ProtocolError, "configured policy"):
                                        read_completed(run, "coverage", reader=package_reader)
                                finally:
                                    for path in (output / "manifest.json", output / "content_receipt.json"):
                                        path.write_bytes(saved[path])
                    finally:
                        writers = getattr(subject, "writers", {})
                        for writer in writers.values():
                            writer.stream.close()


if __name__ == "__main__":
    unittest.main()
