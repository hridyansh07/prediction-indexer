"""Fast offline output pins for declared corpus semantics.

After reviewing an intentional output change and bumping its declared version,
print the replacement table with:
python -c 'import json; from replay.tests.test_strategy_versions import observed_versions; print(json.dumps(observed_versions(), indent=2, sort_keys=True))'
"""

import hashlib
import importlib
import json
from pathlib import Path
import tempfile
import unittest

from replay import semantics
from replay.tests.economic_scenarios import M, ladder
from replay.tests.test_strategy_packages import CASES


def observed_versions():
    """Reuse each package's existing smallest nonempty deterministic tape and reader."""
    observed = {}
    for name, harness_class, _, _ in CASES:
        package = "replay.strategies." + name
        kwargs = {}
        if name == "same_venue_complement":
            kwargs["known"] = True
        elif name.endswith("implication_cover"):
            kwargs["mode"] = "same_venue" if name.startswith("same_venue") else "cross_venue"
            kwargs["policy"] = {"audit_intervals": True}
        elif name == "cross_venue_arbitrage":
            kwargs["policy"] = {"audit_intervals": True}
        elif name == "market_profile":
            kwargs["mixed"] = True
        with tempfile.TemporaryDirectory() as tmp:
            h = harness_class(Path(tmp), **kwargs)
            try:
                if name == "bundle_coverage":
                    # test_delayed_initialization_trades_quiet_fault_and_recovery
                    h.window()
                    h.group(11, trades=("observed", "duplicate"))
                    h.group(14, h.initial["plans"][:1])
                    h.group(17, h.initial["plans"][1:])
                    h.group(19, trades=("observed",))
                    h.group(20)
                    h.group(28, h.initial["plans"][:1], {"kind": "quantity_underflow"})
                    h.group(31, h.initial["plans"][:1])
                elif name == "same_venue_complement":
                    # test_unknown_positive_is_inconclusive_known_zero_is_present
                    h.window()
                    h.quote(12, 0)
                    h.quote(12, 1)
                elif name == "same_venue_multi_market":
                    # test_edge_fill_walks_every_leg_and_the_reader_rechecks_it
                    h.window()
                    h.kalshi_ask(12, "series", "outcome", (45, 3), (50, 10))
                    h.kalshi_ask(12, "beta", "outcome", (50, 5))
                elif name == "market_profile":
                    # test_time_partitions_sum_to_each_clipped_bucket
                    h.window()
                    ladder(h, 12, "polymarket:123", bids=((400, 2 * M),), asks=((450, M),))
                    ladder(h, 16, "polymarket:123", bids=(), asks=((450, M),))
                    ladder(h, 19, "polymarket:123", why={"kind": "connection_closed"})
                else:
                    # Cross-venue native scales and both implication modes.
                    h.populate()
                h.finish()  # the existing independent reader validates all output
                output = h.profile_output if name == "market_profile" else h.output
                files = {p.relative_to(output).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                         for p in sorted(output.rglob("*")) if p.is_file()}
                digest = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                observed[package] = {"version": importlib.import_module(package).STRATEGY_VERSION, "digest": digest}
            finally:
                subject = h.profile if name == "market_profile" else h.strategy
                writers = list(getattr(subject, "writers", {}).values())
                if hasattr(subject, "writer"):
                    writers.append(subject.writer)
                for writer in writers:
                    writer.stream.close()
    combined = {package: row["digest"] for package, row in observed.items()}
    observed["replay_semantics"] = {"version": semantics.REPLAY_SEMANTICS_VERSION,
        "digest": hashlib.sha256(json.dumps(combined, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
    return observed


class StrategyVersionTests(unittest.TestCase):
    def test_declared_versions_and_outputs_match_digest_table(self):
        expected = json.loads(Path(__file__).with_name("strategy_version_digests.json").read_bytes())
        observed = observed_versions()
        self.assertEqual(set(expected), set(observed), "update the digest table for the strategy packages")
        for package, row in observed.items():
            with self.subTest(package=package):
                self.assertEqual(row["version"], expected[package]["version"],
                                 f"{package}: update the digest table after a version bump")
                bump = ("REPLAY_SEMANTICS_VERSION" if package == "replay_semantics" else
                        "STRATEGY_VERSION (or REPLAY_SEMANTICS_VERSION for shared-layer changes)")
                self.assertEqual(row["digest"], expected[package]["digest"],
                                 f"outputs of {package} changed: bump {bump} and update the digest table")


if __name__ == "__main__":
    unittest.main()
