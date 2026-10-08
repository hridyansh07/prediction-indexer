import sys
import unittest
from replay.economic_sdk import bounds
from replay.economic_sdk.reader import Budget
from replay.bench.specs import LIMITS
from replay.streams.protocol import ProtocolError


class StateLimitTests(unittest.TestCase):
    def test_55_scope_retained_shape_requires_explicit_larger_budget(self):
        # Equal but separately retained JSON members must not be value-deduped.
        snapshot = {"scopes": [{"members": [
            {"market": "x" * 1000, "books": ["y" * 1000 for _ in range(8)]}
            for _ in range(100)]} for _ in range(55)]}
        with self.assertRaisesRegex(ProtocolError, "state budget"):
            Budget(snapshot)
        budget = Budget(snapshot, limit=1024**3)
        seen = set()
        def size(value):
            if id(value) in seen:
                return 0
            seen.add(id(value))
            children = (list(value.keys()) + list(value.values()) if isinstance(value, dict)
                        else value if isinstance(value, (list, tuple)) else [])
            return sys.getsizeof(value) + sum(size(child) for child in children)
        self.assertGreaterEqual(budget.used, size(snapshot))

    def test_runtime_budget_precedes_output_and_does_not_change_semantic_identity(self):
        import tempfile
        from pathlib import Path
        from dataclasses import replace
        from replay.tests.test_cross_venue_metadata import WideHarness
        from replay.economic_sdk.runtime import Runtime
        with tempfile.TemporaryDirectory() as temporary:
            h = WideHarness(Path(temporary), scope_count=55)
            try:
                strategy = h.strategy.strategy
                strategy.experiment = replace(strategy.experiment, static_reservation=128 * 1024**2)
                identity = strategy.experiment.experiment_sha256
                output = Path(temporary) / "budget-output"
                output.mkdir()
                context = {**h.context, "output_directory": str(output)}
                with self.assertRaisesRegex(ProtocolError, "detached state budget"):
                    Runtime(strategy, context)
                self.assertEqual(list(output.iterdir()), [])
                runtime = Runtime(strategy, {**context, "limits": {"state_bytes": 1024**3}})
                self.assertEqual(runtime.experiment.experiment_sha256, identity)
                for writer in runtime.writers.values():
                    writer.stream.close()
            finally:
                h.close()

    def test_limit_is_closed_positive_integer_and_bench_accepts_override(self):
        for bad in (True, 0, -1, 1.5):
            with self.assertRaises(ProtocolError):
                bounds.StateBudget(bad)
        self.assertIn("state_bytes", LIMITS)
