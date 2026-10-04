import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.economic_fills import Fill, retained_size
from replay.economic_intervals import CutClock
from replay.same_venue_complement import _deep_size
from replay.streams.protocol import ProtocolError
from replay.tests.test_same_venue_complement import Harness


class _Cut:
    kind = "cut"

    def __init__(self, origin):
        self.body = {"origin": origin}


class ComplementRuntimeBoundTests(unittest.TestCase):
    def test_deep_size_traverses_ordinary_and_slotted_instances(self):
        class Ordinary:
            pass

        class Slotted:
            __slots__ = ("payload",)

        ordinary = Ordinary()
        ordinary.payload = bytearray(4096)
        slotted = Slotted()
        slotted.payload = bytearray(4096)
        self.assertGreater(_deep_size(ordinary), 4096)
        self.assertGreater(_deep_size(slotted), 4096)

    def test_fixed_inputs_and_resolver_capacity_are_inside_state_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True)
            self.assertIsNone(h.strategy.inputs.prepared.snapshot)
            self.assertGreaterEqual(
                h.strategy.state_bytes,
                h.strategy._RESOLVER_CACHE_RESERVATION + _deep_size(h.strategy.inputs),
            )

    def test_episode_replacement_checks_bound_before_retained_mutation(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True)
            h.window(); h.quote(12, 0); h.quote(12, 1); h.quote(13, 0)
            h.strategy._flush_stage()
            key = next(iter(h.strategy.episodes))
            before = h.strategy.episodes[key]
            economic = h.strategy._evaluate(key[0])["economic"]
            with patch("replay.same_venue_complement.MAX_STATE", h.strategy.state_bytes - 1):
                with self.assertRaisesRegex(ProtocolError, "detached state budget"):
                    h.strategy._update_episode(before, 14, economic, 0)
            self.assertIs(h.strategy.episodes[key], before)

    def test_fill_cost_cache_is_stage_local(self):
        fill = Fill(1, 2, False, ((3, 4),), ((3, 5),))
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True)
            before = len(h.strategy._stage_fill_costs)
            h.strategy._entry_cost("one", {"fill": fill})
            h.strategy._entry_cost("two", {"fill": fill})
            self.assertEqual(len(h.strategy._stage_fill_costs), before + 1)
            h.strategy.staged_time = h.strategy.clock.start
            h.strategy._flush_stage()
            self.assertEqual(h.strategy._stage_fill_costs, {})

    def test_fill_schema_size_is_conservative_and_cached(self):
        fill = Fill(10**30, 10**40, False, ((3, 4), (5, 6)), ((3, 7), (5, 8)))
        generic = _deep_size(fill)
        self.assertGreaterEqual(retained_size(fill), generic)
        cache = {}
        self.assertEqual(_deep_size(fill, fill_costs=cache), retained_size(fill))
        self.assertEqual(len(cache), 1)

    def test_kalshi_projection_reuses_projected_fills_across_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True, mixed=True)
            kalshi = [i for i, plan in enumerate(h.initial["plans"])
                      if plan["venue"] == "kalshi"]
            h.window(); h.quote(12, kalshi[0]); h.quote(12, kalshi[1])
            entity = next(e for e, desc in h.strategy.layout.items()
                          if desc["venue"] == "kalshi" and desc["size_contracts"] == "1")
            dependencies = h.strategy._dependencies(h.strategy.layout[entity])
            first = [h.strategy._fill(key, "bid", 1, True) for key in dependencies]
            second = [h.strategy._fill(key, "bid", 1, True) for key in dependencies]
            self.assertIs(first[0], second[0])
            self.assertIs(first[1], second[1])

    def test_nonpositive_gross_retains_measurement_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True)
            h.window(); h.quote(12, 0, ask=600); h.quote(12, 1, ask=600)
            entity = next(e for e, desc in h.strategy.layout.items()
                          if desc["venue"] == "polymarket" and desc["direction"] == "long"
                          and desc["size_contracts"] == "1" and not desc["placebo"])
            value = h.strategy._evaluate(entity)
            self.assertEqual(value["value_class"], "GROSS_NONPOSITIVE")
            self.assertNotIn("economic", value)

    def test_cut_clock_retains_only_bounded_window_fields(self):
        snapshot = {"config": {"start_ns": "10", "end_ns": "40", "lower_bound": "requested"},
                    "scopes": [{"end_ns": "40"}]}
        clock = CutClock(snapshot)
        origin = {"kind": "window", "start_ns": "0", "end_ns": "40", "pin": "p",
                  "large_callback_owned_field": [object()] * 10_000}
        clock.observe(_Cut(origin))
        self.assertEqual(clock.window, ("0", "40", "p"))
        self.assertNotIn(origin["large_callback_owned_field"], clock.window)

    def test_hot_path_does_not_rescan_staged_mapping_and_drops_reader_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True)
            original = __import__("replay.same_venue_complement", fromlist=["_deep_size"])._deep_size

            def bounded(value, seen=None, fill_costs=None):
                if value is h.strategy.staged_values:
                    raise AssertionError("whole staged mapping scanned")
                return original(value, seen, fill_costs)

            with patch("replay.same_venue_complement._deep_size", side_effect=bounded):
                h.window()
                h.quote(12, 0)
                h.quote(12, 1)

            import replay.complement_output as output
            validate = output.validate_content
            calls = 0

            def inspect_release(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    self.assertEqual(h.strategy.projections, {})
                    self.assertEqual(h.strategy.episodes, {})
                    self.assertEqual(h.strategy.open_measurements, {})
                    self.assertEqual(h.strategy.layout, {})
                return validate(*args, **kwargs)

            with patch.object(output, "validate_content", side_effect=inspect_release):
                h.finish()

    def test_scope_keeps_no_historical_layout_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            h = Harness(Path(tmp), known=True, scopes=True)
            self.assertFalse(hasattr(h.strategy, "layout_cache"))
            h.window()
            h.quote(12, 0)
            h.quote(12, 1)
            h.quote(29, 0)
            h.finish()


if __name__ == "__main__":
    unittest.main()
