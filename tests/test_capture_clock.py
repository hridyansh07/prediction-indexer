from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from splices.common.clock import CaptureClock


class CaptureClockTests(unittest.TestCase):
    def test_linux_boot_scope_is_shared_but_lane_labelled(self) -> None:
        """Labelled by capture lane, not venue — Polymarket now runs two.

        The market channel and the sports channel are separate processes with
        separate spools, so a single `polymarket` label here would suggest their
        readings came from one clock source when they did not.
        """
        with tempfile.TemporaryDirectory() as directory:
            boot_id = Path(directory) / "boot_id"
            boot_id.write_text("11111111-2222-3333-4444-555555555555\n")
            lanes = ("polymarket", "polymarket_sports", "limitless", "kalshi")
            scopes = [
                CaptureClock(
                    lane,
                    platform_name="Linux",
                    boot_id_path=boot_id,
                    wall_ns=lambda: 10,
                    monotonic_ns=lambda: 5,
                ).scope
                for lane in lanes
            ]

        self.assertEqual({scope.scope_id for scope in scopes}, {"11111111-2222-3333-4444-555555555555"})
        self.assertEqual({scope.lane for scope in scopes}, set(lanes))
        self.assertTrue(all(scope.comparable_across_processes for scope in scopes))

    def test_non_linux_fallback_is_process_scoped(self) -> None:
        clock = CaptureClock(
            "polymarket",
            platform_name="Darwin",
            fallback_scope_id="test-process",
            wall_ns=lambda: 20,
            monotonic_ns=lambda: 7,
        )
        self.assertEqual(clock.scope.scope, "process")
        self.assertFalse(clock.scope.comparable_across_processes)
        self.assertEqual(clock.sample().visible_ns, 20)
        self.assertEqual(clock.sample().monotonic_ns, 7)


if __name__ == "__main__":
    unittest.main()
