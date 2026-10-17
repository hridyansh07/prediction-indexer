"""A failed participant leaves a bounded failure.json only when the bench opts in."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay import strategy_adapter
from replay.streams import TransportError


def run_main(root, error):
    def execute(*_):
        try:
            raise ValueError("detached state budget: requires 140000000 bytes")
        except ValueError as cause:
            raise error from cause
    with patch.object(strategy_adapter, "execute", side_effect=execute), \
         patch.object(strategy_adapter.signal, "signal"), \
         patch.object(strategy_adapter.sys, "argv", ["adapter", str(root), "a1", "cross"]):
        return strategy_adapter.main()


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "a1" / "cross").mkdir(parents=True)
        self.failure = self.root / "a1" / "cross" / "failure.json"

    def test_off_by_default_keeps_the_supervisor_contract(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(strategy_adapter.DIAGNOSTICS_ENV, None)
            self.assertEqual(run_main(self.root, strategy_adapter.HookFailure()), 20)
        self.assertFalse(self.failure.exists())

    def test_fatal_failure_records_the_cause_chain_and_keeps_the_exit_code(self):
        with patch.dict(os.environ, {strategy_adapter.DIAGNOSTICS_ENV: "1"}):
            self.assertEqual(run_main(self.root, strategy_adapter.HookFailure()), 20)
        document = json.loads(self.failure.read_bytes())
        self.assertEqual(document["exit"], 20)
        self.assertEqual([e["type"] for e in document["errors"]], ["HookFailure", "ValueError"])
        self.assertIn("detached state budget", document["errors"][1]["message"])
        self.assertLessEqual(len(document["traceback"]), strategy_adapter.MAX_DIAGNOSTIC_CHARS)

    def test_transport_failure_is_retryable_and_still_diagnosed(self):
        with patch.dict(os.environ, {strategy_adapter.DIAGNOSTICS_ENV: "1"}):
            self.assertEqual(run_main(self.root, TransportError("redis gone")), 21)
        self.assertEqual(json.loads(self.failure.read_bytes())["exit"], 21)

    def test_an_unwritable_directory_never_changes_the_exit_code(self):
        with patch.dict(os.environ, {strategy_adapter.DIAGNOSTICS_ENV: "1"}):
            self.assertEqual(run_main(self.root / "missing", strategy_adapter.HookFailure()), 20)


if __name__ == "__main__":
    unittest.main()
