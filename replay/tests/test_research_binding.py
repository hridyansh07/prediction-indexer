import ast
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.research.io import document, write
from replay.research.verify.runs import bound_report


class ResearchBindingTests(unittest.TestCase):
    def test_build_recomputes_checks_and_rejects_report_edit_or_input_change(self):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "verify.json"
            original = {"research_verify_version": 1, "inputs": {"context": {"sha256": "a" * 64, "byte_length": 17}},
                        "archive_inputs": {}, "events": [{"lenses": {"profile": {"state": "ok"}}}]}
            write(report_path, original)
            with patch("replay.research.verify.runs.inspect", return_value=(original, {}, {})):
                self.assertEqual(bound_report("config", "runs", report_path)[0], original)
            changed = {**original, "inputs": {"context": {"sha256": "b" * 64, "byte_length": 17}}}
            with patch("replay.research.verify.runs.inspect", return_value=(changed, {}, {})):
                with self.assertRaisesRegex(ValueError, "exact-input"):
                    bound_report("config", "runs", report_path)
            changed = {**original, "events": [{"lenses": {"profile": {"state": "failed"}}}]}
            with patch("replay.research.verify.runs.inspect", return_value=(changed, {}, {})):
                with self.assertRaisesRegex(ValueError, "check binding"):
                    bound_report("config", "runs", report_path)

    def test_existing_temporary_or_commit_is_never_changed_and_crash_has_no_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            p = Path(directory) / "verify.json"
            temporary = p.with_name("verify.json.open")
            temporary.write_bytes(b"existing evidence")
            with self.assertRaises(FileExistsError):
                write(p, {"a": 1})
            self.assertEqual(temporary.read_bytes(), b"existing evidence")
            temporary.unlink()
            with patch("replay.research.io.fsync_directory", side_effect=OSError("crash")):
                with self.assertRaisesRegex(OSError, "crash"):
                    write(p, {"a": 1})
            self.assertFalse(p.exists())
            write(p, {"a": 2})
            with self.assertRaises(FileExistsError):
                write(p, {"a": 3})
            self.assertEqual(document(p), {"a": 2})

    def test_verifiers_do_not_import_writers_runtime_or_readers_under_test(self):
        root = Path(__file__).resolve().parents[1] / "research"
        forbidden = ("replay.economic_sdk.reader", "replay.economic_sdk.aggregate_reader",
                     "replay.economic_sdk.profile", "replay.economic_sdk.profile_reader",
                     "replay.economic_sdk.transitions_reader", "replay.economic_sdk.runtime")
        for path in [root / "inputs.py", *(root / "verify").glob("*.py")]:
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn(node.module, forbidden, str(path))
                    self.assertFalse(node.module and node.module.endswith((".strategy", ".output")), str(path))
