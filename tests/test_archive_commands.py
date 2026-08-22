"""§8.1 and §8.2 — what the two commands do, and what they refuse to do.

The library is where archival and reaping are proven; this is where the
*deployment* gate is proven. §5.3 draws that line deliberately: a test may point
the reaper at a temporary backend, and the CLI keeps the durability gate anyway,
because the thing being guarded against is a compression probe on the capture
disk quietly becoming deletion authority.
"""

from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from archive.archiver import cli as run_archiver
from archive.reaper import cli as run_reaper
from archive.archiver import Archiver
from archive.storage.local import LocalObjectStore
from encoder import stored_identity_of
from tests.archive_fixtures import canonical_input_for, write_canonical_receipt, write_sealed_segment


def run(main, arguments: list[str]) -> tuple[int, dict]:
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        status = main(arguments)
    output = captured.getvalue().strip().splitlines()
    return status, json.loads(output[-1]) if output else {}


class CommandCase(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.spool = self.root / "spool"
        self.canonical = self.root / "canonical"
        self.archive_root = self.root / "archive"
        self.segment = write_sealed_segment(self.spool)
        self.seal = self.segment.with_name(self.segment.name[: -len(".ndjson")] + ".seal.json")

    def archiver_arguments(self, *extra: str) -> list[str]:
        return [
            "--spool-root",
            str(self.spool),
            "--archive-root",
            str(self.archive_root),
            *extra,
        ]

    def reaper_arguments(self, *extra: str) -> list[str]:
        return [
            "--spool-root",
            str(self.spool),
            "--canonical-root",
            str(self.canonical),
            "--archive-root",
            str(self.archive_root),
            *extra,
        ]


class ArchiverCommandTests(CommandCase):
    def test_interval_must_be_positive(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must be positive"):
            run(run_archiver.main, self.archiver_arguments("--interval-seconds", "0"))

    def test_a_one_shot_sweep_reports_structured_counts(self) -> None:
        status, record = run(run_archiver.main, self.archiver_arguments())
        self.assertEqual(status, run_archiver.EXIT_OK)
        self.assertEqual(record["counts"]["archived"], 1)
        self.assertEqual(record["archive"]["durability"], "local_conformance")
        self.assertEqual(record["archive"]["receipt_kind"], "local")

    def test_the_same_sweep_archives_committed_canonical_windows(self) -> None:
        source_receipt = write_canonical_receipt(self.canonical, evidence_lines=1)
        status, record = run(
            run_archiver.main,
            self.archiver_arguments("--canonical-root", str(self.canonical)),
        )
        self.assertEqual(status, run_archiver.EXIT_OK)
        self.assertEqual(record["canonical"]["counts"]["archived"], 1)
        self.assertTrue(
            source_receipt.with_name("canonical_archive_receipt.local.json").is_file()
        )

    def test_the_sweep_writes_daily_manifests_when_asked(self) -> None:
        manifests = self.root / "manifests"
        status, record = run(
            run_archiver.main, self.archiver_arguments("--manifest-root", str(manifests))
        )
        self.assertEqual(status, run_archiver.EXIT_OK)
        # A conformance archive produces conformance receipts, so its manifest
        # is a catalog over a test archive and says so.
        manifest = json.loads(
            (manifests / "date=2026-07-30" / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["receipt_kind"], "local")
        self.assertFalse(manifest["authorizes_deletion"])
        self.assertEqual(manifest["segment_count"], 1)

    def test_a_malformed_seal_exits_non_zero_without_publishing(self) -> None:
        self.seal.write_text("{", encoding="utf-8")
        status, record = run(run_archiver.main, self.archiver_arguments())
        self.assertEqual(status, run_archiver.EXIT_FAILURES)
        self.assertEqual(record["counts"]["failed"], 1)
        self.assertEqual(list(self.spool.rglob("*.archive*.json")), [])

    def test_an_immutable_key_conflict_exits_with_its_own_code(self) -> None:
        store = LocalObjectStore(self.archive_root)
        from archive.archiver import object_keys
        from archive.common.seal import read_sealed_segment

        data_key, _ = object_keys(read_sealed_segment("polymarket", self.segment))
        store.put_immutable(
            data_key, io.BytesIO(b"squatter"), stored_identity_of(io.BytesIO(b"squatter"))
        )
        status, record = run(run_archiver.main, self.archiver_arguments())
        self.assertEqual(status, run_archiver.EXIT_CONFLICT)
        self.assertIsNotNone(record["halted"])

    def test_independence_is_refused_when_the_archive_shares_the_capture_disk(self) -> None:
        """Invariant 7, as a `st_dev` comparison rather than a promise."""
        with self.assertRaises(SystemExit) as raised:
            run(run_archiver.main, self.archiver_arguments("--archive-durability", "independent"))
        self.assertIn("same filesystem", str(raised.exception))

    def test_the_s3_backend_requires_its_fields_through_the_real_cli(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run(run_archiver.main, self.archiver_arguments("--archive-backend", "s3"))
        self.assertIn("--s3-bucket", str(raised.exception))

    def test_a_live_s3_option_is_refused_while_the_local_backend_is_selected(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run(run_archiver.main, self.archiver_arguments("--s3-bucket", "oops"))
        self.assertIn("--s3-bucket", str(raised.exception))


class ReaperCommandTests(CommandCase):
    def setUp(self) -> None:
        super().setUp()
        Archiver(self.spool, LocalObjectStore(self.archive_root)).sweep()
        write_canonical_receipt(self.canonical, inputs=[canonical_input_for(self.segment)])

    def test_the_default_run_deletes_nothing_and_reports_the_durability_gate(self) -> None:
        status, record = run(run_reaper.main, self.reaper_arguments())
        self.assertEqual(status, run_reaper.EXIT_OK)
        self.assertFalse(record["destructive"])
        self.assertEqual(record["retained_by_reason"], {"durability_gate": 1})
        self.assertTrue(self.segment.exists())
        self.assertTrue(self.seal.exists())

    def test_delete_is_refused_against_a_conformance_store(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run(run_reaper.main, self.reaper_arguments("--delete"))
        self.assertIn("local conformance store", str(raised.exception))
        self.assertTrue(self.segment.exists())

    def test_mode_delete_is_the_same_explicit_durability_gate(self) -> None:
        with self.assertRaisesRegex(SystemExit, "local conformance store"):
            run(run_reaper.main, self.reaper_arguments("--mode", "delete"))

    def test_interval_must_be_positive(self) -> None:
        with self.assertRaisesRegex(SystemExit, "must be positive"):
            run(run_reaper.main, self.reaper_arguments("--interval-seconds", "0"))

    def test_periodic_mode_runs_the_same_sweep_more_than_once(self) -> None:
        handlers = {}
        sweeps = 0

        def install(kind, handler):
            handlers[kind] = handler

        def sweep(arguments, store):
            nonlocal sweeps
            sweeps += 1
            if sweeps == 2:
                handlers[run_reaper.signal.SIGTERM]()
            return run_reaper.EXIT_OK

        with (
            mock.patch.object(run_reaper.signal, "signal", side_effect=install),
            mock.patch.object(run_reaper, "sweep_once", side_effect=sweep),
            mock.patch.object(run_reaper.time, "monotonic", side_effect=[0.0, 2.0, 2.0]),
        ):
            status = run_reaper.main(self.reaper_arguments("--interval-seconds", "1"))
        self.assertEqual(status, run_reaper.EXIT_OK)
        self.assertEqual(sweeps, 2)

    def test_delete_is_refused_when_the_archive_shares_the_capture_disk(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run(
                run_reaper.main,
                self.reaper_arguments("--archive-durability", "independent", "--delete"),
            )
        self.assertIn("same filesystem", str(raised.exception))
        self.assertTrue(self.segment.exists())

    def test_the_report_file_records_every_decision(self) -> None:
        report = self.root / "reports" / "reaper.json"
        run(run_reaper.main, self.reaper_arguments("--report", str(report)))
        record = json.loads(report.read_text(encoding="utf-8"))
        self.assertEqual(len(record["decisions"]), 1)
        self.assertEqual(record["decisions"][0]["decision"], "retained")

    def test_the_s3_backend_requires_its_fields_through_the_real_cli(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run(run_reaper.main, self.reaper_arguments("--archive-backend", "s3"))
        self.assertIn("--s3-bucket", str(raised.exception))

    def test_a_live_s3_option_is_refused_while_the_local_backend_is_selected(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run(run_reaper.main, self.reaper_arguments("--s3-bucket", "oops"))
        self.assertIn("--s3-bucket", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
