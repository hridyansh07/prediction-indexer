"""Local deletion of archived Targeter v2 run directories.

Deletion is the one irreversible act on this side of the pipeline, so almost
every test here asserts that it did *not* happen. The few that do delete exist
to prove the gate can open at all: a reaper that never deletes passes every
safety test and bounds no disk.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import os
import tempfile
import time
import unittest
from datetime import timedelta
from pathlib import Path
from unittest import mock

from archive.storage import CONFORMANCE, INDEPENDENT, LocalObjectStore
from archive.storage.base import IntegrityConflict
from targeter.v2 import run_archiver_cli, run_reaper_cli
from targeter.v2.lease import TargeterRunLease
from targeter.v2.run_archiver import (
    ARCHIVED,
    FAILED,
    INCOMPLETE_RUN_FLOOR_HOURS,
    PENDING,
    SKIPPED,
    TargetRunArchiveSweep,
)
from targeter.v2.publication import publish_run
from targeter.v2.domain import CatalogSnapshot
from targeter.v2.registry import load_strategy
from targeter.v2.run import run_shadow
from targeter.v2.run_archive import archive_run
from targeter.v2.run_reaper import (
    ALREADY_REAPED,
    ARCHIVE_OBJECT_UNVERIFIED,
    ARTIFACTS_ABSENT,
    AUDIT_MODE,
    DURABILITY_GATE,
    LOCAL_RUN_CHANGED,
    NOT_ARCHIVED,
    POINTER_UNREADABLE,
    PROVEN,
    PUBLISHED_GENERATION,
    REAPED,
    RECEIPT_INVALID,
    RETAINED,
    RETENTION_FLOOR,
    RETENTION_FLOOR_HOURS,
    RUN_CLOCK_UNREADABLE,
    UNEXPECTED_RUN_ARTIFACT,
    PublicationPointer,
    TargetRunReaper,
    discover_runs,
    parse_run_id_ns,
)
from tests.test_targeter_v2 import NOW, STRATEGY_PATH, snapshot
from tests.test_targeter_v2_delivery import _Adapter


ROOT = Path(__file__).resolve().parents[1]
HOUR = timedelta(hours=1)
RECEIPT = "archive_receipt.json"


def run_command(main, arguments: list[str]) -> tuple[int, dict]:
    captured = io.StringIO()
    with contextlib.redirect_stdout(captured):
        status = main(arguments)
    output = captured.getvalue().strip().splitlines()
    return status, json.loads(output[-1]) if output else {}


class _CountingStore:
    """Delegates everything and counts the requests a decision really makes."""

    def __init__(self, delegate) -> None:
        self.delegate = delegate
        self.store_id = delegate.store_id
        self.durability = delegate.durability
        self.heads = 0

    def put_immutable(self, key, reader, expected_identity, **kwargs):
        return self.delegate.put_immutable(key, reader, expected_identity, **kwargs)

    def head(self, key):
        self.heads += 1
        return self.delegate.head(key)

    def open(self, key, *, max_bytes=None):
        return self.delegate.open(key, max_bytes=max_bytes)


class _ConflictingStore:
    """A backend whose namespace already holds different bytes for one run."""

    def __init__(self, delegate, *, run_id: str) -> None:
        self.delegate = delegate
        self.store_id = delegate.store_id
        self.durability = delegate.durability
        self.run_id = run_id

    def put_immutable(self, key, reader, expected_identity, **kwargs):
        if f"run={self.run_id}/" in key:
            raise IntegrityConflict(f"{key} already holds different content")
        return self.delegate.put_immutable(key, reader, expected_identity, **kwargs)

    def head(self, key):
        return self.delegate.head(key)

    def open(self, key, *, max_bytes=None):
        return self.delegate.open(key, max_bytes=max_bytes)


class TargetRunCase(unittest.TestCase):
    """One archived run under an authorized backend, ready to be reaped."""

    #: Overridden by the conformance case, which is what a default deployment
    #: actually runs.
    durability = INDEPENDENT

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.output_root = self.root / "runs"
        self.live_root = self.root / "live"
        self.strategy = load_strategy(STRATEGY_PATH)
        self.store = LocalObjectStore(
            self.root / "object-store",
            store_id="targeter-test-bucket",
            durability=self.durability,
        )

    # -- fixtures ----------------------------------------------------------

    def run_directory(self, *, now=None) -> Path:
        # An hour after NOW by default, so a bare call is never accidentally
        # the same run id as the published fixture, which must sit at NOW.
        now = NOW + HOUR if now is None else now
        adapters = [
            _Adapter(snapshot("kalshi", "k", "km")),
            _Adapter(snapshot("polymarket", "p", "pm")),
            _Adapter(CatalogSnapshot("limitless", (), ())),
        ]
        result = run_shadow(
            strategy=self.strategy,
            output_root=self.output_root,
            cache_root=self.root / "cache",
            now=now,
            adapters=adapters,
            client=object(),
        )
        return result.directory

    def archived(self, *, now=None) -> Path:
        """A run with a receipt for this case's store.

        Defaults to an hour after ``NOW`` so it never collides with the
        published fixture, which has to sit exactly at ``NOW`` because that is
        the instant the shared snapshot's events are selectable at.
        """
        now = NOW + HOUR if now is None else now
        directory = self.run_directory(now=now)
        archive_run(directory, self.store, now=now)
        return directory

    def published(self) -> Path:
        """The run the live pointer names.

        Every test that expects a decision past the pointer gate needs one of
        these, because an absent pointer retains everything by design.
        """
        directory = self.run_directory(now=NOW)
        receipt = archive_run(directory, self.store, now=NOW)
        publish_run(
            directory,
            receipt,
            self.store,
            live_root=self.live_root,
            strategy=self.strategy,
            now=NOW,
        )
        return directory

    def reaper(self, *, destructive: bool = True, retention_ns: int = 0) -> TargetRunReaper:
        return TargetRunReaper(
            self.output_root,
            self.live_root,
            self.store,
            destructive=destructive,
            retention_ns=retention_ns,
        )

    def only(self, result):
        self.assertEqual(len(result.decisions), 1, result.as_record())
        return result.decisions[0]

    def decide(self, directory: Path, **kwargs):
        return self.only(self.reaper(**kwargs).sweep([directory]))

    def artifacts(self, directory: Path) -> set[str]:
        return {path.name for path in directory.iterdir()}

    def age_out(self, directory: Path) -> None:
        """Backdate the receipt so every clock agrees the run is past the floor.

        The run id and `archived_at_ns` are already old in these fixtures; the
        receipt's mtime is the one that is always "just now", which is exactly
        the property that makes it the binding clock.
        """
        stale = time.time() - (RETENTION_FLOOR_HOURS + 2) * 3600
        os.utime(directory / RECEIPT, (stale, stale))


class RetentionTests(TargetRunCase):
    def test_a_run_nothing_archived_is_retained_and_counted_as_unarchived(self) -> None:
        self.published()
        directory = self.run_directory()
        result = self.reaper().sweep([directory])
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, NOT_ARCHIVED)
        self.assertEqual(result.counts["unarchived"], 1)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_conformance_receipt_authorizes_nothing(self) -> None:
        self.published()
        directory = self.run_directory()
        conformance = LocalObjectStore(self.root / "conformance", durability=CONFORMANCE)
        archive_run(directory, conformance, now=NOW)
        self.assertTrue((directory / "archive_receipt.local.json").is_file())

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, DURABILITY_GATE)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_receipt_naming_another_run_retains(self) -> None:
        self.published()
        first = self.archived(now=NOW + HOUR)
        second = self.archived(now=NOW + 2 * HOUR)
        (second / RECEIPT).write_bytes((first / RECEIPT).read_bytes())

        decision = self.decide(second)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, RECEIPT_INVALID)
        self.assertIn(first.name, decision.detail)
        self.assertTrue((second / "selection_report.json").exists())

    def test_a_corrupt_receipt_retains(self) -> None:
        self.published()
        directory = self.archived()
        (directory / RECEIPT).write_text("{not json", encoding="utf-8")

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, RECEIPT_INVALID)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_missing_archive_object_retains(self) -> None:
        self.published()
        directory = self.archived()
        key = json.loads((directory / RECEIPT).read_text())["objects"][0]["key"]
        (Path(self.store.root) / key).unlink()

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_mutated_archive_object_retains(self) -> None:
        self.published()
        directory = self.archived()
        key = json.loads((directory / RECEIPT).read_text())["objects"][0]["key"]
        path = Path(self.store.root) / key
        path.write_bytes(path.read_bytes() + b"tampered")

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_changed_local_artifact_retains_even_with_a_verified_archive(self) -> None:
        self.published()
        directory = self.archived()
        drift = directory / "rule_drift.ndjson"
        drift.write_text(drift.read_text() + '{"injected": true}\n', encoding="utf-8")

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, LOCAL_RUN_CHANGED)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_leftover_atomic_write_temporary_retains_the_whole_run(self) -> None:
        """analysis/storage.py names its temporaries `.<file>.<random>`."""
        self.published()
        directory = self.archived()
        (directory / ".selection_report.json.a1b2c3d4").write_text("partial")

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, UNEXPECTED_RUN_ARTIFACT)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_leftover_durable_open_file_retains_the_whole_run(self) -> None:
        """archive/common/durable.py names its temporaries `.<file>.<pid>.<hex>.open`."""
        self.published()
        directory = self.archived()
        (directory / ".run_manifest.json.4242.deadbeef.open").write_text("partial")

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, UNEXPECTED_RUN_ARTIFACT)
        self.assertTrue((directory / "run_manifest.json").exists())

    def test_a_subdirectory_inside_a_run_retains_the_whole_run(self) -> None:
        self.published()
        directory = self.archived()
        (directory / "unexpected").mkdir()

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, UNEXPECTED_RUN_ARTIFACT)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_the_published_generation_is_never_reaped(self) -> None:
        published = self.published()
        decision = self.decide(published)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, PUBLISHED_GENERATION)
        self.assertTrue((published / "selection_report.json").exists())

    def test_a_run_inside_the_retention_floor_is_retained(self) -> None:
        self.published()
        directory = self.archived()
        decision = self.decide(directory, retention_ns=RETENTION_FLOOR_HOURS * 3600 * 10**9)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, RETENTION_FLOOR)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_an_absent_publication_pointer_retains_everything(self) -> None:
        directory = self.archived()
        result = self.reaper().sweep([directory])
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, POINTER_UNREADABLE)
        self.assertEqual(len(result.pointer_faults), 1)
        self.assertIsNone(result.published_run_id)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_malformed_publication_pointer_retains_everything(self) -> None:
        self.published()
        pointer = self.live_root / "targeter-v2" / "current.json"
        pointer.write_text(json.dumps({"target_generation_pointer_version": 1}))
        directory = self.archived()

        result = self.reaper().sweep([directory])
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, POINTER_UNREADABLE)
        self.assertEqual(len(result.pointer_faults), 1)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_the_run_lease_is_not_mistaken_for_a_run(self) -> None:
        self.published()
        self.archived()
        (self.output_root / ".targeter-v2.lock").write_text("pid=1\n")
        (self.output_root / "not-a-run").mkdir()

        discovered = {path.name for path in discover_runs(self.output_root)}
        self.assertNotIn(".targeter-v2.lock", discovered)
        self.assertNotIn("not-a-run", discovered)

        result = self.reaper(destructive=False).sweep()
        self.assertEqual(result.unrecognized, ["not-a-run"])
        self.assertEqual(result.counts["considered"], len(discovered))


class ClockTests(TargetRunCase):
    def test_a_run_id_that_names_no_instant_is_unreadable(self) -> None:
        self.assertIsNone(parse_run_id_ns("not-a-run"))
        self.assertIsNone(parse_run_id_ns("20261301T000000.000000Z"))
        self.assertEqual(
            parse_run_id_ns("19700101T000000.000001Z"), 1_000
        )

    def test_the_floor_uses_the_latest_of_every_available_clock(self) -> None:
        """`--now` can backdate a run id and its receipt, but not the host."""
        self.published()
        ancient = NOW.replace(year=2020)
        directory = self.run_directory(now=ancient)
        archive_run(directory, self.store, now=ancient)
        self.assertTrue(directory.name.startswith("2020"))

        decision = self.decide(
            directory, retention_ns=RETENTION_FLOOR_HOURS * 3600 * 10**9
        )
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, RETENTION_FLOOR)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_an_unparseable_run_id_retains_when_considered_directly(self) -> None:
        self.published()
        directory = self.archived()
        renamed = directory.parent / "handmade-copy"
        directory.rename(renamed)
        # The receipt still names the original run, so the earlier identity
        # check fires first; that is the point — neither path can proceed.
        decision = self.decide(renamed)
        self.assertEqual(decision.decision, RETAINED)
        self.assertIn(decision.reason, (RECEIPT_INVALID, RUN_CLOCK_UNREADABLE))


class DurabilityGateTests(TargetRunCase):
    durability = CONFORMANCE

    def test_a_conformance_store_retains_every_run_and_reports_the_gate(self) -> None:
        directory = self.run_directory()
        archive_run(directory, self.store, now=NOW)
        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, DURABILITY_GATE)
        self.assertTrue((directory / "selection_report.json").exists())


class DeletionTests(TargetRunCase):
    def candidate(self) -> Path:
        self.published()
        return self.archived()

    def test_audit_mode_proves_eligibility_without_touching_anything(self) -> None:
        directory = self.candidate()
        before = self.artifacts(directory)

        result = self.reaper(destructive=False).sweep([directory])
        decision = self.only(result)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, AUDIT_MODE)
        self.assertEqual(result.counts["reapable"], 1)
        self.assertEqual(self.artifacts(directory), before)

    def test_a_proven_run_loses_every_artifact_but_keeps_its_receipt(self) -> None:
        directory = self.candidate()
        decision = self.decide(directory)

        self.assertEqual(decision.decision, REAPED)
        self.assertEqual(decision.reason, PROVEN)
        self.assertGreater(decision.artifacts_deleted, 0)
        self.assertGreater(decision.bytes_reclaimed, 0)
        # The receipt is what keeps the deletion auditable, so it stays, and so
        # does the directory holding it.
        self.assertTrue(directory.is_dir())
        self.assertEqual(self.artifacts(directory), {RECEIPT})

    def test_a_second_sweep_after_a_reaping_is_idempotent(self) -> None:
        directory = self.candidate()
        self.decide(directory)

        decision = self.decide(directory)
        self.assertEqual(decision.decision, ALREADY_REAPED)
        self.assertEqual(decision.reason, ARTIFACTS_ABSENT)
        self.assertEqual(self.artifacts(directory), {RECEIPT})

    def test_a_reaped_run_is_recognized_without_asking_the_object_store(self) -> None:
        directory = self.candidate()
        self.decide(directory)

        counting = _CountingStore(self.store)
        reaper = TargetRunReaper(
            self.output_root, self.live_root, counting, destructive=True, retention_ns=0
        )
        decision = self.only(reaper.sweep([directory]))
        self.assertEqual(decision.decision, ALREADY_REAPED)
        self.assertEqual(counting.heads, 0)

    def test_a_crash_midway_through_deletion_resumes_after_revalidation(self) -> None:
        directory = self.candidate()
        (directory / "selection_report.json").unlink()
        (directory / "rule_drift.ndjson").unlink()

        decision = self.decide(directory)
        self.assertEqual(decision.decision, REAPED)
        self.assertIn("partial cleanup", decision.detail)
        self.assertEqual(self.artifacts(directory), {RECEIPT})

    def test_a_partial_cleanup_is_not_finished_when_the_archive_stops_verifying(self) -> None:
        directory = self.candidate()
        (directory / "selection_report.json").unlink()
        key = json.loads((directory / RECEIPT).read_text())["objects"][0]["key"]
        (Path(self.store.root) / key).unlink()

        decision = self.decide(directory)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, ARCHIVE_OBJECT_UNVERIFIED)
        self.assertTrue((directory / "rule_drift.ndjson").exists())

    def test_a_partial_cleanup_is_reported_but_not_finished_in_audit_mode(self) -> None:
        directory = self.candidate()
        (directory / "selection_report.json").unlink()

        decision = self.decide(directory, destructive=False)
        self.assertEqual(decision.decision, RETAINED)
        self.assertEqual(decision.reason, AUDIT_MODE)
        self.assertIn("partial cleanup", decision.detail)
        self.assertTrue((directory / "rule_drift.ndjson").exists())

    def test_reaping_one_run_leaves_the_published_generation_intact(self) -> None:
        published = self.published()
        directory = self.archived()

        result = self.reaper().sweep()
        by_run = {item.run_id: item for item in result.decisions}
        self.assertEqual(by_run[directory.name].decision, REAPED)
        self.assertEqual(by_run[published.name].decision, RETAINED)
        self.assertEqual(by_run[published.name].reason, PUBLISHED_GENERATION)
        self.assertTrue((published / "selection_report.json").exists())

    def test_the_report_carries_what_an_audit_needs(self) -> None:
        directory = self.candidate()
        result = self.reaper().sweep([directory])
        record = result.as_record()

        self.assertTrue(record["destructive"])
        self.assertEqual(
            set(record),
            {
                "destructive",
                "published_run_id",
                "counts",
                "retained_by_reason",
                "pointer_faults",
                "unrecognized",
                "decisions",
            },
        )
        decision = record["decisions"][0]
        self.assertEqual(
            set(decision),
            {
                "run_id",
                "run_directory",
                "archive_receipt",
                "prefix",
                "decision",
                "reason",
                "detail",
                "verified_at_ns",
                "age_ns",
                "artifacts_deleted",
                "bytes_reclaimed",
            },
        )
        self.assertGreater(decision["verified_at_ns"], 0)
        self.assertEqual(record["counts"]["reaped"], 1)
        self.assertGreater(record["counts"]["bytes_reclaimed"], 0)
        self.assertTrue(decision["prefix"].startswith("targeter-v2/runs/date="))


class ArchiveSweepTests(TargetRunCase):
    def sweeper(self, **kwargs) -> TargetRunArchiveSweep:
        return TargetRunArchiveSweep(self.output_root, self.store, **kwargs)

    def test_the_sweep_archives_every_complete_unarchived_run(self) -> None:
        first = self.run_directory(now=NOW)
        second = self.run_directory(now=NOW + HOUR)

        result = self.sweeper().sweep()
        self.assertEqual(result.counts["archived"], 2, result.as_record())
        self.assertEqual(result.counts["failed"], 0)
        self.assertTrue((first / RECEIPT).is_file())
        self.assertTrue((second / RECEIPT).is_file())

    def test_the_sweep_skips_a_run_that_already_holds_this_stores_receipt(self) -> None:
        directory = self.archived()
        before = (directory / RECEIPT).read_bytes()

        result = self.sweeper().sweep()
        outcome = result.outcomes[0]
        self.assertEqual(outcome.status, SKIPPED)
        self.assertEqual((directory / RECEIPT).read_bytes(), before)

    def test_the_sweep_never_reads_a_reaped_run_directory(self) -> None:
        """archive_run opens the selection report before it checks for a receipt."""
        self.published()
        directory = self.archived()
        self.only(self.reaper().sweep([directory]))
        self.assertEqual(self.artifacts(directory), {RECEIPT})

        outcome = self.only_outcome(self.sweeper().sweep([directory]))
        self.assertEqual(outcome.status, SKIPPED)
        self.assertEqual(self.artifacts(directory), {RECEIPT})

    def test_a_run_still_being_written_is_pending_not_failed(self) -> None:
        directory = self.run_directory()
        (directory / "rule_drift.ndjson").unlink()

        outcome = self.only_outcome(
            self.sweeper(now_ns=self._clock_at(directory, hours=1)).sweep([directory])
        )
        self.assertEqual(outcome.status, PENDING)
        self.assertIn("rule_drift.ndjson", outcome.detail)
        self.assertFalse((directory / RECEIPT).exists())

    def test_an_incomplete_run_past_the_floor_is_a_fault_an_operator_must_clear(self) -> None:
        directory = self.run_directory()
        (directory / "rule_drift.ndjson").unlink()
        stale = self._clock_at(directory, hours=INCOMPLETE_RUN_FLOOR_HOURS + 2)

        outcome = self.only_outcome(
            self.sweeper(now_ns=stale).sweep([directory])
        )
        self.assertEqual(outcome.status, FAILED)
        self.assertIn("died before finishing", outcome.detail)

    def test_a_stray_temp_file_does_not_stop_a_complete_run_being_archived(self) -> None:
        """The sweep archives; refusing strays is the reaper's job, not this one."""
        directory = self.run_directory()
        (directory / ".selection_report.json.a1b2c3d4").write_text("partial")

        outcome = self.only_outcome(self.sweeper().sweep([directory]))
        self.assertEqual(outcome.status, FAILED)
        self.assertIn("unexpected run artifact", outcome.detail)
        self.assertFalse((directory / RECEIPT).exists())

    def test_a_key_conflict_halts_the_sweep(self) -> None:
        first = self.run_directory(now=NOW)
        second = self.run_directory(now=NOW + HOUR)
        conflicting = _ConflictingStore(self.store, run_id=first.name)

        sweep = TargetRunArchiveSweep(self.output_root, conflicting)
        result = sweep.sweep()
        self.assertEqual(result.counts["conflicted"], 1)
        self.assertIsNotNone(result.halted)
        self.assertEqual(result.discovered, 1, result.as_record())
        self.assertFalse((second / RECEIPT).exists())

    def test_the_sweep_report_carries_what_an_operator_needs(self) -> None:
        self.run_directory()
        record = self.sweeper().sweep().as_record()

        self.assertEqual(
            set(record), {"lease_acquired", "counts", "halted", "runs"}
        )
        self.assertEqual(
            set(record["counts"]),
            {"discovered", "archived", "skipped", "pending", "failed", "conflicted"},
        )
        self.assertEqual(
            set(record["runs"][0]),
            {
                "run_id",
                "run_directory",
                "status",
                "detail",
                "receipt",
                "prefix",
                "object_count",
            },
        )

    def only_outcome(self, result):
        self.assertEqual(len(result.outcomes), 1, result.as_record())
        return result.outcomes[0]

    @staticmethod
    def _clock_at(directory: Path, *, hours: float):
        """A sweep clock a fixed distance after the run started."""
        moment = parse_run_id_ns(directory.name) + int(hours * 3600 * 10**9)
        return lambda: moment


class SeparationTests(unittest.TestCase):
    """An absence is only a fact if something checks for it."""

    def test_the_archive_sweep_never_imports_a_removal_primitive(self) -> None:
        source = (ROOT / "targeter" / "v2" / "run_archiver.py").read_text()
        tree = ast.parse(source)
        imported = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
        self.assertNotIn("remove_durable", imported)
        self.assertNotIn("shutil", imported)
        called = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertFalse(called & {"unlink", "rmdir", "rmtree", "remove"})

    def test_the_archive_sweep_command_has_no_flag_that_can_delete(self) -> None:
        options = {
            option
            for action in run_archiver_cli.build_parser()._actions
            for option in action.option_strings
        }
        self.assertFalse(options & {"--mode", "--delete", "--destructive"})

    def test_neither_targeter_command_owns_an_interval_loop(self) -> None:
        """Targeter v2 is a one-shot transaction a host scheduler repeats."""
        for module in (run_reaper_cli, run_archiver_cli):
            with self.subTest(command=module.__name__):
                options = {
                    option
                    for action in module.build_parser()._actions
                    for option in action.option_strings
                }
                self.assertNotIn("--interval-seconds", options)
                source = (ROOT / "targeter" / "v2" / f"{module.__name__.rsplit('.', 1)[-1]}.py").read_text()
                self.assertNotIn("signal.signal", source)

    def test_the_archive_package_never_imports_targeter(self) -> None:
        """archive/ is the layer targeter/ builds on, and not the reverse."""
        offenders = []
        for path in sorted((ROOT / "archive").rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                    "targeter"
                ):
                    offenders.append(str(path))
                elif isinstance(node, ast.Import) and any(
                    alias.name.startswith("targeter") for alias in node.names
                ):
                    offenders.append(str(path))
        self.assertEqual(offenders, [])


class ReaperCommandTests(TargetRunCase):
    """The library is where the gate is proven; this is the deployment gate."""

    def arguments(self, *extra: str) -> list[str]:
        return [
            "--output-root",
            str(self.output_root),
            "--live-root",
            str(self.live_root),
            "--archive-root",
            str(self.root / "object-store"),
            "--store-id",
            "targeter-test-bucket",
            "--retention-hours",
            str(RETENTION_FLOOR_HOURS),
            *extra,
        ]

    def independent_backend(self):
        """Put the archive on a different device, as a real deployment must."""
        runs = self.output_root.resolve()
        return mock.patch(
            "archive.storage.factory._device_of",
            side_effect=lambda path: 1 if Path(path).resolve() == runs else 2,
        )

    def test_audit_is_the_default_mode(self) -> None:
        self.published()
        directory = self.archived()
        self.age_out(directory)
        with self.independent_backend():
            status, record = run_command(
                run_reaper_cli.main, self.arguments("--archive-durability", "independent")
            )

        self.assertEqual(status, run_reaper_cli.EXIT_OK)
        self.assertFalse(record["destructive"])
        # Everything proven, nothing deleted: the run is reported as reapable
        # and stays exactly where it is.
        self.assertEqual(record["counts"]["reapable"], 1)
        self.assertEqual(record["counts"]["reaped"], 0)
        self.assertTrue((directory / "selection_report.json").exists())

    def test_delete_mode_against_a_conformance_store_is_refused_at_startup(self) -> None:
        self.published()
        directory = self.archived()
        self.age_out(directory)
        with self.assertRaises(SystemExit) as raised:
            run_command(run_reaper_cli.main, self.arguments("--mode", "delete"))

        self.assertIn("conformance", str(raised.exception))
        self.assertTrue((directory / "selection_report.json").exists())

    def test_a_retention_shorter_than_the_floor_is_refused(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            run_command(run_reaper_cli.main, self.arguments("--retention-hours", "1"))
        self.assertIn("--retention-hours", str(raised.exception))

    def test_the_live_root_is_not_optional(self) -> None:
        with self.assertRaises(SystemExit):
            run_reaper_cli.build_parser().parse_args(
                ["--output-root", str(self.output_root), "--archive-root", str(self.root)]
            )

    def test_an_unreadable_pointer_exits_non_zero(self) -> None:
        self.archived()
        status, record = run_command(run_reaper_cli.main, self.arguments())
        self.assertEqual(status, run_reaper_cli.EXIT_RETAINED_FAULT)
        self.assertEqual(record["counts"]["pointer_faults"], 1)

    def test_an_unarchived_run_is_reported_without_being_a_fault(self) -> None:
        self.published()
        self.run_directory(now=NOW + 2 * HOUR)
        status, record = run_command(run_reaper_cli.main, self.arguments())

        self.assertEqual(status, run_reaper_cli.EXIT_OK)
        self.assertEqual(record["counts"]["unarchived"], 1)

    def test_the_report_file_is_written_durably(self) -> None:
        self.published()
        report = self.root / "ops" / "last_sweep.json"
        status, record = run_command(
            run_reaper_cli.main, self.arguments("--report", str(report))
        )

        self.assertEqual(status, run_reaper_cli.EXIT_OK)
        self.assertEqual(json.loads(report.read_text()), record)
        self.assertEqual(record["retention_hours"], RETENTION_FLOOR_HOURS)
        # A default invocation names a conformance backend, which is exactly
        # why the command refuses to delete against one.
        self.assertEqual(record["archive"]["durability"], "local_conformance")

    def test_delete_mode_reaps_only_once_the_backend_is_declared_independent(self) -> None:
        self.published()
        directory = self.archived()
        self.age_out(directory)
        with self.independent_backend():
            status, record = run_command(
                run_reaper_cli.main,
                self.arguments(
                    "--mode", "delete", "--archive-durability", "independent"
                ),
            )

        self.assertEqual(status, run_reaper_cli.EXIT_OK)
        self.assertTrue(record["destructive"])
        self.assertEqual(record["counts"]["reaped"], 1)
        self.assertEqual(self.artifacts(directory), {RECEIPT})
        # The published generation is still whole; the audit can still read it.
        published = self.output_root / record["published_run_id"]
        self.assertTrue((published / "selection_report.json").exists())


class ArchiveSweepCommandTests(TargetRunCase):
    def arguments(self, *extra: str) -> list[str]:
        return [
            "--output-root",
            str(self.output_root),
            "--archive-root",
            str(self.root / "object-store"),
            "--store-id",
            "targeter-test-bucket",
            *extra,
        ]

    def test_the_sweep_archives_what_has_no_receipt(self) -> None:
        directory = self.run_directory()
        status, record = run_command(run_archiver_cli.main, self.arguments())

        self.assertEqual(status, run_archiver_cli.EXIT_OK)
        self.assertTrue(record["lease_acquired"])
        self.assertEqual(record["counts"]["archived"], 1)
        # A conformance backend writes the receipt that authorizes nothing,
        # which is the correct default: this command proves the protocol works
        # before an operator declares a real durability domain.
        self.assertTrue((directory / "archive_receipt.local.json").is_file())

    def test_a_run_in_progress_holds_the_lease_and_the_sweep_defers(self) -> None:
        self.run_directory()
        lease = TargeterRunLease.acquire(self.output_root)
        self.addCleanup(lease.close)

        status, record = run_command(run_archiver_cli.main, self.arguments())
        self.assertEqual(status, run_archiver_cli.EXIT_OK)
        self.assertFalse(record["lease_acquired"])
        self.assertEqual(record["counts"]["discovered"], 0)

    def test_the_lease_is_released_when_the_sweep_finishes(self) -> None:
        self.run_directory()
        run_command(run_archiver_cli.main, self.arguments())
        TargeterRunLease.acquire(self.output_root).close()

    def test_an_abandoned_incomplete_run_exits_non_zero(self) -> None:
        directory = self.run_directory()
        (directory / "rule_drift.ndjson").unlink()

        status, record = run_command(run_archiver_cli.main, self.arguments())
        self.assertEqual(status, run_archiver_cli.EXIT_FAILURES)
        self.assertEqual(record["counts"]["failed"], 1)

    def test_the_report_file_is_written(self) -> None:
        self.run_directory()
        report = self.root / "ops" / "last_archive_sweep.json"
        _status, record = run_command(
            run_archiver_cli.main, self.arguments("--report", str(report))
        )
        self.assertEqual(json.loads(report.read_text()), record)

    def test_the_sweep_makes_an_unarchived_run_reapable(self) -> None:
        """The two commands compose: no sweep means nothing is ever reclaimed."""
        self.published()
        directory = self.run_directory(now=NOW + 2 * HOUR)

        before = self.only(self.reaper().sweep([directory]))
        self.assertEqual(before.reason, NOT_ARCHIVED)

        run_command(run_archiver_cli.main, self.arguments())
        # The sweep's conformance receipt still authorizes nothing, so the run
        # moves from unarchived to durability-gated rather than straight to
        # reapable. That is the gate working, not the sweep failing.
        after = self.only(self.reaper().sweep([directory]))
        self.assertEqual(after.reason, DURABILITY_GATE)
        self.assertTrue((directory / "selection_report.json").exists())


class PointerTests(TargetRunCase):
    def test_the_pointer_reader_reports_a_fault_instead_of_raising(self) -> None:
        pointer = PublicationPointer.read(self.live_root)
        self.assertIsNone(pointer.run_id)
        self.assertIsNotNone(pointer.fault)

    def test_the_pointer_reader_agrees_with_publication(self) -> None:
        published = self.published()
        pointer = PublicationPointer.read(self.live_root)
        self.assertEqual(pointer.run_id, published.name)
        self.assertIsNone(pointer.fault)


if __name__ == "__main__":
    unittest.main()
