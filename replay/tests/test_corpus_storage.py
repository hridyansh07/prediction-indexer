"""Small offline crash-boundary tests; no materializer, bench or services."""

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from replay.corpus import results, spec as specs
from replay.corpus.__main__ import corpus_key, main
from replay.corpus.private import NAMESPACE, PrivateAttempt


def private_spec(root):
    return {**{key: str(root / key) for key in
               ("work_root", "canonical_store", "results_root", "ledger", "runner_config", "materializer")},
            "fees": {"template_catalog": str(root / "catalog"), "sources": str(root / "sources")},
            "derivative_cache": {"root": str(root / "cache"), "max_bytes": 1}}


class PrivateStorageTests(unittest.TestCase):
    def test_fresh_attempts_remove_only_the_current_owned_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            spec = private_spec(Path(tmp))
            old = PrivateAttempt(spec)
            first = PrivateAttempt(spec)
            second = PrivateAttempt(spec)
            try:
                (old.path / "input").write_bytes(b"orphan consumer's input")
                (first.path / "input").write_bytes(b"first")
                (first.path / "derivatives/addr").mkdir(parents=True)
                (first.path / "derivatives/addr/events").write_bytes(b"nested private input")
                (second.path / "input").write_bytes(b"second")
                self.assertEqual(len({old.path, first.path, second.path}), 3)
                self.assertEqual(first.path.parent, Path(spec["work_root"]) / NAMESPACE)
                self.assertEqual(first.path.stat().st_mode & 0o777, 0o700)
                first.discard()
                self.assertFalse(first.path.exists())
                self.assertEqual((old.path / "input").read_bytes(), b"orphan consumer's input")
                self.assertEqual((second.path / "input").read_bytes(), b"second")
            finally:
                for attempt in (old, first, second):
                    attempt.close()

    def test_rejects_root_overlap_symlinks_and_nested_previous_attempts_before_writing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            base = private_spec(root)
            alias = root / "alias"
            alias.symlink_to(root, target_is_directory=True)
            for changes in ({"work_root": str(root)}, {"work_root": str(root / "canonical_store/work")},
                            {"results_root": str(root / "work_root/results")},
                            {"canonical_store": str(root / "results_root/canonical")},
                            {"work_root": str(alias / "work")},
                            {"work_root": str(root / "work_root" / NAMESPACE / "old")},
                            {"ledger": str(root / "work_root/ledger.sqlite3")},
                            {"derivative_cache": {"root": str(root / "work_root/cache"), "max_bytes": 1}}):
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    PrivateAttempt({**base, **changes})
            self.assertFalse(Path(base["work_root"]).exists())

    def test_replaced_attempt_or_namespace_cannot_authorize_deletion(self):
        for replace_parent in (False, True):
            with self.subTest(replace_parent=replace_parent), tempfile.TemporaryDirectory() as tmp:
                spec = private_spec(Path(tmp))
                attempt = PrivateAttempt(spec)
                try:
                    (attempt.path / "input").write_bytes(b"owned")
                    victim = Path(tmp) / "victim"
                    victim.mkdir()
                    (victim / "input").write_bytes(b"do not delete")
                    target = attempt.path.parent if replace_parent else attempt.path
                    retained = target.with_name(target.name + "-moved")
                    target.rename(retained)
                    target.symlink_to(victim, target_is_directory=True)
                    with self.assertRaises(ValueError):
                        attempt.discard()
                    self.assertEqual((victim / "input").read_bytes(), b"do not delete")
                    self.assertEqual((retained / (attempt.path.name if replace_parent else "") / "input").read_bytes(),
                                     b"owned")
                finally:
                    attempt.close()

    def test_replacement_after_identity_check_survives_removal(self):
        with tempfile.TemporaryDirectory() as tmp:
            attempt = PrivateAttempt(private_spec(Path(tmp)))
            moved = attempt.path.with_name(attempt.path.name + "-moved")
            (attempt.path / "owned-input").write_bytes(b"owned")
            (attempt.path / "derivatives/addr").mkdir(parents=True)
            (attempt.path / "derivatives/addr/events").write_bytes(b"nested owned input")
            original_stat, replaced = os.stat, False

            def interleave(path, *args, **kwargs):
                nonlocal replaced
                info = original_stat(path, *args, **kwargs)
                if str(path) == attempt.path.name and kwargs.get("dir_fd") is not None and not replaced:
                    # Return the old inode identity, then replace the name before
                    # the remover can do its own lstat/open of that name.
                    replaced = True
                    attempt.path.rename(moved)
                    attempt.path.mkdir()
                    (attempt.path / "replacement-input").write_bytes(b"another consumer")
                return info

            try:
                error = ""
                with patch("os.stat", side_effect=interleave):
                    try:
                        attempt.discard()
                    except ValueError as caught:
                        error = str(caught)
                self.assertTrue(replaced)
                self.assertTrue((attempt.path / "replacement-input").is_file(), "replacement inputs were deleted")
                self.assertEqual((attempt.path / "replacement-input").read_bytes(), b"another consumer")
                self.assertFalse((moved / "owned-input").exists())
                self.assertFalse((moved / "derivatives").exists())
                self.assertIn("identity changed", error)
            finally:
                attempt.close()


class DiscardOptionTests(unittest.TestCase):
    def test_run_only_flag_is_rejected_before_spec_or_tool_access(self):
        for command in (["status"], ["verify"], ["verify", "--deep"]):
            with self.subTest(command=command), patch("sys.stderr"), \
                 patch.object(specs, "load", side_effect=AssertionError("must not load")), \
                 self.assertRaises(SystemExit) as caught:
                main([*command, "unused.json", "--discard-materialized-inputs"])
            self.assertEqual(caught.exception.code, 2)

    def test_flag_skips_writable_cache_probe_without_changing_spec_or_semantic_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            document = json.loads((Path(__file__).resolve().parents[2] /
                                   "configs/corpus/execution-realism.example.json").read_bytes())
            document.update(private_spec(root))
            Path(document["canonical_store"]).mkdir()
            for name in ("runner_config", "materializer"):
                Path(document[name]).write_bytes(b"{}")
            sources = Path(document["fees"]["sources"])
            sources.mkdir()
            (sources / "source.txt").write_bytes(b"synthetic fee evidence")
            # Use valid fee settings, not a permissive alternate spec reader.
            document["fees"].update(kalshi_member_class="DIRECT", limitless_buy_bps=300, limitless_sell_bps=150)
            path = root / "spec.json"
            path.write_text(json.dumps(document))
            with patch("tempfile.TemporaryFile", side_effect=AssertionError("writable cache probe")):
                loaded = specs.load(path, discard_materialized_inputs=True)
            self.assertEqual(loaded, document)
            self.assertFalse(Path(document["derivative_cache"]["root"]).exists())
            args = dict(source={"commit": "exact", "dirty": False}, materializer_describe=b"producer", runner_config=b"{}")
            self.assertEqual(corpus_key(loaded, **args), corpus_key(document, **args))
            # The default-off path still performs its original writable probe.
            with patch("tempfile.TemporaryFile", side_effect=PermissionError("read-only cache")), \
                 self.assertRaisesRegex(ValueError, "writable local directory"):
                specs.load(path)


class DurableResultsTests(unittest.TestCase):
    def test_durable_commit_preserves_bytes_and_syncs_entries_before_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            bench = root / "bench"
            (bench / "run/group").mkdir(parents=True)
            (bench / "result.json").write_bytes(b'{"status":"SUCCESS"}\n')
            (bench / "run/group/rows.ndjson").write_bytes(b'{"edge":17}\n')
            options = dict(corpus_key="k", bundle_id="b", status="SUCCESS", bench_exit=0, game_state=None)
            legacy, durable = root / "legacy", root / "new/results/key/b"
            results.commit(bench, legacy, **options)
            synced = []
            fsync = os.fsync

            def sync(fd):
                if stat.S_ISDIR(os.fstat(fd).st_mode):
                    synced.append((os.fstat(fd).st_ino, (durable / results.RECEIPT).exists()))
                fsync(fd)

            with patch("os.fsync", side_effect=sync):
                results.commit(bench, durable, durable=True, **options)
            self.assertEqual((legacy / results.RECEIPT).read_bytes(), (durable / results.RECEIPT).read_bytes())
            self.assertEqual(results.verify(legacy), results.verify(durable))
            # Every new ancestor and nested output entry survives power loss before
            # the receipt is published; publishing the receipt syncs its directory again.
            for path in (root, root / "new", root / "new/results", root / "new/results/key",
                         durable, durable / "run", durable / "run/group"):
                self.assertIn((path.stat().st_ino, False), synced, str(path))
            self.assertEqual(synced[-1], (durable.stat().st_ino, True))

    def test_directory_sync_errors_prevent_success_before_and_after_receipt(self):
        for receipt_present in (False, True):
            with self.subTest(receipt_present=receipt_present), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                bench, dest = root / "bench", root / "results/key/b"
                bench.mkdir()
                (bench / "result.json").write_bytes(b"{}")
                fsync = os.fsync

                def sync(fd):
                    if stat.S_ISDIR(os.fstat(fd).st_mode) and (dest / results.RECEIPT).exists() == receipt_present:
                        raise OSError("directory sync failed")
                    fsync(fd)

                with patch("os.fsync", side_effect=sync), self.assertRaisesRegex(OSError, "directory sync failed"):
                    results.commit(bench, dest, corpus_key="k", bundle_id="b", status="SUCCESS", bench_exit=0,
                                   game_state=None, durable=True)
                self.assertEqual((dest / results.RECEIPT).exists(), receipt_present)
                if receipt_present:
                    # Visible is not the same as durable: the writer still failed.
                    self.assertEqual(results.verify(dest)["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
