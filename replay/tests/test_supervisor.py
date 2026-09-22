"""Offline budget/crash tests and opt-in disposable Redis subprocess acceptance."""

import copy
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

from replay import supervisor as s
from replay.streams import ProtocolError, TransportError
from replay.tests.test_streams import records

URL = os.environ.get("REPLAY_REDIS_URL")


def config(publisher="/unused"):
    body = records()[0]["body"]
    return {
        "version": 1,
        "publisher": publisher,
        "python": sys.executable,
        "transport": {
            "run_id": "contract",
            "scope": "test",
            "inputs": [{"directory": "/unused", **p} for p in body["pins"]],
            **{
                k: body[k]
                for k in ("start_ns", "end_ns", "lower_bound", "plans", "groups")
            },
            "command_timeout_ms": 500,
            "max_entry_bytes": int(body["max_entry_bytes"]),
            "max_queue_bytes": int(body["max_queue_bytes"]),
        },
        "strategies": {
            g: {
                "factory": "replay.tests.test_supervisor:strategy",
                "revision": "test-v1",
                "config": {},
            }
            for g in body["groups"]
        },
        "limits": {
            "attempts": 3,
            "no_progress": 2,
            "progress_margin": 3,
            "stall_seconds": 2,
            "attempt_seconds": 5,
            "run_seconds": 20,
            "poll_seconds": 0.01,
            "stop_seconds": 0.2,
        },
    }


def state():
    return {
        "version": 1,
        "identity": "unused",
        "attempts": 0,
        "best": -1,
        "stagnant": 0,
        "started": time.time(),
        "active": None,
        "fatal": False,
    }


class BudgetTests(unittest.TestCase):
    def test_high_water_not_previous_attempt_or_oscillation(self):
        x, limits = state(), config()["limits"]
        for progress, best, stagnant in [
            (10, 10, 0),
            (4, 10, 1),
            (10, 10, 2),
            (12, 12, 3),
            (15, 15, 0),
        ]:
            s.failed(x, progress, limits)
            self.assertEqual((x["best"], x["stagnant"]), (best, stagnant))

    def test_exact_margin_and_budget_boundaries(self):
        limits = config()["limits"]
        for progress, stagnant in [(1, 1), (2, 0), (3, 0)]:
            x = state()
            s.failed(x, progress, limits)
            self.assertEqual(x["stagnant"], stagnant)
        for field, cap in [("attempts", 3), ("stagnant", 2)]:
            x = state()
            x[field] = cap - 1
            self.assertTrue(s.allowed(x, limits))
            x[field] = cap
            self.assertFalse(s.allowed(x, limits))
            x[field] += 1
            self.assertFalse(s.allowed(x, limits))

    def test_start_persisted_before_launch_and_interrupted_restart(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(s, "client", return_value=Mock()),
        ):
            root = Path(tmp)
            c = config()
            seen = []

            def crash(root, c, x, *_):
                disk = s.read_state(root, s.identity(c))
                self.assertEqual(disk["attempts"], len(seen) + 1)
                self.assertEqual(disk["active"]["progress"], -1)
                seen.append(disk["active"]["id"])
                raise OSError("crash before launch")

            for _ in range(2):
                with (
                    patch.object(s, "attempt", side_effect=crash),
                    self.assertRaises(OSError),
                ):
                    s.run(c, root, "redis://unused")
            with (
                patch.object(s, "attempt") as launch,
                self.assertRaises(s.AttemptFailure),
            ):
                s.run(c, root, "redis://unused")
            launch.assert_not_called()
            self.assertEqual(len(set(seen)), 2)
            self.assertTrue(all((root / a / "interrupted.json").exists() for a in seen))
            self.assertEqual(s.read_state(root, s.identity(c))["stagnant"], 2)
            self.assertFalse((root / "SUCCESS.json").exists())

    def test_fatal_result_survives_crash_before_checkpoint_update(self):
        with (
            tempfile.TemporaryDirectory() as tmp,
            patch.object(s, "client", return_value=Mock()),
        ):
            root, c = Path(tmp), config()
            x = state()
            x.update(
                identity=s.identity(c),
                attempts=1,
                active={"id": "a" * 32, "progress": 9},
            )
            s.write_json_durable(root / "run.json", c)
            s.write_json_durable(root / "state.json", x)
            (root / x["active"]["id"]).mkdir()
            s.write_json_durable(
                root / x["active"]["id"] / "result.json",
                {
                    "version": 1,
                    "identity": s.identity(c),
                    "attempt": x["active"]["id"],
                    "outcome": "invariant",
                    "fatal": True,
                    "progress": 9,
                    "terminal": None,
                    "participants": {},
                },
            )
            with (
                patch.object(s, "attempt") as launch,
                self.assertRaises(s.AttemptFailure) as e,
            ):
                s.run(c, root, "redis://unused")
            self.assertTrue(e.exception.fatal)
            launch.assert_not_called()

    def test_config_closed_identity_and_path_safety(self):
        c = config()
        s.validate(c)
        for group in (
            "..",
            ".",
            "publisher",
            "ready",
            "publisher.json",
            "result.json",
            "interrupted.json",
        ):
            bad = copy.deepcopy(c)
            bad["transport"]["groups"][0] = group
            bad["strategies"][group] = bad["strategies"].pop("fast")
            with self.assertRaises(ProtocolError):
                s.validate(bad)
        bad = copy.deepcopy(c)
        bad["limits"]["poll_seconds"] = float("nan")
        with self.assertRaises(ProtocolError):
            s.validate(bad)
        self.assertNotEqual(s.identity(c), s.identity({**c, "python": "/another"}))


class Strategy:
    def __init__(self, context):
        self.context = context
        self.path = Path(context["output_directory"]) / "sequences.txt"

    def __call__(self, cut):
        with self.path.open("a") as f:
            f.write(f"{cut.sequence}\n")
        mode = self.context["config"].get("mode")
        if mode == "hook_transport":
            raise TransportError("a hook error must never be retried")
        if mode == "finish_failure" and cut.kind == "terminal":
            return
        if mode == "stall":
            (self.path.parent / "pid").write_text(str(os.getpid()))
            time.sleep(10)
        if mode == "ack_ambiguity":
            from replay.streams import Consumer

            original = Consumer._eval

            def ambiguous(consumer, *args):
                result = original(consumer, *args)
                if args[0] == "ack":
                    raise TransportError("lost ACK reply")
                return result

            Consumer._eval = ambiguous
        if (
            mode == "retry_once"
            and s.read(self.path.parents[3] / "state.json")["attempts"] == 1
        ):
            os._exit(21)

    def finish(self):
        if self.context["config"].get("mode") == "finish_failure":
            raise RuntimeError("failed final local processing")


def strategy(context):
    return Strategy(context)


def fake_publisher(path, ready, mode):
    """Real Redis wire, deliberate publisher faults; not production strategy."""
    from replay.streams.consumer import SCRIPT

    c = s.read(path)
    if mode in ("fatal", "early", "retry"):
        return {"fatal": 20, "early": 0, "retry": 21}[mode]
    r = s.client(os.environ["REDIS_URL"], 0.5)
    k = s.keys({"transport": c}, c["attempt_id"])
    r.eval(
        SCRIPT,
        2,
        *k,
        "setup",
        json.dumps(c["groups"]),
        c["max_queue_bytes"],
        c["max_entry_bytes"],
    )
    values = records()
    if mode == "truncated":
        values = values[:-1]
    for value in values:
        value["attempt_id"] = c["attempt_id"]
        seq = int(value["sequence"])
        r.eval(
            SCRIPT,
            2,
            *k,
            "publish",
            "-1" if seq == 0 else str(seq),
            str(seq + 1),
            json.dumps(value),
            "1" if value["kind"] == "terminal" else "0",
        )
    Path(ready).touch()
    r.close()
    return 0


@unittest.skipUnless(URL, "requires explicitly disposable REPLAY_REDIS_URL >=8.2")
class SubprocessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.env = patch.dict(os.environ, {"REDIS_URL": URL})
        self.env.start()
        self.redis = s.client(URL, 0.5)
        self.unrelated = "supervisor-unrelated-" + self.root.name
        self.redis.set(self.unrelated, "preserve")

    def tearDown(self):
        self.assertEqual(self.redis.get(self.unrelated), "preserve")
        self.redis.delete(self.unrelated)
        self.redis.close()
        self.env.stop()
        self.tmp.cleanup()

    def config(self, mode="normal"):
        exe = self.root / "publisher-executable"
        exe.write_text(
            f"#!{sys.executable}\nfrom replay.tests.test_supervisor import fake_publisher\nimport sys\nsys.exit(fake_publisher(sys.argv[1], sys.argv[2], {mode!r}))\n"
        )
        exe.chmod(0o700)
        return config(str(exe))

    def test_success_receipt_independent_readback_and_no_relaunch(self):
        c, root = self.config(), self.root / "run"
        receipt = s.run(c, root, URL)
        self.assertEqual(s.read_success(root), receipt)
        self.assertEqual(s.run(c, root, URL), receipt)
        for group, location in receipt["outputs"].items():
            seq = (root / location / "sequences.txt").read_text().splitlines()
            self.assertEqual(seq, [str(i) for i in range(len(records()))])
            self.assertEqual(self.redis.exists(*s.keys(c, receipt["attempt"])), 0)
        completion = root / receipt["attempt"] / "slow" / "complete.json"
        completion.unlink()
        with self.assertRaises(FileNotFoundError):
            s.read_success(root)

    def test_retry_fresh_books_sequence_namespace_and_partial_preserved(self):
        c, root = self.config(), self.root / "run"
        c["strategies"]["slow"]["config"]["mode"] = "retry_once"
        receipt = s.run(c, root, URL)
        self.assertEqual(s.read_state(root, s.identity(c))["attempts"], 2)
        attempts = [p for p in root.iterdir() if p.is_dir()]
        self.assertEqual(len(attempts), 2)
        for a in attempts:
            self.assertTrue(
                (a / "slow/output/sequences.txt").read_text().startswith("0\n")
            )
        self.assertEqual(
            len(
                (root / receipt["outputs"]["slow"] / "sequences.txt")
                .read_text()
                .splitlines()
            ),
            len(records()),
        )

    def test_fatal_hook_finish_and_publisher_errors_never_retry(self):
        for mode in ("hook_transport", "finish_failure", "publisher"):
            with self.subTest(mode=mode):
                c, root = (
                    self.config("fatal" if mode == "publisher" else "normal"),
                    self.root / mode,
                )
                if mode != "publisher":
                    c["strategies"]["slow"]["config"]["mode"] = mode
                with self.assertRaises(s.AttemptFailure) as e:
                    s.run(c, root, URL)
                self.assertTrue(e.exception.fatal)
                self.assertEqual(s.read_state(root, s.identity(c))["attempts"], 1)
                self.assertFalse((root / "SUCCESS.json").exists())

    def test_missing_terminal_stall_fast_group_not_progress_and_bounded_cleanup(self):
        for mode in ("stall", "truncated", "early", "retry"):
            with self.subTest(mode=mode):
                c, root = (
                    self.config("normal" if mode == "stall" else mode),
                    self.root / mode,
                )
                c["limits"].update(stall_seconds=0.6, attempts=2)
                if mode == "stall":
                    c["strategies"]["slow"]["config"]["mode"] = "stall"
                with self.assertRaises(s.AttemptFailure):
                    s.run(c, root, URL)
                x = s.read_state(root, s.identity(c))
                self.assertLessEqual(x["attempts"], 2)
                if mode == "stall":
                    self.assertEqual(x["best"], -1)
                self.assertFalse((root / "SUCCESS.json").exists())
                for a in root.iterdir():
                    if a.is_dir():
                        self.assertEqual(self.redis.exists(*s.keys(c, a.name)), 0)
                        self.assertTrue(
                            all(
                                v is not None
                                for v in s.read(a / "result.json")[
                                    "participants"
                                ].values()
                            )
                        )

    def test_ack_ambiguity_after_local_work_never_commits(self):
        c, root = self.config(), self.root / "ambiguous"
        c["strategies"]["slow"]["config"]["mode"] = "ack_ambiguity"
        with self.assertRaises(s.AttemptFailure):
            s.run(c, root, URL)
        self.assertFalse((root / "SUCCESS.json").exists())
        self.assertTrue(list(root.glob("*/slow/output/sequences.txt")))
        self.assertFalse(list(root.glob("*/slow/complete.json")))

    def test_no_ready_never_deletes_preexisting_namespace(self):
        c, root = self.config("fatal"), self.root / "collision"
        attempt = "a" * 32
        keys = s.keys(c, attempt)
        self.redis.set(keys[0], "preexisting-evidence")
        try:
            with (
                patch.object(s.uuid, "uuid4", return_value=Mock(hex=attempt)),
                self.assertRaises(s.AttemptFailure),
            ):
                s.run(c, root, URL)
            self.assertEqual(self.redis.get(keys[0]), "preexisting-evidence")
        finally:
            self.redis.delete(*keys)

    def test_crash_before_receipt_never_blesses_completed_provisional_attempt(self):
        c, root = self.config(), self.root / "receipt-crash"
        original = s.write_json_durable

        def crash(path, value):
            if path.name == "SUCCESS.json":
                self.assertEqual(value["terminal"], len(records()))
                for group in c["transport"]["groups"]:
                    s.attestation(
                        root / value["attempt"] / group / "complete.json",
                        s.identity(c),
                        value["attempt"],
                        group,
                    )
                raise OSError("before receipt commit")
            original(path, value)

        with (
            patch.object(s, "write_json_durable", side_effect=crash),
            self.assertRaises(OSError),
        ):
            s.run(c, root, URL)
        old = s.read_state(root, s.identity(c))["active"]["id"]
        self.assertFalse((root / "SUCCESS.json").exists())
        receipt = s.run(c, root, URL)
        self.assertNotEqual(receipt["attempt"], old)
        self.assertTrue((root / old / "interrupted.json").exists())
        self.assertEqual(s.read_state(root, s.identity(c))["attempts"], 2)

    def test_sigkill_parent_kills_owned_children_and_restart_consumes_budget(self):
        c, root = self.config(), self.root / "crash"
        c["strategies"]["slow"]["config"]["mode"] = "stall"
        c["limits"]["attempts"] = 1
        path = self.root / "config.json"
        s.write_json_durable(path, c)
        p = subprocess.Popen(
            [sys.executable, "-m", "replay.supervisor", str(path), str(root)]
        )
        try:
            deadline = time.monotonic() + 5
            files = []
            while not files and time.monotonic() < deadline:
                files = list(root.glob("*/slow/output/pid"))
                time.sleep(0.01)
            self.assertTrue(files)
            pid = int(files[0].read_text())
            with self.assertRaises(BlockingIOError):
                s.run(c, root, URL)
            p.kill()
            p.wait(timeout=2)
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                stat = Path(f"/proc/{pid}/stat")
                if not stat.exists() or stat.read_text().split()[2] == "Z":
                    break
                time.sleep(0.01)
            else:
                self.fail("owned child survived supervisor SIGKILL")
            with self.assertRaises(s.AttemptFailure):
                s.run(c, root, URL)
            self.assertEqual(s.read_state(root, s.identity(c))["attempts"], 1)
            self.assertFalse((root / "SUCCESS.json").exists())
            self.assertTrue(list(root.glob("*/interrupted.json")))
        finally:
            if p.poll() is None:
                p.kill()
                p.wait()


def real_publisher(config_path, publisher):
    """Rust test owns derivative lifetime while this acceptance test runs."""
    c = config(str(Path(publisher).resolve()))
    transport = s.read(config_path)
    del transport["attempt_id"]
    c["transport"] = transport
    c["limits"].update(stall_seconds=3, attempt_seconds=10, run_seconds=30)
    c["strategies"]["slow"]["config"]["mode"] = "retry_once"
    with (
        tempfile.TemporaryDirectory() as tmp,
        patch.dict(os.environ, {"REDIS_URL": URL}),
    ):
        root = Path(tmp)
        receipt = s.run(c, root, URL)
        assert s.read_success(root) == receipt
        assert s.read_state(root, s.identity(c))["attempts"] == 2
        attempts = [p for p in root.iterdir() if p.is_dir()]
        assert len(attempts) == 2
        for a in attempts:
            assert (a / "slow/output/sequences.txt").read_text().startswith("0\n")
        for location in receipt["outputs"].values():
            assert (root / location / "sequences.txt").read_text().splitlines() == [
                str(i) for i in range(len(records()))
            ]
        print(
            "Rust publisher -> two strategy processes: failed attempt retained; fresh retry sequence 0; receipt independently verified"
        )


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--real-publisher":
        real_publisher(*sys.argv[2:])
    else:
        unittest.main()
