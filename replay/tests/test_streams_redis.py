"""Opt-in disposable Redis only; OOM and pause tests change server settings."""

import json
import os
import sys
import time
import unittest
import uuid
from pathlib import Path

from replay.streams import Consumer, ProtocolError, TransportError
from replay.streams.consumer import SCRIPT
from replay.tests.test_streams import assert_contract, encoded, records

URL = os.environ.get("REPLAY_REDIS_URL")


@unittest.skipUnless(URL, "requires explicitly disposable REPLAY_REDIS_URL >=8.2")
class RedisTests(unittest.TestCase):
    def setUp(self):
        import redis

        self.redis = redis.Redis.from_url(
            URL, socket_timeout=2, socket_connect_timeout=2
        )
        self.attempt = uuid.uuid4().hex
        self.keys = [
            f"replay:test:contract:{self.attempt}:{s}" for s in ("stream", "state")
        ]
        self.values = records()
        for r in self.values:
            r["attempt_id"] = self.attempt
        self.initial = self.values[0]["body"]
        self.consumers = []
        self.redis.eval(
            SCRIPT,
            2,
            *self.keys,
            "setup",
            json.dumps(self.initial["groups"]),
            self.initial["max_queue_bytes"],
            self.initial["max_entry_bytes"],
        )

    def tearDown(self):
        for c in self.consumers:
            c.close()
        self.redis.delete(*self.keys)
        self.redis.close()

    def publish(self, values=None):
        values = self.values if values is None else values
        for r in values:
            seq = int(r["sequence"])
            self.redis.eval(
                SCRIPT,
                2,
                *self.keys,
                "publish",
                "-1" if seq == 0 else str(seq),
                str(seq + 1),
                encoded(r),
                "1" if r["kind"] == "terminal" else "0",
            )

    def consumer(self, group="fast", **kwargs):
        c = Consumer(
            URL,
            scope="test",
            run_id="contract",
            attempt_id=self.attempt,
            group=group,
            initial=self.initial,
            **kwargs,
        )
        self.consumers.append(c)
        return c

    def test_slow_group_unread_protection_last_ack_delete_and_hook_order(self):
        self.publish()
        fast, slow = self.consumer(), self.consumer("slow")
        cuts = []

        def hook(cut):
            previous = b"-1" if cut.sequence == 0 else str(cut.sequence).encode()
            self.assertEqual(self.redis.hget(self.keys[1], "done:fast"), previous)
            self.assertGreater(self.redis.xpending(self.keys[0], "fast")["pending"], 0)
            cuts.append(cut)

        self.assertEqual(fast.poll(hook), len(self.values))
        assert_contract(self, cuts)
        self.assertEqual(self.redis.xlen(self.keys[0]), len(self.values))
        self.assertFalse(fast.finish())
        slow_cuts = []
        self.assertEqual(slow.poll(slow_cuts.append), len(self.values))
        assert_contract(self, slow_cuts)
        self.assertTrue(fast.finish())
        self.assertTrue(slow.finish())
        self.assertEqual(self.redis.xlen(self.keys[0]), 0)
        self.assertEqual(self.redis.hget(self.keys[1], "bytes"), b"0")

    def test_caller_failure_no_ack_and_no_reapplication(self):
        self.publish()
        c = self.consumer()
        seen = []

        def fail(cut):
            seen.append(cut.sequence)
            raise RuntimeError("strategy failed")

        with self.assertRaisesRegex(RuntimeError, "strategy failed"):
            c.poll(fail)
        self.assertEqual(self.redis.hget(self.keys[1], "done:fast"), b"-1")
        self.assertEqual(
            self.redis.xpending(self.keys[0], "fast")["pending"], len(self.values)
        )
        with self.assertRaises(ProtocolError):
            c.poll(fail)
        self.assertEqual(seen, [0])
        self.assertEqual(self.redis.hget(self.keys[1], "poisoned"), b"1")

    def test_duplicate_entry_and_membership_change_fail(self):
        self.publish(self.values[:1])
        c = self.consumer()
        c.poll(lambda _: None)
        self.redis.xadd(self.keys[0], {"record": encoded(self.values[0])}, id="2-0")
        with self.assertRaisesRegex(ProtocolError, "sequence"):
            c.poll(lambda _: self.fail("duplicate callback"))

    def test_fixed_membership_and_single_join(self):
        c = self.consumer()
        with self.assertRaises(ProtocolError):
            self.consumer()
        with self.assertRaises(ProtocolError):
            c.poll(lambda _: None)

    def test_group_removal_is_detected(self):
        c = self.consumer()
        self.redis.xgroup_destroy(self.keys[0], "slow")
        with self.assertRaises(ProtocolError):
            c.poll(lambda _: None)

    def test_url_cannot_override_finite_timeouts(self):
        with self.assertRaises(ProtocolError):
            Consumer(
                URL + "?socket_timeout=0",
                scope="test",
                run_id="contract",
                attempt_id=self.attempt,
                group="fast",
                initial=self.initial,
            )

    def test_truncated_tail_never_finishes(self):
        self.publish(self.values[:-1])
        c = self.consumer()
        c.poll(lambda _: None)
        self.assertEqual(c.poll(lambda _: None, block_ms=10), 0)
        with self.assertRaisesRegex(ProtocolError, "missing terminal"):
            c.finish()

    def test_timeout_poison(self):
        c = self.consumer(timeout=0.05)
        self.redis.execute_command("CLIENT", "PAUSE", 200, "ALL")
        with self.assertRaises(TransportError):
            c.poll(lambda _: None, block_ms=10)
        with self.assertRaises(ProtocolError):
            c.poll(lambda _: None, block_ms=10)
        time.sleep(0.25)

    def test_oom_poisons_consumer_without_dropping_unread_data(self):
        self.publish()
        c = self.consumer()
        old = self.redis.config_get("maxmemory")["maxmemory"]
        try:
            self.redis.config_set("maxmemory", 1)
            # Redis allows reads and memory-releasing ACKs while over budget.
            # Force an actual OOM error in provisional caller processing instead
            # of falsely assuming XREADGROUP itself must reject an existing PEL.
            with self.assertRaises(TransportError):
                c.poll(
                    lambda _: self.redis.hset(self.keys[1], "provisional", "x" * 1000)
                )
            with self.assertRaises(ProtocolError):
                c.poll(lambda _: None)
            self.assertEqual(self.redis.xlen(self.keys[0]), len(self.values))
            self.assertEqual(self.redis.hget(self.keys[1], "done:fast"), b"-1")
        finally:
            self.redis.config_set("maxmemory", old)

    def test_queue_limit_retains_unread_prefix(self):
        self.publish(self.values[:1])
        self.redis.hset(self.keys[1], "limit", 1)
        import redis

        with self.assertRaisesRegex(redis.ResponseError, "resource_limit"):
            self.publish(self.values[1:2])
        self.assertEqual(self.redis.xlen(self.keys[0]), 1)
        self.assertEqual(self.redis.hget(self.keys[1], "published"), b"1")


def existing(initial_path, attempt):
    """Called by Rust integration test after the real CLI has published."""
    test = unittest.TestCase()
    initial = json.loads(Path(initial_path).read_text())
    consumers = [
        Consumer(
            URL,
            scope="test",
            run_id="contract",
            attempt_id=attempt,
            group=g,
            initial=initial,
        )
        for g in initial["groups"]
    ]
    try:
        for consumer in consumers:
            cuts = []
            while not consumer.terminal:
                test.assertGreater(consumer.poll(cuts.append), 0)
            assert_contract(test, cuts)
        for consumer in consumers:
            test.assertTrue(consumer.finish())
        print(
            "Rust CLI -> Redis -> two Python consumers: exact cuts, terminal and all-group completion verified"
        )
    finally:
        consumers[0]._redis.delete(*consumers[0]._keys)
        for consumer in consumers:
            consumer.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--existing":
        existing(*sys.argv[2:])
    else:
        unittest.main()
