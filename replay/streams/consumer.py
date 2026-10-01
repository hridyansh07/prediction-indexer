"""Finite polling and hook-before-ACK. No reconnect, claim, retry or resume."""

from hashlib import sha1
from importlib.resources import files
from urllib.parse import urlsplit

from .protocol import Decoder, ProtocolError, TransportError, array, obj, require, uint

SCRIPT = files(__package__).joinpath("attempt.lua").read_text()
SCRIPT_SHA = sha1(SCRIPT.encode()).hexdigest()


DEFAULT_BATCH_BYTES = 134_217_728  # 128 MiB: 128 entries at a 1 MiB entry cap


class Consumer:
    def __init__(
        self,
        url,
        *,
        scope,
        run_id,
        attempt_id,
        group,
        initial,
        timeout=5.0,
        batch_entries=None,
        batch_bytes=DEFAULT_BATCH_BYTES,
    ):
        """`batch_entries=None` reads as many whole entries as `batch_bytes`
        admits at the attempt's entry cap, at most 1024."""
        import redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        # redis-py URL query parameters override keyword timeout/retry options.
        require(not urlsplit(url).query, "Redis URL options are not permitted")
        require(0 < timeout <= 60)
        obj(
            initial,
            "pins start_ns end_ns lower_bound plans groups max_entry_bytes max_queue_bytes",
        )
        entry = uint(initial["max_entry_bytes"])
        if batch_entries is None:
            batch_entries = min(1024, batch_bytes // entry) if entry > 0 else 0
        require(type(batch_entries) is int and 0 < batch_entries <= 1024)
        require(0 < entry <= batch_bytes and batch_entries * entry <= batch_bytes)
        require(group in array(initial["groups"]))
        self._redis = redis.Redis.from_url(
            url,
            socket_connect_timeout=timeout,
            socket_timeout=timeout,
            retry=Retry(NoBackoff(), 0),
            retry_on_error=[],
            health_check_interval=0,
            decode_responses=False,
        )
        self._keys = [
            f"replay:{scope}:{run_id}:{attempt_id}:{suffix}"
            for suffix in ("stream", "state")
        ]
        self._group = group
        self._decoder = Decoder(run_id, attempt_id, initial, entry)
        self._batch_entries, self._batch_bytes = batch_entries, batch_bytes
        self._timeout = timeout
        self._completed = -1
        self._poisoned = False
        try:
            self._eval("join", group)
        except Exception as e:
            self._fail(e)

    @property
    def books(self):
        return self._decoder.books

    @property
    def terminal(self):
        return self._decoder.terminal and not self._poisoned

    def _eval(self, *args):
        from redis.exceptions import NoScriptError

        try:
            return self._redis.evalsha(SCRIPT_SHA, 2, *self._keys, *args)
        except NoScriptError:
            # NOSCRIPT guarantees no execution; never retry an ambiguous failure.
            return self._redis.eval(SCRIPT, 2, *self._keys, *args)

    def _fail(self, error):
        self._poisoned = True
        self._decoder.poisoned = True
        try:
            self._redis.hset(self._keys[1], "poisoned", "1")
        except Exception:
            pass
        if isinstance(error, ProtocolError):
            raise error
        # Preserve caller exceptions; don't pretend they are Redis retry evidence.
        import redis

        if isinstance(error, redis.ResponseError) and str(error).startswith("REPLAY "):
            raise ProtocolError("Redis attempt invariant") from error
        if isinstance(error, redis.RedisError):
            raise TransportError("Redis command failed; attempt poisoned") from error
        raise error

    def poll(self, hook, *, block_ms=100):
        """Apply each cut and call hook(cut) in order, then ACK the batch once.

        Returns processed entry count, including control records. Empty finite
        poll is not EOF. The caller must impose an attempt deadline and finish().
        A hook exception poisons the attempt and nothing in the batch is ACKed.
        Books and cut bodies are valid only during their hook invocation.
        """
        require(not self._poisoned, "poisoned consumer")
        require(0 < block_ms < self._timeout * 1000)
        if self.terminal:
            return 0
        try:
            self._eval("check")
            batches = self._redis.xreadgroup(
                self._group,
                "worker",
                {self._keys[0]: ">"},
                count=self._batch_entries,
                block=block_ms,
            )
            entries = []
            for stream, batch in batches:
                require(stream == self._keys[0].encode())
                entries.extend(batch)
            require(len(entries) <= self._batch_entries)
            require(
                sum(len(v) for _, fields in entries for v in fields.values())
                <= self._batch_bytes,
                "batch limit",
            )
            ids = []
            for entry_id, fields in entries:
                require(len(fields) == 1 and b"record" in fields, "entry fields")
                require(
                    entry_id == f"{self._decoder.sequence + 2}-0".encode(),
                    "entry sequence",
                )
                hook(self._decoder.apply(fields[b"record"]))
                ids.append(entry_id)
            if ids:
                last = self._decoder.sequence + 1
                self._eval("ack", self._group, str(self._completed), str(last), *ids)
                self._completed = last
            return len(entries)
        except Exception as e:
            self._fail(e)

    def progress(self):
        require(not self._poisoned, "poisoned consumer")
        try:
            raw = self._eval("check")
            return {raw[i].decode(): raw[i + 1].decode() for i in range(0, len(raw), 2)}
        except Exception as e:
            self._fail(e)

    def finish(self):
        """Validate local terminal and ALL registered groups' hook completions.

        False means another group is still processing, never success. This is a
        completion fact, not a receipt writer or a strategy finalizer.
        """
        try:
            self._decoder.finish()
        except Exception as e:
            self._fail(e)
        p = self.progress()
        return bool(p["terminal"]) and all(
            p[f"done:{g}"] == p["terminal"] for g in self._decoder._expected["groups"]
        )

    def abort(self):
        self._fail(ProtocolError("caller aborted attempt"))

    def close(self):
        self._redis.close()
