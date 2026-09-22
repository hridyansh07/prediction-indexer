"""Finite polling and hook-before-ACK. No reconnect, claim, retry or resume."""

from importlib.resources import files
from urllib.parse import urlsplit

from .protocol import Decoder, ProtocolError, TransportError, require

SCRIPT = files(__package__).joinpath("attempt.lua").read_text()


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
        batch_entries=16,
        batch_bytes=16_777_216,
    ):
        import redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry

        # redis-py URL query parameters override keyword timeout/retry options.
        require(not urlsplit(url).query, "Redis URL options are not permitted")
        require(0 < timeout <= 60 and 0 < batch_entries <= 1024)
        entry = int(initial["max_entry_bytes"])
        require(0 < entry <= batch_bytes and batch_entries * entry <= batch_bytes)
        require(group in initial["groups"])
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
        """Apply complete cuts, call hook(cut), then ACK each successful hook.

        Returns processed entry count, including control records. Empty finite
        poll is not EOF. The caller must impose an attempt deadline and finish().
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
            for entry_id, fields in entries:
                require(set(fields) == {b"record"}, "entry fields")
                require(
                    entry_id == f"{self._decoder.sequence + 2}-0".encode(),
                    "entry sequence",
                )
                cut = self._decoder.apply(fields[b"record"])
                hook(cut)
                self._eval(
                    "ack",
                    self._group,
                    str(self._completed),
                    str(cut.sequence + 1),
                    entry_id,
                )
                self._completed = cut.sequence + 1
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
