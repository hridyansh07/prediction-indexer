"""Streaming reads of the market profile's Zstandard row files.

``stream_lines`` decodes one committed frame through the shared codec in two
passes and never writes a decoded byte to disk: pass one verifies both
identities with a sink that keeps nothing, so no row is judged from an
unverified frame; pass two decodes the same frame again in a worker thread and
hands each line to the caller as it forms. The caller pulls, so two files can be
read in lockstep with memory bounded by a few chunks.

It is codec plumbing shared by the independent reader and the SDK iterators; it
knows nothing about rows and imports neither the collector nor the recorders.
"""

from __future__ import annotations

import queue
import threading
from pathlib import Path

from encoder import CodecError, LogicalIdentity, StoredIdentity, decode_stream
from replay.streams.protocol import require

_CHUNKS = 4  # decoded chunks in flight between the codec and the consumer


class _Discard:
    def write(self, data):
        return len(data)

    def flush(self):
        pass


class _Abandoned(Exception):
    pass


class _Pipe:
    """A bounded hand-off from the decoding thread to the consumer."""

    def __init__(self):
        self.chunks = queue.Queue(maxsize=_CHUNKS)
        self.abandoned = threading.Event()

    def put(self, item):
        while True:
            if self.abandoned.is_set():
                raise _Abandoned
            try:
                self.chunks.put(item, timeout=0.05)
                return
            except queue.Full:
                continue

    def write(self, data):
        self.put(bytes(data))
        return len(data)

    def flush(self):
        pass


def _expected(logical, stored):
    return dict(expected_logical=LogicalIdentity(logical["sha256"], logical["byte_length"], logical["records"]),
                expected_stored=StoredIdentity(stored["sha256"], stored["byte_length"]),
                max_decoded_bytes=logical["byte_length"])


def verify(path, logical, stored, label):
    """Pass one: both identities, nothing kept."""
    try:
        with Path(path).open("rb") as source:
            decode_stream(source, _Discard(), **_expected(logical, stored))
    except CodecError as error:
        require(False, f"{label} codec: {error}")


def stream_lines(path, logical, stored, max_line, label):
    """Yield every line (with its LF) of a verified frame; fail on any codec or line fault.

    The generator must be exhausted or closed; closing stops the worker.
    """
    verify(path, logical, stored, label)
    pipe = _Pipe()
    failure = []

    def work():
        try:
            with Path(path).open("rb") as source:
                decode_stream(source, pipe, **_expected(logical, stored))
            pipe.put(None)
        except _Abandoned:
            pass
        except BaseException as error:  # delivered to the consumer
            failure.append(error)
            try:
                pipe.put(None)
            except _Abandoned:
                pass

    worker = threading.Thread(target=work, name=f"{label}-decode", daemon=True)
    worker.start()
    partial = b""
    try:
        while (chunk := pipe.chunks.get()) is not None:
            parts = (partial + chunk).split(b"\n")
            partial = parts.pop()
            for part in parts:
                require(len(part) < max_line, f"{label} line/truncation")
                yield part + b"\n"
            require(len(partial) < max_line, f"{label} line/truncation")
        if failure:
            error = failure[0]
            if isinstance(error, CodecError):
                require(False, f"{label} codec: {error}")
            raise error
        require(not partial, f"{label} line/truncation")
    finally:
        pipe.abandoned.set()
        while True:
            try:
                pipe.chunks.get_nowait()
            except queue.Empty:
                break
        worker.join()
