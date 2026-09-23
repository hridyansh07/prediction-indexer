"""Small offline strategy boundary: pinned inputs and bounded durable NDJSON."""

import hashlib
import os
from collections.abc import Mapping
from pathlib import Path

from replay.preparation import encoded, load_snapshot, sha
from replay.streams.protocol import obj, require
from replay.supervisor import fsync_directory


def plain(value):
    """Copy immutable SDK metadata into JSON containers, without copying books."""
    if isinstance(value, Mapping):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    return value


class PreparedInput:
    def __init__(self, config):
        config = obj(plain(config), "version snapshot_directory snapshot_sha256")
        require(type(config["version"]) is int and config["version"] == 1)
        sha(config["snapshot_sha256"])
        require(type(config["snapshot_directory"]) is str)
        self.sha256 = config["snapshot_sha256"]
        self.snapshot = load_snapshot(
            config["snapshot_directory"], expected_sha256=self.sha256
        )
        require(
            self.snapshot["plans"],
            "no native risk plans: entirely uncaptured probe unsupported by transport",
        )
        self.bound = False

    def bind(self, initial):
        require(not self.bound, "initial already bound")
        snapshot = self.snapshot
        for field in ("pins", "start_ns", "end_ns", "lower_bound"):
            require(
                initial[field] == snapshot["config"][field],
                "snapshot/transport " + field,
            )
        require(initial["plans"] == snapshot["plans"], "snapshot/transport plans")
        self.bound = True


class LineWriter:
    """Exclusive provisional file, one encode/hash/write, bounded independently of tape."""

    def __init__(self, path, *, max_bytes, max_records, max_line_bytes):
        self.path = Path(path)
        self.temporary = self.path.with_suffix(self.path.suffix + ".open")
        require(not self.path.exists(), "existing output")
        self.stream = self.temporary.open("xb")
        self.max_bytes, self.max_records, self.max_line_bytes = (
            max_bytes,
            max_records,
            max_line_bytes,
        )
        self.size = self.count = 0
        self.hash = hashlib.sha256()

    def append(self, value):
        payload = encoded(value) + b"\n"
        require(len(payload) <= self.max_line_bytes, "output line budget")
        require(
            self.size + len(payload) <= self.max_bytes
            and self.count < self.max_records,
            "output budget",
        )
        self.stream.write(payload)
        self.hash.update(payload)
        self.size += len(payload)
        self.count += 1

    def finish(self):
        self.stream.flush()
        os.fsync(self.stream.fileno())
        self.stream.close()
        require(not self.path.exists(), "existing output")
        self.temporary.rename(self.path)
        fsync_directory(self.path.parent)
        return {
            "sha256": self.hash.hexdigest(),
            "byte_length": self.size,
            "records": self.count,
        }
