"""Canonical JSON bytes and durable file writes for a research build."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

MAX_SAFE_INTEGER = 2 ** 53 - 1


class ResearchError(ValueError):
    """An input or build fault; the message names what failed."""


def require(condition, message):
    if not condition:
        raise ResearchError(message)


def dumps(value):
    """UTF-8, sorted keys, compact separators, final LF."""
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode()


def read_json(path):
    return json.loads(Path(path).read_bytes())


def identity(data):
    return {"sha256": hashlib.sha256(data).hexdigest(), "byte_length": len(data)}


def file_identity(path):
    digest, length = hashlib.sha256(), 0
    with Path(path).open("rb") as handle:
        while chunk := handle.read(1 << 22):
            digest.update(chunk)
            length += len(chunk)
    return {"sha256": digest.hexdigest(), "byte_length": length}


def write_bytes(path, data):
    """Create a new file, fsync it; never overwrite."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    return identity(data)


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def safe_int(value, label):
    """A JSON integer for the site: fail rather than lose precision in a browser."""
    value = int(value)
    require(-MAX_SAFE_INTEGER <= value <= MAX_SAFE_INTEGER, f"{label} exceeds 2^53 - 1")
    return value
