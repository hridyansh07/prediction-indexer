"""Bounded strict input reads and immutable local artifact commits."""
import hashlib
import json
import os
import re
from pathlib import Path

from archive.common.durable import fsync_directory
from archive.storage.base import normalize_key
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.streams.protocol import decode

METADATA = 32 * 1024**2
MAX_LINE = 1024**2
MAX_INPUT = 16 * 1024**3
MAX_ROWS = 50_000_000


def need(condition, message):
    if not condition:
        raise ValueError(message)


def encoded(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(value):
    return hashlib.sha256(encoded(value).rstrip(b"\n")).hexdigest()


def safe_path(value):
    path = Path(value).absolute()
    need(not any(p.startswith(".env") or p.endswith((".key", ".pem")) or p in ("secrets", "credentials")
                 for p in path.parts), "secret path forbidden")
    return path


def child(root, name):
    normalize_key(name)
    result = safe_path(root) / name
    need(result.resolve().is_relative_to(Path(root).resolve()), "input path escape")
    current = result
    while current != safe_path(root):
        need(not current.is_symlink(), "input symlink")
        current = current.parent
    return result


def identity(path, *, maximum=MAX_INPUT):
    path = safe_path(path)
    need(path.is_file() and not path.is_symlink(), "regular input required")
    need(path.stat().st_size <= maximum, "input byte limit")
    checksum, size = hashlib.sha256(), 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            size += len(chunk)
            need(size <= maximum, "input byte limit")
            checksum.update(chunk)
    return {"sha256": checksum.hexdigest(), "byte_length": size}


def document(path, *, maximum=METADATA):
    path = safe_path(path)
    need(path.is_file() and not path.is_symlink(), "regular input required")
    with path.open("rb") as stream:
        return decode(stream.read(maximum + 1), maximum)


def closed(value, fields):
    need(type(value) is dict and set(value) == set(fields.split()), "closed schema: " + fields)
    return value


def natural(value):
    need(type(value) is str and re.fullmatch(r"0|[1-9][0-9]{0,79}", value), "canonical natural")
    return int(value)


def rows(root, name, expected, *, max_line=MAX_LINE):
    path = child(root, name)
    if name.endswith(".zst"):
        closed(expected, "logical stored")
        logical = closed(expected["logical"], "sha256 byte_length records")
        closed(expected["stored"], "sha256 byte_length")
        need(0 <= logical["byte_length"] <= MAX_INPUT and 0 <= logical["records"] <= MAX_ROWS, "stream limit")
        source = stream_lines(path, logical, expected["stored"], max_line, name)
    else:
        closed(expected, "sha256 byte_length records")
        need(identity(path) == {k: expected[k] for k in ("sha256", "byte_length")}, "file identity")
        def lines():
            with path.open("rb") as stream:
                while line := stream.readline(max_line + 1):
                    need(len(line) <= max_line and line.endswith(b"\n"), "line/truncation")
                    yield line
        source = lines()
        logical = expected
    count = 0
    try:
        for line in source:
            count += 1
            need(count <= logical["records"] and count <= MAX_ROWS, "row limit")
            yield decode(line, max_line)
        need(count == logical["records"], "row count")
    finally:
        source.close()


def write(path, value):
    return write_chunks(path, (encoded(value),))


def write_chunks(path, chunks, *, maximum=METADATA):
    """No replacement, receipt-last callers; failure cannot leave a durable marker."""
    path = safe_path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".open")
    linked, created, size = False, False, 0
    try:
        with temporary.open("xb") as stream:
            created = True
            for chunk in chunks:
                size += len(chunk)
                need(size <= maximum, "JSON output limit")
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        linked = True
        fsync_directory(path.parent)
    except BaseException:
        if linked:
            path.unlink(missing_ok=True)
        raise
    finally:
        if created:
            temporary.unlink(missing_ok=True)
    return identity(path)
