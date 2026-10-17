"""Committed per-bundle corpus results: copied bench output plus a closed receipt written last."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

RECEIPT = "corpus_receipt.json"
VERSION = 1
FIELDS = {"corpus_receipt_version", "corpus_key", "bundle_id", "status", "bench_exit", "game_state",
          "excluded_groups", "files"}


def _hash(path):
    digest, length = hashlib.sha256(), 0
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 22):
            digest.update(chunk)
            length += len(chunk)
    return digest.hexdigest(), length


def _copy(source, destination):
    destination.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as reader, destination.open("xb") as writer:   # never overwrite
        while chunk := reader.read(1 << 22):
            writer.write(chunk)
        writer.flush()
        os.fsync(writer.fileno())


def commit(bench_output, destination, *, corpus_key, bundle_id, status, bench_exit, game_state,
           excluded_groups=(), inputs=(), durable=False):
    """Copy every regular file of a finished bench output, hash it, then write the receipt last.

    `inputs` are (relative path, source) pairs kept beside the outputs: the prepared
    context and game state, which the bench only mounts under in/.
    `durable` also syncs directory entries before and after receipt publication;
    callers discarding their only private inputs must require this gate.
    """
    bench_output, destination = Path(bench_output), Path(destination)
    if destination.exists():
        raise FileExistsError(f"corpus result already exists: {destination}")
    directories = set()
    if durable:
        from replay.supervisor import fsync_directory
        # Include the existing ancestor that will gain the first new entry.
        parent = destination
        while not parent.exists():
            directories.add(parent)
            parent = parent.parent
        directories.add(parent)
    sources = {}
    for path in sorted(p for p in bench_output.rglob("*") if p.is_file() and not p.is_symlink()):
        relative = path.relative_to(bench_output).as_posix()
        if not relative.startswith("in/"):   # mounted inputs, not results
            sources[relative] = path
    for relative, path in inputs:
        if relative in sources:
            raise FileExistsError(f"corpus input collides with a bench output: {relative}")
        sources[relative] = Path(path)
    files = []
    for relative, path in sorted(sources.items()):
        _copy(path, destination / relative)
        sha, length = _hash(destination / relative)
        files.append({"path": relative, "byte_length": length, "sha256": sha})
        if durable:
            parent = (destination / relative).parent
            while parent != destination:
                directories.add(parent)
                parent = parent.parent
    if durable:
        destination.mkdir(parents=True, exist_ok=True)
        # Files were individually fsynced above. Persist their names and every
        # newly created ancestor before publishing the commit marker.
        for directory in sorted(directories, key=lambda p: (-len(p.parts), str(p))):
            fsync_directory(directory)
    receipt = {"corpus_receipt_version": VERSION, "corpus_key": corpus_key, "bundle_id": bundle_id,
               "status": status, "bench_exit": bench_exit, "game_state": game_state,
               "excluded_groups": list(excluded_groups), "files": files}
    raw = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with (destination / RECEIPT).open("xb") as writer:
        writer.write(raw)
        writer.flush()
        os.fsync(writer.fileno())
    if durable:
        fsync_directory(destination)
    return hashlib.sha256(raw).hexdigest()


def verify(destination):
    """Independent reader: the receipt is closed and every listed file matches its hash."""
    destination = Path(destination)
    receipt = json.loads((destination / RECEIPT).read_bytes())
    if set(receipt) != FIELDS or receipt["corpus_receipt_version"] != VERSION:
        raise ValueError("corpus receipt schema")
    listed = set()
    for row in receipt["files"]:
        if set(row) != {"path", "byte_length", "sha256"} or row["path"] in listed or ".." in row["path"].split("/"):
            raise ValueError("corpus receipt file row")
        listed.add(row["path"])
        if _hash(destination / row["path"]) != (row["sha256"], row["byte_length"]):
            raise ValueError(f"corpus result file mismatch: {row['path']}")
    present = {p.relative_to(destination).as_posix() for p in destination.rglob("*")
               if p.is_file() and p.name != RECEIPT and not p.name.startswith("._")}
    if present != listed:
        raise ValueError("corpus result has unlisted or missing files")
    return receipt
