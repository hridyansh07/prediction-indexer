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
           excluded_groups=()):
    """Copy every regular file of a finished bench output, hash it, then write the receipt last."""
    bench_output, destination = Path(bench_output), Path(destination)
    if destination.exists():
        raise FileExistsError(f"corpus result already exists: {destination}")
    files = []
    for path in sorted(p for p in bench_output.rglob("*") if p.is_file() and not p.is_symlink()):
        relative = path.relative_to(bench_output).as_posix()
        if relative.startswith("in/"):   # mounted inputs, not results
            continue
        _copy(path, destination / relative)
        sha, length = _hash(destination / relative)
        files.append({"path": relative, "byte_length": length, "sha256": sha})
    receipt = {"corpus_receipt_version": VERSION, "corpus_key": corpus_key, "bundle_id": bundle_id,
               "status": status, "bench_exit": bench_exit, "game_state": game_state,
               "excluded_groups": list(excluded_groups), "files": files}
    raw = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with (destination / RECEIPT).open("xb") as writer:
        writer.write(raw)
        writer.flush()
        os.fsync(writer.fileno())
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
