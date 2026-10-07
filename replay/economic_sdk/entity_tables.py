"""Exact layout-2 entity encoding, shared by early admission and final commit.

Each pass resolves one scope at a time; no cross-scope descriptor cache is kept.
Chunks concatenate to ``encoded({"scopes": rows}) + b"\n"`` exactly.
"""
import hashlib
import os
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.output import aggregate_files, group_of
from replay.preparation import encoded
from replay.streams.protocol import require
from replay.supervisor import fsync_directory


def entity_rows(entities, group):
    return [{"hash": e.id, "descriptor": e.descriptor}
            for e in sorted(entities.values(), key=lambda e: e.order) if group_of(e) == group]


def table_chunks(strategy, snapshot, plans, group):
    yield b'{"scopes":['
    for index in range(len(snapshot["scopes"])):
        if index:
            yield b","  # scope separator, including empty scopes
        entities = resolve(strategy, snapshot, index, plans)
        yield b"["
        first = True
        for row in entity_rows(entities, group):
            if not first:
                yield b","  # row separator
            yield encoded(row)
            first = False
        yield b"]"
        del entities
    yield b"]}\n"


def preflight(strategy, snapshot=None, plans=None):
    """Return exact bytes per table; fail before creating any output file.

    Layout 1 has no entity table and keeps its frozen output behavior.
    """
    if strategy.experiment.layout == 1:
        return {}
    snapshot = strategy.snapshot if snapshot is None else snapshot
    plans = ({(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
             if plans is None else plans)
    result = {}
    for group in aggregate_files(strategy.experiment):
        name = group + "entities.json"
        size = sum(len(chunk) for chunk in table_chunks(strategy, snapshot, plans, group))
        require(size <= bounds.MAX_METADATA,
                f"entity metadata preflight: {name} requires {size} bytes; limit {bounds.MAX_METADATA}")
        result[name] = size
    return result


def write_table(root, name, chunks, *, expected_size=None):
    """Commit one canonical table, hashing the exact streamed bytes persisted."""
    path = Path(root) / name
    temporary = path.with_suffix(path.suffix + ".open")
    require(not path.exists(), "existing output")
    size, identity = 0, hashlib.sha256()
    with temporary.open("xb") as stream:
        for chunk in chunks:
            size += len(chunk)
            require(size <= bounds.MAX_METADATA, "output line budget")
            stream.write(chunk)
            identity.update(chunk)
        require(expected_size is None or size == expected_size,
                "entity metadata changed after preflight")
        stream.flush()
        os.fsync(stream.fileno())
    require(not path.exists(), "existing output")
    temporary.rename(path)
    fsync_directory(path.parent)
    return {"sha256": identity.hexdigest(), "byte_length": size, "records": 1}
