"""Exact aggregate table encoding, shared by early admission and final commit.

Each pass resolves one scope at a time. Layout 2 chunks concatenate to
``encoded({"scopes": rows}) + b"\n"`` exactly. Layout 3 keeps one bounded,
deduplicated descriptor cache and streams the scoped index independently.
"""
import hashlib
import os
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.output import aggregate_files, group_of
from replay.preparation import encoded
from replay.strategy_sdk import LineWriter
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


def indexed_rows(strategy, snapshot, plans, group):
    for scope in range(len(snapshot["scopes"])):
        entities = resolve(strategy, snapshot, scope, plans)
        for index, row in enumerate(entity_rows(entities, group)):
            yield {"scope": scope, "entity": index, "hash": row["hash"]}


def descriptor_rows(strategy, snapshot, plans, group):
    """One bounded descriptor cache, not one descriptor copy per scope."""
    distinct, budget = {}, bounds.StateBudget()
    for scope in range(len(snapshot["scopes"])):
        entities = resolve(strategy, snapshot, scope, plans)
        for row in entity_rows(entities, group):
            key = row["hash"]
            if key in distinct:
                require(distinct[key] == row, "descriptor hash conflict")
            else:
                require(len(distinct) < bounds.MAX_ROWS, "entity table row budget")
                budget.charge(bounds.SLOT + bounds.json_cost(row), "entity table state budget")
                distinct[key] = row
    for key in sorted(distinct):
        yield distinct[key]


def table3_rows(strategy, snapshot, plans, group, name):
    return (descriptor_rows if name == "descriptors.ndjson" else indexed_rows)(
        strategy, snapshot, plans, group)


def write_rows(root, name, rows, *, expected_size=None):
    writer = LineWriter(Path(root) / name, max_bytes=bounds.MAX_BYTES,
                        max_records=bounds.MAX_ROWS, max_line_bytes=bounds.MAX_LINE)
    try:
        for row in rows:
            writer.append(row)
        require(expected_size is None or writer.size == expected_size,
                "entity metadata changed after preflight")
        return writer.finish()
    finally:
        writer.stream.close()


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
        if strategy.experiment.layout == 3:
            for leaf in ("descriptors.ndjson", "entities.ndjson"):
                size = count = 0
                for row in table3_rows(strategy, snapshot, plans, group, leaf):
                    length = len(encoded(row)) + 1
                    require(length <= bounds.MAX_LINE, "entity table line budget")
                    count += 1
                    size += length
                    require(count <= bounds.MAX_ROWS and size <= bounds.MAX_BYTES,
                            "entity table row budget")
                result[group + leaf] = size
            continue
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
