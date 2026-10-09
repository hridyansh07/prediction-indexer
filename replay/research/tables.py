"""Closed V1 schemas. Exact decimals remain strings, never floating point."""
import pyarrow as pa
import pyarrow.parquet as pq

from replay.research.io import child, encoded, identity, need, write_chunks

TEXT = pa.string()
TEXTS = pa.list_(pa.field("element", TEXT))
LENS = pa.struct([("lens", TEXT), ("state", TEXT), ("error", TEXT), ("episode_count", pa.int32())])
SCHEMAS = {
    "events": pa.schema([("event_id", TEXT), ("bundle_id", TEXT), ("game", TEXT), ("title", TEXT),
                         ("capture_start_ns", TEXT), ("duration_s", TEXT), ("venues", TEXTS),
                         ("book_count", pa.int16()), ("scope_count", pa.int16()), ("game_state", TEXT),
                         ("pack", TEXT), ("pack_error", TEXT), ("lenses", pa.list_(pa.field("element", LENS)))],
                        metadata={b"event_research_table_version": b"1"}),
    "episodes": pa.schema([("event_id", TEXT), ("lens", TEXT), ("episode_id", TEXT), ("start_ns", TEXT),
                           ("end_ns", TEXT), ("duration_ms", TEXT), ("books", pa.list_(pa.field("element", pa.int16()))),
                           ("markets", TEXTS), ("venues", TEXTS), ("venue_pair", TEXT), ("censored", pa.bool_()),
                           ("end_reason", TEXT), ("net", TEXT), ("net_unit", TEXT), ("quantity", TEXTS),
                           ("seconds_to_capture_end", TEXT), ("route_id", TEXT), ("shares_leg_with", pa.int32())],
                          metadata={b"event_research_table_version": b"1"})}
MAX_TABLE = 256 * 1024**2
MAX_TABLE_ROWS = 1_000_000


def write_table(root, name, records):
    """Bounded Arrow batches and receipt-owned immutable file, no internal compression."""
    schema = SCHEMAS[name]
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory(prefix="parquet-", dir=root) as temporary:
        source = Path(temporary) / "table.parquet"
        with pq.ParquetWriter(source, schema, compression="NONE", use_dictionary=False,
                              write_statistics=False, version="2.6") as writer:
            batch, count, batch_bytes = [], 0, 0
            for record in records:
                need(set(record) == set(schema.names), "closed table row")
                count += 1
                need(count <= MAX_TABLE_ROWS, "table row bound")
                size = len(encoded(record))
                need(size <= 1024**2, "table record byte bound")
                batch_bytes += size
                batch.append(record)
                if len(batch) == 512 or batch_bytes >= 1024**2:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch = []
                    batch_bytes = 0
                    need(source.stat().st_size <= MAX_TABLE, "Parquet byte bound")
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        need(source.stat().st_size <= MAX_TABLE, "Parquet byte bound")
        def chunks():
            with source.open("rb") as stream:
                while chunk := stream.read(1024**2):
                    yield chunk
        return write_chunks(child(root, name + ".parquet"), chunks(), maximum=MAX_TABLE)


def verified_table(path, name, expected):
    """All bytes verified before Arrow can inspect even the footer."""
    need(identity(path, maximum=MAX_TABLE) == expected, "Parquet identity")
    table = pq.ParquetFile(path, memory_map=False, pre_buffer=False,
                           thrift_string_size_limit=1024**2, thrift_container_size_limit=100000)
    need(table.schema_arrow.equals(SCHEMAS[name], check_metadata=True), "Parquet schema/version")
    need(table.metadata.num_rows <= MAX_TABLE_ROWS and table.metadata.num_row_groups <= 2048, "Parquet row/group bound")
    total = sum(table.metadata.row_group(i).total_byte_size for i in range(table.metadata.num_row_groups))
    need(total <= MAX_TABLE, "Parquet decoded byte bound")
    for i in range(table.metadata.num_row_groups):
        group = table.metadata.row_group(i)
        for j in range(group.num_columns):
            column = group.column(j)
            need(column.compression == "UNCOMPRESSED" and not set(column.encodings) & {"RLE_DICTIONARY", "PLAIN_DICTIONARY"},
                 "Parquet bounded V1 encoding")
    return table
