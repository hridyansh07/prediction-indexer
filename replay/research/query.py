"""Complete verified Parquet inputs and fixed, bounded backend queries; no server."""
import re

from archive.storage.base import ObjectExpectation
from encoder import StoredIdentity
from replay.research.io import child, closed, document, encoded, identity, need, write_chunks
from replay.research.inputs import FACTORIES
from replay.research.tables import MAX_TABLE, SCHEMAS, verified_table


def download_table(store, key, expected, destination):
    """Never expose a partial download. Archive's reader verifies EOF and identity."""
    closed(expected, "sha256 byte_length")
    need(type(expected["byte_length"]) is int and 0 <= expected["byte_length"] <= MAX_TABLE, "download byte bound")
    metadata = store.head(key)
    need(metadata is not None and metadata.stored == StoredIdentity(**expected), "download metadata identity")
    need(metadata.content_encoding is None and metadata.content_type in
         (None, "application/vnd.apache.parquet", "application/x-parquet"), "download Parquet encoding/type")
    request = ObjectExpectation(key, metadata.stored, metadata.provider_checksum, metadata.provider_checksum_algorithm,
                                metadata.content_type, None)
    def chunks():
        with store.open_verified(request) as stream:
            while chunk := stream.read(1024**2):
                yield chunk
    have = write_chunks(destination, chunks(), maximum=MAX_TABLE)
    need(have == expected, "complete download identity")
    return have


def query(directory, table, *, event_id=None, lens=None, offset=0, limit=100):
    need(table in SCHEMAS and type(offset) is int and 0 <= offset <= 1000000
         and type(limit) is int and 1 <= limit <= 500, "query bounds/table")
    need(event_id is None or (type(event_id) is str and re.fullmatch(r"event:d1:[0-9a-f]{64}", event_id)), "query event")
    need(lens is None or (type(lens) is str and lens in FACTORIES and table == "episodes"), "query lens")
    receipt = closed(document(child(directory, "receipt.json")), "research_build_version verification files")
    need(type(receipt["research_build_version"]) is int and receipt["research_build_version"] == 1, "query receipt version")
    filename = table + ".parquet"
    expected = receipt["files"][filename]
    path = child(directory, filename)
    parquet = verified_table(path, table, expected)
    result, matched = [], 0
    for batch in parquet.iter_batches(batch_size=128, use_threads=False):
        need(batch.nbytes <= 4 * 1024**2, "query batch byte bound")
        for row in batch.to_pylist():
            if (event_id is not None and row["event_id"] != event_id) or (lens is not None and row["lens"] != lens):
                continue
            if offset <= matched < offset + limit:
                result.append(row)
            matched += 1
    response = {"query_version": 1, "table": table, "offset": offset, "limit": limit, "matched": matched,
                "rows": result, "next_offset": offset + len(result) if offset + len(result) < matched else None}
    need(len(encoded(response)) <= 1024**2, "query response byte bound")
    need(identity(path, maximum=MAX_TABLE) == expected, "query input changed")
    return response
