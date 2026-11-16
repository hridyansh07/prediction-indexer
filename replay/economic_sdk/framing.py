"""Durable shared-codec frame publication for newly owned output spools."""

import os
from pathlib import Path

from encoder import encode_stream
from replay.streams.protocol import require
from replay.supervisor import fsync_directory


def commit_frame(plain, logical):
    """Frame a sealed spool in a fresh output directory, then remove only that spool.

    The caller created the spool exclusively and publishes no manifest/receipt
    until this returns. Failures leave evidence of the incomplete transaction.
    """
    plain = Path(plain)
    stored = plain.with_name(plain.name + ".zst")
    temporary = stored.with_name(stored.name + ".open")
    require(not stored.exists() and not stored.is_symlink(), "existing output")
    with plain.open("rb") as source, temporary.open("xb") as sink:
        result = encode_stream(source, sink)
        sink.flush()
        os.fsync(sink.fileno())
    require({"sha256": result.logical.sha256, "byte_length": result.logical.byte_length,
             "records": result.logical.line_count} == logical, stored.name + " logical identity")
    temporary.rename(stored)
    fsync_directory(stored.parent)
    plain.unlink()
    fsync_directory(stored.parent)
    return {"logical": logical,
            "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}


def finish_frame(writer):
    return commit_frame(writer.path, writer.finish())
