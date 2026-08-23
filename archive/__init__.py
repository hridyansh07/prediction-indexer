"""Capture archival package.

Ownership is split between ``archiver`` (publication), ``reaper`` (local
deletion), ``storage`` (provider adapters), and ``common`` (shared evidence
validation and durable filesystem primitives).
"""

from archive.stream import ArchivedSegmentByteStreamer

__all__ = ["ArchivedSegmentByteStreamer"]
