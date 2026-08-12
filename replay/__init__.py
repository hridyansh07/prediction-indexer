"""Storage-agnostic deterministic replay.

This package deliberately imports nothing from the capture, ingester, or legacy
analysis packages. The only boundary it accepts is a :class:`ByteStreamer`; NFS,
S3, memory fixtures, and local directories are adapters outside replay logic.
"""

from replay.stream import (
    ByteStreamer,
    CompositeByteStreamer,
    DirectoryByteStreamer,
    MemoryByteStreamer,
)

__all__ = [
    "ByteStreamer",
    "CompositeByteStreamer",
    "DirectoryByteStreamer",
    "MemoryByteStreamer",
]
