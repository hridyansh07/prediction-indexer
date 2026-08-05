"""Write and remove files so a crash cannot leave a half-truth behind.

The same discipline a seal uses (`splices/common/segment.py`) and the finalizer
repeats (`canonical.rs`): the bytes are made durable, the name is swapped in one
step, and the directory entry is synced so the rename itself survives a crash.
It is here a third time because a receipt is a commit marker too, and a commit
marker that can appear without its content is worse than no marker at all.

Directory fsync errors propagate. On the Linux deployment target an
unsuccessful directory sync is an unsuccessful commit, never a warning behind a
marker that has already been published.
"""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any

__all__ = [
    "confirm_durable",
    "fsync_directory",
    "remove_durable",
    "write_json_durable",
]


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def confirm_durable(path: Path) -> None:
    """Re-establishes the durability of a commit marker before trusting it.

    A marker can exist and still not be durable: `write_json_durable` renames
    before it syncs the directory, so an attempt that failed at that last step
    reported failure while leaving the final name in place. Nothing acted on it
    — but a later run that *accepts* it is making a durability claim the failed
    attempt never established. One directory fsync makes the claim true instead
    of assuming it.
    """
    fsync_directory(Path(path).parent)


def write_json_durable(path: Path, document: Any) -> None:
    """Writes JSON through a unique `.open` file and an atomic rename."""
    path = Path(path)
    encoded = (
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    # Process-unique: two sweeps over one spool are already an operator error,
    # but a shared intermediate is what turns that into corrupt output rather
    # than a loud failure.
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.open")
    replaced = False
    try:
        with temporary.open("wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        replaced = True
        fsync_directory(path.parent)
    except Exception:
        temporary.unlink(missing_ok=True)
        if replaced:
            # The rename landed and the directory sync did not, so the marker
            # is visible but not durable while this call reports failure. Take
            # the name back, so what a later run finds matches what this one
            # said happened. Best-effort: if the filesystem is refusing syncs it
            # may refuse this too, which is why `confirm_durable` exists on the
            # reading side as well.
            try:
                path.unlink(missing_ok=True)
                fsync_directory(path.parent)
            except OSError:
                pass
        raise


def remove_durable(path: Path) -> bool:
    """Unlinks a file and makes its absence durable. Idempotent.

    Returns whether the file was there. The directory fsync is not optional:
    without it a crash can resurrect a name the reaper reported as deleted,
    and the next run would then have to decide whether the archive it verified
    still describes the file that came back.
    """
    path = Path(path)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    fsync_directory(path.parent)
    return True
