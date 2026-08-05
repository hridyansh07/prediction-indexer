"""Process lease preventing overlapping one-shot Targeter v2 discoveries."""

from __future__ import annotations

import errno
import fcntl
import os
from dataclasses import dataclass
from pathlib import Path


class TargeterLeaseError(ValueError):
    """Another targeter run already owns the shared output root."""


@dataclass
class TargeterRunLease:
    path: Path
    descriptor: int

    @classmethod
    def acquire(cls, output_root: Path) -> "TargeterRunLease":
        root = Path(output_root)
        root.mkdir(parents=True, exist_ok=True)
        path = root / ".targeter-v2.lock"
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            os.close(descriptor)
            if error.errno in {errno.EACCES, errno.EAGAIN}:
                raise TargeterLeaseError(
                    f"another Targeter v2 run holds {path}; refusing an overlapping discovery"
                ) from error
            raise
        try:
            payload = f"pid={os.getpid()}\n".encode("ascii")
            os.ftruncate(descriptor, 0)
            os.write(descriptor, payload)
            os.fsync(descriptor)
        except BaseException:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
            raise
        return cls(path=path, descriptor=descriptor)

    def close(self) -> None:
        if self.descriptor < 0:
            return
        descriptor = self.descriptor
        self.descriptor = -1
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def __enter__(self) -> "TargeterRunLease":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
