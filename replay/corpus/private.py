"""Attempt-owned corpus work, never a cache or a previous attempt's directory."""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

# ':' cannot occur in a valid bundle ID. Legacy work_root/<bundle_id> cleanup
# must validate the ID before deleting, so it cannot sweep this namespace.
NAMESPACE = ".corpus-private-attempts:"


def _no_symlinks(path):
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError(f"private attempt path must not contain symlinks: {parent}")


def _overlaps(left, right):
    if left.is_relative_to(right) or right.is_relative_to(left):
        return True
    # Also reject existing bind-mount aliases, which Path.resolve cannot see.
    for path, other in ((left, right), (right, left)):
        if other.exists():
            for ancestor in (path, *path.parents):
                if ancestor.exists() and ancestor.samefile(other):
                    return True
    return False


def validate_roots(spec):
    """Validate only configured paths/ancestors, never search their contents."""
    roots = {key: Path(spec[key]) for key in ("work_root", "canonical_store", "results_root")}
    if "derivative_cache" in spec:
        roots["derivative_cache.root"] = Path(spec["derivative_cache"]["root"])
    if NAMESPACE in roots["work_root"].parts:
        raise ValueError("work_root must not be inside a private attempt namespace")
    for name, path in roots.items():
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError(f"private attempts require an absolute non-traversing {name}")
        _no_symlinks(path)
    roots = {name: path.resolve() for name, path in roots.items()}
    for index, (name, path) in enumerate(roots.items()):
        for other, target in list(roots.items())[index + 1:]:
            if _overlaps(path, target):
                raise ValueError(f"private attempt roots overlap: {name} and {other}")
    # These configured evidence/config files must not live in disposable work.
    protected = [spec[key] for key in ("ledger", "runner_config", "materializer")]
    protected += [spec["fees"][key] for key in ("template_catalog", "sources")]
    for value in protected:
        if _overlaps(roots["work_root"], Path(value).resolve()):
            raise ValueError(f"private attempt work_root overlaps a configured input: {value}")
    return roots


class PrivateAttempt:
    """An exclusive 0700 directory; the only deletion target is the one we create.

    Keep namespace/attempt descriptors open (preventing inode reuse) and check
    their identities before fd-relative, symlink-resistant removal. Never adopt,
    enumerate or sweep another attempt, even after a crash.
    """

    def __init__(self, spec):
        if not shutil.rmtree.avoids_symlink_attacks or os.open not in os.supports_dir_fd:
            raise ValueError("private attempts require fd-relative symlink-resistant cleanup")
        self._spec, self._roots = spec, validate_roots(spec)
        parent = self._roots["work_root"] / NAMESPACE
        _no_symlinks(parent)
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        _no_symlinks(parent)
        self._parent = parent
        self._fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self._attempt_fd = None
        try:
            info = os.fstat(self._fd)
            if info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError(f"private attempt namespace must be owned and mode 0700: {parent}")
            self._path = Path(tempfile.mkdtemp(prefix="attempt-", dir=parent))
            self._attempt_fd = os.open(self._path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._fd)
        except BaseException:
            self.close()
            raise

    @property
    def path(self):
        return self._path

    def discard(self):
        if validate_roots(self._spec) != self._roots:
            raise ValueError("private attempt configured roots changed; retaining work")
        _no_symlinks(self._parent)
        if not os.path.samestat(self._parent.stat(), os.fstat(self._fd)):
            raise ValueError("private attempt namespace identity changed; retaining work")
        info = os.stat(self._path.name, dir_fd=self._fd, follow_symlinks=False)
        if not os.path.samestat(info, os.fstat(self._attempt_fd)):
            raise ValueError("private attempt identity changed; retaining work")
        # Anchor every content operation to the fd opened at exclusive creation,
        # not to the attempt name checked above: that name can be replaced before
        # rmtree does its own initial lstat/open.
        with os.scandir(self._attempt_fd) as entries:
            for entry in entries:
                if entry.is_dir(follow_symlinks=False):
                    shutil.rmtree(entry.name, dir_fd=self._attempt_fd)
                else:
                    os.unlink(entry.name, dir_fd=self._attempt_fd)
        info = os.stat(self._path.name, dir_fd=self._fd, follow_symlinks=False)
        if not os.path.samestat(info, os.fstat(self._attempt_fd)):
            raise ValueError("private attempt identity changed during cleanup; replacement retained")
        # Deliberately nonrecursive: even a replacement after this last check
        # cannot lose any contents. A nonempty replacement makes rmdir fail.
        os.rmdir(self._path.name, dir_fd=self._fd)

    def close(self):
        try:
            if self._attempt_fd is not None:
                os.close(self._attempt_fd)
        finally:
            os.close(self._fd)
