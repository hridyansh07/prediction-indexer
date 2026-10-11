"""A read-only canonical store with a disposable writable layer.

Bundle staging reads canonical windows and writes derivatives and its bundle
receipt back into the same store. The canonical copy lives on a drive that the
corpus runner never mutates (and whose filesystem may lack hard links), so writes
land in a per-bundle local store instead. Reads prefer the writable layer, so a
key written there shadows nothing: a canonical key can never be rewritten.
"""

from __future__ import annotations

from archive.storage.base import ObjectStoreError


class OverlayStore:
    def __init__(self, base, layer):
        self.base, self.layer = base, layer
        self.store_id = f"overlay:{layer.store_id}+{base.store_id}"
        self.durability = layer.durability

    def _owner(self, key):
        return self.layer if self.layer.head(key) is not None else self.base

    def put_immutable(self, key, reader, expected_identity, **kwargs):
        if self.base.head(key) is not None:
            raise ObjectStoreError(f"overlay refuses to write a base key: {key}")
        return self.layer.put_immutable(key, reader, expected_identity, **kwargs)

    def head(self, key):
        found = self.layer.head(key)
        return found if found is not None else self.base.head(key)

    def verify_metadata(self, expected):
        return self._owner(expected.key).verify_metadata(expected)

    def verify(self, expected):
        return self._owner(expected.key).verify(expected)

    def open(self, key, *, max_bytes=None):
        return self._owner(key).open(key, max_bytes=max_bytes)

    def open_verified(self, expected):
        return self._owner(expected.key).open_verified(expected)

    def list_keys(self, prefix):
        seen = set()
        for key in self.layer.list_keys(prefix):
            seen.add(key)
            yield key
        for key in self.base.list_keys(prefix):
            if key not in seen:
                yield key
