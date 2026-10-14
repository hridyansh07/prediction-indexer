"""Disposable, hash-verified window cache. A lease lives through the entire bundle."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path

from archive.common.durable import fsync_directory, remove_durable, write_json_durable
from replay.jobs import bundle
from replay.preparation import encoded
from replay.streams.protocol import ProtocolError, decode
from replay.supervisor import normalizer_descriptor


def _key(remote, producer):
    return {"window_start_ns": remote.window_start_ns, "window_end_ns": remote.window_end_ns,
            "canonical_receipt_sha256": remote.receipt_sha256, "producer": producer.document()}


def _entry(key, pin):
    return {"index_version": 1, "key": key, "derivative_address": pin.address,
            "receipt_sha256": pin.receipt_sha256}


def _json_bytes(value):
    # Same serialization as the shared durable JSON writer; admission counts the index too.
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def _bind(pin, remote, producer):
    receipt = json.loads((pin.directory / "receipt.json").read_bytes())
    descriptor = normalizer_descriptor(producer.document()["normalizer"])
    expected = {"derivative_address": pin.address, "source_receipt_sha256": remote.receipt_sha256,
                "normalized_schema_version": producer.normalized_schema_version,
                "materializer_version": producer.materializer_version,
                "policy_sha256": producer.materialization_policy_sha256,
                "normalizer_bundle_sha256": descriptor["bundle_sha256"],
                "normalizer_config_sha256": descriptor["config_sha256"]}
    if any(receipt.get(name) != value for name, value in expected.items()):
        raise bundle.BundleFailure("integrity_failure", "cached derivative source or producer mismatch")


class DerivativeCache:
    def __init__(self, root, max_bytes):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("derivative_cache.max_bytes must be a positive integer")
        self.root, self.max_bytes = Path(root), max_bytes
        self.derivatives = self.root / "derivatives"
        self.index, self.touches, self.leases = (self.root / name for name in ("index", "touches", "leases"))
        for path in (self.root, self.derivatives, self.index, self.touches, self.leases):
            bundle._owned_root(path, "derivative cache")

    @contextmanager
    def locked(self):
        with open(self.root / "lock", "a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            yield

    @contextmanager
    def lease(self):
        # Lock the lease inode, not a PID in another container's PID namespace.
        # A unique suffix also permits multiple bundles in one Python process.
        handle = None
        try:
            with self.locked():
                fd, name = tempfile.mkstemp(prefix=f"{os.getpid()}-", suffix=".json", dir=self.leases)
                handle = os.fdopen(fd, "w+b")
                fcntl.flock(handle, fcntl.LOCK_EX)
                session = WindowLease(self, Path(name), handle)
                session._write_lease()
                # A crashed installer cannot still hold root/lock.
                for stage in self.derivatives.glob(".bundle-stage-*"):
                    if stage.is_dir() and not stage.is_symlink():
                        shutil.rmtree(stage)
                for temporary in self.index.glob(".*.open"):
                    remove_durable(temporary)
                self._evict(0)
            yield session
        finally:
            if handle is not None:
                try:
                    with self.locked():
                        remove_durable(Path(name))
                finally:
                    handle.close()

    def _live_addresses(self):
        addresses = set()
        for path in self.leases.glob("*.json"):
            if path.is_symlink() or not path.is_file():
                raise bundle.BundleFailure("integrity_failure", "invalid cache lease file")
            with path.open("r+b") as handle:
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    document = decode(handle.read(bundle.MAX_BUNDLE_RECEIPT_BYTES + 1),
                                      bundle.MAX_BUNDLE_RECEIPT_BYTES)
                    addresses.update(document["addresses"])
                else:
                    # Released/dead owner, including PID reuse or a container restart.
                    remove_durable(path)
        return addresses

    def _read_entry(self, path):
        try:
            if path.is_symlink():
                raise ValueError("index symlink")
            with path.open("rb") as handle:
                value = decode(handle.read(bundle.MAX_BUNDLE_RECEIPT_BYTES + 1),
                               bundle.MAX_BUNDLE_RECEIPT_BYTES)
            if type(value) is not dict or set(value) != {"index_version", "key", "derivative_address", "receipt_sha256"}:
                raise ValueError("index schema")
            if type(value["index_version"]) is not int or value["index_version"] != 1:
                raise ValueError("index version")
            key = value["key"]
            if type(key) is not dict or set(key) != {"window_start_ns", "window_end_ns", "canonical_receipt_sha256", "producer"}:
                raise ValueError("index key schema")
            bundle.BundleWindow(key["window_start_ns"], key["window_end_ns"], key["canonical_receipt_sha256"],
                                value["derivative_address"], value["receipt_sha256"])
            bundle.parse_producer(encoded(key["producer"]))
            if path.name != hashlib.sha256(encoded(key)).hexdigest() + ".json":
                raise ValueError("index key filename")
            return value
        except FileNotFoundError:
            return None
        except (ValueError, ProtocolError, bundle.BundleFailure, bundle.ContractError):
            remove_durable(path)
            remove_durable(self.touches / path.stem)
            return None

    def total_bytes(self):
        # Includes index and lease JSON, not just compressed derivative payloads.
        return bundle._tree_bytes(self.root, 2**63 - 1)

    def _evict(self, required, protected=frozenset()):
        live = self._live_addresses() | set(protected)
        if required > self.max_bytes:
            return False
        entries = {}
        for path in self.index.glob("*.json"):
            value = self._read_entry(path)
            if value is not None:
                entries.setdefault(value["derivative_address"], []).append(path)
        candidates = []
        for directory in self.derivatives.iterdir():
            if directory.name in live:
                continue
            if directory.is_symlink() or not directory.is_dir() or bundle._HEX64.fullmatch(directory.name) is None:
                raise bundle.BundleFailure("integrity_failure", "unexpected cache derivative entry")
            touches = [self.touches / path.stem for path in entries.get(directory.name, ())]
            touched = max((path.stat().st_mtime_ns for path in touches if path.exists()),
                          default=directory.stat().st_mtime_ns)
            candidates.append((touched, directory.name, directory))
        total = self.total_bytes()
        for _, address, directory in sorted(candidates):
            if total + required <= self.max_bytes:
                break
            for path in entries.get(address, ()):
                remove_durable(path)
                remove_durable(self.touches / path.stem)
            shutil.rmtree(directory)
            fsync_directory(self.derivatives)
            total = self.total_bytes()
        return total + required <= self.max_bytes


class WindowLease:
    def __init__(self, cache, path, handle):
        self.cache, self.path, self.handle = cache, path, handle
        self.addresses = set()
        self.diagnostics = {"hits": 0, "misses": 0, "cache_bypass": []}

    def _write_lease(self):
        self.handle.seek(0)
        self.handle.write(_json_bytes({"pid": os.getpid(), "addresses": sorted(self.addresses)}))
        self.handle.truncate()
        self.handle.flush()
        os.fsync(self.handle.fileno())
        fsync_directory(self.cache.leases)

    def _paths(self, key):
        digest = hashlib.sha256(encoded(key)).hexdigest()
        return self.cache.index / f"{digest}.json", self.cache.touches / digest

    def _use(self, address, touch):
        self.addresses.add(address)
        self._write_lease()
        touch.touch()

    def lookup(self, remote, producer):
        key = _key(remote, producer)
        path, touch = self._paths(key)
        with self.cache.locked():
            entry = self.cache._read_entry(path)
            if entry is not None:
                try:
                    if entry["key"] != key:
                        raise ValueError("index key mismatch")
                    pin, _ = bundle._local_pin(self.cache.derivatives / entry["derivative_address"],
                                               entry["derivative_address"], entry["receipt_sha256"])
                    _bind(pin, remote, producer)
                except (ValueError, bundle.BundleFailure):
                    remove_durable(path)
                    remove_durable(touch)
                else:
                    self._use(pin.address, touch)
                    self.diagnostics["hits"] += 1
                    return pin
        self.diagnostics["misses"] += 1
        return None

    def install(self, source, remote, producer, private_root, materializer, scratch):
        key = _key(remote, producer)
        path, touch = self._paths(key)
        document = _entry(key, source)
        size = bundle._check_pinned_files(source.directory, source.receipt_sha256, "cache candidate")
        _bind(source, remote, producer)
        reason = None
        with self.cache.locked():
            target = self.cache.derivatives / source.address
            existing = None
            if target.exists():
                try:
                    existing, _ = bundle._local_pin(target, source.address, source.receipt_sha256)
                    _bind(existing, remote, producer)
                except (ValueError, bundle.BundleFailure):
                    existing = None
                    if source.address in self.cache._live_addresses():
                        reason = "corrupt_derivative_in_live_lease"
                    else:
                        if target.is_symlink() or not target.is_dir():
                            raise bundle.BundleFailure("integrity_failure", "invalid cache derivative directory")
                        shutil.rmtree(target)
                        fsync_directory(self.cache.derivatives)
            entry = self.cache._read_entry(path)
            if entry is not None and entry != document:
                raise bundle.BundleFailure("integrity_failure", "immutable window index conflict")
            next_addresses = self.addresses | {source.address}
            lease_growth = len(_json_bytes({"pid": os.getpid(), "addresses": sorted(next_addresses)})) - self.path.stat().st_size
            required = (0 if existing else size) + (0 if entry else len(_json_bytes(document))) + lease_growth
            if reason is None and self.cache._evict(required, {source.address}):
                pin = existing or bundle._install_local(source.directory, source.address, source.receipt_sha256,
                                                       self.cache.derivatives, materializer, scratch,
                                                       freshly_verified=True)
                if entry is None:
                    write_json_durable(path, document)
                self._use(pin.address, touch)
                return pin
            if reason is None:
                reason = "max_bytes" if size + len(_json_bytes(document)) > self.cache.max_bytes else "live_leases"
        self.diagnostics["cache_bypass"].append({"window_start_ns": remote.window_start_ns, "reason": reason})
        return bundle._install_local(source.directory, source.address, source.receipt_sha256,
                                     private_root, materializer, scratch, freshly_verified=True)
