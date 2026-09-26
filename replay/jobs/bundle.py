"""Immutable Replay derivative generations restored from archived canonical evidence."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shutil
import signal
import stat
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple

from archive.canonical_restore import (
    CanonicalRestoreError,
    RemoteCanonicalWindow,
    preflight_canonical_window,
    restore_canonical_window,
)
from archive.storage.base import (
    JSON_CONTENT_TYPE,
    NDJSON_CONTENT_TYPE,
    ZSTD_CONTENT_ENCODING,
    IntegrityConflict,
    ObjectExpectation,
    ObjectMetadata,
    ObjectStoreError,
    VerificationFailure,
)
from encoder import StoredIdentity, stored_identity_of
from replay.jobs.contracts import (
    DERIVATIVE_FILES,
    MAX_BUNDLE_RECEIPT_BYTES,
    MAX_BUNDLE_WINDOWS,
    BundleReceipt,
    BundleWindow,
    ContractError,
    Producer,
    bundle_generation_sha256,
    bundle_receipt_bytes,
    bundle_receipt_key,
    canonical_window_keys,
    derivative_key,
    parse_bundle_receipt,
    parse_producer,
    window_bounds,
)
from replay.preparation import encoded
from replay.streams.protocol import ProtocolError, decode

MAX_SUBPROCESS_STDOUT = 4 * 1024 * 1024
MAX_SUBPROCESS_STDERR = 1024 * 1024
MAX_SUBPROCESS_SECONDS = 3600
MAX_CANONICAL_SCRATCH_BYTES = 1024 * 1024 * 1024 * 1024
MAX_DERIVATIVE_BYTES = 1024 * 1024 * 1024 * 1024
MAX_GENERATION_SCRATCH_BYTES = MAX_CANONICAL_SCRATCH_BYTES + MAX_DERIVATIVE_BYTES
MAX_GENERATION_OBJECTS = MAX_BUNDLE_WINDOWS


class BundlePin(NamedTuple):
    directory: Path
    address: str
    receipt_sha256: str


@dataclass(frozen=True)
class BundleReady:
    receipt: BundleReceipt
    receipt_bytes: bytes
    pins: tuple[BundlePin, ...]


@dataclass(frozen=True)
class NotReady:
    code: str
    detail: str


@dataclass(frozen=True)
class StaleCache:
    cached_producer: Producer
    current_producer: Producer


class BundleFailure(RuntimeError):
    def __init__(self, code: str, detail: str):
        super().__init__(detail)
        self.code = code
        self.detail = detail


_HEX64 = re.compile(r"[0-9a-f]{64}")


def _pin_values(address, receipt_sha256) -> tuple[str, str]:
    if (
        type(address) is not str
        or _HEX64.fullmatch(address) is None
        or type(receipt_sha256) is not str
        or _HEX64.fullmatch(receipt_sha256) is None
    ):
        raise BundleFailure("tool_failure", "materializer returned an invalid pin identity")
    return address, receipt_sha256


def _owned_root(path: Path, where: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or not stat.S_ISDIR(path.stat(follow_symlinks=False).st_mode):
        raise BundleFailure("integrity_failure", f"{where} must be an owned regular directory")


def _failure(code: str, error: BaseException) -> BundleFailure:
    return BundleFailure(code, str(error).replace("\n", " ")[:1024])


def _expectation(metadata: ObjectMetadata) -> ObjectExpectation:
    if not metadata.provider_checksum or not metadata.provider_checksum_algorithm:
        raise BundleFailure("integrity_failure", f"{metadata.key}: provider checksum metadata is absent")
    return ObjectExpectation(
        metadata.key,
        metadata.stored,
        metadata.provider_checksum,
        metadata.provider_checksum_algorithm,
        metadata.content_type,
        metadata.content_encoding,
    )


def _read_verified(store, key: str, maximum: int, *, content_type: str, content_encoding=None) -> bytes:
    metadata = store.head(key)
    if metadata is None:
        raise BundleFailure("integrity_failure", f"committed object {key} is absent")
    if metadata.byte_length > maximum:
        raise BundleFailure("integrity_failure", f"{key} exceeds its byte budget")
    if metadata.content_type != content_type or metadata.content_encoding != content_encoding:
        raise BundleFailure("integrity_failure", f"{key} has invalid provider metadata")
    chunks = []
    total = 0
    with store.open_verified(_expectation(metadata)) as source:
        while chunk := source.read(min(1024 * 1024, maximum + 1 - total)):
            chunks.append(chunk)
            total += len(chunk)
            if total > maximum:
                raise BundleFailure("integrity_failure", f"{key} exceeds its byte budget")
    return b"".join(chunks)


def _run_tool(executable: Path, arguments: tuple[str, ...], stdin: bytes, scratch: Path) -> bytes:
    executable = Path(executable)
    if not executable.is_absolute() or executable.is_symlink() or not executable.is_file():
        raise BundleFailure("tool_failure", "materializer must be an absolute regular executable")
    if len(stdin) > 1024 * 1024:
        raise BundleFailure("tool_failure", "materializer stdin exceeds 1 MiB")
    _owned_root(scratch, "tool scratch")
    try:
        process = subprocess.Popen(
            [str(executable), *arguments],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            cwd=scratch,
            env={"LANG": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin"},
            start_new_session=True,
        )
    except OSError as error:
        raise _failure("tool_failure", error) from error

    outputs = {"stdout": bytearray(), "stderr": bytearray()}
    overrun = threading.Event()
    pipe_errors: list[OSError] = []

    def terminate() -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def read_pipe(name: str, limit: int) -> None:
        pipe = getattr(process, name)
        try:
            try:
                while chunk := pipe.read(64 * 1024):
                    if len(outputs[name]) + len(chunk) > limit:
                        overrun.set()
                        terminate()
                        return
                    outputs[name].extend(chunk)
            except OSError as error:
                pipe_errors.append(error)
                terminate()
        finally:
            pipe.close()

    def write_stdin() -> None:
        try:
            process.stdin.write(stdin)
            process.stdin.flush()
        except OSError as error:
            if not isinstance(error, BrokenPipeError):
                pipe_errors.append(error)
                terminate()
        finally:
            process.stdin.close()

    threads = (
        threading.Thread(target=read_pipe, args=("stdout", MAX_SUBPROCESS_STDOUT)),
        threading.Thread(target=read_pipe, args=("stderr", MAX_SUBPROCESS_STDERR)),
        threading.Thread(target=write_stdin),
    )
    for thread in threads:
        thread.start()
    try:
        try:
            process.wait(timeout=MAX_SUBPROCESS_SECONDS)
        except subprocess.TimeoutExpired:
            terminate()
            process.wait()
            raise BundleFailure("tool_failure", "materializer timed out") from None
    except BaseException:
        terminate()
        process.wait()
        raise
    finally:
        for thread in threads:
            thread.join()

    if overrun.is_set():
        raise BundleFailure("tool_failure", "materializer output exceeds its byte budget")
    if pipe_errors:
        raise _failure("tool_failure", pipe_errors[0])
    output = bytes(outputs["stdout"])
    diagnostic = bytes(outputs["stderr"]).decode("utf-8", "replace").replace("\n", " ")[:1024]
    if process.returncode != 0:
        raise BundleFailure("tool_failure", f"materializer exited {process.returncode}: {diagnostic}")
    if not output.endswith(b"\n") or output.count(b"\n") != 1:
        raise BundleFailure("tool_failure", "materializer stdout is not one JSON line")
    return output[:-1]


def _describe(materializer: Path, scratch: Path) -> Producer:
    try:
        return parse_producer(_run_tool(materializer, ("--describe",), b"", scratch))
    except BundleFailure:
        raise
    except ContractError as error:
        raise _failure("tool_failure", error) from error


def _strict_response(raw: bytes) -> dict:
    try:
        value = decode(raw, MAX_SUBPROCESS_STDOUT)
    except ProtocolError as error:
        raise _failure("tool_failure", error) from error
    if type(value) is not dict or set(value) != {"version", "normalizer", "derivatives"} or value["version"] != 1:
        raise BundleFailure("tool_failure", "materializer response has an invalid closed schema")
    if encoded(value) != raw or type(value["derivatives"]) is not list:
        raise BundleFailure("tool_failure", "materializer response is not canonical JSON")
    return value


def _inspect(materializer: Path, pin: BundlePin, scratch: Path) -> None:
    request = encoded(
        {
            "derivative_address": pin.address,
            "directory": str(pin.directory),
            "receipt_sha256": pin.receipt_sha256,
            "version": 1,
        }
    )
    response = _run_tool(materializer, ("--inspect-pin",), request, scratch)
    try:
        value = decode(response, 1024 * 1024)
    except ProtocolError as error:
        raise _failure("tool_failure", error) from error
    expected = {"derivative_address": pin.address, "receipt_sha256": pin.receipt_sha256, "version": 1}
    if value != expected or encoded(value) != response:
        raise BundleFailure("tool_failure", "pin inspector returned an invalid attestation")


def _source_matches(receipt: BundleReceipt, bundle_id: str, interval, window_seconds: int, remotes) -> bool:
    return (
        receipt.bundle_id == bundle_id
        and (receipt.start_ns, receipt.end_ns) == interval
        and receipt.canonical_window_seconds == window_seconds
        and tuple(window.canonical_receipt_sha256 for window in receipt.windows)
        == tuple(remote.receipt_sha256 for remote in remotes)
    )


def _cached_receipts(store, bundle_id: str) -> tuple[tuple[BundleReceipt, bytes], ...]:
    prefix = f"replay/bundles/{bundle_id}/generations/"
    keys = tuple(store.list_keys(prefix))
    if len(keys) > MAX_GENERATION_OBJECTS:
        raise BundleFailure("integrity_failure", "bundle generation object count exceeds limit")
    receipts = []
    for key in keys:
        parts = key.split("/")
        if len(parts) != 6 or parts[:3] != ["replay", "bundles", bundle_id] or parts[3] != "generations" or parts[5] != "bundle_receipt.json":
            raise BundleFailure("integrity_failure", f"unexpected object below bundle prefix: {key}")
        raw = _read_verified(store, key, MAX_BUNDLE_RECEIPT_BYTES, content_type=JSON_CONTENT_TYPE)
        try:
            receipt = parse_bundle_receipt(raw)
        except ContractError as error:
            raise _failure("integrity_failure", error) from error
        generation = bundle_generation_sha256(receipt)
        if key != bundle_receipt_key(receipt.bundle_id, generation):
            raise BundleFailure("integrity_failure", "bundle receipt is stored under the wrong generation key")
        receipts.append((receipt, raw))
    return tuple(receipts)


def _cache_decision(store, bundle_id, interval, window_seconds, remotes, producer):
    stale = None
    for receipt, raw in _cached_receipts(store, bundle_id):
        if not _source_matches(receipt, bundle_id, interval, window_seconds, remotes):
            continue
        if receipt.producer == producer:
            return receipt, raw
        stale = receipt.producer
    return StaleCache(stale, producer) if stale is not None else None


def _file_identity(path: Path) -> StoredIdentity:
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode):
        raise BundleFailure("integrity_failure", f"derivative entry {path} is not a regular file")
    with path.open("rb") as source:
        return stored_identity_of(source)


def _attributes(name: str):
    if name.endswith(".ndjson.zst"):
        return NDJSON_CONTENT_TYPE, ZSTD_CONTENT_ENCODING
    return JSON_CONTENT_TYPE, None


def _copy_file(source: Path, destination: Path, budget: list[int]) -> None:
    identity = _file_identity(source)
    budget[0] += identity.byte_length
    if budget[0] > MAX_DERIVATIVE_BYTES:
        raise BundleFailure("integrity_failure", "aggregate derivative bytes exceed budget")
    with source.open("rb") as reader, open(destination, "xb") as writer:
        while chunk := reader.read(1024 * 1024):
            writer.write(chunk)
        writer.flush()
        os.fsync(writer.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _directory_names(path: Path) -> set[str]:
    try:
        entries = tuple(os.scandir(path))
    except OSError as error:
        raise _failure("tool_failure", error) from error
    for entry in entries:
        if not entry.is_dir(follow_symlinks=False):
            raise BundleFailure("tool_failure", f"unexpected non-directory entry beneath {path}")
    return {entry.name for entry in entries}


def _validate_materialized_window(path: Path, address: str) -> None:
    try:
        entries = {entry.name: entry for entry in os.scandir(path)}
    except OSError as error:
        raise _failure("tool_failure", error) from error
    lock = f".{address}.lock"
    if set(entries) != {address, lock}:
        raise BundleFailure("tool_failure", "materializer output has unexpected derivative entries")
    if not entries[address].is_dir(follow_symlinks=False) or not entries[lock].is_file(follow_symlinks=False):
        raise BundleFailure("tool_failure", "materializer output contains a non-regular derivative entry")


def _tree_bytes(root: Path, maximum: int) -> int:
    total = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        try:
            entries = tuple(os.scandir(directory))
        except OSError as error:
            raise _failure("tool_failure", error) from error
        for entry in entries:
            if entry.is_dir(follow_symlinks=False):
                pending.append(Path(entry.path))
            elif entry.is_file(follow_symlinks=False):
                total += entry.stat(follow_symlinks=False).st_size
                if total > maximum:
                    raise BundleFailure("tool_failure", "generation scratch exceeds aggregate byte budget")
            else:
                raise BundleFailure("tool_failure", "generation scratch contains a non-regular entry")
    return total


def _install_local(source: Path, address: str, receipt_sha256: str, root: Path, materializer: Path, scratch: Path) -> BundlePin:
    _pin_values(address, receipt_sha256)
    _owned_root(root, "derivatives_root")
    target = root / address
    if target.exists():
        if target.is_symlink() or not target.is_dir():
            raise BundleFailure("integrity_failure", "derivative target is not a regular directory")
        pin = BundlePin(target, address, receipt_sha256)
        if not (target / "receipt.json").exists():
            shutil.rmtree(target)
        else:
            _inspect(materializer, pin, scratch)
            return pin
    stage_root = Path(tempfile.mkdtemp(prefix=".bundle-stage-", dir=root))
    stage = stage_root / address
    stage.mkdir()
    budget = [0]
    try:
        for name in (*DERIVATIVE_FILES[:-1], DERIVATIVE_FILES[-1]):
            _copy_file(source / name, stage / name, budget)
        _fsync_directory(stage)
        _inspect(materializer, BundlePin(stage, address, receipt_sha256), scratch)
        try:
            os.rename(stage, target)
            _fsync_directory(root)
        except OSError:
            if target.is_dir() and not target.is_symlink():
                pin = BundlePin(target, address, receipt_sha256)
                _inspect(materializer, pin, scratch)
                return pin
            raise
        return BundlePin(target, address, receipt_sha256)
    finally:
        shutil.rmtree(stage_root, ignore_errors=True)


def _add_derivative_bytes(paths, budget: list[int]) -> None:
    for path in paths:
        budget[0] += _file_identity(path).byte_length
        if budget[0] > MAX_DERIVATIVE_BYTES:
            raise BundleFailure("integrity_failure", "aggregate derivative bytes exceed budget")


def _download_derivative(store, address, receipt_sha256, root, materializer, scratch, budget) -> BundlePin:
    target = root / address
    if target.exists() and (target / "receipt.json").is_file():
        pin = BundlePin(target, address, receipt_sha256)
        _inspect(materializer, pin, scratch)
        _add_derivative_bytes((target / name for name in DERIVATIVE_FILES), budget)
        return pin
    stage_source = Path(tempfile.mkdtemp(prefix=".bundle-download-", dir=scratch)) / address
    stage_source.mkdir()
    try:
        for name in DERIVATIVE_FILES:
            key = derivative_key(address, name)
            metadata = store.head(key)
            if metadata is None:
                raise BundleFailure("integrity_failure", f"committed derivative object {key} is absent")
            content_type, content_encoding = _attributes(name)
            if (metadata.content_type, metadata.content_encoding) != (content_type, content_encoding):
                raise BundleFailure("integrity_failure", f"{key} has invalid provider metadata")
            budget[0] += metadata.byte_length
            if budget[0] > MAX_DERIVATIVE_BYTES:
                raise BundleFailure("integrity_failure", "aggregate derivative bytes exceed budget")
            with store.open_verified(_expectation(metadata)) as reader, open(stage_source / name, "xb") as writer:
                while chunk := reader.read(1024 * 1024):
                    writer.write(chunk)
                writer.flush()
                os.fsync(writer.fileno())
        return _install_local(stage_source, address, receipt_sha256, root, materializer, scratch)
    finally:
        shutil.rmtree(stage_source.parent, ignore_errors=True)


def _ready(store, receipt, raw, derivatives_root, materializer, scratch) -> BundleReady:
    budget = [0]
    pins = tuple(
        _download_derivative(
            store, window.derivative_address, window.receipt_sha256,
            derivatives_root, materializer, scratch, budget,
        )
        for window in receipt.windows
    )
    return BundleReady(receipt, raw, pins)


def _upload_derivative(store, pin: BundlePin, budget: list[int]) -> None:
    for name in DERIVATIVE_FILES:
        path = pin.directory / name
        identity = _file_identity(path)
        budget[0] += identity.byte_length
        if budget[0] > MAX_DERIVATIVE_BYTES:
            raise BundleFailure("integrity_failure", "aggregate derivative bytes exceed budget")
        content_type, content_encoding = _attributes(name)
        with path.open("rb") as source:
            store.put_immutable(
                derivative_key(pin.address, name), source, identity,
                content_type=content_type, content_encoding=content_encoding,
            )


def _build(bundle_id, interval, store, derivatives_root, materializer, window_seconds, remotes, producer, scratch):
    canonical_root = scratch / "canonical"
    output_root = scratch / "materialized"
    canonical_stored_bytes = sum(
        remote.evidence.expected.stored.byte_length + remote.provenance.expected.stored.byte_length
        for remote in remotes
    )
    canonical_decoded_bytes = sum(
        remote.evidence.logical.byte_length + remote.provenance.logical.byte_length
        for remote in remotes
    )
    if max(canonical_stored_bytes, canonical_decoded_bytes) > MAX_CANONICAL_SCRATCH_BYTES:
        raise BundleFailure("integrity_failure", "aggregate canonical scratch exceeds budget")
    for remote in remotes:
        restore_canonical_window(store, remote, canonical_root)
    request = encoded(
        {
            "canonical_root": str(canonical_root),
            "end_ns": interval[1],
            "output_root": str(output_root),
            "start_ns": interval[0],
            "version": 1,
        }
    )
    tool_output = _run_tool(materializer, (), request, scratch)
    _tree_bytes(scratch, MAX_GENERATION_SCRATCH_BYTES)
    response = _strict_response(tool_output)
    if response["normalizer"] != producer.document()["normalizer"]:
        raise BundleFailure("tool_failure", "materializer response normalizer disagrees with --describe")
    if len(response["derivatives"]) != len(remotes):
        raise BundleFailure("tool_failure", "materializer returned the wrong derivative count")
    if _directory_names(output_root) != {f"window={remote.window_start_ns}" for remote in remotes}:
        raise BundleFailure("tool_failure", "materializer output has unexpected window entries")
    windows = []
    pins = []
    for remote, value in zip(remotes, response["derivatives"], strict=True):
        if type(value) is not dict or set(value) != {
            "window_start_ns", "window_end_ns", "derivative_address", "receipt_sha256"
        }:
            raise BundleFailure("tool_failure", "materializer pin has an invalid closed schema")
        if value["window_start_ns"] != remote.window_start_ns or value["window_end_ns"] != remote.window_end_ns:
            raise BundleFailure("tool_failure", "materializer pins are not ordered by requested window")
        address, receipt_sha256 = _pin_values(value["derivative_address"], value["receipt_sha256"])
        window_root = output_root / f"window={remote.window_start_ns}"
        _validate_materialized_window(window_root, address)
        source = window_root / address
        built_pin = BundlePin(source, address, receipt_sha256)
        _inspect(materializer, built_pin, scratch)
        pin = _install_local(source, address, receipt_sha256, derivatives_root, materializer, scratch)
        pins.append(pin)
        windows.append(BundleWindow(remote.window_start_ns, remote.window_end_ns, remote.receipt_sha256, address, receipt_sha256))
    receipt = BundleReceipt(bundle_id, interval[0], interval[1], window_seconds, producer, tuple(windows))
    raw = bundle_receipt_bytes(receipt)
    derivative_budget = [0]
    for pin in pins:
        _upload_derivative(store, pin, derivative_budget)
    generation = bundle_generation_sha256(receipt)
    identity = StoredIdentity(hashlib.sha256(raw).hexdigest(), len(raw))
    with tempfile.SpooledTemporaryFile(max_size=MAX_BUNDLE_RECEIPT_BYTES) as source:
        source.write(raw)
        source.seek(0)
        store.put_immutable(bundle_receipt_key(bundle_id, generation), source, identity, content_type=JSON_CONTENT_TYPE)
    return BundleReady(receipt, raw, tuple(pins))


def ensure_bundle(
    bundle_id,
    window_interval,
    *,
    store,
    work_root,
    derivatives_root,
    materializer,
    window_seconds,
):
    """Return verified local pins for one immutable source-and-producer generation."""
    work_root = Path(work_root)
    derivatives_root = Path(derivatives_root)
    materializer = Path(materializer)
    try:
        _owned_root(work_root, "work_root")
        tool_scratch = work_root / "tool-scratch"
        first, last, starts = window_bounds(window_interval[0], window_interval[1], window_seconds)
        if (first, last) != tuple(window_interval) or len(starts) > MAX_BUNDLE_WINDOWS:
            raise BundleFailure("integrity_failure", "window interval is not aligned or exceeds the window limit")
        # Validate the bundle key component before it reaches list prefixes or paths.
        bundle_receipt_key(bundle_id, "0" * 64)
        producer = _describe(materializer, tool_scratch)
        for start in starts:
            if store.head(canonical_window_keys(start)[2]) is None:
                return NotReady("canonical_not_archived", f"canonical receipt for window {start} is absent")
        remotes: list[RemoteCanonicalWindow] = []
        for start in starts:
            remote = preflight_canonical_window(store, start, window_seconds)
            if remote is None:
                return NotReady("canonical_not_archived", f"canonical receipt for window {start} is absent")
            remotes.append(remote)
        _owned_root(derivatives_root, "derivatives_root")
        decision = _cache_decision(store, bundle_id, (first, last), window_seconds, remotes, producer)
        if isinstance(decision, StaleCache):
            return decision
        if decision is not None:
            return _ready(store, *decision, derivatives_root, materializer, tool_scratch)

        coordinate = hashlib.sha256(
            encoded(
                {
                    "bundle_id": bundle_id,
                    "canonical_receipts": [remote.receipt_sha256 for remote in remotes],
                    "canonical_window_seconds": window_seconds,
                    "interval": {"end_ns": str(last), "start_ns": str(first)},
                    "producer": producer.document(),
                }
            )
        ).hexdigest()
        lock_path = work_root / f"generation-{coordinate}.lock"
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            decision = _cache_decision(store, bundle_id, (first, last), window_seconds, remotes, producer)
            if isinstance(decision, StaleCache):
                return decision
            if decision is not None:
                return _ready(store, *decision, derivatives_root, materializer, tool_scratch)
            scratch = Path(tempfile.mkdtemp(prefix=f"generation-{coordinate}-", dir=work_root))
            try:
                return _build(
                    bundle_id, (first, last), store, derivatives_root,
                    materializer, window_seconds, remotes, producer, scratch,
                )
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
    except BundleFailure:
        raise
    except (IntegrityConflict, VerificationFailure, CanonicalRestoreError, ContractError) as error:
        raise _failure("integrity_failure", error) from error
    except ObjectStoreError as error:
        raise _failure("archive_unavailable", error) from error
    except OSError as error:
        raise _failure("integrity_failure", error) from error
