"""Durable Replay job stages and receipt-last job archival.

Stage functions are deliberately dependency-injected.  The production runner
supplies the object store, bundle builder, Universe client, and executables;
tests use bounded fakes without retained data or live services.
"""

from __future__ import annotations

import hashlib
import importlib
import io
import os
import signal
import stat
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

from archive.storage.base import (
    JSON_CONTENT_TYPE,
    IntegrityConflict,
    ObjectExpectation,
    ObjectStoreError,
    VerificationFailure,
)
from encoder import StoredIdentity
from replay.jobs.bundle import BundleFailure, BundleReady, NotReady, StaleCache
from replay.jobs.contracts import (
    MAX_BUNDLE_RECEIPT_BYTES,
    MAX_JOB_OBJECTS,
    MAX_JOB_RECEIPT_BYTES,
    MAX_JOB_RESULT_BYTES,
    MAX_REQUEST_BYTES,
    MAX_RESOLVED_JOB_BYTES,
    BundleReceipt,
    ContractError,
    HistoryEntry,
    JobObject,
    JobReceipt,
    JobResult,
    Occurrence,
    Request,
    ResolvedJob,
    RunnerConfig,
    bundle_receipt_bytes,
    job_object_key,
    job_receipt_bytes,
    job_receipt_key,
    job_result_bytes,
    parse_bundle_receipt,
    parse_job_receipt,
    parse_job_result,
    parse_resolved_job,
    reason_detail,
    resolve_occurrences,
    resolved_job_bytes,
)
from replay.preparation import (
    MAX_BYTES as MAX_PREPARATION_BYTES,
    SourceUnavailable,
    UniverseHTTP,
    encoded,
    load_snapshot,
    prepare,
)
from replay.strategy_sdk import plain
from replay.streams.protocol import ProtocolError, choice, decode, obj, require
from replay.supervisor import read as read_supervisor_json
from replay.supervisor import read_success, validate as validate_supervisor


MAX_HISTORY_PAGES = 128
MAX_HISTORY_BODY_BYTES = 1024 * 1024
MAX_ARCHIVE_DEPTH = 16
MAX_ARCHIVE_BYTES = 1024 * 1024 * 1024 * 1024
MAX_SUPERVISOR_STDOUT_BYTES = 1024 * 1024
MAX_SUPERVISOR_STDERR_BYTES = 1024 * 1024
MAX_SUPERVISOR_SECONDS = 24 * 60 * 60
ARCHIVE_STATE_VERSION = 1

_TOP_LEVEL = frozenset(
    {
        "request.json",
        "resolved.json",
        "bundle.json",
        "context",
        "run",
        "result.json",
        "job_receipt.json",
        "archive_state.json",
    }
)
_ARCHIVE_ROOTS = frozenset(
    {"request.json", "resolved.json", "bundle.json", "context", "run", "result.json"}
)


class StageFailure(RuntimeError):
    """A typed runner outcome. Behavior depends on ``code``, never detail."""

    def __init__(self, code: str, detail: str | None = None):
        self.code = code
        self.detail = reason_detail(detail) if detail else None
        super().__init__(self.detail or code)


class LocalStateError(StageFailure):
    def __init__(self, detail: str):
        super().__init__("local_state_lost", detail)


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _regular(path: Path, *, maximum: int | None = None, links: bool = False) -> os.stat_result:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise LocalStateError(f"cannot inspect {path.name}: {error}") from error
    if not stat.S_ISREG(metadata.st_mode) or path.is_symlink():
        raise LocalStateError(f"{path.name} is not a regular file")
    if not links and metadata.st_nlink != 1:
        raise LocalStateError(f"{path.name} has multiple hard links")
    if maximum is not None and metadata.st_size > maximum:
        raise LocalStateError(f"{path.name} exceeds its byte budget")
    return metadata


def read_regular(path: Path, maximum: int) -> bytes:
    metadata = _regular(path, maximum=maximum)
    with path.open("rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) != metadata.st_size:
        raise LocalStateError(f"{path.name} changed while it was read")
    return raw


def _directory(path: Path, where: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as error:
        raise LocalStateError(f"cannot inspect {where}: {error}") from error
    if path.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise LocalStateError(f"{where} is not a regular directory")


def write_marker(path: Path, raw: bytes, *, mode: int = 0o600) -> None:
    """Create one exact durable marker; an existing marker must be byte-identical."""
    if path.exists() or path.is_symlink():
        if read_regular(path, len(raw)) != raw:
            raise LocalStateError(f"committed marker {path.name} disagrees with expected bytes")
        return
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.open")
    descriptor = None
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        with os.fdopen(descriptor, "wb", closefd=True) as target:
            descriptor = None
            target.write(raw)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode, follow_symlinks=False)
        fsync_directory(path.parent)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def initialize_job_root(jobs_root: Path, job_id: str, request_bytes: bytes) -> Path:
    jobs_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    _directory(jobs_root, "jobs root")
    root = jobs_root / job_id
    if root.exists() or root.is_symlink():
        raise LocalStateError("initialize requires an absent job directory")
    root.mkdir(mode=0o700)
    fsync_directory(jobs_root)
    write_marker(root / "request.json", request_bytes)
    return root


def validate_job_root(root: Path, request_bytes: bytes, request_sha256: str) -> None:
    try:
        metadata = root.lstat()
    except OSError as error:
        raise LocalStateError("resumed job directory is missing") from error
    if root.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise LocalStateError("resumed job root is not a regular directory")
    actual = read_regular(root / "request.json", MAX_REQUEST_BYTES)
    if actual != request_bytes or hashlib.sha256(encoded(plain(parse_request_document(actual)))).hexdigest() != request_sha256:
        raise LocalStateError("request.json identity disagrees with the submitted request")


def parse_request_document(raw: bytes):
    """Decode only for canonical request hashing when RunnerConfig is unavailable."""
    try:
        return decode(raw, MAX_REQUEST_BYTES)
    except ProtocolError as error:
        raise LocalStateError("request.json is not strict JSON") from error


def _timestamp_ns(value: object) -> int:
    from targeter.v2.models import isoformat, parse_timestamp

    parsed = parse_timestamp(value)
    if parsed is None or isoformat(parsed) != value:
        raise StageFailure("bundle_history_invalid", "history timestamp is not canonical")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    result = (delta.days * 86400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000
    if not 0 <= result < 2**64:
        raise StageFailure("bundle_history_invalid", "history timestamp is outside u64 ns")
    return result


class HistoryClient:
    """Bounded, strict client for the selected-order bundle history endpoint."""

    def __init__(self, base_url: str, *, timeout: float = 10, opener=None):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.opener = opener or build_opener(ProxyHandler({}), _NoRedirect())

    def resolve(self, job_id: str, request: Request, config: RunnerConfig) -> ResolvedJob:
        cursor = None
        seen_cursors: set[str] = set()
        seen_runs: set[str] = set()
        history: list[HistoryEntry] = []
        retirement = None
        for _ in range(MAX_HISTORY_PAGES):
            query = {"sort": "selected", "limit": "100"}
            if cursor is not None:
                query["cursor"] = cursor
            url = (
                f"{self.base_url}/v1/bundles/{quote(request.bundle_id, safe='')}/history?"
                + urlencode(query)
            )
            page = self._get(url)
            try:
                obj(page, "selections sort next_cursor")
                require(page["sort"] == "selected" and type(page["selections"]) is list)
                for selection in page["selections"]:
                    entry, selected_retirement = self._selection(selection, request.bundle_id)
                    if entry.run_id in seen_runs:
                        raise StageFailure("bundle_history_invalid", "bundle history repeats a run")
                    seen_runs.add(entry.run_id)
                    history.append(entry)
                    if selected_retirement is not None:
                        if retirement is not None and retirement != selected_retirement:
                            raise StageFailure("bundle_history_invalid", "bundle retirement is incoherent")
                        retirement = selected_retirement
                next_cursor = page["next_cursor"]
                require(next_cursor is None or (type(next_cursor) is str and bool(next_cursor)))
            except StageFailure:
                raise
            except (ProtocolError, ContractError, KeyError, TypeError) as error:
                raise StageFailure("bundle_history_invalid", "bundle history page is malformed") from error
            if next_cursor is None:
                break
            if next_cursor in seen_cursors:
                raise StageFailure("bundle_history_invalid", "bundle history cursor loop")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        else:
            raise StageFailure("bundle_history_invalid", "bundle history exceeds page limit")
        if retirement is None:
            raise StageFailure("bundle_not_retired", "bundle is not retired")
        try:
            bundle, job, occurrences = resolve_occurrences(history, retirement[0], request.interval)
            return ResolvedJob(
                job_id,
                request.sha256,
                request.bundle_id,
                config.canonical_window_seconds,
                bundle,
                job,
                occurrences,
            )
        except ContractError as error:
            raise StageFailure(error.code or "bundle_history_invalid", str(error)) from error

    def _get(self, url: str):
        try:
            with self.opener.open(url, timeout=self.timeout) as response:
                if response.status != 200:
                    raise StageFailure("universe_unavailable", f"Universe returned HTTP {response.status}")
                raw = response.read(MAX_HISTORY_BODY_BYTES + 1)
                if len(raw) > MAX_HISTORY_BODY_BYTES:
                    raise StageFailure("bundle_history_invalid", "bundle history body exceeds limit")
                return decode(raw, MAX_HISTORY_BODY_BYTES)
        except StageFailure:
            raise
        except HTTPError as error:
            code = "universe_unavailable" if error.code in {502, 503, 504} else "bundle_history_invalid"
            raise StageFailure(code, f"Universe returned HTTP {error.code}") from error
        except (URLError, TimeoutError, ConnectionError, OSError) as error:
            raise StageFailure("universe_unavailable", str(error)) from error
        except ProtocolError as error:
            raise StageFailure("bundle_history_invalid", "bundle history is not strict JSON") from error

    @staticmethod
    def _selection(value, bundle_id: str):
        obj(
            value,
            "run_id generated_at bundle_id occurrence_kind continuity_selected "
            "continuity_disposition sport game topology activation_at capture_start_at "
            "retirement source origin",
        )
        require(value["bundle_id"] == bundle_id and type(value["run_id"]) is str)
        choice(value["occurrence_kind"], "complete retained")
        require(type(value["continuity_selected"]) is bool)
        if value["continuity_disposition"] is not None:
            choice(value["continuity_disposition"], "held_current_candidate retained")
        for field in ("sport", "game", "topology", "activation_at", "capture_start_at"):
            require(type(value[field]) is str and bool(value[field]))
        source = obj(value["source"], "manifest_key manifest_sha256 report_key report_sha256")
        HistoryClient._source(value["run_id"], source)
        origin = obj(
            value["origin"],
            "run_id generated_at manifest_key manifest_sha256 report_key report_sha256",
        )
        HistoryClient._source(
            origin["run_id"],
            {name: origin[name] for name in source},
        )
        _timestamp_ns(origin["generated_at"])
        entry = HistoryEntry(
            value["run_id"],
            _timestamp_ns(value["generated_at"]),
            source["manifest_key"],
            source["manifest_sha256"],
            source["report_key"],
            source["report_sha256"],
        )
        retirement = value["retirement"]
        if retirement is None:
            return entry, None
        obj(retirement, "retired_at disposition terminal_observed_at source")
        obj(
            retirement["source"],
            "run_id manifest_key manifest_sha256 report_key report_sha256",
        )
        choice(retirement["disposition"], "all_markets_terminal terminal_clamp_elapsed")
        require(
            retirement["terminal_observed_at"]
            == (
                retirement["retired_at"]
                if retirement["disposition"] == "all_markets_terminal"
                else None
            )
        )
        retirement_source = retirement["source"]
        HistoryClient._source(
            retirement_source["run_id"],
            {name: retirement_source[name] for name in source},
        )
        return entry, (_timestamp_ns(retirement["retired_at"]), encoded(retirement))

    @staticmethod
    def _source(run_id, source):
        Occurrence(run_id, 0, 1, **source)
        prefix = source["manifest_key"].rsplit("/", 1)[0]
        require(
            source["report_key"]
            in {
                prefix + "/selection_report.json",
                prefix + "/selection_report.json.zst",
            }
        )


def validate_committed_markers(root: Path, stage: str) -> None:
    """Validate all markers that SQLite says must already be committed."""
    required = {
        "resolve": (),
        "bundle": ("resolved",),
        "prepare": ("resolved", "bundle"),
        "run": ("resolved", "bundle", "context"),
        "read": ("resolved", "bundle", "context", "run"),
        "archive": (),
    }[stage]
    validators = {
        "resolved": lambda: parse_resolved_job(read_regular(root / "resolved.json", MAX_RESOLVED_JOB_BYTES)),
        "bundle": lambda: parse_bundle_receipt(read_regular(root / "bundle.json", MAX_BUNDLE_RECEIPT_BYTES)),
        "context": lambda: load_snapshot(root / "context"),
        "run": lambda: read_success(root / "run"),
    }
    try:
        context = root / "context"
        if context.exists() or context.is_symlink():
            _directory(context, "preparation directory")
        run = root / "run"
        if run.exists() or run.is_symlink():
            _directory(run, "supervisor directory")
        if context.exists() and not (context / "receipt.json").exists():
            try:
                names = {path.name for path in context.iterdir()}
                if names - {".lock"}:
                    raise LocalStateError("preparation directory has no commit marker")
                if ".lock" in names:
                    _regular(context / ".lock")
            except OSError as error:
                raise LocalStateError(f"cannot inspect preparation directory: {error}") from error
        for name in required:
            validators[name]()
        # Existing later markers are immutable evidence even if SQLite did not
        # advance yet; validate them so the runner can adopt rather than rerun.
        for name, present in (
            ("resolved", (root / "resolved.json").exists()),
            ("bundle", (root / "bundle.json").exists()),
            ("context", (root / "context" / "receipt.json").exists()),
            ("run", (root / "run" / "SUCCESS.json").exists()),
        ):
            if present and name not in required:
                validators[name]()
        if (root / "result.json").exists():
            parse_job_result(read_regular(root / "result.json", MAX_JOB_RESULT_BYTES))
    except LocalStateError:
        raise
    except Exception as error:
        raise LocalStateError(f"committed {stage} state is invalid: {error}") from error


def resolve_stage(root: Path, job_id: str, request: Request, config: RunnerConfig, history: HistoryClient):
    path = root / "resolved.json"
    if path.exists():
        resolved = parse_resolved_job(read_regular(path, MAX_RESOLVED_JOB_BYTES))
    else:
        resolved = history.resolve(job_id, request, config)
        write_marker(path, resolved_job_bytes(resolved))
    if resolved.job_id != job_id or resolved.request_sha256 != request.sha256:
        raise LocalStateError("resolved.json identity mismatch")
    return resolved


def bundle_stage(root: Path, request: Request, resolved: ResolvedJob, ensure, **kwargs):
    path = root / "bundle.json"
    if path.exists():
        raw = read_regular(path, MAX_BUNDLE_RECEIPT_BYTES)
        receipt = parse_bundle_receipt(raw)
        try:
            outcome = ensure(request.bundle_id, resolved.window_interval, **kwargs)
        except BundleFailure as error:
            raise StageFailure(error.code, error.detail) from error
        if not isinstance(outcome, BundleReady) or outcome.receipt_bytes != raw:
            raise StageFailure("integrity_failure", "committed bundle marker cannot recover its exact pins")
        return receipt, raw, outcome.pins
    try:
        outcome = ensure(request.bundle_id, resolved.window_interval, **kwargs)
    except BundleFailure as error:
        raise StageFailure(error.code, error.detail) from error
    if isinstance(outcome, NotReady):
        raise StageFailure(outcome.code, outcome.detail)
    if isinstance(outcome, StaleCache):
        detail = encoded(
            {
                "cached_producer": outcome.cached_producer.document(),
                "current_producer": outcome.current_producer.document(),
            }
        ).decode()
        raise StageFailure("stale_bundle_cache", detail)
    if not isinstance(outcome, BundleReady):
        raise StageFailure("internal_failure", "ensure_bundle returned an unknown outcome")
    if outcome.receipt_bytes != bundle_receipt_bytes(outcome.receipt):
        raise StageFailure("integrity_failure", "bundle receipt bytes are not exact")
    write_marker(path, outcome.receipt_bytes)
    return outcome.receipt, outcome.receipt_bytes, outcome.pins


def _pins_for_interval(receipt: BundleReceipt, pins, interval):
    selected = []
    by_address = {pin.address: pin for pin in pins}
    for window in receipt.windows:
        if window.window_end_ns <= interval[0] or window.window_start_ns >= interval[1]:
            continue
        pin = by_address.get(window.derivative_address)
        if pin is None or pin.receipt_sha256 != window.receipt_sha256:
            raise StageFailure("integrity_failure", "bundle pins disagree with its receipt")
        selected.append(pin)
    if not selected:
        raise StageFailure("integrity_failure", "job interval has no derivative pins")
    return tuple(selected)


def prepare_stage(root: Path, request: Request, resolved: ResolvedJob, receipt: BundleReceipt, pins, config: RunnerConfig):
    context = root / "context"
    selected = _pins_for_interval(receipt, pins, resolved.job_interval)
    descriptor = receipt.producer.normalizer
    scales = {
        venue["venue"]: (
            venue["config"]["variables"]["price_scale"]["value"],
            venue["config"]["variables"]["quantity_scale"]["value"],
        )
        for venue in descriptor["venues"]
    }
    preparation = {
        "version": 1,
        "pins": [
            {"derivative_address": pin.address, "receipt_sha256": pin.receipt_sha256}
            for pin in selected
        ],
        "start_ns": str(resolved.job_interval[0]),
        "end_ns": str(resolved.job_interval[1]),
        "lower_bound": "clip",
        "bundle_id": request.bundle_id,
        "market_namespace": "targeter_target_id",
        "probe_markets": None if request.probe_markets is None else list(request.probe_markets),
        "occurrences": [occurrence.document() for occurrence in resolved.occurrences],
        "authorities": [
            {
                "venue": venue,
                "lane": config.authorities[venue],
                "price_scale": str(scales[venue][0]),
                "quantity_scale": str(scales[venue][1]),
            }
            for venue in sorted(config.authorities)
        ],
    }
    if (context / "receipt.json").exists():
        snapshot = load_snapshot(context)
        if plain(snapshot["config"]) != preparation:
            raise LocalStateError("preparation snapshot configuration mismatch")
        return snapshot
    try:
        return prepare(
            preparation,
            context,
            universe=UniverseHTTP(config.universe_base_url, timeout=10),
            fallback=None,
        )
    except SourceUnavailable as error:
        raise StageFailure("universe_unavailable", str(error)) from error
    except OSError as error:
        raise StageFailure("resource_exhausted", str(error)) from error
    except (ProtocolError, ValueError) as error:
        raise StageFailure("integrity_failure", str(error)) from error


def _import_callable(specification: str):
    module, name = specification.split(":", 1)
    return getattr(importlib.import_module(module), name)


def snapshot_sha256(context: Path) -> str:
    load_snapshot(context)
    try:
        receipt = decode(read_regular(context / "receipt.json", 4096), 4096)
        obj(receipt, "version snapshot_sha256 snapshot_byte_length config_sha256")
        require(
            type(receipt["snapshot_sha256"]) is str
            and len(receipt["snapshot_sha256"]) == 64
        )
        return receipt["snapshot_sha256"]
    except (ProtocolError, KeyError) as error:
        raise LocalStateError("preparation receipt is invalid") from error


def supervisor_config(
    job_id: str,
    request: Request,
    config: RunnerConfig,
    receipt: BundleReceipt,
    pins,
    snapshot,
    context: Path,
    *,
    publisher: Path,
    python: Path,
    image_revision: str,
):
    entry = config.strategies[request.strategy]
    preset = plain(config.limits[request.limits])
    selected = _pins_for_interval(receipt, pins, (int(snapshot["config"]["start_ns"]), int(snapshot["config"]["end_ns"])))
    strategy_config = plain(request.strategy_config)
    strategy_config.update(
        {
            "version": 1,
            "snapshot_directory": str(context.resolve()),
            "snapshot_sha256": snapshot_sha256(context),
        }
    )
    document = {
        "version": 1,
        "publisher": str(Path(publisher).resolve()),
        "python": str(Path(python).resolve()),
        "transport": {
            "run_id": job_id,
            "scope": config.scope,
            "normalizer": plain(receipt.producer.normalizer),
            "inputs": [
                {
                    "directory": str(pin.directory.resolve()),
                    "derivative_address": pin.address,
                    "receipt_sha256": pin.receipt_sha256,
                }
                for pin in selected
            ],
            "start_ns": snapshot["config"]["start_ns"],
            "end_ns": snapshot["config"]["end_ns"],
            "lower_bound": "clip",
            "plans": [plain(plan) for plan in snapshot["plans"]],
            "groups": [request.strategy],
            "command_timeout_ms": preset["command_timeout_ms"],
            "max_entry_bytes": preset["max_entry_bytes"],
            "max_queue_bytes": preset["max_queue_bytes"],
        },
        "strategies": {
            request.strategy: {
                "factory": entry.factory,
                "revision": image_revision,
                "config": strategy_config,
            }
        },
        "limits": {
            name: preset[name]
            for name in (
                "attempts",
                "no_progress",
                "progress_margin",
                "stall_seconds",
                "attempt_seconds",
                "run_seconds",
                "poll_seconds",
                "stop_seconds",
            )
        },
    }
    try:
        validate_supervisor(document)
    except OSError as error:
        raise StageFailure("resource_exhausted", str(error)) from error
    except (ProtocolError, ValueError) as error:
        raise StageFailure("integrity_failure", str(error)) from error
    return document


def _run_bounded(argv, *, env, timeout):
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            start_new_session=True,
            env=env,
        )
    except OSError as error:
        raise StageFailure("resource_exhausted", str(error)) from error
    output = {"stdout": bytearray(), "stderr": bytearray()}
    overrun = threading.Event()

    def kill():
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def drain(name, maximum):
        pipe = getattr(process, name)
        try:
            while chunk := pipe.read(65536):
                if len(output[name]) + len(chunk) > maximum:
                    overrun.set()
                    kill()
                    return
                output[name].extend(chunk)
        finally:
            pipe.close()

    threads = (
        threading.Thread(target=drain, args=("stdout", MAX_SUPERVISOR_STDOUT_BYTES)),
        threading.Thread(target=drain, args=("stderr", MAX_SUPERVISOR_STDERR_BYTES)),
    )
    for thread in threads:
        thread.start()
    try:
        try:
            process.wait(timeout=min(timeout, MAX_SUPERVISOR_SECONDS))
        except subprocess.TimeoutExpired:
            kill()
            process.wait()
            raise StageFailure("resource_exhausted", "supervisor subprocess timed out") from None
    finally:
        for thread in threads:
            thread.join()
    if overrun.is_set():
        raise StageFailure("resource_exhausted", "supervisor output exceeds its byte budget")
    return process.returncode


def run_stage(root: Path, document, *, python: Path, redis_url: str, scratch_root: Path):
    run_root = root / "run"
    if (run_root / "SUCCESS.json").exists():
        return read_success(run_root)
    scratch_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix="supervisor-", suffix=".json", dir=scratch_root, delete=False
    ) as temporary:
        temporary.write(encoded(document))
        temporary.flush()
        os.fsync(temporary.fileno())
        config_path = Path(temporary.name)
    os.chmod(config_path, 0o600)
    try:
        code = _run_bounded(
            [str(Path(python).resolve()), "-m", "replay.supervisor", str(config_path), str(run_root)],
            env={
                "LANG": "C.UTF-8",
                "PATH": "/usr/local/bin:/usr/bin:/bin",
                "REDIS_URL": redis_url,
            },
            timeout=document["limits"]["run_seconds"] + document["limits"]["stop_seconds"] + 60,
        )
    finally:
        config_path.unlink(missing_ok=True)
    if code == 20:
        raise StageFailure("supervisor_failed", "supervisor exited 20")
    if code == 21:
        raise StageFailure("supervisor_exhausted", "supervisor exited 21")
    if code != 0:
        raise StageFailure("resource_exhausted", f"supervisor exited {code}")
    try:
        return read_success(run_root)
    except Exception as error:
        raise StageFailure("result_invalid", f"supervisor success marker is invalid: {error}") from error


def read_stage(root: Path, job_id: str, request: Request, receipt_raw: bytes, snapshot, config: RunnerConfig):
    path = root / "result.json"
    if path.exists():
        result = parse_job_result(read_regular(path, MAX_JOB_RESULT_BYTES))
    else:
        entry = config.strategies[request.strategy]
        try:
            semantic = _import_callable(entry.reader)(root / "run", request.strategy)
            success = read_success(root / "run")
            state = read_supervisor_json(root / "run" / "state.json")
            result = JobResult(
                job_id=job_id,
                strategy=request.strategy,
                strategy_semantic_sha256=semantic["receipt"]["semantic_sha256"],
                snapshot_sha256=snapshot_sha256(root / "context"),
                bundle_receipt_sha256=hashlib.sha256(receipt_raw).hexdigest(),
                supervisor_identity=success["identity"],
                attempt_id=success["attempt"],
                attempts_started=state["attempts"],
            )
            write_marker(path, job_result_bytes(result))
        except Exception as error:
            raise StageFailure("result_invalid", str(error)) from error
    try:
        success = read_success(root / "run")
        state = read_supervisor_json(root / "run" / "state.json")
    except Exception as error:
        raise StageFailure("result_invalid", str(error)) from error
    if (
        result.job_id != job_id
        or result.strategy != request.strategy
        or result.snapshot_sha256 != snapshot_sha256(root / "context")
        or result.bundle_receipt_sha256 != hashlib.sha256(receipt_raw).hexdigest()
        or result.supervisor_identity != success["identity"]
        or result.attempt_id != success["attempt"]
        or result.attempts_started != state["attempts"]
    ):
        raise StageFailure("result_invalid", "result identity mismatch")
    return result


@dataclass(frozen=True)
class ArchiveState:
    job_id: str
    receipt_sha256: str
    receipt_byte_length: int
    finished_at_ns: int
    diagnostic: str | None = None

    def bytes(self):
        return encoded(
            {
                "replay_archive_state_version": ARCHIVE_STATE_VERSION,
                "job_id": self.job_id,
                "receipt_sha256": self.receipt_sha256,
                "receipt_byte_length": self.receipt_byte_length,
                "finished_at_ns": str(self.finished_at_ns),
                "diagnostic": self.diagnostic,
            }
        )


def parse_archive_state(raw: bytes) -> ArchiveState:
    try:
        value = decode(raw, MAX_JOB_RECEIPT_BYTES)
        obj(
            value,
            "replay_archive_state_version job_id receipt_sha256 receipt_byte_length finished_at_ns diagnostic",
        )
        require(value["replay_archive_state_version"] == ARCHIVE_STATE_VERSION)
        state = ArchiveState(
            value["job_id"],
            value["receipt_sha256"],
            value["receipt_byte_length"],
            int(value["finished_at_ns"]),
            value["diagnostic"],
        )
        require(state.bytes() == raw and len(state.receipt_sha256) == 64)
        return state
    except (ProtocolError, ValueError, TypeError, KeyError) as error:
        raise LocalStateError("archive_state.json is invalid") from error


def record_archive_diagnostic(root: Path, diagnostic: str) -> None:
    path = root / "archive_state.json"
    state = parse_archive_state(read_regular(path, MAX_JOB_RECEIPT_BYTES))
    updated = ArchiveState(
        state.job_id,
        state.receipt_sha256,
        state.receipt_byte_length,
        state.finished_at_ns,
        reason_detail(diagnostic),
    )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.open")
    try:
        with temporary.open("xb") as target:
            target.write(updated.bytes())
            target.flush()
            os.fsync(target.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _archive_files(root: Path):
    try:
        entries = tuple(os.scandir(root))
    except OSError as error:
        raise LocalStateError(f"cannot scan job root: {error}") from error
    unexpected = sorted(entry.name for entry in entries if entry.name not in _TOP_LEVEL)
    if unexpected:
        raise LocalStateError(f"unexpected top-level job entries: {unexpected}")
    files = []
    total = 0
    pending = []
    for entry in entries:
        if entry.name not in _ARCHIVE_ROOTS:
            continue
        path = Path(entry.path)
        if entry.is_dir(follow_symlinks=False):
            pending.append((path, 1))
        elif entry.is_file(follow_symlinks=False):
            pending.append((path, 0))
        else:
            raise LocalStateError(f"archive entry {entry.name} is not regular")
    while pending:
        path, depth = pending.pop()
        if depth > MAX_ARCHIVE_DEPTH:
            raise LocalStateError("archive tree exceeds depth limit")
        metadata = path.lstat()
        if stat.S_ISDIR(metadata.st_mode) and not path.is_symlink():
            with os.scandir(path) as children:
                for entry in children:
                    child = Path(entry.path)
                    if entry.is_dir(follow_symlinks=False):
                        pending.append((child, depth + 1))
                    elif entry.is_file(follow_symlinks=False):
                        pending.append((child, depth))
                    else:
                        raise LocalStateError(f"archive entry {child.relative_to(root)} is not regular")
            continue
        if not stat.S_ISREG(metadata.st_mode) or path.is_symlink() or metadata.st_nlink != 1:
            raise LocalStateError(f"archive file {path.relative_to(root)} is not an owned regular file")
        total += metadata.st_size
        if total > MAX_ARCHIVE_BYTES:
            raise LocalStateError("archive aggregate bytes exceed limit")
        files.append(path)
        if len(files) > MAX_JOB_OBJECTS:
            raise LocalStateError("archive file count exceeds limit")
    return tuple(sorted(files, key=lambda path: path.relative_to(root).as_posix()))


def _identity(path: Path) -> StoredIdentity:
    digest = hashlib.sha256()
    length = 0
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
            length += len(chunk)
    return StoredIdentity(digest.hexdigest(), length)


def _stage_identities(root: Path, row):
    identities = []
    resolved = bundle = result = success = None
    for path, maximum, parser, accessor in (
        (root / "resolved.json", MAX_RESOLVED_JOB_BYTES, parse_resolved_job, None),
        (root / "bundle.json", MAX_BUNDLE_RECEIPT_BYTES, parse_bundle_receipt, None),
        (root / "context" / "receipt.json", MAX_PREPARATION_BYTES, None, "snapshot_sha256"),
        (root / "run" / "SUCCESS.json", 1024 * 1024, None, "identity"),
        (root / "result.json", MAX_JOB_RESULT_BYTES, parse_job_result, "strategy_semantic_sha256"),
    ):
        if not path.exists():
            identities.append(None)
            continue
        if parser is parse_resolved_job or parser is parse_bundle_receipt:
            raw = read_regular(path, maximum)
            parsed = parser(raw)
            if parser is parse_resolved_job:
                resolved = parsed
                if parsed.job_id != row.job_id or parsed.request_sha256 != row.request_sha256:
                    raise LocalStateError("resolved marker disagrees with the job row")
            else:
                bundle = parsed
                if resolved is None or parsed.bundle_id != resolved.bundle_id:
                    raise LocalStateError("bundle marker disagrees with resolved job")
            identities.append(hashlib.sha256(raw).hexdigest())
        elif path.name == "receipt.json":
            snapshot = load_snapshot(root / "context")
            if (
                resolved is None
                or bundle is None
                or plain(snapshot["config"])["bundle_id"] != resolved.bundle_id
                or plain(snapshot["config"])["start_ns"] != str(resolved.job_interval[0])
                or plain(snapshot["config"])["end_ns"] != str(resolved.job_interval[1])
            ):
                raise LocalStateError("preparation marker disagrees with resolved job")
            available = {
                (window.derivative_address, window.receipt_sha256)
                for window in bundle.windows
            }
            if not all(
                (pin["derivative_address"], pin["receipt_sha256"]) in available
                for pin in snapshot["config"]["pins"]
            ):
                raise LocalStateError("preparation pins disagree with bundle receipt")
            identities.append(snapshot_sha256(root / "context"))
        elif path.name == "SUCCESS.json":
            success = read_success(root / "run")
            identities.append(success[accessor])
        else:
            result = parser(read_regular(path, maximum))
            if result.job_id != row.job_id:
                raise LocalStateError("result marker disagrees with the job row")
            identities.append(result.__getattribute__(accessor))
    absent = False
    for value in identities:
        if value is None:
            absent = True
        elif absent:
            raise LocalStateError("stage identity chain has a gap")
    if result is not None and (
        result.bundle_receipt_sha256 != identities[1]
        or result.snapshot_sha256 != identities[2]
        or result.supervisor_identity != identities[3]
        or success is None
        or result.attempt_id != success["attempt"]
    ):
        raise LocalStateError("result marker disagrees with prior stage identities")
    return identities


def freeze_job_receipt(root: Path | None, row, image_revision: str, now_ns: int):
    if root is None:
        files = ()
        identities = [None] * 5
    else:
        files = _archive_files(root)
        identities = _stage_identities(root, row)
    objects = tuple(
        JobObject(
            job_object_key(row.job_id, path.relative_to(root).as_posix()),
            identity.sha256,
            identity.byte_length,
        )
        for path in files
        for identity in (_identity(path),)
    )
    receipt = JobReceipt(
        row.job_id,
        row.request_sha256,
        row.submitted_by,
        image_revision,
        row.pending_outcome,
        row.reason_code,
        row.reason_detail,
        *identities,
        objects,
        row.created_at_ns,
        now_ns,
    )
    raw = job_receipt_bytes(receipt)
    state = ArchiveState(row.job_id, hashlib.sha256(raw).hexdigest(), len(raw), now_ns)
    if root is not None:
        receipt_path = root / "job_receipt.json"
        state_path = root / "archive_state.json"
        if receipt_path.exists():
            raw = read_regular(receipt_path, MAX_JOB_RECEIPT_BYTES)
            receipt = parse_job_receipt(raw)
            _validate_receipt_row(receipt, row, image_revision)
            if state_path.exists():
                state = parse_archive_state(read_regular(state_path, MAX_JOB_RECEIPT_BYTES))
                if (
                    state.job_id != row.job_id
                    or state.receipt_sha256 != hashlib.sha256(raw).hexdigest()
                    or state.receipt_byte_length != len(raw)
                    or state.finished_at_ns != receipt.finished_at_ns
                ):
                    raise LocalStateError("archive state disagrees with frozen receipt")
            else:
                state = ArchiveState(row.job_id, hashlib.sha256(raw).hexdigest(), len(raw), receipt.finished_at_ns)
                write_marker(state_path, state.bytes())
            return receipt, raw, state, files
        write_marker(receipt_path, raw)
        write_marker(state_path, state.bytes())
    return receipt, raw, state, files


def _validate_receipt_row(receipt: JobReceipt, row, image_revision: str) -> None:
    if (
        receipt.job_id != row.job_id
        or receipt.request_sha256 != row.request_sha256
        or receipt.submitted_by != row.submitted_by
        or receipt.image_revision != image_revision
        or receipt.final_outcome != row.pending_outcome
        or receipt.reason_code != row.reason_code
        or receipt.reason_detail != row.reason_detail
        or receipt.created_at_ns != row.created_at_ns
    ):
        raise LocalStateError("frozen job receipt disagrees with the job row")


def adopt_published_for_row(store, row, image_revision: str) -> int | None:
    """Bridge only a valid receipt-publication/SQLite-finalization crash."""
    try:
        raw = _read_remote(
            store,
            job_receipt_key(row.job_id),
            MAX_JOB_RECEIPT_BYTES,
            content_type=JSON_CONTENT_TYPE,
        )
        if raw is None:
            return None
        receipt = parse_job_receipt(raw)
        _validate_receipt_row(receipt, row, image_revision)
        _verify_remote_objects(store, receipt)
        return receipt.finished_at_ns
    except LocalStateError as error:
        raise StageFailure("archive_conflict", error.detail) from error
    except StageFailure:
        raise
    except (ObjectStoreError, OSError) as error:
        raise StageFailure("archive_unavailable", str(error)) from error
    except (ContractError, VerificationFailure) as error:
        raise StageFailure("archive_conflict", str(error)) from error


def verify_published_receipt(store, job_id: str) -> JobReceipt:
    """Strictly read a committed job receipt and every object it names."""
    try:
        raw = _read_remote(
            store,
            job_receipt_key(job_id),
            MAX_JOB_RECEIPT_BYTES,
            content_type=JSON_CONTENT_TYPE,
        )
        if raw is None:
            raise StageFailure("archive_conflict", "job receipt is absent")
        receipt = parse_job_receipt(raw)
        if receipt.job_id != job_id:
            raise StageFailure("archive_conflict", "job receipt has the wrong job ID")
        _verify_remote_objects(store, receipt)
        return receipt
    except StageFailure:
        raise
    except (ObjectStoreError, OSError) as error:
        raise StageFailure("archive_unavailable", str(error)) from error
    except (ContractError, VerificationFailure) as error:
        raise StageFailure("archive_conflict", str(error)) from error


def _expectation(metadata):
    if not metadata.provider_checksum or not metadata.provider_checksum_algorithm:
        raise StageFailure("archive_conflict", f"remote object {metadata.key} lacks provider checksum metadata")
    return ObjectExpectation(
        metadata.key,
        metadata.stored,
        metadata.provider_checksum,
        metadata.provider_checksum_algorithm,
        metadata.content_type,
        metadata.content_encoding,
    )


def _read_remote(store, key, maximum, *, content_type=None):
    metadata = store.head(key)
    if metadata is None:
        return None
    if metadata.byte_length > maximum:
        raise StageFailure("archive_conflict", f"remote {key} exceeds limit")
    if content_type is not None and (
        metadata.content_type != content_type or metadata.content_encoding is not None
    ):
        raise StageFailure("archive_conflict", f"remote {key} has invalid provider metadata")
    chunks = []
    with store.open_verified(_expectation(metadata)) as source:
        while chunk := source.read(1024 * 1024):
            chunks.append(chunk)
    return b"".join(chunks)


def _verify_remote_objects(store, receipt: JobReceipt) -> None:
    prefix = f"replay/jobs/{receipt.job_id}/"
    objects = {item.key.removeprefix(prefix): item for item in receipt.objects}
    selected = {}
    bounds = {
        "resolved.json": MAX_RESOLVED_JOB_BYTES,
        "bundle.json": MAX_BUNDLE_RECEIPT_BYTES,
        "context/receipt.json": 4096,
        "run/SUCCESS.json": 1024 * 1024,
        "result.json": MAX_JOB_RESULT_BYTES,
    }
    for item in receipt.objects:
        metadata = store.head(item.key)
        if metadata is None or not metadata.matches(StoredIdentity(item.sha256, item.byte_length)):
            raise StageFailure("archive_conflict", f"remote object {item.key} disagrees with receipt")
        relative = item.key.removeprefix(prefix)
        maximum = bounds.get(relative)
        if maximum is None:
            with store.open_verified(_expectation(metadata)) as source:
                while source.read(1024 * 1024):
                    pass
        else:
            selected[relative] = _read_remote(store, item.key, maximum)

    direct = (
        ("resolved.json", receipt.resolved_sha256),
        ("bundle.json", receipt.bundle_receipt_sha256),
    )
    for relative, expected in direct:
        item = objects.get(relative)
        if (item is None) != (expected is None) or (
            item is not None and item.sha256 != expected
        ):
            raise StageFailure("archive_conflict", f"remote {relative} identity chain mismatch")
    if receipt.snapshot_sha256 is not None:
        try:
            preparation = decode(selected["context/receipt.json"], 4096)
            obj(preparation, "version snapshot_sha256 snapshot_byte_length config_sha256")
            require(preparation["snapshot_sha256"] == receipt.snapshot_sha256)
            context = objects["context/context.json"]
            require(
                context.sha256 == receipt.snapshot_sha256
                and context.byte_length == preparation["snapshot_byte_length"]
            )
        except (KeyError, ProtocolError) as error:
            raise StageFailure("archive_conflict", "remote snapshot identity chain mismatch") from error
    if receipt.supervisor_identity is not None:
        try:
            success = decode(selected["run/SUCCESS.json"], 1024 * 1024)
            obj(success, "version identity attempt terminal outputs")
            require(success["identity"] == receipt.supervisor_identity)
        except (KeyError, ProtocolError) as error:
            raise StageFailure("archive_conflict", "remote supervisor identity chain mismatch") from error
    if receipt.strategy_semantic_sha256 is not None:
        try:
            result = parse_job_result(selected["result.json"])
        except (KeyError, ContractError) as error:
            raise StageFailure("archive_conflict", "remote result identity chain mismatch") from error
        if (
            result.job_id != receipt.job_id
            or result.strategy_semantic_sha256 != receipt.strategy_semantic_sha256
            or result.snapshot_sha256 != receipt.snapshot_sha256
            or result.bundle_receipt_sha256 != receipt.bundle_receipt_sha256
            or result.supervisor_identity != receipt.supervisor_identity
        ):
            raise StageFailure("archive_conflict", "remote result identity chain mismatch")


def adopt_remote_receipt(store, expected: JobReceipt, expected_raw: bytes) -> bool:
    key = job_receipt_key(expected.job_id)
    try:
        remote = _read_remote(
            store, key, MAX_JOB_RECEIPT_BYTES, content_type=JSON_CONTENT_TYPE
        )
        if remote is None:
            return False
        receipt = parse_job_receipt(remote)
        if remote != expected_raw or receipt != expected:
            raise StageFailure("archive_conflict", "remote job receipt disagrees with frozen receipt")
        _verify_remote_objects(store, receipt)
        return True
    except StageFailure:
        raise
    except (ObjectStoreError, OSError) as error:
        raise StageFailure("archive_unavailable", str(error)) from error
    except (ContractError, VerificationFailure) as error:
        raise StageFailure("archive_conflict", str(error)) from error


def archive_stage(root: Path | None, row, store, image_revision: str, now_ns: int):
    try:
        receipt, raw, state, files = freeze_job_receipt(root, row, image_revision, now_ns)
        if adopt_remote_receipt(store, receipt, raw):
            return state.finished_at_ns
        if root is None:
            files = ()
        by_relative = {path.relative_to(root).as_posix(): path for path in files}
        for item in receipt.objects:
            relative = item.key.removeprefix(f"replay/jobs/{row.job_id}/")
            path = by_relative.get(relative)
            if path is None:
                raise LocalStateError(f"frozen archive object {relative} is missing")
            identity = _identity(path)
            if identity != StoredIdentity(item.sha256, item.byte_length):
                raise LocalStateError(f"archive object {relative} changed after freeze")
            with path.open("rb") as source:
                store.put_immutable(item.key, source, identity)
        identity = StoredIdentity(hashlib.sha256(raw).hexdigest(), len(raw))
        store.put_immutable(job_receipt_key(row.job_id), io.BytesIO(raw), identity, content_type=JSON_CONTENT_TYPE)
        if not adopt_remote_receipt(store, receipt, raw):
            raise StageFailure("archive_unavailable", "job receipt was not visible after upload")
        return state.finished_at_ns
    except StageFailure:
        raise
    except IntegrityConflict as error:
        raise StageFailure("archive_conflict", str(error)) from error
    except (ObjectStoreError, OSError) as error:
        raise StageFailure("archive_unavailable", str(error)) from error
