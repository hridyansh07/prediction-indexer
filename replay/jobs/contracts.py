"""Closed shared contracts for Replay jobs V1 (``docs/REPLAY_JOBS_V1.md`` §3).

Pure: no filesystem, network, clock, or randomness. The Universe server and the
runner both import this module; neither redefines these shapes. A change here is
a W0 contract amendment, not a local edit by one workstream.
"""

import re
from dataclasses import dataclass
from datetime import date, timedelta
from types import MappingProxyType
from urllib.parse import urlsplit

from replay.preparation import digest, encoded
from replay.streams.protocol import ProtocolError, decode, freeze, obj, uint
from replay.supervisor import _normalizer_descriptor

REQUEST_VERSION = 1
BUNDLE_RECEIPT_VERSION = 1
RUNNER_CONFIG_VERSION = 1

MAX_REQUEST_BYTES = 64 * 1024
MAX_BUNDLE_RECEIPT_BYTES = 1024 * 1024
MAX_RUNNER_CONFIG_BYTES = 1024 * 1024
MAX_PROBE_MARKETS = 4096
MAX_BUNDLE_WINDOWS = 4096

_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,128}")
_HEX64 = re.compile(r"[0-9a-f]{64}")
_JOB_ID = re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{16}")
_FACTORY = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*")
_NS_PER_DAY = 86_400_000_000_000


class ContractError(ValueError):
    """Input that does not satisfy a closed Replay jobs contract."""


def _require(ok, message):
    if not ok:
        raise ContractError(message)


def _closed(value, fields, where):
    try:
        return obj(value, fields)
    except ProtocolError:
        raise ContractError(f"{where} must have exactly the fields: {fields}") from None


def _uint(value, where):
    try:
        return uint(value)
    except ProtocolError:
        raise ContractError(f"{where} must be a canonical unsigned decimal string") from None


def _identifier(value, where):
    _require(
        type(value) is str
        and _IDENTIFIER.fullmatch(value) is not None
        and value not in (".", ".."),
        f"{where} must match [A-Za-z0-9_.-]{{1,128}}",
    )
    return value


def _hex64(value, where):
    _require(
        type(value) is str and _HEX64.fullmatch(value) is not None,
        f"{where} must be 64 lowercase hex characters",
    )
    return value


def _positive_int(value, where):
    _require(type(value) is int and value > 0, f"{where} must be a positive integer")
    return value


def _decode(raw, limit, where):
    _require(type(raw) is bytes, f"{where} must be bytes")
    _require(len(raw) <= limit, f"{where} exceeds {limit} bytes")
    try:
        return decode(raw, limit)
    except ProtocolError:
        raise ContractError(f"{where} is not strict JSON") from None


# --- Job status and stage (§3.2) -------------------------------------------

QUEUED = "queued"
RUNNING = "running"
ARCHIVING = "archiving"
SUCCEEDED = "succeeded"
FAILED = "failed"
EXHAUSTED = "exhausted"
NOT_READY = "not_ready"
STALE_BUNDLE_CACHE = "stale_bundle_cache"
CANCELLED = "cancelled"

STATUSES = frozenset(
    {
        QUEUED,
        RUNNING,
        ARCHIVING,
        SUCCEEDED,
        FAILED,
        EXHAUSTED,
        NOT_READY,
        STALE_BUNDLE_CACHE,
        CANCELLED,
    }
)
TERMINAL = frozenset(
    {SUCCEEDED, FAILED, EXHAUSTED, NOT_READY, STALE_BUNDLE_CACHE, CANCELLED}
)
RESUMABLE = frozenset({RUNNING, ARCHIVING})
ARCHIVED_OUTCOMES = frozenset({SUCCEEDED, FAILED, EXHAUSTED})

STAGES = ("resolve", "bundle", "prepare", "run", "read", "archive")

ALLOWED_TRANSITIONS = MappingProxyType(
    {
        QUEUED: frozenset({RUNNING, CANCELLED}),
        RUNNING: frozenset({ARCHIVING, NOT_READY, STALE_BUNDLE_CACHE}),
        ARCHIVING: frozenset(ARCHIVED_OUTCOMES),
        **{status: frozenset() for status in TERMINAL},
    }
)


def check_transition(current, new):
    _require(current in STATUSES, f"unknown job status {current!r}")
    _require(new in STATUSES, f"unknown job status {new!r}")
    _require(
        new in ALLOWED_TRANSITIONS[current],
        f"job status cannot move from {current} to {new}",
    )


def check_stage(stage):
    _require(stage in STAGES, f"unknown job stage {stage!r}")
    return stage


# --- Jobs table (§3.3) ------------------------------------------------------

JOBS_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    created_at_ns INTEGER NOT NULL CHECK(created_at_ns >= 0),
    submitted_by TEXT NOT NULL,
    request_json BLOB NOT NULL,
    request_sha256 TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'queued', 'running', 'archiving', 'succeeded', 'failed', 'exhausted',
        'not_ready', 'stale_bundle_cache', 'cancelled'
    )),
    stage TEXT CHECK(stage IS NULL OR stage IN (
        'resolve', 'bundle', 'prepare', 'run', 'read', 'archive'
    )),
    reason TEXT,
    started_at_ns INTEGER,
    updated_at_ns INTEGER NOT NULL,
    finished_at_ns INTEGER,
    archive_receipt_key TEXT
) STRICT;
CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, created_at_ns, job_id);
"""


def job_id(now_ns, suffix_hex):
    """Deterministic ID from caller-supplied time and 64 bits of randomness."""
    _require(type(now_ns) is int and now_ns >= 0, "job time must be nonnegative ns")
    _require(
        type(suffix_hex) is str and re.fullmatch(r"[0-9a-f]{16}", suffix_hex) is not None,
        "job suffix must be 16 lowercase hex characters",
    )
    seconds = now_ns // 1_000_000_000
    day = date(1970, 1, 1) + timedelta(days=seconds // 86_400)
    remainder = seconds % 86_400
    clock = f"{remainder // 3600:02}{remainder % 3600 // 60:02}{remainder % 60:02}"
    return f"{day:%Y%m%d}T{clock}Z-{suffix_hex}"


def check_job_id(value):
    _require(
        type(value) is str and _JOB_ID.fullmatch(value) is not None,
        "job id must be <yyyymmddTHHMMSSZ>-<16 hex>",
    )
    return value


# --- Object keys (§3.4) -----------------------------------------------------


def date_partition(window_start_ns):
    """UTC date of a window start, matching the finalizer's ``date_partition``."""
    _require(
        type(window_start_ns) is int and window_start_ns >= 0,
        "window start must be nonnegative ns",
    )
    return f"{date(1970, 1, 1) + timedelta(days=window_start_ns // _NS_PER_DAY):%Y-%m-%d}"


def canonical_window_keys(window_start_ns):
    """``(evidence, provenance, receipt)`` keys of one archived canonical window."""
    base = f"canonical/date={date_partition(window_start_ns)}/window={window_start_ns}"
    return (
        f"{base}/evidence.ndjson.zst",
        f"{base}/provenance.ndjson.zst",
        f"{base}/receipt.json",
    )


DERIVATIVE_FILES = (
    "events.ndjson.zst",
    "rejects.ndjson.zst",
    "sources.ndjson.zst",
    "manifest.json",
    "receipt.json",
)


def derivative_key(derivative_address, file):
    _hex64(derivative_address, "derivative address")
    _require(file in DERIVATIVE_FILES, f"unknown derivative file {file!r}")
    return f"replay/derivatives/{derivative_address}/{file}"


def bundle_receipt_key(bundle_id):
    return f"replay/bundles/{_identifier(bundle_id, 'bundle id')}/bundle_receipt.json"


def job_object_key(job_id_value, relative):
    check_job_id(job_id_value)
    parts = relative.split("/") if type(relative) is str else []
    _require(
        bool(parts) and all(part not in ("", ".", "..") for part in parts),
        "job object path must be a nonempty relative path without traversal",
    )
    _require(relative != "job_receipt.json", "job_receipt.json is reserved")
    return f"replay/jobs/{job_id_value}/{relative}"


def job_receipt_key(job_id_value):
    return f"replay/jobs/{check_job_id(job_id_value)}/job_receipt.json"


def window_bounds(start_ns, end_ns, window_seconds):
    """Aligned ``[first_start, last_end)`` and every window start covering a range."""
    _require(start_ns < end_ns, "interval must satisfy start_ns < end_ns")
    period = _window_period(window_seconds)
    first = start_ns // period * period
    last = -(-end_ns // period) * period
    return first, last, tuple(range(first, last, period))


def _window_period(window_seconds):
    _require(
        type(window_seconds) is int
        and window_seconds > 0
        and 86_400 % window_seconds == 0,
        "canonical_window_seconds must be a positive divisor of 86400",
    )
    return window_seconds * 1_000_000_000


# --- Request (§3.1) ---------------------------------------------------------


@dataclass(frozen=True)
class Request:
    bundle_id: str
    probe_markets: tuple[str, ...] | None
    interval: tuple[int, int] | None
    strategy: str
    strategy_config: MappingProxyType
    limits: str
    document: MappingProxyType

    @property
    def sha256(self):
        return request_sha256(self)


def parse_request(raw, config):
    """Strictly parse submitted request bytes against a parsed runner config."""
    value = _decode(raw, MAX_REQUEST_BYTES, "request")
    _closed(
        value,
        "replay_request_version bundle_id probe_markets interval strategy limits",
        "request",
    )
    _require(
        type(value["replay_request_version"]) is int
        and value["replay_request_version"] == REQUEST_VERSION,
        f"replay_request_version must be {REQUEST_VERSION}",
    )
    bundle_id = _identifier(value["bundle_id"], "bundle_id")

    probe = value["probe_markets"]
    if probe is not None:
        _require(
            type(probe) is list and 0 < len(probe) <= MAX_PROBE_MARKETS,
            f"probe_markets must be null or a list of 1-{MAX_PROBE_MARKETS} ids",
        )
        for market in probe:
            _require(
                type(market) is str
                and re.fullmatch(r"[a-z]+:[^\s]+", market) is not None,
                "probe_markets entries must be venue:native-id",
            )
        _require(
            probe == sorted(set(probe)),
            "probe_markets must be sorted and unique",
        )
        probe = tuple(probe)

    interval = value["interval"]
    if interval is not None:
        _closed(interval, "start_ns end_ns", "interval")
        start = _uint(interval["start_ns"], "interval.start_ns")
        end = _uint(interval["end_ns"], "interval.end_ns")
        _require(start < end, "interval must satisfy start_ns < end_ns")
        interval = (start, end)

    strategy = _closed(value["strategy"], "name config", "strategy")
    name = strategy["name"]
    _require(
        type(name) is str and name in config.strategies,
        f"strategy.name must be one of: {', '.join(sorted(config.strategies))}",
    )
    strategy_config = _check_strategy_config(
        config.strategies[name].config_schema, strategy["config"]
    )

    limits = value["limits"]
    _require(
        type(limits) is str and limits in config.limits,
        f"limits must be one of: {', '.join(sorted(config.limits))}",
    )
    return Request(
        bundle_id=bundle_id,
        probe_markets=probe,
        interval=interval,
        strategy=name,
        strategy_config=freeze(strategy_config),
        limits=limits,
        document=freeze(value),
    )


def request_sha256(request):
    return digest(_plain(request.document))


def _plain(value):
    if isinstance(value, MappingProxyType):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, tuple):
        return [_plain(v) for v in value]
    return value


# --- Strategy config schemas ------------------------------------------------


@dataclass(frozen=True)
class StrategyConfigSchema:
    request_keys: frozenset
    runner_keys: frozenset


#: Registry schemas a runner config may name. ``request_keys`` may be supplied
#: by a request; ``runner_keys`` are filled by the runner and rejected from one.
STRATEGY_CONFIG_SCHEMAS = MappingProxyType(
    {
        "bundle_coverage_v1": StrategyConfigSchema(
            request_keys=frozenset(),
            runner_keys=frozenset({"version", "snapshot_directory", "snapshot_sha256"}),
        ),
    }
)


def _check_strategy_config(schema_name, value):
    schema = STRATEGY_CONFIG_SCHEMAS[schema_name]
    _require(type(value) is dict, "strategy.config must be an object")
    owned = sorted(set(value) & schema.runner_keys)
    _require(not owned, f"strategy.config may not set runner-owned keys: {owned}")
    unknown = sorted(set(value) - schema.request_keys)
    _require(not unknown, f"strategy.config has unknown keys: {unknown}")
    return value


# --- Bundle receipt (§3.5) --------------------------------------------------


@dataclass(frozen=True)
class BundleWindow:
    window_start_ns: int
    window_end_ns: int
    canonical_receipt_sha256: str
    derivative_address: str
    receipt_sha256: str


@dataclass(frozen=True)
class BundleReceipt:
    bundle_id: str
    start_ns: int
    end_ns: int
    canonical_window_seconds: int
    normalizer: MappingProxyType
    windows: tuple[BundleWindow, ...]
    built_by_job: str

    def normalizer_document(self):
        return _plain(self.normalizer)


def parse_bundle_receipt(raw):
    value = _decode(raw, MAX_BUNDLE_RECEIPT_BYTES, "bundle receipt")
    _closed(
        value,
        "replay_bundle_receipt_version bundle_id interval canonical_window_seconds "
        "normalizer windows built_by_job",
        "bundle receipt",
    )
    _require(
        type(value["replay_bundle_receipt_version"]) is int
        and value["replay_bundle_receipt_version"] == BUNDLE_RECEIPT_VERSION,
        f"replay_bundle_receipt_version must be {BUNDLE_RECEIPT_VERSION}",
    )
    interval = _closed(value["interval"], "start_ns end_ns", "bundle receipt interval")
    start = _uint(interval["start_ns"], "bundle receipt interval.start_ns")
    end = _uint(interval["end_ns"], "bundle receipt interval.end_ns")
    windows = value["windows"]
    _require(
        type(windows) is list and 0 < len(windows) <= MAX_BUNDLE_WINDOWS,
        f"bundle receipt windows must list 1-{MAX_BUNDLE_WINDOWS} windows",
    )
    parsed = tuple(_bundle_window(window) for window in windows)
    receipt = BundleReceipt(
        bundle_id=_identifier(value["bundle_id"], "bundle receipt bundle_id"),
        start_ns=start,
        end_ns=end,
        canonical_window_seconds=value["canonical_window_seconds"],
        normalizer=freeze(value["normalizer"]),
        windows=parsed,
        built_by_job=check_job_id(value["built_by_job"]),
    )
    _check_bundle_receipt(receipt)
    _require(
        bundle_receipt_bytes(receipt) == raw,
        "bundle receipt is not in canonical serialization",
    )
    return receipt


def _bundle_window(value):
    _closed(
        value,
        "window_start_ns window_end_ns canonical_receipt_sha256 "
        "derivative_address receipt_sha256",
        "bundle receipt window",
    )
    return BundleWindow(
        window_start_ns=_uint(value["window_start_ns"], "window_start_ns"),
        window_end_ns=_uint(value["window_end_ns"], "window_end_ns"),
        canonical_receipt_sha256=_hex64(
            value["canonical_receipt_sha256"], "canonical_receipt_sha256"
        ),
        derivative_address=_hex64(value["derivative_address"], "derivative_address"),
        receipt_sha256=_hex64(value["receipt_sha256"], "receipt_sha256"),
    )


def _check_bundle_receipt(receipt):
    try:
        _normalizer_descriptor(receipt.normalizer_document())
    except ProtocolError:
        raise ContractError("bundle receipt normalizer is not a valid identity") from None
    first, last, starts = window_bounds(
        receipt.start_ns, receipt.end_ns, receipt.canonical_window_seconds
    )
    _require(
        (first, last) == (receipt.start_ns, receipt.end_ns),
        "bundle receipt interval must be aligned to canonical windows",
    )
    period = _window_period(receipt.canonical_window_seconds)
    _require(
        tuple(w.window_start_ns for w in receipt.windows) == starts
        and all(w.window_end_ns == w.window_start_ns + period for w in receipt.windows),
        "bundle receipt windows must be ordered, adjacent, and exactly cover its interval",
    )
    addresses = [w.derivative_address for w in receipt.windows]
    _require(
        len(set(addresses)) == len(addresses),
        "bundle receipt repeats a derivative address",
    )


def bundle_receipt_bytes(receipt):
    """Canonical serialization; the exact bytes uploaded and hashed."""
    _check_bundle_receipt(receipt)
    return encoded(
        {
            "replay_bundle_receipt_version": BUNDLE_RECEIPT_VERSION,
            "bundle_id": _identifier(receipt.bundle_id, "bundle id"),
            "interval": {
                "start_ns": str(receipt.start_ns),
                "end_ns": str(receipt.end_ns),
            },
            "canonical_window_seconds": receipt.canonical_window_seconds,
            "normalizer": receipt.normalizer_document(),
            "windows": [
                {
                    "window_start_ns": str(w.window_start_ns),
                    "window_end_ns": str(w.window_end_ns),
                    "canonical_receipt_sha256": w.canonical_receipt_sha256,
                    "derivative_address": w.derivative_address,
                    "receipt_sha256": w.receipt_sha256,
                }
                for w in receipt.windows
            ],
            "built_by_job": check_job_id(receipt.built_by_job),
        }
    )


# --- Runner configuration (§3.6) --------------------------------------------

VENUES = ("kalshi", "limitless", "polymarket")

_LIMIT_FIELDS = (
    "max_entry_bytes max_queue_bytes command_timeout_ms attempts no_progress "
    "progress_margin stall_seconds attempt_seconds run_seconds poll_seconds stop_seconds"
)


@dataclass(frozen=True)
class StrategyEntry:
    factory: str
    reader: str
    config_schema: str


@dataclass(frozen=True)
class RunnerConfig:
    universe_base_url: str
    scope: str
    canonical_window_seconds: int
    authorities: MappingProxyType
    strategies: MappingProxyType
    limits: MappingProxyType


def parse_runner_config(raw):
    value = _decode(raw, MAX_RUNNER_CONFIG_BYTES, "runner config")
    _closed(
        value,
        "replay_runner_config_version universe_base_url scope "
        "canonical_window_seconds authorities strategies limits",
        "runner config",
    )
    _require(
        type(value["replay_runner_config_version"]) is int
        and value["replay_runner_config_version"] == RUNNER_CONFIG_VERSION,
        f"replay_runner_config_version must be {RUNNER_CONFIG_VERSION}",
    )
    url = value["universe_base_url"]
    parts = urlsplit(url) if type(url) is str else None
    _require(
        parts is not None
        and parts.scheme in ("http", "https")
        and bool(parts.netloc)
        and not parts.query
        and not parts.fragment
        and not parts.username
        and not parts.password,
        "universe_base_url must be an http(s) URL without credentials or query",
    )
    _window_period(value["canonical_window_seconds"])

    authorities = _closed(value["authorities"], " ".join(VENUES), "authorities")
    for venue, lane in authorities.items():
        _identifier(lane, f"authorities.{venue}")

    strategies = value["strategies"]
    _require(
        type(strategies) is dict and bool(strategies),
        "strategies must be a nonempty object",
    )
    entries = {}
    for name, entry in strategies.items():
        _identifier(name, "strategy name")
        _closed(entry, "factory reader config_schema", f"strategies.{name}")
        for field in ("factory", "reader"):
            _require(
                type(entry[field]) is str and _FACTORY.fullmatch(entry[field]) is not None,
                f"strategies.{name}.{field} must be module:function",
            )
        _require(
            entry["config_schema"] in STRATEGY_CONFIG_SCHEMAS,
            f"strategies.{name}.config_schema is unknown",
        )
        entries[name] = StrategyEntry(**entry)

    presets = value["limits"]
    _require(type(presets) is dict and bool(presets), "limits must be a nonempty object")
    for name, preset in presets.items():
        _identifier(name, "limits preset name")
        _check_limits(_closed(preset, _LIMIT_FIELDS, f"limits.{name}"), name)

    return RunnerConfig(
        universe_base_url=url,
        scope=_identifier(value["scope"], "scope"),
        canonical_window_seconds=value["canonical_window_seconds"],
        authorities=MappingProxyType(dict(authorities)),
        strategies=MappingProxyType(entries),
        limits=freeze(presets),
    )


def _check_limits(preset, name):
    """Early mirror of ``replay.supervisor.validate``; the supervisor stays authoritative."""
    where = f"limits.{name}"
    for field in ("max_entry_bytes", "max_queue_bytes", "command_timeout_ms"):
        _positive_int(preset[field], f"{where}.{field}")
    _require(
        2 <= preset["command_timeout_ms"] <= 60_000,
        f"{where}.command_timeout_ms must be 2-60000",
    )
    _require(
        preset["max_entry_bytes"] <= preset["max_queue_bytes"] <= 1_000_000_000,
        f"{where} requires max_entry_bytes <= max_queue_bytes <= 1000000000",
    )
    for field in ("attempts", "no_progress", "progress_margin"):
        _positive_int(preset[field], f"{where}.{field}")
    for field in ("stall_seconds", "attempt_seconds", "run_seconds", "poll_seconds", "stop_seconds"):
        number = preset[field]
        _require(
            type(number) in (int, float) and number > 0 and number != float("inf"),
            f"{where}.{field} must be a positive finite number",
        )
    _require(
        preset["poll_seconds"]
        < preset["stall_seconds"]
        <= preset["attempt_seconds"]
        <= preset["run_seconds"],
        f"{where} requires poll < stall <= attempt <= run seconds",
    )
