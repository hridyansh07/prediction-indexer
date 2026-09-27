"""Bounded, secret-safe production preflight for Replay scheduling."""

from __future__ import annotations

import os
import re
import sqlite3
import stat
import subprocess
import urllib.error
import urllib.request
from contextlib import closing
from pathlib import Path

from archive.storage.factory import build_store
from replay.jobs.contracts import (
    RUNNER_CONFIG_VERSION,
    parse_producer,
    parse_runner_config,
)
from universe.auth import AuthStore
from universe.config import load_config
from universe.replay_jobs import ReplayJobStore

MAX_DESCRIPTOR_BYTES = 1024 * 1024
_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_PUBLIC_HOST = re.compile(
    r"(?=.{1,253}\Z)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z"
)


class PreflightError(RuntimeError):
    pass


def check_private_root(root: Path) -> None:
    root = Path(root)
    metadata = root.stat(follow_symlinks=False)
    if not stat.S_ISDIR(metadata.st_mode) or root.is_symlink():
        raise PreflightError("REPLAY_DATA_ROOT must be a regular directory")
    if metadata.st_uid != os.geteuid() or metadata.st_gid != os.getegid():
        raise PreflightError("REPLAY_DATA_ROOT must be owned by the runtime uid and gid")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise PreflightError("REPLAY_DATA_ROOT must not grant group or world access")
    probe = root / f".preflight-{os.getpid()}"
    try:
        with probe.open("xb") as handle:
            handle.write(b"ok")
            handle.flush()
            os.fsync(handle.fileno())
    except OSError as error:
        raise PreflightError("REPLAY_DATA_ROOT is not durably writable") from error
    finally:
        probe.unlink(missing_ok=True)


def check_capacity(filesystem, *, required_bytes: int, required_inodes: int, quota_bytes: int) -> None:
    for name, value in (
        ("required_bytes", required_bytes),
        ("required_inodes", required_inodes),
        ("quota_bytes", quota_bytes),
    ):
        if type(value) is not int or value <= 0:
            raise PreflightError(f"{name} must be a positive integer")
    available = filesystem.f_bavail * filesystem.f_frsize
    if quota_bytes < required_bytes:
        raise PreflightError("declared filesystem/project quota is below required capacity")
    if available < required_bytes:
        raise PreflightError("free bytes are below required capacity")
    if filesystem.f_favail < required_inodes:
        raise PreflightError("free inodes are below required capacity")


def _positive_environment(environment, name: str) -> int:
    value = environment.get(name, "")
    try:
        parsed = int(value)
    except ValueError as error:
        raise PreflightError(f"{name} must be a positive integer") from error
    if parsed <= 0 or str(parsed) != value:
        raise PreflightError(f"{name} must be a positive integer")
    return parsed


def _required(environment, *names: str) -> dict[str, str]:
    values = {name: environment.get(name, "") for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise PreflightError("missing required environment: " + ", ".join(missing))
    return values


def _executable(path: str, name: str) -> Path:
    value = Path(path)
    try:
        metadata = value.stat(follow_symlinks=False)
    except OSError as error:
        raise PreflightError(f"{name} is unavailable") from error
    if (
        not value.is_absolute()
        or value.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or not os.access(value, os.X_OK)
    ):
        raise PreflightError(f"{name} must be an absolute executable regular file")
    return value


def _descriptor(materializer: Path) -> str:
    try:
        result = subprocess.run(
            [str(materializer), "--describe"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
            env={"LANG": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin"},
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError("materialize_range --describe failed") from error
    if (
        result.returncode != 0
        or len(result.stdout) > MAX_DESCRIPTOR_BYTES
        or len(result.stderr) > MAX_DESCRIPTOR_BYTES
        or result.stdout.count(b"\n") != 1
        or not result.stdout.endswith(b"\n")
    ):
        raise PreflightError("materialize_range --describe returned an invalid bounded response")
    try:
        producer = parse_producer(result.stdout[:-1])
    except ValueError as error:
        raise PreflightError("materialize_range descriptor is not the release contract") from error
    return producer.materialization_policy_sha256


def _publisher_contract(publisher: Path) -> None:
    try:
        result = subprocess.run(
            [str(publisher)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=10,
            check=False,
            env={"LANG": "C.UTF-8", "PATH": "/usr/local/bin:/usr/bin:/bin"},
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise PreflightError("replay-publish contract probe failed") from error
    if (
        result.returncode != 20
        or result.stdout
        or len(result.stderr) > 4096
        or not result.stderr.startswith(b"replay-publish: usage:")
    ):
        raise PreflightError("replay-publish is not the expected release binary")


def _mount_type(path: Path) -> str:
    resolved = str(path.resolve())
    selected = ("", "")
    try:
        lines = Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise PreflightError("cannot inspect REPLAY_DATA_ROOT mount durability") from error
    for line in lines:
        left, separator, right = line.partition(" - ")
        if not separator:
            continue
        fields = left.split()
        if len(fields) < 5:
            continue
        mountpoint = fields[4].replace("\\040", " ")
        if (resolved == mountpoint or resolved.startswith(mountpoint.rstrip("/") + "/")) and len(mountpoint) > len(selected[0]):
            selected = (mountpoint, right.split()[0])
    if not selected[1] or selected[1] in {"overlay", "tmpfs", "ramfs"}:
        raise PreflightError("REPLAY_DATA_ROOT is not on an accepted durable filesystem mount")
    return selected[1]


def _database(config, root: Path) -> None:
    if config.replay.database_path != root / "jobs.sqlite3":
        raise PreflightError("Event Universe and runner do not share REPLAY_DATA_ROOT/jobs.sqlite3")
    AuthStore(config.replay.database_path, config.replay.auth).initialize()
    ReplayJobStore(config.replay.database_path, config.replay.jobs).initialize()
    with closing(sqlite3.connect(f"file:{config.replay.database_path}?mode=ro", uri=True)) as connection:
        if connection.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise PreflightError("jobs.sqlite3 failed integrity_check")


def _redis(url: str, expected_maxmemory: int) -> dict[str, object]:
    try:
        import redis

        client = redis.Redis.from_url(
            url,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry=None,
            decode_responses=True,
        )
        info = client.info()
        memory = client.config_get("maxmemory")
        policy = client.config_get("maxmemory-policy")
        persistence = client.config_get("save") | client.config_get("appendonly")
    except Exception as error:
        raise PreflightError("Redis connectivity/configuration check failed") from error
    version = tuple(int(part) for part in str(info.get("redis_version", "0")).split(".")[:2])
    if version < (8, 2):
        raise PreflightError("Redis 8.2 or newer is required")
    if int(memory.get("maxmemory", "0")) != expected_maxmemory:
        raise PreflightError("Redis maxmemory disagrees with the configured exact value")
    if policy.get("maxmemory-policy") != "noeviction":
        raise PreflightError("Redis requires noeviction")
    if persistence.get("save") not in {"", None} or persistence.get("appendonly") != "no":
        raise PreflightError("Redis RDB and AOF persistence must be disabled")
    if int(info.get("evicted_keys", 0)) != 0:
        raise PreflightError("Redis reports evictions")
    return {"version": str(info["redis_version"]), "maxmemory": int(memory["maxmemory"])}


def _request(url: str, timeout: int = 10) -> int:
    request = urllib.request.Request(url, headers={"User-Agent": "prediction-indexer-replay-preflight/1"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read(1024 * 1024 + 1)
            return response.status
    except urllib.error.HTTPError as error:
        error.read(1024 * 1024 + 1)
        return error.code
    except (OSError, urllib.error.URLError) as error:
        raise PreflightError("HTTP/TLS connectivity check failed") from error


def _http_checks(internal_url: str, public_host: str) -> None:
    if _request(internal_url.rstrip("/") + "/healthz") != 200:
        raise PreflightError("private Event Universe health check failed")
    public = f"https://{public_host}"
    if _request(public + "/healthz") != 200:
        raise PreflightError("public Caddy TLS/proxy health check failed")


def run_preflight(config_path: Path, environ=None) -> dict[str, object]:
    environment = os.environ if environ is None else environ
    required = _required(
        environment,
        "REPLAY_DATA_ROOT",
        "REPLAY_MATERIALIZER",
        "REPLAY_PUBLISHER",
        "REPLAY_IMAGE_REVISION",
        "REPLAY_IMAGE_DIGEST",
        "REPLAY_RUNNER_IMAGE",
        "REPLAY_VOLUME_ID",
        "REDIS_URL",
        "REPLAY_PUBLIC_HOST",
        "REPLAY_INTERNAL_URL",
        "REPLAY_ARCHIVE_PROBE_KEY",
    )
    if _REVISION.fullmatch(required["REPLAY_IMAGE_REVISION"]) is None:
        raise PreflightError("REPLAY_IMAGE_REVISION must be a full immutable Git SHA")
    if _SHA256.fullmatch(required["REPLAY_IMAGE_DIGEST"]) is None:
        raise PreflightError("REPLAY_IMAGE_DIGEST must be an immutable sha256 digest")
    if not required["REPLAY_RUNNER_IMAGE"].endswith(
        "@" + required["REPLAY_IMAGE_DIGEST"]
    ):
        raise PreflightError("REPLAY_RUNNER_IMAGE must end with @REPLAY_IMAGE_DIGEST")
    if _PUBLIC_HOST.fullmatch(required["REPLAY_PUBLIC_HOST"]) is None:
        raise PreflightError("REPLAY_PUBLIC_HOST must be a DNS hostname")
    revision_file = Path("/etc/prediction-indexer-replay-image-revision")
    if not revision_file.is_file() or revision_file.is_symlink():
        raise PreflightError("runner image revision file is missing")
    if revision_file.read_text(encoding="ascii").strip() != required["REPLAY_IMAGE_REVISION"]:
        raise PreflightError("REPLAY_IMAGE_REVISION disagrees with the image build identity")
    root = Path(required["REPLAY_DATA_ROOT"])
    check_private_root(root)
    filesystem = os.statvfs(root)
    capacity = _positive_environment(environment, "REPLAY_REQUIRED_CAPACITY_BYTES")
    inodes = _positive_environment(environment, "REPLAY_REQUIRED_INODES")
    quota = _positive_environment(environment, "REPLAY_QUOTA_BYTES")
    check_capacity(filesystem, required_bytes=capacity, required_inodes=inodes, quota_bytes=quota)
    mount_type = _mount_type(root)
    materializer = _executable(required["REPLAY_MATERIALIZER"], "REPLAY_MATERIALIZER")
    publisher = _executable(required["REPLAY_PUBLISHER"], "REPLAY_PUBLISHER")
    runner_config = parse_runner_config(Path(config_path).read_bytes())
    if runner_config.universe_base_url.rstrip("/") != required[
        "REPLAY_INTERNAL_URL"
    ].rstrip("/"):
        raise PreflightError("runner config and REPLAY_INTERNAL_URL disagree")
    policy = _descriptor(materializer)
    _publisher_contract(publisher)
    universe_config = load_config(Path(environment.get("EVENT_UNIVERSE_CONFIG", "/etc/prediction-indexer/event_universe.json")))
    _database(universe_config, root)
    try:
        store = build_store((root,), environ=environment)
    except SystemExit as error:
        raise PreflightError("archive configuration or independence check failed") from error
    if not store.durability.independent:
        raise PreflightError("Replay production ObjectStore must be independently durable")
    try:
        archive_probe = store.head(required["REPLAY_ARCHIVE_PROBE_KEY"])
    except Exception as error:
        raise PreflightError("archive receipt/IAM probe failed") from error
    if (
        archive_probe is None
        or not archive_probe.provider_checksum
        or not archive_probe.provider_checksum_algorithm
    ):
        raise PreflightError("archive receipt/IAM probe is absent or unverifiable")
    redis_maxmemory = _positive_environment(environment, "REPLAY_REDIS_MAXMEMORY_BYTES")
    redis_report = _redis(required["REDIS_URL"], redis_maxmemory)
    _http_checks(required["REPLAY_INTERNAL_URL"], required["REPLAY_PUBLIC_HOST"])
    return {
        "status": "ready",
        "image_revision": required["REPLAY_IMAGE_REVISION"],
        "image_digest": required["REPLAY_IMAGE_DIGEST"],
        "volume_id": required["REPLAY_VOLUME_ID"],
        "mount_type": mount_type,
        "materialization_policy_sha256": policy,
        "runner_config_version": RUNNER_CONFIG_VERSION,
        "archive_provider": store.provider,
        "archive_store_id": store.store_id,
        "redis": redis_report,
        "rate_limit_enforcement": "event_universe_process",
    }
