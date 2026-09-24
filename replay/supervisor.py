"""Whole-attempt subprocess runner. The checkpoint is a retry budget, not a cursor."""

import argparse
import ctypes
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

from replay.streams.protocol import Decoder, ProtocolError, decode, obj, require


def fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_json_durable(path, value):
    # Same discipline as archive.common.durable, without importing archive's
    # package initializer (which imports capture-only, uninstalled splices).
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.open")
    try:
        with temporary.open("xb") as f:
            f.write(
                (json.dumps(value, sort_keys=True, allow_nan=False) + "\n").encode()
            )
            f.flush()
            os.fsync(f.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def read(path):
    with Path(path).open("rb") as f:
        return decode(f.read(1_048_577), 1_048_576)


def identity(config):
    return hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _normalizer_descriptor(value):
    """Validate identity V1 and reproduce replay-normalizers' exact hashes."""
    obj(value, "identity_version venues")
    require(type(value["identity_version"]) is int and value["identity_version"] == 1)
    require(type(value["venues"]) is list)
    expected = {
        "kalshi": (2, ("price_scale", "quantity_scale")),
        "limitless": (1, ("price_scale", "quantity_scale")),
        "polymarket": (
            1,
            ("accept_additive_fields", "price_scale", "quantity_scale"),
        ),
    }
    require(
        [entry.get("venue") if type(entry) is dict else None for entry in value["venues"]]
        == list(expected),
        "normalizer venues",
    )
    normalized_venues = []
    scales = {}
    for entry in value["venues"]:
        obj(entry, "venue bundle_id parser_version config")
        venue = entry["venue"]
        require(type(entry["bundle_id"]) is str and bool(entry["bundle_id"]))
        require(type(entry["parser_version"]) is int and entry["parser_version"] > 0)
        config = obj(entry["config"], "schema_version variables")
        schema, variables = expected[venue]
        require(type(config["schema_version"]) is int and config["schema_version"] == schema)
        require(
            type(config["variables"]) is dict
            and set(config["variables"]) == set(variables)
        )
        normalized_variables = {}
        for name in variables:
            variable = obj(config["variables"][name], "type value")
            if name == "accept_additive_fields":
                require(variable["type"] == "boolean" and type(variable["value"]) is bool)
            else:
                require(
                    variable["type"] == "unsigned"
                    and type(variable["value"]) is int
                    and 0 <= variable["value"] <= 18,
                    "normalizer scale",
                )
            normalized_variables[name] = {
                "type": variable["type"],
                "value": variable["value"],
            }
        scales[venue] = (
            normalized_variables["price_scale"]["value"],
            normalized_variables["quantity_scale"]["value"],
        )
        normalized_venues.append(
            {
                "venue": venue,
                "bundle_id": entry["bundle_id"],
                "parser_version": entry["parser_version"],
                "config": {
                    "schema_version": config["schema_version"],
                    "variables": normalized_variables,
                },
            }
        )
    canonical = {"identity_version": 1, "venues": normalized_venues}
    encode = lambda item: json.dumps(
        item, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode()
    bundle = [
        {k: entry[k] for k in ("venue", "bundle_id", "parser_version")}
        for entry in normalized_venues
    ]
    return {
        "bundle_sha256": hashlib.sha256(
            b"prediction-indexer/replay-normalizers/bundle/v1\0" + encode(bundle)
        ).hexdigest(),
        "config_sha256": hashlib.sha256(
            b"prediction-indexer/replay-normalizers/config/v1\0" + encode(canonical)
        ).hexdigest(),
        "scales": scales,
    }


def _metadata(path):
    with Path(path).open("rb") as file:
        data = file.read(1_048_577)
    require(len(data) <= 1_048_576, "metadata limit")
    return data, decode(data, 1_048_576)


def _validate_input_binding(pin, descriptor):
    directory = Path(pin["directory"])
    receipt_bytes, receipt = _metadata(directory / "receipt.json")
    require(
        hashlib.sha256(receipt_bytes).hexdigest() == pin["receipt_sha256"],
        "receipt does not match pin",
    )
    obj(
        receipt,
        "receipt_version derivative_address source_receipt_sha256 normalized_schema_version materializer_version normalizer_bundle_sha256 normalizer_config_sha256 policy_sha256 manifest events rejects sources",
    )
    require(
        receipt["receipt_version"] == 2
        and receipt["normalized_schema_version"] == 3
        and receipt["materializer_version"] == 2,
        "pinned derivative is not source-evidence profile 2",
    )
    require(receipt["derivative_address"] == pin["derivative_address"])
    manifest_identity = obj(receipt["manifest"], "file sha256 byte_length")
    require(manifest_identity["file"] == "manifest.json")
    manifest_bytes, manifest = _metadata(directory / "manifest.json")
    obj(
        manifest,
        "manifest_version derivative_address source_receipt requested_start_ns requested_end_ns effective_start_ns effective_end_ns normalized_schema_version event_serialization_version reject_serialization_version materializer_version normalizer_bundle_sha256 normalizer_config_sha256 policy counts events rejects sources",
    )
    require(
        manifest["manifest_version"] == 2
        and manifest["normalized_schema_version"] == 3
        and manifest["materializer_version"] == 2,
        "pinned derivative is not source-evidence profile 2",
    )
    require(
        manifest["derivative_address"] == pin["derivative_address"]
        and manifest_identity["sha256"] == hashlib.sha256(manifest_bytes).hexdigest()
        and manifest_identity["byte_length"] == len(manifest_bytes),
        "manifest binding",
    )
    for document in (receipt, manifest):
        require(
            document["normalizer_bundle_sha256"] == descriptor["bundle_sha256"],
            "normalizer bundle mismatch",
        )
        require(
            document["normalizer_config_sha256"] == descriptor["config_sha256"],
            "normalizer config mismatch",
        )


def initial(config):
    t = config["transport"]
    return {
        **{k: t[k] for k in ("start_ns", "end_ns", "lower_bound", "plans", "groups")},
        "pins": [
            {k: p[k] for k in ("derivative_address", "receipt_sha256")}
            for p in t["inputs"]
        ],
        "max_entry_bytes": str(t["max_entry_bytes"]),
        "max_queue_bytes": str(t["max_queue_bytes"]),
    }


def validate(config):
    obj(config, "version publisher python transport strategies limits")
    require(config["version"] == 1)
    for name in ("publisher", "python"):
        require(type(config[name]) is str and Path(config[name]).is_absolute())
    t = obj(
        config["transport"],
        "run_id scope normalizer inputs start_ns end_ns lower_bound plans groups command_timeout_ms max_entry_bytes max_queue_bytes",
    )
    for s in [t["run_id"], t["scope"], *t["groups"]]:
        require(
            type(s) is str and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", s) is not None
        )
    require(0 < len(t["groups"]) <= 128 and len(set(t["groups"])) == len(t["groups"]))
    require(
        not set(t["groups"])
        & {
            ".",
            "..",
            "publisher",
            "ready",
            "publisher.json",
            "result.json",
            "interrupted.json",
        },
        "reserved participant name",
    )
    require(type(t["inputs"]) is list and 0 < len(t["inputs"]) <= 4096)
    descriptor = _normalizer_descriptor(t["normalizer"])
    for p in t["inputs"]:
        obj(p, "directory derivative_address receipt_sha256")
        require(type(p["directory"]) is str and Path(p["directory"]).is_absolute())
        require(
            all(
                type(p[name]) is str
                and re.fullmatch(r"[0-9a-f]{64}", p[name]) is not None
                for name in ("derivative_address", "receipt_sha256")
            )
        )
        _validate_input_binding(p, descriptor)
    require(type(t["plans"]) is list and bool(t["plans"]))
    for plan in t["plans"]:
        obj(plan, "instrument orientation lane venue price_scale quantity_scale")
        require(plan["venue"] in descriptor["scales"], "plan venue missing from normalizer")
        price_scale, quantity_scale = descriptor["scales"][plan["venue"]]
        require(
            plan["price_scale"] == str(price_scale)
            and plan["quantity_scale"] == str(quantity_scale),
            "plan scales disagree with normalizer",
        )
    for k in ("command_timeout_ms", "max_entry_bytes", "max_queue_bytes"):
        require(type(t[k]) is int and t[k] > 0)
    require(
        2 <= t["command_timeout_ms"] <= 60_000
        and t["max_entry_bytes"] <= t["max_queue_bytes"] <= 1_000_000_000
    )
    require(
        type(config["strategies"]) is dict
        and set(config["strategies"]) == set(t["groups"])
    )
    for s in config["strategies"].values():
        obj(s, "factory revision config")
        require(
            type(s["factory"]) is str
            and re.fullmatch(
                r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", s["factory"]
            )
            is not None
        )
        require(type(s["revision"]) is str and bool(s["revision"]))
    limits = obj(
        config["limits"],
        "attempts no_progress progress_margin stall_seconds attempt_seconds run_seconds poll_seconds stop_seconds",
    )
    for k, v in limits.items():
        require(type(v) in (int, float) and math.isfinite(v) and v > 0)
        if k in ("attempts", "no_progress", "progress_margin"):
            require(type(v) is int)
    require(
        limits["poll_seconds"]
        < limits["stall_seconds"]
        <= limits["attempt_seconds"]
        <= limits["run_seconds"]
    )
    expected = initial(config)
    Decoder(t["run_id"], "validation", expected, t["max_entry_bytes"]).apply(
        json.dumps(
            {
                "version": "1",
                "run_id": t["run_id"],
                "attempt_id": "validation",
                "sequence": "0",
                "kind": "initial",
                "body": expected,
            }
        ).encode()
    )
    return config


def client(url, timeout):
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    require(not urlsplit(url).query, "Redis URL options forbidden")
    return redis.Redis.from_url(
        url,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
        retry=Retry(NoBackoff(), 0),
        retry_on_error=[],
        decode_responses=True,
    )


def keys(config, attempt):
    t = config["transport"]
    return [
        f"replay:{t['scope']}:{t['run_id']}:{attempt}:{s}" for s in ("stream", "state")
    ]


def failed(state, progress, limits, fatal=False):
    """Compare with the historical high water mark, never the last attempt."""
    state["stagnant"] = (
        0
        if progress >= state["best"] + limits["progress_margin"]
        else state["stagnant"] + 1
    )
    state["best"] = max(state["best"], progress)
    state["active"] = None
    state["fatal"] = fatal


def allowed(state, limits):
    return (
        not state["fatal"]
        and state["attempts"] < limits["attempts"]
        and state["stagnant"] < limits["no_progress"]
        and time.time() < state["started"] + limits["run_seconds"]
    )


def read_state(root, digest):
    s = obj(
        read(root / "state.json"),
        "version identity attempts best stagnant started active fatal",
    )
    require(s["version"] == 1 and s["identity"] == digest, "checkpoint identity")
    for k in ("attempts", "stagnant"):
        require(type(s[k]) is int and s[k] >= 0)
    require(type(s["best"]) is int and s["best"] >= -1 and type(s["fatal"]) is bool)
    require(type(s["started"]) in (int, float) and math.isfinite(s["started"]))
    if s["active"] is not None:
        a = obj(s["active"], "id progress")
        require(
            type(a["id"]) is str and re.fullmatch(r"[0-9a-f]{32}", a["id"]) is not None
        )
        require(type(a["progress"]) is int and a["progress"] >= -1)
    return s


def attestation(path, digest, attempt, group):
    value = obj(read(path), "version identity attempt group terminal")
    require(
        value
        == {
            "version": 1,
            "identity": digest,
            "attempt": attempt,
            "group": group,
            "terminal": value["terminal"],
        },
        "participant identity",
    )
    require(
        type(value["terminal"]) is int and value["terminal"] >= 2, "terminal sequence"
    )
    return value


def read_result(path, digest, attempt, groups):
    value = obj(
        read(path),
        "version identity attempt outcome fatal progress terminal participants",
    )
    require(type(value["version"]) is int and value["version"] == 1)
    require(value["identity"] == digest and value["attempt"] == attempt)
    require(
        value["outcome"]
        in (
            "success",
            "participant_failure",
            "invariant",
            "deadline",
            "transport_or_resource",
            "interrupted",
            "poisoned",
            "missing_ready",
        )
    )
    require(type(value["fatal"]) is bool)
    require(type(value["progress"]) is int and value["progress"] >= -1)
    require(
        value["terminal"] is None
        or (type(value["terminal"]) is int and value["terminal"] >= 2)
    )
    require(
        type(value["participants"]) is dict
        and set(value["participants"]) <= {"publisher", *groups}
    )
    require(all(type(code) is int for code in value["participants"].values()))
    return value


def read_success(root):
    """Independent closed reader. Attests completion, NOT episode schema/content."""
    root = Path(root)
    config = validate(read(root / "run.json"))
    digest = identity(config)
    receipt = obj(
        read(root / "SUCCESS.json"), "version identity attempt terminal outputs"
    )
    require(receipt["version"] == 1 and receipt["identity"] == digest)
    attempt = receipt["attempt"]
    require(type(attempt) is str and re.fullmatch(r"[0-9a-f]{32}", attempt) is not None)
    require(type(receipt["terminal"]) is int and receipt["terminal"] >= 2)
    expected = {g: f"{attempt}/{g}/output" for g in config["transport"]["groups"]}
    require(receipt["outputs"] == expected)
    for g, location in expected.items():
        require((root / location).is_dir() and not (root / location).is_symlink())
        a = attestation(root / attempt / g / "complete.json", digest, attempt, g)
        require(a["terminal"] == receipt["terminal"])
    evidence = read_result(root / attempt / "result.json", digest, attempt, expected)
    require(
        evidence
        == {
            "version": 1,
            "identity": digest,
            "attempt": attempt,
            "outcome": "success",
            "fatal": False,
            "progress": receipt["terminal"],
            "terminal": receipt["terminal"],
            "participants": {g: 0 for g in ["publisher", *expected]},
        },
        "attempt completion evidence",
    )
    fsync_directory(root)
    return receipt


def sync_outputs(path):
    """No hashes or episode interpretation; durably close the strategy-owned tree."""
    for directory, dirs, files in os.walk(path, followlinks=False):
        for name in dirs + files:
            require(not (Path(directory) / name).is_symlink(), "output symlink")
        for name in files:
            p = Path(directory) / name
            require(p.is_file(), "output must be regular files")
            with p.open("rb") as f:
                os.fsync(f.fileno())
        fsync_directory(Path(directory))


class Interrupted(Exception):
    pass


class AttemptFailure(Exception):
    def __init__(self, outcome, fatal=False):
        self.outcome, self.fatal = outcome, fatal


def stop(children, timeout):
    # Each participant is a new process group. Never signal a recovered PID.
    for p in children.values():
        try:
            os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    until = time.monotonic() + timeout
    for p in children.values():
        try:
            p.wait(timeout=max(0, until - time.monotonic()))
        except subprocess.TimeoutExpired:
            pass
    for p in children.values():
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        p.wait()


def attempt(root, config, state, redis, lock_fd, url):
    import redis as redis_module

    digest, a = identity(config), state["active"]
    directory = root / a["id"]
    directory.mkdir()
    fsync_directory(root)
    t = {**config["transport"], "attempt_id": a["id"]}
    write_json_durable(directory / "publisher.json", t)
    children = {}
    limits = config["limits"]
    started = last_progress = time.monotonic()
    terminal = None
    outcome, fatal, interrupted = "interrupted", False, False
    parent_pid = os.getpid()
    libc = ctypes.CDLL(None, use_errno=True)

    def parent_death():
        # Linux, single-threaded runner only. Arm before exec and close the
        # parent-death race; no recovered PID is ever signalled on restart.
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            os._exit(21)
        if os.getppid() != parent_pid:
            os._exit(21)

    def launch(name, command):
        # Untrusted stdout is discarded, not an unbounded log or retry protocol.
        children[name] = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            pass_fds=(lock_fd,),
            preexec_fn=parent_death,
            env={**os.environ, "REDIS_URL": url},
        )

    try:
        launch(
            "publisher",
            [
                config["publisher"],
                str(directory / "publisher.json"),
                str(directory / "ready"),
            ],
        )
        joined = False
        while True:
            codes = {name: p.poll() for name, p in children.items()}
            if any(code is not None and code != 0 for code in codes.values()):
                raise AttemptFailure(
                    "participant_failure",
                    any(
                        code is not None and code > 0 and code != 21
                        for code in codes.values()
                    ),
                )
            if not joined and (directory / "ready").exists():
                for group in t["groups"]:
                    participant = directory / group
                    participant.mkdir()
                    (participant / "output").mkdir()
                    launch(
                        group,
                        [
                            config["python"],
                            "-m",
                            "replay.strategy_adapter",
                            str(root),
                            a["id"],
                            group,
                        ],
                    )
                joined = True
            if codes.get("publisher") == 0 and not joined:
                raise AttemptFailure("missing_ready", True)
            if joined:
                p = redis.hgetall(keys(config, a["id"])[1])
                require(
                    p and all(f"done:{g}" in p for g in t["groups"]), "missing progress"
                )
                require(
                    all(
                        -1 <= int(p[f"done:{g}"]) <= int(p["published"])
                        for g in t["groups"]
                    ),
                    "invalid completion",
                )
                if codes.get("publisher") == 0:
                    require(
                        p["terminal"] and p["published"] == p["terminal"],
                        "publisher missing terminal",
                    )
                progress = min(int(p[f"done:{g}"]) for g in t["groups"])
                require(progress >= a["progress"], "progress regressed")
                if progress > a["progress"]:
                    a["progress"] = progress
                    last_progress = time.monotonic()
                    write_json_durable(root / "state.json", state)
                if p.get("poisoned") != "0":
                    # The failing child owns the error class. Wait boundedly for
                    # its exit, rather than treating shared poison as retry proof.
                    outcome = "poisoned"
                elif len(codes) == len(t["groups"]) + 1 and all(
                    c == 0 for c in codes.values()
                ):
                    require(
                        p["terminal"] and p["published"] == p["terminal"],
                        "missing terminal",
                    )
                    terminal = int(p["terminal"])
                    require(progress == terminal, "incomplete groups")
                    for group in t["groups"]:
                        require(
                            attestation(
                                directory / group / "complete.json",
                                digest,
                                a["id"],
                                group,
                            )["terminal"]
                            == terminal
                        )
                    outcome = "success"
                    break
            now = time.monotonic()
            if (
                now - last_progress >= limits["stall_seconds"]
                or now - started >= limits["attempt_seconds"]
                or time.time() >= state["started"] + limits["run_seconds"]
            ):
                raise AttemptFailure("deadline", outcome == "poisoned")
            time.sleep(limits["poll_seconds"])
    except AttemptFailure as e:
        outcome, fatal = e.outcome, e.fatal
    except (ProtocolError, ValueError, KeyError, TypeError):
        outcome, fatal = "invariant", True
    except (redis_module.RedisError, OSError):
        outcome = "transport_or_resource"
    except Interrupted:
        interrupted = True
    finally:
        stop(children, limits["stop_seconds"])
    codes = {name: p.returncode for name, p in children.items()}
    # A late fatal exit must never be hidden by a transport error or Redis ACKs.
    fatal = fatal or any(c > 0 and c != 21 for c in codes.values())
    if fatal and outcome == "success":
        outcome = "participant_failure"
    result = {
        "version": 1,
        "identity": digest,
        "attempt": a["id"],
        "outcome": outcome,
        "fatal": fatal,
        "progress": a["progress"],
        "terminal": terminal,
        "participants": codes,
    }
    write_json_durable(directory / "result.json", result)
    # Cleanup cannot turn failure into success; errors leave only owned keys.
    try:
        # Setup can reject existing keys. Only the post-setup handshake proves
        # ownership; failed/ambiguous setup deliberately leaves its keys alone.
        if (directory / "ready").exists():
            redis.delete(*keys(config, a["id"]))
    except redis_module.RedisError:
        pass
    return result, fatal, interrupted


def run(config, root, url):
    config = validate(config)
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    fsync_directory(root.parent)
    digest = identity(config)
    with (root / ".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        # The descriptor is inherited by children. After SIGKILL, a new runner
        # cannot touch the old attempt until its participants have all exited.
        if (root / "run.json").exists():
            require(read(root / "run.json") == config, "immutable run configuration")
        else:
            require(
                not (root / "state.json").exists()
                and not (root / "SUCCESS.json").exists()
            )
            write_json_durable(root / "run.json", config)
        if (root / "SUCCESS.json").exists():
            return read_success(root)
        if (root / "state.json").exists():
            state = read_state(root, digest)
        else:
            require(not any(p.is_dir() for p in root.iterdir()), "missing checkpoint")
            state = {
                "version": 1,
                "identity": digest,
                "attempts": 0,
                "best": -1,
                "stagnant": 0,
                "started": time.time(),
                "active": None,
                "fatal": False,
            }
        limits = config["limits"]
        redis = client(url, config["transport"]["command_timeout_ms"] / 1000)
        old_handlers = {}

        def interrupt(signum, frame):
            # Repeated signals cannot interrupt child teardown.
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, signal.SIG_IGN)
            raise Interrupted()

        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                old_handlers[sig] = signal.signal(sig, interrupt)
            if state["active"] is not None:
                # No resume and no inferred success, even if terminal was ACKed.
                old = state["active"]
                directory = root / old["id"]
                directory.mkdir(exist_ok=True)
                write_json_durable(
                    directory / "interrupted.json",
                    {"version": 1, "identity": digest, "attempt": old["id"]},
                )
                # A persisted fatal result stays fatal across a crash.
                prior = (
                    read_result(
                        directory / "result.json",
                        digest,
                        old["id"],
                        config["transport"]["groups"],
                    )
                    if (directory / "result.json").exists()
                    else None
                )
                fatal = prior is not None and prior["fatal"]
                failed(state, old["progress"], limits, fatal)
                write_json_durable(root / "state.json", state)
            while allowed(state, limits):
                state["attempts"] += 1
                state["active"] = {"id": uuid.uuid4().hex, "progress": -1}
                # Budget and namespace become durable BEFORE any process launch.
                write_json_durable(root / "state.json", state)
                result, fatal, interrupted = attempt(
                    root, config, state, redis, lock.fileno(), url
                )
                if result["outcome"] == "success":
                    directory = root / result["attempt"]
                    try:
                        sync_outputs(directory)
                    except ProtocolError:
                        failed(state, result["progress"], limits, True)
                        write_json_durable(root / "state.json", state)
                        raise
                    receipt = {
                        "version": 1,
                        "identity": digest,
                        "attempt": result["attempt"],
                        "terminal": result["terminal"],
                        "outputs": {
                            g: f"{result['attempt']}/{g}/output"
                            for g in config["transport"]["groups"]
                        },
                    }
                    write_json_durable(root / "SUCCESS.json", receipt)
                    return read_success(root)
                failed(state, result["progress"], limits, fatal)
                write_json_durable(root / "state.json", state)
                if interrupted:
                    raise Interrupted()
            raise AttemptFailure(
                "budget_exhausted" if not state["fatal"] else "fatal", state["fatal"]
            )
        finally:
            redis.close()
            for sig, handler in old_handlers.items():
                signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config")
    parser.add_argument("run_directory")
    args = parser.parse_args()
    try:
        run(read(args.config), args.run_directory, os.environ["REDIS_URL"])
    except Interrupted:
        return 21
    except AttemptFailure as e:
        print(e.outcome, file=sys.stderr)
        return 20 if e.fatal else 21
    except (ProtocolError, ValueError, KeyError, TypeError):
        print("invalid supervisor input/state", file=sys.stderr)
        return 20
    except OSError:
        print("supervisor resource/lock failure", file=sys.stderr)
        return 21
    return 0


if __name__ == "__main__":
    sys.exit(main())
