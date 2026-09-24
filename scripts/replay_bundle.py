#!/usr/bin/env python3
"""Stage-B narrow Replay runner over materialization, Risk transport, and supervisor.

This baseline has no preparation, strategy SDK, bundle_coverage, or completed-result
registry. The request therefore pins immutable book plans without scales, invokes
one existing strategy factory, and returns validated supervisor completion only.
"""

import hashlib
import json
import math
import os
import re
import subprocess
import sys
import uuid
from pathlib import Path

from replay import supervisor
from replay.streams.protocol import ProtocolError, decode, obj, require, uint


class StepError(Exception):
    def __init__(self, step, retryable=False):
        self.step = step
        self.retryable = retryable


def _canonical(value):
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode()


def _write_bytes(path, data):
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.open")
    try:
        with temporary.open("xb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        supervisor.fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _absolute(value):
    require(type(value) is str and Path(value).is_absolute())
    return value


def validate_request(value):
    obj(value, "version run_id interval capture plans strategy runtime")
    require(value["version"] == 1)
    require(
        type(value["run_id"]) is str
        and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value["run_id"])
    )
    interval = obj(value["interval"], "start_ns end_ns lower_bound")
    start, end = uint(interval["start_ns"]), uint(interval["end_ns"])
    require(start < end)
    require(
        interval["lower_bound"]
        in ("clip", "expand_to_window_start", "require_window_boundary")
    )
    capture = obj(value["capture"], "canonical_root derivative_root")
    _absolute(capture["canonical_root"])
    _absolute(capture["derivative_root"])
    require(type(value["plans"]) is list and bool(value["plans"]))
    seen = set()
    for plan in value["plans"]:
        obj(plan, "instrument orientation lane venue")
        require(
            all(type(plan[name]) is str and bool(plan[name]) for name in plan)
            and plan["orientation"] in ("outcome", "complement")
            and plan["instrument"].startswith(plan["venue"] + ":")
        )
        key = plan["instrument"], plan["orientation"]
        require(key not in seen, "duplicate plan")
        seen.add(key)
    strategy = obj(value["strategy"], "group factory revision config")
    require(
        type(strategy["group"]) is str
        and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", strategy["group"])
        and strategy["group"]
        not in {
            ".",
            "..",
            "publisher",
            "ready",
            "publisher.json",
            "result.json",
            "interrupted.json",
        }
        and type(strategy["factory"]) is str
        and re.fullmatch(
            r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*",
            strategy["factory"],
        )
        and type(strategy["revision"]) is str
        and bool(strategy["revision"])
        and type(strategy["config"]) is dict
    )
    runtime = obj(
        value["runtime"],
        "materializer publisher python scope max_entry_bytes max_queue_bytes command_timeout_ms limits",
    )
    for name in ("materializer", "publisher", "python"):
        _absolute(runtime[name])
    require(
        type(runtime["scope"]) is str
        and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", runtime["scope"])
    )
    for name in ("max_entry_bytes", "max_queue_bytes"):
        uint(runtime[name])
    entry_bytes = int(runtime["max_entry_bytes"])
    queue_bytes = int(runtime["max_queue_bytes"])
    require(
        type(runtime["command_timeout_ms"]) is int
        and 2 <= runtime["command_timeout_ms"] <= 60_000
        and 0 < entry_bytes <= queue_bytes <= 1_000_000_000
    )
    limits = obj(
        runtime["limits"],
        "attempts no_progress progress_margin stall_seconds attempt_seconds run_seconds poll_seconds stop_seconds",
    )
    for name, limit in limits.items():
        require(type(limit) in (int, float) and math.isfinite(limit) and limit > 0)
        if name in ("attempts", "no_progress", "progress_margin"):
            require(type(limit) is int)
    require(
        limits["poll_seconds"]
        < limits["stall_seconds"]
        <= limits["attempt_seconds"]
        <= limits["run_seconds"]
    )
    return value


def _materialize(request):
    interval, capture, runtime = (
        request["interval"],
        request["capture"],
        request["runtime"],
    )
    body = {
        "version": 1,
        "canonical_root": capture["canonical_root"],
        "output_root": capture["derivative_root"],
        "start_ns": int(interval["start_ns"]),
        "end_ns": int(interval["end_ns"]),
    }
    try:
        process = subprocess.run(
            [runtime["materializer"]],
            input=_canonical(body),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=runtime["limits"]["run_seconds"],
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise StepError("materialize", retryable=True) from error
    if process.returncode != 0:
        raise StepError("materialize")
    try:
        result = decode(process.stdout, 4 * 1024 * 1024)
        obj(result, "version normalizer derivatives")
        require(result["version"] == 1)
        supervisor._normalizer_descriptor(result["normalizer"])
        require(type(result["derivatives"]) is list)
        require(0 < len(result["derivatives"]) <= 4096, "pin count")
        pins = []
        previous_end = None
        for item in result["derivatives"]:
            obj(
                item,
                "window_start_ns window_end_ns derivative_address receipt_sha256",
            )
            start, end = item["window_start_ns"], item["window_end_ns"]
            require(type(start) is int and type(end) is int and start < end)
            require(previous_end is None or start == previous_end, "non-adjacent pins")
            previous_end = end
            for name in ("derivative_address", "receipt_sha256"):
                require(
                    type(item[name]) is str
                    and re.fullmatch(r"[0-9a-f]{64}", item[name])
                )
            pins.append(
                {
                    "directory": str(
                        Path(capture["derivative_root"])
                        / f"window={start}"
                        / item["derivative_address"]
                    ),
                    "derivative_address": item["derivative_address"],
                    "receipt_sha256": item["receipt_sha256"],
                }
            )
        return result["normalizer"], pins
    except (ProtocolError, KeyError, TypeError, ValueError) as error:
        raise StepError("materialize") from error


def _supervisor_config(request, normalizer, pins):
    descriptor = supervisor._normalizer_descriptor(normalizer)
    plans = []
    for plan in request["plans"]:
        require(plan["venue"] in descriptor["scales"], "plan venue missing")
        price, quantity = descriptor["scales"][plan["venue"]]
        plans.append(
            {
                **plan,
                "price_scale": str(price),
                "quantity_scale": str(quantity),
            }
        )
    runtime, strategy = request["runtime"], request["strategy"]
    config = {
        "version": 1,
        "publisher": runtime["publisher"],
        "python": runtime["python"],
        "transport": {
            "run_id": request["run_id"],
            "scope": runtime["scope"],
            "normalizer": normalizer,
            "inputs": pins,
            "start_ns": request["interval"]["start_ns"],
            "end_ns": request["interval"]["end_ns"],
            "lower_bound": request["interval"]["lower_bound"],
            "plans": plans,
            "groups": [strategy["group"]],
            "command_timeout_ms": runtime["command_timeout_ms"],
            "max_entry_bytes": int(runtime["max_entry_bytes"]),
            "max_queue_bytes": int(runtime["max_queue_bytes"]),
        },
        "strategies": {
            strategy["group"]: {
                "factory": strategy["factory"],
                "revision": strategy["revision"],
                "config": strategy["config"],
            }
        },
        "limits": runtime["limits"],
    }
    return supervisor.validate(config)


def execute(request, workdir, redis_url):
    request = validate_request(request)
    workdir = Path(workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    supervisor.fsync_directory(workdir.parent)
    request_bytes = _canonical(request)
    request_path = workdir / "request.json"
    if request_path.exists():
        require(request_path.read_bytes() == request_bytes, "immutable request")
    else:
        _write_bytes(request_path, request_bytes)
    request_sha256 = hashlib.sha256(request_bytes).hexdigest()

    normalizer, pins = _materialize(request)
    config = _supervisor_config(request, normalizer, pins)
    try:
        completion = supervisor.run(config, workdir / "run", redis_url)
        completion = supervisor.read_success(workdir / "run")
    except supervisor.Interrupted as error:
        raise StepError("supervisor", retryable=True) from error
    except supervisor.AttemptFailure as error:
        raise StepError("supervisor", retryable=not error.fatal) from error
    except (OSError, ProtocolError) as error:
        raise StepError("supervisor", retryable=isinstance(error, OSError)) from error
    result = {
        "version": 1,
        "request_sha256": request_sha256,
        "normalizer": normalizer,
        "pins": pins,
        "completion": completion,
    }
    _write_bytes(workdir / "result.json", _canonical(result))
    return result


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2:
        print("usage: replay_bundle.py REQUEST.json WORKDIR", file=sys.stderr)
        return 20
    try:
        redis_url = os.environ["REDIS_URL"]
        with Path(argv[0]).open("rb") as file:
            request = decode(file.read(1_048_577), 1_048_576)
        result = execute(request, argv[1], redis_url)
        sys.stdout.buffer.write(_canonical(result))
    except StepError as error:
        print(f"replay_bundle: {error.step} failed", file=sys.stderr)
        return 21 if error.retryable else 20
    except (KeyError, OSError, ProtocolError, TypeError, ValueError):
        print("replay_bundle: request/preflight failed", file=sys.stderr)
        return 20
    return 0


if __name__ == "__main__":
    sys.exit(main())
