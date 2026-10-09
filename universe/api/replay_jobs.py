"""SIWE auth, allowlist and Replay job endpoints (retiring with the jobs UI).

Each handler is one former branch of the API's if-chain, unchanged.
"""

from __future__ import annotations

import time
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

from replay.jobs.contracts import (
    STRATEGY_CONFIG_SCHEMAS,
    RunnerConfig,
    parse_request,
    request_sha256,
)
from universe.api.framing import (
    _encode_cursor,
    _exact_body,
    _integer,
    _only,
    _optional,
    _path_value,
    _replay_job_events_cursor,
    _replay_jobs_cursor,
    _single_header,
)
from universe.jobs.auth import checksum_address
from universe.jobs.store import ReplayJobError

if TYPE_CHECKING:
    from universe.api.routes import Request
    from universe.api.server import UniverseApplication

REPLAY_STRATEGIES_VERSION = 1


def _replay_strategies(config: RunnerConfig) -> dict[str, Any]:
    """The runner registry as the UI needs it; module paths are not exposed.

    Retired strategies stay listed so earlier jobs keep their labels; only
    ``status: active`` entries accept new jobs.
    """
    return {
        "version": REPLAY_STRATEGIES_VERSION,
        "strategies": [
            {
                "name": name,
                "label": entry.label,
                "description": entry.description,
                "status": entry.status,
                "config_keys": sorted(
                    STRATEGY_CONFIG_SCHEMAS[entry.config_schema].request_keys
                ),
            }
            for name, entry in sorted(config.strategies.items())
        ],
        "limits": sorted(config.limits),
    }


# GET


def nonce(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _only(r.query, set())
    return HTTPStatus.OK, app.auth.create_nonce()


def list_members(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _only(r.query, set())
    app.auth.require_admin(r.headers)
    return HTTPStatus.OK, {"members": app.auth.list_members()}


def list_jobs(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    assert app.replay_jobs is not None
    query = r.query
    _only(query, {"status", "limit", "cursor"})
    limit = _integer(query, "limit", default=100)
    after = _replay_jobs_cursor(_optional(query, "cursor"))
    rows, has_more = app.replay_jobs.list_jobs(
        status=_optional(query, "status"), limit=limit, after=after
    )
    records = []
    for row in rows:
        record = app.replay_jobs.job_record(row)
        record["submitted_by"] = checksum_address(row.submitted_by)
        records.append(record)
    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = _encode_cursor(
            ["replay_jobs", str(last.created_at_ns), last.job_id]
        )
    return HTTPStatus.OK, {"jobs": records, "next_cursor": next_cursor}


def strategies(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _only(r.query, set())
    return HTTPStatus.OK, _replay_strategies(app.runner_config)


def job_events(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    job_id = _path_value(r.value, "job id")
    assert app.replay_jobs is not None
    query = r.query
    _only(query, {"limit", "cursor"})
    if app.replay_jobs.get_job(job_id) is None:
        raise ReplayJobError(404, "job not found")
    limit = _integer(query, "limit", default=100)
    after = _replay_job_events_cursor(_optional(query, "cursor"), job_id)
    events, has_more = app.replay_jobs.list_events(
        job_id, limit=limit, after_event_id=after
    )
    next_cursor = None
    if has_more and events:
        next_cursor = _encode_cursor(
            ["replay_job_events", job_id, events[-1]["event_id"]]
        )
    return HTTPStatus.OK, {"events": events, "next_cursor": next_cursor}


def job(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _only(r.query, set())
    job_id = _path_value(r.value, "job id")
    stored = app.replay_jobs.get_job(job_id)
    if stored is None:
        return HTTPStatus.NOT_FOUND, {"error": "job not found"}
    row, request_bytes = stored
    # The request was validated against the registry when it was
    # submitted. Re-validating history against today's registry would
    # hide every job whose preset or strategy was later renamed.
    record = app.replay_jobs.job_record(row, request_bytes)
    record["submitted_by"] = checksum_address(row.submitted_by)
    return HTTPStatus.OK, record


# POST


def siwe(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    document = r.document
    _exact_body(document, {"message", "signature"})
    if not isinstance(document["message"], str) or not isinstance(document["signature"], str):
        raise ValueError("message and signature must be strings")
    return HTTPStatus.OK, app.auth.verify_siwe(
        document["message"], document["signature"]
    )


def logout(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _exact_body(r.document, set())
    app.auth.logout(r.headers)
    return HTTPStatus.OK, {"ok": True}


def add_member(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    document = r.document
    _exact_body(document, {"address", "note"})
    principal = app.auth.require_admin(r.headers)
    return HTTPStatus.OK, app.auth.add_member(
        document["address"], document["note"], principal.address
    )


def submit(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    raw_body = r.raw_body
    principal = app.auth.require_member(r.headers)
    key = _single_header(r.headers, "Idempotency-Key")
    if raw_body is None or app.runner_config is None:
        raise ValueError("invalid request")
    # A retired strategy still names jobs accepted before retirement,
    # so an idempotent replay of one returns it; only new jobs are refused.
    request = parse_request(
        raw_body, app.runner_config, accept_retired=True
    )
    existing = app.replay_jobs.lookup_submission(
        principal.address, key, request_sha256(request)
    )
    if existing is not None:
        return HTTPStatus.OK, {
            "job_id": existing.row.job_id,
            "status": existing.row.status,
            "replayed": True,
        }
    request = parse_request(raw_body, app.runner_config)
    occurrences, _more = app.database.list_selections(
        bundle_id=request.bundle_id, limit=1
    )
    result = app.replay_jobs.submit(
        raw_body,
        request,
        principal,
        key,
        time.time_ns(),
        bundle_exists=bool(occurrences),
    )
    return (
        HTTPStatus.CREATED if result.created else HTTPStatus.OK,
        {
            "job_id": result.row.job_id,
            "status": result.row.status,
            "replayed": not result.created,
        },
    )


def cancel(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _exact_body(r.document, set())
    principal = app.auth.require_member(r.headers)
    job_id = _path_value(r.value, "job id")
    row = app.replay_jobs.cancel_job(job_id, principal, time.time_ns())
    record = app.replay_jobs.job_record(row)
    record["submitted_by"] = checksum_address(row.submitted_by)
    return HTTPStatus.OK, record


# DELETE


def remove_member(app: UniverseApplication, r: Request) -> tuple[int, dict[str, Any]]:
    _exact_body(r.document, set())
    principal = app.auth.require_admin(r.headers)
    address = _path_value(r.value, "address")
    return HTTPStatus.OK, app.auth.remove_member(address, principal.address)
