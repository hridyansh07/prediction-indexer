"""Read-only HTTP API for historical Targeter selection occurrences."""

from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from targeter.v2.models import isoformat, parse_timestamp
from replay.jobs.contracts import (
    ContractError,
    RunnerConfig,
    parse_request,
    request_sha256,
)
from universe.auth import AuthError, AuthStore, checksum_address
from universe.replay_jobs import ReplayJobError, ReplayJobStore
from universe.store import (
    EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES,
    DetailTooLarge,
    UniverseStore,
)


class UniverseApplication:
    def __init__(
        self,
        database: UniverseStore,
        auth: AuthStore | None = None,
        replay_jobs: ReplayJobStore | None = None,
        runner_config: RunnerConfig | None = None,
    ) -> None:
        self.database = database
        self.auth = auth
        self.replay_jobs = replay_jobs
        self.runner_config = runner_config

    def get(self, target: str, headers: Any = None) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.path == "/v1/auth/nonce" and self.auth is not None:
            _only(query, set())
            return HTTPStatus.OK, self.auth.create_nonce()
        if parsed.path == "/v1/admin/allowlist" and self.auth is not None:
            _only(query, set())
            self.auth.require_admin(headers)
            return HTTPStatus.OK, {"members": self.auth.list_members()}
        if parsed.path == "/v1/replay/jobs" and self.replay_jobs is not None:
            return HTTPStatus.OK, self._replay_jobs(query)
        if (
            parsed.path.startswith("/v1/replay/jobs/")
            and parsed.path.endswith("/events")
            and self.replay_jobs is not None
        ):
            job_id = _path_value(
                parsed.path.removeprefix("/v1/replay/jobs/").removesuffix("/events"),
                "job id",
            )
            return HTTPStatus.OK, self._replay_job_events(job_id, query)
        if parsed.path.startswith("/v1/replay/jobs/") and self.replay_jobs is not None:
            _only(query, set())
            job_id = _path_value(
                parsed.path.removeprefix("/v1/replay/jobs/"), "job id"
            )
            stored = self.replay_jobs.get_job(job_id)
            if stored is None:
                return HTTPStatus.NOT_FOUND, {"error": "job not found"}
            row, request_bytes = stored
            # The request was validated against the registry when it was
            # submitted. Re-validating history against today's registry would
            # hide every job whose preset or strategy was later renamed.
            record = self.replay_jobs.job_record(row, request_bytes)
            record["submitted_by"] = checksum_address(row.submitted_by)
            return HTTPStatus.OK, record
        if parsed.path == "/healthz":
            _only(query, set())
            return HTTPStatus.OK, self.database.status()
        if parsed.path == "/v1/runs":
            return HTTPStatus.OK, self._runs(query)
        if parsed.path == "/v1/selections":
            return HTTPStatus.OK, self._selections(query)
        if parsed.path == "/v1/bundles":
            return HTTPStatus.OK, self._bundles(query)
        if parsed.path == "/v1/events":
            return HTTPStatus.OK, self._events(query)
        if parsed.path == "/v1/relationship-types":
            _only(query, set())
            return HTTPStatus.OK, _relationship_types()
        if parsed.path == "/v1/targeter/status":
            _only(query, {"limit"})
            return HTTPStatus.OK, self.database.targeter_status_snapshot(
                limit=_integer(query, "limit", default=5)
            )
        if parsed.path.startswith("/v1/targeter/runs/"):
            _only(query, set())
            run_id = _path_value(
                parsed.path.removeprefix("/v1/targeter/runs/"), "run id"
            )
            detail = self.database.targeter_run_detail(run_id)
            if detail is None:
                return HTTPStatus.NOT_FOUND, {"error": "run not found"}
            return HTTPStatus.OK, detail
        if parsed.path.startswith("/v1/events/"):
            _only(query, set())
            event_id = _path_value(
                parsed.path.removeprefix("/v1/events/"), "event id"
            )
            detail = self.database.event_detail(event_id)
            if detail is None:
                return HTTPStatus.NOT_FOUND, {"error": "event not found"}
            return HTTPStatus.OK, detail
        if parsed.path.startswith("/v1/markets/"):
            _only(query, {"market_template_version", "outcome_space_version"})
            market_id = _path_value(
                parsed.path.removeprefix("/v1/markets/"), "market id"
            )
            detail = self.database.market_detail(
                market_id,
                market_template_version=_optional_positive_integer(
                    query, "market_template_version"
                ),
                outcome_space_version=_optional_positive_integer(
                    query, "outcome_space_version"
                ),
            )
            if detail is None:
                return HTTPStatus.NOT_FOUND, {"error": "market not found"}
            return HTTPStatus.OK, detail
        if parsed.path.startswith("/v1/claims/") and parsed.path.endswith("/markets"):
            _only(query, {"limit", "cursor"})
            claim_id = _path_value(
                parsed.path.removeprefix("/v1/claims/").removesuffix("/markets"),
                "claim id",
            )
            if not self.database.claim_exists(claim_id):
                return HTTPStatus.NOT_FOUND, {"error": "claim not found"}
            return HTTPStatus.OK, self._claim_markets(claim_id, query)
        if parsed.path.startswith("/v1/claims/"):
            _only(query, set())
            claim_id = _path_value(
                parsed.path.removeprefix("/v1/claims/"), "claim id"
            )
            detail = self.database.claim_detail(claim_id)
            if detail is None:
                return HTTPStatus.NOT_FOUND, {"error": "claim not found"}
            return HTTPStatus.OK, detail
        if parsed.path.startswith("/v1/bundles/") and parsed.path.endswith("/history"):
            bundle_id = _path_value(
                parsed.path.removeprefix("/v1/bundles/").removesuffix("/history"),
                "bundle id",
            )
            return HTTPStatus.OK, self._selections(
                query, bundle_id=bundle_id, default_sort="selected"
            )
        if parsed.path.startswith("/v1/runs/"):
            suffix = parsed.path.removeprefix("/v1/runs/")
            parts = suffix.split("/")
            run_id = _path_value(parts[0], "run id")
            if len(parts) == 1:
                _only(query, set())
                detail = self.database.run_detail(run_id)
                if detail is None:
                    return HTTPStatus.NOT_FOUND, {"error": "run not found"}
                return HTTPStatus.OK, detail
            if len(parts) == 2 and parts[1] == "audit":
                _only(query, set())
                audit = self.database.audit_run(run_id)
                if audit is None:
                    return HTTPStatus.NOT_FOUND, {"error": "run not found"}
                return HTTPStatus.OK, audit
            if len(parts) == 2 and parts[1] == "selections":
                return HTTPStatus.OK, self._selections(
                    query, run_id=run_id, default_sort="activation"
                )
            if len(parts) == 3 and parts[1] == "selections":
                _only(query, set())
                bundle_id = _path_value(parts[2], "bundle id")
                detail = self.database.selection_detail(run_id, bundle_id)
                if detail is None:
                    return HTTPStatus.NOT_FOUND, {"error": "selection not found"}
                return HTTPStatus.OK, detail
        return HTTPStatus.NOT_FOUND, {"error": "not found"}

    def post(
        self,
        target: str,
        headers: Any,
        document: dict[str, Any],
        raw_body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        _only(parse_qs(parsed.query, keep_blank_values=True), set())
        if self.auth is None:
            return HTTPStatus.NOT_FOUND, {"error": "not found"}
        if parsed.path == "/v1/auth/siwe":
            _exact_body(document, {"message", "signature"})
            if not isinstance(document["message"], str) or not isinstance(document["signature"], str):
                raise ValueError("message and signature must be strings")
            return HTTPStatus.OK, self.auth.verify_siwe(
                document["message"], document["signature"]
            )
        if parsed.path == "/v1/auth/logout":
            _exact_body(document, set())
            self.auth.logout(headers)
            return HTTPStatus.OK, {"ok": True}
        if parsed.path == "/v1/admin/allowlist":
            _exact_body(document, {"address", "note"})
            principal = self.auth.require_admin(headers)
            return HTTPStatus.OK, self.auth.add_member(
                document["address"], document["note"], principal.address
            )
        if parsed.path == "/v1/replay/jobs" and self.replay_jobs is not None:
            principal = self.auth.require_member(headers)
            key = _single_header(headers, "Idempotency-Key")
            if raw_body is None or self.runner_config is None:
                raise ValueError("invalid request")
            request = parse_request(raw_body, self.runner_config)
            existing = self.replay_jobs.lookup_submission(
                principal.address, key, request_sha256(request)
            )
            if existing is not None:
                return HTTPStatus.OK, {
                    "job_id": existing.row.job_id,
                    "status": existing.row.status,
                    "replayed": True,
                }
            occurrences, _more = self.database.list_selections(
                bundle_id=request.bundle_id, limit=1
            )
            result = self.replay_jobs.submit(
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
        cancel_prefix = "/v1/replay/jobs/"
        if (
            parsed.path.startswith(cancel_prefix)
            and parsed.path.endswith("/cancel")
            and self.replay_jobs is not None
        ):
            _exact_body(document, set())
            principal = self.auth.require_member(headers)
            job_id = _path_value(
                parsed.path.removeprefix(cancel_prefix).removesuffix("/cancel"),
                "job id",
            )
            row = self.replay_jobs.cancel_job(job_id, principal, time.time_ns())
            record = self.replay_jobs.job_record(row)
            record["submitted_by"] = checksum_address(row.submitted_by)
            return HTTPStatus.OK, record
        return HTTPStatus.NOT_FOUND, {"error": "not found"}

    def delete(
        self,
        target: str,
        headers: Any,
        document: dict[str, Any],
        raw_body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        _only(parse_qs(parsed.query, keep_blank_values=True), set())
        if self.auth is None:
            return HTTPStatus.NOT_FOUND, {"error": "not found"}
        prefix = "/v1/admin/allowlist/"
        if parsed.path.startswith(prefix):
            _exact_body(document, set())
            principal = self.auth.require_admin(headers)
            address = _path_value(parsed.path.removeprefix(prefix), "address")
            return HTTPStatus.OK, self.auth.remove_member(address, principal.address)
        return HTTPStatus.NOT_FOUND, {"error": "not found"}

    def _runs(self, query: dict[str, list[str]]) -> dict[str, Any]:
        _only(
            query,
            {"generated_start", "generated_end", "input_complete", "limit", "cursor"},
        )
        limit = _integer(query, "limit", default=100)
        after = _run_cursor(_optional(query, "cursor"))
        runs, has_more = self.database.list_runs(
            generated_start_ns=_timestamp(query, "generated_start"),
            generated_end_ns=_timestamp(query, "generated_end"),
            input_complete=_boolean(query, "input_complete"),
            after=after,
            limit=limit,
        )
        next_cursor = None
        if has_more and runs:
            last = runs[-1]
            next_cursor = _encode_cursor(
                ["runs", _timestamp_ns(last["generated_at"]), last["run_id"]]
            )
        return {"runs": runs, "next_cursor": next_cursor}

    def _selections(
        self,
        query: dict[str, list[str]],
        *,
        run_id: str | None = None,
        bundle_id: str | None = None,
        default_sort: str = "activation",
    ) -> dict[str, Any]:
        _only(
            query,
            {
                "activation_start",
                "activation_end",
                "selected_start",
                "selected_end",
                "venue",
                "sort",
                "limit",
                "cursor",
            },
        )
        sort = _optional(query, "sort") or default_sort
        if sort not in {"activation", "selected"}:
            raise ValueError("sort must be activation or selected")
        limit = _integer(query, "limit", default=100)
        after = _selection_cursor(_optional(query, "cursor"), sort)
        selections, has_more = self.database.list_selections(
            run_id=run_id,
            bundle_id=bundle_id,
            venue=_optional(query, "venue"),
            activation_start_ns=_timestamp(query, "activation_start"),
            activation_end_ns=_timestamp(query, "activation_end"),
            selected_start_ns=_timestamp(query, "selected_start"),
            selected_end_ns=_timestamp(query, "selected_end"),
            sort=sort,
            after=after,
            limit=limit,
        )
        next_cursor = None
        if has_more and selections:
            last = selections[-1]
            timestamp = (
                last["activation_at"] if sort == "activation" else last["generated_at"]
            )
            next_cursor = _encode_cursor(
                [sort, _timestamp_ns(timestamp), last["run_id"], last["bundle_id"]]
            )
        return {
            "selections": selections,
            "sort": sort,
            "next_cursor": next_cursor,
        }

    def _bundles(self, query: dict[str, list[str]]) -> dict[str, Any]:
        _only(query, {"limit", "cursor"})
        after = _bundle_cursor(_optional(query, "cursor"))
        bundles, has_more = self.database.list_bundles(
            after=after,
            limit=_integer(query, "limit", default=100),
        )
        next_cursor = None
        if has_more and bundles:
            last = bundles[-1]
            next_cursor = _encode_cursor(
                [
                    "bundles",
                    _timestamp_ns(last["last_selected_at"]),
                    last["bundle_id"],
                ]
            )
        return {"bundles": bundles, "next_cursor": next_cursor}

    def _claim_markets(
        self, claim_id: str, query: dict[str, list[str]]
    ) -> dict[str, Any]:
        after = _claim_market_cursor(_optional(query, "cursor"))
        markets, has_more = self.database.claim_markets(
            claim_id,
            after=after,
            limit=_integer(query, "limit", default=100),
        )
        next_cursor = None
        if has_more and markets:
            last = markets[-1]
            next_cursor = _encode_cursor(
                [
                    "claim_markets",
                    last["venue"],
                    last["venue_market_id"],
                    last["claim_key"],
                ]
            )
        return {"markets": markets, "next_cursor": next_cursor}

    def _events(self, query: dict[str, list[str]]) -> dict[str, Any]:
        _only(query, {"limit", "cursor"})
        after = _event_cursor(_optional(query, "cursor"))
        events, has_more = self.database.list_events(
            after=after,
            limit=_integer(query, "limit", default=100),
        )
        next_cursor = None
        if has_more and events:
            last = events[-1]
            next_cursor = _encode_cursor(
                ["events", _timestamp_ns(last["activation_at"]), last["event_id"]]
            )
        return {"events": events, "next_cursor": next_cursor}

    def _replay_jobs(self, query: dict[str, list[str]]) -> dict[str, Any]:
        assert self.replay_jobs is not None
        _only(query, {"status", "limit", "cursor"})
        limit = _integer(query, "limit", default=100)
        after = _replay_jobs_cursor(_optional(query, "cursor"))
        rows, has_more = self.replay_jobs.list_jobs(
            status=_optional(query, "status"), limit=limit, after=after
        )
        records = []
        for row in rows:
            record = self.replay_jobs.job_record(row)
            record["submitted_by"] = checksum_address(row.submitted_by)
            records.append(record)
        next_cursor = None
        if has_more and rows:
            last = rows[-1]
            next_cursor = _encode_cursor(
                ["replay_jobs", str(last.created_at_ns), last.job_id]
            )
        return {"jobs": records, "next_cursor": next_cursor}

    def _replay_job_events(
        self, job_id: str, query: dict[str, list[str]]
    ) -> dict[str, Any]:
        assert self.replay_jobs is not None
        _only(query, {"limit", "cursor"})
        if self.replay_jobs.get_job(job_id) is None:
            raise ReplayJobError(404, "job not found")
        limit = _integer(query, "limit", default=100)
        after = _replay_job_events_cursor(_optional(query, "cursor"), job_id)
        events, has_more = self.replay_jobs.list_events(
            job_id, limit=limit, after_event_id=after
        )
        next_cursor = None
        if has_more and events:
            next_cursor = _encode_cursor(
                ["replay_job_events", job_id, events[-1]["event_id"]]
            )
        return {"events": events, "next_cursor": next_cursor}


def build_server(
    database: UniverseStore,
    auth: AuthStore,
    host: str,
    port: int,
    replay_jobs: ReplayJobStore | None = None,
    runner_config: RunnerConfig | None = None,
) -> ThreadingHTTPServer:
    application = UniverseApplication(database, auth, replay_jobs, runner_config)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            self._dispatch(lambda: application.get(self.path, self.headers))

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            self._dispatch_with_body(application.post)

        def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            self._dispatch_with_body(application.delete)

        def _dispatch_with_body(self, dispatch) -> None:
            try:
                document, raw_body = self._read_json_body()
                status, response = dispatch(
                    self.path, self.headers, document, raw_body
                )
            except _FramingError as error:
                self.close_connection = True
                self._framing_rejected = True
                self._send_json(error.status, {"error": error.message})
                return
            except AuthError as error:
                self._send_json(error.status, {"error": str(error)})
                return
            except ReplayJobError as error:
                self._send_json(error.status, {"error": str(error)})
                return
            except (ContractError, _RequestError) as error:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(error)})
                return
            except (ValueError, TypeError):
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid request"})
                return
            except Exception:  # noqa: BLE001 - secrets and internals must not be logged
                self.log_error("request failed")
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": "internal server error"},
                )
                return
            self._send_json(status, response)

        def _dispatch(self, dispatch) -> None:
            try:
                status, document = dispatch()
            except DetailTooLarge as error:
                status, document = HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {
                    "error": str(error)
                }
            except AuthError as error:
                status, document = error.status, {"error": str(error)}
            except ReplayJobError as error:
                status, document = error.status, {"error": str(error)}
            except (ValueError, TypeError) as error:
                status, document = HTTPStatus.BAD_REQUEST, {"error": str(error)}
            except Exception:  # noqa: BLE001 - do not expose or log secrets
                self.log_error("request failed")
                status, document = HTTPStatus.INTERNAL_SERVER_ERROR, {
                    "error": "internal server error"
                }
            self._send_json(status, document)

        def _read_json_body(self) -> tuple[dict[str, Any], bytes]:
            if self.headers.get_all("Transfer-Encoding", []):
                raise _FramingError(HTTPStatus.BAD_REQUEST, "invalid request framing")
            lengths = self.headers.get_all("Content-Length", [])
            if not lengths:
                raise _FramingError(HTTPStatus.LENGTH_REQUIRED, "content length required")
            if len(lengths) != 1 or not lengths[0].isdigit():
                raise _FramingError(HTTPStatus.BAD_REQUEST, "invalid content length")
            length = int(lengths[0])
            if length > 65_536:
                raise _FramingError(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "request body too large")
            content_types = self.headers.get_all("Content-Type", [])
            if len(content_types) != 1 or not _json_content_type(content_types[0]):
                raise _FramingError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, "application/json required")
            payload = self.rfile.read(length)
            if len(payload) != length:
                raise _FramingError(HTTPStatus.BAD_REQUEST, "incomplete request body")
            try:
                text = payload.decode("utf-8")
            except UnicodeDecodeError as error:
                raise _RequestError("request body must be UTF-8 JSON") from error
            try:
                document = json.loads(text, object_pairs_hook=_unique_object)
            except json.JSONDecodeError as error:
                raise _RequestError(
                    f"invalid JSON at line {error.lineno} column {error.colno}"
                ) from error
            if not isinstance(document, dict):
                raise _RequestError("JSON body must be an object")
            return document, payload

        def _send_json(self, status: int, document: dict[str, Any]) -> None:
            payload = (
                json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                + "\n"
            ).encode("utf-8")
            if len(payload) > EVENT_UNIVERSE_RESPONSE_BUDGET_BYTES:
                status = HTTPStatus.REQUEST_ENTITY_TOO_LARGE
                payload = b'{"error":"response exceeds Event Universe size budget"}\n'
            self.send_response(int(status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            if getattr(self, "_framing_rejected", False):
                self.send_header("Connection", "close")
            if int(status) == HTTPStatus.UNAUTHORIZED:
                self.send_header("WWW-Authenticate", "Bearer")
            self.end_headers()
            self.wfile.write(payload)

    return ThreadingHTTPServer((host, port), Handler)


def serve(
    database: UniverseStore,
    auth: AuthStore,
    host: str,
    port: int,
    replay_jobs: ReplayJobStore | None = None,
    runner_config: RunnerConfig | None = None,
) -> None:
    server = build_server(
        database, auth, host, port, replay_jobs, runner_config
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


class _FramingError(Exception):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


class _RequestError(ValueError):
    """A validation failure whose message is safe to return to the client."""


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _RequestError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _json_content_type(value: str) -> bool:
    parts = [part.strip().lower() for part in value.split(";")]
    return parts[0] == "application/json" and (
        len(parts) == 1 or (len(parts) == 2 and parts[1] == "charset=utf-8")
    )


def _single_header(headers: Any, field: str) -> str:
    values = headers.get_all(field, []) if hasattr(headers, "get_all") else []
    if not values and isinstance(headers, dict) and field in headers:
        values = [headers[field]]
    if len(values) != 1 or not isinstance(values[0], str):
        raise ReplayJobError(400, f"exactly one {field} header is required")
    return values[0]


def _exact_body(document: dict[str, Any], fields: set[str]) -> None:
    if set(document) != fields:
        missing = sorted(fields - set(document))
        unexpected = sorted(set(document) - fields)
        details = []
        if missing:
            details.append(f"missing fields: {', '.join(missing)}")
        if unexpected:
            details.append(f"unexpected fields: {', '.join(unexpected)}")
        raise _RequestError("request body fields are invalid; " + "; ".join(details))


def _only(query: dict[str, list[str]], expected: set[str]) -> None:
    unexpected = set(query) - expected
    if unexpected:
        raise ValueError(f"unexpected query parameter: {sorted(unexpected)[0]}")


def _optional(query: dict[str, list[str]], field: str) -> str | None:
    values = query.get(field)
    if values is None:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError(f"query parameter {field} must appear once and be non-empty")
    return values[0]


def _integer(query: dict[str, list[str]], field: str, *, default: int) -> int:
    raw = _optional(query, field)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"query parameter {field} must be an integer") from error
    if value <= 0 or value > 100:
        raise ValueError(f"query parameter {field} must be between 1 and 100")
    return value


def _optional_positive_integer(
    query: dict[str, list[str]], field: str
) -> int | None:
    raw = _optional(query, field)
    if raw is None:
        return None
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError(f"query parameter {field} must be an integer") from error
    if value <= 0:
        raise ValueError(f"query parameter {field} must be positive")
    return value


def _boolean(query: dict[str, list[str]], field: str) -> bool | None:
    raw = _optional(query, field)
    if raw is None:
        return None
    if raw not in {"true", "false"}:
        raise ValueError(f"query parameter {field} must be true or false")
    return raw == "true"


def _timestamp(query: dict[str, list[str]], field: str) -> int | None:
    raw = _optional(query, field)
    return None if raw is None else _timestamp_ns(raw)


def _timestamp_ns(value: str) -> int:
    parsed = parse_timestamp(value)
    if parsed is None or value != isoformat(parsed):
        raise ValueError("timestamp query parameters must be UTC RFC 3339 timestamps")
    delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1_000
    )


def _path_value(value: str, label: str) -> str:
    decoded = unquote(value)
    if not decoded or "/" in decoded:
        raise ValueError(f"invalid {label}")
    return decoded


def _encode_cursor(value: list[Any]) -> str:
    payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_cursor(value: str) -> list[Any]:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError("cursor is invalid")
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.b64decode(padded, altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=") != value:
            raise ValueError("noncanonical cursor")
        decoded = json.loads(raw)
    except (ValueError, json.JSONDecodeError) as error:
        raise ValueError("cursor is invalid") from error
    if not isinstance(decoded, list):
        raise ValueError("cursor is invalid")
    return decoded


def _replay_jobs_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "replay_jobs"
        or not isinstance(decoded[1], str)
        or not re.fullmatch(r"0|[1-9][0-9]*", decoded[1])
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to replay jobs")
    return int(decoded[1]), decoded[2]


def _replay_job_events_cursor(
    value: str | None, job_id: str
) -> int | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "replay_job_events"
        or decoded[1] != job_id
        or not isinstance(decoded[2], str)
        or not re.fullmatch(r"[1-9][0-9]*", decoded[2])
    ):
        raise ValueError("cursor does not belong to this replay job")
    return int(decoded[2])


def _claim_market_cursor(value: str | None) -> tuple[str, str, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 4
        or decoded[0] != "claim_markets"
        or not all(isinstance(item, str) for item in decoded[1:])
    ):
        raise ValueError("cursor is invalid")
    return (decoded[1], decoded[2], decoded[3])


def _run_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "runs"
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to the runs query")
    return decoded[1], decoded[2]


def _selection_cursor(value: str | None, sort: str) -> tuple[int, str, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 4
        or decoded[0] != sort
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
        or not isinstance(decoded[3], str)
    ):
        raise ValueError("cursor does not belong to this selections query")
    return decoded[1], decoded[2], decoded[3]


def _bundle_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "bundles"
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to the bundles query")
    return decoded[1], decoded[2]


def _event_cursor(value: str | None) -> tuple[int, str] | None:
    if value is None:
        return None
    decoded = _decode_cursor(value)
    if (
        len(decoded) != 3
        or decoded[0] != "events"
        or not isinstance(decoded[1], int)
        or not isinstance(decoded[2], str)
    ):
        raise ValueError("cursor does not belong to the events query")
    return decoded[1], decoded[2]


def _relationship_types() -> dict[str, Any]:
    """The relation types a claim pair can carry.

    IDENTITY is absent by construction: equal outcome subsets are one claim, so
    equivalence is shared membership rather than a relation. REVERSE_IMPLICATION
    is normalized to IMPLICATION with the antecedent named first. OVERLAP is the
    catch-all branch of the mask comparison rather than a finding, and the
    targeter's own scorer discards it, so it is never stored.
    """
    return {
        "relationship_type_catalog_version": 2,
        "types": [
            {
                "type": "IMPLICATION",
                "directed": True,
                "member_roles": ["antecedent", "consequent"],
            },
            {
                "type": "MUTUAL_EXCLUSION",
                "directed": False,
                "member_roles": ["member"],
            },
        ],
    }
