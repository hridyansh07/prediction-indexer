"""Read-only HTTP API for historical Targeter selection occurrences.

``UniverseApplication`` resolves a request through the route table in
``universe.api.routes``; ``build_server`` frames HTTP around it.
"""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from replay.jobs.contracts import RunnerConfig
from universe.api.framing import (
    _boolean,
    _bundle_cursor,
    _claim_market_cursor,
    _encode_cursor,
    _event_cursor,
    _FramingError,
    _integer,
    _json_content_type,
    _only,
    _optional,
    _RequestError,
    _run_cursor,
    _selection_cursor,
    _timestamp,
    _timestamp_ns,
    _unique_object,
    body_error,
    encode_response,
    read_error,
)
from universe.api.rate_limit import RateLimiter
from universe.api.routes import ROUTES, Request
from universe.jobs.auth import AuthStore
from universe.jobs.store import ReplayJobStore
from universe.store import UniverseStore


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
        return self._route("GET", target, headers)

    def post(
        self,
        target: str,
        headers: Any,
        document: dict[str, Any],
        raw_body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        return self._route("POST", target, headers, document, raw_body)

    def delete(
        self,
        target: str,
        headers: Any,
        document: dict[str, Any],
        raw_body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        return self._route("DELETE", target, headers, document, raw_body)

    def _route(
        self,
        method: str,
        target: str,
        headers: Any,
        document: dict[str, Any] | None = None,
        raw_body: bytes | None = None,
    ) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if method != "GET":
            # Every write takes no query and exists only when auth is configured.
            _only(query, set())
            if self.auth is None:
                return HTTPStatus.NOT_FOUND, {"error": "not found"}
        for route in ROUTES:
            if route.method != method:
                continue
            if route.requires is not None and getattr(self, route.requires) is None:
                continue
            value = route.pattern.match(parsed.path)
            if value is None:
                continue
            return route.handler(self, Request(query, headers, value, document, raw_body))
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


def build_server(
    database: UniverseStore,
    auth: AuthStore,
    host: str,
    port: int,
    replay_jobs: ReplayJobStore | None = None,
    runner_config: RunnerConfig | None = None,
    rate_limiter: RateLimiter | None = None,
) -> ThreadingHTTPServer:
    application = UniverseApplication(database, auth, replay_jobs, runner_config)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            if self._rate_limited():
                return
            self._dispatch(lambda: application.get(self.path, self.headers))

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            if self._rate_limited():
                return
            self._dispatch_with_body(application.post)

        def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            if self._rate_limited():
                return
            self._dispatch_with_body(application.delete)

        def _rate_limited(self) -> bool:
            if rate_limiter is None:
                return False
            retry_after = rate_limiter.retry_after(self.client_address[0], self.headers)
            if retry_after is None:
                return False
            self._send_json(
                HTTPStatus.TOO_MANY_REQUESTS,
                {"error": "rate limit exceeded"},
                retry_after=retry_after,
            )
            return True

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
            except Exception as error:  # noqa: BLE001 - secrets and internals must not be logged
                mapped = body_error(error)
                if mapped is None:
                    self.log_error("request failed")
                    mapped = HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal server error"}
                self._send_json(*mapped)
                return
            self._send_json(status, response)

        def _dispatch(self, dispatch) -> None:
            try:
                status, document = dispatch()
            except Exception as error:  # noqa: BLE001 - do not expose or log secrets
                mapped = read_error(error)
                if mapped is None:
                    self.log_error("request failed")
                    mapped = HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal server error"}
                status, document = mapped
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
            except RecursionError as error:
                raise _RequestError("request JSON is too deeply nested") from error
            except json.JSONDecodeError as error:
                raise _RequestError(
                    f"invalid JSON at line {error.lineno} column {error.colno}"
                ) from error
            if not isinstance(document, dict):
                raise _RequestError("JSON body must be an object")
            return document, payload

        def _send_json(
            self,
            status: int,
            document: dict[str, Any],
            *,
            retry_after: int | None = None,
        ) -> None:
            status, payload = encode_response(status, document)
            self.send_response(int(status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            if getattr(self, "_framing_rejected", False):
                self.send_header("Connection", "close")
            if int(status) == HTTPStatus.UNAUTHORIZED:
                self.send_header("WWW-Authenticate", "Bearer")
            if retry_after is not None:
                self.send_header("Retry-After", str(retry_after))
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
    rate_limiter: RateLimiter | None = None,
) -> None:
    server = build_server(
        database, auth, host, port, replay_jobs, runner_config, rate_limiter
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()
