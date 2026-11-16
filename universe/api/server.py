"""Read-only HTTP API for historical Targeter selection occurrences.

``UniverseApplication`` resolves a request through the route table in
``universe.api.routes``; ``build_server`` frames HTTP around it.
"""

from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from universe.api.framing import (
    _boolean,
    _bundle_cursor,
    _claim_market_cursor,
    _encode_cursor,
    _event_cursor,
    _integer,
    _only,
    _optional,
    _run_cursor,
    _selection_cursor,
    _timestamp,
    _timestamp_ns,
    encode_response,
    read_error,
)
from universe.api.routes import ROUTES, Request
from universe.store import UniverseStore


class UniverseApplication:
    def __init__(self, database: UniverseStore) -> None:
        self.database = database

    def get(self, target: str, headers: Any = None) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        query = parse_qs(parsed.query, keep_blank_values=True)
        for route in ROUTES:
            if route.method != "GET":
                continue
            value = route.pattern.match(parsed.path)
            if value is None:
                continue
            return route.handler(self, Request(query, headers, value))
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
    database: UniverseStore, host: str, port: int
) -> ThreadingHTTPServer:
    application = UniverseApplication(database)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            try:
                status, document = application.get(self.path, self.headers)
            except Exception as error:  # noqa: BLE001 - do not expose or log secrets
                mapped = read_error(error)
                if mapped is None:
                    self.log_error("request failed")
                    mapped = HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal server error"}
                status, document = mapped
            self._send_json(status, document)

        def _send_json(self, status: int, document: dict[str, Any]) -> None:
            status, payload = encode_response(status, document)
            self.send_response(int(status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

    return ThreadingHTTPServer((host, port), Handler)


def serve(database: UniverseStore, host: str, port: int) -> None:
    server = build_server(database, host, port)
    try:
        server.serve_forever()
    finally:
        server.server_close()
