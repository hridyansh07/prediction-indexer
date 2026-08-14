"""Small read-only HTTP API over the Event Universe SQLite index."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from universe.store import UniverseStore


class UniverseApplication:
    def __init__(self, database: UniverseStore) -> None:
        self.database = database

    def get(self, target: str) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        query = parse_qs(parsed.query)
        if parsed.path == "/healthz":
            return HTTPStatus.OK, self.database.status()
        if parsed.path == "/v1/bundles":
            limit = _integer_query(query, "limit", default=100)
            return HTTPStatus.OK, {"bundles": self.database.list_bundles(limit=limit)}
        if parsed.path.startswith("/v1/bundles/"):
            bundle_id = unquote(parsed.path.removeprefix("/v1/bundles/"))
            if not bundle_id or "/" in bundle_id:
                return HTTPStatus.BAD_REQUEST, {"error": "invalid bundle id"}
            detail = self.database.bundle_detail(bundle_id)
            if detail is None:
                return HTTPStatus.NOT_FOUND, {"error": "bundle not found"}
            return HTTPStatus.OK, detail
        if parsed.path == "/v1/segments":
            start_ns = _integer_query(query, "start_ns")
            end_ns = _integer_query(query, "end_ns")
            lane = _optional_query(query, "lane_id")
            return HTTPStatus.OK, {
                "segments": self.database.overlapping_segments(
                    start_ns=start_ns,
                    end_ns=end_ns,
                    lane_id=lane,
                )
            }
        if parsed.path == "/v1/epochs":
            start_ns = _integer_query(query, "start_ns")
            end_ns = _integer_query(query, "end_ns")
            lane = _optional_query(query, "lane_id")
            return HTTPStatus.OK, {
                "epochs": self.database.overlapping_epochs(
                    start_ns=start_ns,
                    end_ns=end_ns,
                    lane_id=lane,
                )
            }
        return HTTPStatus.NOT_FOUND, {"error": "not found"}


def serve(database: UniverseStore, host: str, port: int) -> None:
    application = UniverseApplication(database)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            try:
                status, document = application.get(self.path)
            except (ValueError, TypeError) as error:
                status, document = HTTPStatus.BAD_REQUEST, {"error": str(error)}
            except Exception as error:  # noqa: BLE001 - return no internals to clients
                self.log_error("request failed: %s", error)
                status, document = HTTPStatus.INTERNAL_SERVER_ERROR, {
                    "error": "internal server error"
                }
            payload = (
                json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
                + "\n"
            ).encode("utf-8")
            self.send_response(int(status))
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            super().log_message(format, *args)

    server = ThreadingHTTPServer((host, port), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _integer_query(
    query: dict[str, list[str]], field: str, *, default: int | None = None
) -> int:
    values = query.get(field)
    if not values:
        if default is not None:
            return default
        raise ValueError(f"missing query parameter: {field}")
    if len(values) != 1:
        raise ValueError(f"query parameter {field} must appear once")
    try:
        value = int(values[0])
    except ValueError as error:
        raise ValueError(f"query parameter {field} must be an integer") from error
    if value < 0:
        raise ValueError(f"query parameter {field} must be non-negative")
    return value


def _optional_query(query: dict[str, list[str]], field: str) -> str | None:
    values = query.get(field)
    if not values:
        return None
    if len(values) != 1 or not values[0]:
        raise ValueError(f"query parameter {field} must appear once and be non-empty")
    return values[0]
