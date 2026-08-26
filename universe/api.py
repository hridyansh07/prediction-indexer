"""Read-only HTTP API for historical Targeter selection occurrences."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from targeter.v2.models import isoformat, parse_timestamp
from universe.store import UniverseStore


class UniverseApplication:
    def __init__(self, database: UniverseStore) -> None:
        self.database = database

    def get(self, target: str) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(target)
        query = parse_qs(parsed.query, keep_blank_values=True)
        if parsed.path == "/healthz":
            _only(query, set())
            return HTTPStatus.OK, self.database.status()
        if parsed.path == "/v1/runs":
            return HTTPStatus.OK, self._runs(query)
        if parsed.path == "/v1/selections":
            return HTTPStatus.OK, self._selections(query)
        if parsed.path == "/v1/targeter/cadence":
            _only(query, {"limit"})
            return HTTPStatus.OK, self.database.cadence_snapshot(
                limit=_integer(query, "limit", default=5)
            )
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


def serve(database: UniverseStore, host: str, port: int) -> None:
    application = UniverseApplication(database)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler contract
            try:
                status, document = application.get(self.path)
            except (ValueError, TypeError) as error:
                status, document = HTTPStatus.BAD_REQUEST, {"error": str(error)}
            except Exception as error:  # noqa: BLE001 - do not expose internals
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

    server = ThreadingHTTPServer((host, port), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


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
    if value <= 0 or value > 1000:
        raise ValueError(f"query parameter {field} must be between 1 and 1000")
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
    try:
        padded = value + "=" * (-len(value) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, json.JSONDecodeError) as error:
        raise ValueError("cursor is invalid") from error
    if not isinstance(decoded, list):
        raise ValueError("cursor is invalid")
    return decoded


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
