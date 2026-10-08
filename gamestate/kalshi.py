#!/usr/bin/env python3
"""One-shot Kalshi game-state archive. See KALSHI_GAME_STATE_PULL_V1.md."""

from __future__ import annotations

import argparse
import base64
import hashlib
import http.client
import io
import json
import math
import os
import re
import socket
import sys
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from archive.common.durable import write_json_durable
from archive.storage.base import (
    ObjectExpectation,
    ObjectStoreError,
    VerificationFailure,
    normalize_key,
)
from archive.storage.factory import build_store
from encoder import LogicalIdentity, StoredIdentity, decode_stream, encode_stream

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
# Seconds between requests to one host. Universe admits three unauthenticated
# requests per ten seconds (configs/event_universe.json); Kalshi is public.
KALSHI_SPACING = 0.2
UNIVERSE_SPACING = 3.4
DERIVATION_VERSION = 1
MAX_BODY = 4 * 1024 * 1024
MAX_RAW = 128 * 1024 * 1024
MAX_METADATA = 1024 * 1024
MAX_PAGES = 100
MAX_ITEMS = 1000
MAX_ATTEMPTS = 10000
RECORD_FIELDS = {
    "record_version",
    "seq",
    "requested_at_ns",
    "received_at_ns",
    "method",
    "url",
    "status",
    "error",
    "content_type",
    "body_sha256",
}
RECEIPT_FIELDS = {
    "receipt_version",
    "source",
    "milestone_id",
    "bundle_ids",
    "script_version",
    "fetch_started_ns",
    "fetch_ended_ns",
    "status",
    "logical",
    "stored",
    "request_count",
    "provider_checksum",
    "provider_checksum_algorithm",
}


class FetchError(ValueError):
    """A transport, HTTP, bound, or vendor-shape failure; no response bodies in errors."""


def dumps(value):
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode()


def loads(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate_json_key")
            result[key] = value
        return result

    def finite_float(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite_json")
        return result

    return json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_float=finite_float,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite_json")),
    )


def identifier(value):
    if (
        not isinstance(value, str)
        or not re.fullmatch(r"[A-Za-z0-9_:.-]{1,256}", value)
        or value in (".", "..")
    ):
        raise ValueError("invalid_identifier")
    return value


def timestamp(value):
    match = (
        re.fullmatch(
            r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)", value
        )
        if isinstance(value, str)
        else None
    )
    if match is None:
        raise ValueError("invalid_rfc3339")
    # Avoid float rounding and preserve all nine fractional digits.
    base, fraction, suffix = match.groups()
    dt = datetime.fromisoformat(base + suffix.replace("Z", "+00:00"))
    delta = dt.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1_000_000_000 + int(
        (fraction or "").ljust(9, "0")
    )


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def transport(url):
    request = Request(
        url, headers={"Accept": "application/json", "Accept-Encoding": "identity"}
    )
    try:
        response = build_opener(NoRedirect()).open(request, timeout=20)
    except HTTPError as error:
        response = error
    with response:
        body = response.read(MAX_BODY + 1)
        if len(body) > MAX_BODY:
            raise FetchError("body_too_large")
        return response.code, dict(response.headers), body


def is_kalshi(url):
    return url.startswith(KALSHI + "/")


def body_bytes(record):
    if not isinstance(record, dict):
        raise ValueError("record_shape")
    body_field = {"body", "body_b64"} & record.keys()
    if len(body_field) != 1 or record.keys() != RECORD_FIELDS | body_field:
        raise ValueError("record_fields")
    if (
        type(record["record_version"]) is not int
        or record["record_version"] != 1
        or record["method"] != "GET"
    ):
        raise ValueError("record_version")
    for name in ("seq", "requested_at_ns"):
        if type(record[name]) is not int or record[name] < 0:
            raise ValueError("record_time_or_seq")
    for name in ("received_at_ns", "status"):
        if record[name] is not None and (
            type(record[name]) is not int or record[name] < 0
        ):
            raise ValueError("record_response")
    if not isinstance(record["url"], str) or len(record["url"]) > 8192:
        raise ValueError("record_url")
    for name in ("error", "content_type", "body_sha256"):
        if record[name] is not None and (
            not isinstance(record[name], str) or len(record[name]) > 1024
        ):
            raise ValueError("record_metadata")
    if (
        record["received_at_ns"] is not None
        and record["received_at_ns"] < record["requested_at_ns"]
    ):
        raise ValueError("record_time_order")
    if record["status"] is not None and not 100 <= record["status"] <= 599:
        raise ValueError("record_status")
    value = record[next(iter(body_field))]
    if value is None:
        if "body_b64" in record or record["body_sha256"] is not None:
            raise ValueError("record_missing_body")
        return None
    if not isinstance(value, str):
        raise ValueError("record_body")
    raw = (
        value.encode("utf-8")
        if "body" in record
        else base64.b64decode(value, validate=True)
    )
    if len(raw) > MAX_BODY or hashlib.sha256(raw).hexdigest() != record["body_sha256"]:
        raise ValueError("record_body_identity")
    if "body_b64" in record:
        if record["error"] != "non_utf8_body":
            raise ValueError("record_encoding")
        try:
            raw.decode("utf-8")
        except UnicodeDecodeError:
            pass
        else:
            raise ValueError("record_encoding")
    return raw


class AttemptLog:
    """Disk-backed attempt journal; only offsets, never complete raw bodies, in RAM."""

    def __init__(self):
        self.file = tempfile.TemporaryFile()
        self.offsets = []

    def __len__(self):
        return len(self.offsets)

    def append(self, record):
        self.file.seek(0, 2)
        encoded = dumps(record)
        if self.file.tell() + len(encoded) > MAX_RAW:
            raise FetchError("invocation_raw_limit")
        self.offsets.append(self.file.tell())
        self.file.write(encoded)

    def __getitem__(self, index):
        self.file.seek(self.offsets[index])
        return loads(self.file.readline(MAX_BODY * 6 + 16384))

    def __iter__(self):
        for index in range(len(self)):
            yield self[index]

    def close(self):
        self.file.close()


class Client:
    def __init__(
        self,
        send=transport,
        *,
        sleep=time.sleep,
        clock=time.time_ns,
        monotonic=time.monotonic,
        records=None,
        universe_spacing=UNIVERSE_SPACING,
        kalshi_spacing=KALSHI_SPACING,
    ):
        self.send, self.sleep, self.clock, self.monotonic = (
            send,
            sleep,
            clock,
            monotonic,
        )
        self.records = AttemptLog() if records is None else records
        self.retries = 0
        self.next_request: dict[str, float] = {}
        self.universe_spacing, self.kalshi_spacing = universe_spacing, kalshi_spacing

    def close(self):
        if isinstance(self.records, AttemptLog):
            self.records.close()

    def get(self, url):
        if len(url) > 8192:
            raise FetchError("url_too_large")
        for attempt in range(5):
            if len(self.records) >= MAX_ATTEMPTS:
                raise FetchError("attempt_limit")
            if isinstance(self.records, AttemptLog):
                self.records.file.seek(0, 2)
                if self.records.file.tell() + MAX_BODY * 6 + 16384 > MAX_RAW:
                    raise FetchError("invocation_raw_limit")
            host = urlsplit(url).netloc
            spacing = self.kalshi_spacing if is_kalshi(url) else self.universe_spacing
            self.sleep(max(0, self.next_request.get(host, 0) - self.monotonic()))
            self.next_request[host] = self.monotonic() + spacing
            r = dict(
                record_version=1,
                seq=len(self.records),
                requested_at_ns=self.clock(),
                received_at_ns=None,
                method="GET",
                url=url,
                status=None,
                error=None,
                content_type=None,
                body_sha256=None,
                body=None,
            )
            headers = {}
            retry = False
            try:
                status, headers, raw = self.send(url)
                r["received_at_ns"] = self.clock()
                r["status"] = status
                if len(raw) > MAX_BODY:
                    raise FetchError("body_too_large")
                headers = {k.lower(): v for k, v in headers.items()}
                r["content_type"] = headers.get("content-type")
                if r["content_type"] is not None and len(r["content_type"]) > 1024:
                    r["content_type"] = None
                    raise FetchError("metadata_too_large")
                r["body_sha256"] = hashlib.sha256(raw).hexdigest()
                try:
                    r["body"] = raw.decode("utf-8")
                except UnicodeDecodeError:
                    del r["body"]
                    r["body_b64"] = base64.b64encode(raw).decode()
                    r["error"] = "non_utf8_body"
                retry = status == 429 or 500 <= status <= 599
                if r["error"] is None and not 200 <= status < 300:
                    r["error"] = "http_status"
            except (TimeoutError, socket.timeout):
                r["error"], retry = "timeout", True
            except URLError as error:
                r["error"] = (
                    "timeout"
                    if isinstance(error.reason, TimeoutError)
                    else "connection"
                )
                retry = True
            except (OSError, http.client.HTTPException):
                # A reset or a body cut short (IncompleteRead) is transient.
                r["error"], retry = "connection", True
            except FetchError as error:
                r["error"] = str(error)
            self.records.append(r)
            if retry and attempt < 4:
                delay = min(2**attempt, 16)
                value = headers.get("retry-after")
                if value:
                    try:
                        wait = (
                            float(value)
                            if re.fullmatch(r"\d+", value)
                            else parsedate_to_datetime(value).timestamp()
                            - self.clock() / 1e9
                        )
                    except (ValueError, TypeError, OverflowError):
                        wait = 0
                    if wait > 60:
                        raise FetchError("retry_after_exceeds_budget")
                    if wait >= 0:
                        delay = max(delay, wait)
                self.retries += 1
                self.sleep(delay)
                continue
            if r["error"] is not None:
                raise FetchError(r["error"])
            try:
                document = loads(r["body"])
                if not isinstance(document, dict):
                    raise ValueError()
                return document
            except (ValueError, TypeError, RecursionError):
                raise FetchError("invalid_json") from None
        raise FetchError("retry_exhausted")


def pages(client, base, path, query=None):
    query = dict(query or {}, limit=50)
    seen = set()
    count = 0
    for _ in range(MAX_PAGES):
        doc = client.get(base.rstrip("/") + path + "?" + urlencode(query))
        if set(doc) != {"selections", "sort", "next_cursor"} and set(doc) != {
            "selections",
            "next_cursor",
        }:
            raise FetchError("page_shape")
        rows, cursor = doc["selections"], doc["next_cursor"]
        if (
            not isinstance(rows, list)
            or len(rows) > 50
            or any(not isinstance(r, dict) for r in rows)
        ):
            raise FetchError("page_shape")
        count += len(rows)
        if count > MAX_ITEMS:
            raise FetchError("item_limit")
        yield from rows
        if cursor is None:
            return
        if (
            not isinstance(cursor, str)
            or not cursor
            or len(cursor) > 4096
            or cursor in seen
        ):
            raise FetchError("cursor_loop_or_invalid")
        seen.add(cursor)
        query["cursor"] = cursor
    raise FetchError("page_limit")


def bundle_tickers(client, base, bundle):
    refs: set[str] = set()
    rows = list(
        pages(client, base, f"/v1/bundles/{quote(identifier(bundle), safe='')}/history")
    )
    for run in sorted({identifier(row["run_id"]) for row in rows}):
        doc = client.get(
            base.rstrip("/")
            + f"/v1/runs/{quote(run, safe='')}/selections/{quote(bundle, safe='')}"
        )
        if doc.get("bundle_id") != bundle or doc.get("run_id") != run:
            raise FetchError("selection_identity")
        context = doc.get("context")
        values = context.get("event_refs") if isinstance(context, dict) else None
        if (
            not isinstance(values, list)
            or len(values) > MAX_ITEMS
            or any(not isinstance(v, str) for v in values)
        ):
            raise FetchError("context_shape")
        refs.update(identifier(v[7:]) for v in values if v.startswith("kalshi:"))
    return sorted(refs)


def validate_milestone(m):
    if not isinstance(m, dict) or m.get("type") != "esports_match":
        raise FetchError("milestone_shape")
    identifier(m["id"])
    timestamp(m["start_date"])
    if not isinstance(m.get("details"), dict):
        raise FetchError("milestone_shape")
    for field in ("related_event_tickers", "primary_event_tickers"):
        values = m.get(field)
        if (
            not isinstance(values, list)
            or not values
            or len(values) > 100
            or len(set(values)) != len(values)
        ):
            raise FetchError("milestone_tickers")
        for value in values:
            identifier(value)
    return m


def validate_response(url, doc, milestone_id):
    """Validate fields this adapter reads; vendor objects themselves are open."""
    path = urlsplit(url).path
    if "/live_data/milestone/" in path:
        value = doc.get("live_data")
        if (
            not isinstance(value, dict)
            or value.get("milestone_id") != milestone_id
            or not isinstance(value.get("details"), dict)
        ):
            raise FetchError("live_data_shape")
        details = value["details"]
        for side in ("home", "away"):
            rows, periods = (
                details.get(side + "_stats"),
                details.get(side + "_periods", {}),
            )
            if (
                not isinstance(rows, list)
                or len(rows) > 100
                or not isinstance(periods, dict)
                or len(periods) > 100
            ):
                raise FetchError("live_period_shape")
            for row in rows:
                if (
                    not isinstance(row, dict)
                    or not isinstance(row.get("period"), str)
                    or not isinstance(row.get("stats"), dict)
                ):
                    raise FetchError("live_period_shape")
    elif "/events/" in path:
        event = doc.get("event")
        if (
            not isinstance(event, dict)
            or event.get("event_ticker") != path.rsplit("/", 1)[-1]
        ):
            raise FetchError("event_identity")
        markets = event.get("markets")
        if (
            not isinstance(markets, list)
            or not markets
            or len(markets) > MAX_ITEMS
            or not isinstance(event.get("product_metadata", {}), dict)
        ):
            raise FetchError("event_shape")
        for market in markets:
            if not isinstance(market, dict):
                raise FetchError("market_shape")
            identifier(market.get("ticker"))
            for field in ("result", "yes_sub_title"):
                if not isinstance(market.get(field), str):
                    raise FetchError("market_shape")
            if not isinstance(market.get("custom_strike", {}), dict):
                raise FetchError("market_shape")
            for field in ("close_time", "settlement_ts"):
                if market.get(field) is not None:
                    timestamp(market[field])
    return doc


def map_bundle(client, tickers):
    if not tickers:
        return None, "no_kalshi_events"
    milestones = []
    try:
        for ticker in sorted(set(tickers)):
            doc = client.get(
                KALSHI
                + "/milestones?"
                + urlencode({"limit": 10, "related_event_ticker": identifier(ticker)})
            )
            found = doc.get("milestones")
            if not isinstance(found, list):
                raise FetchError("milestone_shape")
            if len(found) == 0:
                return None, "no_milestone"
            if len(found) != 1 or doc.get("cursor"):
                return None, "multiple_milestones"
            m = found[0]
            if not isinstance(m, dict):
                raise FetchError("milestone_shape")
            if m.get("type") != "esports_match":
                return None, "no_milestone"
            milestones.append(validate_milestone(m))
        if len({m["id"] for m in milestones}) != 1:
            return None, "multiple_milestones"
        if any(not set(tickers) <= set(m["related_event_tickers"]) for m in milestones):
            return None, "ticker_not_related"
        if any(m != milestones[0] for m in milestones):
            raise FetchError("milestone_changed")
        return milestones[0], None
    except (FetchError, ValueError, KeyError, TypeError):
        return None, "fetch_failed"


def put_json(store, key, doc):
    raw = dumps(doc)
    if len(raw) > MAX_METADATA:
        raise ValueError("metadata_too_large")
    return store.put_immutable(
        key,
        io.BytesIO(raw),
        StoredIdentity(hashlib.sha256(raw).hexdigest(), len(raw)),
        content_type="application/json",
    )


def prefix_valid(prefix):
    normalize_key(prefix)
    if not re.fullmatch(
        r"gamestate/source=kalshi/(?:event=[0-9a-f]{64}/)?date=\d{4}-\d\d-\d\d/milestone=[A-Za-z0-9_.:-]+/fetch=\d{8}T\d{6}\.\d{6}Z",
        prefix,
    ):
        raise ValueError("invalid_fetch_prefix")
    parts = prefix.split("/")
    datetime.strptime(parts[-3].removeprefix("date="), "%Y-%m-%d")
    datetime.strptime(parts[-1].removeprefix("fetch="), "%Y%m%dT%H%M%S.%fZ")
    return prefix


def event_identity(value):
    if type(value) is not str or not re.fullmatch(r"event:d1:[0-9a-f]{64}", value):
        raise ValueError("event_identity")
    return value


def read_receipt(store, prefix):
    prefix_valid(prefix)
    with store.open(prefix + "/receipt.json", max_bytes=MAX_METADATA) as handle:
        raw = handle.read(MAX_METADATA + 1)
        if len(raw) > MAX_METADATA or handle.read(1):
            raise ValueError("receipt_size")
    r = loads(raw)
    event_keyed = "/event=" in prefix
    fields = RECEIPT_FIELDS | ({"event_id"} if event_keyed else set())
    if not isinstance(r, dict) or set(r) != fields:
        raise ValueError("receipt_fields")
    if event_keyed and f"/event={event_identity(r['event_id']).split(':')[-1]}/" not in prefix:
        raise ValueError("receipt_event_identity")
    if (
        type(r["receipt_version"]) is not int
        or r["receipt_version"] != (2 if event_keyed else 1)
        or r["source"] != "kalshi"
        or type(r["script_version"]) is not int
        or r["script_version"] != 1
    ):
        raise ValueError("receipt_version")
    if (
        r["status"] not in ("complete", "incomplete")
        or f"/milestone={identifier(r['milestone_id'])}/" not in prefix
    ):
        raise ValueError("receipt_identity")
    ids = r["bundle_ids"]
    if (
        not isinstance(ids, list)
        or not ids
        or len(ids) > MAX_ITEMS
        or ids != sorted(set(ids))
    ):
        raise ValueError("receipt_bundles")
    for value in ids:
        identifier(value)
    for field in ("fetch_started_ns", "fetch_ended_ns", "request_count"):
        if type(r[field]) is not int or r[field] < 0:
            raise ValueError("receipt_counts")
    if r["fetch_ended_ns"] < r["fetch_started_ns"]:
        raise ValueError("receipt_times")
    stamp = datetime.fromtimestamp(
        r["fetch_started_ns"] // 1_000_000_000, timezone.utc
    ).strftime("%Y%m%dT%H%M%S")
    stamp += f".{r['fetch_started_ns'] % 1_000_000_000 // 1000:06d}Z"
    if not prefix.endswith("fetch=" + stamp):
        raise ValueError("receipt_prefix")
    if not isinstance(r["logical"], dict) or set(r["logical"]) != {
        "sha256",
        "byte_length",
        "line_count",
    }:
        raise ValueError("logical_fields")
    if not isinstance(r["stored"], dict) or set(r["stored"]) != {
        "sha256",
        "byte_length",
    }:
        raise ValueError("stored_fields")
    logical, stored = (
        LogicalIdentity.from_record(r["logical"]),
        StoredIdentity.from_record(r["stored"]),
    )
    if (
        logical.byte_length > MAX_RAW
        or stored.byte_length > MAX_RAW
        or logical.line_count != r["request_count"]
    ):
        raise ValueError("receipt_bounds")
    for field in ("provider_checksum", "provider_checksum_algorithm"):
        if not isinstance(r[field], str) or not r[field] or len(r[field]) > 256:
            raise ValueError("receipt_checksum")
    return r


def read_records(store, prefix):
    r = read_receipt(store, prefix)
    logical, stored = (
        LogicalIdentity.from_record(r["logical"]),
        StoredIdentity.from_record(r["stored"]),
    )
    expected = ObjectExpectation(
        prefix + "/responses.ndjson.zst",
        stored,
        r["provider_checksum"],
        r["provider_checksum_algorithm"],
        "application/x-ndjson",
        "zstd",
    )
    store.verify_metadata(expected)

    def iterator():
        with tempfile.TemporaryFile() as decoded:
            with store.open_verified(expected) as source:
                decode_stream(
                    source,
                    decoded,
                    expected_logical=logical,
                    expected_stored=stored,
                    max_decoded_bytes=MAX_RAW,
                )
            decoded.seek(0)
            count = 0
            while line := decoded.readline(MAX_BODY * 6 + 16384):
                if not line.endswith(b"\n"):
                    raise ValueError("record_size")
                record = loads(line)
                body_bytes(record)
                if record["seq"] != count:
                    raise ValueError("record_sequence")
                count += 1
                yield record
            if count != r["request_count"]:
                raise ValueError("request_count")

    return r, iterator()


def existing_complete(store, milestone_id, *, event_id=None):
    identifier(milestone_id)
    if event_id is not None:
        event_identity(event_id)
    count = 0
    root = "gamestate/source=kalshi/" + (f"event={event_id.split(':')[-1]}/" if event_id else "")
    for key in store.list_keys(root):
        count += 1
        if count > 100000:
            raise ValueError("listing_limit")
        if f"/milestone={milestone_id}/" not in key or not key.endswith(
            "/receipt.json"
        ):
            continue
        try:
            receipt, rows = read_records(store, key.removesuffix("/receipt.json"))
            timeline = derive(receipt, rows)
            if any(
                i["code"]
                in {
                    "response_shape",
                    "live_data_missing",
                    "related_event_missing",
                    "milestone_changed",
                }
                for i in timeline["inconsistencies"]
            ):
                continue
        except (ValueError, KeyError, TypeError, VerificationFailure):
            continue
        if receipt["status"] == "complete":
            return True
    return False


def archive_fetch(store, m, bundle_ids, records, started_ns, status, *, event_id=None):
    validate_milestone(m)
    dt = datetime.fromtimestamp(started_ns // 1_000_000_000, timezone.utc)
    stamp = dt.strftime("%Y%m%dT%H%M%S") + f".{started_ns % 1_000_000_000 // 1000:06d}Z"
    date = datetime.fromtimestamp(
        timestamp(m["start_date"]) // 1_000_000_000, timezone.utc
    ).strftime("%Y-%m-%d")
    prefix = prefix_valid(
        "gamestate/source=kalshi/"
        + (f"event={event_identity(event_id).split(':')[-1]}/" if event_id else "")
        + f"date={date}/milestone={m['id']}/fetch={stamp}"
    )
    if any(store.list_keys(prefix + "/")):
        raise ValueError("fetch_prefix_exists")
    with tempfile.TemporaryFile() as raw, tempfile.TemporaryFile() as frame:
        count = 0
        for seq, record in enumerate(records):
            record = dict(record, seq=seq)
            body_bytes(record)
            raw.write(dumps(record))
            count += 1
            if raw.tell() > MAX_RAW:
                raise ValueError("raw_size")
        raw.seek(0)
        result = encode_stream(raw, frame)
        frame.seek(0)
        metadata = store.put_immutable(
            prefix + "/responses.ndjson.zst",
            frame,
            result.stored,
            content_type="application/x-ndjson",
            content_encoding="zstd",
        )
        store.verify_metadata(
            ObjectExpectation(
                metadata.key,
                result.stored,
                metadata.provider_checksum,
                metadata.provider_checksum_algorithm,
                "application/x-ndjson",
                "zstd",
            )
        )
    receipt = dict(
        receipt_version=2 if event_id else 1,
        source="kalshi",
        milestone_id=m["id"],
        bundle_ids=sorted(set(bundle_ids)),
        script_version=1,
        fetch_started_ns=started_ns,
        fetch_ended_ns=max(time.time_ns(), started_ns),
        status=status,
        logical=result.logical.as_record(),
        stored=result.stored.as_record(),
        request_count=count,
        provider_checksum=metadata.provider_checksum,
        provider_checksum_algorithm=metadata.provider_checksum_algorithm,
    )
    if event_id:
        receipt["event_id"] = event_id
    put_json(store, prefix + "/receipt.json", receipt)
    return prefix


def timeline_key(prefix):
    return f"{prefix}/timeline.v{DERIVATION_VERSION + int('/event=' in prefix)}.json"


def regenerate(store, prefix, output=None):
    receipt, records = read_records(store, prefix)
    timeline = derive(receipt, records)
    if "event_id" in receipt:
        timeline.update(event_id=receipt["event_id"], derivation_version=DERIVATION_VERSION + 1)
    if output is None:
        # Versioned so that a derivation fix is republished beside the raw
        # object under the next version; an identical rerun proves the object.
        put_json(store, timeline_key(prefix), timeline)
    else:
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)
        write_json_durable(output, timeline)
    return timeline


def derive(receipt, records):
    """Vendor adapter: successful archived responses, never caller memory."""
    milestones: list[dict[str, Any]] = []
    live, events = None, {}
    issues = []

    def issue(code, index=None):
        value = {"code": code, "map_index": index}
        if value not in issues:
            issues.append(value)

    for r in records:
        raw = body_bytes(r)
        if r["error"] or r["status"] is None or not 200 <= r["status"] < 300:
            continue
        try:
            doc = loads(raw)
            path = urlsplit(r["url"]).path
            if path.endswith("/milestones"):
                milestones.extend(
                    m
                    for m in doc["milestones"]
                    if m.get("id") == receipt["milestone_id"]
                )
            elif "/live_data/milestone/" in path:
                validate_response(r["url"], doc, receipt["milestone_id"])
                value = doc["live_data"]
                if value["milestone_id"] != receipt["milestone_id"]:
                    raise ValueError()
                live = value["details"]
            elif "/events/" in path:
                validate_response(r["url"], doc, receipt["milestone_id"])
                event = doc["event"]
                events[event["event_ticker"]] = event
        except (KeyError, TypeError, ValueError):
            issue("response_shape")
    if not milestones:
        raise ValueError("archived_milestone_missing")
    m = validate_milestone(milestones[0])
    if any(item != m for item in milestones):
        issue("milestone_changed")
    details = m["details"]
    for ticker in m["related_event_tickers"]:
        if ticker not in events:
            issue("related_event_missing")
    live = live if isinstance(live, dict) else {}
    if not live:
        issue("live_data_missing")

    def ns(value):
        if value is None:
            return None
        try:
            return timestamp(value)
        except ValueError:
            issue("invalid_time")
            return None

    def winner(event, index):
        markets = event.get("markets", [])
        if any(x.get("result") not in ("yes", "no") for x in markets):
            issue("unsettled_map_market" if index else "unsettled_series_market", index)
        yes = [x for x in markets if x.get("result") == "yes"]
        if len(yes) != 1:
            issue(
                ("unsettled_map_market" if index else "unsettled_series_market")
                if not yes
                else "multiple_winner_markets",
                index,
            )
            return None
        value = yes[0]
        return {
            "ticker": value.get("ticker"),
            "yes_sub_title": value.get("yes_sub_title"),
            "source_field": "markets[result=yes]",
        }

    stats: dict[str, dict[int, dict[str, Any]]] = {"home": {}, "away": {}}
    periods = set()
    for side in ("home", "away"):
        values = live.get(side + "_stats", [])
        scores = live.get(side + "_periods", {})
        if (
            not isinstance(values, list)
            or not isinstance(scores, dict)
            or len(values) > 100
            or len(scores) > 100
        ):
            issue("period_shape")
            continue
        for name in scores:
            if re.fullmatch(r"period_[1-9]\d?", name):
                periods.add(int(name[7:]))
            else:
                issue("period_shape")
        for value in values:
            if (
                not isinstance(value, dict)
                or not isinstance(value.get("period"), str)
                or not re.fullmatch(r"period_[1-9]\d?", value["period"])
                or not isinstance(value.get("stats"), dict)
            ):
                issue("period_shape")
                continue
            index = int(value["period"][7:])
            periods.add(index)
            if index in stats[side]:
                issue("duplicate_period", index)
                stats[side][index] = {}
            else:
                stats[side][index] = value["stats"]
    map_events: dict[int, dict[str, Any]] = {}
    for ticker in sorted(events):
        event = events[ticker]
        # Vendor metadata, not event names, tickers, or scheduled times.
        scope = event.get("product_metadata", {}).get("competition_scope", "")
        match = (
            re.fullmatch(r"Map ([1-9]\d?) Winner", scope)
            if isinstance(scope, str)
            else None
        )
        if match:
            index = int(match[1])
            if index in map_events:
                issue("map_event_shape", index)
                map_events[index] = {}
            else:
                map_events[index] = event
        elif ticker not in m["primary_event_tickers"] and scope != "Total Maps":
            issue("unsupported_event_scope")
    if periods != set(map_events):
        issue("map_count_difference")

    def agreed(field, index, code):
        home = stats["home"].get(index, {}).get(field)
        away = stats["away"].get(index, {}).get(field)
        if home != away:
            issue(code, index)
            return None
        return home

    def crosscheck(event, win, index):
        if win is None:
            return
        yes = next(x for x in event["markets"] if x.get("ticker") == win["ticker"])
        competitor = yes.get("custom_strike", {}).get("esports_competitor")
        home, away = (
            details.get("home_competitor_id"),
            details.get("away_competitor_id"),
        )
        if not home or not away or home == away or competitor not in (home, away):
            issue("winner_crosscheck_unavailable", index)
            return
        if index is None:
            h, a = live.get("home_score"), live.get("away_score")
            if (
                not isinstance(h, (int, float))
                or not isinstance(a, (int, float))
                or isinstance(h, bool)
                or isinstance(a, bool)
                or h == a
            ):
                issue("winner_crosscheck_unavailable", index)
                return
            expected = home if h > a else away
        else:
            h, a = (
                stats["home"].get(index, {}).get("winner"),
                stats["away"].get(index, {}).get("winner"),
            )
            if (h, a) not in ((1, 0), (0, 1)):
                issue("winner_crosscheck_unavailable", index)
                return
            expected = home if h == 1 else away
        if expected != competitor:
            issue("winner_disagreement", index)

    maps = []
    for index in sorted(periods | set(map_events)):
        event = map_events.get(index, {})
        win = winner(event, index) if event else None
        crosscheck(event, win, index)
        if not event:
            issue("map_event_missing", index)
        duration = agreed("map_duration_seconds", index, "duration_disagreement")
        if duration is not None and (
            type(duration) not in (int, float) or duration < 0
        ):
            duration = None
            issue("duration_invalid", index)
        markets = event.get("markets", [])
        close_values = {x.get("close_time") for x in markets}
        settlement_values = {x.get("settlement_ts") for x in markets}
        if len(close_values) > 1:
            issue("market_close_disagreement", index)
        close = ns(next(iter(close_values))) if len(close_values) == 1 else None
        settlement = (
            ns(next(iter(settlement_values))) if len(settlement_values) == 1 else None
        )
        if len(settlement_values) > 1:
            issue("market_settlement_disagreement", index)
        maps.append(
            {
                "index": index,
                "winner_market": win,
                "forfeit": {
                    "value": agreed("map_forfeit", index, "forfeit_disagreement"),
                    "source_field": f"home_stats/away_stats[period=period_{index}].stats.map_forfeit",
                },
                "duration_s": {
                    "value": duration,
                    "source_field": f"home_stats/away_stats[period=period_{index}].stats.map_duration_seconds",
                },
                "scores": {
                    **{
                        side: {
                            "value": live.get(side + "_periods", {}).get(
                                f"period_{index}"
                            ),
                            "source_field": f"{side}_periods.period_{index}",
                        }
                        for side in ("home", "away")
                    },
                    **{
                        side + "_stats": {
                            "value": stats[side].get(index, {}),
                            "source_field": f"{side}_stats[period=period_{index}].stats",
                        }
                        for side in ("home", "away")
                    },
                },
                "close_ns": close,
                "settlement_ns": settlement,
                "derived_start_ns": close - int(Decimal(str(duration)) * 1_000_000_000)
                if close is not None and duration is not None
                else None,
                "time_basis": {
                    "end": "kalshi_market_close",
                    "start": "close_minus_duration",
                },
            }
        )
    series_ticker = details.get("main_game_event_ticker")
    series_source = "details.main_game_event_ticker"
    if series_ticker is None and len(m["primary_event_tickers"]) == 1:
        series_ticker = m["primary_event_tickers"][0]
        series_source = "primary_event_tickers"
    primary = events.get(series_ticker, {}) if isinstance(series_ticker, str) else {}
    if not primary:
        issue("series_event_missing")
    series_winner = winner(primary, None) if primary else None
    crosscheck(primary, series_winner, None)
    return {
        "derivation_version": DERIVATION_VERSION,
        "milestone_id": m["id"],
        "bundle_ids": receipt["bundle_ids"],
        "game": {"value": details.get("game"), "source_field": "details.game"},
        "league": {"value": details.get("league"), "source_field": "details.league"},
        "tournament": {
            "value": details.get("tournament_name"),
            "source_field": "details.tournament_name",
        },
        "scheduled_start_ns": ns(m["start_date"]),
        "match_end_ns": ns(m.get("end_date")),
        "status": {"value": details.get("status"), "source_field": "details.status"},
        "series": {
            "event_ticker": {"value": series_ticker, "source_field": series_source},
            "winner_market": series_winner,
            "score": {
                "value": {k: live.get(k) for k in ("home_score", "away_score")},
                "source_field": "live_data.details",
            },
        },
        "maps": maps,
        "inconsistencies": sorted(
            issues, key=lambda x: (x["map_index"] or 0, x["code"])
        ),
    }


def run(client, store, base, bundles, *, skip_existing=True, event_id=None):
    if event_id is not None:
        event_identity(event_id)
    report: dict[str, Any] = {
        "report_version": 1,
        "bundles_considered": len(set(bundles)),
        "bundles_mapped": 0,
        "bundles_unmapped": {},
        "milestones_fetched": 0,
        "milestones_skipped": 0,
        "milestones_incomplete": 0,
        "requests": 0,
        "retries": 0,
        "bundles": [],
        "fetches": [],
        "failures": [],
    }
    grouped: dict[str, Any] = {}
    if len(set(bundles)) > MAX_ITEMS:
        raise ValueError("bundle_limit")
    for bundle in sorted(set(bundles)):
        start = len(client.records)
        try:
            tickers = bundle_tickers(client, base, bundle)
            m, reason = map_bundle(client, tickers)
        except (FetchError, KeyError, ValueError, TypeError):
            m, reason = None, "fetch_failed"
        report["bundles"].append(
            {
                "bundle_id": bundle,
                "milestone_id": m["id"] if m else None,
                "reason": reason,
            }
        )
        if m:
            report["bundles_mapped"] += 1
            group = grouped.setdefault(
                m["id"], {"milestone": m, "bundles": [], "ranges": []}
            )
            group["bundles"].append(bundle)
            group["ranges"].append((start, len(client.records)))
        else:
            report["bundles_unmapped"][reason] = (
                report["bundles_unmapped"].get(reason, 0) + 1
            )
    for mid, group in sorted(grouped.items()):
        try:
            if skip_existing and existing_complete(store, mid, event_id=event_id):
                report["milestones_skipped"] += 1
                continue
            start = len(client.records)
            m = group["milestone"]
            complete = True
            errors = []
            urls = [KALSHI + "/live_data/milestone/" + quote(mid, safe="")]
            urls += [
                KALSHI + "/events/" + quote(t, safe="") + "?with_nested_markets=true"
                for t in sorted(m["related_event_tickers"])
            ]
            for url in urls:
                try:
                    validate_response(url, client.get(url), mid)
                except (FetchError, ValueError, KeyError, TypeError) as error:
                    complete = False
                    errors.append(
                        {
                            "url": url,
                            "reason": str(error)
                            if isinstance(error, FetchError)
                            else "vendor_shape",
                        }
                    )
            ranges = group["ranges"] + [(start, len(client.records))]
            # Only Kalshi responses are game-state evidence; the Universe
            # mapping requests stay in report.json.
            kalshi = [
                i
                for first, last in ranges
                for i in range(first, last)
                if is_kalshi(client.records[i]["url"])
            ]
            started = client.records[kalshi[0]]["requested_at_ns"]
            rows = (client.records[i] for i in kalshi)
            prefix = archive_fetch(
                store,
                m,
                group["bundles"],
                rows,
                started,
                "complete" if complete else "incomplete",
                event_id=event_id,
            )
            report["milestones_fetched"] += 1
            report["milestones_incomplete"] += int(not complete)
            report["fetches"].append(
                {
                    "milestone_id": mid,
                    "prefix": prefix,
                    "status": "complete" if complete else "incomplete",
                    "errors": errors,
                }
            )
        except (ValueError, ObjectStoreError, OSError, KeyError, TypeError) as error:
            reason = (
                "object_store_failed"
                if isinstance(error, ObjectStoreError)
                else "io_failed"
                if isinstance(error, OSError)
                else "archive_schema_failed"
            )
            report["failures"].append({"milestone_id": mid, "reason": reason})
            continue
        try:
            regenerate(store, prefix)
        except (ValueError, ObjectStoreError, OSError, KeyError, TypeError):
            report["failures"].append(
                {"milestone_id": mid, "reason": "timeline_failed"}
            )
    report["requests"], report["retries"] = len(client.records), client.retries
    report["request_attempts"] = [
        {
            k: r[k]
            for k in (
                "seq",
                "url",
                "status",
                "error",
                "requested_at_ns",
                "received_at_ns",
            )
        }
        for r in client.records
    ]
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", action="append", default=[])
    parser.add_argument("--activation-start")
    parser.add_argument("--activation-end")
    parser.add_argument(
        "--output-root", type=Path, default=Path("kalshi-game-state-output")
    )
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--skip-existing", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--regenerate", metavar="FETCH_PREFIX")
    parser.add_argument("--timeline-output", type=Path)
    args = parser.parse_args(argv)
    if args.regenerate and (
        args.bundle or args.activation_start or args.activation_end
    ):
        parser.error("--regenerate cannot be combined with input selection")
    if not args.regenerate and (
        bool(args.bundle) == bool(args.activation_start and args.activation_end)
    ):
        parser.error("provide --bundle or both activation bounds")
    if args.bundle and (args.activation_start or args.activation_end):
        parser.error("bundle and activation inputs are exclusive")
    if args.timeline_output and not args.regenerate:
        parser.error("--timeline-output requires --regenerate")
    args.output_root.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.setdefault("ARCHIVE_ROOT", str(args.output_root / "archive"))
    store = build_store([], environ=env)
    if args.regenerate:
        regenerate(store, args.regenerate, args.timeline_output)
        return 0
    base = os.environ.get("UNIVERSE_BASE_URL", "")
    parsed = urlsplit(base)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        parser.error(
            "set UNIVERSE_BASE_URL to a public API base URL without credentials"
        )
    try:
        for bundle in args.bundle:
            identifier(bundle)
        if args.activation_start and timestamp(args.activation_start) >= timestamp(
            args.activation_end
        ):
            parser.error("activation-start must precede activation-end")
    except ValueError:
        parser.error("invalid bundle id or RFC 3339 activation bound")
    c = Client()
    try:
        bundles = args.bundle
        if args.activation_start:
            bundles = sorted(
                {
                    identifier(r.get("bundle_id"))
                    for r in pages(
                        c,
                        base,
                        "/v1/selections",
                        {
                            "activation_start": args.activation_start,
                            "activation_end": args.activation_end,
                            "venue": "kalshi",
                        },
                    )
                }
            )
    except (FetchError, ValueError, KeyError, TypeError) as error:
        bundles = None
        detail = str(error) if isinstance(error, FetchError) else "selection_shape"
    try:
        if bundles is not None:
            report = run(c, store, base, bundles, skip_existing=args.skip_existing)
        else:
            report = run(c, store, base, [])
            report["failures"].append(
                {
                    "milestone_id": None,
                    "reason": "selection_discovery_failed",
                    "detail": detail,
                }
            )
    finally:
        c.records.close()
    destination = args.report or args.output_root / "report.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    write_json_durable(destination, report)
    return int(
        bool(
            report["failures"]
            or report["milestones_incomplete"]
            or report["bundles_unmapped"].get("fetch_failed")
        )
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, ObjectStoreError):
        print(
            "game-state pull failed: invalid input, storage, or schema; no response bodies logged",
            file=sys.stderr,
        )
        sys.exit(1)
