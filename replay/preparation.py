"""Pinned historical scope preparation. No source is consulted by the loader."""

import fcntl
import hashlib
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

from replay.streams.protocol import (
    array,
    choice,
    decode,
    freeze,
    key,
    obj,
    pin,
    require,
    text,
    uint,
)
from replay.supervisor import fsync_directory, write_json_durable

MAX_BYTES = 8 * 1024 * 1024
MAX_OCCURRENCES = 128
MAX_MARKETS = 4096
MAX_BOOKS = 8192


def encoded(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def sha(value):
    pin({"derivative_address": value, "receipt_sha256": value})
    return value


def source_pin(value):
    obj(value, "manifest_key manifest_sha256 report_key report_sha256")
    for field in ("manifest_key", "report_key"):
        text(value[field])
    for field in ("manifest_sha256", "report_sha256"):
        sha(value[field])
    return value


def validate_config(config):
    from targeter.v2.manifest import manifest_run_id

    require(len(encoded(config)) <= MAX_BYTES, "preparation config too large")
    obj(
        config,
        "version pins start_ns end_ns lower_bound bundle_id market_namespace probe_markets occurrences authorities",
    )
    require(type(config["version"]) is int and config["version"] == 1)
    require(config["market_namespace"] == "targeter_target_id")
    text(config["bundle_id"])
    pins = array(config["pins"])
    require(0 < len(pins) <= 4096)
    require(len({pin(p) for p in pins}) == len(pins))
    start, end = uint(config["start_ns"]), uint(config["end_ns"])
    require(start < end)
    choice(config["lower_bound"], "clip expand_to_window_start require_window_boundary")
    probes = config["probe_markets"]
    if probes is not None:
        array(probes)
        require(0 < len(probes) <= MAX_MARKETS)
        require(all(text(p) for p in probes))
        require(
            probes == sorted(set(probes)), "probe markets must be sorted and unique"
        )
    occurrences = array(config["occurrences"])
    require(0 < len(occurrences) <= MAX_OCCURRENCES)
    cursor = start
    for occurrence in occurrences:
        obj(occurrence, "run_id start_ns end_ns source")
        text(occurrence["run_id"])
        source_pin(occurrence["source"])
        source = occurrence["source"]
        require(
            manifest_run_id(source["manifest_key"]) == occurrence["run_id"],
            "manifest run conflict",
        )
        prefix = source["manifest_key"].rsplit("/", 1)[0]
        require(
            source["report_key"]
            in {
                prefix + "/selection_report.json",
                prefix + "/selection_report.json.zst",
            },
            "report key conflict",
        )
        require(
            uint(occurrence["start_ns"]) == cursor,
            "explicit intervals must partition request",
        )
        cursor = uint(occurrence["end_ns"])
        require(uint(occurrence["start_ns"]) < cursor <= end)
    require(cursor == end, "incomplete explicit history")
    plans = array(config["authorities"])
    require(0 < len(plans) <= 3)
    seen = set()
    for plan in plans:
        obj(plan, "lane venue price_scale quantity_scale")
        choice(plan["venue"], "polymarket kalshi limitless")
        require(plan["venue"] not in seen, "duplicate venue authority")
        seen.add(plan["venue"])
        text(plan["lane"])
        uint(plan["price_scale"], 18)
        uint(plan["quantity_scale"], 18)
    return config


class SourceUnavailable(Exception):
    """Only absence or transport unavailability permits fallback."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class UniverseHTTP:
    """Bounded single-occurrence API; deliberately no current-market/history scan."""

    def __init__(self, base_url, *, timeout=10):
        parsed = urlsplit(base_url)
        require(
            parsed.scheme in {"http", "https"}
            and parsed.netloc
            and not parsed.username
            and not parsed.password
            and not parsed.query
            and not parsed.fragment
        )
        require(type(timeout) in (int, float) and 0 < timeout <= 60)
        self.base_url, self.timeout = base_url.rstrip("/"), timeout

    def __call__(self, occurrence, bundle_id):
        url = (
            self.base_url
            + "/v1/runs/"
            + quote(occurrence["run_id"], safe="")
            + "/selections/"
            + quote(bundle_id, safe="")
        )
        deadline = time.monotonic() + self.timeout
        try:
            with build_opener(ProxyHandler({}), _NoRedirect()).open(
                url, timeout=self.timeout
            ) as response:
                require(response.status == 200, "unexpected Universe response")
                payload = bytearray()
                while True:
                    if time.monotonic() >= deadline:
                        raise SourceUnavailable("Universe response deadline")
                    chunk = response.read1(min(65536, MAX_BYTES + 1 - len(payload)))
                    if not chunk:
                        break
                    payload.extend(chunk)
                    require(len(payload) <= MAX_BYTES, "Universe body budget")
                return decode(bytes(payload), MAX_BYTES)
        except HTTPError as error:
            if error.code == 404 or error.code in {502, 503, 504}:
                raise SourceUnavailable("Universe absent/unavailable") from error
            raise
        except (URLError, TimeoutError, ConnectionError) as error:
            raise SourceUnavailable("Universe unavailable") from error


def validate_detail(detail, occurrence, bundle_id):
    # Reuse Universe's closed historical-context validator, not its current-era
    # claim/market projection. Imports stay on the preparation side of replay.
    from targeter.v2.manifest import manifest_run_id, manifest_run_instant
    from targeter.v2.models import isoformat, parse_timestamp
    from universe.store import _occurrence

    obj(
        detail,
        "run_id generated_at bundle_id occurrence_kind continuity_selected continuity_disposition sport game topology activation_at capture_start_at retirement source origin context",
    )
    require(
        detail["run_id"] == occurrence["run_id"] and detail["bundle_id"] == bundle_id,
        "occurrence identity conflict",
    )
    require(source_pin(detail["source"]) == occurrence["source"], "source pin conflict")
    choice(detail["occurrence_kind"], "complete retained")
    require(type(detail["continuity_selected"]) is bool)
    if detail["continuity_disposition"] is not None:
        choice(detail["continuity_disposition"], "held_current_candidate retained")
    for field in ("generated_at", "activation_at", "capture_start_at"):
        parsed = parse_timestamp(detail[field])
        require(
            parsed is not None and isoformat(parsed) == detail[field],
            "invalid timestamp",
        )
    require(
        parse_timestamp(detail["generated_at"])
        == manifest_run_instant(detail["source"]["manifest_key"]),
        "run timestamp conflict",
    )
    origin = obj(
        detail["origin"],
        "run_id generated_at manifest_key manifest_sha256 report_key report_sha256",
    )
    text(origin["run_id"])
    parsed = parse_timestamp(origin["generated_at"])
    require(parsed is not None and isoformat(parsed) == origin["generated_at"])
    source_pin({f: origin[f] for f in detail["source"]})
    require(
        manifest_run_id(origin["manifest_key"]) == origin["run_id"]
        and manifest_run_instant(origin["manifest_key"]) == parsed,
        "origin identity conflict",
    )
    prefix = origin["manifest_key"].rsplit("/", 1)[0]
    require(
        origin["report_key"]
        in {prefix + "/selection_report.json", prefix + "/selection_report.json.zst"}
    )
    if detail["occurrence_kind"] == "complete":
        require(
            origin
            == {
                **detail["source"],
                "run_id": detail["run_id"],
                "generated_at": detail["generated_at"],
            },
            "complete origin conflict",
        )
    else:
        require(
            origin["run_id"] < detail["run_id"]
            and detail["continuity_selected"]
            and detail["continuity_disposition"] == "retained",
            "retained origin conflict",
        )
    retirement = detail["retirement"]
    if retirement is not None:
        obj(retirement, "retired_at disposition terminal_observed_at source")
        choice(retirement["disposition"], "all_markets_terminal terminal_clamp_elapsed")
        text(retirement["retired_at"])
        require(
            retirement["terminal_observed_at"]
            == (
                retirement["retired_at"]
                if retirement["disposition"] == "all_markets_terminal"
                else None
            )
        )
        for field in ("retired_at", "terminal_observed_at"):
            if retirement[field] is not None:
                parsed = parse_timestamp(retirement[field])
                require(parsed is not None and isoformat(parsed) == retirement[field])
        obj(
            retirement["source"],
            "run_id manifest_key manifest_sha256 report_key report_sha256",
        )
        text(retirement["source"]["run_id"])
        source_pin({f: retirement["source"][f] for f in detail["source"]})
        require(
            manifest_run_id(retirement["source"]["manifest_key"])
            == retirement["source"]["run_id"]
            and parse_timestamp(retirement["retired_at"])
            == manifest_run_instant(retirement["source"]["manifest_key"])
            and retirement["source"]["run_id"] > detail["run_id"],
            "retirement provenance conflict",
        )
    normalized = _occurrence(
        {
            **{
                f: detail[f]
                for f in (
                    "run_id",
                    "bundle_id",
                    "occurrence_kind",
                    "continuity_selected",
                    "continuity_disposition",
                    "context",
                )
            },
            "origin_run_id": origin["run_id"],
        },
        expected_run_id=occurrence["run_id"],
    )
    context = normalized["context"]
    require(len(context["markets"]) <= MAX_MARKETS)
    for field in ("sport", "game", "topology", "activation_at", "capture_start_at"):
        require(detail[field] == context[field], "context metadata conflict")
    require(context == detail["context"], "noncanonical historical context")
    return detail


def resolve_scope(config, occurrence, detail):
    context = detail["context"]
    markets = {m["target_id"]: m for m in context["markets"]}
    targets = {t["target_id"]: t for t in context["targets"]}
    probes = config["probe_markets"] or sorted(markets)
    require(set(probes) <= set(markets), "probe absent from pinned occurrence")
    members, required, owners = [], set(), {}
    for market_id in sorted(markets):
        market = markets[market_id]
        books = []
        target = targets.get(market_id)
        if target is not None:
            subscriptions = target["subscription_ids"]
            require(
                subscriptions and len(subscriptions) == len(set(subscriptions)),
                "missing/ambiguous native mapping",
            )
            venue = market["venue"]
            if venue == "kalshi":
                require(
                    subscriptions == [market_id.split(":", 1)[1]],
                    "Kalshi ticker conflict",
                )
            elif venue == "limitless":
                require(len(subscriptions) == 1, "ambiguous Limitless slug")
            else:
                require(
                    venue == "polymarket"
                    and all(s.isascii() and s.isdigit() for s in subscriptions),
                    "invalid Polymarket token mapping",
                )
            for subscription in subscriptions:
                for orientation in (
                    ("outcome", "complement") if venue == "kalshi" else ("outcome",)
                ):
                    native = (venue + ":" + subscription, orientation)
                    require(
                        native not in owners, "native book mapped to multiple markets"
                    )
                    owners[native] = market_id
                    if market_id in probes:
                        required.add(native)
                    books.append({"instrument": native[0], "orientation": native[1]})
        if market_id not in probes:
            continue
        members.append(
            {
                "market_id": market_id,
                "capture_selected": market["selected"],
                "mapping_status": "resolved" if books else "uncaptured_mapping_unknown",
                "books": sorted(
                    books, key=lambda b: (b["instrument"], b["orientation"])
                ),
            }
        )
    require(len(required) <= MAX_BOOKS)
    return {
        "start_ns": occurrence["start_ns"],
        "end_ns": occurrence["end_ns"],
        "run_id": occurrence["run_id"],
        "bundle_id": config["bundle_id"],
        "context_sha256": digest(context),
        "listed_market_ids": sorted(markets),
        "capture_selected_market_ids": sorted(targets),
        "members": members,
        "required_books": [
            {"instrument": i, "orientation": o} for i, o in sorted(required)
        ],
        "unresolved_market_ids": [m["market_id"] for m in members if not m["books"]],
    }


def build_snapshot(config, evidence):
    validate_config(config)
    array(evidence)
    require(len(encoded(evidence)) <= MAX_BYTES, "evidence byte budget")
    require(len(evidence) == len(config["occurrences"]))
    scopes = []
    seen = {}
    for occurrence, item in zip(config["occurrences"], evidence, strict=True):
        obj(item, "provider detail")
        choice(item["provider"], "universe targeter")
        detail = validate_detail(item["detail"], occurrence, config["bundle_id"])
        identity = digest(detail)
        require(
            seen.get(detail["run_id"], identity) == identity,
            "occurrence evidence changed",
        )
        seen[detail["run_id"]] = identity
        scopes.append(resolve_scope(config, occurrence, detail))
    required = {key(b) for s in scopes for b in s["required_books"]}
    require(len(required) <= MAX_BOOKS, "book plan budget")
    authorities = {p["venue"]: p for p in config["authorities"]}
    plans = []
    for instrument, orientation in sorted(required):
        venue = instrument.split(":", 1)[0]
        require(venue in authorities, "explicit source authority required")
        plans.append(
            {"instrument": instrument, "orientation": orientation, **authorities[venue]}
        )
    snapshot = {
        "version": 1,
        "config": config,
        "evidence": evidence,
        "scopes": scopes,
        "plans": plans,
        "membership_basis": "caller_pinned_expectations",
        "history_complete": False,
    }
    require(len(encoded(snapshot)) <= MAX_BYTES, "context snapshot too large")
    require(
        len((json.dumps(snapshot, sort_keys=True, allow_nan=False) + "\n").encode())
        <= MAX_BYTES,
        "serialized snapshot too large",
    )
    return snapshot


def load_snapshot(directory, *, expected_sha256=None):
    """Independently hash-check and re-resolve the closed snapshot; return immutable data."""
    root = Path(directory)
    with (root / "receipt.json").open("rb") as stream:
        receipt = decode(stream.read(4097), 4096)
    obj(receipt, "version snapshot_sha256 snapshot_byte_length config_sha256")
    require(type(receipt["version"]) is int and receipt["version"] == 1)
    sha(receipt["snapshot_sha256"])
    sha(receipt["config_sha256"])
    require(
        type(receipt["snapshot_byte_length"]) is int
        and 0 < receipt["snapshot_byte_length"] <= MAX_BYTES
    )
    with (root / "context.json").open("rb") as stream:
        payload = stream.read(MAX_BYTES + 1)
    require(
        len(payload) == receipt["snapshot_byte_length"]
        and hashlib.sha256(payload).hexdigest() == receipt["snapshot_sha256"],
        "snapshot identity mismatch",
    )
    if expected_sha256 is not None:
        require(receipt["snapshot_sha256"] == expected_sha256, "snapshot pin mismatch")
    snapshot = decode(payload, MAX_BYTES)
    obj(
        snapshot,
        "version config evidence scopes plans membership_basis history_complete",
    )
    require(
        encoded(snapshot)
        == encoded(build_snapshot(snapshot["config"], snapshot["evidence"])),
        "resolved snapshot conflict",
    )
    require(
        digest(snapshot["config"]) == receipt["config_sha256"],
        "config identity mismatch",
    )
    return freeze(snapshot)


def prepare(config, directory, *, universe, fallback=None):
    """Commit once, or load the exact previous result without any source lookup.

    Sources accept (occurrence, bundle_id), returning selection-detail or None.
    Only None/SourceUnavailable selects fallback; malformed evidence propagates.
    """
    config = decode(encoded(config), MAX_BYTES)
    validate_config(config)
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=True)
    fsync_directory(root.parent)
    with (root / ".lock").open("a+b") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if (root / "receipt.json").exists():
            snapshot = load_snapshot(root)
            # Compare before freezing: tuples/mapping proxies are intentionally
            # not the JSON configuration's mutable containers.
            with (root / "receipt.json").open("rb") as stream:
                receipt = decode(stream.read(4097), 4096)
            require(
                receipt["config_sha256"] == digest(config), "preparation config changed"
            )
            return snapshot
        require(
            not (root / "context.json").exists(),
            "uncommitted preparation; use a new directory",
        )
        evidence = []
        for occurrence in config["occurrences"]:
            provider = "universe"
            try:
                detail = universe(occurrence, config["bundle_id"])
            except SourceUnavailable:
                detail = None
            if detail is None:
                require(fallback is not None, "pinned occurrence unavailable")
                provider = "targeter"
                detail = fallback(occurrence, config["bundle_id"])
            require(detail is not None, "pinned occurrence unavailable")
            detail = decode(encoded(detail), MAX_BYTES)
            validate_detail(detail, occurrence, config["bundle_id"])
            evidence.append({"provider": provider, "detail": detail})
            require(len(encoded(evidence)) <= MAX_BYTES, "evidence byte budget")
        snapshot = build_snapshot(config, evidence)
        write_json_durable(root / "context.json", snapshot)
        payload = (root / "context.json").read_bytes()
        require(len(payload) <= MAX_BYTES, "serialized snapshot too large")
        write_json_durable(
            root / "receipt.json",
            {
                "version": 1,
                "snapshot_sha256": hashlib.sha256(payload).hexdigest(),
                "snapshot_byte_length": len(payload),
                "config_sha256": digest(config),
            },
        )
        return load_snapshot(root)
