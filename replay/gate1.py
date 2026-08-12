"""Gate 1: prove that a dataset carries the irreversible facts replay needs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from replay.catalog import (
    ASSERTED,
    CAPTURED,
    canonical_sha256 as _canonical_sha256,
    has_fee_terms,
)
from replay.envelope import Envelope, EnvelopeError, parse_envelope
from replay.events import GAME_STATE_MARKERS
from replay.lanes import PARTITION_PREFIXES, lane_of
from replay.stream import (
    ByteStreamer,
    DirectoryByteStreamer,
    StreamError,
    build_input_manifest,
    iter_ndjson_lines,
    read_object,
)

#: The sidecar that commits a segment. Duplicated from `splices.common.segment`
#: rather than imported: replay reads datasets it did not write, from adapters
#: that may not have the capture half installed at all.
SEAL_SUFFIX = ".seal.json"

PASS = "PASS"
FAIL = "FAIL"

#: Reported but not blocking. Used where a fact is worth measuring on every
#: dataset but is not a precondition for the analysis actually being run — an
#: advisory check that blocked would force a capture change to satisfy a
#: question nobody is asking yet.
ADVISORY = "ADVISORY"


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    evidence: dict[str, Any]
    requirement: str

    def as_record(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "status": self.status,
            "requirement": self.requirement,
            "evidence": self.evidence,
        }


@dataclass(frozen=True)
class Gate1Report:
    input_manifest: dict[str, Any]
    records: int
    checks: tuple[Check, ...]
    observations: dict[str, Any]

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(
            check.status != FAIL for check in self.checks
        )

    def as_record(self) -> dict[str, Any]:
        body = {
            "gate": "GATE_1_FUTURE_ANSWERABLE_TAPE",
            "gate_version": 1,
            "passed": self.passed,
            "input": self.input_manifest,
            "records": self.records,
            "checks": [check.as_record() for check in self.checks],
            "observations": self.observations,
        }
        canonical = json.dumps(
            body, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        return {
            **body,
            "report_sha256": hashlib.sha256(b"replay-gate1-v1\0" + canonical).hexdigest(),
        }


class Gate1Auditor:
    """A streaming audit; payloads are counted and discarded, never accumulated."""

    def __init__(self, *, allow_asserted_records: bool = False) -> None:
        self.allow_asserted_records = bool(allow_asserted_records)

    def audit(self, streamer: ByteStreamer) -> Gate1Report:
        manifest = build_input_manifest(streamer)
        state = _AuditState(allow_asserted=self.allow_asserted_records)
        parse_failures: list[str] = []

        # Target records are NDJSON but they are not tape: they carry no
        # envelope and no lane partition, so feeding them to `parse_envelope`
        # would report a parse failure, and feeding them to `lane_of` would
        # raise. Routed by key before either is reached.
        record_keys = tuple(
            key for key in streamer.object_keys() if target_record_object(key)
        )
        tape_keys = tuple(
            key
            for key in streamer.object_keys()
            if key.endswith(".ndjson") and key not in set(record_keys)
        )

        for line in iter_ndjson_lines(streamer, keys=tape_keys):
            try:
                envelope = parse_envelope(line.data)
            except EnvelopeError as error:
                parse_failures.append(f"{line.object_key}:{line.line_number}: {error}")
                continue
            state.observe(line.object_key, envelope)

        auxiliary_failures: list[str] = []
        for line in iter_ndjson_lines(streamer, keys=record_keys):
            try:
                state.observe_target_record(json.loads(line.data))
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                auxiliary_failures.append(
                    f"{line.object_key}:{line.line_number}: {error}"
                )

        for key in streamer.object_keys():
            if key.endswith(".ndjson"):
                continue
            if "/metadata/" in f"/{key}" and key.endswith(".json"):
                try:
                    state.observe_metadata(key, json.loads(read_object(streamer, key)))
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                    auxiliary_failures.append(f"{key}: {error}")
            elif key.endswith(SEAL_SUFFIX):
                try:
                    state.observe_seal(key, json.loads(read_object(streamer, key)))
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                    auxiliary_failures.append(f"{key}: {error}")
            elif key.endswith("coverage.json"):
                try:
                    state.observe_coverage(json.loads(read_object(streamer, key)))
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
                    auxiliary_failures.append(f"{key}: {error}")

        # The manifest already content-addressed every object, so verifying a
        # seal costs a dictionary lookup rather than a second pass over the
        # bytes. That equality is why the seal's digest is a plain sha256 and not
        # domain-separated like the rest of this system's hashes.
        state.verify_seals({item.key: item for item in manifest.objects})

        checks = state.checks(
            parse_failures=parse_failures,
            auxiliary_failures=auxiliary_failures,
            object_count=len(manifest.objects),
        )
        return Gate1Report(
            input_manifest=manifest.as_record(),
            records=state.records,
            checks=tuple(checks),
            observations=state.observations(),
        )


class _AuditState:
    def __init__(self, *, allow_asserted: bool = False) -> None:
        #: Whether records fetched after the fact satisfy the metadata checks.
        #: Off by default: an asserted record is a belief that the venue's terms
        #: are immutable, not proof of what the targeter used, and the two must
        #: not be silently interchangeable.
        self.allow_asserted = bool(allow_asserted)
        self.records = 0
        self.v2_records = 0
        self.frames = 0
        self.venues: Counter[str] = Counter()
        self.streams: Counter[str] = Counter()
        self.lane_last_delivery: dict[str, int] = {}
        self.delivery_breaks: list[str] = []
        self.epoch_last_local: dict[tuple[str, str], int] = {}
        self.local_breaks: list[str] = []
        self.connection_opened: list[dict[str, Any]] = []
        self.connection_closed: Counter[tuple[str, str]] = Counter()
        self.connection_started: Counter[tuple[str, str]] = Counter()
        self.metadata_digests_seen: set[str] = set()
        self.metadata_digests_referenced: set[str] = set()
        self.metadata_targets = 0
        self.metadata_invalid: list[str] = []
        self.rules_records = 0
        self.fee_records = 0
        self.target_records = 0
        self.target_record_provenance: Counter[str] = Counter()
        #: Assets by the strength of the evidence behind them. An asset may be
        #: in both — a market observed live and later re-fetched by a backfill —
        #: and when it is, the captured evidence is what counts. Subtracting
        #: `asserted_assets` wholesale let one backfill row nullify a real
        #: sighting, reporting an asset as uncovered in the same breath as
        #: reporting captured evidence for it.
        self.captured_assets: set[tuple[str, str]] = set()
        self.asserted_assets: set[tuple[str, str]] = set()
        self.rules_assets: set[tuple[str, str]] = set()
        self.rules_assets_asserted: set[tuple[str, str]] = set()
        self.fee_assets: set[tuple[str, str]] = set()
        self.fee_assets_asserted: set[tuple[str, str]] = set()
        self.coverage_sightings: dict[tuple[str, str], dict[str, Any]] = {}
        self.pm_books = 0
        self.pm_changes = 0
        self.pm_hashes: set[str] = set()
        self.pm_trades = 0
        self.pm_trade_fields_complete = 0
        self.pm_snapshot_books = 0
        self.pm_snapshot_hashes: set[str] = set()
        self.limitless_books = 0
        self.limitless_books_complete = 0
        self.seals: dict[str, dict[str, Any]] = {}
        self.seal_failures: list[str] = []
        self.segments_seen: set[str] = set()
        #: Sealed segments that hold no records. Evidence, not failure — see
        #: `verify_seals`.
        self.empty_segments: list[str] = []
        self.reference_events = 0
        self.reference_events_stamped = 0
        self.game_state_events = 0
        self.game_state_events_stamped = 0
        self.game_states_unkeyed = 0
        self.market_created_events = 0
        self.market_resolved_events = 0
        self.payload_errors: list[str] = []

    def observe(self, object_key: str, envelope: Envelope) -> None:
        self.records += 1
        self.v2_records += envelope.envelope_version == 2
        self.venues[envelope.venue] += 1
        self.streams[envelope.stream] += 1
        self.segments_seen.add(object_key)
        lane = lane_of(object_key)
        previous_delivery = self.lane_last_delivery.get(lane)
        if previous_delivery is not None and envelope.delivery_index != previous_delivery + 1:
            self.delivery_breaks.append(
                f"{lane}:{previous_delivery}->{envelope.delivery_index}"
            )
        self.lane_last_delivery[lane] = envelope.delivery_index

        epoch_key = (lane, envelope.connection_epoch)
        previous_local = self.epoch_last_local.get(epoch_key)
        if previous_local is not None and envelope.local_counter != previous_local + 1:
            self.local_breaks.append(
                f"{lane}/{envelope.connection_epoch}:{previous_local}->{envelope.local_counter}"
            )
        self.epoch_last_local[epoch_key] = envelope.local_counter

        if envelope.kind == "venue_frame":
            self.frames += 1
            self._observe_frame(envelope)
        elif envelope.stream == "process":
            self._observe_control(lane, envelope)

    def _observe_control(self, lane: str, envelope: Envelope) -> None:
        try:
            payload = envelope.payload_json()
        except json.JSONDecodeError as error:
            self.payload_errors.append(f"{envelope.record_id}: control JSON: {error}")
            return
        if not isinstance(payload, dict):
            self.payload_errors.append(f"{envelope.record_id}: control is not an object")
            return
        event = payload.get("event")
        key = (lane, envelope.connection_epoch)
        if event == "connection_opened":
            self.connection_opened.append(
                {**payload, "_capture_venue": envelope.venue, "_capture_lane": lane}
            )
            self.connection_started[key] += 1
            digest = payload.get("target_metadata_digest")
            if isinstance(digest, str) and digest not in ("", "broadcast"):
                self.metadata_digests_referenced.add(digest)
        elif event == "target_metadata_changed":
            digest = payload.get("to_metadata_digest")
            if isinstance(digest, str) and digest:
                self.metadata_digests_referenced.add(digest)
        elif event == "connection_closed":
            self.connection_closed[key] += 1

    def _observe_frame(self, envelope: Envelope) -> None:
        if envelope.raw_payload in ("", "PONG"):
            return
        try:
            payload = envelope.payload_json()
        except json.JSONDecodeError:
            # Raw text frames are valid transport evidence; they are not a broken
            # envelope and do not make the capture gate fail.
            return
        if envelope.stream == "public_snapshot" and isinstance(payload, dict):
            if _book_shape(payload):
                self.pm_snapshot_books += 1
            value = payload.get("hash")
            if isinstance(value, str) and value:
                self.pm_snapshot_hashes.add(value)
            return
        if envelope.stream == "reference_event":
            # Two different feeds share this stream, and they answer different
            # questions: a price tick is the underlying a ladder settles against,
            # a game state is an exogenous world event. Counted together, sports
            # frames inflated the price total while never being able to satisfy
            # it — they carry no top-level `timestamp` at all.
            if isinstance(payload, dict) and any(
                marker in payload for marker in GAME_STATE_MARKERS
            ):
                self.game_state_events += 1
                state = payload.get("eventState")
                if isinstance(state, dict) and state.get("updatedAt"):
                    self.game_state_events_stamped += 1
                if payload.get("gameId") is None and payload.get("metadataGameId") is None:
                    self.game_states_unkeyed += 1
                return
            self.reference_events += 1
            if isinstance(payload, dict) and payload.get("timestamp") is not None:
                self.reference_events_stamped += 1
            return

        events = payload if isinstance(payload, list) else [payload]
        for event in events:
            if not isinstance(event, dict):
                continue
            if envelope.venue == "limitless":
                self._observe_limitless(event)
            elif envelope.venue == "polymarket":
                self._observe_polymarket(event)

    def _observe_polymarket(self, event: dict[str, Any]) -> None:
        name = str(event.get("event_type") or event.get("event") or "")
        normalized = name.lower()
        if normalized in {"newmarketevent", "new_market", "market_created"}:
            self.market_created_events += 1
        if normalized in {"marketresolved", "market_resolved", "resolved"}:
            self.market_resolved_events += 1
        if name == "book":
            self.pm_books += _book_shape(event)
            value = event.get("hash")
            if isinstance(value, str) and value:
                self.pm_hashes.add(value)
        elif name == "price_change":
            self.pm_changes += 1
            for change in event.get("price_changes") or []:
                if isinstance(change, dict):
                    value = change.get("hash")
                    if isinstance(value, str) and value:
                        self.pm_hashes.add(value)
        elif name == "last_trade_price":
            self.pm_trades += 1
            required = ("asset_id", "price", "size", "side", "timestamp", "transaction_hash")
            self.pm_trade_fields_complete += all(event.get(field) is not None for field in required)

    def _observe_limitless(self, event: dict[str, Any]) -> None:
        name = str(event.get("event") or "")
        normalized = name.lower()
        if normalized in {"marketcreated", "market_created"}:
            self.market_created_events += 1
        if normalized in {"marketresolved", "market_resolved"}:
            self.market_resolved_events += 1
        if name != "orderbookUpdate":
            return
        self.limitless_books += 1
        data = event.get("data")
        if not isinstance(data, dict):
            return
        book = data.get("orderbook")
        complete = (
            isinstance(book, dict)
            and isinstance(book.get("bids"), list)
            and isinstance(book.get("asks"), list)
            and data.get("marketSlug") is not None
            and data.get("version") is not None
            and data.get("timestamp") is not None
        )
        self.limitless_books_complete += complete

    def observe_metadata(self, key: str, document: Any) -> None:
        if not isinstance(document, dict):
            raise ValueError("metadata snapshot is not an object")
        digest = document.get("metadata_digest")
        venue = document.get("venue")
        targets = document.get("targets")
        if not isinstance(digest, str) or not isinstance(venue, str) or not isinstance(targets, list):
            raise ValueError("metadata snapshot lacks digest, venue, or targets")
        canonical = json.dumps(
            {"version": 1, "venue": venue, "targets": targets},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8")
        computed = hashlib.sha256(canonical).hexdigest()
        if computed != digest:
            self.metadata_invalid.append(f"{key}: declared {digest}, computed {computed}")
        self.metadata_digests_seen.add(digest)
        for target in targets:
            if not isinstance(target, dict):
                self.metadata_invalid.append(f"{key}: target is not an object")
                continue
            self.metadata_targets += 1
            evidence = target.get("resolution")
            record = evidence.get("catalogue_record") if isinstance(evidence, dict) else None
            if not isinstance(record, dict):
                continue
            record_hash = evidence.get("catalogue_record_hash")
            encoded = json.dumps(
                record,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
            if record_hash != hashlib.sha256(encoded).hexdigest():
                self.metadata_invalid.append(f"{key}: catalogue record hash mismatch")
            if any(record.get(field) for field in ("description", "rules_primary", "rules", "question")):
                self.rules_records += 1
            if has_fee_terms(record):
                self.fee_records += 1

    def observe_target_record(self, document: Any) -> None:
        """One venue record for one market a run subscribed to.

        Feeds the same counters as `observe_metadata`, deliberately. A v1 dataset
        carries the record inside `resolution.catalogue_record`; a v2 dataset
        carries it here. The question the check asks — is the contemporaneous
        venue record present for the markets we watched — is identical, and a
        dataset holding both should satisfy it once rather than being made to
        choose a source.
        """
        if not isinstance(document, dict):
            raise ValueError("target record is not an object")
        venue = document.get("venue")
        target_id = document.get("target_id")
        record = document.get("record")
        provenance = document.get("provenance")
        if not isinstance(venue, str) or not isinstance(target_id, str):
            raise ValueError("target record lacks venue or target_id")
        if not isinstance(record, dict):
            raise ValueError(f"target record {target_id} carries no record object")
        if provenance not in (CAPTURED, ASSERTED):
            raise ValueError(
                f"target record {target_id} has unknown provenance {provenance!r}"
            )
        declared = document.get("record_sha256")
        if declared != _canonical_sha256(record):
            self.metadata_invalid.append(f"{target_id}: record hash disagrees with bytes")

        self.target_records += 1
        self.target_record_provenance[str(provenance)] += 1
        captured = provenance != ASSERTED
        has_rules = any(
            record.get(field)
            for field in ("description", "rules_primary", "rules", "question")
        )
        has_fees = has_fee_terms(record)

        # Counted per asset rather than per row. Rows repeat every run by
        # design, so counting rows would let one market observed 144 times a day
        # look like 144 markets — and the checks these feed are then intersected
        # with the subscribed set, because "the markets we watched" is what the
        # requirement says and a record for an asset nobody subscribed to
        # answers no question about this tape.
        observed_at = document.get("observed_at")
        created_at = record.get("createdAt") or record.get("created_time")
        for asset in document.get("subscription_ids") or ():
            key = (venue, str(asset))
            (self.captured_assets if captured else self.asserted_assets).add(key)
            if has_rules:
                (self.rules_assets if captured else self.rules_assets_asserted).add(key)
            if has_fees:
                (self.fee_assets if captured else self.fee_assets_asserted).add(key)
            # Discovery coverage without the live tree. The earliest run naming
            # an asset is when we first saw it, and the venue's own creation
            # timestamp travels in the record — `createdAt` on Polymarket Gamma,
            # `created_time` on Kalshi — so the lag this check measures survives
            # the move off `live/coverage.json` unchanged.
            if isinstance(observed_at, str):
                self._record_sighting(
                    venue,
                    str(asset),
                    first_seen_at=observed_at,
                    created_at=created_at if isinstance(created_at, str) else None,
                )

    def _record_sighting(
        self,
        venue: str,
        asset_id: str,
        *,
        first_seen_at: str | None,
        created_at: str | None,
    ) -> None:
        """Keep the earliest sighting of an asset, whichever source supplied it.

        Both `live/coverage.json` and the target records describe the same fact,
        and a dataset may carry both. Two things this must not do, and both were
        done before it existed:

        - **Let arrival order decide.** The auxiliary-object loop runs after the
          target-record loop, so an unconditional assignment meant the ledger
          always won — including when it held a strictly later, less informative
          sighting, which could flip a passing dataset to failing by *adding*
          evidence.
        - **Compare timestamps as strings.** The two sources do not spell an
          instant the same way: `analysis.storage.utc_now` emits
          `...+00:00` and `targeter.v2.domain.isoformat` emits `...Z`, and
          `isoformat` further drops the fractional part on a whole second.
          Lexically `'Z' > '.'` and `'+' < '.'`, so string comparison picks the
          later row in both directions. `targeter/coverage.py:131` already got
          this right by parsing; this is the same rule.
        """
        key = (venue, asset_id)
        existing = self.coverage_sightings.get(key)
        instant = _instant(first_seen_at)
        if existing is not None:
            previous = _instant(existing.get("first_seen_at"))
            # An unparseable instant never displaces a parseable one, and a
            # later one never displaces an earlier one.
            if previous is not None and (instant is None or previous <= instant):
                if created_at is not None and existing.get("created_at") is None:
                    existing["created_at"] = created_at
                return
        self.coverage_sightings[key] = {
            "venue": venue,
            "asset_id": asset_id,
            "first_seen_at": first_seen_at,
            "created_at": (
                created_at
                if created_at is not None
                else (existing or {}).get("created_at")
            ),
        }

    def observe_seal(self, key: str, document: Any) -> None:
        """One sealed segment's commit marker."""
        if not isinstance(document, dict):
            raise ValueError("seal is not an object")
        for field in ("data_file", "byte_length", "line_count", "sha256"):
            if field not in document:
                raise ValueError(f"seal lacks {field}")
        # Seals sit beside their data file, so the segment's object key is the
        # seal's own key with the sidecar name swapped for `data_file`.
        directory = key.rsplit("/", 1)[0] if "/" in key else ""
        segment = f"{directory}/{document['data_file']}" if directory else document["data_file"]
        self.seals[segment] = document

    def verify_seals(self, objects: dict[str, Any]) -> None:
        """Every segment sealed, and every seal agreeing with the bytes.

        An `.ndjson` without a valid seal is not evidence: §3 makes the seal the
        commit marker, so a segment missing one is a file whose writer never
        finished claiming it — indistinguishable, without this check, from one
        that did.

        **A sealed segment holding no records is not a missing segment.** It is
        the positive evidence that the lane was alive and the venue was silent
        for that window, which is the normal state of a low-volume venue —
        Limitless produced 30 such windows out of 82 in one real capture. This
        walks the union of the segments records were observed in and the
        segments a seal names, and consults the input manifest to tell the two
        apart: a key the manifest does not carry is genuinely absent, while a
        key it does carry is present and simply empty.

        The earlier loop walked `segments_seen` alone, which is populated per
        parsed record (`observe`). An empty segment therefore fell out of it and
        was reported as absent — and, less visibly, was never length- or
        digest-checked at all. Both are fixed here: an empty segment is verified
        against its seal like any other and then recorded as evidence.
        """
        for segment in sorted(self.segments_seen | set(self.seals)):
            seal = self.seals.get(segment)
            if seal is None:
                self.seal_failures.append(f"{segment}: no seal")
                continue
            identity = objects.get(segment)
            if identity is None:
                self.seal_failures.append(f"{segment}: sealed but the segment is absent")
                continue
            if identity.size != seal["byte_length"]:
                self.seal_failures.append(
                    f"{segment}: byte_length {seal['byte_length']} != {identity.size}"
                )
            if identity.sha256 != seal["sha256"]:
                self.seal_failures.append(f"{segment}: sha256 disagrees with the bytes")
            if segment not in self.segments_seen:
                self.empty_segments.append(segment)

    def observe_coverage(self, document: Any) -> None:
        if not isinstance(document, dict) or not isinstance(document.get("sightings"), list):
            raise ValueError("coverage ledger lacks sightings")
        for item in document["sightings"]:
            if not isinstance(item, dict):
                continue
            venue = item.get("venue")
            asset_id = item.get("asset_id")
            if not isinstance(venue, str) or not isinstance(asset_id, str):
                continue
            first_seen_at = item.get("first_seen_at")
            created_at = item.get("created_at")
            # A live sighting is an observation the targeter made at the time,
            # so it is captured evidence and admissible under strict mode.
            self.captured_assets.add((venue, asset_id))
            self._record_sighting(
                venue,
                asset_id,
                first_seen_at=first_seen_at if isinstance(first_seen_at, str) else None,
                created_at=created_at if isinstance(created_at, str) else None,
            )

    def checks(
        self,
        *,
        parse_failures: list[str],
        auxiliary_failures: list[str],
        object_count: int,
    ) -> list[Check]:
        checks: list[Check] = []

        def add(name: str, passed: bool, requirement: str, **evidence: Any) -> None:
            checks.append(Check(name, PASS if passed else FAIL, evidence, requirement))

        add(
            "byte_and_envelope_integrity",
            object_count > 0
            and self.records > 0
            and not parse_failures
            and not auxiliary_failures
            and not self.payload_errors,
            "Every object is stable, every NDJSON line is complete, and every envelope is closed.",
            objects=object_count,
            records=self.records,
            parse_failures=parse_failures[:10],
            auxiliary_failures=auxiliary_failures[:10],
            control_payload_failures=self.payload_errors[:10],
        )
        add(
            "deterministic_capture_order",
            not self.delivery_breaks and not self.local_breaks,
            "Delivery indices are dense per lane and local counters are dense per epoch.",
            lanes=len(self.lane_last_delivery),
            delivery_breaks=self.delivery_breaks[:10],
            local_counter_breaks=self.local_breaks[:10],
        )
        scopes = [item.get("clock_scope") for item in self.connection_opened]
        scopes_complete = all(
            isinstance(scope, dict)
            and scope.get("scope_id")
            and isinstance(scope.get("comparable_across_processes"), bool)
            for scope in scopes
        )
        add(
            "receive_clock_provenance",
            self.records == self.v2_records and bool(scopes) and scopes_complete,
            "Every record has wall and monotonic receive clocks, with an explicit comparison scope.",
            v2_records=self.v2_records,
            total_records=self.records,
            scoped_connections=sum(isinstance(scope, dict) for scope in scopes),
        )
        opened_complete = all(
            item.get("target_digest") is not None
            and isinstance(item.get("asset_ids"), list)
            and item.get("delivers_deltas") is not None
            and item.get("fsync_interval_seconds") is not None
            for item in self.connection_opened
        )
        add(
            "subscription_provenance",
            bool(self.connection_opened) and opened_complete,
            "Every connection states its subscribed assets, target identity, and fidelity.",
            connections=len(self.connection_opened),
            complete=sum(
                item.get("target_digest") is not None and isinstance(item.get("asset_ids"), list)
                for item in self.connection_opened
            ),
        )
        # Which assets the tape says we watched. Every target-record check is
        # scored against this rather than against the rows on their own: a
        # record for an asset nobody subscribed to answers no question about
        # this tape, and counting it made the checks satisfiable by a run bundle
        # that had nothing to do with the capture beside it.
        subscribed = {
            (str(item.get("_capture_venue") or ""), str(asset))
            for item in self.connection_opened
            if item.get("target_digest") != "broadcast"
            for asset in item.get("asset_ids") or []
        }

        def watched(captured: set, asserted: set) -> int:
            admitted = captured | (asserted if self.allow_asserted else set())
            return len(subscribed & admitted)

        rules_watched = watched(self.rules_assets, self.rules_assets_asserted)
        # v1 datasets carry the record inside a metadata snapshot, which is not
        # keyed by asset, so those rows are counted as they always were.
        rules_admitted = rules_watched + self.rules_records
        add(
            "market_rules_and_metadata",
            self.metadata_targets + rules_watched > 0
            and rules_admitted > 0
            and not self.metadata_invalid,
            "The contemporaneous raw venue record is present and content-addressed for the markets we watched.",
            metadata_targets=self.metadata_targets,
            rules_records=rules_admitted,
            subscribed_with_rules=rules_watched,
            subscribed_with_rules_captured=len(subscribed & self.rules_assets),
            subscribed_with_rules_asserted=len(subscribed & self.rules_assets_asserted),
            snapshot_rules_records=self.rules_records,
            allow_asserted_records=self.allow_asserted,
            target_records=self.target_records,
            provenance=dict(sorted(self.target_record_provenance.items())),
            invalid=self.metadata_invalid[:10],
        )
        # Split out of `market_rules_and_metadata`, which conflated two
        # questions. Whether the tape's referenced metadata snapshots resolve is
        # about the tape and its snapshots; whether the venue's record survived
        # is about the catalogue. Left joined, an archive-only dataset — which
        # carries no snapshots, because `live/` is not archived — failed a tape
        # that was fine, for lacking something that was never in it.
        missing_metadata = sorted(
            self.metadata_digests_referenced - self.metadata_digests_seen
        )
        checks.append(
            Check(
                "metadata_snapshot_references",
                FAIL
                if self.metadata_digests_seen and missing_metadata
                else PASS
                if self.metadata_digests_seen
                else ADVISORY,
                {
                    "referenced_snapshots": len(self.metadata_digests_referenced),
                    "present_snapshots": len(self.metadata_digests_seen),
                    "missing_references": missing_metadata[:10],
                    "note": (
                        None
                        if self.metadata_digests_seen
                        else "dataset carries no metadata snapshots; nothing to resolve against"
                    ),
                },
                "Every metadata digest the tape references resolves to a snapshot the dataset carries.",
            )
        )
        # An asserted sighting proves the market's terms, not when we first saw
        # it, so it counts only when the run allows it. Excluded is the
        # asserted-*only* set: an asset with captured evidence keeps it, because
        # subtracting every asserted asset let one backfill row nullify a real
        # sighting the same report was affirming.
        admissible = set(self.coverage_sightings)
        if not self.allow_asserted:
            admissible -= self.asserted_assets - self.captured_assets
        uncovered = sorted(subscribed - admissible)
        created_known = sum(
            self.coverage_sightings[key].get("created_at") is not None
            for key in subscribed & admissible
        )
        add(
            "discovery_coverage",
            bool(subscribed) and not uncovered and created_known > 0,
            "First sighting is retained for every subscribed asset and creation time is measured where published.",
            subscribed_assets=len(subscribed),
            covered_assets=len(subscribed) - len(uncovered),
            with_created_at=created_known,
            # The three buckets, named rather than summarized: an asset backed
            # by a live observation, one backed only by a later re-fetch, and
            # one backed by nothing. Only the third is a failure.
            covered_by_captured=len(subscribed & self.captured_assets),
            covered_by_asserted_only=len(
                subscribed & (self.asserted_assets - self.captured_assets)
            ),
            allow_asserted_records=self.allow_asserted,
            uncovered=uncovered[:10],
        )
        add(
            "polymarket_book_chain",
            self.pm_books > 0 and self.pm_changes > 0 and bool(self.pm_hashes),
            "Polymarket supplies initial books, raw changes, and venue state hashes.",
            books=self.pm_books,
            changes=self.pm_changes,
            unique_hashes=len(self.pm_hashes),
        )
        overlap = self.pm_hashes & self.pm_snapshot_hashes
        add(
            "polymarket_recovery_anchors",
            self.pm_snapshot_books > 0 and bool(overlap),
            "Independent full-book polls exist and at least one hash locates an anchor in the stream.",
            snapshot_books=self.pm_snapshot_books,
            snapshot_hashes=len(self.pm_snapshot_hashes),
            stream_hash_matches=len(overlap),
        )
        add(
            "every_segment_is_sealed",
            not self.seal_failures and bool(self.segments_seen),
            "Every segment carries a seal whose length and digest match its bytes.",
            segments=len(self.segments_seen),
            seals=len(self.seals),
            # A silent window is reported rather than hidden: a lane that is
            # alive and producing nothing looks identical to a dead one in every
            # other number here.
            empty_segments=len(self.empty_segments),
            empty_examples=sorted(self.empty_segments)[:10],
            failures=self.seal_failures[:10],
        )
        add(
            "limitless_full_books",
            self.limitless_books > 0
            and self.limitless_books == self.limitless_books_complete,
            "Every observed Limitless orderbook update is a timestamped, versioned full book.",
            books=self.limitless_books,
            complete=self.limitless_books_complete,
        )
        add(
            "trade_and_fill_observability",
            self.pm_trades > 0 and self.pm_trades == self.pm_trade_fields_complete,
            "Trades carry asset, price, size, side, venue time, and transaction identity.",
            trades=self.pm_trades,
            complete=self.pm_trade_fields_complete,
        )
        add(
            "reference_price_observability",
            self.reference_events > 0 and self.reference_events_stamped > 0,
            "A timestamped underlying reference feed is captured with the same receive clocks.",
            events=self.reference_events,
            timestamped=self.reference_events_stamped,
        )
        add(
            "game_event_observability",
            self.game_state_events > 0 and self.game_states_unkeyed == 0,
            "Game states are captured and every one carries an identifier we key on.",
            events=self.game_state_events,
            with_venue_update_time=self.game_state_events_stamped,
            without_identifier=self.game_states_unkeyed,
        )
        # Advisory: market creation and resolution arrive only on a Polymarket
        # subscription sending `custom_feature_enabled`, which this capture does
        # not set, and resolution additionally needs a window long enough to
        # contain one. Neither is a precondition for the exogenous-clock work, so
        # it is measured and reported rather than allowed to block the chain.
        checks.append(
            Check(
                "market_lifecycle_observability",
                ADVISORY,
                {
                    "created_events": self.market_created_events,
                    "resolved_events": self.market_resolved_events,
                    "note": (
                        "requires custom_feature_enabled on the market subscription "
                        "and a window containing a resolution"
                    ),
                },
                "Raw market creation and final-resolution events are both present.",
            )
        )
        fees_watched = watched(self.fee_assets, self.fee_assets_asserted)
        fees_admitted = fees_watched + self.fee_records
        add(
            "fee_model_evidence",
            fees_admitted > 0,
            "Raw market metadata contains the contemporaneous fee schedule needed for net economics.",
            metadata_records_with_fees=fees_admitted,
            subscribed_with_fees_captured=len(subscribed & self.fee_assets),
            subscribed_with_fees_asserted=len(subscribed & self.fee_assets_asserted),
            snapshot_records_with_fees=self.fee_records,
            allow_asserted_records=self.allow_asserted,
        )
        unclosed = sorted(
            f"{lane}/{epoch}"
            for lane, epoch in self.connection_started
            if self.connection_closed[(lane, epoch)] < self.connection_started[(lane, epoch)]
        )
        add(
            "closed_capture_fixture",
            bool(self.connection_started) and not unclosed,
            "Every fixture connection has an explicit terminal lifecycle record.",
            started=sum(self.connection_started.values()),
            closed=sum(self.connection_closed.values()),
            unclosed=unclosed[:10],
        )
        return checks

    def observations(self) -> dict[str, Any]:
        return {
            "venues": dict(sorted(self.venues.items())),
            "streams": dict(sorted(self.streams.items())),
            "frames": self.frames,
            "connections": len(self.connection_opened),
            # Broken out because `reference_event` carries two unrelated feeds
            # and the stream count alone cannot tell them apart.
            "reference_feeds": {
                "price_ticks": self.reference_events,
                "price_ticks_with_venue_time": self.reference_events_stamped,
                "game_states": self.game_state_events,
                "game_states_with_venue_update_time": self.game_state_events_stamped,
            },
        }


def _instant(value: Any) -> datetime | None:
    """Parse a timestamp for comparison, or `None` if it cannot be compared.

    Deliberately not a string compare. The two sources that write sightings
    disagree on spelling — `+00:00` from `analysis.storage.utc_now`, `Z` from
    `targeter.v2.domain.isoformat`, and the latter drops the fractional part on
    a whole second — and every one of those differences sorts the wrong way
    lexically.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _book_shape(value: dict[str, Any]) -> bool:
    return isinstance(value.get("bids"), list) and isinstance(value.get("asks"), list)


def generation_metadata_object(key: str) -> bool:
    """One Targeter v2 generation's content-addressed metadata snapshot.

    v1 wrote these to a flat `<live>/metadata/<venue>/<digest>.json`. v2 writes
    the same document beside the target files each generation publishes, at
    `<live>/targeter-v2/generations/<run_id>/metadata/<venue>/<digest>.json`,
    because a generation commits to its snapshot by name. Only the location
    moved — `observe_metadata` validates both identically — so admitting the v1
    shape alone reports a v2 capture as carrying no catalogue evidence at all,
    with every digest the tape references landing in `missing_references`.

    **Every generation is admitted, not only the published one.** A capture
    window spans as many generations as it contains republishes, and a
    `connection_opened` record naming a since-superseded generation's digest
    still needs that snapshot present to resolve.
    """
    parts = key.split("/")
    if parts[:1] == ["live"]:
        parts = parts[1:]
    return (
        len(parts) == 6
        and parts[0] == "targeter-v2"
        and parts[1] == "generations"
        and parts[3] == "metadata"
        and parts[5].endswith(".json")
    )


def target_record_object(key: str) -> bool:
    """One run's venue records for the markets it subscribed to.

    Admitted from a run bundle even though the rest of the bundle is not: the
    selection report is the record of *why* markets were chosen, and the
    catalogue artifacts describe markets that were never subscribed. Neither can
    interpret a tape. This one can, and it is the only part of a run that a gate
    reads.

    `.ndjson` only, never `.ndjson.zst`, for the reason
    `test_a_compressed_segment_is_not_mistaken_for_a_readable_one` states:
    `iter_ndjson_lines` skips a key it cannot frame with a bare `continue`, so a
    compressed artifact admitted here would produce a vacuously empty check
    rather than an error. Excluded, a zstd run fails loudly for having no
    records at all — which is the failure that gets noticed.
    """
    name = key.rsplit("/", 1)[-1]
    return (
        name.startswith("target_records_")
        and name.endswith(".ndjson")
        and "/" in key
    )


def gate1_object(key: str) -> bool:
    """The immutable capture bundle inputs that can satisfy this gate.

    Sealed segments and their sidecars only. A `.ndjson.open` is a segment the
    writer still owns, so it is excluded here for the same reason the ingester
    excludes it: an unsealed file has no committed length and no digest, and
    admitting one would let a dataset's identity change under a reader that had
    already hashed it.
    """
    partitioned = key.startswith("spool/") or any(
        key.startswith(prefix) for prefix in PARTITION_PREFIXES
    )
    return (
        (partitioned and (key.endswith(".ndjson") or key.endswith(".seal.json")))
        or (
            (key.startswith("live/metadata/") or key.startswith("metadata/"))
            and key.endswith(".json")
        )
        or generation_metadata_object(key)
        or target_record_object(key)
        or key in {"live/coverage.json", "coverage.json"}
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset_root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--allow-asserted-records",
        action="store_true",
        help=(
            "Count venue records fetched after the fact toward the metadata "
            "checks. Off by default: an asserted record is a belief that the "
            "venue's terms are immutable, not proof of what the targeter used."
        ),
    )
    arguments = parser.parse_args()
    try:
        report = Gate1Auditor(
            allow_asserted_records=arguments.allow_asserted_records
        ).audit(DirectoryByteStreamer(arguments.dataset_root, include=gate1_object))
    except StreamError as error:
        parser.error(str(error))
    encoded = json.dumps(report.as_record(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if arguments.output is not None:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
