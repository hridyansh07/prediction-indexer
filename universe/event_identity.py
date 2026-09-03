"""Deterministic umbrella-event identity resolution for Event Universe."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from typing import Any

from universe.market_projection import canonical_market_id


EVENT_IDENTITY_VERSION = 1


class EventIdentityError(ValueError):
    """Raised when native aliases contradict an existing umbrella identity."""


def canonical_event_id(
    *,
    sport: str,
    game: str | None,
    topology: str | None,
    participant_keys: list[str],
    activation_date: str,
    ordinal: int,
) -> str:
    """Return the v1 canonical ID for one allocated event occurrence."""

    preimage = {
        "activation_date": activation_date,
        "game": game,
        "identity_version": EVENT_IDENTITY_VERSION,
        "ordinal": ordinal,
        "participant_keys": sorted(participant_keys),
        "sport": sport,
        "topology": topology,
    }
    digest = hashlib.sha256(_canonical_json(preimage).encode("utf-8")).hexdigest()
    return f"event:d1:{digest}"


def resolve_market_projection(
    connection: sqlite3.Connection,
    projection: dict[str, Any],
) -> dict[str, Any]:
    """Resolve proposal IDs against durable native-event aliases.

    The caller must hold the write transaction that will persist the resolved
    projection. That makes ordinal allocation and alias insertion one atomic
    operation.
    """

    projection = copy.deepcopy(projection)
    event_id_map: dict[str, str] = {}
    alias_bindings: dict[tuple[str, str], str] = {}
    next_ordinals: dict[tuple[Any, ...], int] = {}

    events = sorted(
        projection["events"],
        key=lambda event: (event["activation_at"], event["source_bundle_id"]),
    )
    for event in events:
        proposal_id = event["event_id"]
        refs = [_split_event_ref(ref) for ref in event["event_refs"]]
        resolved_ids: set[str] = set()
        for alias in refs:
            locally_resolved = alias_bindings.get(alias)
            if locally_resolved is not None:
                resolved_ids.add(locally_resolved)
                continue
            row = connection.execute(
                """
                SELECT event_id
                FROM venue_events
                WHERE venue = ? AND venue_event_id = ?
                """,
                alias,
            ).fetchone()
            if row is not None:
                resolved_ids.add(str(row["event_id"]))

        if len(resolved_ids) > 1:
            aliases = ", ".join(f"{venue}:{native_id}" for venue, native_id in refs)
            raise EventIdentityError(
                f"native event aliases resolve to multiple umbrella events: {aliases}"
            )

        if resolved_ids:
            event_id = next(iter(resolved_ids))
            stored = connection.execute(
                "SELECT * FROM umbrella_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if stored is None:
                raise EventIdentityError(f"umbrella event {event_id} is missing")
            _require_compatible_identity(stored, event)
            event["identity_version"] = int(stored["identity_version"])
            event["identity_activation_date"] = str(
                stored["identity_activation_date"]
            )
            event["identity_ordinal"] = int(stored["identity_ordinal"])
        else:
            participant_keys_json = _canonical_json(sorted(event["participant_keys"]))
            activation_date = _activation_date(event["activation_at"])
            base_key = (
                EVENT_IDENTITY_VERSION,
                event["sport"],
                event["game"],
                event["topology"],
                participant_keys_json,
                activation_date,
            )
            ordinal = next_ordinals.get(base_key)
            if ordinal is None:
                row = connection.execute(
                    """
                    SELECT MAX(identity_ordinal) AS max_ordinal
                    FROM umbrella_events
                    WHERE identity_version = ?
                      AND sport = ?
                      AND game IS ?
                      AND topology IS ?
                      AND participant_keys_json = ?
                      AND identity_activation_date = ?
                    """,
                    base_key,
                ).fetchone()
                ordinal = 0 if row["max_ordinal"] is None else int(row["max_ordinal"]) + 1
            next_ordinals[base_key] = ordinal + 1
            event_id = canonical_event_id(
                sport=event["sport"],
                game=event["game"],
                topology=event["topology"],
                participant_keys=event["participant_keys"],
                activation_date=activation_date,
                ordinal=ordinal,
            )
            event["identity_version"] = EVENT_IDENTITY_VERSION
            event["identity_activation_date"] = activation_date
            event["identity_ordinal"] = ordinal

        event_id_map[proposal_id] = event_id
        event["event_id"] = event_id
        for alias in refs:
            previous = alias_bindings.setdefault(alias, event_id)
            if previous != event_id:
                raise EventIdentityError(
                    f"native event alias {alias[0]}:{alias[1]} resolves to "
                    "multiple umbrella events"
                )

    market_id_map: dict[tuple[str, int, int], str] = {}
    for market in projection["markets"]:
        previous_event_id = market["event_id"]
        market["event_id"] = event_id_map[previous_event_id]
        previous_market_id = market["market_id"]
        market["market_id"] = canonical_market_id(
            event_id=market["event_id"],
            canonical_class=market["canonical_class"],
            market_type=market["market_type"],
            scope=market["scope"],
            parameters=market["parameters"],
            market_template_version=market["market_template_version"],
            outcome_space_version=market["outcome_space_version"],
        )
        market_id_map[
            (
                previous_market_id,
                market["market_template_version"],
                market["outcome_space_version"],
            )
        ] = market["market_id"]

    for venue_event in projection["venue_events"]:
        venue_event["event_id"] = event_id_map[venue_event["event_id"]]
    for decision in projection["decisions"]:
        decision["event_id"] = event_id_map[decision["event_id"]]
    for relation in projection["relations"]:
        relation["event_id"] = event_id_map[relation["event_id"]]

    for row in (*projection["venue_markets"], *projection["selected_markets"]):
        row["event_id"] = event_id_map[row["event_id"]]
        row["market_id"] = market_id_map[
            (
                row["market_id"],
                row["market_template_version"],
                row["outcome_space_version"],
            )
        ]

    return projection


def _require_compatible_identity(
    stored: sqlite3.Row,
    proposed: dict[str, Any],
) -> None:
    proposed_keys = _canonical_json(sorted(proposed["participant_keys"]))
    if (
        int(stored["identity_version"]) != EVENT_IDENTITY_VERSION
        or stored["sport"] != proposed["sport"]
        or stored["game"] != proposed["game"]
        or stored["topology"] != proposed["topology"]
        or stored["participant_keys_json"] != proposed_keys
    ):
        raise EventIdentityError(
            f"native aliases for {proposed['source_bundle_id']} contradict "
            f"umbrella event {stored['event_id']}"
        )


def _split_event_ref(event_ref: str) -> tuple[str, str]:
    venue, separator, native_id = event_ref.partition(":")
    if not separator or not venue or not native_id:
        raise EventIdentityError(f"invalid native event reference {event_ref!r}")
    return venue, native_id


def _activation_date(activation_at: str) -> str:
    date, separator, _ = activation_at.partition("T")
    if not separator:
        raise EventIdentityError(f"invalid activation timestamp {activation_at!r}")
    return date


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
