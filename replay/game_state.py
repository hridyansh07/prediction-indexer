"""Closed, pinned prepared game input. This loader never contacts a source."""

import hashlib
import math
from pathlib import Path
import re

from replay.preparation import sha
from replay.strategy_sdk import plain
from replay.streams.protocol import decode, freeze, obj, require

MAX_BYTES = 1024 * 1024
SIDES = ("home", "away")
FIELDS = ("version event_id state reason source sport game market_types competitors "
          "scheduled_start_ns segment_kind segments match")


def natural(value, *, nullable=False):
    require(nullable and value is None or type(value) is int and 0 <= value <= 2**64 - 1,
            "game state unsigned integer")
    return value


def normalized_label(value):
    return " ".join(value.casefold().split())


def align(labels, participants):
    result = {}
    for side in SIDES:
        label = labels[side]
        matches = ([] if label is None else [i for i, p in enumerate(participants)
                                            if normalized_label(p) == normalized_label(label)])
        result[side] = {"label": label, "participant": matches[0] if len(matches) == 1 else None}
    if result["home"]["participant"] is not None and result["home"]["participant"] == result["away"]["participant"]:
        for side in SIDES:
            result[side]["participant"] = None
    return result


def context_header(context):
    context = plain(context)
    outcomes = context.get("outcomes", {})
    require(outcomes.get("provider") == "universe", "game state requires pinned event identity")
    document = outcomes["document"]
    event_id = document["event_id"]
    require(type(event_id) is str and re.fullmatch(r"event:d1:[0-9a-f]{64}", event_id) is not None,
            "game state event identity")
    evidence = [row["detail"]["context"] for row in context["evidence"]]
    require(bool(evidence), "game state context evidence")
    sport, game = evidence[0]["sport"], evidence[0]["game"]
    require(all((row["sport"], row["game"]) == (sport, game) for row in evidence),
            "game state context header conflict")
    return {"event_id": event_id, "sport": sport, "game": game,
            "market_types": sorted({row["market_type"] for row in document["markets"]})}, document["participants"]


def _label(value):
    require(value is None or type(value) is str and 0 < len(value) <= 4096, "game state label")


def score(value):
    obj(value, "home away")
    for side in SIDES:
        natural(value[side])
    return value


def validate(value, context):
    value = obj(value, FIELDS)
    pending = [value]
    while pending:
        item = pending.pop()
        if type(item) is dict:
            pending.extend(item.values())
        elif type(item) is list:
            pending.extend(item)
        elif type(item) is float:
            require(math.isfinite(item), "game state nonfinite number")
    require(type(value["version"]) is int and value["version"] == 1, "game state version")
    header, participants = context_header(context)
    require(all(value[k] == v for k, v in header.items()), "game state event/header binding")
    require(type(value["market_types"]) is list, "game state market types")
    competitors = obj(value["competitors"], "home away")
    for side in SIDES:
        row = obj(competitors[side], "label participant")
        _label(row["label"])
        require(row["participant"] is None or type(row["participant"]) is int, "game state participant")
    require(competitors == align({s: competitors[s]["label"] for s in SIDES}, participants),
            "game state competitor alignment")
    natural(value["scheduled_start_ns"], nullable=True)
    _label(value["segment_kind"])
    require(type(value["segments"]) is list, "game state segments")
    require(value["state"] in ("ok", "unavailable"), "game state availability")
    if value["source"] is not None:
        source = obj(value["source"], "name prefix timeline_sha256")
        require(all(type(source[k]) is str and bool(source[k]) for k in ("name", "prefix")),
                "game state source identity")
        _label(source["name"]); _label(source["prefix"]); sha(source["timeline_sha256"])
    if value["state"] == "unavailable":
        require(value["reason"] in ("no_source", "no_fetch", "incomplete") and not value["segments"]
                and value["match"] is None, "unavailable game state")
        return value
    require(value["reason"] is None and value["source"] is not None, "available game state source")
    previous_index, previous_end = 0, None
    for segment in value["segments"]:
        obj(segment, "index start_ns start_estimated end_ns settled_ns winner details")
        index = natural(segment["index"])
        require(index > previous_index, "game state segment order")
        start, end, settled = (natural(segment[k], nullable=True) for k in ("start_ns", "end_ns", "settled_ns"))
        require(end is not None and (start is None or start <= end)
                and (settled is None or settled >= end), "game state segment time order")
        require(previous_end is None or (start if start is not None else end) >= previous_end,
                "game state overlapping segments")
        require(type(segment["start_estimated"]) is bool, "game state estimated flag")
        require(segment["winner"] in (*SIDES, None) and type(segment["details"]) is dict,
                "game state segment result")
        previous_index, previous_end = index, end
    require(not value["segments"] or value["segment_kind"] is not None, "game state segment kind")
    if value["match"] is not None:
        match = obj(value["match"], "end_ns winner score")
        end = natural(match["end_ns"])
        require(previous_end is None or end >= previous_end, "game state match time order")
        require(match["winner"] in (*SIDES, None), "game state match winner")
        score(match["score"])
    return value


def load(path, expected_sha256, context):
    """Read once, validate exact bytes against the external pin, detach and freeze."""
    sha(expected_sha256)
    with Path(path).open("rb") as stream:
        payload = stream.read(MAX_BYTES + 1)
    require(len(payload) <= MAX_BYTES, "game state byte bound")
    require(hashlib.sha256(payload).hexdigest() == expected_sha256, "game state SHA-256 mismatch")
    return freeze(validate(decode(payload, MAX_BYTES), context))
