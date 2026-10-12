"""``game.json``: prepared game facts with their source-time windows (spec §1.5, §2.6).

Sources, in order: the result's committed ``game_state/game_state.json`` (read
with the SDK's pinned loader), else a local Part A archive root given with
``--game-state`` (projected in memory exactly as runner-side preparation does),
else ``no_source``. A result without a context always gets ``no_source``.
"""

from __future__ import annotations

from pathlib import Path

GAME_VERSION = 1


def prepared_document(event, archive_root):
    """Return ``(document or None, how)`` for one event."""
    if event.context is None:
        return None, "no_context"
    committed = event.root / "game_state" / "game_state.json"
    if not committed.is_file() and archive_root is None:
        return None, "no_archive"
    from replay.preparation import load_snapshot
    context = load_snapshot(event.root / "context", expected_sha256=event.result["context"]["snapshot_sha256"])
    if committed.is_file():
        from replay.game_state import load
        expected = event.result.get("game_state_sha256")
        load(committed, expected, context)   # the pinned reader: sha, schema, event binding
        return event.game_state_document, "committed"
    from archive.storage.local import LocalObjectStore
    from replay.prepare_game_state import game_state_document
    return game_state_document(context, store=LocalObjectStore(Path(archive_root))), "archive"


def _side(competitors, side):
    entry = (competitors or {}).get(side) if side else None
    return None if entry is None else entry.get("label") or side


def game_events(document, windows):
    """Every fact of a prepared document as a windowed game event, in time order."""
    events = []

    def add(kind, segment, label, basis, source_ns, detail):
        if source_ns is None:
            return
        source_ns = int(source_ns)
        window = windows.get(basis)
        before = 0 if window is None else window["before_ms"] * 1_000_000
        after = 0 if window is None else window["after_ms"] * 1_000_000
        latest = source_ns + after
        events.append({"kind": kind, "segment": segment, "label": label, "time_basis": basis,
                       "source_ns": str(source_ns), "earliest_ns": str(source_ns - before),
                       "latest_ns": str(latest), "exact": basis == "market_settlement",
                       "known_at_ns": str(latest), "detail": detail})

    competitors = document.get("competitors")
    noun = (document.get("segment_kind") or "segment").capitalize()
    add("scheduled_start", None, "Scheduled start", "scheduled", document.get("scheduled_start_ns"), {})
    for segment in document.get("segments") or []:
        index = segment["index"]
        basis = "close_minus_duration" if segment.get("start_estimated") else "kalshi_market_close"
        add("segment_start", index, f"{noun} {index} start", basis, segment.get("start_ns"),
            {"estimated": bool(segment.get("start_estimated"))})
        winner = _side(competitors, segment.get("winner"))
        add("segment_end", index, f"{noun} {index} end" + (f" · {winner}" if winner else ""),
            "kalshi_market_close", segment.get("end_ns"),
            {"winner": segment.get("winner"), "winner_label": winner, "details": segment.get("details")})
        add("settlement", index, f"{noun} {index} settled", "market_settlement", segment.get("settled_ns"), {})
    match = document.get("match")
    if match is not None:
        winner = _side(competitors, match.get("winner"))
        add("match_end", None, "Match end" + (f" · {winner}" if winner else ""), "kalshi_milestone_end",
            match.get("end_ns"), {"winner": match.get("winner"), "winner_label": winner, "score": match.get("score")})
    order = {"scheduled_start": 0, "segment_start": 1, "segment_end": 2, "settlement": 3, "match_end": 4}
    events.sort(key=lambda e: (int(e["source_ns"]), order[e["kind"]], e["segment"] or 0))
    return events


def game_json(document, how, windows):
    if document is None:
        return {"game_version": GAME_VERSION, "state": "no_source", "reason": how, "source": None,
                "competitors": None, "events": []}
    if document["state"] == "ok":
        state, reason = "ok", None
    else:
        state = "incomplete" if document["reason"] == "incomplete" else "no_source"
        reason = document["reason"]
    return {"game_version": GAME_VERSION, "state": state, "reason": reason, "source": document.get("source"),
            "competitors": document.get("competitors"), "events": game_events(document, windows)}
