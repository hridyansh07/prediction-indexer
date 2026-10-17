"""Hand-authored corpus results for the research tests: only the fields the builder reads."""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

from encoder import encode_stream
from replay.corpus import results as corpus_results
from replay.economic_sdk.availability_reader import book_id

S = 1_000_000_000
T0 = 1_791_000_000 * S            # capture start
ATTEMPT = "a" * 32
KALSHI = "kalshi:KXLOLGAME-X-C9"
PM_A, PM_B = "polymarket:111", "polymarket:222"
BOOKS = [
    {"instrument": KALSHI, "orientation": "outcome", "venue": "kalshi", "market_id": KALSHI,
     "price_scale": "4", "quantity_scale": "2", "tick_atoms": "100", "ask_source": "projected",
     "ask_source_book": [KALSHI, "complement"]},
    {"instrument": PM_A, "orientation": "outcome", "venue": "polymarket", "market_id": "polymarket:9",
     "price_scale": "4", "quantity_scale": "6", "tick_atoms": "10", "ask_source": "native", "ask_source_book": None},
    {"instrument": PM_B, "orientation": "outcome", "venue": "polymarket", "market_id": "polymarket:9",
     "price_scale": "4", "quantity_scale": "6", "tick_atoms": "10", "ask_source": "native", "ask_source_book": None},
]
# Two scopes with a gap: [T0, T0+15 min) and [T0+20 min, T0+30 min).
SCOPES = [{"start_ns": str(T0), "end_ns": str(T0 + 900 * S), "run_id": "r0", "scheduled_start_ns": str(T0 + 3600 * S),
           "capture_start_ns": str(T0)},
          {"start_ns": str(T0 + 1200 * S), "end_ns": str(T0 + 1800 * S), "run_id": "r1",
           "scheduled_start_ns": str(T0 + 3600 * S), "capture_start_ns": str(T0)}]


def q(price, qty="100"):
    return [str(price), str(qty)]


def top(t, book, bid, ask, *, scope=0, validity="usable", cause="operations", cut=0):
    return {"type": "top", "scope": scope, "book": book, "cut": cut, "t_ns": str(t), "cause": cause,
            "validity": validity, "bid": bid, "ask": ask}


def trade(t, book, price, qty, *, scope=0, cut=0):
    return {"type": "trade", "scope": scope, "book": book, "cut": cut, "t_ns": str(t), "price": str(price),
            "qty": str(qty), "aggressor": "bid", "disposition": "applied"}


def default_rows():
    """Book 0 changes twice and goes unusable; book 1 trades; scope 1 reopens books 0 and 1."""
    return [
        top(T0, 0, q(4000), q(4200), cause="open"),
        top(T0, 1, q(5000), q(5100), cause="open"),
        top(T0, 2, None, None, cause="open", validity="not_initialized"),
        top(T0 + 30 * S, 0, q(4100), q(4200)),
        trade(T0 + 45 * S, 1, 5100, 2_000_000),
        top(T0 + 61 * S, 0, q(9999), q(9999)),
        top(T0 + 61 * S, 0, q(3900), q(4300)),   # zero-duration predecessor at the same time
        top(T0 + 700 * S, 0, None, None, validity="unusable:connection_closed", cause="invalidation"),
        top(T0 + 1200 * S, 0, q(6000), q(6100), scope=1, cause="open"),
        top(T0 + 1200 * S, 1, q(5500), q(5600), scope=1, cause="open"),
        trade(T0 + 1300 * S, 0, 6100, 500, scope=1),
    ]


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(value, sort_keys=True) + "\n").encode())


def ndjson(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join((json.dumps(r, sort_keys=True) + "\n").encode() for r in rows))


def profile_output(directory, rows=None, *, books=BOOKS, scopes=SCOPES, availability=None):
    rows = default_rows() if rows is None else rows
    directory.mkdir(parents=True, exist_ok=True)
    plain = b"".join((json.dumps(r, sort_keys=True) + "\n").encode() for r in rows)
    sink = io.BytesIO()
    result = encode_stream(io.BytesIO(plain), sink)
    (directory / "transitions.ndjson.zst").write_bytes(sink.getvalue())
    write_json(directory / "manifest.json", {"version": 2, "files": {"transitions.ndjson.zst": {
        "logical": {"sha256": result.logical.sha256, "byte_length": result.logical.byte_length,
                    "records": result.logical.line_count},
        "stored": {"sha256": result.stored.sha256, "byte_length": result.stored.byte_length}}}})
    write_json(directory / "summary.json", {"transition_books": books, "transition_scopes": scopes})
    if availability is None:
        entity = book_id({"instrument": KALSHI, "orientation": "outcome"})
        availability = [
            {"kind": "book", "entity": entity, "scope": 0, "start_ns": str(T0), "end_ns": str(T0 + 700 * S),
             "state": "usable", "reason": None},
            {"kind": "book", "entity": entity, "scope": 0, "start_ns": str(T0 + 700 * S),
             "end_ns": str(T0 + 900 * S), "state": "unusable", "reason": {"kind": "connection_closed"}},
            {"kind": "bundle", "entity": "bundle", "scope": 0, "start_ns": str(T0), "end_ns": str(T0 + 900 * S),
             "state": "PARTIAL"},
        ]
    ndjson(directory / "availability.ndjson", availability)
    write_json(directory / "content_receipt.json", {"semantic_sha256": "1" * 64})


def cross_leg(instrument, orientation, market):
    return {"instrument": instrument, "orientation": orientation, "market_id": market}


def fill(value, steps, *, stop="edge"):
    return {"sources": ["kalshi_complement_ask", "ask"], "units": ["1", "10000"],
            "results": [{"name": "edge", "role": "governs", "mode": "edge", "steps": str(steps), "stop": stop,
                         "value": None if value is None else str(value), "legs": [], "tradeable": True,
                         "kill_prices": ["9000", "6000"]}],
            "kill_prices": ["9000", "6000"], "end_books": [{"validity": "usable"}, {"validity": "usable"}]}


def episode(start, end, entity, kind="net", value=None, steps=None):
    row = {"scope": 0, "entity": entity, "episode_id": hashlib.sha256(f"{entity}{kind}{start}".encode()).hexdigest(),
           "kind": kind, "start_ns": str(start), "end_ns": str(end), "end_reason": "KILL_PRICE",
           "censored": False, "viable_tiers": ["250000000"]}
    if steps is not None:
        row["fill"] = fill(value, steps)
    return row


def default_episodes():
    return [
        episode(T0 + 40 * S, T0 + 40 * S + 250_000_000, 0, value=3 * 10 ** 36 + 5 * 10 ** 34, steps=106_700),
        episode(T0 + 40 * S + 100_000_000, T0 + 41 * S, 1, value=10 ** 35, steps=500),
        episode(T0 + 100 * S, T0 + 100 * S + 50_000_000, 0, value=-10 ** 34, steps=100),
        episode(T0 + 39 * S, T0 + 42 * S, 0, kind="gross"),
    ]


def cross_output(directory, episodes=None):
    directory.mkdir(parents=True, exist_ok=True)
    entities = [[
        {"hash": "e0", "descriptor": {"basket_kind": "CROSS_VENUE_COMPLETE_SET", "route_id": "route-0",
                                      "legs": [cross_leg(KALSHI, "complement", KALSHI),
                                               cross_leg(PM_A, "outcome", "polymarket:9")]}},
        {"hash": "e1", "descriptor": {"basket_kind": "CROSS_VENUE_COMPLETE_SET", "route_id": "route-1",
                                      "legs": [cross_leg(KALSHI, "complement", KALSHI),
                                               cross_leg(PM_B, "outcome", "polymarket:9")]}},
    ]]
    write_json(directory / "entities.json", {"scopes": entities})
    ndjson(directory / "episodes.ndjson", default_episodes() if episodes is None else episodes)
    write_json(directory / "manifest.json", {"version": 2, "policy": {"fills": {"step_contracts": "0.01"}}})
    write_json(directory / "content_receipt.json", {"semantic_sha256": "2" * 64})


def context_document(bundle_id):
    claims = {"k": "c" * 64, "a": "d" * 64, "b": "e" * 64}
    return {
        "evidence": [{"provider": "universe", "detail": {"game": "league_of_legends", "context": {
            "participants": ["Cloud9", "LYON"]}}}],
        "scopes": [{"outcome_books": [
            {"instrument": KALSHI, "orientation": "complement", "negated": True, "claim_id": claims["k"],
             "market_id": KALSHI},
            {"instrument": KALSHI, "orientation": "outcome", "negated": False, "claim_id": claims["k"],
             "market_id": KALSHI},
            {"instrument": PM_A, "orientation": "outcome", "negated": False, "claim_id": claims["k"],
             "market_id": "polymarket:9"},
            {"instrument": PM_B, "orientation": "outcome", "negated": False, "claim_id": claims["b"],
             "market_id": "polymarket:9"}]}],
        "outcomes": {"provider": "universe", "document": {
            "bundle_id": bundle_id, "event_id": "event:d1:" + "f" * 64, "participants": ["Cloud9", "LYON"],
            "markets": [
                {"market_id": KALSHI, "market_type": "series_moneyline", "outcome_labels": ["Cloud9", "Cloud9"],
                 "market_status": "determined",
                 "claims": [{"claim_id": claims["k"], "claim_key": "claim=0"}], "venue": "kalshi"},
                {"market_id": "polymarket:9", "market_type": "series_moneyline", "outcome_labels": ["Cloud9", "LYON"],
                 "claims": [{"claim_id": claims["k"], "claim_key": "claim=0"},
                            {"claim_id": claims["b"], "claim_key": "claim=1"}], "venue": "polymarket"}]}},
    }


def corpus_result(key_dir, bundle_id, *, corpus_key=None, status="SUCCESS", groups=("profile", "cross", "coverage"),
                  excluded=(), failed_group=None, context=True, success_marker=True, attempts=(ATTEMPT,),
                  checks=None, rows=None, episodes=None, commit=True, tamper_context=False, game_state=None,
                  before_commit=None):
    """Write a bench output into a staging directory and commit it as the corpus runner does."""
    key_dir = Path(key_dir)
    corpus_key = corpus_key or (key_dir.name + "0" * (64 - len(key_dir.name)))
    staging = key_dir.parent / f".staging-{key_dir.name}-{bundle_id}"
    for attempt in attempts:
        write_json(staging / "run" / attempt / "result.json", {"attempt": attempt})
    attempt = attempts[0]
    if success_marker:
        write_json(staging / "run" / "SUCCESS.json", {"attempt": attempt})
    snapshot = b"{}\n"
    # As the corpus runner does: the prepared context (and game state, when prepared)
    # live outside the bench output and are committed beside it through `inputs`.
    prepared = key_dir.parent / f".inputs-{key_dir.name}-{bundle_id}"
    inputs = []
    if context:
        document = context_document(bundle_id)
        snapshot = (json.dumps(document, sort_keys=True) + "\n").encode()
        (prepared / "context").mkdir(parents=True)
        (prepared / "context" / "context.json").write_bytes(snapshot)
        receipt_sha = hashlib.sha256(b"tampered" if tamper_context else snapshot).hexdigest()
        write_json(prepared / "context" / "receipt.json", {"version": 1, "snapshot_sha256": receipt_sha})
        inputs += [("context/context.json", prepared / "context" / "context.json"),
                   ("context/receipt.json", prepared / "context" / "receipt.json")]
    if game_state is not None:
        write_json(prepared / "game_state" / "game_state.json", game_state)
        inputs.append(("game_state/game_state.json", prepared / "game_state" / "game_state.json"))
    snapshot_sha256 = hashlib.sha256(snapshot).hexdigest()
    result_groups = []
    for group in groups:
        output = staging / "run" / attempt / group / "output"
        if group == "profile":
            profile_output(output, rows)
        elif group == "cross":
            cross_output(output, episodes)
        else:
            output.mkdir(parents=True)
            write_json(output / "manifest.json", {"version": 1})
        receipt = None if (status == "FAILED" or group == failed_group) else {"semantic_sha256": group[0] * 64}
        result_groups.append({"name": group, "factory": f"x.{group}:build", "receipt": receipt,
                              "checks": (checks or {}).get(group, {})})
    if failed_group is not None:
        write_json(staging / "run" / attempt / failed_group / "failure.json", {
            "version": 1, "exit": 20, "traceback": "...",
            "errors": [{"type": "RuntimeError", "message": "detached state budget"},
                       {"type": "MemoryError", "message": "128 MiB"}]})
    write_json(staging / "result.json", {
        "version": 1, "label": "corpus", "status": status, "started_at": "2026-10-12T00:00:00+00:00",
        "run_seconds": 1, "image": {"id": "sha256:" + "0" * 64, "built": True, "source_commit": "c" * 40,
                                    "source_dirty": False, "dockerfile": "d"},
        "context": {"snapshot_sha256": snapshot_sha256, "outcomes_provider": "universe"},
        "fee_catalog_identity": None, "groups": result_groups,
        "error": None if status != "FAILED" else {"type": "SupervisorError", "message": "group failed",
                                                  "trace_tail": ""}})
    destination = key_dir / bundle_id
    if before_commit is not None:
        before_commit(staging)
    if commit:
        corpus_results.commit(staging, destination, corpus_key=corpus_key, bundle_id=bundle_id, status=status,
                              bench_exit=0 if status == "SUCCESS" else 3, game_state=None,
                              excluded_groups=[{"group": g, "reason": "entities.json over 8 MiB"} for g in excluded],
                              inputs=inputs)
    else:
        for relative, source in inputs:
            (staging / relative).parent.mkdir(parents=True, exist_ok=True)
            (staging / relative).write_bytes(Path(source).read_bytes())
        staging.rename(destination)
    import shutil
    for leftover in (staging, prepared):
        if leftover.exists():
            shutil.rmtree(leftover)
    return destination
