"""Event and book metadata from the committed context, with a visible degrade path (spec §1.4)."""

from __future__ import annotations


def _outcome_books(context):
    """(instrument, orientation) -> the non-negated outcome-book entry, first scope that has it."""
    found = {}
    for scope in context["scopes"]:
        for row in scope.get("outcome_books") or []:
            key = row["instrument"], row["orientation"]
            if not row.get("negated") and key not in found:
                found[key] = row
    return found


def _game(context):
    games = {e["detail"].get("game") for e in context.get("evidence") or [] if type(e.get("detail")) is dict}
    games.discard(None)
    return None if not games else games.pop() if len(games) == 1 else "mixed"


def _participants(context):
    outcomes = (context.get("outcomes") or {}).get("document") or {}
    if outcomes.get("participants"):
        return list(outcomes["participants"])
    for evidence in context.get("evidence") or []:
        inner = (evidence.get("detail") or {}).get("context") or {}
        if inner.get("participants"):
            return list(inner["participants"])
    return []


def event_header(bundle_id, context):
    if context is None:
        return {"event_id": None, "game": None, "title": bundle_id, "participants": []}
    outcomes = (context.get("outcomes") or {}).get("document") or {}
    participants = _participants(context)
    return {"event_id": outcomes.get("event_id"), "game": _game(context),
            "title": " vs ".join(participants) if participants else bundle_id, "participants": participants}


def book_rows(transition_books, context):
    """One manifest row per written book, in ``transition_books`` order."""
    markets, outcome = {}, {}
    if context is not None:
        outcomes = (context.get("outcomes") or {}).get("document") or {}
        markets = {m["market_id"]: m for m in outcomes.get("markets") or []}
        outcome = _outcome_books(context)
    rows = []
    for index, book in enumerate(transition_books):
        key = book["instrument"], book["orientation"]
        entry = outcome.get(key)
        market = markets.get(entry["market_id"] if entry else book.get("market_id"))
        claim_id = entry["claim_id"] if entry else None
        label = book["instrument"]
        if market is not None:
            claims = [c["claim_id"] for c in market.get("claims") or []]
            labels = market.get("outcome_labels") or []
            position = claims.index(claim_id) if claim_id in claims else None
            outcome_label = labels[position] if position is not None and position < len(labels) else None
            label = f"{market.get('market_type') or 'market'} · {outcome_label or book['instrument']}"
        rows.append({"book": index, "instrument": book["instrument"], "orientation": book["orientation"],
                     "venue": book["venue"], "market_id": book.get("market_id"),
                     "market_type": None if market is None else market.get("market_type"),
                     "market_status": None if market is None else market.get("market_status"),
                     "label": label, "claim_id": claim_id,
                     "price_scale": int(book["price_scale"]), "quantity_scale": int(book["quantity_scale"])})
    return rows


def scope_rows(transition_scopes):
    return [{"start_ns": str(s["start_ns"]), "end_ns": str(s["end_ns"]), "run_id": s.get("run_id"),
             "scheduled_start_ns": None if s.get("scheduled_start_ns") is None else str(s["scheduled_start_ns"])}
            for s in transition_scopes]
