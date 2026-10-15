"""Pure per-book delay lookup from the prepared outcomes document."""

from replay.execution_terms import instant_ns
from replay.streams.protocol import require


def execution_delays(snapshot, book_keys, time_ns):
    """Return delay nanoseconds, with ``None`` for unavailable observations.

    Scope membership supplies market identity: a Polymarket token and a
    Limitless subscription slug are not their venue market IDs. Version-1
    outcomes remain untouched and are interpreted as unavailable.
    """
    require(type(time_ns) is int and time_ns >= 0, "execution terms lookup time")
    outcomes = snapshot.get("outcomes")
    doc = outcomes.get("document") if outcomes else None
    if doc is None or doc["version"] == 1:
        return (None,) * len(book_keys)
    scope = next((s for s in snapshot["scopes"]
                  if int(s["start_ns"]) <= time_ns < int(s["end_ns"])), None)
    if scope is None:
        return (None,) * len(book_keys)
    identities = {(b["instrument"], b["orientation"]): m["market_id"]
                  for m in scope["members"] for b in m["books"]}
    markets = {m["market_id"]: m for m in doc["markets"]}
    result = []
    for key in book_keys:
        market = markets.get(identities.get(key))
        delay = None
        if market is not None:
            for change in market["execution_terms"]["changes"]:
                if instant_ns(change["from"]) > time_ns:
                    break
                delay = change["taker_delay_ms"]
        result.append(None if delay is None else delay * 10**6)
    return tuple(result)
