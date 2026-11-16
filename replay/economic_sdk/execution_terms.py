"""Pure per-book delay lookup from the prepared outcomes document."""

from replay.execution_terms import instant_ns
from replay.streams.protocol import require


def execution_delays(snapshot, book_keys, time_ns, *, entity=None):
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
    cached = entity.delay_index if entity is not None else None
    if cached is not None and cached[0] is scope:
        series = cached[1]
    else:
        identities = {(b["instrument"], b["orientation"]): m["market_id"]
                      for m in scope["members"] for b in m["books"]}
        markets = {m["market_id"]: m for m in doc["markets"]}
        series = tuple(markets[identities[key]]["execution_terms"]["changes"]
                       if identities.get(key) in markets else () for key in book_keys)
        if entity is not None:
            # Only references into the already reserved pinned snapshot. This
            # compact index fits the entity reservation; no selected term is
            # retained, and changing time still walks the entire causal prefix.
            entity.delay_index = (scope, series)
    result = []
    for changes in series:
        delay = None
        for change in changes:
            if instant_ns(change["from"]) > time_ns:
                break
            delay = change["taker_delay_ms"]
        result.append(None if delay is None else delay * 10**6)
    return tuple(result)
