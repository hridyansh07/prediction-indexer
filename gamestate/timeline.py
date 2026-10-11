"""Strict event-keyed archive selection and offline timeline re-derivation."""
import hashlib
from gamestate import kalshi
from archive.storage.base import ObjectExpectation, VerificationFailure
from encoder import StoredIdentity


def latest(store, event_id, *, require_timeline=True):
    """The best archived fetch for one event: complete first, then newest.

    The listing streams and only the best candidate is retained. A receipt that
    fails strict reading is skipped and counted in ``rejected_fetches``; it never
    hides a readable fetch of the same event.
    """
    kalshi.event_identity(event_id)
    root = "gamestate/source=kalshi/event=" + event_id.split(":")[-1] + "/"
    best, rejected = None, 0
    for key in store.list_keys(root):
        if not key.endswith("/receipt.json"):
            continue
        prefix = key.removesuffix("/receipt.json")
        try:
            receipt = kalshi.read_receipt(store, prefix)
        except (ValueError, KeyError, TypeError, VerificationFailure):
            rejected += 1
            continue
        candidate = (receipt["status"] == "complete", receipt["fetch_started_ns"], prefix)
        best = candidate if best is None or candidate > best else best
    if best is None:
        return {"state": "no_source", "inputs": {}, "rejected_fetches": rejected}
    _, _, prefix = best
    result = read_fetch(store, prefix, require_timeline=require_timeline)
    result["rejected_fetches"] = rejected
    return result


def read_fetch(store, prefix, *, require_timeline=True):
    """Verify one archived fetch and its immutable offline timeline."""
    receipt, records = kalshi.read_records(store, prefix)
    derived = kalshi.event_timeline(receipt, records)
    state = "ok" if receipt["status"] == "complete" and not kalshi.disqualified(derived) else "incomplete"
    inputs = {prefix + "/responses.ndjson.zst": receipt["stored"]}
    key = prefix + "/receipt.json"
    with store.open(key, max_bytes=kalshi.MAX_METADATA) as stream:
        payload = stream.read(kalshi.MAX_METADATA + 1)
    inputs[key] = {"sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload)}
    if require_timeline:
        key = kalshi.timeline_key(prefix)
        metadata = store.head(key)
        if metadata is None:
            state = "incomplete"
        else:
            expected = ObjectExpectation(key, metadata.stored, metadata.provider_checksum,
                                         metadata.provider_checksum_algorithm, "application/json", None)
            with store.open_verified(expected) as stream:
                payload = stream.read(kalshi.MAX_METADATA + 1)
                if len(payload) > kalshi.MAX_METADATA or stream.read(1):
                    raise ValueError("timeline_size")
            if payload != kalshi.dumps(derived):
                raise ValueError("timeline_raw_binding")
            inputs[key] = StoredIdentity(hashlib.sha256(payload).hexdigest(), len(payload)).as_record()
    return {"state": state, "timeline": derived, "inputs": inputs, "rejected_fetches": 0}
