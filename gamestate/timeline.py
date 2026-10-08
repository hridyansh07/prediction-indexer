"""Strict event-keyed archive selection and offline timeline re-derivation."""
import hashlib
from gamestate import kalshi
from archive.storage.base import ObjectExpectation
from encoder import StoredIdentity


def latest(store, event_id, *, require_timeline=True):
    kalshi.event_identity(event_id)
    root = "gamestate/source=kalshi/event=" + event_id.split(":")[-1] + "/"
    candidates, count = [], 0
    for key in store.list_keys(root):
        count += 1
        if count > 100000:
            raise ValueError("listing_limit")
        if key.endswith("/receipt.json"):
            prefix = key.removesuffix("/receipt.json")
            receipt = kalshi.read_receipt(store, prefix)
            candidates.append((receipt["status"] == "complete", receipt["fetch_started_ns"], prefix))
            if len(candidates) > 1000:
                raise ValueError("event_fetch_limit")
    if not candidates:
        return {"state": "no_source", "inputs": {}}
    complete, _, prefix = max(candidates)
    receipt, records = kalshi.read_records(store, prefix)
    derived = kalshi.derive(receipt, records)
    derived.update(event_id=event_id, derivation_version=kalshi.DERIVATION_VERSION + 1)
    bad = {"response_shape", "live_data_missing", "related_event_missing", "milestone_changed"}
    state = "ok" if complete and not any(i["code"] in bad for i in derived["inconsistencies"]) else "incomplete"
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
    return {"state": state, "timeline": derived, "inputs": inputs}
