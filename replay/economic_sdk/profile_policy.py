"""Market profile policy, identity, file set and pair structure.

Kept apart from the collector so the independent readers can share the policy
contract without importing the code that writes the output.

Version 1 is the embedded and standalone policy of SDK spec section 7. Version 2
has the same closed fields and may add the standalone-only ``availability`` and
``transitions`` groups (``docs/specs/MARKET_PROFILE_V2.md``); a version-1
policy is byte-for-byte what it always was.
"""

from __future__ import annotations

from replay.preparation import digest
from replay.strategy_sdk import plain
from replay.streams.protocol import obj, require, uint

STRATEGY = "market_profile_v1"
GROUPS = ("activity", "depth", "pair_consistency", "quote_stability", "self_crossing", "top_of_book")
V2_GROUPS = ("availability", "transitions")
FILES = ("incidents.ndjson", "pair_profile.ndjson", "profile.ndjson")
AVAILABILITY_FILE = "availability.ndjson"
TRANSITIONS_FILE = "transitions.ndjson.zst"
TRANSITIONS_PLAIN = "transitions.ndjson"  # provisional; deleted once the frame is committed
TRANSITIONS_MAX_ROWS = 50_000_000
TRANSITIONS_MAX_BYTES = 16 * 1024**3
DISPOSITIONS = ("applied", "duplicate", "invalidated", "not_authority", "observed")
MAX_PROFILE_ROWS = 2_000_000


def profile_policy(value, *, standalone=False):
    """Validate a profile policy; ``standalone`` admits the version-2 groups.

    An embedded profile (``Requirements.profile``) never passes ``standalone``,
    so a policy that requests a version-2 group fails where it is constructed.
    """
    value = obj(plain(value), "version bucket_ns groups sizes_contracts tick_atoms depth_ticks survival_edges_ns")
    require(type(value["version"]) is int and value["version"] in (1, 2), "profile policy version")
    require(uint(value["bucket_ns"]) > 0, "profile bucket width")
    groups = value["groups"]
    allowed = set(GROUPS) | (set(V2_GROUPS) if value["version"] == 2 else set())
    require(type(groups) is list and groups == sorted(set(groups)) and set(groups) <= allowed,
            "profile groups")
    require(standalone or not set(groups) & set(V2_GROUPS),
            "profile group requires the standalone market profile")
    for name, cap in (("sizes_contracts", 16), ("depth_ticks", 8), ("survival_edges_ns", 32)):
        entries = value[name]
        require(type(entries) is list and 1 <= len(entries) <= cap, "profile list budget")
        numbers = [uint(v) for v in entries]
        require(numbers == sorted(set(numbers)) and numbers[0] > 0, "profile list order/positive")
    ticks = value["tick_atoms"]
    require(type(ticks) is dict and ticks and all(uint(v) > 0 for v in ticks.values()), "tick atoms")
    return value


def profile_identity(snapshot_sha256, policy):
    return digest({"strategy": STRATEGY, "policy": policy, "snapshot_sha256": snapshot_sha256})


def profile_files(policy):
    """The exact file set a policy produces; manifests and readers use this."""
    groups = set(policy["groups"])
    return (FILES + ((AVAILABILITY_FILE,) if "availability" in groups else ())
            + ((TRANSITIONS_FILE,) if "transitions" in groups else ()))


def pairs_of(scope):
    """Two-book members with a complement structure: PM token pairs, Kalshi YES/NO."""
    result = []
    for member in scope["members"]:
        if not member["capture_selected"]:
            continue
        venue = member["market_id"].split(":", 1)[0]
        books = sorted((b["instrument"], b["orientation"]) for b in member["books"])
        if len(books) != 2:
            continue
        if venue == "polymarket" and all(o == "outcome" for _, o in books):
            result.append((member["market_id"], tuple(books)))
        elif venue == "kalshi" and books[0][0] == books[1][0] == member["market_id"] \
                and {o for _, o in books} == {"complement", "outcome"}:
            result.append((member["market_id"], tuple(books)))
    return sorted(result)
