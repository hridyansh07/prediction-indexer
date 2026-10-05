"""Immutable normal-resolution outcome views, used during basket construction."""

from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from types import MappingProxyType


@dataclass(frozen=True)
class Space:
    shape_id: str
    scope: str
    coverage: str
    best_of: int | None
    keys: tuple[str, ...]


@dataclass(frozen=True)
class Leg:
    book: tuple[str, str]
    market_id: str
    shape_id: str
    claim_id: str
    negated: bool
    keys: frozenset[str]


class OutcomeScope:
    """Set algebra only. Exhaustive coverage is required for partitions."""

    def __init__(self, snapshot, scope_index):
        scope = snapshot["scopes"][scope_index]
        recorded = snapshot.get("outcomes") if snapshot["version"] == 2 else None
        self.available = bool(recorded and recorded["provider"] == "universe")
        self.unavailable = None if self.available else "outcomes_unavailable"
        spaces, legs, statuses = {}, {}, {}
        if self.available:
            doc = recorded["document"]
            spaces = {
                s["space_shape_id"]: Space(
                    s["space_shape_id"],
                    s["scope"],
                    s["coverage"],
                    s["best_of"],
                    tuple(s["outcome_keys"]),
                )
                for s in doc["spaces"]
            }
            claims = {c["claim_id"]: c for c in doc["claims"]}
            for row in scope["outcome_books"]:
                book = row["instrument"], row["orientation"]
                statuses[book] = row["status"], row["reason"]
                if row["status"] == "MASKED":
                    keys = frozenset(claims[row["claim_id"]]["outcome_keys"])
                    if row["negated"]:
                        keys = frozenset(spaces[row["space_shape_id"]].keys) - keys
                    legs[book] = Leg(
                        book,
                        row["market_id"],
                        row["space_shape_id"],
                        row["claim_id"],
                        row["negated"],
                        keys,
                    )
        else:
            statuses = {
                (b["instrument"], b["orientation"]): ("OUTCOMES_UNAVAILABLE", None)
                for m in scope["members"]
                for b in m["books"]
            }
        self.spaces = MappingProxyType(spaces)
        self._legs, self._statuses = MappingProxyType(legs), MappingProxyType(statuses)

    def leg(self, book_key):
        return self._legs.get(book_key)

    def status(self, book_key):
        return self._statuses.get(book_key, ("NOT_IN_MODEL", None))

    def payoff(self, book_keys):
        legs = [self.leg(k) for k in book_keys]
        if not legs:
            return {}
        if any(leg is None for leg in legs):
            raise ValueError("payoff requires MASKED legs")
        shapes = {leg.shape_id for leg in legs}
        if len(shapes) != 1:
            raise ValueError("payoff requires one outcome space")
        shape = next(iter(shapes))
        return {
            shape: tuple(
                sum(key in leg.keys for leg in legs) for key in self.spaces[shape].keys
            )
        }

    def is_partition(self, book_keys):
        legs = [self.leg(k) for k in book_keys]
        if not legs or any(leg is None for leg in legs):
            return False
        shape = legs[0].shape_id
        if (
            any(leg.shape_id != shape for leg in legs)
            or self.spaces[shape].coverage != "EXHAUSTIVE"
        ):
            return False
        union = set()
        for leg in legs:
            if union & leg.keys:
                return False
            union.update(leg.keys)
        return union == set(self.spaces[shape].keys)

    def implications(self, book_keys):
        legs = [self._legs[k] for k in sorted(set(book_keys)) if k in self._legs]
        return [
            (a.book, b.book)
            for a in legs
            for b in legs
            if a.shape_id == b.shape_id and a.keys < b.keys
        ]

    def complete_sets(self, book_keys, *, max_legs=4, limit=4096):
        if (
            type(max_legs) is not int
            or max_legs < 1
            or type(limit) is not int
            or limit < 0
        ):
            raise ValueError("invalid complete-set bounds")
        by_shape = {}
        for book in sorted(set(book_keys)):
            leg = self.leg(book)
            if leg is not None and self.spaces[leg.shape_id].coverage == "EXHAUSTIVE":
                by_shape.setdefault(leg.shape_id, []).append(leg)
        result = []
        for shape, legs in sorted(by_shape.items()):
            all_keys = frozenset(self.spaces[shape].keys)
            candidates = {
                key: [leg for leg in legs if key in leg.keys] for key in all_keys
            }

            def search(remaining, chosen, candidates=candidates):
                if not remaining:
                    result.append(tuple(sorted(chosen)))
                    if len(result) > limit:
                        raise ValueError("complete-set limit exceeded")
                    return
                if len(chosen) == max_legs:
                    return
                # The least-covered outcome prunes gaps and large sibling lists.
                eligible = {
                    k: [leg for leg in candidates[k] if leg.keys <= remaining]
                    for k in remaining
                }
                pivot = min(remaining, key=lambda k: (len(eligible[k]), k))
                for leg in eligible[pivot]:
                    search(remaining - leg.keys, chosen + (leg.book,))

            search(all_keys, ())
        return sorted(result)


# Identity keys avoid hashing a snapshot's nested JSON. Strong owners prevent id
# reuse, and the LRU retains at most eight recently requested scope views.
_CACHE = OrderedDict()
_CACHE_LOCK = RLock()


def outcome_scope(snapshot, scope_index):
    if type(scope_index) is not int or not 0 <= scope_index < len(snapshot["scopes"]):
        raise IndexError("outcome scope index")
    cache_key = id(snapshot), scope_index
    with _CACHE_LOCK:
        if cache_key in _CACHE:
            owner, view = _CACHE.pop(cache_key)
        else:
            owner, view = snapshot, OutcomeScope(snapshot, scope_index)
        _CACHE[cache_key] = owner, view
        while len(_CACHE) > 8:
            _CACHE.popitem(last=False)
        return view
