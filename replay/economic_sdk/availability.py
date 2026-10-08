"""Availability interval engine shared by bundle coverage and the market profile.

Book, member and bundle availability under Risk policy as exact half-open
intervals (bundle coverage SPEC): status, availability states, scope and
boundary handling, the denominator with uncaptured members, and provenance.
The engine writes rows through any object with ``append(row)``; the caller
owns the file, the time order of cuts and every identity. ``availability_reader``
validates what it writes and does not import this module.
"""

from __future__ import annotations

from replay.economic_sdk.availability_reader import book_id
from replay.strategy_sdk import plain
from replay.streams.protocol import require

DISPOSITIONS = ("observed", "applied", "duplicate", "not_authority", "invalidated")
WINDOW_REASONS = {"lane_missing", "lane_invalid", "lane_not_expected", "visible_clock_regression"}


def status(book):
    """Detached copy of the fields coverage keeps from a live, in-place Book."""
    return book.validity, plain(book.reason), plain(book.as_of)


def availability(usable, required, uncaptured):
    if not required:
        return "NOT_CAPTURED"
    if usable == required and not uncaptured:
        return "AVAILABLE_UNDER_POLICY"
    return "PARTIAL" if usable else "UNAVAILABLE"


class IntervalEngine:
    """Rows for every scope entity of one snapshot, driven by Replay cuts.

    ``initial`` receives the initial cut, ``cut`` each later cut with its raw
    and effective (clipped, validated) times, and ``terminal`` the requested
    end. Time order and the window/group contract belong to the caller.
    """

    def __init__(self, snapshot, writer):
        self.snapshot = snapshot
        self.writer = writer
        self.scopes = snapshot["scopes"]
        self.scope = 0
        self.start = int(snapshot["config"]["start_ns"])
        self.end = int(snapshot["config"]["end_ns"])
        self.time = self.start
        # Detached (validity, reason, source) per planned book. Decoder books
        # mutate in place and are valid only during the hook, so the PRIOR
        # state needed at scope boundaries must be this explicit copy.
        self.books = None
        self.evidence = {}
        self.open = {}
        self.layouts = {}
        self.counts = []
        self.trades = {d: 0 for d in DISPOSITIONS}
        self.observed_trades = 0

    def initial(self, cut):
        self.books = {k: status(book) for k, book in cut.books.items()}
        self._evaluate(self.time)

    def cut(self, cut, raw_time, t):
        origin = cut.body["origin"]
        # The decoder has ALREADY applied this cut to its books in place. Scope
        # boundaries before this cut use the previous detached copy.
        self._advance(t)
        # Only books named by this cut (or whose window evidence was reset or
        # set) can change coverage fields; every other book would re-derive
        # exactly its previous fields, which _set ignores.
        changed = set()
        if origin["kind"] == "window":
            window = plain(origin)
            changed.update(self.evidence)
            self.evidence = {}
            for transition in cut.body["book_transitions"]:
                decision = transition["decision"]
                require(decision["kind"] == "invalidation", "window decision")
                why = decision["reason"]["kind"]
                require(why in WINDOW_REASONS, "window reason")
                k = (transition["key"]["instrument"], transition["key"]["orientation"])
                self.evidence[k] = (why, window)
                changed.add(k)
        for transition in cut.body["book_transitions"]:
            k = (transition["key"]["instrument"], transition["key"]["orientation"])
            self.books[k] = status(cut.books[k])
            changed.add(k)
        if changed:
            self._evaluate(t, changed)

    def count_trades(self, cut, raw_time):
        """Trade observations of currently required keys, by disposition."""
        if raw_time >= self.start:
            required = self._layout()["required"]
            for observation in cut.body["market_events"]:
                event = observation["event"]
                if event["kind"] == "trade":
                    value = event["value"]
                    k = (value["instrument"], value["orientation"])
                    if k in required:
                        disposition = observation["disposition"]
                        self.trades[disposition] += 1
                        if disposition != "duplicate":
                            self.observed_trades += 1

    def terminal(self, end):
        self._advance(end)
        self._close(end)

    def _advance(self, t):
        while (
            self.scope + 1 < len(self.scopes)
            and int(self.scopes[self.scope]["end_ns"]) <= t
        ):
            boundary = int(self.scopes[self.scope]["end_ns"])
            self._close(boundary)
            self.scope += 1
            self._evaluate(boundary)
        self.time = t

    def _set(self, entity, t, fields):
        prior = self.open.get(entity)
        # Provenance denotes the FIRST observation establishing this status;
        # revision/tick churn is deliberately not an interval boundary.
        # Explicit interval faults are bounded by their own source window.
        comparable = lambda x: {k: v for k, v in x.items() if k != "source"}
        if prior is not None and comparable(prior[1]) == comparable(fields):
            return
        if prior is not None:
            self._emit(entity, prior, t)
        self.open[entity] = (t, fields)

    def _emit(self, entity, prior, end):
        start, fields = prior
        if start < end:
            self.writer.append(
                {
                    "version": 1,
                    "scope": self.scope,
                    "entity": entity,
                    "start_ns": str(start),
                    "end_ns": str(end),
                    "vendor_completeness": "NOT_PROVEN",
                    **fields,
                }
            )

    def _close(self, t):
        for entity in sorted(self.open):
            self._emit(entity, self.open[entity], t)
        self.open.clear()

    def _layout(self):
        """Per-scope constants: book ids, member shapes, required trade keys.

        A book id hashes only (instrument, orientation), so it is computed once
        per scope instead of once per book per cut.
        """
        layout = self.layouts.get(self.scope)
        if layout is None:
            scope = self.scopes[self.scope]
            layout = {
                "members": [
                    (
                        "member:" + member["market_id"],
                        [
                            ((b["instrument"], b["orientation"]), book_id(b))
                            for b in member["books"]
                        ],
                        int(not member["capture_selected"]),
                    )
                    for member in scope["members"]
                ],
                "required": {
                    (b["instrument"], b["orientation"])
                    for b in scope["required_books"]
                },
            }
            self.layouts[self.scope] = layout
        return layout

    def _evaluate(self, t, changed=None):
        """Re-derive coverage fields; with `changed`, only for those books.

        Entities are visited in the same order either way (members in scope
        order, each member's books, the member, then the bundle), so the rows
        _set emits are identical to a full evaluation.
        """
        layout = self._layout()
        if changed is None:
            self.counts = [None] * len(layout["members"])
        touched = False
        for index, (member_id, books, outside) in enumerate(layout["members"]):
            if changed is not None and not any(k in changed for k, _ in books):
                continue
            touched = True
            count = 0
            for k, entity in books:
                validity, why, source = self.books[k]
                count += validity == "usable"
                if changed is not None and k not in changed:
                    continue
                evidence, evidence_source = self.evidence.get(k, ("unknown", None))
                self._set(
                    entity,
                    t,
                    {
                        "kind": "book",
                        "state": validity,
                        "evidence": evidence,
                        "reason": why,
                        "source": source,
                        "evidence_source": evidence_source,
                    },
                )
            required = len(books)
            self.counts[index] = (count, required, outside)
            self._set(
                member_id,
                t,
                {
                    "kind": "member",
                    "state": availability(count, required, outside),
                    "usable_books": count,
                    "required_books": required,
                    "uncaptured_members": outside,
                },
            )
        if not touched:
            return
        total = usable = uncaptured = 0
        for count, required, outside in self.counts:
            total += required
            usable += count
            uncaptured += outside
        self._set(
            "bundle",
            t,
            {
                "kind": "bundle",
                "state": availability(usable, total, uncaptured),
                "usable_books": usable,
                "required_books": total,
                "uncaptured_members": uncaptured,
            },
        )
