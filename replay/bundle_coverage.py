"""bundle_coverage_v1: coverage under Risk policy, never economic opportunities."""

from pathlib import Path

from replay.coverage_output import (
    MAX_BYTES,
    MAX_LINE,
    MAX_RECORDS,
    book_id,
    entities,
    validate_content,
)
from replay.preparation import digest
from replay.strategy_sdk import LineWriter, PreparedInput, plain
from replay.streams.protocol import require
from replay.supervisor import write_json_durable


def status(book):
    """Detached copy of the fields coverage keeps from a live, in-place Book."""
    return book.validity, plain(book.reason), plain(book.as_of)


def availability(usable, required, uncaptured):
    if not required:
        return "NOT_CAPTURED"
    if usable == required and not uncaptured:
        return "AVAILABLE_UNDER_POLICY"
    return "PARTIAL" if usable else "UNAVAILABLE"


class Coverage:
    def __init__(self, context):
        self.input = PreparedInput(context["config"])
        self.snapshot = plain(self.input.snapshot)
        self.root = Path(context["output_directory"])
        require(
            self.root.is_dir() and not any(self.root.iterdir()),
            "output directory must be empty",
        )
        self.binding = {
            k: context[k] for k in ("run_id", "attempt_id", "group", "identity")
        }
        entities(self.snapshot)  # fail the context budget before opening output
        self.writer = LineWriter(
            self.root / "intervals.ndjson",
            max_bytes=MAX_BYTES,
            max_records=MAX_RECORDS,
            max_line_bytes=MAX_LINE,
        )
        self.scopes = self.snapshot["scopes"]
        self.scope = 0
        self.time = int(self.snapshot["config"]["start_ns"])
        self.end = int(self.snapshot["config"]["end_ns"])
        self.sequence = -1
        # Detached (validity, reason, source) per planned book. Decoder books
        # mutate in place and are valid only during the hook, so the PRIOR
        # state needed at scope boundaries must be this explicit copy.
        self.books = None
        self.window = None
        self.evidence = {}
        self.open = {}
        self.layouts = {}
        self.counts = []
        self.terminal = self.finished = self.poisoned = False
        self.trades = {
            d: 0
            for d in (
                "observed",
                "applied",
                "duplicate",
                "not_authority",
                "invalidated",
            )
        }
        self.observed_trades = 0

    def __call__(self, cut):
        try:
            require(not self.poisoned and not self.terminal, "closed coverage")
            self._apply(cut)
        except Exception:
            self.poisoned = True
            raise

    def _apply(self, cut):
        require(cut.sequence == self.sequence + 1, "coverage sequence")
        self.sequence = cut.sequence
        if cut.kind == "initial":
            require(cut.sequence == 0)
            self.input.bind(cut.body)
            self.books = {k: status(book) for k, book in cut.books.items()}
            self._evaluate(self.time)
            return
        require(self.input.bound, "missing initial")
        if cut.kind == "terminal":
            require(
                self.window is not None and int(self.window["end_ns"]) >= self.end,
                "incomplete window coverage",
            )
            self._advance(self.end)
            self._close(self.end)
            self.terminal = True
            return
        require(cut.kind == "cut")
        origin = plain(cut.body["origin"])
        raw_time = int(
            origin["start_ns"] if origin["kind"] == "window" else origin["visible_ns"]
        )
        if origin["kind"] == "window":
            if self.window is not None:
                require(origin["start_ns"] == self.window["end_ns"], "window partition")
            else:
                require(
                    raw_time <= self.time < int(origin["end_ns"]), "first window range"
                )
        else:
            require(
                self.window is not None and origin["pin"] == self.window["pin"],
                "group window",
            )
            require(
                int(self.window["start_ns"]) <= raw_time < int(self.window["end_ns"]),
                "group range",
            )
            require(
                raw_time >= int(self.snapshot["config"]["start_ns"])
                or self.snapshot["config"]["lower_bound"] == "expand_to_window_start",
                "group before requested start",
            )
        t = max(raw_time, int(self.snapshot["config"]["start_ns"]))
        require(self.time <= t < self.end, "coverage time order")
        # The decoder has ALREADY applied this cut to its books in place. Scope
        # boundaries before this cut use the previous detached copy.
        self._advance(t)
        # Only books named by this cut (or whose window evidence was reset or
        # set) can change coverage fields; every other book would re-derive
        # exactly its previous fields, which _set ignores.
        changed = set()
        if origin["kind"] == "window":
            self.window = origin
            changed.update(self.evidence)
            self.evidence = {}
            for transition in cut.body["book_transitions"]:
                decision = transition["decision"]
                require(decision["kind"] == "invalidation", "window decision")
                why = decision["reason"]["kind"]
                require(
                    why
                    in {
                        "lane_missing",
                        "lane_invalid",
                        "lane_not_expected",
                        "visible_clock_regression",
                    },
                    "window reason",
                )
                k = (transition["key"]["instrument"], transition["key"]["orientation"])
                self.evidence[k] = (why, origin)
                changed.add(k)
        for transition in cut.body["book_transitions"]:
            k = (transition["key"]["instrument"], transition["key"]["orientation"])
            self.books[k] = status(cut.books[k])
            changed.add(k)
        if changed:
            self._evaluate(t, changed)
        if raw_time >= int(self.snapshot["config"]["start_ns"]):
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

    def finish(self):
        try:
            require(
                self.terminal and not self.poisoned and not self.finished,
                "missing terminal / failed coverage",
            )
            identity = self.writer.finish()
            manifest = {
                "version": 1,
                "strategy": "bundle_coverage_v1",
                "snapshot_sha256": self.input.sha256,
                "intervals": identity,
                "trades": self.trades,
                "nonduplicate_trade_observations": self.observed_trades,
            }
            # Independent streaming validation precedes publication of metadata.
            summary = validate_content(self.root, self.input.snapshot, manifest)
            write_json_durable(self.root / "summary.json", summary)
            manifest["summary_sha256"] = digest(summary)
            write_json_durable(self.root / "manifest.json", manifest)
            write_json_durable(
                self.root / "content_receipt.json",
                {
                    "version": 1,
                    "semantic_sha256": digest(manifest),
                    **self.binding,
                    "terminal": self.sequence + 1,
                },
            )
            self.finished = True
        except Exception:
            self.poisoned = True
            raise


def build(context):
    return Coverage(context)
