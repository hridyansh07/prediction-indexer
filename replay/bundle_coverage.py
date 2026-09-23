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
        self.books = None
        self.window = None
        self.evidence = {}
        self.open = {}
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
            self.books = cut.books
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
        # The decoder has ALREADY installed the new books. Scope boundaries
        # before this cut must use the previous immutable snapshot.
        self._advance(t)
        if origin["kind"] == "window":
            self.window = origin
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
        self.books = cut.books
        self._evaluate(t)
        if raw_time >= int(self.snapshot["config"]["start_ns"]):
            required = {
                (b["instrument"], b["orientation"])
                for b in self.scopes[self.scope]["required_books"]
            }
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

    def _evaluate(self, t):
        scope = self.scopes[self.scope]
        total = usable = uncaptured = 0
        for member in scope["members"]:
            count = 0
            for b in member["books"]:
                k = b["instrument"], b["orientation"]
                book = self.books[k]
                evidence, evidence_source = self.evidence.get(k, ("unknown", None))
                self._set(
                    book_id(b),
                    t,
                    {
                        "kind": "book",
                        "state": book.validity,
                        "evidence": evidence,
                        "reason": plain(book.reason),
                        "source": plain(book.as_of),
                        "evidence_source": evidence_source,
                    },
                )
                count += book.validity == "usable"
            required = len(member["books"])
            outside = int(not member["capture_selected"])
            self._set(
                "member:" + member["market_id"],
                t,
                {
                    "kind": "member",
                    "state": availability(count, required, outside),
                    "usable_books": count,
                    "required_books": required,
                    "uncaptured_members": outside,
                },
            )
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
