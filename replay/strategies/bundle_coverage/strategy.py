"""bundle_coverage_v1: coverage under Risk policy, never economic opportunities."""

from pathlib import Path

from .output import (
    MAX_BYTES,
    MAX_LINE,
    MAX_RECORDS,
    entities,
    validate_content,
)
from replay.economic_sdk.availability import IntervalEngine
from replay.preparation import digest
from replay.strategy_sdk import LineWriter, PreparedInput, plain
from replay.streams.protocol import require
from replay.supervisor import write_json_durable


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
        # The interval engine owns scopes, status, evidence and the rows.
        self.engine = IntervalEngine(self.snapshot, self.writer)
        self.end = int(self.snapshot["config"]["end_ns"])
        self.sequence = -1
        self.window = None
        self.terminal = self.finished = self.poisoned = False

    @property
    def books(self):
        """Detached (validity, reason, source) per planned book."""
        return self.engine.books

    @property
    def trades(self):
        return self.engine.trades

    @property
    def observed_trades(self):
        return self.engine.observed_trades

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
            self.engine.initial(cut)
            return
        require(self.input.bound, "missing initial")
        if cut.kind == "terminal":
            require(
                self.window is not None and int(self.window["end_ns"]) >= self.end,
                "incomplete window coverage",
            )
            self.engine.terminal(self.end)
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
                    raw_time <= self.engine.time < int(origin["end_ns"]),
                    "first window range",
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
        require(self.engine.time <= t < self.end, "coverage time order")
        if origin["kind"] == "window":
            self.window = origin
        self.engine.cut(cut, raw_time, t)
        self.engine.count_trades(cut, raw_time)

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
