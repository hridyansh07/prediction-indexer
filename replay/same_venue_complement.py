"""Runtime for the approved same-venue complement V1 experiment."""

from __future__ import annotations

import sys
from dataclasses import fields, is_dataclass
from pathlib import Path

from replay.complement_contract import (
    FILES, MAX_BYTES, MAX_LINE, MAX_METADATA, MAX_ROWS, MAX_STATE, PAYOUT,
    STRATEGY, Inputs, episode_id, layouts,
)
from replay.economic_fills import Fill, retained_size, walk
from replay.economic_intervals import CutClock, EpisodeMath, changed_keys
from replay.preparation import digest, encoded
from replay.strategy_sdk import LineWriter, plain
from replay.streams.protocol import require
from replay.supervisor import write_json_durable


def _signed(value):
    return str(value)


def _deep_size(value, seen=None, fill_costs=None):
    """Conservative recursive retained-size accounting without serialization."""
    # These closed leaves dominate candidate graphs. Charging repeated immutable
    # scalars is conservative and avoids identity/MRO/dataclass work for each one.
    if value is None or isinstance(value, (bool, int, float, complex, str, bytes, bytearray)):
        return sys.getsizeof(value)
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    if isinstance(value, Fill) and fill_costs is not None:
        cached = fill_costs.get(identity)
        if cached is not None:
            return cached
        cost = retained_size(value)
        fill_costs[identity] = cost
        return cost
    total = sys.getsizeof(value)
    if isinstance(value, dict):
        total += sum(_deep_size(k, seen, fill_costs) + _deep_size(v, seen, fill_costs) for k, v in value.items())
    elif isinstance(value, (tuple, list, set, frozenset)):
        total += sum(_deep_size(v, seen, fill_costs) for v in value)
    elif is_dataclass(value):
        total += sum(_deep_size(getattr(value, field.name), seen, fill_costs) for field in fields(value))
    elif hasattr(value, "__dict__"):
        total += _deep_size(vars(value), seen, fill_costs)
    else:
        for cls in type(value).__mro__:
            slots = cls.__dict__.get("__slots__", ())
            if isinstance(slots, str):
                slots = (slots,)
            for name in slots:
                if name not in {"__dict__", "__weakref__"} and hasattr(value, name):
                    total += _deep_size(getattr(value, name), seen, fill_costs)
    return total


class Complement:
    # Resolver memoization is bounded at 1,024 entries. Reserve a complete
    # maximum-line-sized graph per entry rather than scanning its hot cache.
    _RESOLVER_CACHE_RESERVATION = 1024 * MAX_LINE

    def __init__(self, context):
        self.inputs = Inputs(context["config"])
        self.snapshot, self.policy, self.bridge = self.inputs.snapshot, self.inputs.policy, self.inputs.bridge
        self.root = Path(context["output_directory"])
        require(self.root.is_dir() and not any(self.root.iterdir()), "output directory must be empty")
        self.binding = {k: context[k] for k in ("run_id", "attempt_id", "group", "identity")}
        self.writers = {name: LineWriter(self.root / name, max_bytes=MAX_BYTES,
                                         max_records=MAX_ROWS, max_line_bytes=MAX_LINE) for name in FILES}
        self.clock = CutClock(self.snapshot)
        self.scope = 0
        self.sequence = -1
        self.projections = {}
        self.projection_costs = {}
        self.plan = {(p["instrument"], p["orientation"]): plain(p) for p in self.snapshot["plans"]}
        self.layout = {}
        self.reverse = {}
        self.open_measurements = {}
        self.measurement_costs = {}
        self.episodes = {}
        self.episode_costs = {}
        self.instantaneous = {}
        self.instantaneous_costs = {}
        self.staged_time = None
        self.staged_values = {}
        self.staged_costs = {}
        self._stage_fill_costs = {}
        self.episode_rows = 0
        self.terminal = self.finished = self.poisoned = False
        self._prepared_snapshot_cost = _deep_size(self.inputs.prepared.snapshot)
        # Values shared by several structures are deliberately charged more than
        # once.  That keeps accounting conservative without a global identity
        # walk, and cached entry costs make replacement proportional to the new
        # detached value rather than to all retained state.
        self.state_bytes = self._RESOLVER_CACHE_RESERVATION + sum(_deep_size(value) for value in (
            self.inputs, self.plan, self.layout, self.reverse, self.projections,
            self.projection_costs,
            self.open_measurements, self.episodes, self.instantaneous,
            self.staged_values, self.measurement_costs, self.episode_costs,
            self.instantaneous_costs, self.staged_costs, self.clock,
        ))
        self.layout_cost = _deep_size(self.layout)
        self.reverse_cost = _deep_size(self.reverse)
        self._require_state()

    def __call__(self, cut):
        try:
            require(not self.poisoned and not self.terminal, "closed complement")
            require(cut.sequence == self.sequence + 1, "complement sequence")
            self.sequence = cut.sequence
            if cut.kind == "initial":
                require(cut.sequence == 0)
                self.inputs.prepared.bind(cut.body)
                # Binding is the final consumer of PreparedInput's independently
                # decoded snapshot. Keep only the strategy's plain snapshot.
                self.inputs.prepared.snapshot = None
                self.state_bytes -= self._prepared_snapshot_cost
                self._prepared_snapshot_cost = 0
                for key, book in cut.books.items():
                    self._retain_projection(key, self._project(key, book, self.clock.start, False))
                self._scope(0)
                self._stage(self.clock.start, set(self.layout))
                return
            require(self.inputs.prepared.bound, "missing initial")
            if cut.kind == "terminal":
                end = self.clock.terminal()
                self._flush_stage()
                self._advance(end)
                self._close_scope(end, "RUN_END", True)
                self.terminal = True
                return
            require(cut.kind == "cut")
            raw, time = self.clock.observe(cut)
            if self.staged_time is not None and time > self.staged_time:
                self._flush_stage()
                entered = self._advance(time)
            elif self.staged_time is None:
                entered = self._advance(time)
            else:
                entered = set()
            changed = changed_keys(cut)
            for key in changed:
                self._retain_projection(key, self._project(key, cut.books[key], time, True))
            affected = entered | (set(self.layout) if raw < self.clock.start else set()) | {
                entity for key in changed for entity in self.reverse.get(key, ())
            }
            if affected:
                self._stage(time, affected)
        except Exception:
            self.poisoned = True
            raise

    def _project(self, key, book, time, changed):
        # Book exposes no key; the caller installs this result under the key and
        # quantity scale comes from the immutable plan.
        sizes = tuple(int(size) * 10 ** int(self.plan[key]["quantity_scale"])
                      for size in self.policy["sizes_contracts"])
        bids, asks = tuple(book.levels("bid")), tuple(book.levels("ask"))
        bid_fills = dict(zip(map(int, self.policy["sizes_contracts"]), walk(bids, sizes)))
        projection = {"validity": book.validity, "reason": plain(book.reason),
                "last_change": time if changed else self.clock.start,
                "bid_present": bool(bids), "ask_present": bool(asks),
                "bid_fills": bid_fills,
                "ask_fills": dict(zip(map(int, self.policy["sizes_contracts"]), walk(asks, sizes)))}
        if self.plan[key]["venue"] == "kalshi":
            unit = 10 ** int(self.plan[key]["price_scale"])
            projection["projected_bid_fills"] = {
                size: Fill(fill.filled_atoms, unit * fill.filled_atoms - fill.cost,
                           fill.depth_limited,
                           tuple((unit - price, quantity) for price, quantity in fill.taken),
                           tuple((unit - price, quantity) for price, quantity in fill.consumed))
                for size, fill in bid_fills.items()
            }
        return projection

    def _retain_projection(self, key, projection):
        current = self.projection_costs.get(key, 0)
        cost = self._entry_cost(key, projection)
        self._require_state(cost)
        self.projections[key] = projection
        self.projection_costs[key] = cost
        self.state_bytes += cost - current

    def _scope(self, index):
        layout = layouts(self.snapshot, self.policy, index)
        reverse = {}
        for entity, desc in layout.items():
            for key in self._dependencies(desc):
                reverse.setdefault(key, set()).add(entity)
        layout_cost, reverse_cost = _deep_size(layout), _deep_size(reverse)
        self._require_state(layout_cost + reverse_cost)
        self.layout = layout
        self.reverse = reverse
        self.state_bytes += layout_cost + reverse_cost - self.layout_cost - self.reverse_cost
        self.layout_cost, self.reverse_cost = layout_cost, reverse_cost

    def _opposite(self, leg):
        orientation = "complement" if leg["orientation"] == "outcome" else "outcome"
        return (leg["instrument"], orientation)

    def _dependencies(self, desc):
        return tuple((leg["instrument"], leg["orientation"]) for leg in desc["legs"])

    def _fill(self, key, side, size, project=False):
        state = self.projections[key]
        if project:
            return state["projected_bid_fills"][size]
        fill = state[side + "_fills"][size]
        return fill

    def _evaluate(self, entity):
        desc = self.layout[entity]
        if desc.get("admission") is not None:
            return self._measurement(desc["admission"], [str(len(desc["legs"]))] if desc["admission"] == "UNSUPPORTED_SHAPE" else [])
        size = int(desc["size_contracts"])
        venue, direction = desc["venue"], desc["direction"]
        dependencies = self._dependencies(desc)
        states = [self.projections[k] for k in dependencies]
        if any(s["validity"] != "usable" for s in states):
            return self._measurement("UNUSABLE", [
                f"leg:{index}:{s['validity']}:{encoded(s['reason']) if s['reason'] is not None else 'null'}"
                for index, s in enumerate(states)
            ])
        if venue == "limitless":
            bid, ask = self._fill(dependencies[0], "bid", size), self._fill(dependencies[0], "ask", size)
            if not states[0]["bid_present"] or not states[0]["ask_present"]:
                return self._measurement("ONE_SIDED", [])
            if bid.depth_limited or ask.depth_limited:
                return self._measurement("DEPTH_LIMITED", [str(min(bid.filled_atoms, ask.filled_atoms))])
            value = "LOCKED" if bid.cost == ask.cost else ("CROSSED" if bid.cost > ask.cost else "NOT_CROSSED")
            return self._measurement(value, [], fee="NOT_APPLICABLE")
        side = "ask" if direction == "long" else "bid"
        projected = venue == "kalshi"
        fills = [self._fill(key, "bid" if projected else side, size, projected) for key in dependencies]
        if any(not states[i][("bid" if projected else side) + "_present"] for i in range(2)):
            return self._measurement("ONE_SIDED", [])
        if any(fill.depth_limited for fill in fills):
            return self._measurement("DEPTH_LIMITED", [str(min(f.filled_atoms for f in fills))])
        p = self.plan[dependencies[0]]
        gross_scale = int(p["price_scale"]) + int(p["quantity_scale"])
        unit = size * 10 ** gross_scale
        gross = (unit - sum(f.cost for f in fills)) if direction == "long" else (sum(f.cost for f in fills) - unit)
        if projected:
            # Projected BUY costs are P-p, so U-cost is exactly bid_1+bid_2-U.
            gross = unit - sum(f.cost for f in fills)
        assessment = {"gap_net": None, "fee_status": "SKIPPED", "assessments": [[], []],
                      "reasons": [], "assumptions": [], "evidence": []}
        if gross > 0:
            fee_direction = "SELL" if direction == "short" else "BUY"
            legs = tuple({"market_id": leg["market_id"], "key": self._opposite(leg) if projected else (leg["instrument"], leg["orientation"]),
                          "fill": fill, "price_scale": int(self.plan[(leg["instrument"], leg["orientation"])]["price_scale"]),
                          "quantity_scale": int(self.plan[(leg["instrument"], leg["orientation"])]["quantity_scale"]),
                          "side": fee_direction} for leg, fill in zip(desc["legs"], fills))
            assessment = self.bridge.assess(experiment=self.inputs.experiment_sha256, scope=self.scope,
                                            basket=desc, direction=fee_direction, size=size,
                                            time=self.staged_time if self.staged_time is not None else self.clock.time,
                                            sequence=self.sequence, legs=legs)
        threshold = size * int(self.policy["minimum_net_gap_per_contract_e18"])
        net_positive = assessment["gap_net"] is not None and assessment["gap_net"] > 0 and assessment["gap_net"] >= threshold
        value = "GROSS_NONPOSITIVE" if gross <= 0 else ("FEE_UNKNOWN" if assessment["fee_status"] == "UNKNOWN" else ("NET_POSITIVE" if net_positive else "NET_NONPOSITIVE"))
        skew = abs(states[0]["last_change"] - states[1]["last_change"])
        bucket = next((i for i, edge in enumerate(self.policy["leg_skew_buckets_ns"]) if skew < int(edge)), len(self.policy["leg_skew_buckets_ns"]))
        result = self._measurement("DEPTH_SUFFICIENT", assessment["reasons"], bucket, value, assessment["fee_status"])
        # A nonpositive gross proves both episode predicates false. Keep only the
        # measurement class; fills and fee-shaped structural data are not state.
        if gross <= 0:
            return result
        result["economic"] = {"gross": gross, "gross_scale": gross_scale, "net": assessment["gap_net"],
                              "assessment": assessment, "fills": fills, "net_positive": net_positive}
        return result

    @staticmethod
    def _measurement(status, reasons, skew=None, value=None, fee="NOT_APPLICABLE"):
        return {"status": status, "reasons": reasons, "skew_bucket": skew,
                "value_class": value, "fee_status": fee, "diagnostic": None}

    def _stage(self, time, entities):
        if self.staged_time is None:
            self.staged_time = time
        require(self.staged_time == time, "staging time")
        for entity in sorted(entities):
            previous = self.staged_values.get(entity)
            value = self._evaluate(entity)
            if previous and previous.get("economic", {}).get("gross", 0) > 0 and previous != value:
                self._set_instantaneous(entity, self.instantaneous.get(entity, 0) + 1)
            current = self.staged_costs.get(entity, 0)
            cost = self._entry_cost(entity, value)
            self._require_state(cost)
            self.staged_values[entity] = value
            self.staged_costs[entity] = cost
            self.state_bytes += cost - current

    def _flush_stage(self):
        if self.staged_time is None:
            return
        time = self.staged_time
        for entity in sorted(self.staged_values, key=self._entity_order):
            self._commit(entity, time, self.staged_values[entity])
        self.staged_values.clear()
        self.state_bytes -= sum(self.staged_costs.values())
        self.staged_costs.clear()
        # Identity keys are safe only while this stage's values are retained;
        # clearing also prevents a whole-history cache of fills.
        self._stage_fill_costs.clear()
        self.staged_time = None

    def _commit(self, entity, time, value):
        fields = {k: v for k, v in value.items() if k != "economic"}
        prior = self.open_measurements.get(entity)
        if prior is None or prior[1] != fields:
            if prior is not None:
                self._emit_measurements([(entity, prior, time)])
            self._set_measurement(entity, (time, fields))
        economic = value.get("economic")
        predicates = {"gross": economic is not None and economic["gross"] > 0,
                      "net": economic is not None and economic["net_positive"]}
        for kind, active in predicates.items():
            key = (entity, kind)
            if active:
                if key not in self.episodes:
                    episode = self._open_episode(entity, kind, time, economic, value["skew_bucket"])
                    self._set_episode(key, episode)
                else:
                    self._update_episode(self.episodes[key], time, economic, value["skew_bucket"])
            elif key in self.episodes:
                reason = value["status"] if value["status"] != "DEPTH_SUFFICIENT" else "PREDICATE_FALSE"
                self._close_episodes([(key, time, reason, False)])

    def _values(self, economic):
        a = economic["assessment"]
        return {"gap_gross": _signed(economic["gross"]), "gap_net": None if economic["net"] is None else _signed(economic["net"]),
                "gross_scale": economic["gross_scale"], "net_scale": 18, "fee_status": a["fee_status"],
                "assessments": a["assessments"], "assumptions": a["assumptions"], "evidence": a["evidence"]}

    def _open_episode(self, entity, kind, time, economic, skew):
        values = self._values(economic)
        consumed = [[[str(p), str(q)] for p, q in f.consumed] for f in economic["fills"]]
        episode = {"entity": entity, "kind": kind, "start": time, "open_values": values,
                "max_gross": values["gap_gross"], "max_net": values["gap_net"],
                "opening_skew": skew, "slice_start": time, "slice_skew": skew,
                "slice_values": values, "consumed": consumed, "qualified": {t: 0 for t in self.policy["latency_tiers_ns"]},
                "viable": set(), "opening_survival": None}
        self._check_retained_slice_line(consumed, values)
        return episode

    def _update_episode(self, episode, time, economic, skew):
        # Work on a detached replacement: a failed bound check must leave the
        # currently retained episode unchanged.
        episode = {**episode, "qualified": dict(episode["qualified"]),
                   "viable": set(episode["viable"])}
        values = self._values(economic)
        if int(values["gap_gross"]) > int(episode["max_gross"]):
            episode["max_gross"] = values["gap_gross"]
        if values["gap_net"] is not None and (episode["max_net"] is None or int(values["gap_net"]) > int(episode["max_net"])):
            episode["max_net"] = values["gap_net"]
        consumed = [[[str(p), str(q)] for p, q in f.consumed] for f in economic["fills"]]
        if consumed != episode["consumed"]:
            self._check_retained_slice_line(consumed, values)
            self._emit_slice(episode, time, "CONSUMED_CHANGED", False)
            episode["slice_start"], episode["slice_values"], episode["consumed"], episode["slice_skew"] = time, values, consumed, skew
        key = (episode["entity"], episode["kind"])
        new_cost = self._entry_cost(key, episode)
        old_cost = self.episode_costs[key]
        self._require_state(new_cost - old_cost)
        self.episodes[key] = episode
        self.episode_costs[key] = new_cost
        self.state_bytes += new_cost - old_cost

    def _emit_slice(self, episode, end, reason, censored):
        start = episode["slice_start"]
        if start >= end:
            return
        survival, viable, qualified = EpisodeMath.close(start, end, self.policy["latency_tiers_ns"])
        if episode["opening_survival"] is None:
            episode["opening_survival"] = survival
        episode["viable"].update(viable)
        for tier, amount in qualified.items():
            episode["qualified"][tier] += int(amount)
        row = self._common(episode["entity"], start, end) | {
            "episode_id": episode_id(self.scope, episode["entity"], episode["kind"], episode["start"]),
            "kind": episode["kind"], "end_reason": reason, "censored": censored,
            "consumed": episode["consumed"], "survival_ns": str(survival),
            "viable_tiers": viable, "open_values": episode["slice_values"],
            "opening_skew_bucket": episode["slice_skew"],
        }
        require(len(encoded(row)) + 1 <= MAX_LINE, "slice retained line budget")
        self.writers["slices.ndjson"].append(row)

    def _close_episodes(self, closures):
        closures = sorted(closures, key=lambda item: (
            self._entity_order(item[0][0]), item[0][1], self.episodes[item[0]]["start"]
        ))
        for key, end, reason, censored in closures:
            episode = self.episodes.pop(key)
            self.state_bytes -= self.episode_costs.pop(key)
            self._emit_slice(episode, end, reason, censored)
            if episode["start"] >= end:
                continue
            survival = end - episode["start"]
            row = self._common(episode["entity"], episode["start"], end) | {
                "episode_id": episode_id(self.scope, episode["entity"], episode["kind"], episode["start"]),
                "kind": episode["kind"], "basket": self.layout[episode["entity"]], "end_reason": reason,
                "censored": censored, "gap_lifetime_ns": str(survival),
                "opening_slice_survival_ns": str(episode["opening_survival"] or 0),
                "viable_tiers": [t for t in self.policy["latency_tiers_ns"] if t in episode["viable"]],
                "qualified_ns": {t: str(v) for t, v in episode["qualified"].items()},
                "open_values": episode["open_values"], "max_gap_gross": episode["max_gross"],
                "max_gap_net": episode["max_net"], "opening_skew_bucket": episode["opening_skew"],
            }
            require(self.episode_rows < MAX_ROWS, "combined episode row budget")
            name = "placebo_episodes.ndjson" if self.layout[row["entity"]]["placebo"] else "episodes.ndjson"
            self.writers[name].append(row)
            self.episode_rows += 1

    def _emit_measurements(self, closures):
        for entity, (start, fields), end in sorted(
            closures, key=lambda item: (item[2], self._entity_order(item[0]), item[1][0])
        ):
            if start < end:
                self.writers["measurements.ndjson"].append(
                    self._common(entity, start, end) | fields
                )

    def _common(self, entity, start, end):
        return {"version": 1, "experiment_sha256": self.inputs.experiment_sha256,
                "scope": self.scope, "entity": entity, "start_ns": str(start), "end_ns": str(end)}

    def _entity_order(self, entity):
        d = self.layout[entity]
        return (d["venue"], d["market_id"], d["direction"],
                int(d["size_contracts"]), d["placebo"])

    def _advance(self, time):
        entered = set()
        for _, boundary, new_scope in self.clock.advance(time):
            self._close_scope(boundary, "SCOPE_END", False)
            self.scope = new_scope
            self._scope(new_scope)
            entered = set(self.layout)
            self._stage(boundary, entered)
            if boundary < time:
                # A quiet scope interval starts with the last observed prices,
                # before the already-decoded future cut refreshes projections.
                self._flush_stage()
                entered = set()
        return entered

    def _close_scope(self, time, reason, censored):
        self._emit_measurements([(e, p, time) for e, p in self.open_measurements.items()])
        self.open_measurements.clear()
        self.state_bytes -= sum(self.measurement_costs.values())
        self.measurement_costs.clear()
        self._close_episodes([(key, time, reason, censored) for key in sorted(self.episodes)])

    def _entry_cost(self, key, value):
        # 256 covers dict/set slot growth and allocator rounding without relying
        # on the current CPython table layout.
        return _deep_size(key) + _deep_size(value, fill_costs=self._stage_fill_costs) + 256

    def _require_state(self, delta=0):
        require(self.state_bytes + delta <= MAX_STATE, "detached state budget")

    def _set_measurement(self, entity, value):
        old = self.measurement_costs.get(entity, 0)
        cost = self._entry_cost(entity, value)
        self._require_state(cost)
        self.open_measurements[entity] = value
        self.measurement_costs[entity] = cost
        self.state_bytes += cost - old

    def _set_episode(self, key, episode):
        cost = self._entry_cost(key, episode)
        self._require_state(cost)
        self.episodes[key] = episode
        self.episode_costs[key] = cost
        self.state_bytes += cost

    def _set_instantaneous(self, entity, value):
        old = self.instantaneous_costs.get(entity, 0)
        cost = self._entry_cost(entity, value)
        self._require_state(cost)
        self.instantaneous[entity] = value
        self.instantaneous_costs[entity] = cost
        self.state_bytes += cost - old

    @staticmethod
    def _check_retained_slice_line(consumed, values):
        # This conservative envelope is checked for both the initial and every
        # replacement slice, before either is retained by an open episode.
        require(len(encoded({"consumed": consumed, "open_values": values})) + 1024 <= MAX_LINE,
                "slice retained line budget")

    def _release_runtime(self):
        for value in (self.projections, self.projection_costs, self.plan, self.layout,
                      self.reverse, self.open_measurements, self.measurement_costs,
                      self.episodes, self.episode_costs, self.staged_values,
                      self.staged_costs, self._stage_fill_costs):
            value.clear()
        self.clock.window = None
        self.bridge = None
        self.inputs = None
        self.snapshot = None
        self.policy = None
        self.state_bytes = 0

    def finish(self):
        try:
            require(self.terminal and not self.poisoned and not self.finished, "missing terminal / failed complement")
            files = {name: writer.finish() for name, writer in self.writers.items()}
            manifest = {"version": 1, "strategy": STRATEGY, "snapshot_sha256": self.inputs.prepared.sha256,
                        "policy": self.policy, "policy_sha256": self.inputs.policy_sha256,
                        "experiment_sha256": self.inputs.experiment_sha256,
                        "fee_config": self.bridge.semantic_config, "fee_engine_identity": self.bridge.engine_identity,
                        "files": files, "instantaneous_positive": self.instantaneous,
                        "payout_assumption": PAYOUT}
            reader_snapshot = self.snapshot
            # The reader owns its own bounded state.  Drop projections and all
            # other runtime-only structures before starting it so the two peaks
            # cannot overlap.  The manifest now owns the small diagnostic map.
            self.instantaneous = {}
            self.instantaneous_costs.clear()
            self._release_runtime()
            from replay.complement_output import validate_content
            summary = validate_content(self.root, reader_snapshot, manifest)
            # Keep the existing post-finish inspection surface without carrying
            # this layout through independent validation.
            self.layout = layouts(reader_snapshot, manifest["policy"], self.scope)
            require(len(encoded(summary)) <= MAX_METADATA, "summary budget")
            write_json_durable(self.root / "summary.json", summary)
            manifest["summary_sha256"] = digest(summary)
            require(len(encoded(manifest)) <= MAX_METADATA, "manifest budget")
            write_json_durable(self.root / "manifest.json", manifest)
            write_json_durable(self.root / "content_receipt.json", {"version": 1,
                "semantic_sha256": digest(manifest), **self.binding, "terminal": self.sequence + 1})
            self.finished = True
        except Exception:
            self.poisoned = True
            raise


def build(context):
    return Complement(context)
