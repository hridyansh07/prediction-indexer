"""The economic strategy runtime: time, staging, intervals, episodes, slices.

A strategy supplies requirements, baskets and a pure ``evaluate``; this module
owns everything else (complement spec §§3, 5, 7 and SDK spec §4):

- the cut clock, quiet scope entry and pre-start prologue initialization;
- one bounded read and one walk per changed book side per cut, shared by
  every real and control entity through detached ``BookView`` objects;
- same-time staging: only the final state at a time is committed, superseded
  positive intermediates are counted in ``instantaneous_positive``;
- half-open measurement intervals, episodes and slices with zero-length
  suppression, ``SCOPE_END`` closure and censoring at run end;
- controls, including the bounded view ring of ``time_shift``;
- count-based retained-state bounds and bounded output in close order.
"""

from __future__ import annotations

import heapq
from pathlib import Path

from replay.economic_fills import Fill  # noqa: F401  (re-exported for strategies)
from replay.economic_intervals import CutClock, EpisodeMath
from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.output import Layout, row_common
from replay.economic_sdk.types import CONTROL, EVALUATED, REAL, BookRequirement, Context, Observation
from replay.economic_sdk.views import ViewBuilder, touched_sides, unavailable_view
from replay.preparation import digest, encoded
from replay.strategy_sdk import LineWriter, plain
from replay.streams.protocol import require
from replay.supervisor import write_json_durable


def episode_id(scope, entity, kind, start):
    return digest([scope, entity, kind, str(start)])


def quotes_json(quotes):
    return [[[str(price), str(quantity)] for price, quantity in leg] for leg in quotes]


class _Episode:
    __slots__ = ("entity", "kind", "start", "open_values", "open_quotes", "maxima",
                 "opening_skew", "slice_start", "slice_skew", "slice_values",
                 "quotes", "qualified", "viable", "opening_survival", "cost")


# Book sides each declared input source reads.
_SOURCE_SIDES = {"best": frozenset(("bid", "ask")), "kalshi_complement_ask": frozenset(("bid",))}


def _fingerprint(view, sources):
    """Everything a declared input reads from one leg's view."""
    if view.validity != "usable":
        return view.validity, view.reason
    parts = []
    for source, size in sources:
        if source == "best":
            parts.append((view.best_bid, view.best_ask))
        elif source in view.fills:
            parts.append((view.fills[source][size], view.present(source)))
        else:
            parts.append(view.transformed[source][size])
    return tuple(parts)


class Runtime:
    """Supervisor-facing callable wrapping one configured ``Strategy``."""

    def __init__(self, strategy, context):
        self.strategy = strategy
        experiment = self.experiment = strategy.experiment
        self.snapshot = strategy.snapshot
        self.root = Path(context["output_directory"])
        require(self.root.is_dir() and not any(self.root.iterdir()), "output directory must be empty")
        self.binding = {k: context[k] for k in ("run_id", "attempt_id", "group", "identity")}
        self.layout_files = Layout(experiment)
        self.writers = {name: LineWriter(self.root / name, max_bytes=bounds.MAX_BYTES,
                                         max_records=bounds.MAX_ROWS,
                                         max_line_bytes=bounds.MAX_LINE)
                        for name in self.layout_files.files}
        self.clock = CutClock(self.snapshot)
        self.plans = {(p["instrument"], p["orientation"]): plain(p) for p in self.snapshot["plans"]}
        self.budget = bounds.StateBudget()
        self.budget.charge(experiment.static_reservation + bounds.json_cost(self.snapshot)
                           + bounds.json_cost(experiment.policy))

        requirements = strategy.requirements(self.snapshot, experiment.policy)
        self.builders = {}
        for key, requirement in sorted(requirements.books.items()):
            require(key in self.plans and type(requirement) is BookRequirement, "book requirement")
            self.builders[key] = ViewBuilder(requirement, self.plans[key])
        self.profile = None
        if requirements.profile is not None:
            from replay.economic_sdk.profile import Collector
            self.profile = Collector(requirements.profile, self.snapshot, strategy.snapshot_sha256,
                                     self.root, experiment.experiment_sha256, self.budget)

        self.kind_classes = {value: frozenset(k for k in experiment.kinds
                                              if value in experiment.episode_classes[k])
                             for value in experiment.value_classes}
        self.kind_classes[None] = frozenset()
        self.statuses = frozenset(("UNUSABLE", "ONE_SIDED", "DEPTH_LIMITED", EVALUATED)
                                  + experiment.diagnostic_statuses)
        self.tiers = experiment.tiers_ns
        self.tier_ints = tuple(int(t) for t in self.tiers)
        self.edges = experiment.skew_edges_ns

        # Time-shift history: one ring per (book, shift) over every scope, so a
        # control entering a later scope still sees exact shifted history.
        self.shifts = {}
        for index in range(len(self.snapshot["scopes"])):
            for entity in resolve(strategy, self.snapshot, index, self.plans).values():
                if entity.shift is not None:
                    key = entity.legs[entity.shift[0]]
                    self.shifts.setdefault(key, set()).add(entity.shift[1])
        self.rings = {key: [] for key in self.shifts}
        self.ring_costs = {key: [] for key in self.shifts}
        self.timers = []

        self.scope = 0
        self.sequence = -1
        self.views = {}
        self.view_costs = {}
        self.entities = {}
        self.entities_cost = 0
        self.reverse = {}
        self.shift_reverse = {}
        self.memo = {}
        self.reads = {}
        self.current = {}
        self._plain_costs = {}
        self.open_measurements = {}
        self.episodes = {}
        self.instantaneous = {}
        self.staged_time = None
        self.staged = {}
        self.staged_books = set()
        self.episode_rows = 0
        self.terminal = self.finished = self.poisoned = False

    @property
    def layout(self):
        """Descriptors of the current scope's entities, for inspection only."""
        return {entity_id: entity.descriptor for entity_id, entity in self.entities.items()}

    # -- callback ---------------------------------------------------------
    def __call__(self, cut):
        try:
            require(not self.poisoned and not self.terminal, "closed economic strategy")
            require(cut.sequence == self.sequence + 1, "economic sequence")
            self.sequence = cut.sequence
            if cut.kind == "initial":
                require(cut.sequence == 0)
                self.strategy.bind(cut.body)
                start = self.clock.start
                for key, builder in self.builders.items():
                    self._set_view(key, builder.build(cut.books[key], start))
                for key in self.rings:
                    self._ring_append(key, start, self.views[key])
                if self.profile is not None:
                    self.profile.initial(cut, start)
                self._scope(0)
                self._stage(start, set(self.entities))
                return
            require(self.strategy.bound, "missing initial")
            if cut.kind == "terminal":
                end = self.clock.terminal()
                self._flush_stage()
                self._advance(end)
                self._close_scope(end, "RUN_END", True)
                if self.profile is not None:
                    self.profile.terminal(end)
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
            if self.profile is not None:
                self.profile.cut(cut, raw, time)
            changed = touched_sides(cut)
            affected = set(entered)
            if raw < self.clock.start:
                affected.update(self.entities)
            for key, sides in changed.items():
                builder = self.builders.get(key)
                if builder is not None:
                    self._set_view(key, builder.build(cut.books[key], time, self.views.get(key), sides))
                    if key in self.rings:
                        self.staged_books.add(key)
            for key, sides in changed.items():
                self._select(key, sides, time, affected)
            # A changed time-shift book must reach its ring at this exact time,
            # even when no entity is staged now.
            if affected or self.staged_books:
                self._stage(time, affected)
        except Exception:
            self.poisoned = True
            raise

    def _select(self, key, sides, time, affected):
        """Add the entities a change to ``key`` can change to ``affected``.

        An entity whose current observation is context-free and whose declared
        inputs on ``key`` were not touched would re-derive the same observation;
        only its skew bucket can move, so it is staged only when that bucket
        changes. Everything else is re-evaluated, exactly as without inputs.
        """
        reads, staged, current = self.reads, self.staged, self.current
        for entity_id in self.reverse.get(key, ()):
            if entity_id in affected:
                continue
            value = staged.get(entity_id)
            value = value[0] if value is not None else current.get(entity_id)
            read = reads.get((entity_id, key))
            if (value is None or not value[0].context_free or read is None or sides is None
                    or not read.isdisjoint(sides)):
                affected.add(entity_id)
            elif value[0].status == EVALUATED:
                entity = self.entities[entity_id]
                if self._skew(entity, self._views(entity, time), value[0].skew_legs) != value[1]:
                    affected.add(entity_id)

    # -- views and history ---------------------------------------------------
    def _set_view(self, key, view):
        cost = bounds.view_cost(view, len(self.builders[key].sizes))
        self.budget.replace(self.view_costs.get(key, 0), cost)
        self.views[key] = view
        self.view_costs[key] = cost

    def _ring_append(self, key, time, view):
        ring, costs = self.rings[key], self.ring_costs[key]
        if ring and ring[-1][0] == time:
            self.budget.release(costs.pop())
            ring.pop()
        require(len(ring) < self.experiment.ring_entries, "time_shift ring bound")
        cost = bounds.RING_ENTRY + bounds.view_cost(view, len(self.builders[key].sizes))
        self.budget.charge(cost)
        ring.append((time, view))
        costs.append(cost)
        for shift in sorted(self.shifts[key]):
            due = time + shift
            if due < self.clock.end:
                heapq.heappush(self.timers, (due, key, shift))

    def _shifted(self, key, shift, time):
        """Committed view of ``key`` at ``time - shift``; pruned to the oldest need."""
        ring = self.rings[key]
        target = time - shift
        index = None
        for i in range(len(ring) - 1, -1, -1):
            if ring[i][0] <= target:
                index = i
                break
        if index is None:
            return unavailable_view(self.clock.start)
        # Times only advance, so entries before the one needed by the largest
        # shift are never needed again.
        keep = time - max(self.shifts[key])
        drop = 0
        while drop + 1 < len(ring) and ring[drop + 1][0] <= keep:
            drop += 1
        if drop:
            del ring[:drop]
            self.budget.release(sum(self.ring_costs[key][:drop]))
            del self.ring_costs[key][:drop]
            index -= drop
        return ring[index][1]

    # -- scopes ---------------------------------------------------------------
    def _scope(self, index):
        entities = resolve(self.strategy, self.snapshot, index, self.plans)
        cost = sum(bounds.ENTITY + bounds.json_cost(e.descriptor) for e in entities.values())
        self.budget.replace(self.entities_cost, cost)
        self.entities_cost = cost
        self.entities = entities
        self.budget.release(sum(memo[2] for memo in self.memo.values()))
        self.memo = {}
        self.current = {}
        self.reads = {}
        self.reverse, self.shift_reverse = {}, {}
        for entity in entities.values():
            if entity.admission is not None:
                continue
            inputs = entity.basket.inputs
            for position, key in enumerate(entity.legs):
                if inputs is not None:
                    read = self.reads.get((entity.id, key), frozenset())
                    for source, _ in inputs[position]:
                        read = read | _SOURCE_SIDES.get(source, frozenset((source,)))
                    self.reads[entity.id, key] = read
                if entity.shift is not None and entity.shift[0] == position:
                    self.shift_reverse.setdefault((key, entity.shift[1]), set()).add(entity.id)
                else:
                    self.reverse.setdefault(key, set()).add(entity.id)
        require(all(key in self.builders for key in self.reverse), "unrequired basket book")

    def _advance(self, time):
        """Process scope boundaries and shift timers up to ``time`` in order."""
        entered = set()
        while True:
            boundary = (int(self.clock.scopes[self.clock.scope]["end_ns"])
                        if self.clock.scope + 1 < len(self.clock.scopes) else None)
            if boundary is not None and boundary > time:
                boundary = None
            due = self.timers[0][0] if self.timers and self.timers[0][0] <= time else None
            if boundary is None and due is None:
                break
            at = min(x for x in (boundary, due) if x is not None)
            affected = set()
            while self.timers and self.timers[0][0] == at:
                _, key, shift = heapq.heappop(self.timers)
                affected.update(self.shift_reverse.get((key, shift), ()))
            if boundary == at:
                self._close_scope(at, "SCOPE_END", False)
                for _ in self.clock.advance(at):
                    pass
                self.scope = self.clock.scope
                self._scope(self.scope)
                affected = set(self.entities)
            if affected:
                self._stage(at, affected)
            if at < time:
                # A quiet interval starts with the last observed prices, before
                # the already-decoded future cut refreshes projections.
                self._flush_stage()
                entered = set()
            else:
                entered = affected
        self.clock.time = time
        return entered

    # -- evaluation and staging ---------------------------------------------------
    def _evaluate(self, entity_id):
        entity = self.entities[entity_id]
        if entity.admission is not None:
            return (Observation(entity.admission, entity.admission_reasons,
                                fields=self.experiment.unevaluated_fields), None)
        views = self._views(entity, self.staged_time)
        inputs = entity.basket.inputs
        fingerprint = None
        if inputs is not None:
            fingerprint = tuple(_fingerprint(view, sources) for view, sources in zip(views, inputs))
            memo = self.memo.get(entity_id)
            if memo is not None and memo[0] == fingerprint:
                observation = memo[1]
            else:
                observation = self._fresh(entity, views)
                old = memo[2] if memo is not None else 0
                if observation.context_free:
                    # The fingerprint may outlive its views, so its fills are charged.
                    cost = bounds.fingerprint_cost(fingerprint) + self._observation_cost(observation)
                    self.budget.replace(old, cost)
                    self.memo[entity_id] = (fingerprint, observation, cost)
                elif memo is not None:
                    self.budget.release(old)
                    del self.memo[entity_id]
        else:
            observation = self._fresh(entity, views)
        skew = None
        if observation.status == EVALUATED:
            skew = self._skew(entity, views, observation.skew_legs)
        return observation, skew

    def _views(self, entity, time):
        shift = entity.shift
        if shift is None:
            return tuple(self.views[key] for key in entity.legs)
        return tuple(self._shifted(key, shift[1], time) if position == shift[0]
                     else self.views[key] for position, key in enumerate(entity.legs))

    def _skew(self, entity, views, legs):
        """Skew bucket of the legs' last-change spread (half-open edges)."""
        legs = legs or range(len(views))
        low = high = views[legs[0]].last_change
        for i in legs:
            change = views[i].last_change
            if change < low:
                low = change
            elif change > high:
                high = change
        spread = high - low
        for i, edge in enumerate(self.edges):
            if spread < edge:
                return i
        return len(self.edges)

    def _fresh(self, entity, views):
        context = Context(self.staged_time, self.sequence, self.scope,
                          self.experiment.experiment_sha256)
        observation = self.strategy.evaluate(entity, views, context)
        require(type(observation) is Observation and observation.status in self.statuses,
                "observation status")
        require(len(observation.fields) == len(self.experiment.measurement_fields),
                "observation fields")
        require((observation.value_class is not None) == (observation.status == EVALUATED)
                and observation.value_class in self.kind_classes, "observation value class")
        require(observation.predicates == self.kind_classes[observation.value_class],
                "observation predicates contradict the declared class map")
        if observation.predicates:
            require(observation.payload is not None and observation.quotes is not None,
                    "episode observation payload")
            for leg in observation.quotes:
                require(len(leg) <= bounds.MAX_CONSUMED_LEVELS, "consumed levels per slice")
        return observation

    def _observation_cost(self, observation):
        if observation.payload is None and not observation.reasons:
            cost = self._plain_costs.get(observation.fields)
            if cost is None:
                cost = self._plain_costs[observation.fields] = bounds.observation_cost(observation)
            return cost
        return bounds.observation_cost(observation)

    def _stage(self, time, entities):
        if self.staged_time is None:
            self.staged_time = time
        require(self.staged_time == time, "staging time")
        for entity in sorted(entities):
            previous = self.staged.get(entity)
            value = self._evaluate(entity)
            cost = self._observation_cost(value[0])
            if previous is not None:
                if previous[0][0].predicates and previous[0] != value:
                    self.instantaneous[entity] = self.instantaneous.get(entity, 0) + 1
                    if self.instantaneous[entity] == 1:
                        self.budget.charge(bounds.SLOT + bounds.STR + 64 + bounds.INT)
                self.budget.replace(previous[1], cost)
            else:
                self.budget.charge(cost)
            self.staged[entity] = (value, cost)

    def _flush_stage(self):
        if self.staged_time is None:
            return
        time = self.staged_time
        entities = self.entities
        for entity in sorted(self.staged, key=lambda e: entities[e].order):
            self._commit(entities[entity], time, self.staged[entity][0])
        self.budget.release(sum(cost for _, cost in self.staged.values()))
        self.staged.clear()
        for key in sorted(self.staged_books):
            self._ring_append(key, time, self.views[key])
        self.staged_books.clear()
        self.staged_time = None

    # -- intervals, episodes and slices ---------------------------------------
    def _commit(self, entity, time, value):
        observation, skew = value
        self.current[entity.id] = value
        fields = (observation.status, observation.reasons, skew, observation.value_class,
                  observation.fields)
        prior = self.open_measurements.get(entity.id)
        if prior is None or prior[1] != fields:
            if prior is not None:
                self._emit_measurements([(entity.id, prior, time)])
            cost = bounds.MEASUREMENT + self._observation_cost(observation) - bounds.OBSERVATION
            self.budget.replace(prior[2] if prior is not None else 0, cost)
            self.open_measurements[entity.id] = (time, fields, cost)
        if self.experiment.detail[entity.cls] == "intervals":
            return
        for kind in self.experiment.kinds:
            key = (entity.id, kind)
            if kind in observation.predicates:
                if key not in self.episodes:
                    self._open_episode(key, time, observation, skew)
                else:
                    self._update_episode(self.episodes[key], time, observation, skew)
            elif key in self.episodes:
                reason = observation.status if observation.status != EVALUATED else "PREDICATE_FALSE"
                self._close_episodes([(key, time, reason, False)])

    def _maxima(self, payload, current=None):
        result = {}
        for name in self.experiment.maxima:
            value = payload[name]
            prior = None if current is None else current[name]
            if value is not None and (prior is None or int(value) > int(prior)):
                prior = value
            result[name] = prior
        return result

    def _check_retained_slice(self, quotes, payload):
        # Checked for the initial and every replacement slice, before retention.
        require(len(encoded({"consumed": quotes_json(quotes), "open_values": payload}))
                + 1024 <= bounds.MAX_LINE, "slice retained line budget")

    def _open_episode(self, key, time, observation, skew):
        self._check_retained_slice(observation.quotes, observation.payload)
        episode = _Episode()
        episode.entity, episode.kind, episode.start = key[0], key[1], time
        episode.open_values = episode.slice_values = observation.payload
        episode.open_quotes = episode.quotes = observation.quotes
        episode.maxima = self._maxima(observation.payload)
        episode.opening_skew = episode.slice_skew = skew
        episode.slice_start = time
        episode.qualified = {t: 0 for t in self.tiers}
        episode.viable = set()
        episode.opening_survival = None
        episode.cost = bounds.episode_cost(observation, len(self.tiers))
        require(len(self.episodes) < len(self.entities) * len(self.experiment.kinds),
                "open episode bound")
        self.budget.charge(episode.cost)
        self.episodes[key] = episode

    def _update_episode(self, episode, time, observation, skew):
        payload = observation.payload
        if observation.quotes != episode.quotes:
            self._check_retained_slice(observation.quotes, payload)
            cost = bounds.episode_cost(observation, len(self.tiers))
            self.budget.replace(episode.cost, cost)
            episode.cost = cost
            self._emit_slice(episode, time, "CONSUMED_CHANGED", False)
            episode.slice_start, episode.slice_values = time, payload
            episode.quotes, episode.slice_skew = observation.quotes, skew
        episode.maxima = self._maxima(payload, episode.maxima)

    def _emit_slice(self, episode, end, reason, censored):
        start = episode.slice_start
        if start >= end:
            return
        survival, viable, qualified = EpisodeMath.close(start, end, self.tiers)
        if episode.opening_survival is None:
            episode.opening_survival = survival
        episode.viable.update(viable)
        for tier, amount in qualified.items():
            episode.qualified[tier] += int(amount)
        entity = self.entities[episode.entity]
        identity = episode_id(self.scope, episode.entity, episode.kind, episode.start)
        if self.experiment.detail[entity.cls] == "slices":
            row = self._common(episode.entity, start, end) | {
                "episode_id": identity, "kind": episode.kind, "end_reason": reason,
                "censored": censored, "consumed": quotes_json(episode.quotes),
                "survival_ns": str(survival), "viable_tiers": viable,
                "open_values": episode.slice_values, "opening_skew_bucket": episode.slice_skew,
            }
            require(len(encoded(row)) + 1 <= bounds.MAX_LINE, "slice retained line budget")
        else:
            row = {"episode_id": identity, "start_ns": str(start), "end_ns": str(end),
                   "end_reason": reason, "censored": censored}
        self.writers[self.layout_files.name("slices", entity.cls)].append(row)

    def _close_episodes(self, closures):
        entities = self.entities
        closures = sorted(closures, key=lambda item: (
            entities[item[0][0]].order, item[0][1], self.episodes[item[0]].start))
        for key, end, reason, censored in closures:
            episode = self.episodes.pop(key)
            self.budget.release(episode.cost)
            self._emit_slice(episode, end, reason, censored)
            if episode.start >= end:
                continue
            entity = entities[episode.entity]
            row = self._common(episode.entity, episode.start, end) | {
                "episode_id": episode_id(self.scope, episode.entity, episode.kind, episode.start),
                "kind": episode.kind, "basket": entity.descriptor, "end_reason": reason,
                "censored": censored, "gap_lifetime_ns": str(end - episode.start),
                "opening_slice_survival_ns": str(episode.opening_survival or 0),
                "viable_tiers": [t for t in self.tiers if t in episode.viable],
                "qualified_ns": {t: str(v) for t, v in episode.qualified.items()},
                "open_values": episode.open_values,
                "opening_skew_bucket": episode.opening_skew,
            } | {"max_" + name: value for name, value in episode.maxima.items()}
            if self.experiment.detail[entity.cls] == "episodes":
                row["open_quotes"] = quotes_json(episode.open_quotes)
            require(self.episode_rows < bounds.MAX_ROWS, "combined episode row budget")
            self.writers[self.layout_files.name("episodes", entity.cls)].append(row)
            self.episode_rows += 1

    def _emit_measurements(self, closures):
        entities = self.entities
        names = self.experiment.measurement_fields
        for entity, (start, fields, _), end in sorted(
            closures, key=lambda item: (item[2], entities[item[0]].order, item[1][0])
        ):
            if start < end:
                status, reasons, skew, value_class, extra = fields
                row = self._common(entity, start, end) | {
                    "status": status, "reasons": list(reasons), "skew_bucket": skew,
                    "value_class": value_class} | dict(zip(names, extra))
                self.writers[self.layout_files.name("measurements", entities[entity].cls)].append(row)

    def _common(self, entity, start, end):
        return row_common(self.layout_files.version, self.experiment.experiment_sha256,
                          self.scope, entity, start, end)

    def _close_scope(self, time, reason, censored):
        self._emit_measurements([(e, p, time) for e, p in self.open_measurements.items()])
        self.budget.release(sum(p[2] for p in self.open_measurements.values()))
        self.open_measurements.clear()
        self._close_episodes([(key, time, reason, censored) for key in sorted(self.episodes)])

    # -- completion ------------------------------------------------------------
    def finish(self):
        try:
            require(self.terminal and not self.poisoned and not self.finished,
                    "missing terminal / failed economic strategy")
            files = {name: writer.finish() for name, writer in self.writers.items()}
            if self.profile is not None:
                files |= self.profile.finish_files()
            manifest = self.strategy.manifest(files, self.instantaneous)
            snapshot, strategy = self.snapshot, self.strategy
            # Release runtime state before the reader so the two peaks never overlap.
            self.views.clear(); self.rings.clear(); self.staged.clear()
            self.open_measurements.clear(); self.episodes.clear(); self.reverse.clear()
            self.budget = None
            summary = strategy.validate(self.root, snapshot, manifest)
            require(len(encoded(summary)) <= bounds.MAX_METADATA, "summary budget")
            write_json_durable(self.root / "summary.json", summary)
            manifest["summary_sha256"] = digest(summary)
            require(len(encoded(manifest)) <= bounds.MAX_METADATA, "manifest budget")
            write_json_durable(self.root / "manifest.json", manifest)
            write_json_durable(self.root / "content_receipt.json", {
                "version": 1, "semantic_sha256": digest(manifest), **self.binding,
                "terminal": self.sequence + 1})
            self.finished = True
        except Exception:
            self.poisoned = True
            raise
