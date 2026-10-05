"""The economic strategy runtime: time, staging, intervals, episodes, slices.

A strategy supplies requirements, baskets and a pure ``evaluate``; this module
owns everything else (complement spec §§3, 5, 7 and SDK spec §4):

- the cut clock, quiet scope entry and pre-start prologue initialization;
- one read and one walk per touched book side per cut, shared by every real
  and control entity through detached ``BookView`` objects;
- same-time staging: only the final state at a time is committed, superseded
  positive intermediates are counted in ``instantaneous_positive``;
- per-key status/value-class time, episodes and slices with zero-length
  suppression, ``SCOPE_END`` closure and censoring at run end;
- controls, including the bounded view ring of ``time_shift``;
- count-based retained-state bounds and bounded output in close order.

Layout 1 (complement policy 1) writes the frozen V1 interval partition, with
skew as a measurement dimension. Layout 2 never splits time on skew: skew is
recorded at episode and slice open, and latency-qualified entry time is
attributed to the skew at each entry instant, inside positive slices only.
"""

from __future__ import annotations

import heapq
import json
from bisect import bisect_right
from operator import is_
from pathlib import Path

from replay.economic_fills import Fill  # noqa: F401  (re-exported for strategies)
from replay.economic_intervals import CutClock, EpisodeMath
from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.output import Layout, aggregate_files, group_of, row_common
from replay.economic_sdk.types import CONTROL, EVALUATED, REAL, BookRequirement, Context, Observation
from replay.economic_sdk.views import ViewBuilder, touched_sides, unavailable_view
from replay.preparation import digest, encoded
from replay.strategy_sdk import LineWriter, plain
from replay.streams.protocol import require
from replay.supervisor import write_json_durable

# Book sides each declared input source reads.
_SOURCE_SIDES = {"best": frozenset(("bid", "ask")), "crossed": frozenset(("bid", "ask")),
                 "kalshi_complement_ask": frozenset(("bid",))}
_FILL, _TRANSFORM, _BEST, _CROSSED = 0, 1, 2, 3


def episode_id(scope, entity, kind, start):
    return digest([scope, entity, kind, str(start)])


def quotes_json(quotes):
    return [[[str(price), str(quantity)] for price, quantity in leg] for leg in quotes]


class _Ring:
    """Committed views of one book in time order, with shared-fill refcounts."""

    __slots__ = ("times", "views", "costs", "fills")

    def __init__(self):
        self.times, self.views, self.costs, self.fills = [], [], [], {}


def _view_fills(view):
    for group in (view.fills, view.transformed):
        for fills in group.values():
            yield from fills.values()


class _Episode:
    __slots__ = ("entity", "kind", "start", "open_values", "open_quotes", "maxima",
                 "opening_skew", "slice_start", "slice_skew", "slice_values",
                 "quotes", "qualified", "viable", "opening_survival", "cost",
                 # layout 2 only
                 "open_class", "open_reasons", "class_now", "class_since", "slice_classes",
                 "class_ns", "qualifying", "skew_points", "by_skew", "at_max")


def _fingerprint(views, plan):
    """Every object a declared input reads, flattened; compared by identity.

    Equal fills are interned by the view builder, so an unchanged input is the
    same object. A false mismatch only costs a re-evaluation.
    """
    parts = []
    for view, sources in zip(views, plan):
        if view.validity != "usable":
            parts.append(view.validity)
            parts.append(view.reason)
            continue
        for kind, name, size in sources:
            if kind == _FILL:
                parts.append(view.fills[name][size])
            elif kind == _TRANSFORM:
                parts.append(view.transformed[name][size])
            elif kind == _CROSSED:
                parts.append(view.crossed)
            else:
                parts.append(view.best_bid)
                parts.append(view.best_ask)
    return parts


def _same(left, right):
    return len(left) == len(right) and all(map(is_, left, right))


class Runtime:
    """Supervisor-facing callable wrapping one configured ``Strategy``."""

    def __init__(self, strategy, context):
        self.strategy = strategy
        experiment = self.experiment = strategy.experiment
        self.legacy = experiment.layout == 1
        self.snapshot = strategy.snapshot
        self.root = Path(context["output_directory"])
        require(self.root.is_dir() and not any(self.root.iterdir()), "output directory must be empty")
        self.binding = {k: context[k] for k in ("run_id", "attempt_id", "group", "identity")}
        if self.legacy:
            self.layout_files = Layout(experiment)
            names = self.layout_files.files
        else:
            self.groups = aggregate_files(experiment)
            names = tuple(group + name for group, files in self.groups.items()
                          for name in files if name.endswith(".ndjson"))
        self.writers = {}
        for name in names:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            self.writers[name] = LineWriter(path, max_bytes=bounds.MAX_BYTES,
                                            max_records=bounds.MAX_ROWS,
                                            max_line_bytes=bounds.MAX_LINE)
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
                                     self.root, experiment.experiment_sha256, self.budget,
                                     views=self.views_for_profile)

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
        self.rings = {key: _Ring() for key in self.shifts}
        self.timers = []

        self.scope = 0
        self.sequence = -1
        self.views = {}
        self.view_costs = {}
        self.entities = {}
        self.entities_cost = 0
        self.index = {}
        self.reverse = {}
        self.shift_reverse = {}
        self.memo = {}
        self.reads = {}
        self.plans_of = {}
        self.current = {}
        self._plain_costs = {}
        self.open_measurements = {}
        self.denominators, self.denominator_cost = {}, 0
        self.reasons, self.reason_list = {}, []
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

    def views_for_profile(self, key):
        return self.views.get(key)

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
            if self.profile is not None:
                self.profile.cut(cut, raw, time, changed)
            skew_only = set()
            for key, sides in changed.items():
                self._select(key, sides, time, affected, skew_only)
            # A changed time-shift book must reach its ring at this exact time,
            # even when no entity is staged now.
            if affected or self.staged_books:
                self._stage(time, affected)
            # Resolve every economic dependency first: another changed leg in
            # this cut may require evaluation rather than a skew-only refresh.
            for entity_id in sorted(skew_only - affected):
                self._stage_skew(entity_id, time)
        except Exception:
            self.poisoned = True
            raise

    def _select(self, key, sides, time, affected, skew_only):
        """Add the entities a change to ``key`` can change to ``affected``.

        An entity whose current observation is context-free and whose declared
        inputs on ``key`` were not touched would re-derive the same observation.
        In layout 2 its observation is reused, while positive episodes still
        track live skew. In layout 1 skew is a measurement dimension, so the
        entity is staged when its skew bucket moves.
        """
        reads, staged, current = self.reads, self.staged, self.current
        for entity_id in self.reverse.get(key, ()):
            if entity_id in affected:
                continue
            value = staged.get(entity_id)
            value = value[0] if value is not None else current.get(entity_id)
            read = reads.get((entity_id, key))
            if value is None or not value[0].context_free or read is None:
                affected.add(entity_id)
            elif sides is None or not read.isdisjoint(sides):
                # A touched input: unchanged when every object it reads is identical.
                memo = self.memo.get(entity_id)
                if (self.legacy or memo is None or memo[1] is not value[0] or not _same(
                        memo[0], _fingerprint(self._views(self.entities[entity_id], time),
                                              self.plans_of[entity_id]))):
                    affected.add(entity_id)
            elif self.legacy and value[0].status == EVALUATED:
                entity = self.entities[entity_id]
                if self._skew(entity, value[0].skew_legs)[1] != value[1]:
                    affected.add(entity_id)
            if not self.legacy and entity_id not in affected and value[0].predicates:
                skew_only.add(entity_id)

        if self.legacy or not self.experiment.controls_episodes:
            return
        # Shifted inputs change only at their timer, but their *live* books
        # also define skew. Reuse the observation until the timer fires.
        for shift in self.shifts.get(key, ()):
            for entity_id in self.shift_reverse.get((key, shift), ()):
                if entity_id in affected:
                    continue
                value = staged.get(entity_id)
                value = value[0] if value is not None else current.get(entity_id)
                if value is not None and value[0].predicates:
                    skew_only.add(entity_id)

    def _stage_skew(self, entity_id, time):
        entity = self.entities[entity_id]
        if entity.cls == CONTROL and not self.experiment.controls_episodes:
            return  # aggregate-only controls have no skew output
        value = self.staged.get(entity_id)
        value = value[0] if value is not None else self.current[entity_id]
        observation, previous_skew = value
        skew = self._skew(entity, observation.skew_legs)
        if skew != previous_skew:
            self._begin_stage(time)
            self._retain_staged(entity_id, (observation, skew), count_instantaneous=False)

    # -- views and history ---------------------------------------------------
    def _set_view(self, key, view):
        cost = bounds.view_cost(view, len(self.builders[key].sizes))
        self.budget.replace(self.view_costs.get(key, 0), cost)
        self.views[key] = view
        self.view_costs[key] = cost

    def _ring_append(self, key, time, view):
        ring = self.rings[key]
        if ring.times and ring.times[-1] == time:
            self._ring_drop(ring, len(ring.times) - 1)
        self._ring_prune(key, time)
        require(len(ring.times) < self.experiment.ring_entries, "time_shift ring bound")
        cost = bounds.RING_ENTRY + bounds.VIEW + 4 * bounds.CONTAINER
        for fill in _view_fills(view):
            entry = ring.fills.get(id(fill))
            if entry is None:
                # Consecutive views share interned fills; each object is
                # charged once while any ring entry references it.
                share = bounds.SLOT + bounds.FILL + (len(fill.taken) + len(fill.consumed)) * (
                    bounds.PAIR + bounds.SLOT)
                self.budget.charge(share)
                ring.fills[id(fill)] = [1, share]
            else:
                entry[0] += 1
        self.budget.charge(cost)
        ring.times.append(time)
        ring.views.append(view)
        ring.costs.append(cost)
        for shift in sorted(self.shifts[key]):
            due = time + shift
            if due < self.clock.end:
                heapq.heappush(self.timers, (due, key, shift))

    def _ring_drop(self, ring, index):
        view = ring.views.pop(index)
        ring.times.pop(index)
        self.budget.release(ring.costs.pop(index))
        for fill in _view_fills(view):
            entry = ring.fills[id(fill)]
            entry[0] -= 1
            if not entry[0]:
                self.budget.release(entry[1])
                del ring.fills[id(fill)]

    def _ring_prune(self, key, time):
        # Times only advance, so entries older than the one the largest shift
        # needs at ``time`` are never needed again.
        ring, keep = self.rings[key], time - max(self.shifts[key])
        drop = bisect_right(ring.times, keep) - 1
        for _ in range(max(0, drop)):
            self._ring_drop(ring, 0)

    def _shifted(self, key, shift, time):
        """Committed view of ``key`` at ``time - shift``."""
        ring = self.rings[key]
        index = bisect_right(ring.times, time - shift) - 1
        if index < 0:
            return unavailable_view(self.clock.start)
        return ring.views[index]

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
        self.plans_of = {}
        self.reverse, self.shift_reverse = {}, {}
        if not self.legacy:
            self.index = {}
            by_group = {}
            for entity in entities.values():
                by_group.setdefault(group_of(entity), []).append(entity)
            for members in by_group.values():
                for position, entity in enumerate(sorted(members, key=lambda e: e.order)):
                    self.index[entity.id] = position
        for entity in entities.values():
            if entity.admission is not None:
                continue
            inputs = entity.basket.inputs
            if inputs is not None:
                self.plans_of[entity.id] = tuple(
                    tuple((_BEST if source == "best" else _CROSSED if source == "crossed"
                           else _TRANSFORM if source in _SOURCE_SIDES else _FILL, source, size)
                          for source, size in leg)
                    for leg in inputs)
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
        plan = self.plans_of.get(entity_id)
        if plan is not None:
            fingerprint = _fingerprint(views, plan)
            memo = self.memo.get(entity_id)
            if memo is not None and _same(memo[0], fingerprint):
                observation = memo[1]
            else:
                observation = self._fresh(entity, views)
                old = memo[2] if memo is not None else 0
                if observation.context_free:
                    # The fingerprint may outlive its views, so its objects are charged.
                    cost = bounds.fingerprint_cost(fingerprint) + self._observation_cost(observation)
                    self.budget.replace(old, cost)
                    self.memo[entity_id] = (fingerprint, observation, cost)
                elif memo is not None:
                    self.budget.release(old)
                    del self.memo[entity_id]
        else:
            observation = self._fresh(entity, views)
        if self.legacy:
            skew = self._skew(entity, observation.skew_legs)[1] if observation.status == EVALUATED else None
        else:
            # Skew never splits time in layout 2; it is needed only inside episodes.
            skew = self._skew(entity, observation.skew_legs) if observation.predicates else None
        return observation, skew

    def _views(self, entity, time):
        shift = entity.shift
        if shift is None:
            views = self.views
            return tuple([views[key] for key in entity.legs])
        return tuple(self._shifted(key, shift[1], time) if position == shift[0]
                     else self.views[key] for position, key in enumerate(entity.legs))

    def _skew(self, entity, legs):
        """``(leg_skew_ns, bucket)`` from the legs' *live* last-change times.

        A time-shifted leg keeps its historical last-change time, so control
        skew always uses the live views of every leg.
        """
        views, keys = self.views, entity.legs
        if legs:
            keys = [keys[i] for i in legs]
        if len(keys) == 2:
            spread = views[keys[0]].last_change - views[keys[1]].last_change
            if spread < 0:
                spread = -spread
        else:
            changes = [views[key].last_change for key in keys]
            spread = max(changes) - min(changes)
        index = 0
        for edge in self.edges:
            if spread < edge:
                return spread, index
            index += 1
        return spread, index

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
        if observation.payload is not None:
            return bounds.observation_cost(observation)
        key = (observation.reasons, observation.fields)
        cost = self._plain_costs.get(key)
        if cost is None:
            if len(self._plain_costs) >= 4096:
                self._plain_costs.clear()  # a cache only; costs are recomputed exactly
            cost = self._plain_costs[key] = bounds.observation_cost(observation)
        return cost

    def _stage(self, time, entities):
        self._begin_stage(time)
        for entity in sorted(entities):
            self._retain_staged(entity, self._evaluate(entity))

    def _begin_stage(self, time):
        if self.staged_time is None:
            self.staged_time = time
        require(self.staged_time == time, "staging time")

    def _retain_staged(self, entity, value, *, count_instantaneous=True):
        previous = self.staged.get(entity)
        cost = self._observation_cost(value[0])
        if previous is not None:
            if count_instantaneous and previous[0][0].predicates and previous[0] != value:
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
        if self.legacy:
            fields = (observation.status, observation.reasons, skew, observation.value_class,
                      observation.fields)
        else:
            fields = (observation.status, observation.reasons, observation.value_class,
                      observation.fields)
        prior = self.open_measurements.get(entity.id)
        if prior is None or prior[1] != fields:
            if prior is not None:
                self._emit_measurements([(entity.id, prior, time)])
            cost = bounds.MEASUREMENT + self._observation_cost(observation) - bounds.OBSERVATION
            self.budget.replace(prior[2] if prior is not None else 0, cost)
            self.open_measurements[entity.id] = (time, fields, cost)
        if not self.legacy and entity.cls == CONTROL and not self.experiment.controls_episodes:
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
        if not self.legacy:
            episode.open_class = episode.class_now = observation.value_class
            episode.open_reasons = observation.reasons
            episode.class_since = time
            episode.slice_classes, episode.class_ns = {}, {}
            episode.qualifying = {t: {} for t in self.tiers}
            episode.by_skew = {t: {} for t in self.tiers}
            episode.skew_points = [(time, skew)]
            episode.at_max = (observation.payload, observation.quotes)
            episode.cost *= 2  # opening, maximum and current slice payloads
        require(len(self.episodes) < len(self.entities) * len(self.experiment.kinds),
                "open episode bound")
        self.budget.charge(episode.cost)
        self.episodes[key] = episode

    def _update_episode(self, episode, time, observation, skew):
        payload = observation.payload
        if not self.legacy and observation.value_class != episode.class_now:
            classes = episode.slice_classes
            classes[episode.class_now] = classes.get(episode.class_now, 0) + time - episode.class_since
            episode.class_now, episode.class_since = observation.value_class, time
        if observation.quotes != episode.quotes:
            self._check_retained_slice(observation.quotes, payload)
            cost = bounds.episode_cost(observation, len(self.tiers)) * (1 if self.legacy else 2)
            self.budget.replace(episode.cost, cost)
            episode.cost = cost
            self._emit_slice(episode, time, "CONSUMED_CHANGED", False)
            episode.slice_start, episode.slice_values = time, payload
            episode.quotes, episode.slice_skew = observation.quotes, skew
            if not self.legacy:
                self.budget.release((len(episode.skew_points) - 1) * (bounds.PAIR + bounds.SLOT))
                episode.skew_points = [(time, skew)]
        elif not self.legacy and skew != episode.skew_points[-1][1]:
            require(len(episode.skew_points) < bounds.MAX_SKEW_POINTS, "skew changes per slice")
            episode.skew_points.append((time, skew))
            self.budget.charge(bounds.PAIR + bounds.SLOT)
        maxima = self._maxima(payload, episode.maxima)
        if not self.legacy:
            first = self.experiment.maxima[0]
            if maxima[first] is not None and maxima[first] != episode.maxima[first]:
                episode.at_max = (payload, observation.quotes)
        episode.maxima = maxima

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
        if self.legacy:
            identity = episode_id(self.scope, episode.entity, episode.kind, episode.start)
            row = self._common(episode.entity, start, end) | {
                "episode_id": identity, "kind": episode.kind, "end_reason": reason,
                "censored": censored, "consumed": quotes_json(episode.quotes),
                "survival_ns": str(survival), "viable_tiers": viable,
                "open_values": episode.slice_values, "opening_skew_bucket": episode.slice_skew,
            }
            require(len(encoded(row)) + 1 <= bounds.MAX_LINE, "slice retained line budget")
            self.writers[self.layout_files.name("slices", entity.cls)].append(row)
            return
        self._close_slice_facts(episode, start, end, survival)
        group = group_of(entity)
        skew_ns, skew_bucket = episode.slice_skew
        row = {"episode_id": episode_id(self.scope, episode.entity, episode.kind, episode.start),
               "start_ns": str(start), "end_ns": str(end), "end_reason": reason,
               "censored": censored, "leg_skew_ns": str(skew_ns), "skew_bucket": skew_bucket}
        if group + "slices.ndjson" in self.writers:
            self.writers[group + "slices.ndjson"].append(row)
        if entity.cls == REAL and self.experiment.audit_intervals:
            row = row | {"quotes": quotes_json(episode.quotes), "values": episode.slice_values}
            require(len(encoded(row)) + 1 <= bounds.MAX_LINE, "slice retained line budget")
            self.writers["audit/slices.ndjson"].append(row)

    def _close_slice_facts(self, episode, start, end, survival):
        """Fold one closed slice into its episode's class and skew facts."""
        classes = episode.slice_classes
        classes[episode.class_now] = classes.get(episode.class_now, 0) + end - episode.class_since
        episode.class_since = end
        for value_class, amount in classes.items():
            episode.class_ns[value_class] = episode.class_ns.get(value_class, 0) + amount
        points = episode.skew_points
        for tier, tier_int in zip(self.tiers, self.tier_ints):
            if survival >= tier_int:
                target = episode.qualifying[tier]
                for value_class, amount in classes.items():
                    target[value_class] = target.get(value_class, 0) + amount
            # Entry instants [start, end - tier) take the skew in force at each.
            window = end - tier_int
            buckets = episode.by_skew[tier]
            for i, (at, skew) in enumerate(points):
                until = points[i + 1][0] if i + 1 < len(points) else end
                overlap = min(until, window) - at
                if overlap > 0:
                    buckets[skew[1]] = buckets.get(skew[1], 0) + overlap
        episode.slice_classes = {}

    def _close_episodes(self, closures):
        entities = self.entities
        closures = sorted(closures, key=lambda item: (
            entities[item[0][0]].order, item[0][1], self.episodes[item[0]].start))
        for key, end, reason, censored in closures:
            episode = self.episodes.pop(key)
            self.budget.release(episode.cost + (len(episode.skew_points) - 1) * (bounds.PAIR + bounds.SLOT)
                                if not self.legacy else episode.cost)
            self._emit_slice(episode, end, reason, censored)
            if episode.start >= end:
                continue
            entity = entities[episode.entity]
            require(self.episode_rows < bounds.MAX_ROWS, "combined episode row budget")
            self.episode_rows += 1
            if self.legacy:
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
                self.writers[self.layout_files.name("episodes", entity.cls)].append(row)
                continue
            skew_ns, skew_bucket = episode.opening_skew
            row = {
                "scope": self.scope, "entity": self.index[episode.entity],
                "episode_id": episode_id(self.scope, episode.entity, episode.kind, episode.start),
                "kind": episode.kind, "start_ns": str(episode.start), "end_ns": str(end),
                "end_reason": reason, "censored": censored,
                "opening_slice_survival_ns": str(episode.opening_survival),
                "viable_tiers": [t for t in self.tiers if t in episode.viable],
                "qualified_ns": {t: str(v) for t, v in episode.qualified.items()},
                "qualified_by_skew_ns": {t: {str(b): str(v) for b, v in sorted(m.items())}
                                         for t, m in episode.by_skew.items()},
                "class_ns": {c: str(v) for c, v in sorted(episode.class_ns.items())},
                "qualifying_class_ns": {t: {c: str(v) for c, v in sorted(m.items())}
                                        for t, m in episode.qualifying.items()},
                "open": {"value_class": episode.open_class,
                         "reasons": [self._reason(r) for r in episode.open_reasons],
                         "leg_skew_ns": str(skew_ns), "skew_bucket": skew_bucket,
                         "values": episode.open_values, "quotes": quotes_json(episode.open_quotes)},
                "maxima": episode.maxima,
                "at_max": {"values": episode.at_max[0], "quotes": quotes_json(episode.at_max[1])},
            }
            self.writers[group_of(entity) + "episodes.ndjson"].append(row)

    # -- measurements and denominators ---------------------------------------
    def _reason(self, text):
        """Index of a structured reason (canonical JSON object text) in the table."""
        index = self.reasons.get(text)
        if index is None:
            value = json.loads(text)
            require(type(value) is dict and encoded(value).decode() == text,
                    "reasons must be canonical JSON objects")
            self.budget.charge(bounds.SLOT + bounds.STR + 4 * len(text), "detached state budget")
            index = self.reasons[text] = len(self.reason_list)
            self.reason_list.append(value)
        return index

    def _emit_measurements(self, closures):
        entities = self.entities
        closures = sorted(closures, key=lambda item: (item[2], entities[item[0]].order, item[1][0]))
        if self.legacy:
            names = self.experiment.measurement_fields
            for entity, (start, fields, _), end in closures:
                if start < end:
                    status, reasons, skew, value_class, extra = fields
                    row = self._common(entity, start, end) | {
                        "status": status, "reasons": list(reasons), "skew_bucket": skew,
                        "value_class": value_class} | dict(zip(names, extra))
                    self.writers[self.layout_files.name("measurements", entities[entity].cls)].append(row)
            return
        audit = self.experiment.audit_intervals
        names = self.experiment.measurement_fields
        for entity_id, (start, fields, _), end in closures:
            if start >= end:
                continue
            status, reasons, value_class, extra = fields
            totals = self.denominators.get(entity_id)
            if totals is None:
                totals = self.denominators[entity_id] = ({}, {}, {})
                self._charge_denominator(bounds.MEASUREMENT)
            duration = end - start
            for table, key in ((totals[0], status), (totals[1], value_class)):
                if key is None:
                    continue
                if key not in table:
                    self._charge_denominator(bounds.SLOT + bounds.STR + bounds.INT)
                table[key] = table.get(key, 0) + duration
            indexes = [self._reason(r) for r in reasons]
            if indexes:
                key = (status, tuple(indexes))
                if key not in totals[2]:
                    self._charge_denominator(bounds.SLOT + bounds.CONTAINER + bounds.INT * (2 + len(indexes)))
                totals[2][key] = totals[2].get(key, 0) + duration
            if audit and entities[entity_id].cls == REAL:
                row = {"scope": self.scope, "entity": self.index[entity_id], "start_ns": str(start),
                       "end_ns": str(end), "status": status, "reasons": indexes}
                if value_class is not None:
                    row["value_class"] = value_class
                row |= dict(zip(names, extra))
                self.writers["audit/measurements.ndjson"].append(row)

    def _charge_denominator(self, amount):
        self.budget.charge(amount)
        self.denominator_cost += amount

    def _write_denominators(self):
        rows = []
        for entity_id, (status, classes, reasons) in self.denominators.items():
            entity = self.entities[entity_id]
            row = {"scope": self.scope, "entity": self.index[entity_id],
                   "status_ns": {k: str(v) for k, v in sorted(status.items())}}
            if classes:
                row["class_ns"] = {k: str(v) for k, v in sorted(classes.items())}
            if reasons:
                row["reason_ns"] = [[s, list(i), str(v)] for (s, i), v in sorted(reasons.items())]
            rows.append((group_of(entity), self.index[entity_id], row))
        for group, _, row in sorted(rows, key=lambda item: (item[0], item[1])):
            self.writers[group + "denominators.ndjson"].append(row)
        self.denominators = {}
        self.budget.release(self.denominator_cost)
        self.denominator_cost = 0

    def _common(self, entity, start, end):
        return row_common(self.layout_files.version, self.experiment.experiment_sha256,
                          self.scope, entity, start, end)

    def _close_scope(self, time, reason, censored):
        self._emit_measurements([(e, p, time) for e, p in self.open_measurements.items()])
        self.budget.release(sum(p[2] for p in self.open_measurements.values()))
        self.open_measurements.clear()
        self._close_episodes([(key, time, reason, censored) for key in sorted(self.episodes)])
        if not self.legacy:
            self._write_denominators()

    # -- completion ------------------------------------------------------------
    def _write_table(self, name, value):
        writer = LineWriter(self.root / name, max_bytes=bounds.MAX_METADATA, max_records=1,
                            max_line_bytes=bounds.MAX_METADATA)
        writer.append(value)
        return writer.finish()

    def _tables(self):
        """Entity tables (once per scope, per group) and the reason table."""
        tables = {}
        scopes = {group: [] for group in self.groups}
        for index in range(len(self.snapshot["scopes"])):
            entities = resolve(self.strategy, self.snapshot, index, self.plans)
            members = {group: [] for group in self.groups}
            for entity in sorted(entities.values(), key=lambda e: e.order):
                members[group_of(entity)].append({"hash": entity.id, "descriptor": entity.descriptor})
            for group in self.groups:
                scopes[group].append(members[group])
        for group in self.groups:
            tables[group + "entities.json"] = self._write_table(group + "entities.json",
                                                                {"scopes": scopes[group]})
        tables["reasons.json"] = self._write_table("reasons.json", {"reasons": self.reason_list})
        return tables

    def finish(self):
        try:
            require(self.terminal and not self.poisoned and not self.finished,
                    "missing terminal / failed economic strategy")
            files = {name: writer.finish() for name, writer in self.writers.items()}
            if not self.legacy:
                files |= self._tables()
            if self.profile is not None:
                files |= self.profile.finish_files()
            manifest = self.strategy.manifest(files, self.instantaneous)
            snapshot, strategy = self.snapshot, self.strategy
            # Release runtime state before the reader so the two peaks never overlap.
            self.views.clear(); self.rings.clear(); self.staged.clear()
            self.open_measurements.clear(); self.episodes.clear(); self.reverse.clear()
            self.memo.clear(); self.reasons.clear()
            self.budget = None
            summary = strategy.validate(self.root, snapshot, manifest)
            require(len(encoded(summary)) <= bounds.MAX_METADATA, "summary budget")
            if not self.legacy:
                for name, control in summary.get("controls", {}).items():
                    write_json_durable(self.root / "controls" / name / "summary.json", control)
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
