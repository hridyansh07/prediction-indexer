"""Bounded causal delayed-fill probes, independent of sparse wire cut cadence.

The runtime invokes ``before`` before refreshing a later book and ``committed``
after all same-time staged updates. Pending scenarios keep their entity and
arrival exit evidence, so neither episode closure nor scope entry discards them.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field

from replay.economic_fills import Fill
from replay.economic_sdk import bounds
from replay.economic_sdk.fills import assess, sells, walk_sizing
from replay.economic_sdk.types import SELL_SOURCES
from replay.economic_sdk.views import quote_ages
from replay.streams.protocol import require

MS = 1_000_000
MAX_TIME = 2**64 - 1
EXIT_SOURCE = {"ask": "bid", "bid": "ask", "kalshi_complement_ask": "kalshi_complement_bid",
               "kalshi_complement_bid": "kalshi_complement_ask"}


def _number(value):
    require(type(value) is int and len(str(abs(value))) <= 80, "probe integer bound")
    return str(value)


def _pairs(levels):
    return [[_number(price), _number(quantity)] for price, quantity in levels]


def _time(value):
    require(type(value) is int and 0 <= value <= MAX_TIME, "probe time overflow")
    return value


def _walk(ladder, quantity, levels, limit=None, sell=False):
    """Walk actual atoms; fail instead of silently truncating a required level."""
    taken, cost, filled = [], 0, 0
    for price, displayed in ladder:
        if filled == quantity or (limit is not None and (price < limit if sell else price > limit)):
            break
        require(len(taken) < levels, "probe consumed level bound")
        amount = min(displayed, quantity - filled)
        if amount:
            taken.append((price, amount))
            filled += amount
            cost += price * amount
    taken = tuple(taken)
    return Fill(filled, cost, filled < quantity, taken, taken)


def _part(fill, quantity):
    return _walk(fill.taken, quantity, len(fill.taken)) if quantity else Fill(0, 0, False, (), ())


@dataclass(slots=True)
class _Episode:
    id: str
    entity: object
    spec: object
    maxima: tuple
    start: int
    limit: int
    cost: int
    end: int | None = None
    count: int = 0


@dataclass(slots=True)
class _Decision:
    # One owner per (episode, sizing, confirmation). Its evidence is immutable
    # after pricing; latency and sequencing cannot change the decision book.
    cost: int
    opening: dict | None
    ages: list | None
    json: dict | None = None
    tradeable: bool = False
    count: int = 0


@dataclass(slots=True)
class _Probe:
    episode: _Episode
    sizing: object
    order: tuple
    confirm: int
    latency: int
    mode: str
    cost: int
    group: _Decision
    decision: dict | None = None
    legs: list = field(default_factory=list)
    fills: list = field(default_factory=list)
    exits: list = field(default_factory=list)
    targets: list = field(default_factory=list)
    delays: tuple = ()
    first: int | None = None
    finished: bool = False
    events: int = 0


class Manager:
    """One finite heap of decisions, arrivals and scope-end censor events."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.policy = runtime.fill.probes
        self.heap, self.episodes = [], {}
        self.pending = 0
        # References to immutable policy sizings, covered by its JSON reservation.
        self.sizings = tuple((i, sizing) for i, sizing in enumerate(runtime.fill.sizings)
                            if sizing.name in self.policy.sizings)
        # Pending reservations share this writer budget with views and episodes.
        runtime.budget.failure_context = self._diagnostics

    def _diagnostics(self):
        # add registers a group only after queuing its whole grid. At failure,
        # at most one admitting group can have queued but unregistered scenarios.
        admitting = self.pending - sum(episode.count for episode in self.episodes.values())
        groups = len(self.episodes) + bool(admitting)
        opened = self.runtime.episodes
        return (f"pending_scenarios={self.pending} max_pending_probes={bounds.MAX_PENDING_PROBES} "
                f"runtime_open_episodes={len(opened)} "
                f"runtime_open_fill_episodes={sum(episode.fill is not None for episode in opened.values())} "
                f"probe_episode_groups={groups} admitting_probe_scenarios={admitting} "
                f"heap_events={len(self.heap)}")

    def add(self, identity, entity, spec, maxima, start, limit, priced, ages):
        episode_cost = (4096 + bounds.json_cost(identity) + bounds.json_cost(entity.descriptor)
                        + bounds.json_cost(entity.legs) + bounds.json_cost(entity.metadata)
                        + bounds.json_cost(spec.sources) + bounds.json_cost(spec.units)
                        + bounds.json_cost(maxima))
        episode = _Episode(identity, entity, spec, maxima, start, limit, episode_cost)
        rows = {row["name"]: row for row in priced.json["results"]}
        cost = bounds.probe_cost(len(spec.units))
        for sizing_index, sizing in self.sizings:
            for confirm in self.policy.confirm_ms:
                opening, opening_ages = (rows[sizing.name], ages) if confirm == 0 else (None, None)
                group = _Decision(4096 + bounds.json_cost(opening) + bounds.json_cost(opening_ages),
                                  opening, opening_ages)
                for latency in self.policy.arrival_ms:
                    for mode_index, mode in enumerate(self.policy.sequencing):
                        # Admit all new owners atomically; a rejected first
                        # scenario must not leave a charged, unreferenced group.
                        requested = cost + (group.cost if not group.count else 0)
                        if not episode.count:
                            requested += episode.cost
                        if self.pending >= bounds.MAX_PENDING_PROBES:
                            budget = self.runtime.budget
                            require(False, f"pending probe bound: retained_bytes={budget.used + requested} "
                                    f"max_state_bytes={budget.limit} used_bytes={budget.used} "
                                    f"replaced_bytes=0 requested_bytes={requested}; {self._diagnostics()}")
                        self.runtime.budget.charge(requested, "pending probe state budget")
                        probe = _Probe(episode, sizing,
                                       (start, sizing_index, confirm, latency, mode_index, entity.id),
                                       confirm, latency, mode, cost, group)
                        self.pending += 1
                        episode.count += 1
                        group.count += 1
                        self._push(probe, _time(start + confirm * MS), -1)
        self.episodes[identity] = episode

    def close(self, identity, end):
        episode = self.episodes.get(identity)
        if episode is not None:
            episode.end = end

    def _push(self, probe, at, leg):
        # A beyond-scope decision is not_confirmed; beyond-scope arrival is
        # censored. Both resolve at the scope limit without waiting for a cut.
        heapq.heappush(self.heap, (min(at, probe.episode.limit), probe.order, leg, probe))
        probe.events += 1

    def before(self, time):
        self._drain(time, inclusive=False)

    def committed(self, time):
        self._drain(time, inclusive=True)

    def terminal(self, time):
        self._drain(time, inclusive=True)
        require(not self.heap and self.pending == 0, "unresolved probe terminal")

    def _drain(self, time, inclusive):
        while self.heap and (self.heap[0][0] < time or inclusive and self.heap[0][0] == time):
            at, _, leg, probe = heapq.heappop(self.heap)
            probe.events -= 1
            if probe.finished:
                if not probe.events:
                    self.runtime.budget.release(probe.cost)
                    probe.cost = 0
                continue
            if leg == -1:
                self._decide(probe)
            else:
                self._arrive(probe, leg)

    def _decide(self, probe):
        episode = probe.episode
        d = _time(episode.start + probe.confirm * MS)
        if probe.confirm and ((episode.end is not None and episode.end <= d) or d >= episode.limit):
            self._emit(probe, "not_confirmed")
            return
        group = probe.group
        if group.json is None:
            self._price_decision(probe, d)
        probe.decision = group.json
        if not group.tradeable:
            self._emit(probe, "decision_untradeable")
            return
        steps = int(probe.decision["steps"])
        hook = self.runtime.strategy.fill_delays(episode.entity, d)
        require(type(hook) is tuple and len(hook) == len(episode.spec.units), "fill delays arity")
        delays, sources = [], []
        for key, delay in zip(episode.entity.legs, hook):
            if delay is None:
                venue = self.runtime.plans[key]["venue"]
                delay = self.policy.unknown_taker_delay_ms[venue] * MS
                source = "default"
            else:
                require(type(delay) is int and delay >= 0 and delay % MS == 0,
                        "fill delays require nonnegative whole milliseconds")
                source = "terms"
            delays.append(_time(delay + probe.latency * MS))
            sources.append((delay // MS, source))
        probe.delays = tuple(delays)
        probe.targets = [steps * unit for unit in episode.spec.units]
        probe.fills = [None] * len(delays)
        probe.exits = [None] * len(delays)
        probe.legs = [{"sent_ns": None, "arrival_ns": None, "venue_delay_ms": delay,
                       "delay_source": source, "usable": None, "filled_atoms": None,
                       "cost": None, "consumed": None, "excess_atoms": None,
                       "exit_consumed": None, "exit_proceeds": None} for delay, source in sources]
        if not steps:
            for row in probe.legs:
                row.update(filled_atoms="0", cost="0", consumed=[], excess_atoms="0",
                           exit_consumed=[], exit_proceeds="0")
            self._emit(probe, "empty", 0, 0, 0)
            return
        if probe.mode == "simultaneous":
            for leg in range(len(delays)):
                self._send(probe, leg, d)
        else:
            probe.first = max(range(len(delays)), key=lambda leg: (delays[leg], -leg))
            self._send(probe, probe.first, d)

    def _price_decision(self, probe, d):
        episode, group = probe.episode, probe.group
        views = self.runtime._views(episode.entity, d)
        if probe.confirm == 0:
            row = group.opening
            steps = int(row["steps"])
            limits = [leg["taken"][-1][0] if leg["taken"] else None for leg in row["legs"]]
            # At c=0 the opening walk is the decision even for a recording
            # sizing with unknown/nonpositive value or short depth.
            tradeable = True
            ages = group.ages
            decision_value, decision_legs = row["value"], row["legs"]
        else:
            usable = all(view.validity == "usable" for view in views)
            ladders = tuple(view.ladders[source] if usable else ()
                            for source, view in zip(episode.spec.sources, views))
            value = lambda steps, legs: self.runtime.strategy.fill_value(episode.entity, steps, legs, d)
            result = walk_sizing(probe.sizing, ladders, episode.spec.units, value,
                                 self.runtime.fill.max_levels)
            _, reason = assess(probe.sizing, result, value, episode.maxima, sells(episode.spec.sources))
            steps = result.steps
            limits = [_number(leg.taken[-1][0]) if leg.taken else None for leg in result.legs]
            tradeable = usable and reason is None
            ages = quote_ages(episode.spec.sources, views, d)
            decision_value = None if result.value is None else _number(result.value)
            decision_legs = [{"atoms": _number(leg.filled_atoms), "cost": _number(leg.cost),
                              "taken": _pairs(leg.taken), "consumed": _pairs(leg.consumed)}
                             for leg in result.legs]
        decision = {"time_ns": str(d), "steps": _number(steps), "limits": limits, "ages": ages}
        if self.runtime.strategy.probe_decision_evidence:
            decision.update(value=decision_value, legs=decision_legs)
        cost = 4096 + bounds.json_cost(decision)
        self.runtime.budget.replace(group.cost, cost, "pending probe state budget")
        group.cost, group.json, group.tradeable = cost, decision, tradeable
        group.opening, group.ages = None, None

    def _send(self, probe, leg, time):
        arrival = _time(time + probe.delays[leg])
        probe.legs[leg]["sent_ns"], probe.legs[leg]["arrival_ns"] = str(time), str(arrival)
        self._push(probe, arrival, leg)

    def _arrive(self, probe, leg):
        row = probe.legs[leg]
        at = int(row["arrival_ns"])
        episode = probe.episode
        if at >= episode.limit:
            self._emit(probe, "censored")
            return
        view = self.runtime._views(episode.entity, at)[leg]
        usable = view.validity == "usable"
        source = episode.spec.sources[leg]
        fill = (_walk(view.ladders[source], probe.targets[leg], self.runtime.fill.max_levels,
                      int(probe.decision["limits"][leg]), source in SELL_SOURCES)
                if usable else Fill(0, 0, True, (), ()))
        filled_atoms, cost = _number(fill.filled_atoms), _number(fill.cost)
        for price, quantity in fill.taken:
            _number(price), _number(quantity)
        key, ladder = self.runtime.strategy.fill_exits(episode.entity)[leg]
        # Like the entry view, live views are committed at the greatest time
        # <= at: the manager drains before any later cut refreshes them.
        exit_view = self.runtime.views[key]
        # Excess cannot exceed this leg's actual fill. Retain the shortest
        # best-first prefix that could cover it, still respecting the exit cap.
        count, quantity = 0, 0
        if exit_view.validity == "usable" and fill.filled_atoms:
            for _, displayed in exit_view.ladders[ladder]:
                if quantity >= fill.filled_atoms or count == self.runtime.fill.max_levels:
                    break
                count += 1
                quantity += displayed
        exit_levels = exit_view.ladders[ladder][:count]
        growth = bounds.probe_cost(0, len(fill.taken) + count) - bounds.probe_cost(0)
        self.runtime.budget.charge(growth, "pending probe state budget")
        probe.cost += growth
        row.update(usable=usable, filled_atoms=filled_atoms, cost=cost)
        probe.fills[leg], probe.exits[leg] = fill, exit_levels
        if probe.mode == "slow_first" and leg == probe.first:
            if not fill.filled_atoms:
                for other in range(len(probe.legs)):
                    if other != leg:
                        probe.legs[other].update(filled_atoms="0", cost="0", consumed=[])
                        probe.fills[other] = Fill(0, 0, True, (), ())
                        probe.exits[other] = ()
                self._complete(probe)
                return
            steps = fill.filled_atoms // episode.spec.units[leg]
            for other, unit in enumerate(episode.spec.units):
                if other != leg:
                    probe.targets[other] = steps * unit
                    self._send(probe, other, at)
        if all(fill is not None for fill in probe.fills):
            self._complete(probe)

    def _complete(self, probe):
        episode = probe.episode
        steps = min(fill.filled_atoms // unit for fill, unit in zip(probe.fills, episode.spec.units))
        matched = tuple(_part(fill, steps * unit) for fill, unit in zip(probe.fills, episode.spec.units))
        native_losses = []
        for leg, (fill, part, source) in enumerate(zip(probe.fills, matched, episode.spec.sources)):
            excess = fill.filled_atoms - part.filled_atoms
            exit_fill = _walk(probe.exits[leg], excess, self.runtime.fill.max_levels)
            excess_cost = fill.cost - part.cost
            # A sold leg repurchases its worst (lowest-priced) excess sales;
            # best-first matched sales have already been retained by _part.
            uncovered = excess - exit_fill.filled_atoms
            native_losses.append(exit_fill.cost + uncovered * episode.maxima[leg] - excess_cost
                                 if source in SELL_SOURCES
                                 else excess_cost - exit_fill.cost)
            probe.legs[leg].update(excess_atoms=_number(excess), exit_consumed=_pairs(exit_fill.taken),
                                   exit_proceeds=_number(exit_fill.cost))
        if any(row["usable"] is False for row in probe.legs):
            status = "unusable"
        elif not steps:
            status = "empty"
        elif all(fill.filled_atoms == int(probe.decision["steps"]) * unit
                 for fill, unit in zip(probe.fills, episode.spec.units)):
            status = "filled"
        elif steps:
            status = "partial"
        else:
            status = "empty"
        value = self.runtime.strategy.fill_value(episode.entity, steps, matched,
                                                 int(probe.decision["time_ns"])) if steps else 0
        require(value is None or type(value) is int, "probe value")
        loss = self.runtime.strategy.fill_loss(episode.entity, tuple(native_losses),
                                               int(probe.decision["time_ns"]))
        require(type(loss) is int, "probe loss value")
        self._emit(probe, status, steps, value, loss)

    def _emit(self, probe, status, steps=None, value=None, loss=None):
        # Arrival walks stay native while pending. Formatting them only here
        # avoids retaining the same quantities as both integers and strings.
        legs = [dict(row, consumed=_pairs(fill.taken)) if fill is not None else row
                for row, fill in zip(probe.legs, probe.fills)]
        row = {"episode_id": probe.episode.id, "sizing": probe.sizing.name,
               "confirm_ms": probe.confirm, "arrival_ms": probe.latency,
               "sequencing": probe.mode, "status": status, "decision": probe.decision,
               "legs": legs, "matched_steps": None if steps is None else _number(steps),
               "value": None if value is None else _number(value),
               "leg_loss_gross": None if loss is None else _number(loss)}
        self.runtime.writers["probes.ndjson"].append(row)
        probe.finished = True
        # A censor may leave other same-boundary events queued. Clear their
        # payload immediately, but charge the small shell until the last pop.
        remaining = bounds.probe_cost(len(probe.episode.spec.units)) if probe.events else 0
        self.runtime.budget.release(probe.cost - remaining)
        probe.cost = remaining
        self.pending -= 1
        probe.group.count -= 1
        if not probe.group.count:
            self.runtime.budget.release(probe.group.cost)
        probe.episode.count -= 1
        if not probe.episode.count:
            self.runtime.budget.release(probe.episode.cost)
            self.episodes.pop(probe.episode.id, None)
        probe.episode, probe.group, probe.decision = None, None, None
        probe.legs, probe.fills, probe.exits, probe.targets = [], [], [], []
        probe.delays = ()
