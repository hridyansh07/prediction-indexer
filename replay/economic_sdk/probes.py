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
    return Fill(filled, cost, filled < quantity, tuple(taken), tuple(taken))


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
    end: int | None = None
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
    opening: dict
    ages: list
    decision: dict | None = None
    legs: list = field(default_factory=list)
    fills: list = field(default_factory=list)
    exits: list = field(default_factory=list)
    targets: list = field(default_factory=list)
    delays: tuple = ()
    first: int | None = None
    finished: bool = False


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

    def add(self, identity, entity, spec, maxima, start, limit, priced, ages):
        episode = _Episode(identity, entity, spec, maxima, start, limit)
        rows = {row["name"]: row for row in priced.json["results"]}
        # Compute once, but reserve every scenario occurrence at the same point.
        cost = bounds.probe_cost(entity, len(spec.units), self.runtime.fill.max_levels)
        for sizing_index, sizing in self.sizings:
            for confirm in self.policy.confirm_ms:
                for latency in self.policy.arrival_ms:
                    for mode_index, mode in enumerate(self.policy.sequencing):
                        require(self.pending < bounds.MAX_PENDING_PROBES, "pending probe bound")
                        self.runtime.budget.charge(cost, "pending probe state budget")
                        probe = _Probe(episode, sizing,
                                       (start, sizing_index, confirm, latency, mode_index, entity.id),
                                       confirm, latency, mode, cost, rows[sizing.name], ages)
                        self.pending += 1
                        episode.count += 1
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
            if probe.finished:
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
        views = self.runtime._views(episode.entity, d)
        if probe.confirm == 0:
            row = probe.opening
            steps = int(row["steps"])
            limits = [leg["taken"][-1][0] if leg["taken"] else None for leg in row["legs"]]
            # At c=0 the opening walk is the decision even for a recording
            # sizing with unknown/nonpositive value or short depth.
            tradeable = True
            ages = probe.ages
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
        probe.decision = {"time_ns": str(d), "steps": _number(steps), "limits": limits, "ages": ages}
        probe.opening, probe.ages = None, None
        if not tradeable:
            self._emit(probe, "decision_untradeable")
            return
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
        row["usable"] = view.validity == "usable"
        source = episode.spec.sources[leg]
        fill = (_walk(view.ladders[source], probe.targets[leg], self.runtime.fill.max_levels,
                      int(probe.decision["limits"][leg]), source in SELL_SOURCES)
                if row["usable"] else Fill(0, 0, True, (), ()))
        row.update(filled_atoms=_number(fill.filled_atoms), cost=_number(fill.cost),
                   consumed=_pairs(fill.taken))
        probe.fills[leg] = fill
        # Only max_levels + 1 exit levels can matter: a required extra level
        # fails the attempt at unwind, while less depth remains visible.
        probe.exits[leg] = view.ladders[EXIT_SOURCE[source]][:self.runtime.fill.max_levels + 1]
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
            native_losses.append(exit_fill.cost - excess_cost if source in SELL_SOURCES
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
        row = {"episode_id": probe.episode.id, "sizing": probe.sizing.name,
               "confirm_ms": probe.confirm, "arrival_ms": probe.latency,
               "sequencing": probe.mode, "status": status, "decision": probe.decision,
               "legs": probe.legs, "matched_steps": None if steps is None else _number(steps),
               "value": None if value is None else _number(value),
               "leg_loss_gross": None if loss is None else _number(loss)}
        self.runtime.writers["probes.ndjson"].append(row)
        probe.finished = True
        self.runtime.budget.release(probe.cost)
        self.pending -= 1
        probe.episode.count -= 1
        if not probe.episode.count:
            self.episodes.pop(probe.episode.id, None)
