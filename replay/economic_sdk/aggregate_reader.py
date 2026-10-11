"""Independent reader for layout-2 output (denominators, episodes, controls).

It does not import the runtime. From the manifest's policy and the snapshot it
re-resolves every scoped entity, then streams each file once and checks:

- the entity and reason tables round-trip exactly;
- denominators: one row per scoped entity, status time summing exactly to the
  scope, value-class time summing exactly to ``DEPTH_SUFFICIENT`` time;
- episodes: closed schema, bounds, close order, no overlap, consistent end
  reasons, and per key and kind the episode lifetimes equal the time of that
  kind's positive classes in the denominators (so every episode lies inside
  ``DEPTH_SUFFICIENT`` time and none is missing);
- slices partition their episodes; tiers, ``Q`` and opening survival recompute;
  skew-attributed entry time sums to ``Q``;
- with ``audit_intervals``, the full interval partition recomputes the
  denominators exactly and its maximal predicate runs equal the episodes;
- in fill mode (SDK spec §13): every governing and recording sizing re-checks
  from the levels it carries (taken against consumed, cost, ``after``, impact,
  steps times units, the target's stop), the strategy's value hook
  re-checks values and kill prices at the episode's start, every governing
  sizing is tradeable, fill episodes are single-slice and lie inside
  trigger-positive time, and live plus no-fill time equals trigger-positive
  time exactly.

Denominators are writer-attested without the audit: the reader proves their
internal arithmetic and their agreement with the episodes, not each interval.
"""

from __future__ import annotations

from array import array
from bisect import bisect_right
from pathlib import Path

from replay.economic_fills import Fill
from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.fills import (FILL_LIVE, FILL_STATES, KILL_PRICE, MAX_END_REASON,
                                       check_experiment)
from replay.economic_sdk.entity_tables import entity_rows
from replay.economic_sdk.output import aggregate_files, file_list, group_of
from replay.economic_sdk.reader import Budget, check_files, document, lines, quantiles, signed
from replay.economic_sdk.types import ADMISSIONS, EVALUATED, SDK_STATUSES, SELL_SOURCES
from replay.economic_sdk.game import check_binding, check_episode_game
from replay.preparation import digest, encoded
from replay.strategy_sdk import plain
from replay.streams.protocol import obj, require, uint


def _closed(row, required, optional=()):
    require(type(row) is dict and set(required) <= set(row) <= set(required) | set(optional),
            "closed row schema")


def _ns_map(value, allowed):
    require(type(value) is dict and set(value) <= set(allowed), "duration map keys")
    result = {}
    for key, amount in value.items():
        result[key] = uint(amount)
        require(result[key] > 0, "zero duration entry")
    return result


def _table(root, name, files):
    return document(root, name, files[name])


class GroupFacts:
    """Recomputed facts for one summary key in one output group."""

    __slots__ = ("status_ns", "class_ns", "episodes", "episode_count", "slice_count", "q",
                 "q_by_skew", "lifetimes", "survival", "censored_episodes", "censored_slices",
                 "qualifying", "slice_ns_by_skew", "fill_ns", "fill_ends")

    def __init__(self, kinds, tiers, episodes):
        self.status_ns, self.class_ns, self.episodes = {}, {}, episodes
        # Fill mode only: trigger-positive time by fill state, fill episodes by end reason.
        self.fill_ns, self.fill_ends = {}, {}
        self.episode_count = {k: 0 for k in kinds}
        self.slice_count = {k: 0 for k in kinds}
        self.q = {k: {t: 0 for t in tiers} for k in kinds}
        self.q_by_skew = {k: {t: {} for t in tiers} for k in kinds}
        self.lifetimes = {k: array("Q") for k in kinds}
        self.survival = {k: array("Q") for k in kinds}
        self.censored_episodes = {k: 0 for k in kinds}
        self.censored_slices = {k: 0 for k in kinds}
        self.qualifying = {k: {t: {} for t in tiers} for k in kinds}
        self.slice_ns_by_skew = {k: {} for k in kinds}


class Aggregates:
    def __init__(self, groups, entities, reasons, budget):
        self.groups, self.entities, self.reasons, self.budget = groups, entities, reasons, budget

    def quantiles(self, values):
        return quantiles(values, self.budget)


def _natural(value):
    require(type(value) is str and 0 < len(value) <= 80 and value.isascii() and value.isdigit()
            and (value == "0" or value[0] != "0"), "canonical natural number")
    return int(value)


def _level(value):
    if value is None:
        return None
    require(type(value) is list and len(value) == 2, "fill level")
    price, quantity = _natural(value[0]), _natural(value[1])
    require(quantity > 0, "fill level quantity")
    return price, quantity


_STOPS = {"target": ("target", "book_exhausted", "level_cap"),
          "edge": ("edge", "book_exhausted", "level_cap", "value_unknown")}


def _beyond(value, after, unit, max_levels, maximum, worse):
    """One leg's carried ``beyond`` levels: closed, best-first from ``after``, minimal."""
    require(type(value) is list and len(value) <= max_levels + 1, "fill beyond levels")
    levels = tuple(_level(level) for level in value)
    require(None not in levels and (levels[0] if levels else None) == after, "fill beyond levels")
    for previous, level in zip(levels, levels[1:]):
        require(worse(level[0], previous[0]), "fill beyond levels")
    require(all(price <= maximum for price, _ in levels), "fill beyond levels")
    # They stop as soon as they hold one step, so every level but the last is needed.
    require(sum(q for _, q in levels[:-1]) < unit, "fill beyond levels")
    return levels


def _one_more(fill, beyond, unit, max_levels):
    """``(fill at one more step or None, state)`` from one leg's carried levels.

    ``state`` is ``fits`` (servable within ``max_levels`` levels), ``capped``
    (depth serves it, but only past the level cap), ``short`` (the ladder ends
    first), or ``unknown`` (``beyond`` was truncated at ``max_levels + 1`` levels:
    past the cap, deeper depth not carried).
    """
    if sum(q for _, q in beyond) < unit:
        return None, "short" if len(beyond) <= max_levels else "unknown"
    taken, consumed = list(fill.taken), list(fill.consumed)
    need, cost = unit, fill.cost
    for index, (price, quantity) in enumerate(beyond):
        amount = min(quantity, need)
        need -= amount
        cost += price * amount
        if index == 0 and consumed and consumed[-1][0] == price:
            taken[-1] = (price, taken[-1][1] + amount)   # the rest of a partly taken level
        else:
            taken.append((price, amount))
            consumed.append((price, quantity))
        if not need:
            break
    return (Fill(fill.filled_atoms + unit, cost, False, tuple(taken), tuple(consumed)),
            "capped" if len(consumed) > max_levels else "fits")


def _check_next(sizing, stop, steps, nexts, fills, strategy, entity, start, recorded):
    """What one more step proves: why the walk stopped where it did.

    An ``edge`` stop's next step is servable and not worth more (a zero-step
    walk compares with zero). A ``book_exhausted`` or ``level_cap`` stop's next
    step cannot be served within ``max_levels``; the label is ``level_cap``
    exactly when depth past the cap would serve it on every leg.
    """
    states = [state for _, state in nexts]
    if stop == "edge":
        require(all(state == "fits" for state in states), "fill edge next step")
        trial = strategy.fill_value(entity, steps + 1, tuple(fill for fill, _ in nexts), start)
        require(type(trial) is int and trial <= (recorded if steps else 0), "fill edge next step")
    elif stop in ("book_exhausted", "level_cap"):
        require(not all(state == "fits" for state in states), "fill exhausted next step")
        if "short" in states:
            require(stop == "book_exhausted", "fill exhausted next step")
        elif "unknown" not in states:
            require(stop == "level_cap", "fill exhausted next step")


def _check_fill(value, entity, end_reason, policy, strategy, maxima, start):
    """Re-check one carried fill from its own levels and the strategy's value hook.

    Every sizing, governing or recording, is checked; values and kill prices use
    the episode's open time ``start``. A bought leg walks asks upward and is
    killed at or above its kill price; a sold leg walks bids downward and is
    killed at or below it.
    """
    value = obj(value, "sources units results kill_prices kill_leg kill_best end_books")
    spec = strategy.fill_spec(entity)
    legs = len(entity.legs)
    require(value["sources"] == list(spec.sources) and value["units"] == [str(u) for u in spec.units]
            and len(spec.units) == legs, "fill spec")
    sell = [source in SELL_SOURCES for source in spec.sources]

    def reached(best, kill, leg):
        return kill is not None and (best <= kill if sell[leg] else best >= kill)

    def worse(price, than, leg):
        return price < than if sell[leg] else price > than
    require(type(value["results"]) is list and len(value["results"]) == len(policy.sizings),
            "fill results")
    effective = [None] * legs
    for sizing, result in zip(policy.sizings, value["results"]):
        result = obj(result, "name role mode steps stop value legs before after impact_ppm "
                             "beyond tradeable kill_prices")
        require((result["name"], result["role"], result["mode"]) == (sizing.name, sizing.role, sizing.mode),
                "fill result sizing")
        steps, stop = _natural(result["steps"]), result["stop"]
        require(stop in _STOPS[sizing.mode], "fill stop")
        if sizing.mode == "target":
            require(steps == sizing.target_steps if stop == "target" else steps < sizing.target_steps,
                    "fill stop")
        for field in ("legs", "before", "after", "impact_ppm", "beyond"):
            require(type(result[field]) is list and len(result[field]) == legs, "fill leg count")
        fills, befores, nexts = [], [], []
        for i, leg in enumerate(result["legs"]):
            leg = obj(leg, "atoms cost taken consumed")
            atoms, cost = _natural(leg["atoms"]), _natural(leg["cost"])
            require(atoms == steps * spec.units[i], "fill atoms are steps times units")
            require(type(leg["taken"]) is list and type(leg["consumed"]) is list, "fill levels")
            taken = tuple(_level(level) for level in leg["taken"])
            consumed = tuple(_level(level) for level in leg["consumed"])
            require(len(taken) == len(consumed) <= policy.max_levels and None not in consumed,
                    "fill levels")
            previous = None
            for j, ((taken_price, taken_quantity), (price, quantity)) in enumerate(zip(taken, consumed)):
                require(taken_price == price and taken_quantity <= quantity and price <= maxima[i]
                        and (previous is None or worse(price, previous, i)), "fill taken/consumed levels")
                require(taken_quantity == quantity or j == len(taken) - 1, "fill taken/consumed levels")
                previous = price
            require(sum(q for _, q in taken) == atoms, "fill taken quantity")
            require(cost == sum(p * q for p, q in taken), "fill cost")
            before, after = _level(result["before"][i]), _level(result["after"][i])
            if consumed:
                require(before == consumed[0], "fill before level")
                price, quantity = consumed[-1]
                if taken[-1][1] < quantity:
                    require(after == (price, quantity - taken[-1][1]), "fill after level")
                else:
                    require(after is None or worse(after[0], price, i), "fill after level")
            else:
                require(atoms == 0 and after == before, "fill after level")
            impact = (None if before is None or after is None or before[0] == 0
                      else abs(after[0] - before[0]) * 10**6 // before[0])
            require(result["impact_ppm"][i] == (None if impact is None else str(impact)),
                    "fill impact")
            fills.append(Fill(atoms, cost, False, taken, consumed))
            befores.append(before)
            beyond = _beyond(result["beyond"][i], after, spec.units[i], policy.max_levels,
                             maxima[i], lambda p, q, leg=i: worse(p, q, leg))
            nexts.append(_one_more(fills[-1], beyond, spec.units[i], policy.max_levels))
        fills = tuple(fills)
        _check_next(sizing, stop, steps, nexts, fills, strategy, entity, start,
                    None if result["value"] is None else signed(result["value"]))
        recorded = None if result["value"] is None else signed(result["value"])
        if steps:
            computed = strategy.fill_value(entity, steps, fills, start)
            require(computed is None or type(computed) is int, "fill value type")
            require(computed == recorded, "fill value")
        else:
            require(recorded is None, "fill value")

        def positive(leg, price):
            quantity = fills[leg].filled_atoms
            single = Fill(quantity, price * quantity, False, ((price, quantity),), ((price, quantity),))
            computed = strategy.fill_value(entity, steps, fills[:leg] + (single,) + fills[leg + 1:], start)
            require(computed is None or type(computed) is int, "fill value type")
            return computed is not None and computed > 0

        kills = None
        if steps and recorded is not None and recorded > 0:
            kills = result["kill_prices"]
            require(type(kills) is list and len(kills) == legs, "fill kill prices")
            kills = [None if kill is None else _natural(kill) for kill in kills]
            for leg, kill in enumerate(kills):
                # The defining property: not positive at the kill price, and positive
                # one atom further toward the fill (below a bought leg, above a sold one).
                if kill is None:
                    require(positive(leg, 0 if sell[leg] else maxima[leg]), "fill kill price")
                elif sell[leg]:
                    require(kill <= maxima[leg] and not positive(leg, kill)
                            and (kill == maxima[leg] or positive(leg, kill + 1)), "fill kill price")
                else:
                    require(kill <= maxima[leg] and not positive(leg, kill)
                            and (kill == 0 or positive(leg, kill - 1)), "fill kill price")
        else:
            require(result["kill_prices"] is None, "fill kill prices")
        short = (steps < sizing.target_steps if sizing.mode == "target"
                 else not steps and stop != "edge")
        tradeable = (not short and kills is not None
                     and not any(reached(before[0], kill, leg)
                                 for leg, (kill, before) in enumerate(zip(kills, befores))))
        require(result["tradeable"] is tradeable, "fill tradeable")
        if sizing.role == "governs":
            # A fill opens only when every governing sizing is tradeable.
            require(tradeable, "fill governing sizing not tradeable")
            effective = [kill if current is None else current if kill is None
                         else max(current, kill) if sell[leg] else min(current, kill)
                         for leg, (current, kill) in enumerate(zip(effective, kills))]
    require(value["kill_prices"] == [None if k is None else str(k) for k in effective],
            "fill kill prices")
    if end_reason == KILL_PRICE:
        leg = value["kill_leg"]
        require(type(leg) is int and 0 <= leg < legs and effective[leg] is not None, "fill kill leg")
        best = _level(value["kill_best"])
        require(best is not None and reached(best[0], effective[leg], leg), "fill kill crossing")
    else:
        require(value["kill_leg"] is None and value["kill_best"] is None, "fill kill fields")
    # Each leg's book at the end: writer-attested, except its shape, that only a
    # usable book has a walked best level, and that a kill names that level.
    books = value["end_books"]
    require(type(books) is list and len(books) == legs, "fill end books")
    for leg, book in enumerate(books):
        book = obj(book, "validity reason crossed best")
        require(type(book["validity"]) is str and book["validity"]
                and (book["reason"] is None or (type(book["reason"]) is str
                                                and len(book["reason"]) <= MAX_END_REASON))
                and type(book["crossed"]) is bool, "fill end book")
        best = _level(book["best"])
        require(best is None or book["validity"] == "usable", "fill end book best")
        if end_reason == KILL_PRICE and leg == value["kill_leg"]:
            require(best == _level(value["kill_best"]), "fill end book kill level")


def validate(directory, snapshot, manifest, strategy):
    check_binding(manifest)
    experiment = strategy.experiment
    check_experiment(experiment)
    snapshot = plain(snapshot)
    root = Path(directory)
    files = manifest["files"]
    check_files(files, file_list(experiment) + strategy.extra_files())
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    scopes = snapshot["scopes"]
    run_end = int(scopes[-1]["end_ns"])
    kinds, tiers = experiment.kinds, experiment.tiers_ns
    tier_ints = [int(t) for t in tiers]
    edges = experiment.skew_edges_ns
    statuses = set(SDK_STATUSES + ADMISSIONS + experiment.diagnostic_statuses)
    end_reasons = (set(SDK_STATUSES + experiment.diagnostic_statuses) - {EVALUATED}) | {"PREDICATE_FALSE"}
    budget = Budget(snapshot, manifest, experiment.policy)

    def bucket_of(spread):
        return next((i for i, edge in enumerate(edges) if spread < edge), len(edges))

    def skew(row):
        spread = uint(row["leg_skew_ns"])
        require(row["skew_bucket"] == bucket_of(spread), "skew bucket/leg skew")
        return row["skew_bucket"]

    table = obj(_table(root, "reasons.json", files), "reasons")
    require(type(table["reasons"]) is list, "reason table")
    texts = []
    for value in table["reasons"]:
        require(type(value) is dict, "structured reason")
        texts.append(encoded(value).decode())
    require(len(set(texts)) == len(texts), "duplicate reason")
    budget.reserve(sum(64 + len(t) for t in texts))

    def reason_indexes(value):
        require(type(value) is list and all(type(i) is int and 0 <= i < len(texts) for i in value),
                "reason index")
        return value

    resolved = [resolve(strategy, snapshot, index, plans) for index in range(len(scopes))]
    aggregates, all_entities = {}, {}
    for group, names in aggregate_files(experiment).items():
        entities = []
        for index in range(len(scopes)):
            members = sorted((e for e in resolved[index].values() if group_of(e) == group),
                             key=lambda e: e.order)
            entities.append(members)
            budget.reserve(1024 * len(members), "reader state budget (layout)")
            for entity in members:
                all_entities.setdefault(group, {})[entity.id] = entity
        stored = obj(_table(root, group + "entities.json", files), "scopes")
        require(stored["scopes"] == [entity_rows({e.id: e for e in members}, group) for members in entities], "entity table")
        aggregates[group] = _group(
            root, group, names, files, entities, scopes, run_end, experiment, strategy, statuses,
            end_reasons, kinds, tiers, tier_ints, texts, reason_indexes, skew, budget, plans)

    budget.reserve(bounds.MAX_METADATA)
    return strategy.summarize_aggregate(
        Aggregates(aggregates, all_entities, table["reasons"], budget), manifest, snapshot)


def _group(root, group, names, files, entities, scopes, run_end, experiment, strategy, statuses,
           end_reasons, kinds, tiers, tier_ints, texts, reason_indexes, skew, budget, plans):
    facts = {}
    fill = experiment.fills
    fill_live = {}
    episodes_present = group + "episodes.ndjson" in files

    def facts_for(entity):
        key = strategy.summary_key(entity)
        if key not in facts:
            budget.reserve(2048)
            facts[key] = GroupFacts(kinds, tiers, episodes_present)
        return facts[key]

    # -- denominators ------------------------------------------------------------
    denominators, expected_next = {}, [(s, i) for s, members in enumerate(entities)
                                       for i in range(len(members))]
    position = 0
    for row in lines(root, group + "denominators.ndjson", files[group + "denominators.ndjson"]):
        _closed(row, ("scope", "entity", "status_ns"),
                ("class_ns", "reason_ns") + (("fill_ns",) if fill is not None else ()))
        require(position < len(expected_next) and (row["scope"], row["entity"]) == expected_next[position],
                "denominator order/coverage")
        position += 1
        scope, index = row["scope"], row["entity"]
        entity = entities[scope][index]
        length = int(scopes[scope]["end_ns"]) - int(scopes[scope]["start_ns"])
        status = _ns_map(row["status_ns"], statuses)
        require(sum(status.values()) == length, "denominators sum to scope length")
        if entity.admission is not None:
            require(status == {entity.admission: length}, "admission denominator")
        classes = _ns_map(row.get("class_ns", {}), experiment.value_classes)
        require(("class_ns" in row) == (EVALUATED in status), "class time presence")
        require(sum(classes.values()) == status.get(EVALUATED, 0), "class time sums to sufficient time")
        reasons = []
        for entry in row.get("reason_ns", []):
            require(type(entry) is list and len(entry) == 3 and entry[0] in status, "reason duration")
            reasons.append((entry[0], tuple(reason_indexes(entry[1])), uint(entry[2])))
        require("reason_ns" not in row or reasons, "empty reason list")
        require([r[:2] for r in reasons] == sorted({r[:2] for r in reasons}), "reason order")
        for name in status:
            require(sum(r[2] for r in reasons if r[0] == name) <= status[name], "reason time")
        budget.reserve(256)
        denominators[scope, index] = (status, classes, reasons)
        target = facts_for(entity)
        if fill is not None:
            # Trigger-positive time is partitioned exactly into live and no-fill time.
            trigger = sum(classes.get(name, 0) for name in experiment.episode_classes[fill.kind])
            partition = _ns_map(row.get("fill_ns", {}), FILL_STATES)
            require(("fill_ns" in row) == (trigger > 0) and sum(partition.values()) == trigger,
                    "fill time partitions trigger-positive time")
            fill_live[scope, index] = partition.get(FILL_LIVE, 0)
            for name, amount in partition.items():
                target.fill_ns[name] = target.fill_ns.get(name, 0) + amount
        for name, amount in status.items():
            target.status_ns[name] = target.status_ns.get(name, 0) + amount
        for name, amount in classes.items():
            target.class_ns[name] = target.class_ns.get(name, 0) + amount
    require(position == len(expected_next), "denominator order/coverage")

    if not episodes_present:
        return facts

    # -- episodes ----------------------------------------------------------------
    index_of, eps = {}, []
    lifetime_by_class, fill_lifetime = {}, {}
    last_end, last = {}, None
    maxima_fields = experiment.maxima
    for row in lines(root, group + "episodes.ndjson", files[group + "episodes.ndjson"]):
        is_fill = fill is not None and type(row) is dict and row.get("kind") == fill.kind
        _closed(row, ("scope", "entity", "episode_id", "kind", "start_ns", "end_ns", "end_reason",
                      "censored", "opening_slice_survival_ns", "viable_tiers", "qualified_ns",
                      "qualified_by_skew_ns", "class_ns", "qualifying_class_ns", "open", "maxima",
                      "at_max") + (("fill",) if is_fill else ()))
        scope, index, kind = row["scope"], row["entity"], row["kind"]
        require(type(scope) is int and 0 <= scope < len(entities) and type(index) is int
                and 0 <= index < len(entities[scope]), "episode entity")
        entity = entities[scope][index]
        require(entity.admission is None and kind in kinds, "episode entity/kind")
        start, end = uint(row["start_ns"]), uint(row["end_ns"])
        scope_start, scope_end = int(scopes[scope]["start_ns"]), int(scopes[scope]["end_ns"])
        require(scope_start <= start < end <= scope_end, "episode bounds")
        identity = digest([scope, entity.id, kind, str(start)])
        require(row["episode_id"] == identity and identity not in index_of, "episode identity")
        require(type(row["censored"]) is bool and row["censored"] == (row["end_reason"] == "RUN_END"),
                "censor reason")
        if end == run_end:
            require(row["end_reason"] == "RUN_END", "episode end reason/scope")
        elif end == scope_end:
            require(row["end_reason"] == "SCOPE_END", "episode end reason/scope")
        else:
            require(row["end_reason"] in end_reasons or is_fill and row["end_reason"] == KILL_PRICE,
                    "episode end reason/scope")
        if is_fill:
            _check_fill(row["fill"], entity, row["end_reason"], fill, strategy,
                        [10 ** int(plans[key]["price_scale"]) for key in entity.legs], start)
            fill_lifetime[scope, index] = fill_lifetime.get((scope, index), 0) + end - start
        order = (end, scope) + entity.order + (kind, start)
        require(last is None or last <= order, "episode close order")
        last = order
        require(last_end.get((scope, index, kind), scope_start) <= start, "overlapping episodes")
        last_end[scope, index, kind] = end
        allowed = experiment.episode_classes[kind]
        classes = _ns_map(row["class_ns"], allowed)
        require(sum(classes.values()) == end - start, "episode class time")
        for name, amount in classes.items():
            key = (scope, index, kind, name)
            lifetime_by_class[key] = lifetime_by_class.get(key, 0) + amount
        game_fields = " game" if "game" in experiment.policy else ""
        opening = obj(row["open"], "value_class reasons leg_skew_ns skew_bucket values quotes" + game_fields)
        if game_fields:
            check_episode_game(opening["game"])
        require(opening["value_class"] in classes, "episode opening class")
        reason_indexes(opening["reasons"])
        opening_skew = skew(opening)
        facts_open = strategy.open_facts(opening["values"], entity, kind)
        strategy.check_open(opening["values"], facts_open, entity, kind, opening["value_class"], "episode")
        strategy.check_quotes(opening["quotes"], opening["values"], entity)
        maxima = obj(row["maxima"], " ".join(maxima_fields))
        for name in maxima_fields:
            value, open_value = maxima[name], opening["values"][name]
            parsed = None if value is None else signed(value)
            require(open_value is None or (parsed is not None and parsed >= signed(open_value)),
                    "maximum consistency")
        at_max = obj(row["at_max"], "values quotes" + game_fields)
        if game_fields:
            check_episode_game(at_max["game"])
            require(at_max["game"]["revision"] >= opening["game"]["revision"], "episode game revision order")
        strategy.open_facts(at_max["values"], entity, kind)
        strategy.check_quotes(at_max["quotes"], at_max["values"], entity)
        require(at_max["values"][maxima_fields[0]] == maxima[maxima_fields[0]], "values at maximum")
        require(type(row["viable_tiers"]) is list
                and row["viable_tiers"] == [t for t in tiers if t in row["viable_tiers"]], "viable tiers")
        qualified = obj(row["qualified_ns"], " ".join(tiers))
        by_skew = obj(row["qualified_by_skew_ns"], " ".join(tiers))
        qualifying = obj(row["qualifying_class_ns"], " ".join(tiers))
        previous = None
        for tier in tiers:
            q = uint(qualified[tier])
            buckets = by_skew[tier]
            require(type(buckets) is dict and all(k.isdigit() and int(k) <= len(experiment.skew_edges_ns)
                                                  for k in buckets), "skew bucket keys")
            require(sum(uint(v) for v in buckets.values()) == q, "skew-attributed entry time")
            tier_classes = _ns_map(qualifying[tier], allowed)
            require(all(tier_classes[c] <= classes.get(c, 0) and
                        (previous is None or tier_classes[c] <= previous.get(c, 0)) for c in tier_classes),
                    "qualifying class time")
            previous = tier_classes
        uint(row["opening_slice_survival_ns"])
        budget.reserve(1024)
        index_of[identity] = len(eps)
        eps.append({"scope": scope, "index": index, "entity": entity, "kind": kind, "start": start,
                    "end": end, "reason": row["end_reason"], "opening": uint(row["opening_slice_survival_ns"]),
                    "skew": opening_skew, "tiers": row["viable_tiers"],
                    "q": [uint(qualified[t]) for t in tiers], "seen_q": [0] * len(tiers), "reached": 0,
                    "last": start, "slices": [], "open": opening, "fill": is_fill})
        target = facts_for(entity)
        target.episode_count[kind] += 1
        if is_fill:
            target.fill_ends[row["end_reason"]] = target.fill_ends.get(row["end_reason"], 0) + 1
        target.lifetimes[kind].append(end - start)
        if row["censored"]:
            target.censored_episodes[kind] += 1
        for t, q in zip(tiers, (uint(qualified[t]) for t in tiers)):
            target.q[kind][t] += q
            for b, v in by_skew[t].items():
                target.q_by_skew[kind][t][b] = target.q_by_skew[kind][t].get(b, 0) + uint(v)
            for c, v in qualifying[t].items():
                target.qualifying[kind][t][c] = target.qualifying[kind][t].get(c, 0) + uint(v)

    # Each kind's positive-class time is exactly its episodes' lifetimes. Fill
    # episodes lie inside the trigger kind's positive-class time and add up to
    # exactly its live share.
    for (scope, index), (status, classes, _) in denominators.items():
        if entities[scope][index].admission is not None:
            continue
        for kind in kinds:
            for name in experiment.episode_classes[kind]:
                lifetime = lifetime_by_class.get((scope, index, kind, name), 0)
                if fill is not None and kind == fill.kind:
                    require(lifetime <= classes.get(name, 0), "fill episodes inside trigger time")
                else:
                    require(lifetime == classes.get(name, 0), "episode lifetimes/denominator class time")
        if fill is not None:
            require(fill_lifetime.get((scope, index), 0) == fill_live.get((scope, index), 0),
                    "fill episode lifetimes/live fill time")

    # -- slices ------------------------------------------------------------------
    if group + "slices.ndjson" in files:
        last = None
        for row in lines(root, group + "slices.ndjson", files[group + "slices.ndjson"]):
            _closed(row, ("episode_id", "start_ns", "end_ns", "end_reason", "censored", "leg_skew_ns",
                          "skew_bucket"))
            require(type(row["episode_id"]) is str and row["episode_id"] in index_of, "orphan slice")
            ep = eps[index_of[row["episode_id"]]]
            start, end = uint(row["start_ns"]), uint(row["end_ns"])
            require(start == ep["last"] and start < end <= ep["end"], "slice partition/order")
            require(not ep["fill"] or (start == ep["start"] and end == ep["end"]),
                    "fill episodes have a single slice")
            require(row["end_reason"] == (ep["reason"] if end == ep["end"] else "CONSUMED_CHANGED"),
                    "slice end reason/parent")
            require(row["censored"] == (row["end_reason"] == "RUN_END"), "slice censor reason")
            bucket = skew(row)
            if start == ep["start"]:
                require(bucket == ep["skew"] and row["leg_skew_ns"] == ep["open"]["leg_skew_ns"],
                        "opening slice skew")
                require(ep["opening"] == end - start, "opening survival")
            order = (end, ep["scope"]) + ep["entity"].order + (ep["kind"], start)
            require(last is None or last <= order, "slice close order")
            last = order
            ep["last"] = end
            for ti, tier in enumerate(tier_ints):
                if end - start >= tier:
                    ep["reached"] |= 1 << ti
                ep["seen_q"][ti] += max(0, end - start - tier)
            budget.reserve(64)
            ep["slices"].append((start, end, row["end_reason"], row["censored"], row["leg_skew_ns"], bucket))
            target = facts_for(ep["entity"])
            target.slice_count[ep["kind"]] += 1
            target.survival[ep["kind"]].append(end - start)
            spread = target.slice_ns_by_skew[ep["kind"]]
            spread[str(bucket)] = spread.get(str(bucket), 0) + end - start
            if row["censored"]:
                target.censored_slices[ep["kind"]] += 1
        for ep in eps:
            require(ep["last"] == ep["end"], "slice partition/order")
            require([t for ti, t in enumerate(tiers) if ep["reached"] & (1 << ti)] == ep["tiers"],
                    "episode tiers")
            require(ep["seen_q"] == ep["q"], "episode Q")

    if group == "" and experiment.audit_intervals:
        _audit(root, files, entities, scopes, experiment, strategy, statuses, kinds, texts,
               reason_indexes, denominators, eps, index_of, budget)
    return facts


def _audit(root, files, entities, scopes, experiment, strategy, statuses, kinds, texts,
           reason_indexes, denominators, eps, index_of, budget):
    """Full interval partition: recompute denominators and episodes exactly."""
    intervals = {}
    recomputed = {}
    last = None
    for row in lines(root, "audit/measurements.ndjson", files["audit/measurements.ndjson"]):
        _closed(row, ("scope", "entity", "start_ns", "end_ns", "status", "reasons"),
                ("value_class",) + experiment.measurement_fields)
        scope, index = row["scope"], row["entity"]
        require(type(scope) is int and 0 <= scope < len(entities) and type(index) is int
                and 0 <= index < len(entities[scope]), "audit entity")
        entity = entities[scope][index]
        start, end = uint(row["start_ns"]), uint(row["end_ns"])
        require(start < end, "empty interval")
        status, value_class = row["status"], row.get("value_class")
        require(status in statuses, "measurement status")
        require((value_class is not None) == (status == EVALUATED)
                and (value_class is None or value_class in experiment.value_classes), "value class")
        indexes = tuple(reason_indexes(row["reasons"]))
        order = (end, scope) + entity.order + ("", start)
        require(last is None or last <= order, "measurement close order")
        last = order
        runs = intervals.setdefault((scope, index), [])
        require((runs[-1][1] if runs else int(scopes[scope]["start_ns"])) == start,
                "measurement gap/overlap/order")
        budget.reserve(128)
        runs.append((start, end, status, value_class))
        totals = recomputed.setdefault((scope, index), ({}, {}, {}))
        totals[0][status] = totals[0].get(status, 0) + end - start
        if value_class is not None:
            totals[1][value_class] = totals[1].get(value_class, 0) + end - start
        if indexes:
            totals[2][status, indexes] = totals[2].get((status, indexes), 0) + end - start
    for scope, members in enumerate(entities):
        for index in range(len(members)):
            runs = intervals.get((scope, index))
            require(runs and runs[-1][1] == int(scopes[scope]["end_ns"]), "incomplete measurements")
            status, classes, reasons = recomputed[scope, index]
            stored = denominators[scope, index]
            require(stored[0] == status and stored[1] == classes
                    and stored[2] == [(s, i, v) for (s, i), v in sorted(reasons.items())],
                    "audit/denominator mismatch")

    # Maximal predicate runs are exactly the episodes, with their end reasons.
    # A fill kind's episodes instead lie inside its runs: one ending at a run's
    # end carries the run's trigger-false reason, one ending inside it is killed.
    fill_kind = experiment.fills.kind if experiment.fills is not None else None
    expected, fill_runs = set(), {}
    for (scope, index), runs in intervals.items():
        for kind in kinds:
            allowed = experiment.episode_classes[kind]
            opened = None
            for i, (start, end, status, value_class) in enumerate(runs):
                active = value_class in allowed
                if active and opened is None:
                    opened = start
                if opened is not None and (not active or i + 1 == len(runs)):
                    stop = end if active else start
                    after = None if active else (
                        "PREDICATE_FALSE" if status == EVALUATED else status)
                    if kind == fill_kind:
                        fill_runs.setdefault((scope, index), []).append((opened, stop, after))
                    else:
                        expected.add((scope, index, kind, opened, stop, after))
                    opened = None
    found = set()
    for ep in eps:
        after = None if ep["reason"] in ("SCOPE_END", "RUN_END") else ep["reason"]
        if ep["kind"] == fill_kind:
            runs = fill_runs.get((ep["scope"], ep["index"]), [])
            i = bisect_right([run[0] for run in runs], ep["start"]) - 1
            require(i >= 0 and ep["end"] <= runs[i][1], "audit fill episode inside trigger run")
            require(after == runs[i][2] if ep["end"] == runs[i][1] else after == KILL_PRICE,
                    "audit fill episode end reason")
            continue
        found.add((ep["scope"], ep["index"], ep["kind"], ep["start"], ep["end"], after))
    require(found == expected, "audit maximal runs/episodes")

    positions = {}
    for row in lines(root, "audit/slices.ndjson", files["audit/slices.ndjson"]):
        _closed(row, ("episode_id", "start_ns", "end_ns", "end_reason", "censored", "leg_skew_ns",
                      "skew_bucket", "quotes", "values"))
        require(row["episode_id"] in index_of, "orphan slice")
        ep = eps[index_of[row["episode_id"]]]
        position = positions.get(row["episode_id"], 0)
        require(position < len(ep["slices"]) and ep["slices"][position] == (
            uint(row["start_ns"]), uint(row["end_ns"]), row["end_reason"], row["censored"],
            row["leg_skew_ns"], row["skew_bucket"]), "audit slice/compact slice")
        positions[row["episode_id"]] = position + 1
        entity, kind, start = ep["entity"], ep["kind"], uint(row["start_ns"])
        runs = intervals[ep["scope"], ep["index"]]
        i = bisect_right([r[0] for r in runs], start) - 1
        opening_class = runs[i][3]
        values = row["values"]
        facts = strategy.open_facts(values, entity, kind)
        strategy.check_open(values, facts, entity, kind, opening_class, "slice")
        strategy.check_quotes(row["quotes"], values, entity)
        if position == 0:
            require(values == ep["open"]["values"] and row["quotes"] == ep["open"]["quotes"],
                    "episode/first slice opening values")
        else:
            require(row["quotes"] != ep["previous_quotes"], "adjacent slices must change consumed depth")
        ep["previous_quotes"] = row["quotes"]
    for ep in eps:
        require(positions.get(digest([ep["scope"], ep["entity"].id, ep["kind"], str(ep["start"])]), 0)
                == len(ep["slices"]), "audit slice coverage")
