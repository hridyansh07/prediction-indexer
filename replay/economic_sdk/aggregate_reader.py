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
  denominators exactly and its maximal predicate runs equal the episodes.

Denominators are writer-attested without the audit: the reader proves their
internal arithmetic and their agreement with the episodes, not each interval.
"""

from __future__ import annotations

from array import array
from bisect import bisect_right
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.entity_tables import entity_rows
from replay.economic_sdk.output import aggregate_files, file_list, group_of
from replay.economic_sdk.reader import Budget, check_files, document, lines, quantiles, signed
from replay.economic_sdk.types import ADMISSIONS, EVALUATED, SDK_STATUSES
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
                 "qualifying", "slice_ns_by_skew")

    def __init__(self, kinds, tiers, episodes):
        self.status_ns, self.class_ns, self.episodes = {}, {}, episodes
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


def validate(directory, snapshot, manifest, strategy):
    experiment = strategy.experiment
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
            end_reasons, kinds, tiers, tier_ints, texts, reason_indexes, skew, budget)

    budget.reserve(bounds.MAX_METADATA)
    return strategy.summarize_aggregate(
        Aggregates(aggregates, all_entities, table["reasons"], budget), manifest, snapshot)


def _group(root, group, names, files, entities, scopes, run_end, experiment, strategy, statuses,
           end_reasons, kinds, tiers, tier_ints, texts, reason_indexes, skew, budget):
    facts = {}
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
        _closed(row, ("scope", "entity", "status_ns"), ("class_ns", "reason_ns"))
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
        for name, amount in status.items():
            target.status_ns[name] = target.status_ns.get(name, 0) + amount
        for name, amount in classes.items():
            target.class_ns[name] = target.class_ns.get(name, 0) + amount
    require(position == len(expected_next), "denominator order/coverage")

    if not episodes_present:
        return facts

    # -- episodes ----------------------------------------------------------------
    index_of, eps = {}, []
    lifetime_by_class = {}
    last_end, last = {}, None
    maxima_fields = experiment.maxima
    for row in lines(root, group + "episodes.ndjson", files[group + "episodes.ndjson"]):
        _closed(row, ("scope", "entity", "episode_id", "kind", "start_ns", "end_ns", "end_reason",
                      "censored", "opening_slice_survival_ns", "viable_tiers", "qualified_ns",
                      "qualified_by_skew_ns", "class_ns", "qualifying_class_ns", "open", "maxima",
                      "at_max"))
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
            require(row["end_reason"] in end_reasons, "episode end reason/scope")
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
        opening = obj(row["open"], "value_class reasons leg_skew_ns skew_bucket values quotes")
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
        at_max = obj(row["at_max"], "values quotes")
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
                    "last": start, "slices": [], "open": opening})
        target = facts_for(entity)
        target.episode_count[kind] += 1
        target.lifetimes[kind].append(end - start)
        if row["censored"]:
            target.censored_episodes[kind] += 1
        for t, q in zip(tiers, (uint(qualified[t]) for t in tiers)):
            target.q[kind][t] += q
            for b, v in by_skew[t].items():
                target.q_by_skew[kind][t][b] = target.q_by_skew[kind][t].get(b, 0) + uint(v)
            for c, v in qualifying[t].items():
                target.qualifying[kind][t][c] = target.qualifying[kind][t].get(c, 0) + uint(v)

    # Each kind's positive-class time is exactly its episodes' lifetimes.
    for (scope, index), (status, classes, _) in denominators.items():
        if entities[scope][index].admission is not None:
            continue
        for kind in kinds:
            for name in experiment.episode_classes[kind]:
                require(lifetime_by_class.get((scope, index, kind, name), 0) == classes.get(name, 0),
                        "episode lifetimes/denominator class time")

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
    expected = set()
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
                    expected.add((scope, index, kind, opened, stop, after))
                    opened = None
    found = set()
    for ep in eps:
        after = None if ep["reason"] in ("SCOPE_END", "RUN_END") else ep["reason"]
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
