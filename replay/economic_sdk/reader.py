"""Independent reader for layout-1 output (the frozen complement V1 wire).

Layout 2 is read by ``aggregate_reader``; the helpers below are shared.

It does not import the runtime. From the manifest's policy and the snapshot it
re-resolves the scoped entities, then streams every file once and checks:

- closed row schemas, file identities, bounds and close order;
- a complete half-open measurement partition per scoped entity;
- every maximal predicate run is exactly one episode, with the expected end;
- slices partition their episodes; lifetimes, tiers and ``Q`` recompute;
- skew attribution of time and of latency-qualified entry time.

Strategy hooks add their own checks (payload schema, quote arithmetic) and turn
the recomputed aggregates into the summary. This verifies internal
consistency; it is not a second reconstruction from the raw tape.
"""

from __future__ import annotations

import hashlib
from array import array
from bisect import bisect_right
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.entities import resolve
from replay.economic_sdk.output import Layout
from replay.economic_sdk.types import ADMISSIONS, CONTROL, EVALUATED, REAL, SDK_STATUSES
from replay.preparation import digest, sha
from replay.strategy_sdk import plain
from replay.streams.protocol import decode, obj, require, uint

UNKNOWN_SKEW = 2**64 - 1


def read_json(path):
    require(path.is_file() and not path.is_symlink(), "regular output required")
    with path.open("rb") as stream:
        return decode(stream.read(bounds.MAX_METADATA + 1), bounds.MAX_METADATA)


def signed(value):
    require(type(value) is str and value and value != "-0", "canonical signed integer")
    body = value[1:] if value.startswith("-") else value
    require(body.isascii() and body.isdigit() and (body == "0" or body[0] != "0"),
            "canonical signed integer")
    return int(value)


def file_identity(value):
    value = obj(value, "sha256 byte_length records")
    sha(value["sha256"])
    require(type(value["byte_length"]) is int and 0 <= value["byte_length"] <= bounds.MAX_BYTES)
    require(type(value["records"]) is int and 0 <= value["records"] <= bounds.MAX_ROWS)
    return value


def check_files(files, names):
    require(type(files) is dict and set(files) == set(names), "output file set")
    for identity in files.values():
        file_identity(identity)


def lines(root, name, identity):
    path = root / name
    require(path.is_file() and not path.is_symlink(), "regular output required")
    checksum = hashlib.sha256()
    size = count = 0
    with path.open("rb") as stream:
        while payload := stream.readline(bounds.MAX_LINE + 1):
            require(len(payload) <= bounds.MAX_LINE and payload.endswith(b"\n"), "line/truncation")
            size += len(payload)
            count += 1
            require(size <= bounds.MAX_BYTES and count <= bounds.MAX_ROWS, "output budget")
            checksum.update(payload)
            yield decode(payload, bounds.MAX_LINE)
    require({"sha256": checksum.hexdigest(), "byte_length": size, "records": count} == identity,
            "file identity")


def document(root, name, identity):
    """One-record NDJSON table (entities, reasons), bounded by MAX_METADATA.

    Tables are written as a single record up to the metadata bound, which real
    bundles exceed the per-row MAX_LINE with; the file identity still binds it.
    """
    path = root / name
    require(path.is_file() and not path.is_symlink(), "regular output required")
    with path.open("rb") as stream:
        payload = stream.read(bounds.MAX_METADATA + 1)
    require(len(payload) <= bounds.MAX_METADATA, "table/size")
    require(payload.endswith(b"\n") and payload.count(b"\n") == 1, "table is one record")
    require({"sha256": hashlib.sha256(payload).hexdigest(), "byte_length": len(payload),
             "records": 1} == identity, "file identity")
    return decode(payload, bounds.MAX_METADATA)


def bucket(value):
    if value is None:
        return "unknown"
    require(type(value) is int and value >= 0, "skew bucket")
    return str(value)


class Budget:
    """Conservative O(1) accounting for reader-owned detached state."""

    def __init__(self, *roots, limit=bounds.MAX_STATE):
        self.limit = bounds.StateBudget(limit).limit
        self.used = sum(bounds.json_cost(root) for root in roots)
        require(self.used <= self.limit, "reader state budget")

    def reserve(self, growth, message="reader state budget"):
        require(self.used + growth <= self.limit, message)
        self.used += growth

    def release(self, size):
        self.used -= size


def quantiles(values, budget=None):
    """Exact nearest-rank quantiles (rank ``ceil(p * n)``) over compact integers."""
    if not values:
        return {k: None for k in ("p50", "p90", "p99", "max")}
    # sorted(array) would box every value; quickselect mutates one compact copy.
    source = (values.buffer_info()[1] * values.itemsize
              if isinstance(values, array) else len(values) * 8)
    working = source + len(values) * 8
    if budget is not None:
        budget.reserve(working, "reader state budget (quantile working copy)")
    else:
        require(working <= bounds.MAX_STATE, "reader state budget (quantile working copy)")
    work = array("Q", values)

    def select(k):
        lo, hi = 0, len(work) - 1
        while lo < hi:
            pivot = work[(lo + hi) // 2]
            i, j = lo, hi
            while i <= j:
                while work[i] < pivot:
                    i += 1
                while work[j] > pivot:
                    j -= 1
                if i <= j:
                    work[i], work[j] = work[j], work[i]
                    i += 1
                    j -= 1
            if k <= j:
                hi = j
            elif k >= i:
                lo = i
            else:
                return work[k]
        return work[lo]

    n = len(work)
    result = {name: str(select((n * p + 99) // 100 - 1))
              for name, p in (("p50", 50), ("p90", 90), ("p99", 99), ("max", 100))}
    if budget is not None:
        budget.release(working)
    return result


class Group:
    """Recomputed facts for one summary key and skew bucket."""

    __slots__ = ("durations", "episode_count", "slice_count", "q", "episode_values",
                 "slice_values", "censored_episodes", "censored_slices")

    def __init__(self, kinds, tiers):
        self.durations = {}
        self.episode_count = {k: 0 for k in kinds}
        self.slice_count = {k: 0 for k in kinds}
        self.q = {k: {t: 0 for t in tiers} for k in kinds}
        self.episode_values = {k: array("Q") for k in kinds}
        self.slice_values = {k: array("Q") for k in kinds}
        self.censored_episodes = {k: 0 for k in kinds}
        self.censored_slices = {k: 0 for k in kinds}


class Aggregates:
    def __init__(self, groups, entities, budget):
        self.groups, self.entities, self.budget = groups, entities, budget

    def quantiles(self, values):
        return quantiles(values, self.budget)


def validate(directory, snapshot, manifest, strategy, *, state_bytes=bounds.MAX_STATE):
    """Validate the semantic files and return the strategy's derived summary."""
    experiment = strategy.experiment
    snapshot = plain(snapshot)
    root = Path(directory)
    layout = Layout(experiment)
    check_files(manifest["files"], layout.files + strategy.extra_files())
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}
    kinds, tiers = experiment.kinds, experiment.tiers_ns
    tier_ints = [int(t) for t in tiers]
    field_names = experiment.measurement_fields
    statuses = set(SDK_STATUSES + ADMISSIONS + experiment.diagnostic_statuses)
    end_reasons = set(SDK_STATUSES + experiment.diagnostic_statuses + ("PREDICATE_FALSE", "SCOPE_END", "RUN_END")) - {EVALUATED}
    budget = Budget(snapshot, manifest, experiment.policy, limit=state_bytes)

    expected, entities = {}, {}
    for scope in range(len(snapshot["scopes"])):
        for entity_id, entity in resolve(strategy, snapshot, scope, plans).items():
            budget.reserve(1024, "reader state budget (layout)")
            expected[scope, entity_id] = entity
            entities[entity_id] = entity
    require(set(manifest.get("instantaneous_positive", {})) <= set(entities), "instantaneous entity")

    # Five compact unsigned arrays per key plus one predicate bitmask byte per row.
    measurements, masks = {}, {}
    for key in expected:
        budget.reserve(512, "reader state budget (measurement layout)")
        measurements[key] = tuple(array("Q") for _ in range(5))
        masks[key] = bytearray()
    status_codes, class_codes = {}, {}
    class_bits = {None: 0}
    for value in experiment.value_classes:
        class_bits[value] = sum(1 << i for i, k in enumerate(kinds)
                                if value in experiment.episode_classes[k])

    def common(row):
        require(type(row["version"]) is int and row["version"] == layout.version)
        require(row["experiment_sha256"] == experiment.experiment_sha256)
        require(type(row["scope"]) is int and 0 <= row["scope"] <= 2**64 - 1
                and (row["scope"], row["entity"]) in expected, "unknown scoped entity")
        sha(row["entity"])
        start, end = uint(row["start_ns"]), uint(row["end_ns"])
        require(start < end, "empty interval")
        return start, end

    def order_key(row, entity, kind=""):
        return (int(row["end_ns"]), row["scope"]) + entity.order + (kind, int(row["start_ns"]))

    def code_of(codes, value):
        return next(k for k, v in codes.items() if v == value)

    fields = "version experiment_sha256 scope entity start_ns end_ns status reasons skew_bucket value_class"
    if field_names:
        fields += " " + " ".join(field_names)
    seen_classes = {}
    for cls in (REAL, CONTROL):
        if ("measurements", cls) not in layout.names:
            continue
        name = layout.name("measurements", cls)
        if name in seen_classes:
            continue
        seen_classes[name] = cls
        mixed = layout.version == 1
        last_order = None
        for row in lines(root, name, manifest["files"][name]):
            obj(row, fields)
            start, end = common(row)
            key = row["scope"], row["entity"]
            entity = expected[key]
            require(mixed or entity.cls == cls, "measurement entity class/file")
            order = order_key(row, entity)
            require(last_order is None or last_order <= order, "measurement close order")
            last_order = order
            require(type(row["reasons"]) is list and all(type(x) is str for x in row["reasons"]))
            status, value_class = row["status"], row["value_class"]
            require(status in statuses, "measurement status")
            require(value_class is None or value_class in experiment.value_classes, "value class")
            if status == EVALUATED:
                require(value_class is not None, "missing value class")
            else:
                require(value_class is None, "class on unevaluated interval")
            strategy.check_measurement(row, entity)
            if entity.admission is not None:
                require(status == entity.admission, "admission/measurement mismatch")
            a = measurements[key]
            require(not a[1] or start == a[1][-1], "measurement gap/overlap/order")
            budget.reserve(128)
            a[0].append(start)
            a[1].append(end)
            a[2].append(status_codes.setdefault(status, len(status_codes)))
            a[3].append(class_codes.setdefault(value_class, len(class_codes)))
            skew = row["skew_bucket"]
            require(skew is None or (type(skew) is int and 0 <= skew <= len(experiment.skew_edges_ns)),
                    "skew bucket integer")
            a[4].append(UNKNOWN_SKEW if skew is None else skew)
            masks[key].append(class_bits[value_class])
    for key, a in measurements.items():
        scope = snapshot["scopes"][key[0]]
        require(a[0] and a[0][0] == int(scope["start_ns"]) and a[1][-1] == int(scope["end_ns"]),
                "incomplete measurements")

    groups = {}

    def group(entity_id, skew):
        group_key = strategy.summary_key(entities[entity_id]) + (skew,)
        if group_key not in groups:
            budget.reserve(2048)
            groups[group_key] = Group(kinds, tiers)
        return groups[group_key]

    def skew_at(a, i):
        return bucket(None if a[4][i] == UNKNOWN_SKEW else a[4][i])

    for key, a in measurements.items():
        for i in range(len(a[0])):
            label = strategy.duration_label(code_of(status_codes, a[2][i]),
                                            code_of(class_codes, a[3][i]))
            g = group(key[1], skew_at(a, i))
            g.durations[label] = g.durations.get(label, 0) + a[1][i] - a[0][i]

    def measurement_at(key, when):
        a = measurements[key]
        i = bisect_right(a[0], when) - 1
        if i >= 0 and when < a[1][i]:
            return a, i
        require(False, "opening outside measurement coverage")

    # Compact parallel episode facts; no decoded row survives except the
    # opening payload needed for the first-slice comparison.
    index_of = {}
    ep = {name: [] for name in ("scope", "entity", "kind", "start", "end", "opening", "last",
                                "quotes", "q", "expected_q", "reached", "tiers", "reason",
                                "open_values", "maxima", "slice_maxima")}
    runs_seen = {key: [0] * len(kinds) for key in expected}
    episode_rows = 0
    max_fields = tuple("max_" + name for name in experiment.maxima)

    for cls in (REAL, CONTROL):
        if ("episodes", cls) not in layout.names:
            continue
        name = layout.name("episodes", cls)
        fields = ("version experiment_sha256 scope entity start_ns end_ns episode_id kind basket "
                  "end_reason censored gap_lifetime_ns opening_slice_survival_ns viable_tiers "
                  "qualified_ns open_values opening_skew_bucket " + " ".join(max_fields))
        last = None
        for row in lines(root, name, manifest["files"][name]):
            episode_rows += 1
            require(episode_rows <= bounds.MAX_ROWS, "combined episode row budget")
            obj(row, fields)
            start, end = common(row)
            key = row["scope"], row["entity"]
            entity = expected[key]
            require(entity.cls == cls and row["basket"] == entity.descriptor, "episode basket")
            require(row["kind"] in kinds and row["episode_id"] == digest(
                [row["scope"], row["entity"], row["kind"], str(start)]))
            kind_index = kinds.index(row["kind"])
            sha(row["episode_id"])
            require(uint(row["gap_lifetime_ns"]) == end - start)
            require(type(row["censored"]) is bool and row["end_reason"] in end_reasons)
            require(row["censored"] == (row["end_reason"] == "RUN_END"), "censor reason")
            require(uint(row["opening_slice_survival_ns"]) <= end - start)
            facts = strategy.open_facts(row["open_values"], entity, row["kind"])
            a, mi = measurement_at(key, start)
            require(start == a[0][mi], "episode not aligned to measurement start")
            opening_class = code_of(class_codes, a[3][mi])
            require(class_bits[opening_class] >> kind_index & 1, "episode/class mismatch")
            require(bucket(row["opening_skew_bucket"]) == skew_at(a, mi), "episode opening skew")
            strategy.check_open(row["open_values"], facts, entity, row["kind"], opening_class, "episode")
            maxima = {}
            for field in experiment.maxima:
                value = row["max_" + field]
                opening = row["open_values"][field]
                parsed = None if value is None else signed(value)
                require(opening is None or (parsed is not None and parsed >= signed(opening)),
                        "maximum consistency")
                maxima[field] = parsed
            require(type(row["viable_tiers"]) is list
                    and row["viable_tiers"] == sorted(set(row["viable_tiers"]), key=int))
            require(type(row["qualified_ns"]) is dict and set(row["qualified_ns"]) == set(tiers))
            for v in row["qualified_ns"].values():
                uint(v)
            order = order_key(row, entity, row["kind"])
            require(last is None or last <= order, "episode close order")
            last = order
            require(row["episode_id"] not in index_of, "duplicate episode")
            # The interval must be exactly one maximal predicate run.
            bit = 1 << kind_index
            mask = masks[key]
            require(mask[mi] & bit, "episode predicate")
            require(mi == 0 or not mask[mi - 1] & bit, "episode not maximal at start")
            stop = mi
            while stop + 1 < len(a[0]) and mask[stop + 1] & bit:
                stop += 1
            require(end == a[1][stop], "episode not maximal at end")
            reason = ("RUN_END" if end == int(snapshot["scopes"][-1]["end_ns"]) else
                      "SCOPE_END" if end == int(snapshot["scopes"][row["scope"]]["end_ns"]) else None)
            if reason is None:
                next_status = code_of(status_codes, a[2][stop + 1])
                reason = "PREDICATE_FALSE" if next_status == EVALUATED else next_status
            require(row["end_reason"] == reason, "episode end reason/scope")
            budget.reserve(768)
            index_of[row["episode_id"]] = len(ep["start"])
            ep["scope"].append(row["scope"]); ep["entity"].append(row["entity"])
            ep["kind"].append(row["kind"]); ep["start"].append(start); ep["end"].append(end)
            ep["opening"].append(uint(row["opening_slice_survival_ns"])); ep["last"].append(start)
            ep["quotes"].append(None); ep["q"].append([0] * len(tiers))
            ep["expected_q"].append([uint(row["qualified_ns"][t]) for t in tiers])
            ep["reached"].append(0); ep["tiers"].append(tuple(row["viable_tiers"]))
            ep["reason"].append(row["end_reason"]); ep["open_values"].append(row["open_values"])
            ep["maxima"].append(maxima); ep["slice_maxima"].append({})
            runs_seen[key][kind_index] += 1
            g = group(row["entity"], bucket(row["opening_skew_bucket"]))
            g.episode_count[row["kind"]] += 1
            g.episode_values[row["kind"]].append(end - start)
            if row["censored"]:
                g.censored_episodes[row["kind"]] += 1

    # Classes determine the required number of maximal runs.
    for key, a in measurements.items():
        for kind_index in range(len(kinds)):
            bit, runs, opened = 1 << kind_index, 0, False
            for value in masks[key]:
                if value & bit and not opened:
                    runs += 1
                    opened = True
                elif not value & bit:
                    opened = False
            require(runs == runs_seen[key][kind_index], "episode predicate/maximal coverage")

    slice_reasons = end_reasons | {"CONSUMED_CHANGED"}
    last = None
    for row in lines(root, layout.name("slices", REAL), manifest["files"][layout.name("slices", REAL)]):
        obj(row, "version experiment_sha256 scope entity start_ns end_ns episode_id kind "
                 "end_reason censored consumed survival_ns viable_tiers open_values "
                 "opening_skew_bucket")
        start, end = common(row)
        require(row["episode_id"] in index_of, "orphan slice")
        ei = index_of[row["episode_id"]]
        require(row["scope"] == ep["scope"][ei] and row["entity"] == ep["entity"][ei],
                "slice parent scope/entity")
        require(row["kind"] in kinds, "slice kind")
        require(row["kind"] == ep["kind"][ei] and ep["start"][ei] <= start < end <= ep["end"][ei])
        require(uint(row["survival_ns"]) == end - start and type(row["censored"]) is bool)
        key = ep["scope"][ei], ep["entity"][ei]
        entity, kind = expected[key], ep["kind"][ei]
        require(row["end_reason"] in slice_reasons)
        require(row["censored"] == (row["end_reason"] == "RUN_END"), "slice censor reason")
        require(row["end_reason"] == (ep["reason"][ei] if end == ep["end"][ei] else "CONSUMED_CHANGED"),
                "slice end reason/parent")
        a, mi = measurement_at(key, start)
        opening_class = code_of(class_codes, a[3][mi])
        bit = 1 << kinds.index(kind)
        require(class_bits[opening_class] & bit, "slice/class mismatch")
        facts = strategy.open_facts(row["open_values"], entity, kind)
        strategy.check_open(row["open_values"], facts, entity, kind, opening_class, "slice")
        overlap = mi
        while overlap < len(a[0]) and a[0][overlap] < end:
            require(masks[key][overlap] & bit, "slice/measurement eligibility mismatch")
            overlap += 1
        require(bucket(row["opening_skew_bucket"]) == skew_at(a, mi), "slice opening skew")
        require(row["viable_tiers"] == [t for t in tiers if end - start >= int(t)], "slice tiers")
        order = order_key(row, entity, kind)
        require(last is None or last <= order, "slice close order")
        last = order
        strategy.check_quotes(row["consumed"], row["open_values"], entity)
        quotes = digest(row["consumed"])
        if start == ep["start"][ei]:
            require(row["open_values"] == ep["open_values"][ei], "episode/first slice opening values")
        if ep["quotes"][ei] is not None:
            require(ep["quotes"][ei] != quotes, "adjacent slices must change consumed depth")
        ep["quotes"][ei] = quotes
        for field in experiment.slice_invariant:
            value = row["open_values"][field]
            prior = ep["slice_maxima"][ei].get(field)
            if value is not None and (prior is None or signed(value) > prior):
                ep["slice_maxima"][ei][field] = signed(value)
        require(start == ep["last"][ei], "slice partition/order")
        budget.reserve(64)
        ep["last"][ei] = end
        if start == ep["start"][ei]:
            require(ep["opening"][ei] == end - start, "opening survival")
        for ti, tier in enumerate(tier_ints):
            if end - start >= tier:
                ep["reached"][ei] |= 1 << ti
            ep["q"][ei][ti] += max(0, end - start - tier)
        g = group(key[1], skew_at(a, mi))
        g.slice_count[kind] += 1
        g.slice_values[kind].append(end - start)
        if row["censored"]:
            g.censored_slices[kind] += 1
        # Entry time is attributed to the skew at each entry instant.
        for tier, tier_int in zip(tiers, tier_ints):
            qend = end - tier_int
            i = max(0, bisect_right(a[1], start))
            while i < len(a[0]) and a[0][i] < qend:
                overlap = max(0, min(qend, a[1][i]) - max(start, a[0][i]))
                if overlap:
                    group(key[1], skew_at(a, i)).q[kind][tier] += overlap
                i += 1

    for ei in range(len(ep["start"])):
        require(ep["quotes"][ei] is not None and ep["last"][ei] == ep["end"][ei], "slice partition/order")
        for field in experiment.slice_invariant:
            require(ep["slice_maxima"][ei].get(field) == ep["maxima"][ei][field], "episode maximum/slices")
        reached = [t for ti, t in enumerate(tiers) if ep["reached"][ei] & (1 << ti)]
        require(reached == list(ep["tiers"][ei]), "episode tiers")
        require(ep["q"][ei] == ep["expected_q"][ei], "episode Q")

    budget.reserve(bounds.MAX_METADATA)
    return strategy.summarize(Aggregates(groups, entities, budget), manifest, snapshot)
