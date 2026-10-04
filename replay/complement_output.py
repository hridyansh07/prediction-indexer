"""Independent, bounded reader for same-venue-complement output.

This module intentionally does not import the strategy implementation.  It checks
the wire format and derives the report from interval facts.
"""

from array import array
from bisect import bisect_right
import hashlib
from pathlib import Path
import sys

from replay.complement_contract import (
    FILES, MAX_BYTES, MAX_LINE, MAX_METADATA, MAX_ROWS, MAX_STATE, PAYOUT,
    STRATEGY, Inputs, experiment_identity, layouts, policy_config, row_order,
)
from replay.preparation import digest, encoded, load_snapshot, sha
from replay.strategy_sdk import plain
from replay.streams.protocol import decode, freeze, obj, require, uint
from replay.supervisor import initial, read_success
from replay.supervisor import read as read_run


def _json(path):
    require(path.is_file() and not path.is_symlink(), "regular output required")
    with path.open("rb") as stream:
        return decode(stream.read(MAX_METADATA + 1), MAX_METADATA)


def _signed(value):
    require(type(value) is str and value and value != "-0", "canonical signed integer")
    body = value[1:] if value.startswith("-") else value
    require(body.isascii() and body.isdigit() and (body == "0" or body[0] != "0"),
            "canonical signed integer")
    return int(value)


def _identity(value):
    value = obj(value, "sha256 byte_length records")
    sha(value["sha256"])
    require(type(value["byte_length"]) is int and 0 <= value["byte_length"] <= MAX_BYTES)
    require(type(value["records"]) is int and 0 <= value["records"] <= MAX_ROWS)
    return value


def _manifest(value, complete=True):
    fields = "version strategy snapshot_sha256 policy policy_sha256 experiment_sha256 fee_config fee_engine_identity files instantaneous_positive payout_assumption"
    if complete:
        fields += " summary_sha256"
    obj(value, fields)
    require(type(value["version"]) is int and value["version"] == 1)
    require(value["strategy"] == STRATEGY and value["payout_assumption"] == PAYOUT)
    for field in ("snapshot_sha256", "policy_sha256", "experiment_sha256", "fee_engine_identity"):
        sha(value[field])
    if complete:
        sha(value["summary_sha256"])
    policy_config(value["policy"])
    require(digest(value["policy"]) == value["policy_sha256"], "policy identity")
    fee_fields = {"catalog_identity", "reference_ns", "limitless_buy_bps",
                  "limitless_sell_bps", "kalshi_member_class", "assets",
                  "instrument_bindings"}
    require(type(value["fee_config"]) is dict and set(value["fee_config"]) == fee_fields,
            "closed semantic fee configuration")
    require(experiment_identity(value["snapshot_sha256"], value["policy"], value["fee_config"])
            == value["experiment_sha256"], "experiment identity")
    require(type(value["files"]) is dict and set(value["files"]) == set(FILES), "output file set")
    for identity in value["files"].values():
        _identity(identity)
    require(type(value["instantaneous_positive"]) is dict)
    for key, count in value["instantaneous_positive"].items():
        sha(key)
        require(type(count) is int and 0 <= count <= 2**64 - 1)


def _common(row, expected, experiment):
    require(type(row["version"]) is int and row["version"] == 1)
    require(row["experiment_sha256"] == experiment)
    require(type(row["scope"]) is int and 0 <= row["scope"] <= 2**64 - 1
            and (row["scope"], row["entity"]) in expected,
            "unknown scoped entity")
    sha(row["entity"])
    start, end = uint(row["start_ns"]), uint(row["end_ns"])
    require(start < end, "empty interval")
    return start, end


def _open_values(value, descriptor, plans):
    obj(value, "gap_gross gap_net gross_scale net_scale fee_status assessments assumptions evidence")
    gross = _signed(value["gap_gross"])
    net = None if value["gap_net"] is None else _signed(value["gap_net"])
    require(type(value["gross_scale"]) is int and 0 <= value["gross_scale"] <= 36)
    require(type(value["net_scale"]) is int and value["net_scale"] == 18)
    require(value["fee_status"] in {"SKIPPED", "KNOWN", "UNKNOWN", "NOT_APPLICABLE"})
    require(type(value["assessments"]) is list and len(value["assessments"]) == 2)
    require(all(type(x) is list and all(type(identity) is str for identity in x)
                for x in value["assessments"]))
    for leg in value["assessments"]:
        for identity in leg: sha(identity)
    require(all(type(x) is str for name in ("assumptions", "evidence") for x in value[name]))
    scales = {int(plans[(leg["instrument"], leg["orientation"])]["price_scale"])
              + int(plans[(leg["instrument"], leg["orientation"])]["quantity_scale"])
              for leg in descriptor["legs"]}
    require(len(scales) == 1 and value["gross_scale"] in scales, "gross scale/plan mismatch")
    if value["fee_status"] == "KNOWN":
        require(net is not None and all(value["assessments"]), "known fee assessment")
    elif value["fee_status"] == "UNKNOWN":
        require(net is None, "unknown fee has net value")
    elif value["fee_status"] in {"SKIPPED", "NOT_APPLICABLE"}:
        require(net is None and not any(value["assessments"]), "unassessed fee values")
    return gross, net


def _lines(root, name, identity):
    path = root / name
    require(path.is_file() and not path.is_symlink(), "regular output required")
    checksum = hashlib.sha256(); size = count = 0
    with path.open("rb") as stream:
        while payload := stream.readline(MAX_LINE + 1):
            require(len(payload) <= MAX_LINE and payload.endswith(b"\n"), "line/truncation")
            size += len(payload); count += 1
            require(size <= MAX_BYTES and count <= MAX_ROWS, "output budget")
            checksum.update(payload)
            yield decode(payload, MAX_LINE)
    require({"sha256": checksum.hexdigest(), "byte_length": size, "records": count} == identity,
            "file identity")


def _bucket(value):
    if value is None:
        return "unknown"
    require(type(value) is int and value >= 0, "skew bucket")
    return str(value)


def _quantiles(values, retained_bytes=0):
    if not values:
        return {k: None for k in ("p50", "p90", "p99", "max")}
    # Keep the compact representation compact: sorted(array) creates a list of
    # millions of boxed Python integers.  Quickselect mutates one compact copy.
    source_bytes = (values.buffer_info()[1] * values.itemsize
                    if isinstance(values, array) else len(values) * 8)
    working_bytes = source_bytes + len(values) * 8
    if hasattr(retained_bytes, "reserve"):
        retained_bytes.reserve(working_bytes, "reader state budget (quantile working copy)")
    else:
        require(retained_bytes + working_bytes <= MAX_STATE,
                "reader state budget (quantile working copy)")
    work = array("Q", values)
    def select(k):
        lo, hi = 0, len(work) - 1
        while lo < hi:
            pivot = work[(lo + hi) // 2]; i, j = lo, hi
            while i <= j:
                while work[i] < pivot: i += 1
                while work[j] > pivot: j -= 1
                if i <= j:
                    work[i], work[j] = work[j], work[i]; i += 1; j -= 1
            if k <= j: hi = j
            elif k >= i: lo = i
            else: return work[k]
        return work[lo]
    n = len(work)
    result = {name: str(select((n*p + 99)//100-1))
              for name, p in (("p50", 50), ("p90", 90), ("p99", 99), ("max", 100))}
    if hasattr(retained_bytes, "release"):
        retained_bytes.release(working_bytes)
    return result


def validate_content(directory, snapshot, manifest):
    """Validate semantic files and return the independently derived summary."""
    _manifest(manifest, "summary_sha256" in manifest)
    snapshot = plain(snapshot); root = Path(directory); policy = manifest["policy"]
    policy_config(policy)
    plans = {(p["instrument"], p["orientation"]): p for p in snapshot["plans"]}

    def retained_size(*roots):
        """Actual detached Python heap owned by the reader (shared objects once)."""
        seen = set()
        def visit(value):
            identity = id(value)
            if identity in seen:
                return 0
            seen.add(identity)
            size = sys.getsizeof(value)
            if isinstance(value, dict):
                size += sum(visit(k) + visit(v) for k, v in value.items())
            elif isinstance(value, (list, tuple, set, frozenset)):
                size += sum(visit(v) for v in value)
            return size
        return sum(visit(root) for root in roots)

    class StateBudget:
        """Conservative O(1) accounting for reader-owned detached state."""
        def __init__(self, *roots):
            self.used = retained_size(*roots)
            require(self.used <= MAX_STATE, "reader state budget")

        def reserve(self, growth, message="reader state budget"):
            require(self.used + growth <= MAX_STATE, message)
            self.used += growth

        def release(self, size):
            self.used -= size

    budget = StateBudget(snapshot, manifest, policy, plans)
    expected = {}
    descriptors = {}
    for scope in range(len(snapshot["scopes"])):
        for entity, descriptor in layouts(snapshot, policy, scope).items():
            budget.reserve(1024, "reader state budget (layout)")
            expected[scope, entity] = descriptor; descriptors[entity] = descriptor
    require(set(manifest["instantaneous_positive"]) <= set(descriptors), "instantaneous entity")

    # Six compact unsigned arrays per key.  No row dictionaries or JSON tape survive.
    measurements = {}
    classes = {}
    for key in expected:
        budget.reserve(512, "reader state budget (measurement layout)")
        measurements[key] = tuple(array("Q") for _ in range(6))
        classes[key] = bytearray()
    status_codes = {}; class_codes = {}

    last_order = None
    for row in _lines(root, "measurements.ndjson", manifest["files"]["measurements.ndjson"]):
        obj(row, "version experiment_sha256 scope entity start_ns end_ns status reasons skew_bucket value_class fee_status diagnostic")
        start, end = _common(row, expected, manifest["experiment_sha256"])
        order = row_order(row, {row["entity"]: expected[row["scope"], row["entity"]]})
        require(last_order is None or last_order <= order, "measurement close order"); last_order = order
        require(type(row["reasons"]) is list and all(type(x) is str for x in row["reasons"]))
        require(row["fee_status"] in {"SKIPPED", "KNOWN", "UNKNOWN", "NOT_APPLICABLE"})
        require(row["diagnostic"] is None or type(row["diagnostic"]) is str)
        status = row["status"]
        require(status in {"UNUSABLE", "ONE_SIDED", "DEPTH_LIMITED", "DEPTH_SUFFICIENT", "NOT_CAPTURED", "UNSUPPORTED_SHAPE", "UNSUPPORTED_SCALE", "NO_MATCH", "LOCKED", "CROSSED", "NOT_CROSSED"})
        vc = row["value_class"]
        require(vc in {None, "GROSS_NONPOSITIVE", "NET_POSITIVE", "NET_NONPOSITIVE", "FEE_UNKNOWN"})
        if status == "DEPTH_SUFFICIENT": require(vc is not None, "missing value class")
        else: require(vc is None, "class on unevaluated interval")
        if vc == "GROSS_NONPOSITIVE": require(row["fee_status"] == "SKIPPED")
        if vc in {"NET_POSITIVE", "NET_NONPOSITIVE"}: require(row["fee_status"] == "KNOWN")
        if vc == "FEE_UNKNOWN": require(row["fee_status"] == "UNKNOWN")
        key = row["scope"], row["entity"]; a = measurements[key]
        descriptor = expected[key]
        if descriptor["admission"] is not None:
            require(status == descriptor["admission"], "admission/measurement mismatch")
        if descriptor["venue"] == "limitless":
            require(status in {"NOT_CAPTURED", "UNSUPPORTED_SHAPE", "UNSUPPORTED_SCALE",
                               "UNUSABLE", "ONE_SIDED", "DEPTH_LIMITED",
                               "LOCKED", "CROSSED", "NOT_CROSSED"}
                    and vc is None and row["fee_status"] == "NOT_APPLICABLE",
                    "Limitless diagnostic measurement")
        else:
            require(status not in {"LOCKED", "CROSSED", "NOT_CROSSED"},
                    "diagnostic status on economic basket")
        require(not a[1] or start == a[1][-1], "measurement gap/overlap/order")
        budget.reserve(128)
        a[0].append(start); a[1].append(end)
        a[2].append(status_codes.setdefault(status, len(status_codes)))
        a[3].append(class_codes.setdefault(vc, len(class_codes)))
        require(row["skew_bucket"] is None or (type(row["skew_bucket"]) is int
                and 0 <= row["skew_bucket"] <= len(policy["leg_skew_buckets_ns"])),
                "skew bucket integer")
        a[4].append(2**64-1 if row["skew_bucket"] is None else row["skew_bucket"])
        a[5].append({"SKIPPED":0,"KNOWN":1,"UNKNOWN":2,"NOT_APPLICABLE":3}[row["fee_status"]])
        classes[key].append(0 if vc == "GROSS_NONPOSITIVE" or vc is None else (2 if vc == "NET_POSITIVE" else 1))
    for key, a in measurements.items():
        scope = snapshot["scopes"][key[0]]
        require(a[0] and a[0][0] == int(scope["start_ns"]) and a[1][-1] == int(scope["end_ns"]),
                "incomplete measurements")

    totals = {}
    def group(entity, skew):
        d = descriptors[entity]
        key = (d["venue"], d["basket_kind"], d["direction"], d["size_contracts"], d["placebo"], skew)
        if key not in totals:
            budget.reserve(2048)
        return totals.setdefault(key, {"durations":{}, "episode_count":{"gross":0,"net":0},
            "slice_count":{"gross":0,"net":0}, "q":{t:0 for t in policy["latency_tiers_ns"]},
            "gross_q":{t:0 for t in policy["latency_tiers_ns"]},
            "episode_values":{"gross":array("Q"), "net":array("Q")},
            "slice_values":{"gross":array("Q"), "net":array("Q")},
            "censored_episodes":{"gross":0,"net":0},
            "censored_slices":{"gross":0,"net":0}})
    for key, a in measurements.items():
        for i in range(len(a[0])):
            status = next(k for k,v in status_codes.items() if v == a[2][i])
            vc = next(k for k,v in class_codes.items() if v == a[3][i])
            label = {"GROSS_NONPOSITIVE":"gross_nonpositive_ns", "NET_POSITIVE":"net_positive_ns",
                     "NET_NONPOSITIVE":"net_nonpositive_ns", "FEE_UNKNOWN":"fee_unknown_ns"}.get(vc, status.lower()+"_ns")
            g = group(key[1], _bucket(None if a[4][i] == 2**64-1 else a[4][i]))
            g["durations"][label] = g["durations"].get(label, 0) + a[1][i]-a[0][i]

    # Compact parallel episode facts. The ID index is required for detached
    # parent lookup; no decoded row or per-episode slice/predicate list survives.
    episode_index = {}; episode_rows = 0
    ep_scope=array("Q"); ep_entity=[]; ep_kind=bytearray(); ep_start=array("Q"); ep_end=array("Q")
    ep_opening=array("Q"); ep_last=array("Q"); ep_consumed=[]; ep_q=[]; ep_expected_q=[]
    ep_reached=array("Q"); ep_tiers=[]; ep_reason=[]
    ep_open_values=[]; ep_max_gross=array("Q"); ep_slice_max_gross=array("Q")
    predicate_seen = {key:[0, 0] for key in expected}
    def measurement_at(key, when):
        a = measurements[key]
        i = bisect_right(a[0], when) - 1
        if i >= 0 and when < a[1][i]:
            return a, i
        require(False, "opening outside measurement coverage")

    def read_episodes(name, placebo):
        nonlocal episode_rows
        last = None
        for row in _lines(root, name, manifest["files"][name]):
            episode_rows += 1
            require(episode_rows <= MAX_ROWS, "combined episode row budget")
            obj(row, "version experiment_sha256 scope entity start_ns end_ns episode_id kind basket end_reason censored gap_lifetime_ns opening_slice_survival_ns viable_tiers qualified_ns open_values max_gap_gross max_gap_net opening_skew_bucket")
            start,end = _common(row, expected, manifest["experiment_sha256"]); d=expected[row["scope"],row["entity"]]
            require(d["placebo"] is placebo and row["basket"] == d, "episode basket")
            require(row["kind"] in {"gross","net"} and row["episode_id"] == digest([row["scope"],row["entity"],row["kind"],str(start)]))
            sha(row["episode_id"]); require(uint(row["gap_lifetime_ns"]) == end-start)
            require(type(row["censored"]) is bool and row["end_reason"] in {"PREDICATE_FALSE","UNUSABLE","ONE_SIDED","DEPTH_LIMITED","SCOPE_END","RUN_END"})
            require(row["censored"] == (row["end_reason"] == "RUN_END"), "censor reason")
            require(uint(row["opening_slice_survival_ns"]) <= end-start)
            require(d["venue"] != "limitless", "Limitless economic episode")
            gross,net=_open_values(row["open_values"], d, plans); require(gross > 0, "nonpositive episode open")
            a, mi = measurement_at((row["scope"], row["entity"]), start)
            require(start == a[0][mi], "episode not aligned to measurement start")
            opening_class = next(k for k,v in class_codes.items() if v == a[3][mi])
            require(opening_class != "GROSS_NONPOSITIVE" and opening_class is not None,
                    "gross episode/class mismatch")
            require(_bucket(row["opening_skew_bucket"]) == _bucket(None if a[4][mi] == 2**64-1 else a[4][mi]),
                    "episode opening skew")
            require((opening_class == "FEE_UNKNOWN") == (row["open_values"]["fee_status"] == "UNKNOWN")
                    and (opening_class in {"NET_POSITIVE", "NET_NONPOSITIVE"}) ==
                    (row["open_values"]["fee_status"] == "KNOWN"),
                    "episode open fee class mismatch")
            if row["kind"] == "net":
                threshold = int(d["size_contracts"]) * int(policy["minimum_net_gap_per_contract_e18"])
                require(opening_class == "NET_POSITIVE" and net is not None and net > 0 and net >= threshold,
                        "noneligible net open")
            mg=_signed(row["max_gap_gross"]); mn=None if row["max_gap_net"] is None else _signed(row["max_gap_net"])
            require(mg >= gross and (net is None or (mn is not None and mn >= net)), "maximum consistency")
            require(type(row["viable_tiers"]) is list and row["viable_tiers"] == sorted(set(row["viable_tiers"]), key=int))
            require(type(row["qualified_ns"]) is dict and set(row["qualified_ns"]) == set(policy["latency_tiers_ns"]))
            for v in row["qualified_ns"].values(): uint(v)
            order=row_order(row,{row["entity"]:d}); require(last is None or last<=order,"episode close order"); last=order
            require(row["episode_id"] not in episode_index,"duplicate episode")
            # The interval must be exactly one maximal predicate run. This
            # removes the former retained predicate history.
            want = (lambda c: c > 0) if row["kind"] == "gross" else (lambda c: c == 2)
            require(want(classes[row["scope"], row["entity"]][mi]), "episode predicate")
            require(mi == 0 or not want(classes[row["scope"], row["entity"]][mi-1]), "episode not maximal at start")
            stop_i = mi
            while stop_i + 1 < len(a[0]) and want(classes[row["scope"], row["entity"]][stop_i+1]):
                stop_i += 1
            require(end == a[1][stop_i], "episode not maximal at end")
            expected_reason = "RUN_END" if end == int(snapshot["scopes"][-1]["end_ns"]) else (
                "SCOPE_END" if end == int(snapshot["scopes"][row["scope"]]["end_ns"]) else None)
            if expected_reason is None:
                next_status = next(k for k,v in status_codes.items() if v == a[2][stop_i+1])
                expected_reason = ("PREDICATE_FALSE" if next_status == "DEPTH_SUFFICIENT" else next_status)
            require(row["end_reason"] == expected_reason, "episode end reason/scope")
            budget.reserve(768)
            index=len(ep_start); episode_index[row["episode_id"]]=index
            ep_scope.append(row["scope"]); ep_entity.append(row["entity"]); ep_kind.append(0 if row["kind"]=="gross" else 1)
            ep_start.append(start); ep_end.append(end); ep_opening.append(uint(row["opening_slice_survival_ns"])); ep_last.append(start)
            ep_consumed.append(None); ep_q.append(array("Q", [0]*len(policy["latency_tiers_ns"])))
            ep_expected_q.append(array("Q", [uint(row["qualified_ns"][t]) for t in policy["latency_tiers_ns"]]))
            ep_reached.append(0); ep_tiers.append(tuple(row["viable_tiers"])); ep_reason.append(row["end_reason"])
            ep_open_values.append(row["open_values"])
            ep_max_gross.append(mg); ep_slice_max_gross.append(0)
            predicate_seen[row["scope"],row["entity"]][ep_kind[-1]] += 1
            g=group(row["entity"],_bucket(row["opening_skew_bucket"])); g["episode_count"][row["kind"]]+=1; g["episode_values"][row["kind"]].append(end-start)
            if row["censored"]: g["censored_episodes"][row["kind"]]+=1
    read_episodes("episodes.ndjson",False); read_episodes("placebo_episodes.ndjson",True)

    # Classes determine the required number of maximal runs.
    for key,a in measurements.items():
        for kind,want in (("gross",lambda c:c>0),("net",lambda c:c==2)):
            runs=0; opened=False
            for i,c in enumerate(classes[key]):
                if want(c) and not opened: runs += 1; opened=True
                elif not want(c): opened=False
            require(runs == predicate_seen[key][0 if kind=="gross" else 1], "episode predicate/maximal coverage")

    last=None
    for row in _lines(root,"slices.ndjson",manifest["files"]["slices.ndjson"]):
        obj(row,"version experiment_sha256 scope entity start_ns end_ns episode_id kind end_reason censored consumed survival_ns viable_tiers open_values opening_skew_bucket")
        start,end=_common(row,expected,manifest["experiment_sha256"]); require(row["episode_id"] in episode_index,"orphan slice")
        ei=episode_index[row["episode_id"]]
        require(row["scope"]==ep_scope[ei] and row["entity"]==ep_entity[ei], "slice parent scope/entity")
        require(row["kind"] in {"gross", "net"}, "slice kind")
        require((row["kind"]=="net") == bool(ep_kind[ei]) and ep_start[ei]<=start<end<=ep_end[ei])
        require(uint(row["survival_ns"])==end-start and type(row["censored"]) is bool)
        require(row["end_reason"] in {"CONSUMED_CHANGED","PREDICATE_FALSE","UNUSABLE","ONE_SIDED","DEPTH_LIMITED","SCOPE_END","RUN_END"})
        require(row["censored"] == (row["end_reason"] == "RUN_END"), "slice censor reason")
        require(row["end_reason"] == (ep_reason[ei] if end == ep_end[ei] else "CONSUMED_CHANGED"),
                "slice end reason/parent")
        require(type(row["consumed"]) is list and len(row["consumed"])==2)
        for leg in row["consumed"]:
            require(type(leg) is list)
            for level in leg:
                require(type(level) is list and len(level)==2, "consumed level")
                uint(level[0], 2**64 - 1); require(uint(level[1], 2**64 - 1)>0, "consumed quantity")
        d = expected[row["scope"], row["entity"]]
        require(d["venue"] != "limitless", "Limitless economic slice")
        gross,net=_open_values(row["open_values"], d, plans); require(gross>0 and (row["kind"]!="net" or net is not None and net>0))
        a, mi = measurement_at((row["scope"], row["entity"]), start)
        opening_class = next(k for k,v in class_codes.items() if v == a[3][mi])
        require(opening_class != "GROSS_NONPOSITIVE" and opening_class is not None,
                "slice/class mismatch")
        if row["kind"] == "net":
            d = expected[row["scope"], row["entity"]]
            require(opening_class == "NET_POSITIVE" and net >= int(d["size_contracts"])*int(policy["minimum_net_gap_per_contract_e18"]),
                    "slice net eligibility")
        threshold = int(d["size_contracts"]) * int(policy["minimum_net_gap_per_contract_e18"])
        fact_class = ("FEE_UNKNOWN" if row["open_values"]["fee_status"] == "UNKNOWN" else
                      "NET_POSITIVE" if net is not None and net > 0 and net >= threshold else
                      "NET_NONPOSITIVE")
        require(opening_class == fact_class, "slice/measurement eligibility mismatch")
        want_class = ((lambda value: value == "NET_POSITIVE") if row["kind"] == "net"
                      else (lambda value: value in {"NET_POSITIVE", "NET_NONPOSITIVE", "FEE_UNKNOWN"}))
        overlap_i = mi
        while overlap_i < len(a[0]) and a[0][overlap_i] < end:
            value_class = next(k for k, v in class_codes.items() if v == a[3][overlap_i])
            require(want_class(value_class), "slice/measurement eligibility mismatch")
            overlap_i += 1
        require((opening_class == "FEE_UNKNOWN") == (row["open_values"]["fee_status"] == "UNKNOWN")
                and (opening_class in {"NET_POSITIVE", "NET_NONPOSITIVE"}) == (row["open_values"]["fee_status"] == "KNOWN"),
                "open fee class mismatch")
        require(_bucket(row["opening_skew_bucket"]) == _bucket(None if a[4][mi] == 2**64-1 else a[4][mi]),
                "slice opening skew")
        tiers=[t for t in policy["latency_tiers_ns"] if end-start>=int(t)]
        require(row["viable_tiers"]==tiers,"slice tiers")
        order=row_order(row,{row["entity"]:expected[row["scope"],row["entity"]]}); require(last is None or last<=order,"slice close order"); last=order
        # Re-walk full displayed consumed levels to the configured ticket. This
        # checks ordering, depth, scales, and the exact gross arithmetic.
        costs=[]
        quantity=int(d["size_contracts"])
        for leg_index, leg in enumerate(row["consumed"]):
            plan=plans[(d["legs"][leg_index]["instrument"],d["legs"][leg_index]["orientation"])]
            need=quantity*(10**int(plan["quantity_scale"])); remaining=need; cost=0; previous=None
            descending = d["direction"] == "short"
            for price_text, amount_text in leg:
                require(remaining > 0, "extra unconsumed level")
                price=uint(price_text,2**64-1); amount=uint(amount_text,2**64-1)
                require(price <= 10**int(plan["price_scale"]), "consumed price scale")
                require(previous is None or (price < previous if descending else price > previous), "consumed price order")
                previous=price; taken=min(remaining,amount); cost += price*taken; remaining -= taken
            require(remaining==0,"consumed depth insufficient")
            costs.append(cost)
        unit=quantity*(10**(int(plans[(d["legs"][0]["instrument"],d["legs"][0]["orientation"])]["price_scale"])+int(plans[(d["legs"][0]["instrument"],d["legs"][0]["orientation"])]["quantity_scale"])))
        calculated = (unit-sum(costs) if d["direction"] in {"long", "both_bids"}
                      else sum(costs)-unit)
        require(gross==calculated,"consumed/gross arithmetic")
        consumed_identity = digest(row["consumed"])
        require(start==ep_last[ei],"slice partition/order")
        if start == ep_start[ei]:
            require(row["open_values"] == ep_open_values[ei], "episode/first slice opening values")
        ep_slice_max_gross[ei] = max(ep_slice_max_gross[ei], gross)
        if ep_consumed[ei] is not None:
            require(ep_consumed[ei] != consumed_identity,
                    "adjacent slices must change consumed depth")
        budget.reserve(64)
        ep_last[ei]=end; ep_consumed[ei]=consumed_identity
        if start==ep_start[ei]: require(ep_opening[ei]==end-start,"opening survival")
        for ti,tier in enumerate(policy["latency_tiers_ns"]):
            if end-start >= int(tier): ep_reached[ei] |= 1 << ti
            ep_q[ei][ti]+=max(0,end-start-int(tier))
        g=group(row["entity"],_bucket(row["opening_skew_bucket"])); g["slice_count"][row["kind"]]+=1; g["slice_values"][row["kind"]].append(end-start)
        if row["censored"]: g["censored_slices"][row["kind"]]+=1
        # Attribute entry time to the skew at each entry instant.
        a=measurements[row["scope"],row["entity"]]
        for tier in policy["latency_tiers_ns"]:
            qend=end-int(tier)
            i = max(0, bisect_right(a[1], start))
            while i < len(a[0]) and a[0][i] < qend:
                overlap=max(0,min(qend,a[1][i])-max(start,a[0][i]))
                if overlap:
                    target=group(row["entity"],_bucket(None if a[4][i]==2**64-1 else a[4][i]))
                    target["q" if row["kind"]=="net" else "gross_q"][tier]+=overlap
                i += 1
    for ei in range(len(ep_start)):
        require(ep_consumed[ei] is not None and ep_last[ei]==ep_end[ei],"slice partition/order")
        require(ep_slice_max_gross[ei] == ep_max_gross[ei], "episode gross maximum/slices")
        reached=[t for ti,t in enumerate(policy["latency_tiers_ns"]) if ep_reached[ei] & (1 << ti)]
        require(reached==list(ep_tiers[ei]),"episode tiers")
        require(ep_q[ei].tolist()==ep_expected_q[ei].tolist(),"episode Q")

    # The summary is independently capped at MAX_METADATA; reserve that full
    # amount before constructing its rows rather than checking after growth.
    budget.reserve(MAX_METADATA)
    rows=[]
    for key,g in sorted(totals.items(),key=lambda x:(x[0][0],x[0][1],x[0][2],int(x[0][3]),x[0][4],x[0][5])):
        venue,bkind,direction,size,placebo,skew=key
        gross_nonpositive=g["durations"].get("gross_nonpositive_ns",0)
        net_positive=g["durations"].get("net_positive_ns",0)
        net_nonpositive=g["durations"].get("net_nonpositive_ns",0)
        fee_unknown=g["durations"].get("fee_unknown_ns",0)
        rows.append({"venue":venue,"basket_kind":bkind,"direction":direction,"size_contracts":size,"placebo":placebo,"skew_bucket":skew,
          "policy_sha256":manifest["policy_sha256"],"durations_ns":{k:str(v) for k,v in sorted(g["durations"].items())},
          "evaluated_ns":str(gross_nonpositive+net_positive+net_nonpositive+fee_unknown),
          "gross_positive_ns":str(net_positive+net_nonpositive+fee_unknown),
          "net_assessed_ns":str(net_positive+net_nonpositive),
          "episode_count":g["episode_count"],"slice_count":g["slice_count"],
          "q_ns":{k:str(v) for k,v in g["q"].items()},"gross_q_ns":{k:str(v) for k,v in g["gross_q"].items()},
          "episode_lifetime_quantiles_ns":{kind:_quantiles(g["episode_values"][kind], budget) for kind in ("gross","net")},
          "slice_survival_quantiles_ns":{kind:_quantiles(g["slice_values"][kind], budget) for kind in ("gross","net")},
          "censored_episodes":g["censored_episodes"],"censored_slices":g["censored_slices"]})
    verdicts=[]
    headline=policy["headline_size_contracts"]; latency=policy["headline_latency_ns"]
    for venue in sorted({d["venue"] for d in descriptors.values()}-{"limitless"}):
        relevant=[r for r in rows if r["venue"]==venue and not r["placebo"] and r["size_contracts"]==headline]
        E=sum(sum(int(v) for k,v in r["durations_ns"].items() if k in {"gross_nonpositive_ns","net_positive_ns","net_nonpositive_ns","fee_unknown_ns"}) for r in relevant)
        Q=sum(int(r["q_ns"][latency]) for r in relevant); X=sum(int(r["durations_ns"].get("fee_unknown_ns","0")) for r in relevant)
        if E < int(policy["verdict"]["minimum_evaluated_ns"]): verdict,reason="INCONCLUSIVE_FIXTURE","INSUFFICIENT_EVALUATED_TIME"
        elif Q*1_000_000 > int(policy["verdict"]["maximum_positive_time_fraction_ppm"])*E: verdict,reason="INTRA_INSTRUMENT_GAPS_PRESENT_INVESTIGATE",None
        elif X: verdict,reason="INCONCLUSIVE_FIXTURE","UNRESOLVED_POSITIVE_GROSS"
        else: verdict,reason="INTRA_INSTRUMENT_GAPS_ABSENT_IN_FIXTURE",None
        verdicts.append({"venue":venue,"verdict":verdict,"reason":reason,"evaluated_ns":str(E),"qualified_ns":str(Q),"fee_unknown_ns":str(X),"basis":"PINNED_FEE_MODEL_AND_DISPLAYED_DEPTH_POLICY"})
    exclusions = sorted({(d["venue"], d["market_id"], d["admission"])
                         for d in descriptors.values() if d["admission"] is not None})
    summary={"version":1,"snapshot_sha256":manifest["snapshot_sha256"],"policy_sha256":manifest["policy_sha256"],"experiment_sha256":manifest["experiment_sha256"],
             "instantaneous_positive":manifest["instantaneous_positive"],"rows":rows,"verdicts":verdicts,
             "member_exclusions":[{"venue":v,"market_id":m,"status":s} for v,m,s in exclusions],
             "history_complete":snapshot["history_complete"],"vendor_completeness":"NOT_PROVEN",
             "qualifiers":["DETECTED_NOT_EXECUTED","RETROSPECTIVE_DISPLAYED_DEPTH_SURVIVAL",
                           "PINNED_FEE_ESTIMATE","NOT_RESOLUTION_PROOF"]}
    require(len(encoded(summary))<=MAX_METADATA,"summary budget")
    require(budget.used + len(encoded(summary)) <= MAX_STATE, "reader state budget")
    return summary


def read_provisional(directory, snapshot_directory, *, expected_sha256):
    root=Path(directory); snapshot=load_snapshot(snapshot_directory,expected_sha256=expected_sha256)
    manifest=_json(root/"manifest.json"); _manifest(manifest,True)
    require(manifest["snapshot_sha256"]==expected_sha256,"snapshot binding")
    receipt=obj(_json(root/"content_receipt.json"),"version semantic_sha256 run_id attempt_id group identity terminal")
    require(type(receipt["version"]) is int and receipt["version"]==1)
    for field in ("semantic_sha256","identity"): sha(receipt[field])
    require(receipt["semantic_sha256"]==digest(manifest),"semantic identity")
    for field in ("run_id","attempt_id","group"): require(type(receipt[field]) is str and 0<len(receipt[field])<=128)
    require(type(receipt["terminal"]) is int and receipt["terminal"]>=2)
    summary=validate_content(root,snapshot,manifest)
    require(encoded(_json(root/"summary.json"))==encoded(summary) and manifest["summary_sha256"]==digest(summary),"summary identity/schema")
    return {"receipt":receipt,"manifest":manifest,"summary":summary}


def read_completed(run_directory, group):
    root=Path(run_directory)
    require((root/"run.json").is_file() and (root/"SUCCESS.json").is_file(),
            "completed complement requires supervisor SUCCESS")
    success=read_success(root); config=read_run(root/"run.json")
    require(group in success["outputs"],"unknown strategy group"); spec=config["strategies"][group]
    require(spec["factory"]=="replay.same_venue_complement:build","complement factory binding")
    inputs=Inputs(spec["config"]); inputs.prepared.bind(freeze(initial(config)))
    result=read_provisional(root/success["outputs"][group],spec["config"]["snapshot_directory"],expected_sha256=spec["config"]["snapshot_sha256"])
    require(result["manifest"]["experiment_sha256"]==inputs.experiment_sha256 and result["manifest"]["fee_engine_identity"]==inputs.bridge.engine_identity,"configured identity")
    receipt=result["receipt"]
    require({k:receipt[k] for k in ("identity","attempt_id","group","run_id","terminal")}=={"identity":success["identity"],"attempt_id":success["attempt"],"group":group,"run_id":config["transport"]["run_id"],"terminal":success["terminal"]},"supervisor/content binding")
    return result
