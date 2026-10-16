"""Independent arithmetic and timing validation for execution-delay probes.

This reader never imports the probe scheduler or reconstructs arrival books.
The carried walks are writer-attested observations; their shape, limits,
quantities, values, delays and gross unwind arithmetic are independently checked.
"""

from replay.economic_fills import Fill
from replay.economic_sdk import bounds
from replay.economic_sdk.fills import check_spec, probe_policy
from replay.economic_sdk.reader import lines, signed
from replay.economic_sdk.types import SELL_SOURCES
from replay.preparation import digest
from replay.streams.protocol import obj, require, uint

NS_PER_MS = 1_000_000
MAX_MS = (2**64 - 1) // NS_PER_MS
STATUSES = ("not_confirmed", "decision_untradeable", "filled", "partial", "empty",
            "unusable", "censored")
_METRICS = ("matched_steps", "value", "leg_loss_gross")
_FILL_FIELDS = ("filled_atoms", "cost", "consumed")
_EXIT_FIELDS = ("excess_atoms", "exit_consumed", "exit_proceeds")


def _natural(value):
    require(type(value) is str and 0 < len(value) <= 80 and value.isascii() and value.isdigit()
            and (value == "0" or value[0] != "0"), "probe canonical natural number")
    return int(value)


def _signed(value):
    require(type(value) is str and len(value.removeprefix("-")) <= 80, "probe signed integer bound")
    return signed(value)


def check_ages(value, count):
    require(type(value) is list and len(value) == count, "probe ages leg count")
    for entry in value:
        entry = obj(entry, "book_ns best_ns")
        for age in entry.values():
            if age is not None:
                uint(age)


def _walk(value, maximum, max_levels, sell, limit=None):
    """An actually taken walk, best-first, bounded by the retained-level cap."""
    require(type(value) is list and len(value) <= max_levels, "probe walk level bound")
    levels, previous = [], None
    for entry in value:
        require(type(entry) is list and len(entry) == 2, "probe level")
        price, quantity = _natural(entry[0]), _natural(entry[1])
        require(quantity > 0 and price <= maximum, "probe level price/quantity")
        require(previous is None or (price < previous if sell else price > previous),
                "probe walk best-first")
        require(limit is None or (price >= limit if sell else price <= limit), "probe limit")
        levels.append((price, quantity))
        previous = price
    return tuple(levels)


def _prefix(levels, quantity):
    """The best atoms: cheapest buys or highest-priced sales, in carried order."""
    taken = []
    for price, amount in levels:
        if not quantity:
            break
        amount = min(amount, quantity)
        taken.append((price, amount))
        quantity -= amount
    require(quantity == 0, "probe matched quantity")
    return tuple(taken)


def _decision(row, ep, policy, count, maxima):
    confirm = row["confirm_ms"]
    when = ep["start"] + confirm * NS_PER_MS
    require(when <= 2**64 - 1, "probe decision time overflow")
    if row["status"] == "not_confirmed":
        require(confirm > 0 and ep["end"] <= when and row["decision"] is None,
                "probe not_confirmed")
        return when, None, None
    require(confirm == 0 or when < ep["end"], "probe decision outside live fill")
    decision = obj(row["decision"], "time_ns steps limits ages")
    require(uint(decision["time_ns"]) == when, "probe decision time")
    steps = _natural(decision["steps"])
    require(type(decision["limits"]) is list and len(decision["limits"]) == count,
            "probe decision limits")
    limits = tuple(None if value is None else _natural(value) for value in decision["limits"])
    require(all(limit is None or limit <= maximum for limit, maximum in zip(limits, maxima)),
            "probe decision limit price")
    require(all(limit is not None for limit in limits) if steps else all(limit is None for limit in limits),
            "probe decision steps/limits")
    check_ages(decision["ages"], count)
    sizing = next(s for s in policy.sizings if s.name == row["sizing"])
    require(sizing.mode != "target" or steps <= sizing.target_steps, "probe decision target")
    if confirm == 0:
        opening = ep["probe_opening"][row["sizing"]]
        require(decision["steps"] == opening["steps"] and decision["limits"] == opening["limits"]
                and decision["ages"] == opening["ages"], "probe opening decision equality")
        require(row["status"] != "decision_untradeable", "probe c0 opening decision")
    elif row["status"] != "decision_untradeable":
        require(steps > 0 and (sizing.mode != "target" or steps == sizing.target_steps),
                "probe tradeable decision steps")
    return when, steps, limits


def _delays(strategy, entity, when, probes, plans):
    observed = strategy.fill_delays(entity, when)
    require(type(observed) is tuple and len(observed) == len(entity.legs), "probe delay hook shape")
    result = []
    for key, delay in zip(entity.legs, observed):
        venue = plans[key]["venue"]
        require(venue in probes.unknown_taker_delay_ms, "probe delay default venue")
        if delay is None:
            result.append((probes.unknown_taker_delay_ms[venue], "default"))
        else:
            require(type(delay) is int and 0 <= delay <= 2**64 - 1 and delay % NS_PER_MS == 0,
                    "probe delay hook value")
            result.append((delay // NS_PER_MS, "terms"))
    return tuple(result)


def _leg(row, delay, sent, arrival, quantity, maximum, max_levels, sell, limit, censored):
    row = obj(row, "sent_ns arrival_ns venue_delay_ms delay_source usable filled_atoms cost consumed "
                   "excess_atoms exit_consumed exit_proceeds")
    require(type(row["venue_delay_ms"]) is int and 0 <= row["venue_delay_ms"] <= MAX_MS
            and (row["venue_delay_ms"], row["delay_source"]) == delay, "probe venue delay")
    require(row["sent_ns"] == (None if sent is None else str(sent))
            and row["arrival_ns"] == (None if arrival is None else str(arrival)), "probe send/arrival time")
    if sent is not None:
        uint(row["sent_ns"])
        uint(row["arrival_ns"])
    require(row["usable"] is None or type(row["usable"]) is bool, "probe usable evidence")
    if censored:
        require(row["usable"] is None and all(row[name] is None for name in _FILL_FIELDS + _EXIT_FIELDS),
                "probe unresolved leg")
        return None, None, None
    if sent is None:
        require(row["usable"] is None and row["filled_atoms"] == row["cost"] == "0"
                and row["consumed"] == [], "probe unsent leg")
    else:
        require(type(row["usable"]) is bool, "probe resolved usable evidence")
    atoms, cost = _natural(row["filled_atoms"]), _natural(row["cost"])
    taken = _walk(row["consumed"], maximum, max_levels, sell, limit)
    require(atoms <= quantity and atoms == sum(q for _, q in taken), "probe filled quantity")
    require(cost == sum(p * q for p, q in taken), "probe fill cost")
    require(row["usable"] is not False or (atoms == cost == 0 and not taken), "probe unusable fill")
    return atoms, cost, taken


def _check(row, ep, scopes, policy, strategy, plans):
    entity = ep["entity"]
    spec = strategy.fill_spec(entity)
    count = len(entity.legs)
    check_spec(spec, entity)
    require(count > 0, "probe fill spec")
    maxima = tuple(10 ** int(plans[key]["price_scale"]) for key in entity.legs)
    when, steps, limits = _decision(row, ep, policy, count, maxima)
    end = int(scopes[ep["scope"]]["end_ns"])
    if row["status"] in ("not_confirmed", "decision_untradeable"):
        require(row["legs"] == [] and all(row[name] is None for name in _METRICS),
                "probe skipped metrics")
        return min(when, end)
    require(type(row["legs"]) is list and len(row["legs"]) == count, "probe leg count")
    for leg in row["legs"]:
        obj(leg, "sent_ns arrival_ns venue_delay_ms delay_source usable filled_atoms cost consumed "
                 "excess_atoms exit_consumed exit_proceeds")
    delays = _delays(strategy, entity, when, policy.probes, plans)
    total = tuple((row["arrival_ms"] + delay[0]) * NS_PER_MS for delay in delays)
    require(all(delay <= 2**64 - 1 for delay in total), "probe total delay overflow")
    slow = max(range(count), key=lambda index: (total[index], -index))
    simultaneous = row["sequencing"] == "simultaneous"
    first_arrival = when + total[slow]
    require(steps == 0 or first_arrival <= 2**64 - 1, "probe arrival time overflow")
    first = row["legs"][slow]
    # In sequential mode the first leg's observed quantity sizes all later
    # orders. Before its arrival, those orders have not yet been sent.
    first_censored = first_arrival >= end
    first_atoms = None
    if not simultaneous and not first_censored:
        first_atoms = _natural(first["filled_atoms"])
    fills, arrivals, unresolved = [], [], False
    for index, leg in enumerate(row["legs"]):
        quantity = steps * spec.units[index]
        if steps == 0:
            sent = None
        elif simultaneous or index == slow:
            sent = when
        elif first_censored:
            sent = None
        elif first_atoms == 0:
            sent = None
            quantity = 0
        else:
            sent = first_arrival
            quantity = (first_atoms // spec.units[slow]) * spec.units[index]
        arrival = None if sent is None else sent + total[index]
        require(arrival is None or arrival <= 2**64 - 1, "probe arrival time overflow")
        pending = (first_censored and steps > 0) if sent is None else arrival >= end
        unresolved = unresolved or pending
        if arrival is not None:
            arrivals.append(arrival)
        fill = _leg(leg, delays[index], sent, arrival, quantity, maxima[index], policy.max_levels,
                    spec.sources[index] in SELL_SOURCES, limits[index], pending)
        fills.append(fill)
    if unresolved:
        require(row["status"] == "censored" and all(row[name] is None for name in _METRICS),
                "probe censored status/metrics")
        require(all(all(leg[name] is None for name in _EXIT_FIELDS) for leg in row["legs"]),
                "probe censored unwind")
        return end
    require(row["status"] != "censored", "probe spurious censoring")
    matched = min(atoms // unit for (atoms, _, _), unit in zip(fills, spec.units))
    require(_natural(row["matched_steps"]) == matched, "probe matched steps")
    matched_legs, native_losses = [], []
    for index, (leg, (atoms, cost, taken), unit) in enumerate(zip(row["legs"], fills, spec.units)):
        kept = _prefix(taken, matched * unit)
        kept_cost = sum(price * quantity for price, quantity in kept)
        matched_legs.append(Fill(matched * unit, kept_cost, False, kept, kept))
        excess = atoms - matched * unit
        require(_natural(leg["excess_atoms"]) == excess, "probe excess atoms")
        exit_levels = _walk(leg["exit_consumed"], maxima[index], policy.max_levels,
                            spec.sources[index] not in SELL_SOURCES)
        require(sum(quantity for _, quantity in exit_levels) <= excess, "probe exit quantity")
        proceeds = _natural(leg["exit_proceeds"])
        require(proceeds == sum(price * quantity for price, quantity in exit_levels),
                "probe exit proceeds")
        require(excess > 0 or (not exit_levels and proceeds == 0), "probe zero-excess unwind")
        excess_cost = cost - kept_cost
        native_losses.append(proceeds - excess_cost if spec.sources[index] in SELL_SOURCES
                             else excess_cost - proceeds)
    recorded = None if row["value"] is None else _signed(row["value"])
    value = 0 if matched == 0 else strategy.fill_value(entity, matched, tuple(matched_legs), when)
    require(value is None or type(value) is int, "probe value hook type")
    require(recorded == value, "probe matched value")
    loss = strategy.fill_loss(entity, tuple(native_losses), when)
    require(type(loss) is int, "probe loss value")
    require(_signed(row["leg_loss_gross"]) == loss, "probe gross loss")
    status = ("unusable" if any(leg["usable"] is False for leg in row["legs"])
              else "empty" if matched == 0 else "partial" if matched < steps else "filled")
    require(row["status"] == status, "probe status")
    return max(arrivals) if arrivals else when


def validate(root, identity, episodes, scopes, policy, strategy, plans, budget):
    """Stream and validate exactly one row per configured probe and fill episode."""
    probes = policy.probes
    checked = probe_policy({"version": 1, "sizings": list(probes.sizings),
                            "confirm_ms": list(probes.confirm_ms), "arrival_ms": list(probes.arrival_ms),
                            "sequencing": list(probes.sequencing),
                            "unknown_taker_delay_ms": probes.unknown_taker_delay_ms}, policy.sizings)
    require(checked == probes, "probe policy")
    budget.reserve(512 + 512 * len(probes.sizings) * len(probes.confirm_ms)
                   * len(probes.arrival_ms) * len(probes.sequencing), "reader state budget (probe policy)")
    configured = {(sizing, confirm, arrival, mode): index for index, (sizing, confirm, arrival, mode)
                  in enumerate((s, c, a, m) for s in probes.sizings for c in probes.confirm_ms
                                for a in probes.arrival_ms for m in probes.sequencing)}
    expected = {digest([ep["scope"], ep["entity"].id, ep["kind"], str(ep["start"])]): ep
                for ep in episodes if ep["fill"]}
    budget.reserve(256 * len(expected), "reader state budget (probe coverage)")
    seen, last = {}, None
    sizing_rank = {sizing.name: index for index, sizing in enumerate(policy.sizings)}
    mode_rank = {mode: index for index, mode in enumerate(probes.sequencing)}
    count = 0
    for row in lines(root, "probes.ndjson", identity):
        row = obj(row, "episode_id sizing confirm_ms arrival_ms sequencing status decision legs "
                       "matched_steps value leg_loss_gross")
        require(type(row["episode_id"]) is str and row["episode_id"] in expected, "probe orphan episode")
        require(type(row["sizing"]) is str and type(row["sequencing"]) is str
                and type(row["confirm_ms"]) is int and 0 <= row["confirm_ms"] <= MAX_MS
                and type(row["arrival_ms"]) is int and 0 <= row["arrival_ms"] <= MAX_MS,
                "probe combination types")
        combination = row["sizing"], row["confirm_ms"], row["arrival_ms"], row["sequencing"]
        require(combination in configured, "probe configured combination")
        require(type(row["status"]) is str and row["status"] in STATUSES, "probe status")
        episode_id = row["episode_id"]
        mask = 1 << configured[combination]
        require(not seen.get(episode_id, 0) & mask, "probe duplicate combination")
        seen[episode_id] = seen.get(episode_id, 0) | mask
        ep = expected[episode_id]
        resolved = _check(row, ep, scopes, policy, strategy, plans)
        order = (resolved, ep["start"], sizing_rank[row["sizing"]], row["confirm_ms"],
                 row["arrival_ms"], mode_rank[row["sequencing"]], ep["entity"].id)
        require(last is None or last <= order, "probe resolution order")
        last = order
        count += 1
    full_mask = (1 << len(configured)) - 1
    require(all(seen.get(episode_id, 0) == full_mask for episode_id in expected)
            and count == len(expected) * len(configured), "probe coverage")
