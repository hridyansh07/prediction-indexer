"""Market profile: time-weighted per-book trading data points (SDK spec §7).

The collector runs either as the standalone ``replay.market_profile`` group or
inside any SDK strategy that requests it through ``Requirements.profile``. It
records no economic judgement. Every duration is exact visible nanoseconds;
every price statistic is an exact integer in plan price atoms (scale carried
on the row), integrated over time where noted (``*_ns`` products), so means are
ratios a consumer computes without rounding here.

Only the policy's ``groups`` are computed: a strategy that does not request a
group pays nothing for it. ``state`` is always on because every other group's
denominators come from it.

Rows are emitted per (scope, book, bucket). Buckets are aligned to multiples
of ``bucket_ns`` in visible time and clipped to scope boundaries, so rows from
different bundles line up on the wall clock.
"""

from __future__ import annotations

from replay.economic_fills import walk
from replay.economic_sdk import bounds
from replay.preparation import digest, encoded
from replay.strategy_sdk import LineWriter, plain
from replay.streams.protocol import obj, require, uint

STRATEGY = "market_profile_v1"
GROUPS = ("activity", "depth", "pair_consistency", "quote_stability", "self_crossing", "top_of_book")
FILES = ("incidents.ndjson", "pair_profile.ndjson", "profile.ndjson")
DISPOSITIONS = ("applied", "duplicate", "invalidated", "not_authority", "observed")
MAX_PROFILE_ROWS = 2_000_000


def profile_policy(value):
    value = obj(plain(value), "version bucket_ns groups sizes_contracts tick_atoms depth_ticks survival_edges_ns")
    require(type(value["version"]) is int and value["version"] == 1, "profile policy version")
    require(uint(value["bucket_ns"]) > 0, "profile bucket width")
    groups = value["groups"]
    require(type(groups) is list and groups == sorted(set(groups)) and set(groups) <= set(GROUPS),
            "profile groups")
    for name, cap in (("sizes_contracts", 16), ("depth_ticks", 8), ("survival_edges_ns", 32)):
        entries = value[name]
        require(type(entries) is list and 1 <= len(entries) <= cap, "profile list budget")
        numbers = [uint(v) for v in entries]
        require(numbers == sorted(set(numbers)) and numbers[0] > 0, "profile list order/positive")
    ticks = value["tick_atoms"]
    require(type(ticks) is dict and ticks and all(uint(v) > 0 for v in ticks.values()), "tick atoms")
    return value


def profile_identity(snapshot_sha256, policy):
    return digest({"strategy": STRATEGY, "policy": policy, "snapshot_sha256": snapshot_sha256})


def _weighted_rank(histogram, percent):
    """Time-weighted nearest rank (``ceil(p * total)``) over an exact histogram."""
    total = sum(ns for _, ns in histogram)
    if not total:
        return None
    rank = (total * percent + 99) // 100
    running = 0
    for value, ns in histogram:
        running += ns
        if running >= rank:
            return str(value)
    return str(histogram[-1][0])


def pairs_of(scope):
    """Two-book members with a complement structure: PM token pairs, Kalshi YES/NO."""
    result = []
    for member in scope["members"]:
        if not member["capture_selected"]:
            continue
        venue = member["market_id"].split(":", 1)[0]
        books = sorted((b["instrument"], b["orientation"]) for b in member["books"])
        if len(books) != 2:
            continue
        if venue == "polymarket" and all(o == "outcome" for _, o in books):
            result.append((member["market_id"], tuple(books)))
        elif venue == "kalshi" and books[0][0] == books[1][0] == member["market_id"] \
                and {o for _, o in books} == {"complement", "outcome"}:
            result.append((member["market_id"], tuple(books)))
    return sorted(result)


class _State:
    """Detached committed profile state of one book."""

    __slots__ = ("kind", "bid", "ask", "depth", "fills")

    def __init__(self, kind, bid=None, ask=None, depth=None, fills=None):
        self.kind, self.bid, self.ask, self.depth, self.fills = kind, bid, ask, depth, fills

    def quotes(self):
        return {"bid": None if self.bid is None else [str(self.bid[0]), str(self.bid[1])],
                "ask": None if self.ask is None else [str(self.ask[0]), str(self.ask[1])]}


class _Accumulator:
    __slots__ = ("start", "durations", "unusable", "spread_hist", "spread_ns", "mid2_ns",
                 "top_qty_ns", "depth_ns", "filled_ns", "limited_ns", "slippage_ns",
                 "activity", "trades", "aggressor", "trade_qty", "trade_mid2", "trades_priced",
                 "trades_unpriced", "trades_scale_mismatch", "survival", "censored",
                 "open", "crossed_ns", "locked_ns")


class Collector:
    """Per-book profile over one stream; scope-, bucket- and staging-aware."""

    def __init__(self, policy, snapshot, snapshot_sha256, root, experiment_sha256, budget=None):
        self.policy = profile_policy(policy)
        self.snapshot = snapshot
        self.snapshot_sha = snapshot_sha256
        self.experiment = experiment_sha256
        self.budget = budget if budget is not None else bounds.StateBudget()
        self.groups = frozenset(self.policy["groups"])
        self.width = int(self.policy["bucket_ns"])
        self.depth_ticks = tuple(int(k) for k in self.policy["depth_ticks"])
        self.edges = tuple(int(e) for e in self.policy["survival_edges_ns"])
        self.sizes = tuple(int(s) for s in self.policy["sizes_contracts"])
        self.plans = {(p["instrument"], p["orientation"]): plain(p) for p in snapshot["plans"]}
        for plan in self.plans.values():
            require(plan["venue"] in self.policy["tick_atoms"], "profile tick for every planned venue")
        self.members = {}
        for scope in snapshot["scopes"]:
            for member in scope["members"]:
                for book in member["books"]:
                    self.members.setdefault((book["instrument"], book["orientation"]), member["market_id"])
        self.counterpart = {}
        for instrument, orientation in self.plans:
            if self.plans[instrument, orientation]["venue"] == "kalshi":
                other = (instrument, "complement" if orientation == "outcome" else "outcome")
                if other in self.plans:
                    self.counterpart[instrument, orientation] = other
        self.writers = {name: LineWriter(root / name, max_bytes=bounds.MAX_BYTES,
                                         max_records=MAX_PROFILE_ROWS, max_line_bytes=bounds.MAX_LINE)
                        for name in FILES}
        self.scopes = snapshot["scopes"]
        self.start = int(snapshot["config"]["start_ns"])
        self.end = int(snapshot["config"]["end_ns"])
        self.scope = 0
        self.raw_books = {}     # key -> (validity, reason kind, bids, asks) of the live book
        self.state = {}         # key -> committed _State
        self.since = {}         # key -> time the committed state started accruing
        self.pending = {}       # key -> _State at the staged time
        self.staged = None
        self.rows = {}          # key -> _Accumulator for the current bucket
        self.pair_rows, self.pair_since, self.pair_index = {}, {}, {}
        self.best_since = {}    # (key, side) -> (price, since, left_censored)
        self.incidents = {}     # key -> open incident
        self.next_edge = None
        self.budget.charge(len(self.plans) * 4096, "profile state budget")

    # -- inputs ------------------------------------------------------------------
    def _snapshot_book(self, key, book):
        validity = book.validity
        reason = None if book.reason is None else book.reason.get("kind")
        need_levels = "depth" in self.groups
        if validity != "usable":
            return validity, reason, (), ()
        if need_levels:
            return validity, reason, book.levels("bid"), book.levels("ask")
        bid, ask = book.best_bid(), book.best_ask()
        return validity, reason, (bid,) if bid else (), (ask,) if ask else ()

    def _derive(self, key):
        validity, reason, bids, asks = self.raw_books[key]
        plan = self.plans[key]
        if validity != "usable":
            return _State(validity if validity == "not_initialized" else "unusable:" + (reason or "unknown"))
        if plan["venue"] == "kalshi":
            unit = 10 ** int(plan["price_scale"])
            other = self.raw_books.get(self.counterpart.get(key))
            asks = (tuple((unit - p, q) for p, q in other[2])
                    if other is not None and other[0] == "usable" else ())
        state = _State("usable", bids[0] if bids else None, asks[0] if asks else None)
        if "depth" in self.groups:
            tick = int(self.policy["tick_atoms"][plan["venue"]])
            unit_q = 10 ** int(plan["quantity_scale"])
            sizes = tuple(size * unit_q for size in self.sizes)
            state.depth, state.fills = {}, {}
            for side, levels in (("bid", bids), ("ask", asks)):
                if not levels:
                    continue
                best = levels[0][0]
                within = []
                for k in self.depth_ticks:
                    band = k * tick
                    total = 0
                    for price, quantity in levels:
                        if abs(price - best) > band:
                            break
                        total += quantity
                    within.append(total)
                state.depth[side] = tuple(within)
                fills = walk(tuple(levels), sizes)
                state.fills[side] = tuple(
                    None if fill.depth_limited else abs(fill.cost - best * fill.filled_atoms)
                    for fill in fills)
        return state

    # -- callbacks ---------------------------------------------------------------
    def initial(self, cut, start):
        for key in self.plans:
            self.raw_books[key] = self._snapshot_book(key, cut.books[key])
        for key in self.plans:
            self.state[key] = self._derive(key)
            self.since[key] = start
        self._open_scope(0, start)

    def cut(self, cut, raw, time):
        if self.staged is not None and time > self.staged:
            self._commit()
        self._advance(time)
        transitions = cut.body["book_transitions"]
        scope_books = self.rows
        if raw >= self.start and "activity" in self.groups:
            for transition in transitions:
                key = (transition["key"]["instrument"], transition["key"]["orientation"])
                row = scope_books.get(key)
                if row is None:
                    continue
                decision = transition["decision"]
                activity = row.activity
                activity["transitions"] += 1
                if decision["kind"] == "snapshot":
                    activity["snapshots"] += 1
                elif decision["kind"] == "operations":
                    activity["operations"] += len(decision["operations"])
                else:
                    kind = decision["reason"]["kind"]
                    if kind not in activity["invalidations"]:
                        self.budget.charge(bounds.SLOT + bounds.STR + bounds.INT, "profile state budget")
                    activity["invalidations"][kind] = activity["invalidations"].get(kind, 0) + 1
        if raw >= self.start:
            for observation in cut.body["market_events"]:
                event = observation["event"]
                if event["kind"] != "trade":
                    continue
                value = event["value"]
                key = (value["instrument"], value["orientation"])
                row = scope_books.get(key)
                if row is None:
                    continue
                disposition = observation["disposition"]
                row.trades[disposition] += 1
                if "activity" not in self.groups or disposition == "duplicate":
                    continue
                self._trade(key, row, value)
        changed = set()
        for transition in transitions:
            key = (transition["key"]["instrument"], transition["key"]["orientation"])
            self.raw_books[key] = self._snapshot_book(key, cut.books[key])
            changed.add(key)
            if key in self.counterpart:
                changed.add(self.counterpart[key])
        if changed:
            self.staged = time
            for key in changed:
                self.pending[key] = self._derive(key)

    def terminal(self, end):
        if self.staged is not None:
            self._commit()
        self._advance(end)
        self._close_scope(end, "RUN_END")

    # -- trades ------------------------------------------------------------------
    def _trade(self, key, row, value):
        plan = self.plans[key]
        price, quantity = value["price"], value["quantity"]
        aggressor = value["aggressor"] or "none"
        row.aggressor[aggressor] = row.aggressor.get(aggressor, 0) + 1
        if price["scale"] != plan["price_scale"] or quantity["scale"] != plan["quantity_scale"]:
            row.trades_scale_mismatch += 1
            return
        row.trade_qty += int(quantity["atoms"])
        state = self.pending.get(key) or self.state[key]
        if state.bid is not None and state.ask is not None:
            row.trade_mid2 += 2 * int(price["atoms"]) - state.bid[0] - state.ask[0]
            row.trades_priced += 1
        else:
            row.trades_unpriced += 1

    # -- time --------------------------------------------------------------------
    def _commit(self):
        time = self.staged
        keys = sorted(self.pending)
        # Integrate every affected book and pair with the state that held
        # until now, then switch to the state committed at this time.
        for key in keys:
            if key in self.rows:
                self._integrate(key, time)
        for pair in sorted({pair for key in keys for pair in self.pair_index.get(key, ())}):
            self._integrate_pair(pair, time)
        for key in keys:
            new = self.pending[key]
            row = self.rows.get(key)
            if row is not None:
                if row.start == time:
                    row.open = new.quotes()
                self._transition(key, self.state[key], new, time, row)
            self.state[key] = new
            self.since[key] = time
        self.pending.clear()
        self.staged = None

    def _advance(self, time):
        while True:
            boundary = (int(self.scopes[self.scope]["end_ns"])
                        if self.scope + 1 < len(self.scopes) else None)
            edge = self.next_edge
            candidates = [x for x in (boundary, edge) if x is not None and x <= time]
            if not candidates:
                break
            at = min(candidates)
            if boundary == at:
                self._close_scope(at, "SCOPE_END")
                self.scope += 1
                self._open_scope(self.scope, at)
            else:
                self._flush_rows(at)

    def _open_scope(self, index, at):
        scope = self.scopes[index]
        self.scope_start, self.scope_end = int(scope["start_ns"]), int(scope["end_ns"])
        books = sorted({(b["instrument"], b["orientation"]) for m in scope["members"] for b in m["books"]
                        if (b["instrument"], b["orientation"]) in self.plans})
        self.rows = {key: self._new_row(key, at) for key in books}
        for key in books:
            self.since[key] = at
            state = self.state[key]
            for side in ("bid", "ask"):
                quote = getattr(state, side)
                self.best_since[key, side] = None if quote is None else (quote[0], at, True)
            if self._crossed(state):
                self._open_incident(key, state, at)
        self.pair_rows, self.pair_since, self.pair_index = {}, {}, {}
        if "pair_consistency" in self.groups:
            for pair in pairs_of(scope):
                scales = {(self.plans[k]["price_scale"], self.plans[k]["quantity_scale"])
                          for k in pair[1] if k in self.plans}
                if len(scales) != 1 or not all(k in self.plans for k in pair[1]):
                    continue
                self.pair_rows[pair] = self._new_pair_row(at)
                self.pair_since[pair] = at
                for key in pair[1]:
                    self.pair_index.setdefault(key, []).append(pair)
        self._set_edge(at)

    def _set_edge(self, at):
        edge = at - at % self.width + self.width
        self.next_edge = edge if edge < self.scope_end else None

    def _close_scope(self, at, reason):
        self._flush_rows(at, closing=True)
        for key in sorted(self.incidents):
            self._close_incident(key, at, reason)
        self.rows, self.pair_rows, self.pair_index = {}, {}, {}

    def _flush_rows(self, at, closing=False):
        for key in sorted(self.rows):
            self._integrate(key, at)
            row = self.rows[key]
            if closing:
                for side in ("bid", "ask"):
                    current = self.best_since.get((key, side))
                    if current is not None:
                        row.censored[side] += 1
                        self.best_since[key, side] = None
            if row.start < at:
                self._write_row(key, row, at)
            self.rows[key] = self._new_row(key, at)
        for pair in sorted(self.pair_rows):
            self._integrate_pair(pair, at)
            row = self.pair_rows[pair]
            if row["start"] < at:
                self._write_pair(pair, row, at)
            self.pair_rows[pair] = self._new_pair_row(at)
        if not closing:
            self._set_edge(at)

    # -- accumulation ------------------------------------------------------------
    def _new_row(self, key, at):
        row = _Accumulator()
        row.start = at
        row.durations = {"usable_ns": 0, "not_initialized_ns": 0, "unusable_ns": 0,
                         "bid_empty_ns": 0, "ask_empty_ns": 0, "both_empty_ns": 0, "two_sided_ns": 0}
        row.unusable = {}
        row.spread_hist, row.spread_ns, row.mid2_ns = {}, 0, 0
        row.top_qty_ns = {"bid": 0, "ask": 0}
        row.depth_ns = {side: [0] * len(self.depth_ticks) for side in ("bid", "ask")}
        row.filled_ns = {side: [0] * len(self.sizes) for side in ("bid", "ask")}
        row.limited_ns = {side: [0] * len(self.sizes) for side in ("bid", "ask")}
        row.slippage_ns = {side: [0] * len(self.sizes) for side in ("bid", "ask")}
        row.activity = {"transitions": 0, "snapshots": 0, "operations": 0, "invalidations": {}}
        row.trades = {d: 0 for d in DISPOSITIONS}
        row.aggressor = {}
        row.trade_qty = row.trade_mid2 = row.trades_priced = row.trades_unpriced = 0
        row.trades_scale_mismatch = 0
        row.survival = {side: [0] * (len(self.edges) + 1) for side in ("bid", "ask")}
        row.censored = {"bid": 0, "ask": 0}
        row.crossed_ns = row.locked_ns = 0
        row.open = self.state[key].quotes()
        return row

    def _integrate(self, key, time):
        since = self.since[key]
        dt = time - since
        if dt <= 0:
            return
        self.since[key] = time
        row, state = self.rows[key], self.state[key]
        d = row.durations
        if state.kind == "not_initialized":
            d["not_initialized_ns"] += dt
            return
        if state.kind != "usable":
            d["unusable_ns"] += dt
            reason = state.kind.split(":", 1)[1]
            if reason not in row.unusable:
                self.budget.charge(bounds.SLOT + bounds.STR + bounds.INT, "profile state budget")
            row.unusable[reason] = row.unusable.get(reason, 0) + dt
            return
        d["usable_ns"] += dt
        bid, ask = state.bid, state.ask
        if bid is None:
            d["bid_empty_ns"] += dt
        if ask is None:
            d["ask_empty_ns"] += dt
        if bid is None and ask is None:
            d["both_empty_ns"] += dt
        if bid is not None and ask is not None:
            d["two_sided_ns"] += dt
            spread = ask[0] - bid[0]
            if "top_of_book" in self.groups or "self_crossing" in self.groups:
                if spread not in row.spread_hist:
                    self.budget.charge(bounds.SLOT + 2 * bounds.INT, "profile state budget")
                row.spread_hist[spread] = row.spread_hist.get(spread, 0) + dt
            if "top_of_book" in self.groups:
                row.spread_ns += spread * dt
                row.mid2_ns += (bid[0] + ask[0]) * dt
            if "self_crossing" in self.groups:
                if spread < 0:
                    row.crossed_ns += dt
                elif spread == 0:
                    row.locked_ns += dt
        if "top_of_book" in self.groups:
            if bid is not None:
                row.top_qty_ns["bid"] += bid[1] * dt
            if ask is not None:
                row.top_qty_ns["ask"] += ask[1] * dt
        if "depth" in self.groups and state.depth is not None:
            for side, within in state.depth.items():
                acc = row.depth_ns[side]
                for i, quantity in enumerate(within):
                    acc[i] += quantity * dt
                for i, slip in enumerate(state.fills[side]):
                    if slip is None:
                        row.limited_ns[side][i] += dt
                    else:
                        row.filled_ns[side][i] += dt
                        row.slippage_ns[side][i] += slip * dt

    def _transition(self, key, old, new, time, row):
        if "quote_stability" in self.groups:
            for side in ("bid", "ask"):
                before, after = getattr(old, side), getattr(new, side)
                if (None if before is None else before[0]) == (None if after is None else after[0]):
                    continue
                current = self.best_since.get((key, side))
                if current is not None:
                    survival = time - current[1]
                    if current[2]:
                        row.censored[side] += 1
                    elif survival > 0:
                        index = next((i for i, e in enumerate(self.edges) if survival < e), len(self.edges))
                        row.survival[side][index] += 1
                self.best_since[key, side] = None if after is None else (after[0], time, False)
        if "self_crossing" in self.groups:
            crossed = self._crossed(new)
            incident = self.incidents.get(key)
            if crossed and incident is None:
                self._open_incident(key, new, time)
            elif crossed:
                cross = new.bid[0] - new.ask[0]
                if cross > incident["max"]:
                    incident["max"], incident["quotes"] = cross, new.quotes()
            elif incident is not None:
                self._close_incident(key, time, "UNCROSSED")

    @staticmethod
    def _crossed(state):
        return state.bid is not None and state.ask is not None and state.bid[0] >= state.ask[0]

    def _open_incident(self, key, state, time):
        if "self_crossing" not in self.groups:
            return
        self.incidents[key] = {"start": time, "max": state.bid[0] - state.ask[0],
                               "quotes": state.quotes(), "scope": self.scope}
        self.budget.charge(1024, "profile state budget")

    def _close_incident(self, key, time, reason):
        incident = self.incidents.pop(key)
        self.budget.release(1024)
        if incident["start"] >= time:
            return
        plan = self.plans[key]
        self.writers["incidents.ndjson"].append(self._identity(key, incident["scope"]) | {
            "kind": "self_cross", "start_ns": str(incident["start"]), "end_ns": str(time),
            "end_reason": reason, "censored": reason == "RUN_END",
            "max_cross_atoms": str(incident["max"]), "quotes_at_max": incident["quotes"],
            "tick_atoms": self.policy["tick_atoms"][plan["venue"]],
            "ask_source": "projected" if plan["venue"] == "kalshi" else "native"})

    # -- pairs -------------------------------------------------------------------
    def _new_pair_row(self, at):
        return {"start": at, "both_bids_ns": 0, "both_asks_ns": 0, "bid_sum_dev_ns": 0,
                "ask_sum_dev_ns": 0, "bid_sum_abs_dev_ns": 0, "ask_sum_abs_dev_ns": 0,
                "bid_sum_above_unit_ns": 0, "ask_sum_below_unit_ns": 0}

    def _integrate_pair(self, pair, time):
        dt = time - self.pair_since[pair]
        if dt <= 0:
            return
        self.pair_since[pair] = time
        row = self.pair_rows[pair]
        first, second = (self.state[key] for key in pair[1])
        unit = 10 ** int(self.plans[pair[1][0]]["price_scale"])
        if first.bid is not None and second.bid is not None:
            deviation = first.bid[0] + second.bid[0] - unit
            row["both_bids_ns"] += dt
            row["bid_sum_dev_ns"] += deviation * dt
            row["bid_sum_abs_dev_ns"] += abs(deviation) * dt
            if deviation > 0:
                row["bid_sum_above_unit_ns"] += dt
        if first.ask is not None and second.ask is not None:
            deviation = first.ask[0] + second.ask[0] - unit
            row["both_asks_ns"] += dt
            row["ask_sum_dev_ns"] += deviation * dt
            row["ask_sum_abs_dev_ns"] += abs(deviation) * dt
            if deviation < 0:
                row["ask_sum_below_unit_ns"] += dt

    def _write_pair(self, pair, row, end):
        market, books = pair
        first = self.plans[books[0]]
        self.writers["pair_profile.ndjson"].append({
            "version": 1, "experiment_sha256": self.experiment, "snapshot_sha256": self.snapshot_sha,
            "bundle_id": self.scopes[self.scope]["bundle_id"], "scope": self.scope,
            "scope_run_id": self.scopes[self.scope]["run_id"], "market_id": market,
            "venue": first["venue"], "books": [list(key) for key in books],
            "price_scale": first["price_scale"], "start_ns": str(row["start"]), "end_ns": str(end),
            **{name: str(value) for name, value in row.items() if name != "start"}})

    # -- output ------------------------------------------------------------------
    def _identity(self, key, scope):
        plan = self.plans[key]
        return {"version": 1, "experiment_sha256": self.experiment,
                "snapshot_sha256": self.snapshot_sha, "bundle_id": self.scopes[scope]["bundle_id"],
                "scope": scope, "scope_run_id": self.scopes[scope]["run_id"],
                "instrument": key[0], "orientation": key[1], "venue": plan["venue"],
                "market_id": self.members.get(key), "price_scale": plan["price_scale"],
                "quantity_scale": plan["quantity_scale"]}

    def _write_row(self, key, row, end):
        plan, state = self.plans[key], self.state[key]
        d = row.durations
        out = self._identity(key, self.scope) | {
            "start_ns": str(row.start), "end_ns": str(end),
            "tick_atoms": self.policy["tick_atoms"][plan["venue"]],
            "ask_source": "projected" if plan["venue"] == "kalshi" else "native",
            "groups": self.policy["groups"],
            "state": {**{k: str(v) for k, v in d.items()},
                      "unusable_ns_by_reason": {k: str(v) for k, v in sorted(row.unusable.items())}}}
        if "top_of_book" in self.groups:
            histogram = sorted(row.spread_hist.items())
            out["top_of_book"] = {
                "open": row.open, "close": state.quotes(),
                "spread_atoms_ns": str(row.spread_ns),
                "spread_histogram": [[str(spread), str(ns)] for spread, ns in histogram],
                "spread_p50_atoms": _weighted_rank(histogram, 50),
                "spread_p90_atoms": _weighted_rank(histogram, 90),
                "mid2_atoms_ns": str(row.mid2_ns),
                "bid_top_quantity_ns": str(row.top_qty_ns["bid"]),
                "ask_top_quantity_ns": str(row.top_qty_ns["ask"])}
        if "depth" in self.groups:
            out["depth"] = {"sizes_contracts": self.policy["sizes_contracts"],
                            "depth_ticks": self.policy["depth_ticks"],
                            **{side: {"within_ticks_quantity_ns": [str(v) for v in row.depth_ns[side]],
                                      "filled_ns": [str(v) for v in row.filled_ns[side]],
                                      "depth_limited_ns": [str(v) for v in row.limited_ns[side]],
                                      "slippage_cost_ns": [str(v) for v in row.slippage_ns[side]]}
                               for side in ("bid", "ask")}}
        if "activity" in self.groups:
            out["activity"] = {
                "transitions": row.activity["transitions"], "snapshots": row.activity["snapshots"],
                "operations": row.activity["operations"],
                "invalidations": dict(sorted(row.activity["invalidations"].items())),
                "trades": row.trades, "traded_quantity_atoms": str(row.trade_qty),
                "trade_mid2_deviation_atoms": str(row.trade_mid2),
                "trades_priced": row.trades_priced, "trades_without_mid": row.trades_unpriced,
                "trades_scale_mismatch": row.trades_scale_mismatch,
                "aggressor": dict(sorted(row.aggressor.items()))}
        if "quote_stability" in self.groups:
            out["quote_stability"] = {"edges_ns": self.policy["survival_edges_ns"],
                                      "bid": row.survival["bid"], "ask": row.survival["ask"],
                                      "censored": row.censored}
        if "self_crossing" in self.groups:
            out["self_crossing"] = {"crossed_ns": str(row.crossed_ns), "locked_ns": str(row.locked_ns)}
        self.writers["profile.ndjson"].append(out)

    def finish_files(self):
        return {name: writer.finish() for name, writer in self.writers.items()}
