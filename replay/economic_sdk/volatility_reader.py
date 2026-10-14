"""Independent complete reconstruction of volatility from verified transition rows.

This reader imports neither collector nor recorders. Unlike the forward writer's
pending-move heap, it runs shifted destination clocks and looks backward into a
bounded ring of defined grid samples. Mask pairs are rebuilt from snapshot fields.
"""

import heapq
from pathlib import Path

from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import VOLATILITY_FILE, VOLATILITY_MAX_LINE
from replay.economic_sdk.profile_stream_io import stream_lines
from replay.economic_sdk.transitions_reader import identity_record, tables
from replay.economic_sdk.volatility_format import MAX_BUCKETS, MAX_PAIRS, MAX_PENDING, accumulator_cost
from replay.preparation import encoded
from replay.streams.protocol import require


def _pair_evidence(snapshot, books, enabled):
    """Independent mask arithmetic, represented as bitsets in the pinned space."""
    result, unavailable = [], []
    if not enabled:
        return result, unavailable
    outcome = snapshot.get("outcomes", {})
    available = snapshot["version"] == 2 and outcome.get("provider") == "universe"
    document = outcome["document"] if available else {"spaces": [], "claims": []}
    spaces = {s["space_shape_id"]: s for s in document["spaces"]}
    claims = {c["claim_id"]: c for c in document["claims"]}
    for scope_index, scope in enumerate(snapshot["scopes"]):
        scoped = {(b["instrument"], b["orientation"]) for m in scope["members"] for b in m["books"]}
        mappings = {(r["instrument"], r["orientation"]): r for r in scope.get("outcome_books", ())}
        candidates = []
        for book_index, book in enumerate(books):
            if (book["instrument"], book["orientation"]) not in scoped:
                continue
            sides = {book["orientation"]}
            if book["venue"] == "kalshi":
                opposite = "complement" if book["orientation"] == "outcome" else "outcome"
                if (book["instrument"], opposite) in scoped:
                    sides.add(opposite)
            for side in sorted(sides):
                row = mappings.get((book["instrument"], side)) if available else None
                status = row["status"] if row else ("NOT_IN_MODEL" if available else "OUTCOMES_UNAVAILABLE")
                reason = row["reason"] if row else None
                if status == "MASKED":
                    space = spaces[row["space_shape_id"]]
                    if space["coverage"] != "EXHAUSTIVE":
                        status = "INCOMPLETE_COVERAGE"
                if status != "MASKED":
                    unavailable.append({"scope": scope_index, "book": book_index, "side": side,
                                        "status": status, "reason": reason})
                    continue
                positions = {key: i for i, key in enumerate(space["outcome_keys"])}
                mask = sum(1 << positions[key] for key in claims[row["claim_id"]]["outcome_keys"])
                full = (1 << len(positions)) - 1
                if row["negated"]:
                    mask ^= full
                identity = {k: row[k] for k in ("space_shape_id", "claim_id", "negated")}
                candidates.append((book_index, side, book["market_id"], row["space_shape_id"], mask, full, identity))
        candidates.sort(key=lambda c: c[:2])
        lookup = {}
        for c in candidates:
            lookup.setdefault((c[3], c[4]), []).append(c)
        for a in candidates:
            for b in lookup.get((a[3], a[4] ^ a[5]), ()):
                if a[:2] < b[:2] and a[2] != b[2]:
                    require(len(result) < MAX_PAIRS, "volatility gap pair budget")
                    result.append({"scope": scope_index, "pair": len(result),
                                   "book_a": a[0], "side_a": a[1], "book_b": b[0], "side_b": b[1],
                                   "market_a": a[2], "market_b": b[2], "mask_a": a[6], "mask_b": b[6]})
    return result, unavailable


class _Reconstruction:
    def __init__(self, snapshot, policy, output):
        self.snapshot, self.policy, self.output = snapshot, policy["volatility"], output
        self.books, _, self.memberships = tables(snapshot, policy)
        self.pairs, self.unavailable = _pair_evidence(snapshot, self.books, self.policy["gap_pairs"] is not None)
        self.period = self.policy["sample_ms"] * 1_000_000
        self.width = self.policy["bucket_ms"] * 1_000_000
        self.offsets = [h * 1_000_000 for h in self.policy["horizons_ms"]]
        self.cost = accumulator_cost(self.policy)
        self.budget = bounds.StateBudget()
        self.budget.charge(bounds.json_cost(self.pairs) + bounds.json_cost(self.unavailable)
                           + (len(self.books) + len(self.pairs)) * self.cost, "volatility reader state budget")
        self.totals = {(kind, index): self._blank() for kind, count in
                       (("book", len(self.books)), ("gap", len(self.pairs))) for index in range(count)}
        self.scope = -1
        self.buckets = {}
        self.rows = 0

    def _blank(self):
        return {"samples": 0, "two_sided_ns": 0, "defined_ns": 0, "below_unit_ns": 0,
                "spread_sum2": 0, "mid_changes": 0, "top_updates": 0,
                "horizons": {str(h): {"n": 0, "sum_abs2": 0, "sum_sq4": 0, "max_abs2": 0,
                                      "jumps": {str(j): 0 for j in self.policy["jump_atoms"]}}
                             for h in self.policy["horizons_ms"]}}

    def _bucket(self, bucket, key):
        identity = bucket, key
        if identity not in self.buckets:
            require(len(self.buckets) < MAX_BUCKETS, "volatility reader bucket budget")
            self.budget.charge(self.cost, "volatility reader state budget")
            self.buckets[identity] = self._blank()
        return self.buckets[identity]

    def _enter(self):
        self.scope += 1
        scope = self.snapshot["scopes"][self.scope]
        self.start, self.end = int(scope["start_ns"]), int(scope["end_ns"])
        self.clock = self.grid = self.start
        self.destinations = [self.start + h for h in self.offsets]
        self.state = {i: (False, None, None) for i in self.memberships[self.scope]}
        self.scoped_pairs = [p for p in self.pairs if p["scope"] == self.scope]
        self.history, self.expiries, self.history_values = {}, [], 0

    def _prices(self, row):
        scale = int(self.books[row["book"]]["price_scale"])
        prices = []
        for name in ("bid", "ask"):
            quote = row[name]
            if quote is None:
                prices.append(None)
                continue
            numerator = int(quote[0]) * 10000
            price, remainder = divmod(numerator, 10 ** scale)
            require(remainder == 0, "volatility scale-4 precision")
            prices.append(price)
        return row["validity"] == "usable", *prices

    @staticmethod
    def _mid(state):
        usable, bid, ask = state
        return None if not usable or bid is None or ask is None else bid + ask

    def _values(self):
        values = {("book", i): self._mid(state) for i, state in self.state.items()}
        for pair in self.scoped_pairs:
            asks = []
            for suffix in ("a", "b"):
                index, side = pair["book_" + suffix], pair["side_" + suffix]
                usable, bid, ask = self.state[index]
                if side != self.books[index]["orientation"]:
                    ask = None if bid is None else 10000 - bid
                asks.append(ask if usable else None)
            values["gap", pair["pair"]] = 2 * sum(asks) if all(a is not None for a in asks) else None
        return values

    def top(self, row):
        while self.scope < row["scope"]:
            if self.scope >= 0:
                self.close()
            self._enter()
        time, index = int(row["t_ns"]), row["book"]
        self.advance(time)
        old_mid = self._mid(self.state[index])
        self.state[index] = self._prices(row)
        if row["cause"] != "open":
            bucket = self._bucket((time - self.start) // self.width, ("book", index))
            bucket["top_updates"] += 1
            new_mid = self._mid(self.state[index])
            if old_mid is not None and new_mid is not None and old_mid != new_mid:
                bucket["mid_changes"] += 1

    def _duration(self, end, values):
        while self.clock < end:
            bucket = (self.clock - self.start) // self.width
            boundary = min(end, self.start + (bucket + 1) * self.width)
            dt = boundary - self.clock
            for key, value in values.items():
                if value is None:
                    continue
                row = self._bucket(bucket, key)
                row["two_sided_ns" if key[0] == "book" else "defined_ns"] += dt
                if key[0] == "gap" and value < 2 * 10000:
                    row["below_unit_ns"] += dt
            self.clock = boundary

    def advance(self, until):
        values = self._values()
        while True:
            time = min(self.grid, *self.destinations)
            if time >= until or time >= self.end:
                break
            self._duration(time, values)
            for h_index, destination in enumerate(self.destinations):
                if destination != time:
                    continue
                origin = time - self.offsets[h_index]
                earlier = self.history.get(origin, {})
                for key, old in earlier.items():
                    new = values.get(key)
                    if new is None:
                        continue
                    stat = self._bucket((origin - self.start) // self.width, key)["horizons"][str(self.policy["horizons_ms"][h_index])]
                    move = new - old
                    stat["n"] += 1
                    stat["sum_abs2"] += abs(move)
                    stat["sum_sq4"] += move ** 2
                    stat["max_abs2"] = max(stat["max_abs2"], abs(move))
                    for threshold in stat["jumps"]:
                        stat["jumps"][threshold] += int(abs(move) >= 2 * int(threshold))
                self.destinations[h_index] += self.period
            # Destination clocks have consumed all endpoints at this instant.
            # Expiries need not be monotone: near scope end a long horizon drops
            # out and a later origin can finish sooner than an earlier origin.
            while self.expiries and self.expiries[0][0] <= time:
                _, origin = heapq.heappop(self.expiries)
                old = self.history.pop(origin)
                self.history_values -= len(old)
                self.budget.release(512 + len(old) * 256)
            if self.grid == time:
                retained = {key: value for key, value in values.items() if value is not None}
                eligible = [h for h in self.offsets if time + h < self.end]
                if retained and eligible:
                    require(self.history_values + len(retained) <= MAX_PENDING, "volatility reader sample budget")
                    self.budget.charge(512 + len(retained) * 256, "volatility reader state budget")
                    self.history[time] = retained
                    heapq.heappush(self.expiries, (time + eligible[-1], time))
                    self.history_values += len(retained)
                for key in retained:
                    row = self._bucket((time - self.start) // self.width, key)
                    row["samples"] += 1
                    if key[0] == "book":
                        _, bid, ask = self.state[key[1]]
                        row["spread_sum2"] += (ask - bid) * 2
                self.grid += self.period
            self.flush(time)
        self._duration(until, values)
        self.flush(until)

    def _horizons(self, row):
        return {h: {k: (str(v) if k in ("sum_abs2", "sum_sq4", "max_abs2") else v)
                    for k, v in stat.items()} for h, stat in row["horizons"].items()}

    def flush(self, time, closing=False):
        for identity in sorted(self.buckets):
            bucket, key = identity
            end = min(self.start + (bucket + 1) * self.width, self.end)
            if not closing and end + self.offsets[-1] > time:
                continue
            row = self.buckets.pop(identity)
            self.budget.release(self.cost)
            if row["samples"] == 0:
                continue
            output = {"type": key[0], "scope": self.scope, "book" if key[0] == "book" else "pair": key[1],
                      "bucket": bucket, "start_ns": str(self.start + bucket * self.width), "end_ns": str(end),
                      "samples": row["samples"], "horizons": self._horizons(row)}
            if key[0] == "book":
                output.update({k: (str(row[k]) if k in ("two_sided_ns", "spread_sum2") else row[k])
                               for k in ("two_sided_ns", "mid_changes", "top_updates", "spread_sum2")})
            else:
                output.update(defined_ns=str(row["defined_ns"]), below_unit_ns=str(row["below_unit_ns"]))
            line = next(self.output, None)
            require(line == encoded(output) + b"\n", "volatility row recomputation")
            self.rows += 1
            total = self.totals[key]
            total["samples"] += row["samples"]
            for h, stat in row["horizons"].items():
                for name in ("n", "sum_abs2", "sum_sq4"):
                    total["horizons"][h][name] += stat[name]
                total["horizons"][h]["max_abs2"] = max(total["horizons"][h]["max_abs2"], stat["max_abs2"])
                for j, count in stat["jumps"].items():
                    total["horizons"][h]["jumps"][j] += count

    def close(self):
        self.advance(self.end)
        self.flush(self.end, closing=True)
        for retained in self.history.values():
            self.budget.release(512 + len(retained) * 256)
        self.history.clear()

    def finish(self):
        while self.scope + 1 < len(self.snapshot["scopes"]):
            if self.scope >= 0:
                self.close()
            self._enter()
        self.close()
        require(next(self.output, None) is None, "volatility extra rows")
        result = {"books": [], "gaps": [], "gap_pairs": self.pairs, "unavailable": self.unavailable}
        for (kind, index), row in sorted(self.totals.items()):
            result["books" if kind == "book" else "gaps"].append(
                {"book" if kind == "book" else "pair": index, "samples": row["samples"], "horizons": self._horizons(row)})
        return result


def validate_volatility(root, snapshot, files, policy, validate_transitions):
    """Reconstruct from the strict reader's same-pass tops, not writer values.

    Defer the first volatility failure until all transitions/levels checks pass,
    preserving the former strict-streams-before-volatility failure order. Only
    bounded reconstruction state is retained, never a tape of transition rows.
    """
    output = None
    failure = None
    try:
        try:
            path = Path(root) / VOLATILITY_FILE
            require(path.is_file() and not path.is_symlink(), "regular volatility required")
            logical, stored = identity_record(files[VOLATILITY_FILE])
            output = stream_lines(path, logical, stored, VOLATILITY_MAX_LINE, "volatility")
            reconstruction = _Reconstruction(snapshot, policy, output)
        except Exception as error:
            failure = error.with_traceback(None)

        def observe(row):
            nonlocal failure
            if failure is not None:
                return
            try:
                reconstruction.top(row)
            except Exception as error:
                failure = error.with_traceback(None)

        streams = validate_transitions(on_top=observe)
        if failure is not None:
            raise failure
        result = reconstruction.finish()
        require(reconstruction.rows == logical["records"], "volatility record count")
        return streams, result
    finally:
        if output is not None:
            output.close()
