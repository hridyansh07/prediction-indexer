"""Causal fixed-clock volatility, fed copied top rows during the capture of transitions.

No fine retention grid is declared. A later top row catches up clock instants
strictly before its time; equal-time rows all apply before that instant samples.
Only a horizon's unfinished destinations and buckets are retained.
"""

import heapq

from replay.economic_sdk import bounds
from replay.economic_sdk.profile_policy import (
    TRANSITIONS_MAX_BYTES, TRANSITIONS_MAX_ROWS, VOLATILITY_FILE,
    VOLATILITY_MAX_LINE, VOLATILITY_PLAIN,
)
from replay.economic_sdk.transitions import Stream
from replay.economic_sdk.volatility_format import (
    MAX_BUCKETS, MAX_PENDING, PENDING_COST, accumulator_cost, pair_tables,
)
from replay.streams.protocol import require


class VolatilityRecorder(Stream):
    def __init__(self, snapshot, plans, policy, counterpart, root, budget):
        super().__init__(snapshot, plans, policy, counterpart, root, budget,
                         plain=VOLATILITY_PLAIN, final=VOLATILITY_FILE,
                         max_rows=TRANSITIONS_MAX_ROWS, max_bytes=TRANSITIONS_MAX_BYTES,
                         max_line=VOLATILITY_MAX_LINE)
        self.config = policy["volatility"]
        self.scopes = snapshot["scopes"]
        self.sample = self.config["sample_ms"] * 1_000_000
        self.width = self.config["bucket_ms"] * 1_000_000
        self.horizons = tuple(h * 1_000_000 for h in self.config["horizons_ms"])
        self.jumps = self.config["jump_atoms"]
        self.cost = accumulator_cost(self.config)
        self.pairs, self.unavailable = pair_tables(
            snapshot, self.table, enabled=self.config["gap_pairs"] is not None)
        # The existing per-series reservation includes totals and fixed lookup
        # state: two index slots per pair, scale factors and doubled thresholds.
        # Pending samples and bucket occurrences keep their separate charges.
        self.budget.charge(bounds.json_cost(self.pairs) + bounds.json_cost(self.unavailable)
                           + (len(self.table) + len(self.pairs)) * self.cost,
                           "volatility state budget")
        self.price_factors = tuple((10 ** max(4 - int(b["price_scale"]), 0),
                                    10 ** max(int(b["price_scale"]) - 4, 0)) for b in self.table)
        self.jump_thresholds = tuple(2 * j for j in self.jumps)
        self.totals = {(kind, i): self._new() for kind, count in
                       (("book", len(self.table)), ("gap", len(self.pairs))) for i in range(count)}
        self.scope = -1
        self.rows, self.pending, self.tops, self.values = {}, [], {}, {}
        self.flushed = -1

    def _new(self):
        return {"samples": 0, "duration": 0, "below": 0, "mid_changes": 0,
                "top_updates": 0, "spread": 0,
                "horizons": [[0, 0, 0, 0, [0] * len(self.jumps)] for _ in self.horizons]}

    def _row(self, bucket, key):
        identity = bucket, key
        if identity not in self.rows:
            require(bucket > self.flushed and len(self.rows) < MAX_BUCKETS, "volatility bucket budget")
            self.budget.charge(self.cost, "volatility state budget")
            self.rows[identity] = self._new()
        return self.rows[identity]

    def _open_scope(self):
        self.scope += 1
        scope = self.scopes[self.scope]
        self.begin_ns, self.end_ns = int(scope["start_ns"]), int(scope["end_ns"])
        self.at = self.next_sample = self.begin_ns
        self.flushed = -1
        native = {(b["instrument"], b["orientation"]) for m in scope["members"] for b in m["books"]}
        self.tops = {i: (None, None, None) for i, b in enumerate(self.table)
                     if (b["instrument"], b["orientation"]) in native}
        self.pairs_by_book = {i: [] for i in self.tops}
        self.values = {("book", i): None for i in self.tops}
        for pair in self.pairs:
            if pair["scope"] == self.scope:
                self.pairs_by_book[pair["book_a"]].append(pair["pair"])
                self.pairs_by_book[pair["book_b"]].append(pair["pair"])
                self.values["gap", pair["pair"]] = None

    def _price(self, value, book):
        if value is None:
            return None
        price = int(value[0])
        multiplier, divisor = self.price_factors[book]
        if divisor == 1:
            return price * multiplier
        require(price % divisor == 0, "volatility scale-4 precision")
        return price // divisor

    @staticmethod
    def _mid(top):
        usable, bid, ask = top
        return bid + ask if usable and bid is not None and ask is not None else None

    def _ask(self, book, side):
        usable, bid, ask = self.tops[book]
        if not usable:
            return None
        if side == self.table[book]["orientation"]:
            return ask
        return None if bid is None else 10000 - bid

    def top(self, row):
        """Observe one top row, never retaining decoder or row objects."""
        while self.scope < row["scope"]:
            if self.scope >= 0:
                self._close_scope()
            self._open_scope()
        time, book = int(row["t_ns"]), row["book"]
        self._advance_before(time)
        old = self._mid(self.tops[book])
        top = (row["validity"] == "usable", self._price(row["bid"], book), self._price(row["ask"], book))
        self.tops[book] = top
        mid = self.values["book", book] = self._mid(top)
        if row["cause"] != "open":
            accumulator = self._row((time - self.begin_ns) // self.width, ("book", book))
            accumulator["top_updates"] += 1
            accumulator["mid_changes"] += int(old is not None and mid is not None and old != mid)
        for index in self.pairs_by_book[book]:
            pair = self.pairs[index]
            a, b = self._ask(pair["book_a"], pair["side_a"]), self._ask(pair["book_b"], pair["side_b"])
            self.values["gap", pair["pair"]] = None if a is None or b is None else 2 * (a + b)

    def _integrate(self, until):
        while self.at < until:
            bucket = (self.at - self.begin_ns) // self.width
            end = min(until, self.begin_ns + (bucket + 1) * self.width)
            dt = end - self.at
            for key, value in self.values.items():
                if value is not None:
                    row = self._row(bucket, key)
                    row["duration"] += dt
                    if key[0] == "gap" and value < 20000:
                        row["below"] += dt
            self.at = end

    def _advance_before(self, until):
        require(self.at <= until <= self.end_ns, "volatility clock bounds")
        while True:
            due = min(self.next_sample, self.pending[0][0] if self.pending else self.end_ns)
            if due >= until or due >= self.end_ns:
                break
            self._integrate(due)
            # Resolve forward moves before admitting samples at the same clock instant.
            while self.pending and self.pending[0][0] == due:
                _, key, bucket, horizon, previous = heapq.heappop(self.pending)
                self.budget.release(PENDING_COST)
                current = self.values[key]
                if current is not None:
                    delta = abs(current - previous)
                    stats = self._row(bucket, key)["horizons"][horizon]
                    stats[0] += 1
                    stats[1] += delta
                    stats[2] += delta * delta
                    stats[3] = max(stats[3], delta)
                    for j, threshold in enumerate(self.jump_thresholds):
                        stats[4][j] += int(delta >= threshold)
            if due == self.next_sample:
                bucket = (due - self.begin_ns) // self.width
                for key, value in self.values.items():
                    if value is None:
                        continue
                    row = self._row(bucket, key)
                    row["samples"] += 1
                    if key[0] == "book":
                        _, bid, ask = self.tops[key[1]]
                        row["spread"] += 2 * (ask - bid)
                    for horizon, offset in enumerate(self.horizons):
                        if due + offset < self.end_ns:
                            require(len(self.pending) < MAX_PENDING, "volatility pending sample budget")
                            self.budget.charge(PENDING_COST, "volatility state budget")
                            heapq.heappush(self.pending, (due + offset, key, bucket, horizon, value))
                self.next_sample += self.sample
            self._flush_ready(due)
        self._integrate(until)
        self._flush_ready(until)

    def _stats_text(self, stats):
        return {str(h): {"n": s[0], "sum_abs2": str(s[1]), "sum_sq4": str(s[2]),
                         "max_abs2": str(s[3]), "jumps": {str(j): n for j, n in zip(self.jumps, s[4])}}
                for h, s in zip(self.config["horizons_ms"], stats)}

    def _flush_ready(self, at, *, closing=False):
        last = ((at - self.begin_ns - self.horizons[-1]) // self.width - 1)
        if closing:
            last = (self.end_ns - self.begin_ns - 1) // self.width
        elif last <= self.flushed:
            # _row rejects already-flushed buckets, so no newly eligible rows
            # can appear until this watermark advances. Closing always drains.
            return
        keys = sorted(identity for identity in self.rows if identity[0] <= last)
        for bucket, key in keys:
            row = self.rows.pop((bucket, key))
            self.budget.release(self.cost)
            if not row["samples"]:
                continue
            total = self.totals[key]
            total["samples"] += row["samples"]
            for a, b in zip(total["horizons"], row["horizons"]):
                for i in range(3):
                    a[i] += b[i]
                a[3] = max(a[3], b[3])
                for i in range(len(self.jumps)):
                    a[4][i] += b[4][i]
            start = self.begin_ns + bucket * self.width
            output = {"type": key[0], "scope": self.scope, key[0] if key[0] == "book" else "pair": key[1],
                      "bucket": bucket, "start_ns": str(start), "end_ns": str(min(start + self.width, self.end_ns)),
                      "samples": row["samples"], "horizons": self._stats_text(row["horizons"])}
            if key[0] == "book":
                output.update(two_sided_ns=str(row["duration"]), mid_changes=row["mid_changes"],
                              top_updates=row["top_updates"], spread_sum2=str(row["spread"]))
            else:
                output.update(defined_ns=str(row["duration"]), below_unit_ns=str(row["below"]))
            self.writer.append(output)
        self.flushed = max(self.flushed, last)

    def _close_scope(self):
        self._advance_before(self.end_ns)
        require(not self.pending, "volatility unfinished in-scope horizons")
        self._flush_ready(self.end_ns, closing=True)

    def terminal(self):
        while self.scope + 1 < len(self.scopes):
            if self.scope >= 0:
                self._close_scope()
            self._open_scope()
        self._close_scope()

    def summary(self):
        def totals(kind):
            return [{kind if kind == "book" else "pair": index, "samples": row["samples"],
                     "horizons": self._stats_text(row["horizons"])}
                    for (name, index), row in sorted(self.totals.items()) if name == kind]
        return {"books": totals("book"), "gaps": totals("gap"),
                "gap_pairs": self.pairs, "unavailable": self.unavailable}
