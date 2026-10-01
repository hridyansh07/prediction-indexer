"""Replay wire V1. All wire integers are canonical unsigned strings.

The strict helpers below (closed objects, canonical integers, duplicate-key
rejection) remain the reusable validators for configuration and output files.
The stream Decoder applies only O(1) guards to cuts; see REPLAY_STREAMS_V1.md.
"""

import hashlib
import heapq
import json
import re
from types import MappingProxyType


class ProtocolError(Exception):
    """Deterministic invalid input, sequence, revision, or lifecycle."""


class TransportError(Exception):
    """Transport/resource failure; outcome may be ambiguous. Attempt is dead."""


def require(ok, message="invalid wire value"):
    if not ok:
        raise ProtocolError(message)


def obj(value, fields):
    require(type(value) is dict and set(value) == set(fields.split()), "closed schema")
    return value


def uint(value, maximum=2**64 - 1):
    require(type(value) is str and re.fullmatch(r"0|[1-9][0-9]*", value) is not None)
    require(len(value) <= 20)
    n = int(value)
    require(n <= maximum, "integer range")
    return n


def text(value):
    require(type(value) is str and bool(value) and all(ord(c) >= 32 for c in value))
    return value


def choice(value, choices):
    require(type(value) is str and value in choices.split())
    return value


def array(value):
    require(type(value) is list)
    return value


def key(value):
    obj(value, "instrument orientation")
    instrument = text(value["instrument"])
    require(":" in instrument and all(instrument.split(":", 1)))
    return instrument, choice(value["orientation"], "outcome complement")


def pin(value):
    obj(value, "derivative_address receipt_sha256")
    for s in value.values():
        require(type(s) is str and re.fullmatch("[0-9a-f]{64}", s) is not None)
    return tuple(value[k] for k in ("derivative_address", "receipt_sha256"))


def address(value):
    obj(value, "canonical_seq lane delivery_index event_index")
    require(uint(value["canonical_seq"], 2**63 - 1) > 0)
    text(value["lane"])
    uint(value["delivery_index"])
    uint(value["event_index"], 2**32 - 1)


def reference(value, pins):
    obj(value, "pin address visible_ns order_ns")
    require(pin(value["pin"]) in pins, "unbound reference")
    address(value["address"])
    uint(value["visible_ns"])
    uint(value["order_ns"])


def origin(value, pins):
    require(type(value) is dict)
    if value.get("kind") == "window":
        obj(value, "kind pin start_ns end_ns")
        require(uint(value["start_ns"]) < uint(value["end_ns"]))
    else:
        obj(value, "kind pin first last visible_ns")
        require(value["kind"] == "group")
        address(value["first"])
        address(value["last"])
        uint(value["visible_ns"])
    require(pin(value["pin"]) in pins)


def number(value, quantity=False):
    obj(value, "atoms scale unit")
    scale = uint(value["scale"], 18)
    n = uint(value["atoms"], 2**63 - 1 if quantity else 10**scale)
    require(value["unit"] == ("contracts" if quantity else "quote_per_contract"))
    require(not quantity or n > 0)
    return n, scale


def book_hash(value):
    if value is not None:
        obj(value, "algorithm digest")
        require(
            value["algorithm"] == "sha1"
            and type(value["digest"]) is str
            and re.fullmatch("[0-9a-f]{40}", value["digest"]) is not None
        )


def delta(value):
    obj(value, "instrument orientation side price change book_hash")
    k = key({k: value[k] for k in ("instrument", "orientation")})
    choice(value["side"], "bid ask")
    p, ps = number(value["price"])
    change = value["change"]
    require(type(change) is dict)
    kind = choice(change.get("kind"), "set delete increase decrease")
    obj(change, "kind" if kind == "delete" else "kind value")
    q, qs = (None, None) if kind == "delete" else number(change["value"], True)
    book_hash(value["book_hash"])
    return k, value["side"], p, ps, kind, q, qs


def event(value):
    obj(value, "kind value")
    if value["kind"] == "trade":
        v = obj(value["value"], "instrument orientation price quantity aggressor")
        key({k: v[k] for k in ("instrument", "orientation")})
        number(v["price"])
        number(v["quantity"], True)
        if v["aggressor"] is not None:
            choice(v["aggressor"], "bid ask")
    else:
        require(value["kind"] == "book")
        v = obj(value["value"], "kind value")
        if v["kind"] == "delta":
            delta(v["value"])
        else:
            require(v["kind"] == "full")
            b = obj(
                v["value"],
                "instrument orientation bids asks snapshot_hash source_observed_ns",
            )
            key({k: b[k] for k in ("instrument", "orientation")})
            scales = set()
            for side in ("bids", "asks"):
                prices = []
                for level in array(b[side]):
                    obj(level, "price quantity")
                    p, ps = number(level["price"])
                    _, qs = number(level["quantity"], True)
                    scales.add((ps, qs))
                    prices.append(p)
                require(prices == sorted(set(prices), reverse=side == "bids"))
            require(len(scales) <= 1)
            book_hash(b["snapshot_hash"])
            if b["source_observed_ns"] is not None:
                uint(b["source_observed_ns"])


def control_event(value):
    obj(value, "kind from to")
    require(value["kind"] == "metadata_changed")
    if value["from"] is not None:
        text(value["from"])
    text(value["to"])


def reason(value):
    require(type(value) is dict)
    kind = value.get("kind")
    if kind == "continuity":
        obj(value, "kind verdict")
        choice(
            value["verdict"],
            "gap_proven cursor_went_backwards local_counter_broken conflict",
        )
    elif kind == "lane_invalid":
        obj(value, "kind detail")
        require(value["detail"] is None or type(value["detail"]) is str)
    elif kind == "visible_clock_regression":
        obj(value, "kind previous_visible_ns observed_visible_ns")
        uint(value["previous_visible_ns"])
        uint(value["observed_visible_ns"])
    else:
        obj(value, "kind")
        choice(
            kind,
            "missing_initialization epoch_changed connection_opened connection_closed connection_failed subscription_changed metadata_changed unsupported_state scale_mismatch quantity_underflow quantity_overflow lane_not_expected lane_missing",
        )


def freeze(value):
    if type(value) is dict:
        return MappingProxyType({k: freeze(v) for k, v in value.items()})
    if type(value) is list:
        return tuple(map(freeze, value))
    return value


def pairs(values):
    result = {}
    for k, v in values:
        require(k not in result, "duplicate JSON key")
        result[k] = v
    return result


def decode(data, limit):
    """Strict JSON for configuration/output readers: duplicate keys and NaN or
    Infinity constants are rejected. The stream Decoder uses plain json.loads."""
    require(type(data) is bytes and len(data) <= limit, "entry limit")
    try:
        return json.loads(
            data, object_pairs_hook=pairs, parse_constant=lambda _: require(False)
        )
    except (ValueError, UnicodeError, RecursionError) as e:
        raise ProtocolError("malformed JSON") from e


INITIAL_FIELDS = (
    "pins start_ns end_ns lower_bound plans groups max_entry_bytes max_queue_bytes"
)
ENVELOPE_FIELDS = "version run_id attempt_id sequence kind body"
SIDES = {"bid": 0, "ask": 1}
# Dense price arrays cost 16 * (10**scale + 1) bytes per initialized book
# (Kalshi/Polymarket scale 4: ~160 KiB; Limitless scale 3: ~16 KiB). Larger
# scales would make that allocation unreasonable, so they use a sparse dict.
DENSE_MAX_PRICE_SCALE = 4
# Removing the best level: scanning adjacent slots is fastest for dense books
# (gaps of a few ticks: ~0.2 us), while max()/min() over the occupied set is
# C-level but O(levels) (~0.1 us at 10 levels, ~7 us at 1000). A long Python
# scan across a wide gap is the slowest option (~12 us per 500 empty ticks).
# Scan a bounded number of slots, then fall back to max()/min() of the set.
BEST_SCAN_SLOTS = 64


class _Sparse(dict):
    """Price -> quantity for scales too wide for a dense array; absent is 0."""

    __slots__ = ()

    def __missing__(self, price):
        return 0


class Book:
    """One planned book, mutated IN PLACE by its Decoder.

    Books (and the parsed cut bodies their metadata references) are valid only
    while the hook that received them runs. A strategy that needs any value
    after its hook returns must copy it. `levels()` and `best_*()` return new
    tuples, which may be retained. Invalid/not-initialized books have no levels.
    """

    __slots__ = (
        "revision",
        "validity",
        "dependency",
        "as_of",
        "reason",
        "_limit",
        "_dense",
        "_quantities",
        "_occupied",
        "_best",
    )

    def __init__(self, price_scale):
        self.revision = 0
        self.validity = "not_initialized"
        self.dependency = self.as_of = self.reason = None
        self._limit = 10**price_scale
        self._dense = price_scale <= DENSE_MAX_PRICE_SCALE
        self._quantities = None  # allocated by the first snapshot
        self._occupied = (set(), set())
        self._best = [None, None]

    def __repr__(self):
        return (
            f"Book(revision={self.revision}, validity={self.validity!r}, "
            f"bids={len(self._occupied[0])}, asks={len(self._occupied[1])})"
        )

    def levels(self, side, n=None):
        """(price_atoms, quantity_atoms) tuples: bids descending, asks ascending."""
        require(side in SIDES)
        require(n is None or (type(n) is int and n >= 0), "level count")
        if self.validity != "usable":
            return ()
        s = SIDES[side]
        occupied, quantities = self._occupied[s], self._quantities[s]
        if n is None:
            prices = sorted(occupied, reverse=s == 0)
        elif n == 1:
            best = self._best[s]
            prices = () if best is None else (best,)
        else:
            prices = (heapq.nlargest if s == 0 else heapq.nsmallest)(n, occupied)
        return tuple((p, quantities[p]) for p in prices)

    def _top(self, s):
        best = self._best[s]
        if self.validity != "usable" or best is None:
            return None
        return best, self._quantities[s][best]

    def best_bid(self):
        """(price_atoms, quantity_atoms) of the highest bid, or None."""
        return self._top(0)

    def best_ask(self):
        """(price_atoms, quantity_atoms) of the lowest ask, or None."""
        return self._top(1)

    @property
    def bids(self):
        return self.levels("bid")

    @property
    def asks(self):
        return self.levels("ask")

    def _clear(self):
        if self._quantities is not None:
            for s in (0, 1):
                occupied, quantities = self._occupied[s], self._quantities[s]
                if self._dense:
                    for price in occupied:
                        quantities[price] = 0
                else:
                    quantities.clear()
                occupied.clear()
        self._best[0] = self._best[1] = None

    def _snapshot(self, bids, asks):
        if self._quantities is None:
            size = self._limit + 1
            self._quantities = (
                ([0] * size, [0] * size) if self._dense else (_Sparse(), _Sparse())
            )
        else:
            self._clear()  # only previously occupied slots
        limit = self._limit
        for s, ladder in ((0, bids), (1, asks)):
            occupied, quantities = self._occupied[s], self._quantities[s]
            for price, quantity in ladder:
                p = int(price)
                # Dense-array index guard: a negative index would silently alias.
                require(0 <= p <= limit, "price outside plan scale")
                quantities[p] = int(quantity)
                occupied.add(p)
            # Publisher ladders are ascending: best bid last, best ask first.
            if ladder:
                self._best[s] = int(ladder[-1 if s == 0 else 0][0])

    def _operations(self, operations):
        limit, dense, best = self._limit, self._dense, self._best
        for op in operations:
            s = SIDES[op["side"]]
            p = int(op["price"]["atoms"])
            require(0 <= p <= limit, "price outside plan scale")
            quantities, occupied = self._quantities[s], self._occupied[s]
            change = op["change"]
            kind = change["kind"]
            if kind == "set":
                new = int(change["value"]["atoms"])
            elif kind == "increase":
                new = quantities[p] + int(change["value"]["atoms"])
            elif kind == "decrease":
                new = quantities[p] - int(change["value"]["atoms"])
            else:
                require(kind == "delete", "unknown level change")
                new = 0
            if new > 0:
                quantities[p] = new
                if p not in occupied:
                    occupied.add(p)
                    top = best[s]
                    if top is None or (p > top if s == 0 else p < top):
                        best[s] = p
                continue
            require(new == 0, "invalid authoritative operation")
            if p not in occupied:
                continue
            occupied.discard(p)
            if dense:
                quantities[p] = 0
            else:
                del quantities[p]
            if p == best[s]:
                best[s] = self._next_best(s, p)

    def _next_best(self, s, removed):
        occupied = self._occupied[s]
        if not occupied:
            return None
        if self._dense:
            quantities = self._quantities[s]
            if s == 0:
                scan = range(removed - 1, max(removed - 1 - BEST_SCAN_SLOTS, -1), -1)
            else:
                scan = range(
                    removed + 1, min(removed + 1 + BEST_SCAN_SLOTS, self._limit + 1)
                )
            for price in scan:
                if quantities[price]:
                    return price
        return max(occupied) if s == 0 else min(occupied)


def books_sha256(books):
    """Terminal digest of final local book state; see REPLAY_STREAMS_V1.md.

    SHA-256 over UTF-8 lines, one per planned book sorted by (instrument,
    orientation) code points:
    instrument TAB orientation TAB revision TAB validity TAB bids TAB asks LF,
    where bids (descending) and asks (ascending) are comma-joined
    `price:quantity` decimal atoms, empty unless the book is usable.
    """
    digest = hashlib.sha256()
    for key in sorted(books):
        book = books[key]
        sides = [
            ",".join(f"{p}:{q}" for p, q in book.levels(side)) for side in SIDES
        ]
        digest.update(
            f"{key[0]}\t{key[1]}\t{book.revision}\t{book.validity}\t{sides[0]}\t{sides[1]}\n".encode()
        )
    return digest.hexdigest()


class Cut:
    """One delivered record. `body` is the parsed wire body (initial/terminal
    bodies are frozen; cut bodies are not) and `books` is the decoder's live
    read-only mapping. Neither may be retained past the hook that received it.
    """

    __slots__ = ("sequence", "kind", "body", "books")

    def __init__(self, sequence, kind, body, books):
        self.sequence, self.kind, self.body, self.books = sequence, kind, body, books

    def __repr__(self):
        return f"Cut(sequence={self.sequence}, kind={self.kind!r})"


MALFORMED = (KeyError, TypeError, ValueError, AttributeError, IndexError)


class Decoder:
    """Single-owner local state, mutated in place; any failure poisons it.

    Cut validation is limited to O(1) guards per record/transition: version,
    identity, stream sequence, planned key, book revision chain, usable book
    for operations, and the terminal cut count plus final book digest. The
    publisher ships in the same image and is trusted for everything else.
    """

    def __init__(self, run_id, attempt_id, expected_initial, max_entry_bytes):
        self.run_id, self.attempt_id = run_id, attempt_id
        self._expected = decode(json.dumps(expected_initial).encode(), max_entry_bytes)
        self.limit = max_entry_bytes
        self._books = {}
        self._view = MappingProxyType(self._books)
        self._plans = {}
        self._pins = set()
        self.sequence = -1
        self.terminal = False
        self.poisoned = False

    @property
    def books(self):
        return self._view

    def apply(self, data):
        require(not self.poisoned and not self.terminal, "closed decoder")
        try:
            return self._apply(data)
        except ProtocolError:
            self.poisoned = True
            raise
        except MALFORMED as e:
            self.poisoned = True
            raise ProtocolError("malformed record") from e
        except Exception:
            self.poisoned = True
            raise

    def _apply(self, data):
        require(type(data) is bytes and len(data) <= self.limit, "entry limit")
        try:
            r = json.loads(data)
        except (ValueError, UnicodeError, RecursionError) as e:
            raise ProtocolError("malformed JSON") from e
        obj(r, ENVELOPE_FIELDS)
        require(
            r["version"] == "1"
            and r["run_id"] == self.run_id
            and r["attempt_id"] == self.attempt_id,
            "identity/version",
        )
        seq = self.sequence + 1
        require(r["sequence"] == str(seq), "stream sequence gap or duplicate")
        kind, b = r["kind"], r["body"]
        if seq == 0:
            self._initial(kind, b)
            b = freeze(b)
        elif kind == "terminal":
            obj(b, "cuts books_sha256")
            require(uint(b["cuts"]) == seq - 1, "terminal count")
            require(
                b["books_sha256"] == books_sha256(self._books), "terminal book digest"
            )
            self.terminal = True
            b = freeze(b)
        else:
            require(kind == "cut", "record kind")
            self._cut(b)
        self.sequence = seq
        return Cut(seq, kind, b, self._view)

    def _initial(self, kind, b):
        """Sequence 0 only: complete closed validation of the pinned plan."""
        require(kind == "initial" and b == self._expected, "initial binding")
        obj(b, INITIAL_FIELDS)
        require(uint(b["start_ns"]) < uint(b["end_ns"]))
        choice(b["lower_bound"], "clip expand_to_window_start require_window_boundary")
        require(uint(b["max_entry_bytes"]) == self.limit)
        require(uint(b["max_queue_bytes"]) >= self.limit)
        self._pins = {pin(p) for p in array(b["pins"])}
        require(self._pins and len(self._pins) == len(b["pins"]))
        groups = [text(g) for g in array(b["groups"])]
        require(groups and len(groups) == len(set(groups)))
        books = {}
        for p in array(b["plans"]):
            obj(p, "instrument orientation lane venue price_scale quantity_scale")
            k = key({f: p[f] for f in ("instrument", "orientation")})
            require(k not in self._plans)
            text(p["lane"])
            require(p["venue"] == k[0].split(":", 1)[0])
            self._plans[k] = (
                uint(p["price_scale"], 18),
                uint(p["quantity_scale"], 18),
            )
            books[k] = Book(self._plans[k][0])
        require(books)
        self._books.update(books)

    def _cut(self, b):
        # Atomic per-cut visibility is unnecessary: any failure below poisons
        # the decoder and therefore the whole attempt, so a partially applied
        # cut can never reach a hook or an ACK.
        origin = b["origin"]
        books = self._books
        for t in b["book_transitions"]:
            k = t["key"]
            book = books.get((k["instrument"], k["orientation"]))
            require(book is not None, "unplanned transition")
            revision = int(t["revision"])
            require(
                int(t["previous_revision"]) == book.revision
                and revision == book.revision + 1,
                "book revision gap",
            )
            d = t["decision"]
            kind = d["kind"]
            if kind == "operations":
                require(book.validity == "usable", "operations on unusable book")
                book._operations(d["operations"])
                book.dependency = t["dependency"]
            elif kind == "snapshot":
                book._snapshot(d["bids"], d["asks"])
                book.validity = "usable"
                book.dependency = t["dependency"]
                book.reason = None
            else:
                require(kind == "invalidation", "unknown decision")
                book._clear()
                book.validity = "unusable"
                book.dependency = None
                book.reason = d["reason"]
            book.revision = revision
            book.as_of = origin

    def finish(self):
        require(
            self.terminal and not self.poisoned, "missing terminal / failed attempt"
        )
