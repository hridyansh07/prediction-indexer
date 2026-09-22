"""Closed Replay wire V1. All wire integers are canonical unsigned strings."""

import json
import re
from dataclasses import dataclass
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
    require(type(data) is bytes and len(data) <= limit, "entry limit")
    try:
        return json.loads(
            data, object_pairs_hook=pairs, parse_constant=lambda _: require(False)
        )
    except (ValueError, UnicodeError, RecursionError) as e:
        raise ProtocolError("malformed JSON") from e


@dataclass(frozen=True)
class Book:
    revision: int = 0
    validity: str = "not_initialized"
    bids: tuple = ()
    asks: tuple = ()
    dependency: object = None
    as_of: object = None
    reason: object = None

    def levels(self, side):
        require(side in ("bid", "ask"))
        return self.bids if side == "bid" else self.asks


@dataclass(frozen=True)
class Cut:
    sequence: int
    kind: str
    body: object
    books: object


class Decoder:
    """Single-owner local state. Returned cuts/books contain no mutable aliases."""

    def __init__(self, run_id, attempt_id, expected_initial, max_entry_bytes):
        self.run_id, self.attempt_id = run_id, attempt_id
        self._expected = decode(json.dumps(expected_initial).encode(), max_entry_bytes)
        self.limit = max_entry_bytes
        self._books = {}
        self._plans = {}
        self._pins = set()
        self.sequence = -1
        self.terminal = False
        self.poisoned = False

    @property
    def books(self):
        return MappingProxyType(self._books)

    def apply(self, data):
        require(not self.poisoned and not self.terminal, "closed decoder")
        try:
            return self._apply(data)
        except Exception:
            self.poisoned = True
            raise

    def _apply(self, data):
        r = obj(
            decode(data, self.limit), "version run_id attempt_id sequence kind body"
        )
        require(
            r["version"] == "1"
            and r["run_id"] == self.run_id
            and r["attempt_id"] == self.attempt_id,
            "identity/version",
        )
        seq = uint(r["sequence"])
        require(seq == self.sequence + 1, "stream sequence gap or duplicate")
        b = r["body"]
        staged = self._books.copy()
        if seq == 0:
            require(r["kind"] == "initial" and b == self._expected, "initial binding")
            obj(
                b,
                "pins start_ns end_ns lower_bound plans groups max_entry_bytes max_queue_bytes",
            )
            require(uint(b["start_ns"]) < uint(b["end_ns"]))
            choice(
                b["lower_bound"], "clip expand_to_window_start require_window_boundary"
            )
            require(uint(b["max_entry_bytes"]) == self.limit)
            require(uint(b["max_queue_bytes"]) >= self.limit)
            self._pins = {pin(p) for p in array(b["pins"])}
            require(self._pins and len(self._pins) == len(b["pins"]))
            groups = [text(g) for g in array(b["groups"])]
            require(groups and len(groups) == len(set(groups)))
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
                staged[k] = Book()
            require(staged)
        elif r["kind"] == "terminal":
            obj(b, "cuts")
            require(uint(b["cuts"]) == seq - 1, "terminal count")
            self.terminal = True
        else:
            require(r["kind"] == "cut")
            obj(b, "origin market_events book_transitions")
            origin(b["origin"], self._pins)
            for m in array(b["market_events"]):
                obj(m, "reference event disposition")
                reference(m["reference"], self._pins)
                event(m["event"])
                choice(
                    m["disposition"],
                    "observed applied duplicate not_authority invalidated",
                )
            affected = set()
            for t in array(b["book_transitions"]):
                obj(t, "key previous_revision revision dependency decision")
                k = key(t["key"])
                require(
                    k in staged and k not in affected, "unplanned/repeated transition"
                )
                affected.add(k)
                old = staged[k]
                revision = uint(t["revision"])
                require(
                    uint(t["previous_revision"]) == old.revision
                    and revision == old.revision + 1,
                    "book revision gap",
                )
                d = t["decision"]
                require(type(d) is dict)
                if d.get("kind") == "invalidation":
                    obj(d, "kind reason")
                    reason(d["reason"])
                    require(t["dependency"] is None)
                    staged[k] = Book(
                        revision,
                        "unusable",
                        as_of=freeze(b["origin"]),
                        reason=freeze(d["reason"]),
                    )
                    continue
                dep = obj(t["dependency"], "epoch anchor through")
                text(dep["epoch"])
                reference(dep["anchor"], self._pins)
                reference(dep["through"], self._pins)
                ps, qs = self._plans[k]
                if d.get("kind") == "snapshot":
                    obj(d, "kind bids asks")
                    sides = []
                    for side in ("bids", "asks"):
                        levels = []
                        for level in array(d[side]):
                            require(type(level) is list and len(level) == 2)
                            p, q = uint(level[0], 10**ps), uint(level[1], 2**63 - 1)
                            require(q > 0)
                            levels.append((p, q))
                        require(
                            [p for p, _ in levels] == sorted({p for p, _ in levels})
                        )
                        sides.append(dict(levels))
                    bids, asks = sides
                else:
                    obj(d, "kind operations")
                    require(d["kind"] == "operations" and old.validity == "usable")
                    bids, asks = dict(old.bids), dict(old.asks)
                    for op in array(d["operations"]):
                        dk, side, p, ops, kind, q, oqs = delta(op)
                        require(
                            dk == k and ops == ps and (oqs is None or oqs == qs),
                            "operation plan",
                        )
                        levels = bids if side == "bid" else asks
                        prior = levels.get(p, 0)
                        if kind == "delete":
                            new = 0
                        elif kind == "set":
                            new = q
                        elif kind == "increase":
                            new = prior + q
                        else:
                            new = prior - q
                        require(
                            0 <= new <= 2**63 - 1, "invalid authoritative operation"
                        )
                        if new:
                            levels[p] = new
                        else:
                            levels.pop(p, None)
                staged[k] = Book(
                    revision,
                    "usable",
                    tuple(sorted(bids.items(), reverse=True)),
                    tuple(sorted(asks.items())),
                    freeze(dep),
                    freeze(b["origin"]),
                )
        self._books = staged  # the only visibility point, after complete validation
        self.sequence = seq
        return Cut(seq, r["kind"], freeze(b), MappingProxyType(staged))

    def finish(self):
        require(
            self.terminal and not self.poisoned, "missing terminal / failed attempt"
        )
