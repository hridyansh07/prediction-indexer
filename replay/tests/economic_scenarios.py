"""Deterministic synthetic tapes shared by the economic SDK port tests.

Each scenario drives hand-authored preparation-shaped books through the real
Decoder and a strategy factory. Expectations are file hashes recorded from the
pre-SDK same-venue complement V1 implementation, so a port that changes any
byte of V1 output fails.
"""

import hashlib
from unittest.mock import patch

from replay.tests.test_same_venue_complement import Harness, paired_detail

_paired_detail = paired_detail


def limitless_detail():
    value = _paired_detail()
    context = value["context"]
    context["markets"].append({"target_id": "limitless:slug-one", "venue": "limitless", "selected": True})
    context["targets"].append({
        "venue": "limitless", "target_id": "limitless:slug-one",
        "canonical_class": "esports.series_moneyline", "subscription_ids": ["slug-one"],
        "source_ref": "limitless:event-one"})
    context["markets"].sort(key=lambda row: row["target_id"])
    context["targets"].sort(key=lambda row: row["target_id"])
    return value


def ladder(h, time, instrument, orientation="outcome", *, bids=(), asks=(), why=None):
    """Send one snapshot (or invalidation) for one book; ladders are best-first."""
    plan = h.initial["plans"][h.plan_index(instrument, orientation)]
    transition = h.transition(plan, time, why)
    if why is None:
        transition["decision"] = {
            "kind": "snapshot",
            # The wire carries ascending pairs: best bid last, best ask first.
            "bids": [[str(p), str(q)] for p, q in reversed(bids)],
            "asks": [[str(p), str(q)] for p, q in asks],
        }
    ref = h.ref(time)
    return h.send("cut", {"origin": {"kind": "group", "pin": h.pin, "first": ref["address"],
                                     "last": ref["address"], "visible_ns": str(time)},
                          "market_events": [], "book_transitions": [transition]})


M = 1_000_000  # one contract at quantity scale 6


def scenario_single_unknown(h):
    h.window(); h.quote(12, 0); h.quote(12, 1)


def scenario_single_known_slices(h):
    h.window(); h.quote(12, 0); h.quote(12, 1)
    h.quote(13, 0, ask=480); h.quote(17, 0, ask=700); h.quote(17, 0, ask=480)
    h.quote(21, 1, quantity=2 * M); h.quote(25, 1, why={"kind": "connection_closed"})
    h.quote(28, 1, ask=None); h.quote(31, 1)


def scenario_pairs_rich(h):
    h.window()
    pm_a, pm_b = "polymarket:123", "polymarket:987"
    pm_c, pm_d = "polymarket:456", "polymarket:654"
    ladder(h, 11, pm_a, bids=((420, M), (410, 2 * M)), asks=((470, M), (480, 4 * M)))
    ladder(h, 11, pm_b, bids=((500, 2 * M), (300, M)), asks=((510, M), (520, 5 * M)))
    ladder(h, 12, pm_c, bids=((600, 3 * M),), asks=((610, 3 * M),))
    ladder(h, 12, pm_d, bids=((450, 3 * M),), asks=((380, M), (390, 3 * M)))
    ladder(h, 12, "kalshi:series", "outcome", bids=((560, 2 * M), (550, 2 * M)))
    ladder(h, 12, "kalshi:series", "complement", bids=((470, M), (460, 3 * M)))
    ladder(h, 13, "kalshi:series-two", "outcome", bids=((300, 5 * M),))
    ladder(h, 13, "kalshi:series-two", "complement", bids=((600, 5 * M),))
    # Unconsumed-level change, then consumed change, then same-time restore.
    ladder(h, 15, pm_a, bids=((420, M), (410, 2 * M)), asks=((470, M), (480, 4 * M), (900, M)))
    ladder(h, 16, pm_a, bids=((420, M), (410, 2 * M)), asks=((470, 2 * M), (480, 4 * M)))
    ladder(h, 18, pm_b, bids=((500, 2 * M),), asks=((700, M),))
    ladder(h, 18, pm_b, bids=((500, 2 * M), (300, M)), asks=((510, M), (520, 5 * M)))
    ladder(h, 20, "kalshi:series", "complement", bids=((300, M),))
    ladder(h, 22, "kalshi:series", "complement", why={"kind": "connection_closed"})
    ladder(h, 24, "kalshi:series", "complement", bids=((470, M), (460, 3 * M)))
    ladder(h, 26, pm_d, bids=(), asks=((380, M),))
    ladder(h, 29, pm_c, bids=((700, 3 * M),), asks=((710, 3 * M),))
    ladder(h, 33, pm_a, why={"kind": "connection_closed"})
    h.group(35)


def scenario_scopes_known(h):
    h.window(); h.quote(12, 0); h.quote(12, 1)
    h.quote(23, 0, ask=800); h.quote(23, 0, ask=480)
    h.quote(29, 0, ask=800)


def scenario_prologue(h):
    h.window(); h.quote(5, 0); h.quote(5, 1); h.quote(14, 1, ask=470)


def scenario_limitless(h):
    h.window()
    ladder(h, 12, "limitless:slug-one", bids=((520, 4 * M),), asks=((500, 4 * M),))
    ladder(h, 12, "polymarket:123", bids=((400, 4 * M),), asks=((490, 4 * M),))
    ladder(h, 12, "polymarket:987", bids=((400, 4 * M),), asks=((490, 4 * M),))
    ladder(h, 14, "limitless:slug-one", bids=((500, 4 * M),), asks=((500, 4 * M),))
    ladder(h, 16, "limitless:slug-one", bids=((400, 4 * M),), asks=((500, 4 * M),))
    ladder(h, 18, "limitless:slug-one", bids=((400, M),), asks=((500, 4 * M),))


SCENARIOS = {
    "single_unknown": ({}, scenario_single_unknown),
    "single_known_slices": ({"known": True}, scenario_single_known_slices),
    "pairs_rich_known": ({"known": True, "pairs": True}, scenario_pairs_rich),
    "pairs_rich_partial": ({"known": {"polymarket:123", "polymarket:987", "kalshi:series"}, "pairs": True},
                           scenario_pairs_rich),
    "scopes_known": ({"known": True, "scopes": True}, scenario_scopes_known),
    "prologue": ({"known": True, "lower_bound": "expand_to_window_start"}, scenario_prologue),
    "limitless": ({"known": True, "pairs": "limitless"}, scenario_limitless),
}

FILES = ("measurements.ndjson", "episodes.ndjson", "placebo_episodes.ndjson",
         "slices.ndjson", "summary.json", "manifest.json")


def run(name, root, *, policy=None, files=FILES):
    kwargs, drive = SCENARIOS[name]
    kwargs = dict(kwargs)
    detail = None
    if kwargs.get("pairs") == "limitless":
        kwargs["pairs"] = True
        detail = limitless_detail
    if policy is not None:
        kwargs["policy"] = policy
    if detail is None:
        h = Harness(root, **kwargs)
    else:
        with patch("replay.tests.test_same_venue_complement.paired_detail", side_effect=detail):
            h = Harness(root, **kwargs)
    drive(h)
    h.finish()
    return h, {f: hashlib.sha256((h.output / f).read_bytes()).hexdigest()
               for f in files if (h.output / f).exists()}


def operations(h, time, instrument, orientation, side, price, quantity):
    """One ``set``/``delete`` operation on a usable book at ``price``."""
    book = h.decoder.books[instrument, orientation]
    ref = h.ref(time)
    change = ({"kind": "set", "value": {"atoms": str(quantity), "scale": "6", "unit": "contracts"}}
              if quantity else {"kind": "delete"})
    operation = {"instrument": instrument, "orientation": orientation, "side": side,
                 "price": {"atoms": str(price), "scale": "3", "unit": "quote_per_contract"},
                 "change": change, "book_hash": None}
    transition = {"key": {"instrument": instrument, "orientation": orientation},
                  "previous_revision": str(book.revision), "revision": str(book.revision + 1),
                  "dependency": {"epoch": "one", "anchor": ref, "through": ref},
                  "decision": {"kind": "operations", "operations": [operation]}}
    return h.send("cut", {"origin": {"kind": "group", "pin": h.pin, "first": ref["address"],
                                     "last": ref["address"], "visible_ns": str(time)},
                          "market_events": [], "book_transitions": [transition]})


def v2_policy(**overrides):
    """Complement policy 2 on the harness's small time scale."""
    policy = {"version": 2, "detail": {"real": "episodes", "control": "intervals"},
              "controls": [{"kind": "time_shift", "shift_ns": ["5"]}],
              "time_shift_ring_entries": "1000", "profile": None}
    policy.update(overrides)
    return policy


PROFILE_POLICY = {
    "version": 1, "bucket_ns": "7",
    "groups": ["activity", "depth", "pair_consistency", "quote_stability", "self_crossing", "top_of_book"],
    "sizes_contracts": ["1", "3"], "tick_atoms": {"kalshi": "10", "limitless": "1", "polymarket": "10"},
    "depth_ticks": ["1", "5"], "survival_edges_ns": ["2", "5", "10"]}
