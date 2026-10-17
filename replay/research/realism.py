"""Execution evidence adapters and exact research aggregates; no strategy runtime.

Money is decoded with the lens's declared scale. Steps become complete sets only
through the pinned ``step_contracts``. Fractions remain fractions when no finite
decimal exists. Thresholds never use binary floating point or arrival values.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict
from fractions import Fraction
from pathlib import Path

from replay.economic_sdk.profile_stream_io import stream_lines
from replay.research.jsonio import dumps, file_identity, read_json, require

MAX_PROBE_ROWS = 1_000_000
MAX_PROBE_BYTES = 128 * 1024 * 1024
MAX_LINE = 1024 * 1024
MAX_VOLATILITY_SERIES = 8192
MAX_PROBE_STATE = 512 * 1024 * 1024
PROBE_STATUSES = ("filled", "partial", "empty", "unusable", "censored", "not_confirmed", "decision_untradeable")


class ProbeBudget:
    """Conservative admission for decoded rows, Arrow copies and sixteen group links.

    Reserve before retaining. A failed event is reported as a pack failure; it
    cannot silently turn a retained-state overflow into zero opportunities.
    """

    def __init__(self):
        self.used = 0

    @staticmethod
    def cost(row):
        return 8192 + 16 * len(dumps(row))

    def reserve(self, rows):
        cost = sum(self.cost(row) for row in rows)
        require(self.used + cost <= MAX_PROBE_STATE, "research retained probe state budget")
        self.used += cost


def exact_ratio(value):
    """Canonical finite decimal, otherwise a reduced ``numerator/denominator``."""
    value = Fraction(value)
    denominator, twos, fives = value.denominator, 0, 0
    while denominator % 2 == 0:
        denominator, twos = denominator // 2, twos + 1
    while denominator % 5 == 0:
        denominator, fives = denominator // 5, fives + 1
    if denominator != 1:
        return str(value)
    scale = max(twos, fives)
    digits = str(abs(value.numerator) * 10 ** scale // value.denominator).rjust(scale + 1, "0")
    whole, fraction = digits[:len(digits) - scale], digits[len(digits) - scale:].rstrip("0")
    return ("-" if value < 0 else "") + whole + ("." + fraction if fraction else "")


def at_threshold(edge, cents):
    return edge is not None and Fraction(edge) >= Fraction(cents, 100)


def _integer(value, *, signed=False):
    require(type(value) is str, "research canonical integer")
    digits = value[1:] if signed and value.startswith("-") else value
    require(0 < len(digits) <= 80 and all("0" <= c <= "9" for c in digits), "research canonical integer")
    number = int(value)
    require(str(number) == value and (signed or number >= 0), "research canonical integer")
    return number


def probe_detail(row, step, scale, unit):
    """Preserve the SDK row and add exact presentation fields. Missing stays null."""
    require(set(row) == {"episode_id", "sizing", "confirm_ms", "arrival_ms", "sequencing", "status", "decision",
                         "legs", "matched_steps", "value", "leg_loss_gross"}, "research probe fields")
    require(all(type(row[k]) is int and row[k] >= 0 for k in ("confirm_ms", "arrival_ms")),
            "research probe milliseconds")
    require(row["sequencing"] in ("simultaneous", "slow_first"), "research probe sequencing")
    decision = row["decision"]
    if decision is not None:
        require(set(decision) in ({"time_ns", "steps", "limits", "ages"},
                                 {"time_ns", "steps", "limits", "ages", "value", "legs"}),
                "research probe decision fields")
        for leg in decision.get("legs", []):
            require(set(leg) == {"atoms", "cost", "taken", "consumed"}, "research probe decision leg fields")
        for age in decision["ages"]:
            require(set(age) == {"book_ns", "best_ns"}, "research probe age fields")
    for leg in row["legs"]:
        require(set(leg) == {"sent_ns", "arrival_ns", "venue_delay_ms", "delay_source", "usable", "filled_atoms",
                             "cost", "consumed", "excess_atoms", "exit_consumed", "exit_proceeds"},
                "research probe arrival leg fields")
    steps = None if decision is None else _integer(decision["steps"])
    require(step > 0, "probe step_contracts must be positive")
    require(row["status"] in PROBE_STATUSES, "unknown probe status")
    matched = None if row["matched_steps"] is None else _integer(row["matched_steps"])
    require(matched is None or matched >= 0, "negative matched probe steps")
    require(steps is None or steps >= 0, "negative decision probe steps")
    require(matched is None or steps is not None and matched <= steps, "matched probe exceeds decision")
    value = None if row["value"] is None else Fraction(_integer(row["value"], signed=True), 10 ** scale)
    loss = None if row["leg_loss_gross"] is None else Fraction(_integer(row["leg_loss_gross"], signed=True), 10 ** scale)
    # Older SDK probes lack decision.value. Do not substitute opening or arrival value.
    decision_value = None if decision is None else decision.get("value")
    edge = (None if decision_value is None or not steps
            else Fraction(_integer(decision_value, signed=True), 10 ** scale) / (steps * step))
    return {**row, "matched_sets": None if matched is None else exact_ratio(matched * step),
            "decision_sets": None if steps is None else exact_ratio(steps * step),
            "value_exact": None if value is None else exact_ratio(value), "value_unit": unit,
            "leg_loss_gross_exact": None if loss is None else exact_ratio(loss),
            "decision_edge_per_set": None if edge is None else exact_ratio(edge),
            "decision_edge_state": "ok" if edge is not None else "unavailable"}


def attach_probes(output, items, step, scale, unit):
    """Bounded, identity-checked sidecar join; rows never silently lose their episode."""
    output = Path(output)
    manifest = read_json(output / "manifest.json")
    policy = (manifest.get("policy") or {}).get("fills") or {}
    entry = manifest.get("files", {}).get("probes.ndjson")
    enabled = policy.get("probes") is not None
    if not enabled:
        require(entry is None and not (output / "probes.ndjson").exists(), "undeclared probe evidence")
    by_id = {item["id"]: item for item in items}
    require(len(by_id) == len(items), "duplicate overlay episode id")
    for item in items:
        item["detail"]["probes"] = {"state": "ok" if enabled else "unavailable",
                                      "reason": None if enabled else "legacy input: probes not recorded", "items": []}
    if not enabled:
        return {"state": "unavailable", "reason": "legacy input: probes not recorded"}
    probe_policy = policy["probes"]
    require(all(type(probe_policy.get(k)) is list and 0 < len(probe_policy[k]) <= 8
                for k in ("sizings", "confirm_ms", "arrival_ms", "sequencing")), "research probe policy shape")
    require(math.prod(len(probe_policy[k]) for k in ("sizings", "confirm_ms", "arrival_ms", "sequencing")) <= 64,
            "research probe policy product bound")
    require(entry is not None, "probe policy has no probes.ndjson identity")
    require(step is not None, "probes without fill step_contracts")
    path = output / "probes.ndjson"
    require(path.is_file(), "probe policy has no probes.ndjson file")
    actual = file_identity(path)
    require(all(actual[k] == entry[k] for k in actual), "probes.ndjson identity mismatch")
    require(actual["byte_length"] <= MAX_PROBE_BYTES, "research probes exceed byte budget")
    count, seen, budget = 0, set(), ProbeBudget()
    with path.open("rb") as handle:
        while line := handle.readline(MAX_LINE + 1):
            require(len(line) <= MAX_LINE and line.endswith(b"\n"), "research probe line bound")
            count += 1
            require(count <= MAX_PROBE_ROWS, "research probes exceed row budget")
            row = json.loads(line)
            require(row["episode_id"] in by_id, "probe has no episode")
            key = tuple(row[k] for k in ("episode_id", "sizing", "confirm_ms", "arrival_ms", "sequencing"))
            require(key not in seen, "duplicate research probe")
            seen.add(key)
            derived = probe_detail(row, step, scale, unit)
            budget.reserve((derived,))
            by_id[row["episode_id"]]["detail"]["probes"]["items"].append(derived)
    require(count == entry.get("records"), "probes.ndjson record count mismatch")
    expected = {(s, c, a, mode) for s in probe_policy["sizings"] for c in probe_policy["confirm_ms"]
                for a in probe_policy["arrival_ms"] for mode in probe_policy["sequencing"]}
    for item in items:
        rows = item["detail"]["probes"]["items"]
        if item["detail"].get("has_fill"):
            require({(r["sizing"], r["confirm_ms"], r["arrival_ms"], r["sequencing"]) for r in rows} == expected,
                    "incomplete research probe grid")
        else:
            require(not rows, "probe on episode without fill")
    return {"state": "ok", "reason": None}


def probe_rows(bundle_id, event_id, lens, items, books, game, pairs):
    """Flat keys plus full SDK fields, exact analytics, and an exact native-side pair join."""
    pair_index = {(p["scope"], tuple(sorted(((p["book_a"], p["side_a"]), (p["book_b"], p["side_b"]))))): p["pair"]
                  for p in pairs}
    from replay.research.overlays import book_mapper
    mapped = book_mapper(books)
    result = []
    for item in items:
        detail = item["detail"]
        sides = [(mapped(leg), leg["orientation"]) for leg in detail["legs"]]
        pair = pair_index.get((detail["scope"], tuple(sorted(sides)))) if len(sides) == 2 and all(
            b is not None for b, _ in sides) else None
        for row in detail["probes"]["items"]:
            result.append({"bundle_id": bundle_id, "event_id": event_id, "lens": lens, "game": game,
                           "scope": detail["scope"], "market_types": sorted({books[b]["market_type"]
                               for b in item["books"] if books[b].get("market_type")}),
                           "markets": sorted({leg["market_id"] for leg in detail["legs"] if leg["market_id"]}),
                           "pairing_class": detail.get("flags", {}).get("pairing_class"),
                           "settlement_review": detail.get("flags", {}).get("settlement_review"),
                           "stale_legs": detail.get("flags", {}).get("stale_legs"),
                           "gap_pair": pair, "gap_pair_state": "ok" if pair is not None else "unavailable",
                           **row})
    return result


def delayed_summaries(rows):
    """Causal decision thresholds, t10 first; exact common-unit sums and ratios."""
    groups = defaultdict(list)
    for row in rows:
        for cents in (0, 1, 2, 3):
            if cents and not at_threshold(row["decision_edge_per_set"], cents):
                continue
            for group_by in ("all", "game", "pairing_class", "settlement_review"):
                group = None if group_by == "all" else row.get(group_by)
                key = (row["lens"], row["sizing"], row["sequencing"], row["confirm_ms"], row["arrival_ms"],
                       row["value_unit"], cents, group_by, group)
                groups[key].append(row)
    result = []
    for key, members in sorted(groups.items(), key=lambda item: (item[0][0], item[0][1] != "t10", str(item[0]))):
        lens, sizing, mode, confirm, arrival, unit, cents, group_by, group = key
        ratios, matched, values, losses, net, event_net = [], [], [], [], [], defaultdict(Fraction)
        for row in members:
            if row["matched_steps"] is not None and row["decision"] and int(row["decision"]["steps"]):
                ratios.append(Fraction(int(row["matched_steps"]), int(row["decision"]["steps"])))
            if row["matched_sets"] is not None:
                matched.append(Fraction(row["matched_sets"]))
            if row["value_exact"] is not None:
                values.append(Fraction(row["value_exact"]))
            if row["leg_loss_gross_exact"] is not None:
                losses.append(Fraction(row["leg_loss_gross_exact"]))
            if row["value_exact"] is not None and row["leg_loss_gross_exact"] is not None:
                amount = Fraction(row["value_exact"]) - Fraction(row["leg_loss_gross_exact"])
                net.append(amount)
                event_net[row.get("event_id") or row["bundle_id"]] += amount
        result.append({"lens": lens, "sizing": sizing, "sequencing": mode, "confirm_ms": confirm,
                       "arrival_ms": arrival, "value_unit": unit, "threshold_cents": cents,
                       "group_by": group_by, "group": group, "probes": len(members),
                       "status_counts": dict(sorted(Counter(r["status"] for r in members).items())),
                       "decision_edge_unavailable": sum(r["decision_edge_per_set"] is None for r in members),
                       "resolved_probes": len(net), "events_with_resolved_probes": len(event_net),
                       "mean_fill_ratio": None if not ratios else exact_ratio(sum(ratios) / len(ratios)),
                       "median_matched_sets": None if not matched else exact_ratio(statistics.median(matched)),
                       "value_sum": None if not values else exact_ratio(sum(values)),
                       "leg_loss_gross_sum": None if not losses else exact_ratio(sum(losses)),
                       "net_after_loss_sum": None if not net else exact_ratio(sum(net)),
                       "positive_event_share": None if not event_net else exact_ratio(Fraction(
                           sum(v > 0 for v in event_net.values()), len(event_net)))})
    return result


def volatility_document(output, bundle_id, event_id, game, books):
    """Aggregate verified bucket evidence; retain exact totals behind RMS and rates."""
    output = Path(output)
    manifest, summary = read_json(output / "manifest.json"), read_json(output / "summary.json")
    entry = manifest.get("files", {}).get("volatility.ndjson.zst")
    if entry is None:
        policy = manifest.get("policy") or {}
        require("volatility" not in policy.get("groups", []) and "volatility" not in policy,
                "profile declares volatility without volatility.ndjson.zst identity")
        require(not (output / "volatility.ndjson.zst").exists(), "undeclared volatility evidence")
        return {"state": "unavailable", "reason": "legacy input: volatility not recorded", "pairs": [], "rows": []}
    require(type(summary.get("volatility")) is dict, "volatility file without summary")
    pairs = summary["volatility"]["gap_pairs"]
    require(len(pairs) <= 4096 and len(books) <= MAX_VOLATILITY_SERIES, "research volatility series bound")
    totals = {}
    for line in stream_lines(output / "volatility.ndjson.zst", entry["logical"], entry["stored"],
                             MAX_LINE, "volatility"):
        row = json.loads(line)
        require(row["type"] in ("book", "gap"), "unknown volatility row type")
        fields = {"type", "scope", "bucket", "start_ns", "end_ns", "samples", "horizons"}
        fields |= ({"book", "two_sided_ns", "mid_changes", "top_updates", "spread_sum2"} if row["type"] == "book"
                   else {"pair", "defined_ns", "below_unit_ns"})
        require(set(row) == fields, "research volatility row fields")
        kind, index = row["type"], row["book"] if row["type"] == "book" else row["pair"]
        key = (kind, index)
        if key not in totals:
            require(len(totals) < MAX_VOLATILITY_SERIES, "research volatility series bound")
            totals[key] = {"samples": 0, "horizons": {}}
        total = totals[key]
        total["samples"] += row["samples"]
        for horizon, stats in row["horizons"].items():
            require(set(stats) == {"n", "sum_abs2", "sum_sq4", "max_abs2", "jumps"},
                    "research volatility horizon fields")
            require(len(total["horizons"]) < 8 or horizon in total["horizons"], "research volatility horizon bound")
            target = total["horizons"].setdefault(horizon, {"n": 0, "sum_sq4": 0, "sum_abs2": 0,
                                                         "max_abs2": 0, "jumps": {}})
            target["n"] += stats["n"]
            for field in ("sum_sq4", "sum_abs2"):
                target[field] += int(stats[field])
            target["max_abs2"] = max(target["max_abs2"], int(stats["max_abs2"]))
            require(len(stats["jumps"]) <= 6, "research volatility jump bound")
            for jump, count in stats["jumps"].items():
                target["jumps"][jump] = target["jumps"].get(jump, 0) + count
    # A recorded group with no defined samples is known unavailable, not absent.
    for book in range(len(books)):
        totals.setdefault(("book", book), {"samples": 0, "horizons": {}})
    for pair in pairs:
        totals.setdefault(("gap", pair["pair"]), {"samples": 0, "horizons": {}})
    require(len(totals) <= MAX_VOLATILITY_SERIES, "research volatility series bound")
    rows = []
    pair_by_index = {p["pair"]: p for p in pairs}
    require(len(pair_by_index) == len(pairs), "duplicate volatility pair")
    for (kind, index), total in sorted(totals.items()):
        require(kind != "book" or 0 <= index < len(books), "volatility book outside table")
        require(kind != "gap" or index in pair_by_index, "volatility gap outside table")
        member_books = [index] if kind == "book" else [pair_by_index[index][f"book_{s}"] for s in ("a", "b")]
        for horizon in ("1000", "3000"):
            stats = total["horizons"].get(horizon)
            n = 0 if stats is None else stats["n"]
            rows.append({"bundle_id": bundle_id, "event_id": event_id, "game": game, "type": kind,
                         "book": index if kind == "book" else None, "pair": index if kind == "gap" else None,
                         "market_types": sorted({books[b]["market_type"] for b in member_books
                                                  if books[b].get("market_type")}),
                         "horizon_ms": int(horizon), "samples": total["samples"], "n": n,
                         "state": "ok" if n else "unavailable",
                         "sum_sq4": None if stats is None else str(stats["sum_sq4"]),
                         "rms": None if not n else math.sqrt(stats["sum_sq4"] / n) / 20000,
                         "jumps": {} if stats is None else {j: {"count": count,
                             "rate": None if not n else exact_ratio(Fraction(count, n))}
                             for j, count in sorted(stats["jumps"].items(), key=lambda item: int(item[0]))}})
    return {"state": "ok", "reason": None, "pairs": pairs, "rows": rows,
            "unavailable": summary["volatility"].get("unavailable", [])}


def pair_loss_comparisons(probes, volatility):
    """Only exact event+native-side pairs join; lens/probe settings remain distinct."""
    rms = {(r["bundle_id"], r["pair"], r["horizon_ms"]): r for r in volatility if r["type"] == "gap"}
    groups = defaultdict(list)
    for row in probes:
        if row["gap_pair"] is None:
            continue
        key = tuple(row[k] for k in ("bundle_id", "lens", "gap_pair", "sizing", "sequencing", "confirm_ms", "arrival_ms", "value_unit"))
        groups[key].append(row)
    result = []
    for key, members in sorted(groups.items(), key=lambda item: str(item[0])):
        bundle, lens, pair, sizing, mode, confirm, arrival, unit = key
        losses = [Fraction(r["leg_loss_gross_exact"]) for r in members if r["leg_loss_gross_exact"] is not None]
        result.append({"bundle_id": bundle, "lens": lens, "pair": pair, "sizing": sizing, "sequencing": mode,
                       "confirm_ms": confirm, "arrival_ms": arrival, "value_unit": unit,
                       "resolved_probes": len(losses), "leg_loss_gross_sum": None if not losses else exact_ratio(sum(losses)),
                       "gap_rms_1s": rms.get((bundle, pair, 1000), {}).get("rms"),
                       "gap_rms_3s": rms.get((bundle, pair, 3000), {}).get("rms")})
    return result
