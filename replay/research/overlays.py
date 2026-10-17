"""Generic overlays; the SDK episode adapter maps layout-2 episode rows to intervals (spec §2.7).

It reads a group's published files (``manifest.json``, ``entities.json``,
``episodes.ndjson``, ``content_receipt.json``) and imports no strategy runtime.
Every episode becomes one interval, in file order. Nothing is filtered.
"""

from __future__ import annotations

import json
from fractions import Fraction
from pathlib import Path

from replay.research.jsonio import ResearchError, read_json, require
from replay.research import realism

OVERLAY_VERSION = 1


def exact_decimal(value):
    """A Fraction whose denominator divides a power of ten, as an exact decimal string."""
    value = Fraction(value)
    denominator, twos, fives = value.denominator, 0, 0
    while denominator % 2 == 0:
        denominator, twos = denominator // 2, twos + 1
    while denominator % 5 == 0:
        denominator, fives = denominator // 5, fives + 1
    require(denominator == 1, "value is not a finite decimal")
    scale = max(twos, fives)
    digits = str(abs(value.numerator) * 10 ** scale // value.denominator).rjust(scale + 1, "0")
    whole, fraction = digits[:len(digits) - scale], digits[len(digits) - scale:].rstrip("0")
    return ("-" if value < 0 else "") + whole + ("." + fraction if fraction else "")


def book_mapper(books):
    """Leg -> written book index: Kalshi legs of either orientation map to the Yes book."""
    exact = {(b["instrument"], b["orientation"]): index for index, b in enumerate(books)}
    kalshi = {}
    for index, b in enumerate(books):   # the Yes (outcome) book wins when both orientations are written
        if b["venue"] == "kalshi" and (b["orientation"] == "outcome" or b["instrument"] not in kalshi):
            kalshi[b["instrument"]] = index

    def mapped(leg):
        if leg["instrument"].startswith("kalshi:"):
            return kalshi.get(leg["instrument"])
        return exact.get((leg["instrument"], leg["orientation"]))
    return mapped


def _legs(descriptor):
    legs = descriptor.get("legs")
    if type(legs) is not list:
        return []
    return [{"market_id": leg.get("market_id"), "instrument": leg["instrument"], "orientation": leg["orientation"]}
            for leg in legs if type(leg) is dict and "instrument" in leg and "orientation" in leg]


def episode_intervals(output, lens_name, lens, books):
    """Return the overlay document for one ``sdk_episodes`` lens output directory."""
    output = Path(output)
    manifest = read_json(output / "manifest.json")
    receipt = read_json(output / "content_receipt.json")
    scopes = read_json(output / "entities.json")["scopes"]
    fills = (manifest.get("policy") or {}).get("fills")
    step = Fraction(fills["step_contracts"]) if fills else None
    mapped = book_mapper(books)
    items = []
    with (output / "episodes.ndjson").open("rb") as handle:
        for line in handle:
            row = json.loads(line)
            try:
                entity = scopes[row["scope"]][row["entity"]]
            except (IndexError, TypeError) as error:
                raise ResearchError(f"{lens_name}: episode {row.get('episode_id')} has no entity") from error
            items.append(_interval(row, entity["descriptor"], lens, step, mapped))
    probe_evidence = realism.attach_probes(output, items, step, lens["value_scale"], lens["value_unit"])
    return {"overlay_version": OVERLAY_VERSION, "lens": lens_name, "kind": "intervals", "label": lens["label"],
            "semantic_sha256": receipt.get("semantic_sha256"), "probes": probe_evidence, "items": items}


def _interval(row, descriptor, lens, step, mapped):
    start, end = int(row["start_ns"]), int(row["end_ns"])
    legs = _legs(descriptor)
    indexes, unmapped = set(), []
    for leg in legs:
        index = mapped(leg)
        if index is None:
            unmapped.append(leg)
        else:
            indexes.add(index)
    metrics = [{"name": "duration", "value": exact_decimal(Fraction(end - start, 1_000_000)), "unit": "ms"}]
    detail = {"kind": row["kind"], "route_id": descriptor.get("route_id"),
              "basket_kind": descriptor.get("basket_kind"), "legs": legs,
              "viable_tiers": row.get("viable_tiers"), "scope": row["scope"],
              "flags_state": "ok" if "flags" in row else "unavailable"}
    if "flags" in row:
        flags = row["flags"]
        require(type(flags) is dict and set(flags) == {"pairing_class", "settlement_review", "stale_legs"},
                "invalid episode flags")
        require(type(flags["pairing_class"]) is str and type(flags["settlement_review"]) is bool and
                type(flags["stale_legs"]) is list and all(type(i) is int and 0 <= i < len(legs)
                for i in flags["stale_legs"]) and flags["stale_legs"] == sorted(set(flags["stale_legs"])),
                "invalid episode flag types")
        detail["flags"] = flags
    if unmapped:
        detail["unmapped_legs"] = unmapped
    fill = row.get("fill")
    if fill is not None:
        detail["has_fill"] = True
        governing = next((r for r in fill["results"] if r["role"] == "governs"), None)
        require(governing is not None, f"episode {row['episode_id']} fill has no governing result")
        require(step is not None, "fill episode without a fill policy in the manifest")
        if governing["value"] is not None:
            metrics.append({"name": "net", "unit": lens["value_unit"],
                            "value": exact_decimal(Fraction(int(governing["value"]), 10 ** lens["value_scale"]))})
        metrics.append({"name": "quantity", "unit": "contracts",
                        "value": exact_decimal(int(governing["steps"]) * step)})
        contracts = int(governing["steps"]) * step
        edge = (None if not contracts or governing["value"] is None else
                Fraction(int(governing["value"]), 10 ** lens["value_scale"]) / contracts)
        recording = next((r for r in fill["results"] if r["name"] == "t10"), None)
        detail.update(step_contracts=exact_decimal(step),
                      edge_per_set=None if edge is None else realism.exact_ratio(edge),
                      t10_value=None if recording is None or recording["value"] is None else
                      exact_decimal(Fraction(int(recording["value"]), 10 ** lens["value_scale"])))
        detail.update(stop=governing["stop"], sizing=governing["name"], kill_prices=fill["kill_prices"],
                      end_books=fill["end_books"])
    label = row["kind"] if not descriptor.get("basket_kind") else f"{row['kind']} · {descriptor['basket_kind']}"
    return {"id": row["episode_id"], "start_ns": str(start), "end_ns": str(end), "books": sorted(indexes),
            "label": label, "censored": row["censored"], "end_reason": row["end_reason"],
            "metrics": metrics, "detail": detail}
