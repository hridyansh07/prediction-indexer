"""Per-bundle fee catalog and fee/valuation config blocks.

The reviewed schedules of a template catalog are re-scoped to this bundle's
instruments with the same models, sources and evidence. Venues without a reviewed
schedule keep unknown fees. Quote assets are valued 1:1 (PARITY_SCENARIO).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

from replay.fees.artifacts import build_catalog, load_catalog
from replay.fees.domain import Asset, AssetAmount, AssetKind, Fixed, InstrumentEconomics, canonical
from replay.fees.schedules import Catalog
from replay.preparation import encoded

QUOTE = {"kalshi": Asset(AssetKind.USD, "kalshi-ledger", "usd"),
         "polymarket": Asset(AssetKind.USDC, "polygon", "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"),
         "limitless": Asset(AssetKind.USDC, "base", "0x833589fcd6edb6e08f4c7c32d59f75031dcd02913")}


def fee_inputs(snapshot, settings, out_root):
    """Return (catalog_directory, fees_block, valuation_block) for one prepared context."""
    assets, bindings, economics = {}, [], {}
    for plan in snapshot["plans"]:
        quote = QUOTE[plan["venue"]]
        outcome = Asset(AssetKind.OUTCOME, plan["venue"], plan["instrument"] + ":" + plan["orientation"])
        econ = InstrumentEconomics(quote, outcome, AssetAmount(quote, Fixed(1, 0)), 18,
                                   int(plan["quantity_scale"]), int(plan["price_scale"]))
        economics[plan["instrument"], plan["orientation"]] = econ
        assets[plan["venue"]] = {"kind": quote.kind.value, "ledger": quote.chain, "token": quote.token}
        bindings.append({"instrument": plan["instrument"], "orientation": plan["orientation"],
                         "economics": json.loads(canonical(econ))})
    template = load_catalog(Path(settings["template_catalog"]))
    by_venue = {}
    for schedule in template.schedules:
        by_venue.setdefault(schedule.scope.venue.value, schedule)
    blobs = {}
    for source in sorted(Path(settings["sources"]).iterdir()):
        if source.is_file():
            blobs[hashlib.sha256(source.read_bytes()).hexdigest()] = source.read_bytes()
    schedules = []
    for (instrument, orientation), econ in sorted(economics.items()):
        venue = instrument.split(":", 1)[0]
        if venue not in by_venue:
            continue
        base = by_venue[venue]
        schedules.append(replace(base, scope=replace(base.scope, instrument=instrument.split(":", 1)[1],
                                                     orientation=orientation), economics=econ))
    catalog = Catalog.build(tuple(schedules))
    needed = {s.sha256 for schedule in schedules for s in schedule.sources}
    missing = needed - set(blobs)
    if missing:
        raise ValueError(f"fee sources missing: {sorted(missing)}")
    directory = Path(out_root) / catalog.identity
    if not directory.exists():
        directory = build_catalog(Path(out_root), catalog, {k: v for k, v in blobs.items() if k in needed})
    fees = {"catalog_directory": "{fees}/{catalog_identity}", "catalog_identity": "{catalog_identity}",
            "reference_ns": snapshot["config"]["start_ns"],
            "limitless_buy_bps": settings["limitless_buy_bps"], "limitless_sell_bps": settings["limitless_sell_bps"],
            "kalshi_member_class": settings["kalshi_member_class"], "assets": assets,
            "instrument_bindings": sorted(bindings, key=lambda b: (b["instrument"], b["orientation"]))}
    valuation = {"version": 1, "kind": "PARITY_SCENARIO", "unit": "research_dollar",
                 "assets": sorted(assets.values(), key=encoded)}
    return directory, fees, valuation
