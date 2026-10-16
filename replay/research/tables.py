"""Corpus tables (Parquet), ``summary.json`` and ``viability.md`` (spec §3)."""

from __future__ import annotations

import io
import statistics
from collections import defaultdict
from fractions import Fraction

import pyarrow as pa
import pyarrow.parquet as pq

from replay.research import realism

DURATION_EDGES_MS = [0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 60_000,
                     300_000, 3_600_000]
NET_EDGES = [0, 0.01, 0.1, 1, 5, 10, 25, 50, 100, 250, 1_000]
QUANTITY_EDGES = [0, 1, 10, 100, 1_000, 10_000, 100_000]
PER_EVENT_EDGES = [0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 1_024]
MIN_DURATION_MS = 100

EVENT_STATES = ("ok", "uncommitted", "receipt_invalid", "context_invalid")
LENS_STATES = ("ok", "failed", "excluded", "not_run")


def _metric(item, name):
    for metric in item["metrics"]:
        if metric["name"] == name:
            return metric
    return None


def leg_keys(item):
    return {(leg["instrument"], leg["orientation"]) for leg in item["detail"]["legs"]}


def overlap_links(items):
    """``(shares_leg_with, group)`` per item: overlapping episodes that buy a common leg.

    ``group`` labels the connected components of that relation, so counting
    distinct groups deduplicates routes that walk one book side at once.
    """
    parent = list(range(len(items)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    shares = [0] * len(items)
    order = sorted(range(len(items)), key=lambda i: (int(items[i]["start_ns"]), i))
    active, keys = [], [leg_keys(item) for item in items]
    for i in order:
        start, end = int(items[i]["start_ns"]), int(items[i]["end_ns"])
        active = [j for j in active if int(items[j]["end_ns"]) > start]
        for j in active:
            if start < int(items[j]["end_ns"]) and int(items[j]["start_ns"]) < end and keys[i] & keys[j]:
                shares[i] += 1
                shares[j] += 1
                parent[find(i)] = find(j)
        active.append(i)
    roots, groups = {}, []
    for i in range(len(items)):
        groups.append(roots.setdefault(find(i), len(roots)))
    return shares, groups


def episode_rows(bundle_id, lens, items, *, books, capture_end_ns, game):
    """Rows for ``episodes.parquet``; links are computed per episode kind."""
    rows = []
    by_kind = defaultdict(list)
    for item in items:
        by_kind[item["detail"]["kind"]].append(item)
    for kind in sorted(by_kind):
        kind_items = by_kind[kind]
        shares, groups = overlap_links(kind_items)
        for item, share, group in zip(kind_items, shares, groups):
            start, end = int(item["start_ns"]), int(item["end_ns"])
            net, quantity = _metric(item, "net"), _metric(item, "quantity")
            legs = item["detail"]["legs"]
            venues = sorted({leg["instrument"].split(":", 1)[0] for leg in legs})
            types = sorted({books[b]["market_type"] for b in item["books"] if books[b]["market_type"]})
            rows.append({
                "bundle_id": bundle_id, "lens": lens, "episode_id": item["id"], "kind": kind,
                "start_ns": start, "end_ns": end, "duration_ms": (end - start) / 1e6,
                "books": item["books"], "markets": sorted({leg["market_id"] for leg in legs if leg["market_id"]}),
                "venues": venues, "venue_pair": "+".join(venues), "censored": item["censored"],
                "end_reason": item["end_reason"],
                "net": None if net is None else float(Fraction(net["value"])),
                "net_exact": None if net is None else net["value"], "net_unit": None if net is None else net["unit"],
                "quantity": None if quantity is None else float(Fraction(quantity["value"])),
                "quantity_exact": None if quantity is None else quantity["value"],
                "pairing_class": item["detail"].get("flags", {}).get("pairing_class"),
                "settlement_review": item["detail"].get("flags", {}).get("settlement_review"),
                "stale_legs": item["detail"].get("flags", {}).get("stale_legs"),
                "flags_state": item["detail"].get("flags_state", "unavailable"),
                "edge_per_set": item["detail"].get("edge_per_set"),
                "t10_value": item["detail"].get("t10_value"),
                "seconds_to_capture_end": (capture_end_ns - start) / 1e9,
                "route_id": item["detail"].get("route_id"), "shares_leg_with": share, "dedup_group": group,
                "game": game, "market_types": types})
    return rows


EPISODE_SCHEMA = pa.schema([
    ("bundle_id", pa.string()), ("lens", pa.string()), ("episode_id", pa.string()), ("kind", pa.string()),
    ("start_ns", pa.int64()), ("end_ns", pa.int64()), ("duration_ms", pa.float64()),
    ("books", pa.list_(pa.int32())), ("markets", pa.list_(pa.string())), ("venues", pa.list_(pa.string())),
    ("venue_pair", pa.string()), ("censored", pa.bool_()), ("end_reason", pa.string()),
    ("net", pa.float64()), ("net_exact", pa.string()), ("net_unit", pa.string()), ("quantity", pa.float64()),
    ("quantity_exact", pa.string()), ("pairing_class", pa.string()), ("settlement_review", pa.bool_()),
    ("stale_legs", pa.list_(pa.int32())), ("flags_state", pa.string()),
    ("edge_per_set", pa.string()), ("t10_value", pa.string()),
    ("seconds_to_capture_end", pa.float64()), ("route_id", pa.string()), ("shares_leg_with", pa.int32()),
    ("dedup_group", pa.int32()), ("game", pa.string()), ("market_types", pa.list_(pa.string())),
])

_PAIRS = pa.list_(pa.list_(pa.string()))
_WALK = pa.struct([("atoms", pa.string()), ("cost", pa.string()), ("taken", _PAIRS), ("consumed", _PAIRS)])
_AGE = pa.struct([("book_ns", pa.string()), ("best_ns", pa.string())])
_DECISION = pa.struct([("time_ns", pa.string()), ("steps", pa.string()), ("limits", pa.list_(pa.string())),
                       ("ages", pa.list_(_AGE)), ("value", pa.string()), ("legs", pa.list_(_WALK))])
_ARRIVAL = pa.struct([("sent_ns", pa.string()), ("arrival_ns", pa.string()), ("venue_delay_ms", pa.int64()),
                     ("delay_source", pa.string()), ("usable", pa.bool_()), ("filled_atoms", pa.string()),
                     ("cost", pa.string()), ("consumed", _PAIRS), ("excess_atoms", pa.string()),
                     ("exit_consumed", _PAIRS), ("exit_proceeds", pa.string())])
PROBE_SCHEMA = pa.schema([
    ("bundle_id", pa.string()), ("event_id", pa.string()), ("lens", pa.string()), ("episode_id", pa.string()),
    ("game", pa.string()), ("scope", pa.int32()), ("markets", pa.list_(pa.string())),
    ("market_types", pa.list_(pa.string())), ("pairing_class", pa.string()), ("settlement_review", pa.bool_()),
    ("stale_legs", pa.list_(pa.int32())), ("gap_pair", pa.int32()), ("gap_pair_state", pa.string()),
    ("sizing", pa.string()), ("confirm_ms", pa.int64()), ("arrival_ms", pa.int64()),
    ("sequencing", pa.string()), ("status", pa.string()), ("decision", _DECISION), ("legs", pa.list_(_ARRIVAL)),
    ("matched_steps", pa.string()), ("value", pa.string()), ("leg_loss_gross", pa.string()),
    ("matched_sets", pa.string()), ("decision_sets", pa.string()), ("value_exact", pa.string()),
    ("value_unit", pa.string()), ("leg_loss_gross_exact", pa.string()),
    ("decision_edge_per_set", pa.string()), ("decision_edge_state", pa.string()),
])
VOLATILITY_SCHEMA = pa.schema([
    ("bundle_id", pa.string()), ("event_id", pa.string()), ("game", pa.string()), ("type", pa.string()),
    ("book", pa.int32()), ("pair", pa.int32()), ("market_types", pa.list_(pa.string())),
    ("horizon_ms", pa.int64()), ("samples", pa.int64()), ("n", pa.int64()), ("state", pa.string()),
    ("sum_sq4", pa.string()), ("rms", pa.float64()), ("jumps_json", pa.string()),
])


def event_schema(lenses):
    fields = [("bundle_id", pa.string()), ("event_id", pa.string()), ("corpus_key", pa.string()),
              ("state", pa.string()), ("message", pa.string()), ("pack", pa.string()), ("pack_error", pa.string()),
              ("context", pa.string()), ("game", pa.string()), ("title", pa.string()),
              ("capture_start_ns", pa.int64()), ("duration_s", pa.float64()), ("venues", pa.list_(pa.string())),
              ("book_count", pa.int32()), ("scope_count", pa.int32()), ("game_state", pa.string()),
              ("bench_status", pa.string())]
    for lens in lenses:
        fields += [(f"{lens}_state", pa.string()), (f"{lens}_error", pa.string()),
                   (f"{lens}_episodes", pa.int32()), (f"{lens}_headline", pa.int32()),
                   (f"{lens}_max_net", pa.float64())]
    return pa.schema(fields)


def events_index(config, event_rows):
    """``events.json``: the events table for the site, with nanosecond times as decimal strings."""
    rows = [{**row, "capture_start_ns": None if row["capture_start_ns"] is None else str(row["capture_start_ns"])}
            for row in event_rows]
    return {"events_version": 1,
            "lenses": [{"lens": name, "label": lens["label"], "adapter": lens["adapter"]}
                       for name, lens in config["lenses"].items()],
            "events": rows}


def parquet_bytes(rows, schema):
    table = pa.Table.from_pylist(rows, schema=schema)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="snappy", write_statistics=True, row_group_size=65_536)
    return sink.getvalue()


def histogram(values, edges):
    """Counts in ``[edges[i], edges[i+1])``, plus an underflow and an overflow bucket."""
    counts = [0] * (len(edges) + 1)
    for value in values:
        index = 0
        while index < len(edges) and value >= edges[index]:
            index += 1
        counts[index] += 1
    return {"edges": edges, "counts": counts}


def _stats(per_event, dedup_per_event, durations, nets, quantities):
    count = len(per_event)
    return {
        "events": count,
        "episodes": sum(per_event), "dedup_episodes": sum(dedup_per_event),
        "events_with_one": sum(1 for n in per_event if n),
        "share_with_one": None if not count else sum(1 for n in per_event if n) / count,
        "per_event_mean": None if not count else statistics.fmean(per_event),
        "per_event_median": None if not count else statistics.median(per_event),
        "dedup_per_event_mean": None if not count else statistics.fmean(dedup_per_event),
        "dedup_per_event_median": None if not count else statistics.median(dedup_per_event),
        "net_total": sum(nets), "net_max": max(nets, default=None),
        "histograms": {"per_event": histogram(per_event, PER_EVENT_EDGES),
                       "duration_ms": histogram(durations, DURATION_EDGES_MS),
                       "net": histogram(nets, NET_EDGES), "quantity": histogram(quantities, QUANTITY_EDGES)},
    }


def summarize(config, event_rows, episodes, *, probes=(), volatility=(), evidence=()):
    """``summary.json``: per lens, headline-kind statistics with and without the 100 ms filter."""
    lenses = {}
    by_lens_event = defaultdict(list)
    for row in episodes:
        by_lens_event[row["lens"], row["bundle_id"]].append(row)
    for name, lens in config["lenses"].items():
        states = {state: sum(1 for e in event_rows if e[f"{name}_state"] == state) for state in LENS_STATES}
        entry = {"label": lens["label"], "adapter": lens["adapter"], "states": states}
        if lens["adapter"] == "sdk_episodes":
            # Episodes are read only where a pack was built (the profile maps legs to books);
            # an ok lens without a pack is counted in ``states`` but not as zero episodes.
            ok = [e["bundle_id"] for e in event_rows if e[f"{name}_state"] == "ok" and e["pack"] == "ok"]
            for label, minimum in (("all", 0), ("min_100ms", MIN_DURATION_MS)):
                per_event, dedup, durations, nets, quantities = [], [], [], [], []
                for bundle in ok:
                    rows = [r for r in by_lens_event[name, bundle]
                            if r["kind"] == lens["headline_kind"] and r["duration_ms"] >= minimum]
                    per_event.append(len(rows))
                    dedup.append(len({r["dedup_group"] for r in rows}))
                    durations += [r["duration_ms"] for r in rows]
                    nets += [r["net"] for r in rows if r["net"] is not None]
                    quantities += [r["quantity"] for r in rows if r["quantity"] is not None]
                entry[label] = _stats(per_event, dedup, durations, nets, quantities)
            entry["headline_kind"] = lens["headline_kind"]
            entry["value_unit"] = lens["value_unit"]
        lenses[name] = entry
    return {"summary_version": 1, "min_duration_ms": MIN_DURATION_MS,
            "events": {state: sum(1 for e in event_rows if e["state"] == state) for state in EVENT_STATES},
            "packs": sum(1 for e in event_rows if e["pack"] == "ok"), "lenses": lenses,
            "execution_realism": {"version": 1, "evidence": list(evidence),
                "delayed_fills": realism.delayed_summaries(probes), "volatility": list(volatility),
                "pair_loss_comparisons": realism.pair_loss_comparisons(probes, volatility)}}


def _fmt(value, digits=2):
    return "—" if value is None else f"{value:,.{digits}f}"


def _breakdown(rows, ok_events, key):
    groups = defaultdict(list)
    for row in rows:
        groups[key(row)].append(row)
    lines = ["| Group | Episodes | Dedup | Events with one | ≥100 ms | Net total | Median quantity |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for group in sorted(groups, key=str):
        members = groups[group]
        quantities = [r["quantity"] for r in members if r["quantity"] is not None]
        lines.append(
            f"| {group or '—'} | {len(members)} | {len({(r['bundle_id'], r['dedup_group']) for r in members})} | "
            f"{len({r['bundle_id'] for r in members})} of {ok_events} | "
            f"{sum(1 for r in members if r['duration_ms'] >= MIN_DURATION_MS)} | "
            f"{_fmt(sum(r['net'] for r in members if r['net'] is not None))} | "
            f"{_fmt(statistics.median(quantities) if quantities else None)} |")
    return lines


def viability(config, version, inputs, superseded, event_rows, episodes, summary):
    lines = ["# Corpus viability report", "",
             f"Corpus version `{version}`. Generated by `python -m replay.research build`; do not edit.", "",
             "Inputs: " + ", ".join(f"`{i['corpus_key'] or i['key_prefix']}` ({i['bundles']} results)"
                                    for i in inputs) + ".", ""]
    if superseded:
        lines.append(f"Superseded results (a later key directory won): {len(superseded)} — "
                     + ", ".join(f"`{s['bundle_id']}` from `{s['key_prefix']}`" for s in superseded) + ".")
        lines.append("")
    lines += ["## Events", "", "| State | Events |", "|---|---:|"]
    lines += [f"| {state} | {count} |" for state, count in summary["events"].items()]
    lines += ["", f"Research packs built: {summary['packs']}.", ""]
    problems = [e for e in event_rows if e["state"] != "ok" or e["pack"] != "ok"]
    for e in problems:
        lines.append(f"- `{e['bundle_id']}`: state `{e['state']}`, pack `{e['pack']}`"
                     + (f" — {e['message'] or e['pack_error']}" if (e['message'] or e['pack_error']) else ""))
    if problems:
        lines.append("")
    lines += ["## Lenses", "",
              "Counts are of each lens's headline episode kind. *Per route* counts every episode; "
              "*dedup* (deduplicated) = connected components of overlapping episodes sharing a leg: if A "
              "overlaps B and B overlaps C they are one group even when A and C never overlap, so dedup "
              "is a conservative lower bound, not a count of distinct opportunities. Net is modeled "
              "apparent edge at episode open, not a return. Nothing is filtered except where a column says "
              "≥100 ms. Duration views are non-causal research context: eventual survival is known only "
              "after the fact. Delayed-fill confirmation uses decision time instead.", ""]
    for name, entry in summary["lenses"].items():
        lines += [f"### {entry['label']} (`{name}`)", "",
                  "Lens states: " + ", ".join(f"{k} {v}" for k, v in entry["states"].items()) + ".", ""]
        failures = [(e["bundle_id"], e[f"{name}_state"], e[f"{name}_error"]) for e in event_rows
                    if e[f"{name}_state"] in ("failed", "excluded")]
        for bundle, state, error in failures:
            lines.append(f"- `{bundle}` {state}: {error}")
        if failures:
            lines.append("")
        if entry["adapter"] != "sdk_episodes":
            continue
        a, b = entry["all"], entry["min_100ms"]
        lines += [f"Headline kind `{entry['headline_kind']}`, value unit {entry['value_unit']}.", "",
                  "| Metric | All | ≥100 ms |", "|---|---:|---:|",
                  f"| Events (lens ok, pack built) | {a['events']} | {b['events']} |",
                  f"| Episodes, per route | {a['episodes']} | {b['episodes']} |",
                  f"| Episodes, dedup | {a['dedup_episodes']} | {b['dedup_episodes']} |",
                  f"| Per event mean (route / dedup) | {_fmt(a['per_event_mean'])} / {_fmt(a['dedup_per_event_mean'])}"
                  f" | {_fmt(b['per_event_mean'])} / {_fmt(b['dedup_per_event_mean'])} |",
                  f"| Per event median (route / dedup) | {_fmt(a['per_event_median'])} / "
                  f"{_fmt(a['dedup_per_event_median'])} | {_fmt(b['per_event_median'])} / "
                  f"{_fmt(b['dedup_per_event_median'])} |",
                  f"| Share of events with at least one | {_fmt(a['share_with_one'])} | {_fmt(b['share_with_one'])} |",
                  f"| Net total | {_fmt(a['net_total'])} | {_fmt(b['net_total'])} |",
                  f"| Largest net | {_fmt(a['net_max'])} | {_fmt(b['net_max'])} |", ""]
        eligible = [r for r in episodes if r["lens"] == name and r["kind"] == entry["headline_kind"]]
        lines += ["Opening edge per set (exact decision-at-open arithmetic; no duration selection):", "",
                  "| Threshold | Episodes | Events | Modeled net total |", "|---|---:|---:|---:|"]
        for cents in (1, 2, 3):
            selected = [r for r in eligible if realism.at_threshold(r.get("edge_per_set"), cents)]
            total = sum((Fraction(r["net_exact"]) for r in selected if r["net_exact"] is not None), Fraction())
            lines.append(f"| ≥{cents}¢ | {len(selected)} | {len({r['bundle_id'] for r in selected})} | "
                         f"{realism.exact_ratio(total)} |")
        lines += ["", f"Opening edge per set unavailable: {sum(r.get('edge_per_set') is None for r in eligible)}.", ""]
        hist = a["histograms"]["duration_ms"]
        lines += ["Duration distribution (ms, all episodes; **non-causal research context**):", "",
                  "| From | To | Episodes |", "|---:|---:|---:|"]
        edges = hist["edges"]
        for index, count in enumerate(hist["counts"]):
            low = "—" if index == 0 else f"{edges[index - 1]:,}"
            high = "—" if index == len(edges) else f"{edges[index]:,}"
            lines.append(f"| {low} | {high} | {count} |")
        lines.append("")
        rows = [r for r in episodes if r["lens"] == name and r["kind"] == entry["headline_kind"]]
        ok_events = a["events"]
        for title, key in (("game", lambda r: r["game"]),
                           ("market type", lambda r: "+".join(r["market_types"])),
                           ("venue pair", lambda r: r["venue_pair"])):
            lines += [f"By {title}:", ""] + _breakdown(rows, ok_events, key) + [""]
    lines += realism_report(summary["execution_realism"])
    return ("\n".join(lines).rstrip() + "\n").encode()


def realism_report(document):
    lines = ["## Delayed fills", "",
             "Thresholds use each sizing's own decision-time value / (decision steps × step_contracts), "
             "with exact rational comparisons. Arrival value and eventual episode duration never select a row. "
             "Money uses the declared common lens unit at scale 36; native leg costs are never summed. "
             "t10 is listed first. Unknown decisions are excluded only from thresholded tables. "
             "Unresolved probes remain in status counts; their ratios and money are unavailable. "
             "Positive-event share uses events with resolved net evidence in the selected group; "
             "the resolved denominator is shown. Gross unwind loss excludes unwind fees.", ""]
    for row in document["evidence"]:
        if row["state"] != "ok":
            lines.append(f"- `{row['bundle_id']}` {row['lens']} {row['kind']}: **unavailable** — {row['reason']}.")
    lines += ["", "| Lens | Sizing | Sequencing | c ms | L ms | Threshold | Group | Status counts | Mean fill ratio | "
              "Median matched sets | Σ value | Σ gross loss | Σ net after loss | Positive event share | Resolved events | Decision edge unavailable | Unit |",
              "|---|---|---|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for r in document["delayed_fills"]:
        threshold = "all" if r["threshold_cents"] == 0 else f"≥{r['threshold_cents']}¢"
        group = "all" if r["group_by"] == "all" else f"{r['group_by']}={r['group']}"
        values = [r[k] if r[k] is not None else "unavailable" for k in (
            "mean_fill_ratio", "median_matched_sets", "value_sum", "leg_loss_gross_sum", "net_after_loss_sum", "positive_event_share")]
        statuses = ", ".join(f"{k}:{r['status_counts'].get(k, 0)}" for k in realism.PROBE_STATUSES)
        lines.append(f"| {r['lens']} | {r['sizing']} | {r['sequencing']} | {r['confirm_ms']} | {r['arrival_ms']} | "
                     f"{threshold} | {group} | {statuses} | " + " | ".join(map(str, values)) +
                     f" | {r['events_with_resolved_probes']} | {r['decision_edge_unavailable']} | {r['value_unit']} |")
    if not document["delayed_fills"]:
        lines += ["", "Delayed-fill evidence unavailable: no recorded probes."]
    lines += ["", "## Volatility", "",
              "Book mid and native-side gap moves at 1 s and 3 s. RMS is a derived square root in quote "
              "currency per contract; exact n and sum_sq4 remain in the table. Jump rates use each "
              "profile threshold in scale-4 price atoms. No evidence is rendered as zero.", "",
              "| Bundle | Game | Market types | Type | Book / pair | Horizon ms | n | RMS | Jump rates |",
              "|---|---|---|---|---|---:|---:|---:|---|"]
    for r in document["volatility"]:
        jumps = ", ".join(f"{j}:{s['rate'] if s['rate'] is not None else 'unavailable'}" for j, s in r["jumps"].items())
        lines.append(f"| {r['bundle_id']} | {r['game']} | {'+'.join(r['market_types'])} | {r['type']} | "
                     f"{r['book'] if r['type'] == 'book' else r['pair']} | {r['horizon_ms']} | {r['n']} | "
                     f"{r['rms'] if r['rms'] is not None else 'unavailable'} | {jumps} |")
    if not document["volatility"]:
        lines += ["", "Volatility evidence unavailable: no recorded volatility rows."]
    lines += ["", "### Gap volatility beside delayed-fill leg loss", "",
              "Joins require the same bundle, scope and both native (book, orientation) sides. Lens and "
              "probe settings remain separate. Multi-leg baskets and pairs without mask evidence remain "
              "unavailable; no event-wide loss is assigned to unrelated pairs.", "",
              "| Bundle | Lens | Pair | Sizing | Sequencing | c ms | L ms | Σ gross loss | Unit | Gap RMS 1 s | Gap RMS 3 s |",
              "|---|---|---:|---|---|---:|---:|---:|---|---:|---:|"]
    for r in document["pair_loss_comparisons"]:
        lines.append(f"| {r['bundle_id']} | {r['lens']} | {r['pair']} | {r['sizing']} | {r['sequencing']} | "
                     f"{r['confirm_ms']} | {r['arrival_ms']} | {r['leg_loss_gross_sum']} | {r['value_unit']} | "
                     f"{r['gap_rms_1s'] if r['gap_rms_1s'] is not None else 'unavailable'} | "
                     f"{r['gap_rms_3s'] if r['gap_rms_3s'] is not None else 'unavailable'} |")
    if not document["pair_loss_comparisons"]:
        lines += ["", "Pair/lens loss comparison unavailable: no matching native-side pair evidence."]
    return lines
