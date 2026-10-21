"""Independent acceptance check for a research build (RESEARCH_SITE_V1 §6, items 2 and 3).

It shares only the manifest-selected evidence reader, never the build's tables
or aggregation logic. Zstandard goes through the shared ``encoder`` package.
For every pack in the current version it:

1. recomputes the overview's bid and ask ``last`` per bucket and book from the
   profile's ``transitions.ndjson.zst`` with its own algorithm (closed state
   intervals per book, then a sweep per bucket), and compares every value;
2. checks that every overlay interval matches one ``episodes.ndjson`` row of its
   lens (id, times, end reason, censoring) and that the counts agree;
3. prints per-lens fill totals (count, net sum, largest net and its quantity)
   recomputed from the raw episode rows, for comparison with known numbers.

Usage:
    python scripts/research_independent_check.py OUT_DIR KEY_DIR [KEY_DIR ...] \
        [--config configs/research.json]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from fractions import Fraction
from pathlib import Path

from replay.economic_sdk import bounds
from replay.research.files import Files


def rows(output):
    return Files(output).lines("transitions.ndjson")


def intervals(transitions, scopes):
    """Per book: [(start, end, bid, ask)] for usable states of positive length, in time order."""
    ends = [int(s["end_ns"]) for s in scopes]
    per_book, open_state = {}, {}
    for row in transitions:
        if row["type"] != "top":
            continue
        book, t = row["book"], int(row["t_ns"])
        previous = open_state.pop(book, None)
        if previous is not None:
            start, scope, bid, ask, usable = previous
            stop = min(t, ends[scope])
            if usable and stop > start:
                per_book.setdefault(book, []).append((start, stop, bid, ask))
        price = lambda quote: None if quote is None else int(quote[0])  # noqa: E731
        open_state[book] = (t, row["scope"], price(row["bid"]), price(row["ask"]), row["validity"] == "usable")
    for book, (start, scope, bid, ask, usable) in open_state.items():
        if usable and ends[scope] > start:
            per_book.setdefault(book, []).append((start, ends[scope], bid, ask))
    return per_book


def expected_last(per_book, books, start, end, bucket):
    count = -(-(end - start) // bucket)
    result = {}
    for book in range(books):
        bid, ask = [None] * count, [None] * count
        for a, b, quote_bid, quote_ask in per_book.get(book, []):
            first, last = (a - start) // bucket, (b - 1 - start) // bucket
            for index in range(max(0, first), min(count - 1, last) + 1):
                # Intervals arrive in time order, so the latest overlapping one wins.
                if quote_bid is not None:
                    bid[index] = quote_bid
                if quote_ask is not None:
                    ask[index] = quote_ask
        result[book] = (bid, ask)
    return result


def check_overview(pack_dir, profile_output):
    overview = json.loads((pack_dir / "overview.json").read_bytes())
    summary = Files(profile_output).document("summary.json", bounds.MAX_METADATA)
    scopes = summary["transition_scopes"]
    start, end, bucket = int(overview["start_ns"]), int(overview["end_ns"]), int(overview["bucket_ns"])
    per_book = intervals(rows(profile_output), scopes)
    expected = expected_last(per_book, len(summary["transition_books"]), start, end, bucket)
    compared = mismatches = 0
    for series in overview["books"]:
        bid, ask = expected[series["book"]]
        for side, values in (("bid", bid), ("ask", ask)):
            for index, value in enumerate(values):
                compared += 1
                if series[side]["last"][index] != value:
                    mismatches += 1
                    if mismatches <= 5:
                        print(f"    MISMATCH book {series['book']} {side} bucket {index}: "
                              f"pack {series[side]['last'][index]} vs recomputed {value}")
    return compared, mismatches


def decimal(value):
    return format(value.numerator / value.denominator, ".6f") if value.denominator != 1 else str(value.numerator)


def check_overlay(pack_dir, lens, lens_config, group_output):
    overlay = json.loads((pack_dir / "overlays" / f"{lens}.json").read_bytes())
    files = Files(group_output)
    episodes = list(files.lines("episodes.ndjson"))
    by_id = {row["episode_id"]: row for row in episodes}
    bad = 0
    for item in overlay["items"]:
        row = by_id.get(item["id"])
        if row is None or (item["start_ns"], item["end_ns"], item["end_reason"], item["censored"]) != (
                str(row["start_ns"]), str(row["end_ns"]), row["end_reason"], row["censored"]):
            bad += 1
    step = Fraction(files.manifest["policy"]["fills"]["step_contracts"]) \
        if any("fill" in row for row in episodes) else None
    fills = []
    for row in episodes:
        if "fill" not in row:
            continue
        governing = next(r for r in row["fill"]["results"] if r["role"] == "governs")
        if governing["value"] is not None:
            fills.append((Fraction(int(governing["value"]), 10 ** lens_config["value_scale"]),
                          int(governing["steps"]) * step))
    total = sum((value for value, _ in fills), Fraction(0))
    largest = max(fills, default=None)
    overlay_total = sum((Fraction(m["value"]) for item in overlay["items"] for m in item["metrics"]
                         if m["name"] == "net"), Fraction(0))
    return {"items": len(overlay["items"]), "rows": len(episodes), "unmatched": bad + (overlay_total != total),
            "overlay_net_equals_rows": overlay_total == total,
            "count_equal": len(overlay["items"]) == len(episodes),
            "fills": len(fills), "net_total": decimal(total),
            "largest": None if largest is None else {"net": decimal(largest[0]), "quantity": decimal(largest[1])}}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("out", type=Path)
    parser.add_argument("key_dirs", nargs="+", type=Path)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1] / "configs" / "research.json")
    args = parser.parse_args(argv)
    config = json.loads(args.config.read_bytes())
    pointer = json.loads((args.out / "current.json").read_bytes())
    manifest_bytes = (args.out / pointer["manifest_key"]).read_bytes()
    assert hashlib.sha256(manifest_bytes).hexdigest() == pointer["manifest_sha256"], "corpus manifest identity"
    manifest = json.loads(manifest_bytes)
    keys = {k.resolve().name: k.resolve() for k in args.key_dirs}
    failures = 0
    for bundle, entry in sorted(manifest["events"].items()):
        pack_dir = args.out / pointer["version"] / "events" / bundle
        pack = json.loads((pack_dir / "manifest.json").read_bytes())
        result = keys[pack["provenance"]["corpus_key"][:16]] / bundle
        attempt = result / "run" / pack["provenance"]["attempt"]
        print(f"{bundle} ({pack['title']})")
        compared, mismatches = check_overview(pack_dir, attempt / config["lenses"]["profile"]["group"] / "output")
        failures += mismatches > 0
        print(f"  overview bid/ask last: {compared} values compared, {mismatches} mismatches")
        for lens, state in pack["lenses"].items():
            if state["state"] != "ok" or "overlay" not in state:
                continue
            report = check_overlay(pack_dir, lens, config["lenses"][lens], attempt / config["lenses"][lens]["group"] / "output")
            failures += report["unmatched"] > 0 or not report["count_equal"]
            print(f"  {lens}: {json.dumps(report)}")
    print("FAILED" if failures else "ALL CHECKS PASSED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
