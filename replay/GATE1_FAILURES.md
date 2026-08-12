# Gate 1 failures, and what each one actually means

Gate 1 is deliberately strict, and a report with seven `FAIL` entries reads worse
than the capture usually is. Four of the eight items below are not defects in the
data at all: two are configuration, one is an artifact of auditing a capture that is
still running, and one was the gate misreporting healthy evidence.

This is the reference for reading a report without re-deriving any of it. Each entry
gives the observed symptom, the mechanism, what kind of problem it is, and what
clearing it requires.

Baseline throughout is a real run over 19 hours of three-venue capture:
14,397,493 records across 165 connections.

---

## Read this first: what already passes

`byte_and_envelope_integrity` and `deterministic_capture_order` **both pass**. No
delivery-index gaps in any lane, no local-counter breaks in any epoch, every
envelope parsed and closed, no payload errors.

Those two are the only checks whose failure could not be repaired after the fact.
Everything else in this document is metadata, configuration, or reporting — fixable
later, on data already on disk. If those two pass, the tape is sound.

---

## 0. The run aborts: `object changed after stream snapshot: live/coverage.json`

**Kind: crash. Blocks every gate, not just this one** — gates 2–5 each construct a
`Gate1Auditor` internally.

`DirectoryByteStreamer` snapshots `(size, mtime_ns, inode)` for every admitted
object at construction and re-checks it before and after each read
(`replay/stream.py:136-141`). `CoverageLedger.save()` writes through
`analysis/storage.py:_atomic_write`, which is tempfile + `os.replace` — a **new
inode every time**. The targeter publishes every 10 minutes; a gate 1 run over three
days needs longer than that for two full passes. The collision is close to certain.

This is self-inflicted and recent: before the coverage ledger was restored the file
did not exist, so gate 1 never read it and this could not happen.

**Clearing it: run against a hardlink snapshot.**

```bash
sudo cp -al /srv/prediction-indexer/data /srv/prediction-indexer/snap-$(date -u +%Y%m%dT%H%M%SZ)
```

`cp -al` is near-free — sealed segments are immutable, so nothing is copied, only
directory entries. When the live tree does `os.replace`, the new inode lands there
while the snapshot's entry still points at the old one, so the snapshot genuinely
cannot change under the reader.

This preserves the mutation detector rather than weakening it, which matters: that
detector is what stops a gate producing a time-dependent mixture of old and new
bytes. It is also what item 7 concludes independently — gate 1 audits a *fixture*.

Not sufficient on its own: making `save()` skip an unchanged ledger. Roughly 775 new
assets appeared over three days (~1.7 per generation), so the ledger legitimately
changes most cycles and would still trip a long run.

---

## 1. `every_segment_is_sealed` — "sealed but the segment is absent"

**Kind: reporting bug. The data was always fine. Fixed.**

Observed: 34 segments named, split limitless 30 / kalshi 4. All 34 present on disk
with `line_count: 0`. Independently corroborated by the raw reaper's report, where
those same segments carry `source_sha256: e3b0c442…b855` — the SHA-256 of the empty
string.

`segments_seen` was populated only inside `observe()`, once per parsed record
(`replay/gate1.py:190`), so a segment holding no records never entered it.
`verify_seals` then reported everything in `seals` but not in `segments_seen` as
absent.

A zero-record segment is positive evidence: it proves the lane was alive and the
venue silent for that window. Limitless produced 1,895 records in 19 hours, so a
quiet 30-minute window is its normal state — 30 of its 82 windows. Conflating that
with a deleted file turned a healthy lane into an integrity failure and buried the
real meaning of that message.

`verify_seals` now walks the union of `segments_seen` and `seals`, and consults the
input manifest to tell the cases apart: a key the manifest does not carry is
genuinely absent and still fails; a key it carries is present and simply empty. The
count appears as `empty_segments` in the check's evidence.

A second bug fell out of the same loop: an empty segment was never length- or
digest-checked, because the loop only walked `segments_seen`. It is now verified
against its seal like any other segment.

To confirm on the host at any time:

```bash
sudo python3 -c "
import json, os, glob
for seal in glob.glob('/srv/prediction-indexer/data/spool/lane=*/date=*/*.seal.json'):
    d = json.load(open(seal))
    data = os.path.join(os.path.dirname(seal), d['data_file'])
    if not os.path.exists(data): print('MISSING', data)
    elif d['line_count'] == 0: print('EMPTY  ', data)
"
```

`EMPTY` is healthy. `MISSING` is not.

---

## 2. `market_rules_and_metadata`

**Kind: two stacked problems — one fixed, one structural.**

Observed: `metadata_targets: 0`, with `missing_references` listing every digest the
tape referenced.

**The filter half — fixed, awaiting verification.** Targeter v2 writes its
content-addressed metadata snapshot to
`live/targeter-v2/generations/<run_id>/metadata/<venue>/<digest>.json`, while
`gate1_object` admitted only v1's flat `live/metadata/`. The documents were correct
and simply excluded before anything could read them. `generation_metadata_object`
now admits them, every generation rather than only the published one — the tape
references digests from superseded generations. Expect `metadata_targets` to go
0 → ~453 and `missing_references` to empty.

**The structural half — still failing.** The check also requires
`rules_records > 0`. `observe_metadata` reads
`target["resolution"]["catalogue_record"]` and hits a bare `continue` when it is
absent, three lines before the `rules_records` increment. Targeter v1 embedded the
raw venue record there via `raw_resolution_evidence` (`targeter/targets.py:110`);
v2's `resolution` (`targeter/v2/publication.py:236-249`) is a set of archive
pointers — `run_id`, `bundle_id`, `archive_manifest_key`, and so on — with no
catalogue record.

So this check will report a much better shape and still fail. Clearing it belongs to
the gates 3–5 work, where the same gap has larger consequences: `replay/catalog.py`
needs `resolution.catalogue_record` **and** a non-null `condition_id` to build an
instrument, and v2 supplies neither, so `MetadataCatalogue` is empty and gates 3–5
have nothing to analyse.

---

## 3. `fee_model_evidence`

**Kind: same root cause as item 2. Not an independent problem.**

Observed: `metadata_records_with_fees: 0`.

`fee_records` increments five lines past the same `continue` in `observe_metadata`.
Nothing about fee handling is broken — the branch is simply never reached. It will
clear when item 2's structural half does, and not before.

---

## 4 & 5. `reference_price_observability` and `game_event_observability`

**Kind: configuration. Not a defect, and not fixable in code.**

Observed: all four counters zero, confirmed by the report's own `observations`
block, where `reference_feeds` shows `price_ticks: 0` and `game_states: 0`.

`splice-polymarket-sports` and `splice-polymarket-rtds` sit behind the `reference`
Compose profile and are not running, so no process produces those records. Either
enable the profile or accept both as out of scope. Neither feeds gates 2–5 — they
are exogenous-clock work.

If the profile is enabled, add `--expect-lane polymarket_sports` and
`--expect-lane polymarket_rtds` to **both** finalizer services in the same change.
A lane that runs without being declared is merged into canonical but never counted
toward completeness, so its outage is invisible and every window still reads
complete. That is the same footgun already fixed for kalshi.

---

## 6. `discovery_coverage`

**Kind: real gap, fixed, awaiting verification.**

Observed: `subscribed_assets: 453`, `covered_assets: 0`.

Targeter v1 maintained a coverage ledger from its discovery loop
(`targeter/run.py:236`); v2 shipped without one, so coverage-from-inception —
`docs/CAPTURE_SPEC.md` §6.1, how much of a market's life the tape contains — was not
being measured at all.

`publish_run` now records first sightings, and `scripts/backfill_coverage.py`
reconstructed history from the 212 published generations: 1,228 sightings, 100%
carrying `created_at`. The check needs every subscribed asset covered plus
`created_known > 0`, and both should now hold.

**Reading the lag numbers.** The median discovery lag of ~3.9 days is *not* how long
discovery takes. Every market that already existed when capture was switched on
carries a lag of `capture_start − created_at`, and those dominate the distribution.
The number that measures discovery is the lag restricted to markets created after
the first generation, which should sit at or under the 10-minute publish cadence.
The whole distribution decays toward the real figure as capture runs.

---

## 7. `closed_capture_fixture`

**Kind: expected artifact of auditing a live capture.**

Observed: 165 started, 161 closed, and the four unclosed are exactly one per running
lane — `kalshi`, `limitless`, `polymarket`, `polymarket_snapshots`. Those are the
live connections.

The check compares per-epoch open and close counts. Its name states the assumption:
it audits a *fixture*, meaning stopped capture. It resolves by itself the moment the
splices stop, and it is the clearest statement that gate 1 expects a snapshot —
which is the same conclusion item 0 reaches from the other direction.

Note that a clean restart improves this rather than harming it: the splices seal and
close on the way down, so the previously-open connections gain their terminal
records.

---

## Summary

| # | Check | Kind | Status |
|---|---|---|---|
| 0 | *(run aborts)* | Crash | Run against a `cp -al` snapshot |
| 1 | `every_segment_is_sealed` | Reporting bug | Fixed |
| 2 | `market_rules_and_metadata` | Filter + structural | Filter fixed; rest is gates 3–5 work |
| 3 | `fee_model_evidence` | Same as 2 | Blocked on 2 |
| 4 | `reference_price_observability` | Configuration | Enable `reference` profile, or accept |
| 5 | `game_event_observability` | Configuration | Enable `reference` profile, or accept |
| 6 | `discovery_coverage` | Real gap | Fixed and backfilled |
| 7 | `closed_capture_fixture` | Live-capture artifact | Resolves when capture stops |
| — | `byte_and_envelope_integrity` | — | **Passes** |
| — | `deterministic_capture_order` | — | **Passes** |
