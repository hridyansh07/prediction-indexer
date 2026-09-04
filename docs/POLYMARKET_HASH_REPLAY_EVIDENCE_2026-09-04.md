# Polymarket state hashes and bounded replay

**Status:** completed exploratory evidence, 2026-09-04. This records what the
retained live sample establishes and what Replay must not infer from it.

The source report, capture/analyse tool, snapshot-lane change, and focused tests
were committed together as `60eae4b1943bd495bb6aca3eef645b2f2252f862` in the
evidence thread workspace. That commit was not on this branch when this design
amendment was written. This file retains the reviewed contract-level findings;
the capture artifacts and executable tooling remain the reproduction authority
and must be integrated separately before running the longer acceptance gate.

## Evidence and conclusion

The probe ran for 600 seconds over 12 active markets and both complementary
assets: 24 assets across high, medium, and relatively quiet activity, both
observed tick sizes, and both negative-risk values. It retained 10 REST cycles
(240 full snapshots), one WebSocket epoch with 8,159 application deliveries,
7,873 `price_change` events, and 15,746 price-change entries. There were no REST
or WebSocket failures, reconnects, malformed relevant entries, or observed
`tick_size_change` events. The retained artifact checksums passed.

After excluding the 24 startup-left-censored first-cycle snapshots:

- 216/216 REST snapshots had at least one WebSocket candidate with the same
  `(asset_id, hash)` and an exact full-level match at a candidate frontier;
- the official SDK SHA-1 algorithm reproduced 240/240 REST hashes;
- 206 snapshots had one candidate delivery and 10 had two;
- in all 10 two-candidate cases, the first delivery differed from REST at one
  level and the immediately following same-hash delivery matched exactly.

The observed REST and WebSocket fields therefore share an **asset-scoped
full-state hash space**. The hash is not per-delta or per-delivery certification:
five logical updates, each affecting complementary assets, required two
consecutive same-hash WebSocket deliveries before reconstructed levels matched.
The narrowly evidenced candidate rule is “the end of a maximal contiguous
per-asset same-hash run,” but a ten-minute sample does not make that a protocol
guarantee. Non-contiguous WebSocket recurrence was not observed; repeated REST
hashes did occur for unchanged states.

The SDK preimage is compact UTF-8 JSON in Go field order:

```text
market, asset_id, timestamp, hash="", bids, asks,
min_order_size, tick_size, neg_risk, last_trade_price
```

It is hashed with SHA-1. Reproducing it verifies a complete snapshot
serialization, not an individual delta, delivery, metadata transition, or the
completeness of a captured suffix.

## Placement, latency, and recovery

The experiment kept source time, request interval, receipt time, and canonical
receive order separate. Candidate placement relative to the REST request was 202
before request start, 18 during the request, and six after response receipt. The
six late candidates carried the exact REST timestamp and levels. Therefore
request or receipt clocks do not locate a historical state; `(asset_id, hash)`
enumerates candidate addresses, and exact levels plus a predeclared run rule must
resolve them.

Observed chosen-anchor-to-REST-receipt lag was:

| minimum | p50 | p95 | maximum |
|---:|---:|---:|---:|
| 13.8 ms | 4.216 s | 35.863 s | **155.659 s** |

This maximum falsifies any proposed time journal shorter than 155.659 seconds.
It is not a production bound: an unchanged quiet book can refer much farther
back. A new divergence can remain undetected until the next usable poll; recovery
can take longer still.

A stale full anchor H2 must never reset a current H3 book. Recovery at H3 needs
the H2 full state and an exact replay of the retained H2→H3 suffix. Polymarket
provides no dense source sequence, so venue completeness of that suffix is not
provable. Canonical delivery continuity proves only that the recorder retained
the deliveries it accepted. Two-pass replay is exact relative to the captured
tape, not proof that the tape is venue-complete.

## Required Replay contract

Every candidate retains every canonical delivery address. Replay must not pool
equal hashes, pick an arbitrary occurrence, erase mismatching candidate
addresses, cross an epoch or known continuity fault, or treat hash equality as
proof of a suffix.

Until a one-pass replacement passes the gate below, exact offline Replay uses a
two-pass oracle:

1. collect independent snapshots with every occurrence and canonical address;
2. reconstruct WebSocket state, completing the predeclared same-hash candidate
   run before comparison;
3. place the historical anchor and replay the exact captured suffix to the
   current frontier;
4. derive trust/demotion intervals without rewriting canonical evidence.

A production one-pass implementation requires a bound declared before its
acceptance run. Per asset it retains current book and metadata; every address in
the open same-hash run; a bounded, lossless journal or references with byte cost,
epoch, source/receipt/canonical times, parse status, and before/after frontier;
and provisional trust/output boundaries subject to retrospective demotion.

On anchor arrival it enumerates all in-bound candidates, compares each
delivery-end state, applies only the approved candidate rule, and replays the
exact suffix before replacing current state. It may promote trust only when
placement and suffix are unambiguous and complete under the captured-tape
contract. Otherwise it emits one of:

- `AnchorTooOld` — every candidate precedes the journal bound;
- `AmbiguousAnchor` — recurrence or multiple runs cannot be resolved;
- `MissingSuffix` — eviction, parse failure, absent delivery/state, unsafe epoch,
  or continuity fault prevents exact suffix replay;
- `Divergence` — the completed candidate state differs from the anchor;
- `AnchorPending` — REST state has not yet arrived on WebSocket.

No outcome permits silent one-pass trust promotion or heuristic current-state
reset. The affected asset remains unusable, waits for a later usable full anchor,
or is processed explicitly by the two-pass oracle under a separately identified
result path.

## Replacement acceptance gate

Run a predeclared 24-hour minimum, preferably 72-hour, stratified capture with
both complementary assets. Preserve source/request/receipt/canonical placement,
candidate multiplicity and every address, level diffs, reconnects, continuity
faults, parse failures, event/byte/time journal demand, and detection and recovery
latency separately.

One-pass may replace two-pass only if:

- the declared bound retains every candidate and complete captured suffix;
- there are zero `AnchorTooOld`, `AmbiguousAnchor`, `MissingSuffix`, unhandled
  relevant shapes, or silent fallbacks;
- every selected candidate exactly matches under the declared completion rule;
- books and trust intervals/reasons/demotion/recovery boundaries are
  byte-identical to the two-pass oracle;
- measured memory stays within declared per-asset event, byte, and time bounds;
- startup, reconnect, split repeated-hash, unchanged REST repetition,
  non-contiguous recurrence, late arrival, stale-H2/current-H3, malformed event,
  known continuity fault, and overflow fixtures all fail closed.

The candidate rule, production journal bounds, pending horizon, automatic
two-pass fallback policy, and whether a completed hash run is also an economic
evaluation group are product/protocol decisions. The Replay design's approval
checklist owns them; this evidence does not silently decide them.

## Tooling checks reviewed

The focused test suite pins the official SDK vector, numeric level
normalization/side ordering, request-boundary classification, apply-before-
delivery-end comparison, and preservation of two equal-hash deliveries as two
distinct candidates. The live analyser additionally reports candidate placement,
multiplicity, complete level differences, SHA-1 reproduction, suffix events and
conservative delivery bytes, and explicitly records that capture counters cannot
prove a venue-complete suffix.
