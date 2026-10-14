# Replay corpus throughput V1 (proposal)

Status: **T0–T3 implemented** on `perf/t0`…`perf/t3-derivative-cache`, byte-identical
on the five-bundle sample (bench sum 3940 → 1914 s). T4 was implemented but is not
proposed for merge (about −2%). T5 is deferred. Written by the corpus-laptop
session (laptop copy `~/pi-data/probe/REPLAY_THROUGHPUT_V1.md`, sha256 `4ad7e966…196a`).

All numbers come from probes on the corpus laptop, 2026-10-13: Ryzen 9 5900HX
(8C/16T), 27 GB, WSL2, max processor state 99% (no boost). Probes ran in a separate
clone (`~/pi-data/probe/repo`, branches `probe/*` on 7823586) with their own images,
results and ledger. Evidence trees (results, ledgers, canonical store) were
read-only throughout.

## 1. Problem

The overnight run (2 lanes, 92 bundles done) finished 5.57 bundles/h, i.e. about
21 min per bundle per lane (median 1284 s).

- About 85% is the bench: median 1046 s, about 4–6k cuts/s per lane.
- About 15% is pre-bench: median about 200 s, mostly materialize.
- The full list of 1212 bundles would take about 9 days.

## 2. Where the time goes (measured)

### 2.1 Bench, original code, bundle_16d5b47b (2.22M cuts), single lane, 199 s

- No process is CPU-bound: profile 56%, publisher 55%, other groups 44–46%, Redis
  73% (86–90% in steady state).
- py-spy, all 6 groups:
  - about 68% of wall waiting on Redis (check/ack Lua 40%, XREADGROUP 28%);
  - 15% in pure-Python RESP (hiredis isn't installed);
  - about 5% json.loads;
  - 1–7% in strategy hooks.
- Every poll runs check, which is 1 + 6 XINFO calls. Each 128-entry batch costs
  XACKDEL ACKED about 650 µs plus XPENDING/HMGET.
- Bytes are freed only after all 6 groups ack, so everything runs in lockstep.
- Single-group runs: profile-only 150 s, complement-only 117 s.

### 2.2 About 97% of cuts are empty

- A read-only XRANGE sample: 2,348 of 2,400 cuts have `origin.kind=group` and no
  market_events, book_transitions or control_events. They are deliveries touching
  only unplanned instruments.
- The profile output agrees: about 93k level rows over 2.22M cuts. On 04ef, 324k of
  3.59M cuts are non-empty.

### 2.3 Publisher, once empty cuts are gone (perf, self time)

- about 30% serde input parse;
- about 31% libc malloc/free/memcpy;
- about 10% output encode (Value tree + exact());
- about 6% zstd;
- about 5% hex/hash;
- about 6% materialize reader/verify;
- about 6% Risk itself.

It fully parses the firehose: 9 GB of logical JSON for 22 books on 16d5. 04ef ends
with about 55 s that produce no cuts. Other findings:

- Default release profile: no LTO, cgu 16, generic CPU.
- On a FULL queue the publisher resends its whole buffered batch on every retry;
  Redis saw about 6× the real stream bytes on 04ef at batch 1024.
- The publisher's `queue_full_waits` stderr line is discarded.

### 2.4 Strategy hooks, cross-heavy 04ef after T1

Cross is the slowest group (the queue stays full). py-spy:

- about 40% is `Closed.identity`: canonical JSON tree + SHA-256 of every fee
  assessment result on every evaluation.
  - 27% at assess_net (contract.py:124, assessment_ids).
  - about 11% in `FeeEngine.assess_many`, which computes fill.identity twice per
    fill (engine.py:507,512).
- 12% is the Fraction/Decimal fee arithmetic.

assessment_ids only reach output when an episode row is written: 296 rows, against
millions of evaluations.

### 2.5 Pre-bench

- 16d5: materialize 70 s, prepare 8.5 s.
- 04ef: materialize 98 s, prepare 18 + 7.7 s. The first prepare returned unavailable
  outcomes, followed by the 60 s retry sleep.
- Resolve/fees/preflight are about 2 s.
- Canonical data is decoded and verified twice (Python restore, then Rust audit), and
  derivatives are hashed 3–4×. Everything is deleted per bundle.
- 1151 window uses but 163 distinct derivatives (7.1× reuse); keeping the last 5
  bundles hits 72%.

### 2.6 Lanes

- Original code: 2 concurrent benches gave 1.52× one lane, with about 30% more
  CPU-seconds per process from SMT sharing.
- Optimised config: 4 concurrent 04ef benches took 486–489 s each, i.e. 122 s/bundle,
  2.3× one optimised lane. About 5 busy cores, peak 78 °C, no throttling; not
  saturated.

## 3. Measured results

Single lane, bench only, from a kept work dir.

- **equal:** semantic_sha256 equal to the night result.
- **masked:** all files identical, or identical row by row with only the
  sequence-derived `cut` and `assessment_ids` masked.

| Variant | 16d5 (2.22M) | 04ef (3.59M) | outputs |
|---|---|---|---|
| original 7823586 | 199 s (782 CPU-s) | 474 s (1592 CPU-s) | — |
| + hiredis + orjson decoder | 173 s | — | equal |
| + consumer COUNT 1024 | 166 s | — | equal |
| + publish_batch_entries 1024 | 156 s | — | equal |
| + fat LTO, cgu=1, target-cpu=native | 150 s (464 CPU-s) | 410 s (1141 CPU-s) | equal |
| + omit empty group cuts (probe wire) | 81 s (134 CPU-s) | 293 s (625 CPU-s) | masked |
| + fee fill.identity once | — | 282 s (607 CPU-s) | identical to previous |
| + lazy assessment_ids (upper bound, placeholder) | — | 236 s (551 CPU-s) | masked |
| optimised, 4 concurrent lanes | — | 487 s each → 122 s/bundle | masked |

No gain: check every 16 polls; a 512 MiB queue.

## 4. Proposed changes

Acceptance throughout is byte identity: all group outputs equal the night results,
unmasked, on a sample of at least 5 bundles (light, cross-heavy, large, many-window,
Polymarket-heavy). A new corpus key is accepted; strategy semantics are unchanged.

### T0. Config and build (no wire or semantic change)

1. hiredis + orjson in the runner image (orjson for cut records only; the strict
   decoder stays for initial and configs).
2. Consumer batch_entries 1024, with batch_bytes raised to match.
3. publish_batch_entries 1024, and fix the resend-on-FULL: append returns the
   remaining capacity, and the publisher sends only what fits or waits for progress.
4. Release profile:
   - fat LTO, cgu 1;
   - evaluate target-cpu (native only if build host = run host; otherwise
     x86-64-v3) and mimalloc;
   - keep panic=unwind;
   - derivative bytes unchanged.
5. Persist the publisher stats line in the attempt directory.
6. 4 lanes (evaluate 6), thermal guard kept at 97.5 °C, stop-at-boundary shutdown.

Expected: bench −25%, bench CPU −30–40%.

### T1. Wire v3: omit empty cuts (largest lever)

- Omit a cut iff `origin.kind=="group"` and it has no market_events,
  book_transitions or control_events.
- Window cuts are always kept. CutClock needs them; omitting them fails with "group
  window", which the probe confirmed.
- Each published cut carries `body.risk_sequence`. Strategies use it for profile
  `cut` rows and the fee order_key, so outputs stay byte-identical.
- The terminal carries `risk_cuts`, so omitted = risk_cuts − cuts is provable.
- Wire sequence contiguity is unchanged. Needs a REPLAY_STREAMS_V1 amendment.
- Probe: 39× fewer cuts on 16d5, 11× on 04ef. Masked-equal everywhere. Profile
  summary, availability, incidents and pair files were byte-identical even unmasked.

Expected: bench −45–60%, CPU −50–80%.

### T2. Strategy hot spots (byte-identical)

1. fill.identity once in assess_many: −4% wall, identical bytes.
2. Lazy assessment_ids (compute only when an episode row is written), or memoise
   canonical sub-trees of long-lived frozen fee objects. Upper bound −16% wall on
   cross-heavy bundles. Must be the same SHA-256 of the same canonical bytes.
3. Profile group: drop the per-transition plain() deep copy of as_of, and share one
   sorted read per side. Est. 1.3–1.5× on its hook.
4. Book.levels: keep incrementally sorted ladders. books_sha256 guards correctness.

### T3. Materialize cache (pre-bench −60–80%)

- A persistent local writable derivative layer, separate from the canonical store.
- Content-addressed by derivative_address + receipt_sha256 + producer, verified on
  use by the existing jobs cache code (`replay/jobs/bundle.py` `_cache_decision`).
- Both verifications kept, but run once per distinct window (163 instead of 1151).
- LRU of about 5–6 GB; optionally stage bundle N+1 during bundle N's bench.
- Separately, revisit the 60 s × 3 outcomes retry sleep.

### T4. Risk input early filter (publisher about 2×; conditional)

- Header-only handling for unplanned keys: address and key read, counted,
  order-checked; the body is not deserialized.
- Check the risk crate's continuity rules first.
- Acceptance: identical outputs and books_sha256, plus a malformed-unplanned-header
  test.
- Targets most of the parse/alloc share and the about 55 s no-output tail.

### T5. Committed cut-stream cache (user decision)

- Content-addressed, receipt written last.
- Key: pins, plans, normalizer versions, Risk/publisher build and wire version.
- Acceptance: identical books_sha256 and outputs vs a live run.
- Size: non-empty cuts average about 4.3 KB raw, about 34× at zstd-3. 04ef about 1.4
  GB raw → about 40 MB; 16d5 a few MB; likely tens of GB for the corpus.
- Strategy re-runs become seconds to tens of seconds per bundle.

## 5. Projection (to validate on the 5-bundle sample)

- T0 + T1 + T2.1: bench about 0.45–0.6× of baseline, bench CPU about 0.2–0.4×.
- T3: pre-bench about 30–60 s.
- 4–6 lanes: about 2–3× more throughput.
- Corpus: about 15–25 bundles/h vs 5.57 (3–4.5×); 1080 non-soccer bundles in about 2–3
  days instead of about 9.
- T2.2 and T4: a further about 1.3–1.6×.
- T5: strategy-only reruns in hours.

## 6. Rejected or deferred

- **GPU (RTX 3060 present): rejected.** The work is sequential, branchy per-cut book
  updates and Python hooks; transfer and launch cost more than the work. zstd and SHA
  run faster than the data rate on the CPU, and GPU zstd risks byte identity.
- **Single-process multiplexed consumer: deferred.** One reader took 106 s vs 150 s
  with six, but after T1 the consumers are 10–25% busy, so the gain is small and
  isolation is lost.
- **check-every-N and a bigger queue:** no gain.
- **3.13 JIT, free-threading, vectorising:** about 0 or harmful.
- **PyPy: untested.** It loses hiredis/orjson and has a pure-Python Decimal; at most
  one trial after T2.

## 7. Validation plan

1. Sample: 16d5b47b, 04ef1fb8, a 10M+ cut bundle (e.g. 0a7e7d83), a many-window
   bundle, a Polymarket-heavy bundle.
2. T0 → T1 → T2.1 on branches. Each must be byte-identical, unmasked, vs
   `results/6dd9f00ff1cce627`.
3. T3 cold and warm: identical derivative receipts.
4. Lanes 4 vs 6: throughput, temperature, memory.
5. Then start a new corpus key.

## Appendix: probe artefacts (laptop, `~/pi-data/probe`)

- repo: branches probe/timing (3ba5e15, stage log + kept work dir), knobs commits,
  probe/lto, probe/skip-empty. Probe hacks, not implementations.
- Images: `probe-replay:{knobs,lto,skip,fee}[-fast]`; tools `probe-pyspy:local`,
  `probe-perf:local`.
- Kept work dirs: `kept/bundle_16d5…`, `kept/bundle_04ef…`. Bench outputs:
  `bench-out/<tag>`.
- Samplers: `cpu-*.log`, `redis-*.log`, `queue-*.log`, `freq-*.log`.
- Profiles: `pyspy/out/*/<group>.txt`, `perf/out/perf.data`.
- Comparators: `semcheck.py`, `outcmp.py`.
