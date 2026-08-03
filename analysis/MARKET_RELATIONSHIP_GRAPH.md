# Market Relationship Graph — Design & Validation Plan

Scope: how to model resolution rules, how to derive and discover relationships between
markets, what signals to extract from price and L2 data, and the gates at which this
project should be killed.

Non-goals for v1: execution, custody, routing, macro/cross-domain inference.

**Revision note:** an earlier version of this doc claimed historical L2 does not exist and
must be self-recorded. That was wrong — see §4. It changes the sequencing but not the
design.

---

## 1. Why resolution modelling is the foundation

A market is not its title. A market is a tuple:

```
(resolution_source, predicate, measurement_window, void_policy, dispute_mechanism)
```

Two markets are the *same bet* only if all five match. Two markets with identical titles
and different sources are not fungible, and treating them as fungible converts a locked
position into a directional one silently. Every failure mode in this system routes back
through this.

So the graph is built on **Conditions**, not on venue Markets.

---

## 2. Core data model

```
Event          — a real-world happening. "EWC 2026 CS2, Team A vs Team B, 2026-08-12"
  |
Condition      — a predicate over that Event. Venue-independent. Canonical.
  |            "Team A wins the series"
Market         — one venue's tradeable instrument implementing a Condition.
                 (venue, market_id, token_ids, ResolutionSpec, book)
```

Edges live **between Conditions**. Markets attach to Conditions via a fungibility test.
This separation is the single most important design decision: it splits *"is this the
same bet"* (semantic, hard, slow-changing) from *"where can I trade it"* (operational,
fast-changing).

### ResolutionSpec

```
source_authority     enum { UMA_OO, KALSHI_SETTLEMENT, VENUE_INTERNAL, OFFICIAL_FEED }
source_identifier    URL / feed name / oracle question id
predicate            structured, not prose (see §3)
measurement_window   (start_ts, end_ts, tz)
threshold_rules      rounding, tie handling, inclusive/exclusive bounds
void_policy          what happens on cancellation, postponement, abandonment
early_close_policy   can it settle before window end
dispute_mechanism    UMA challenge period / exchange discretion / none
raw_text_hash        sha256 of the verbatim rules as published
raw_text_snapshot    immutable, stored
```

### Fungibility test

Two Markets attach to the same Condition only if:

| Dimension | Must match | If mismatched |
|---|---|---|
| predicate | exactly | different Condition |
| source_authority + identifier | exactly | `NEAR_IDENTITY`, flag `source_divergence` |
| measurement_window | exactly | `NEAR_IDENTITY`, flag `timing_divergence` |
| void_policy | exactly | `NEAR_IDENTITY`, flag `void_divergence` |

Only `IDENTITY` markets may be treated as substitutable in routing or as legs of a locked
basket. `NEAR_IDENTITY` is display-only, with the divergence dimension shown to the user.

### Rule drift detection

Resolution text gets edited and clarified after listing. Hash the raw text on every poll;
on change, diff, re-run the fungibility test, and demote the edge if it no longer holds.
Any position open against a demoted edge raises an alert. A clarification issued mid-event
can unlink two markets you were treating as identical.

---

## 3. Deriving relationships symbolically (the ground truth generator)

Do not learn relationships you can compute. For any event with a known combinatorial
structure, enumerate the outcome space and represent every market as a bitmask.

**Algorithm**

1. Enumerate all reachable terminal outcomes of the Event from its current state.
   Call this set `Ω`, with `|Ω| = n`.
2. For each Condition, compute `mask ⊆ Ω` — the subset of terminal outcomes for which
   it resolves YES.
3. All relationships are then set operations:

```
mask_A == mask_B                     -> IDENTITY          P(B|A)=1, P(B|¬A)=0
mask_A ⊂ mask_B                      -> IMPLICATION       P(B|A)=1
mask_A ∩ mask_B == ∅                 -> MUTUAL_EXCLUSION
⋃ masks == Ω and pairwise disjoint   -> PARTITION
otherwise                            -> OVERLAP (needs joint estimation)
```

**Arbitrage condition, fully general.** For any set of markets whose masks form a
partition of `Ω`:

```
Σ ask_i < 1 - fees   ->  locked long basket
Σ bid_i > 1 + fees   ->  locked short basket
```

This subsumes the YES+NO CTF basket as the trivial 2-element partition, and extends to
n-way structures without new logic. It is also the basis of the first experiment (§6).

**Worked example — Bo3 at state (A=1, B=0):**

```
Ω = { A2-0, A2-1, B2-1 }

"A wins series"        -> {A2-0, A2-1}
"B wins series"        -> {B2-1}
"Under 2.5 maps"       -> {A2-0}
"Over 2.5 maps"        -> {A2-1, B2-1}
"A wins map 2"         -> {A2-0}
"B wins map 2"         -> {A2-1, B2-1}
"Correct score A 2-0"  -> {A2-0}

Derived:
  Under2.5 == A-wins-map-2 == CorrectScore-A-2-0     IDENTITY (3-way)
  Over2.5  == B-wins-map-2                            IDENTITY
  B-wins-series ⊂ Over2.5                             IMPLICATION
  {A-wins-series, B-wins-series}                      PARTITION
  {Under2.5, Over2.5}                                 PARTITION
```

Note the state dependence: at (0,0) none of the identities above hold. Masks must be
recomputed on every state transition, which means the live-event feed is part of the
graph, not just a data source.

**Worked example — tournament progression.** This structure is more valuable than
match-level because it is guaranteed to exist and it generates edges for free:

```
Ω = { each team wins the tournament }        (one terminal outcome per team)

"Team X wins tournament"   -> {X}
"Team X reaches final"     -> {X} ∪ {teams X could lose the final to}
"Team X advances group"    -> superset of the above

Derived:
  wins ⊂ reaches-final ⊂ advances-group        IMPLICATION chain, free
  { wins_X : X ∈ teams }                       PARTITION over the whole field
```

**This is also your labelled dataset.** Every symbolic IDENTITY / IMPLICATION / PARTITION
is a known-positive for validating the statistical discovery pipeline in §5. Nothing else
in this system gives you clean labels.

---

## 4. Data: what to rent and what to record

### 4.1 Historical data exists — use it

Oddpool serves full-depth orderbook snapshots, top-of-book timeseries and trade tapes for
Kalshi and Polymarket, back to **March 2026**, at **1-minute or 5-minute granularity**,
cursor-paginated. This is the fastest path to Gates 1–3 and there is no reason to wait on
your own recorder for those.

### 4.2 The granularity ceiling determines what it can and cannot answer

| Analysis | Needs | Oddpool 1m? |
|---|---|---|
| Logit beta / co-jump classification | minute-scale | **yes** |
| Partition sum test (§6) | minute-scale | **yes** |
| Size-adjusted VWAP divergence (Gate 3) | full depth | **yes** |
| Coverage / breadth counts (Gate 5) | any | **yes** |
| Lead-lag — which venue moves first | sub-second | no |
| Synchronised quote withdrawal (§5.4) | sub-second | no |
| Divergence persistence (Gate 4) | sub-second | no |
| Maker address microstructure (§5.5) | on-chain, not Oddpool | no |

So: **rent for the kill gates, record for the microstructure.** Run both in parallel.
Do not sequence the recorder behind the analysis, and do not sequence the analysis behind
the recorder.

**Caveat: the 1m/5m ceiling is a tier limit, not a product limit.** Oddpool's institutional
offering advertises every orderbook delta, every trade, every snapshot, at tick level with
millisecond timestamps, delivered as Parquet / S3 dumps. If a scoped historical slab is
affordable, it removes the need to self-record for Gate 4, lead-lag and §5.4 entirely —
and it back-fills the period before your recorder existed, which self-recording never can.
Get a quote before committing engineering time to the recorder. Two things to confirm on
that call: their archive only covers markets they were already subscribed to (coverage of
thin esports books is not guaranteed), and history begins around March 2026.

**Vendor dependency.** Historical orderbook data here comes from whoever happened to be
capturing it — neither exchange backfills. That makes any single archive a single point of
failure; Polymarket's own undocumented `/orderbook-history` endpoint went dark during the
Dome/Predexon transition and broke downstream loaders. Put the puller behind a source
interface from day one, and know the alternatives (Predexon, FinFeedAPI) before you need
them.

### 4.3 Request budget

At 1m granularity, one market-month is ~43,200 snapshots. At `limit=100` that is ~432
requests. So:

```
200 markets × 3 months ≈ 260K requests
```

Premium ($100/mo) allows 5M requests/month at 25 req/sec, so the above pulls in roughly
three hours of wall clock and uses ~5% of the monthly budget. The free tier (1K req/month)
is not usable for anything beyond a smoke test. Budget for Premium from day one.

### 4.4 Self-recorded capture

Per market, from the venue websockets directly:

```
book_snapshot     full L2 both sides, on connect and on periodic resync
book_delta        every level change: (ts_venue, ts_local, side, price, size, seq)
trade             (ts, price, size, side, maker_addr, taker_addr)   [Polymarket]
market_meta       rules text + hash, on every poll
event_state       live score / series state, from an independent feed
```

Record `ts_venue` and `ts_local` separately and keep the skew series. Cross-venue lead-lag
is meaningless without a clock model.

**Timing note:** EWC 2026 runs 6 July – 23 August 2026. The first three weeks are already
in Oddpool history; the remaining weeks can be self-recorded at full resolution. It is the
one dataset where you can have both, and that window closes on 23 August.

### 4.5 Storage

NDJSON append-only to disk for raw capture, partitioned by `date/venue/market_id`,
gzip-compressed. Parquet for the analysis layer. Do not put raw ticks in Postgres.

For the Oddpool puller specifically: checkpoint the pagination cursor per market to disk
so a crashed pull resumes rather than restarts. Write NDJSON with one JSON object per
snapshot including the request parameters that produced it — provenance matters when you
later find an anomaly and need to know whether it was the data or the market.

---

## 5. Feature extraction

### 5.1 Work in log-odds, on changes

```
z = ln(p / (1-p))
```

Raw prices are bounded and compressed near the edges: a 2c move at 0.50 and at 0.95 carry
completely different information. Logit linearises this. Then difference — correlate
`Δz`, never `z` levels.

**Excluded region.** Drop observations where `p < 0.05` or `p > 0.95`, and drop the final
segment of market life. Near resolution every market converges to its terminal value, so
level correlation there is mechanically near 1 regardless of any real relationship. **Do
not use terminal-region movement for discovery.** Terminal behaviour is a *validation*
signal only — see §7.

### 5.2 Inflection-conditioned co-movement (primary signal)

Full-series correlation is dominated by stale quotes and idle periods. Condition on
information arrival instead.

```
jump_t  = |Δz_t| > k · σ_rolling         (k ≈ 3, σ over a trailing window)

co_jump_rate(A,B) = P(jump in B within w | jump in A)
logit_beta(A,B)   = OLS slope of Δz_B on Δz_A, restricted to jump events
lead_lag(A,B)     = argmax_τ crosscorr(Δz_A, Δz_B(t+τ))       [needs self-recorded data]
```

`logit_beta` is the diagnostic worth building around:

- IDENTITY pairs should show `beta ≈ 1.0` with tight residuals
- IMPLICATION pairs show `|beta| < 1` and **asymmetric** — beta(A→B) ≠ beta(B→A)
- Spurious / common-news pairs show high co-jump rate but unstable, regime-dependent beta

The asymmetry of beta is what separates a logical link from a shared news clock. Note that
`w` must be set to the data granularity: at 1m, `w = 1 bar`, and lead-lag is unavailable.

### 5.3 Common-factor residualisation

The dominant source of co-movement will be shared news arrival, not structure. Two Fed
markets both move at 08:30. Extract a category-level factor (first PC of `Δz` across all
markets in the category, or a simple equal-weight mean) and correlate the **residuals**.
Without this the graph will just rediscover "these markets are in the same category."

### 5.4 L2 book features

You need L2 anyway for size-aware pricing, and it fixes the staleness problem: thin markets
rarely trade but quote constantly, so quote-derived features give far more observations
than trade-derived ones. Oddpool's full-depth snapshots cover the pricing use; the
withdrawal signature below needs self-recorded data.

```
mid, microprice (depth-weighted), spread
top_of_book_size both sides
cumulative_depth at k ticks, k ∈ {1,2,5,10}
depth_delta over bucket
cancel_burst   count of level removals in bucket        [self-recorded only]
quote_withdrawal_flag   spread widened > X and depth dropped > Y%
```

**The signal to hunt: synchronised quote withdrawal.** When depth is pulled from two books
inside the same short window, that is evidence of a shared risk engine — and unlike price
co-movement, it is not driven by public news in the same way. It reflects the maker's
internal inventory state, which makes it a cleaner structural fingerprint than correlation.
At 1m granularity this is invisible, so it is a self-recorder deliverable.

### 5.5 Maker address graph (Polymarket only)

Fills settle on-chain with maker and taker addresses exposed, so the shared-maker
hypothesis can be observed directly rather than inferred from price.

```
maker_overlap(A,B) = Jaccard( top_makers(A), top_makers(B) )   weighted by filled notional
```

*Verify the current CTFExchange fill event schema and field names against the deployed
contract before building the indexer — do not assume the ABI from memory.*

**Critical caveat: shared makers are a confounder, not a relationship.** A maker quotes
everything liquid, so two logically unrelated markets can co-move purely from shared
inventory management. `maker_overlap` is a **control variable**. Measure whether residual
co-movement survives after conditioning on it; relationships that vanish once you control
for shared makers were never logical relationships.

Cross-venue this does not work — Kalshi exposes no identities. There you fall back to
timing signatures: quote update cadence, tick alignment, cancel/replace periodicity.

---

## 6. First experiment: the partition sum test

Run this before anything else. It requires no modelling, no ground-truth labels, and no
self-recorded data.

**Method.** For every market set whose masks form a partition of `Ω`, compute over the
full price history:

```
S_ask(t) = Σ best_ask_i(t)        should be ≥ 1
S_bid(t) = Σ best_bid_i(t)        should be ≤ 1
```

Then the version that actually matters — recompute both using **VWAP at a realistic
ticket size** rather than best price, walking the full depth snapshot. Report:

- distribution of `1 - S_ask` and `S_bid - 1`, at top-of-book and at size
- fraction of observation-minutes where the size-adjusted gap exceeds fees
- how that fraction varies by market category, liquidity decile, and time-to-resolution

**Three datasets, in parallel:**

| Dataset | Why | Status |
|---|---|---|
| FIFA World Cup 2026 | complete resolved lifecycle, deep coverage, high liquidity | resolved 19 July, full history available |
| EWC 2026 title winners | live, esports, thin books — the stress case | running to 23 Aug, partly recordable |
| Kalshi Fed decision buckets | explicit partitions, high liquidity, no sports feed needed | ongoing |

If the size-adjusted gap is negligible across all three, the economic case is dead and you
have spent about a week and $100 finding out. That is the point.

**Market selection criterion, generally.** The binding constraint is *sibling market
density*, not liquidity. You need multiple markets per event with computable relationships.
Verify sibling coverage before committing to any dataset — esports coverage on Polymarket
is frequently series-moneyline-only, with no map winners, correct score or O/U maps. If
the siblings are not listed, there is no graph to test and match-level esports is the wrong
starting point regardless of how interesting the tournament is. Tournament progression
markets are the safe default because the nesting structure exists by construction.

---

## 7. Inference pipeline

```
[1] Symbolic derivation        §3    -> IDENTITY / IMPLICATION / PARTITION edges
                                        confidence = 1.0, machine-checkable
[2] Candidate generation       §5    -> ranked pairs by residual co-jump + beta stability
[3] Classification                   -> LLM or human reads both rules texts, assigns
                                        relationship type, or rejects
[4] Fungibility check          §2    -> IDENTITY vs NEAR_IDENTITY, divergence flags
[5] Post-resolution validation       -> did the edge hold at settlement?
```

**Trust tiers, and what each is allowed to do:**

| Tier | Source | Allowed action |
|---|---|---|
| `PROVEN` | symbolic derivation, rules verified | auto-surface as substitutable; locked-basket eligible |
| `CONFIRMED` | statistical candidate, rules manually verified | surface with divergence flags |
| `ESTIMATED` | statistical only, joint probability modelled | display with explicit uncertainty; never as "same bet" |
| `CANDIDATE` | unclassified | internal only, never shown |

Do not let `ESTIMATED` edges into user-facing "better odds here" claims. That is the path
where you route users into longshots with a confident UI.

**Post-resolution validation is your only real accuracy metric.** Every IDENTITY edge makes
a falsifiable prediction: both markets resolve the same way. Log every resolution pair,
compute edge-level precision over time, and demote edge types whose realised precision
drops below threshold. This is the correct use of terminal data.

---

## 8. Exit gates

Set the numbers yourself before you look at the data; the point is to commit in advance.

**Gate 0a — Historical pull (days).**
Oddpool puller + NDJSON sink running, resumable, covering the three §6 datasets.
*Pass: full pull completes and reconciles against Oddpool's own volume figures.*

**Gate 0b — Tick-level microstructure access (parallel, weeks 1–2).**
Either own WS capture of L2 + rules + event state with clock skew measured, or a scoped
institutional-tier historical slab (§4.2). Price the rent option first — it back-fills
history a recorder cannot. Needed only for Gate 4 and §5.4/5.5; does not block Gates 1–3.

**Gate 1 — Economic significance (partition sum test, §6).**
Promoted to first because it is the cheapest and it kills hardest. What fraction of
size-adjusted observations show a gap exceeding fees at a realistic ticket? Below your
threshold across all three datasets, stop.

**Gate 2 — Recall against ground truth.**
Do the symbolically-known IDENTITY pairs rank in the top decile by the §5 metric? If the
pipeline cannot find relationships you already know exist, it will not find the ones you
don't. **Do not tune your way past this gate.**

**Gate 3 — Discovery precision.**
Of the top N non-obvious candidates, what fraction survive manual rules inspection? Below
threshold, the statistical layer is noise and the product reduces to the symbolic
generator — still a product, a much smaller one.

**Gate 4 — Persistence.** *(requires Gate 0b)*
Median lifetime of a qualifying divergence. Shorter than your end-to-end latency including
the user's decision time means there is no retail product here regardless of graph quality.

**Gate 5 — Breadth.**
Actionable events per week. Determines whether this is a company, a side income, or a blog
post. Compute after Gates 1 and 4.

---

## 9. Implementation stack

**Python for everything in this doc.** Specifically:

| Component | Language | Reason |
|---|---|---|
| Oddpool puller + NDJSON sink | Python (`httpx` + `asyncio`) | I/O bound, cursor pagination, throwaway-fast to write |
| Analysis: logit, jumps, betas, factor model | Python (`polars`, `numpy`, `statsmodels`) | not a close call |
| Mask engine / outcome enumeration | Python | small combinatorics, correctness over speed |
| Live WS recorder | Python first | message rates here are tiny vs crypto venues |
| On-chain maker indexer | Python or Rust | either; Rust only if you already have the alloy plumbing |
| Execution path (later) | Rust | latency-sensitive, as previously planned |

The one honest caveat on the live recorder: if you want sub-millisecond local timestamp
fidelity for lead-lag, GC pauses will contaminate `ts_local`. That is a real Rust argument
— but it is a *later* argument. Prediction market book update rates are low enough that
Python asyncio will keep up for v1, and lead-lag is a Gate 4 concern, not a Gate 1 one.
Rewriting the recorder in Rust before Gate 1 clears is gold-plating on a project that
might not survive Gate 1.

Use `polars` over `pandas` — the NDJSON → Parquet path is cleaner and the lazy API handles
the multi-market joins without materialising everything.

---

## 10. Build order

1. Oddpool puller + NDJSON sink + cursor checkpointing. **(Gate 0a)**
2. Outcome-space enumerator + mask engine for tournament progression, then Bo3/Bo5.
3. Partition sum test with VWAP-at-size. **(Gate 1 — stop and decide.)**
4. Rules parser + fungibility test + drift detection.
5. Feature pipeline: logit transform, jump detection, residualisation, book features.
6. Gate 2 evaluation against the symbolic labels.
7. Live WS recorder, running in parallel from step 1 onward. **(Gate 0b)**
8. Maker address indexer as a control variable.
9. Statistical candidate generation + classification loop. **(Gate 3)**
10. Persistence and breadth measurement. **(Gates 4–5)**

---

## 11. Competitive note

Oddpool is YC-backed and already ships cross-venue normalization, matched-market search
across 700K+ markets on both venues, whale tracking, and an arbitrage endpoint returning
net profit after fees. The normalization layer is therefore a commodity at $100/month.

Two consequences:

1. **Do not build normalization.** Rent it. Your engineering goes into the mask engine and
   the relationship graph.
2. **Cross-venue identity matching cannot be the moat.** The logically-implied relationship
   graph — markets that are the same bet without being the same contract — is the part
   they do not do, and it has to carry the differentiation on its own.

Also note their coverage is Kalshi and Polymarket only. Any third venue is your problem.

---

## 12. Open questions

1. Does Oddpool's historical endpoint expose full depth ladders or only a truncated top-N?
   Determines whether VWAP-at-size is computable from rented data or needs the recorder.
2. Does sibling market coverage (map winners, correct score, O/U maps) actually exist for
   EWC titles on either venue? Verify before committing to esports at all.
3. Does either venue serve historical rules-text versions, or is drift detection
   record-forward-only?
4. WS market-count limit per connection and connection limits — drives coverage breadth
   and therefore Gate 5.
5. Are esports event-state feeds available at low enough latency and cost to keep masks
   fresh mid-match? If state lags the market, the symbolic layer is unusable live.
6. Exact CTFExchange fill event schema on the current deployment.
7. Does Kalshi's API expose full L2 or only top-of-book? Affects size-aware pricing
   cross-venue.