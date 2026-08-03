# CAPTURE_SPEC.md — multi-venue recording and replay

**Purpose.** Record enough market data, at high enough fidelity, that every analysis
question we have now or invent later can be answered by replay rather than by re-collection.

**The governing principle.** Capture decisions are irreversible; analysis decisions are not.
Get the capture contract right and everything downstream is a code change. Get it wrong —
snapshots instead of deltas, missing timestamps, no provenance — and no amount of later work
recovers it.

**Corollary that must not be violated:** never gate capture on an economic threshold.
Thresholds are analysis. Capture everything in scope; threshold only what you *alert* on.

---

## 1. Venues

The honest answer to "is there a third venue comparable to Polymarket and Kalshi" is no —
nothing is close on volume. But volume is the wrong criterion for this exercise. What we
need from a third venue is **overlapping instruments with independent order flow**, so that
the same Ω is priced by a different set of participants.

| Venue | Access | Why include | Risk |
|---|---|---|---|
| **Polymarket** | free data, WSS + on-chain fills, no KYC for data | volume leader; on-chain maker addresses; dense multi-condition structure | none for capture |
| **Kalshi** | account + RSA-PSS keys, 30-min session tokens | regulated flow, different participant base, explicit partition markets (Fed buckets) | account access is the real constraint |
| **Limitless** | REST + WSS, Base, no KYC | **the recommended third**: $1B+ traded, nonstop hourly and daily crypto and stock price markets — exactly the ladder structure we're targeting, with independent flow | no public contract audit found; small protocol; treat as data source before capital |
| **Opinion** | CLOB API, `opinion-clob-sdk`, BNB Chain | third-largest by volume, macro and crypto focus | evaluate after Limitless|

Limitless is the pick because its instrument class overlaps our priority markets almost
exactly — short-dated crypto price questions and sports combination markets — rather than because of its size. It also
lives on an EVM chain, so wallet tooling transfers.

Deliberately excluded: Manifold, PredictIt, PRED and many others in nascent stages

---

## 2. Market classes, in priority order

| Class | Structure | Why | Mask source |
|---|---|---|---|
| **P0 — crypto price ladders** | monotone implication chain + adjacent-strike partitions | masks derivable from strikes, so they **cannot be wrong**; liquid; 24/7; no event feed; all three venues list it | deterministic |
| **P1 — daily team sports** | moneyline / spread / totals siblings | thousands of resolved events; cross-venue identity tests | deterministic given event schema |
| **P2 — macro decision buckets** | explicit n-way partition | Fed, CPI, payrolls: high liquidity, scheduled repricing, clean partitions | deterministic |
| **P3 — tournament progression** | nested implication + field partition | free implication edges; long-lived | deterministic given bracket |
| **P4 — esports series** | Bo2/Bo3/Bo5 state machines | richest logical structure, thinnest books — kept for structure, not for edge | deterministic given format + live state |

P0 is the control condition for the whole thesis: it is the only class where a positive
finding cannot be a mask error and a negative finding cannot be blamed on illiquidity.

Note on Bo2 (group stages): the outcome space is `{2-0, 1-1, 0-2}` — a genuine three-way
partition rather than a binary series winner, and the draw leg is historically the most
mispriced. Structurally more interesting than Bo3; worth capturing wherever listed.

---

## 3. The capture manifest

Adding a market must be a config change, never a code change. One declarative manifest
drives discovery, subscription, and Ω construction.

```yaml
- id: btc_daily_ladder
  structure_type: LADDER          # LADDER | PARTITION | SERIES_BO_N | BRACKET | BINARY
  venues: [polymarket, kalshi, limitless]
  selector:
    polymarket: { tag: crypto, slug_pattern: "bitcoin-above-*", min_liquidity: 5000 }
    kalshi:     { series_ticker: KXBTCD }
    limitless:  { category: crypto, horizon: [hourly, daily] }
  omega:
    generator: price_buckets
    params: { underlying: BTC, reference_feed: binance_spot }
  capture:
    depth_levels: full
    snapshot_interval_s: 300      # resync only; deltas carry the truth
    include_trades: true
    include_chain_fills: true     # polymarket only
  lifecycle:
    discover_every_s: 60
    retain_after_resolution_h: 24 # keep recording through settlement
```

`structure_type` selects the Ω generator. `selector` is per-venue because market
identification differs everywhere. `discover_every_s` matters more than it looks — short-
dated crypto markets are created continuously, and a discovery loop that runs hourly will
miss most of their life.

**Ω generators required for v1:**

- `price_buckets` — from strike ladder; monotonicity is structural
- `explicit_partition` — from the venue's own mutually-exclusive condition set
- `series_state_machine(format)` — enumerate map/game sequences, condition on live state
- `bracket(seed_structure)` — enumerate reachable finalists

Every generator must also emit an `OTHER` outcome and a per-instrument payoff under it,
derived from `void_policy`. A basket whose legs disagree under `OTHER` is not locked — the
solver needs that row.

---

## 4. Capture contract

### 4.1 Deltas are the truth, snapshots are for resync

Store the incremental update stream with sequence numbers, plus periodic full snapshots
used only to resynchronise. **Snapshot-only capture is lossy at every interval and cannot
be repaired later.** Kalshi's own feed is designed this way — initial `orderbook_snapshot`
then incremental `orderbook_delta`, applied in sequence — and Polymarket and Limitless
should be normalised into the same shape.

### 4.2 Every record carries four timestamps and a sequence number

```
ts_venue     venue-assigned event time (may be absent or coarse)
ts_exchange  exchange sequencing time where distinct from ts_venue
ts_recv      when our socket received the frame
ts_write     when we durably wrote it
seq          venue sequence number for the stream
```

Lead-lag, staleness stratification, and replay fidelity are all impossible without these.
Persist the clock-skew series per venue session separately.

### 4.3 Streams recorded

```
book_delta        (venue, market_id, seq, side, price, size, ts_*)
book_snapshot     full ladder both sides, on connect and every snapshot_interval_s
trade             (price, size, taker_side, trade_id, ts_*)
chain_fill        Polymarket only: OrderFilled / PositionSplit / PositionsMerge with
                  maker + taker addresses
market_meta       full record + sha256 of resolution text, on every discovery poll
event_state       live score / series state / underlying reference price
detector_output   our own detections, written alongside the input that produced them
resolution        final settlement per condition, with source and timestamp
```

`detector_output` is not optional. Replay is only useful if you can diff what the detector
saw live against what today's code produces on identical input — otherwise a logic
regression is indistinguishable from a market change.

### 4.4 Gaps are data, not errors

A sequence discontinuity, a reconnect, a rate-limit backoff, or a subscription drop each
produce an explicit `stream_gap` record with start, end, cause, and affected markets. Any
analysis window overlapping a gap is flagged rather than silently trusted. Coverage
percentage per market per day is a first-class output, not a debugging aid.

### 4.5 Storage

Raw: append-only NDJSON, gzip, hive-partitioned `venue=/date=/market_id=`, one file per
stream session. Never mutated.

Analysis: Parquet, same partitioning, regenerated from raw by a pure function. If a Parquet
file can't be reproduced by rerunning the transform over raw, the transform has state and
that's a bug.

Put no raw ticks in Postgres. Postgres holds the manifest, run metadata, adjudication
decisions, and coverage stats only.

---

## 5. What config can decide, and what it can't

The exit gates handle known failure modes. They will never handle novel structures, and
pretending otherwise is how a wrong mask becomes a fake arbitrage.

**Automated (config gates):**

- depth below floor → drop observation
- one-sided book → drop
- leg skew above threshold → drop
- `p` outside `[0.05, 0.95]` → drop from discovery, keep in capture
- stream gap overlap → flag
- `void_policy` mismatch across legs → demote basket to `NEAR_IDENTITY`
- any leg with `void_policy: UNKNOWN` → basket incoherent, excluded from gate arithmetic
- `raw_text_hash` changed since mask derivation → invalidate edge, requeue

**Queued for human adjudication:**

- two markets with identical masks but different resolution sources
- a derived relationship no generator produced (LLM or statistical candidate)
- a basket whose `OTHER` behaviour can't be inferred from the rules text
- an anomalously large or unusually *stable* gap — stability is the signature of an
  unmodelled outcome, not of inefficiency

### 5.1 Adjudication queue

Not a dashboard. A review queue: both rules texts side by side, both masks, divergence
flags, accept / reject / defer on a keystroke.

```
adjudication(
  id, basket_id, market_ids[], masks[],
  rules_text_hashes[],           -- decision is bound to these
  divergence_flags[],
  decision,                      -- ACCEPT | REJECT | DEFER
  decided_by, decided_at, note
)
```

Binding the decision to `rules_text_hashes` means a venue editing its resolution text
auto-invalidates the label and returns the item to the queue. Every decision is therefore
a durable labelled datapoint for the classifier, not an ephemeral click.

---

## 6. Component architecture

Four components plus a shared state store. **The tape is the contract** — components 1–3
exist to produce it, component 4 exists to consume it, and neither side may reach across.

```
┌─────────────┐   manifest    ┌──────────────┐   canonical   ┌─────────────┐
│  Explorer   │ ────────────► │   Tracker    │ ────────────► │    Tape     │
│ (discovery) │               │   (core)     │               │ (immutable) │
└─────────────┘               └──────▲───────┘               └──────┬──────┘
       │                             │                              │
       │                      ┌──────┴───────┐                      ▼
       └──────────────►┌───────────────────┐ │              ┌──────────────┐
              state    │ splices (per venue)│ │              │   Replay +   │
              store    └───────────────────┘ │              │ mask operator│
                 ▲                            │              └──────┬───────┘
                 │        ┌──────────────┐    │                     │
                 └────────┤  Frontend    │◄───┴─────────────────────┘
                   labels │ (label/exclude)│      detections
                          └──────────────┘
```

### 6.1 Explorer — config-gated discovery

Reads the capture manifest (§3), queries each venue's market catalogue, and emits
subscription instructions. Stateless apart from what it writes to the state store.

**The race that matters:** for short-dated markets — hourly crypto especially — the delay
between market creation and first subscription is a permanent hole in the most informative
part of that market's life. A 60-second discovery loop on a 60-minute market loses 1.6% of
it, and it's the opening price-discovery window. Use venue push/event feeds for new-market
notification where they exist and poll only as fallback. Record `market_created_at` and
`first_subscribed_at` on every market so **coverage-from-inception** is a measurable number
rather than an assumption.

Discovery decisions are also data: log every candidate the explorer *rejected* and why.
Otherwise a selector bug looks identical to a venue not listing the market.

### 6.2 Splices — venue-specific tape adapters

One splice per venue. Handles auth, subscription, protocol quirks, reconnection, and
sequence handling, then emits canonical events to the tracker core.

**The one rule that makes this safe: a splice normalises, it never discards.** Every event
carries both the canonical envelope and the verbatim raw frame. Normalisation is a lossy
interpretation, and interpretations get revised; the raw frame is the only thing that
survives being wrong about the schema. This is the capture-irreversibility principle applied
at the component boundary.

```python
class Splice(Protocol):
    venue: str
    async def discover(self, selector: dict) -> list[MarketRef]: ...
    async def subscribe(self, markets: list[MarketRef]) -> AsyncIterator[CanonicalEvent]: ...
    def normalise(self, raw: dict) -> CanonicalEvent: ...   # must attach raw verbatim
    def sequence_of(self, raw: dict) -> int | None: ...     # None => venue has no seq
```

`sequence_of` returning `None` is a first-class case, not a failure — a venue without
sequence numbers has permanently lower replay fidelity and must be labelled that way rather
than silently pooled with the others.

**Build two splices before trusting the interface.** Writing it against Polymarket alone
will bake Polymarket assumptions into the core. Implement Polymarket and Limitless in close
succession, refactor the interface once, and only then add Kalshi.

### 6.3 Tracker core — tagging and durable write

Venue-agnostic. Consumes canonical events, applies gap detection, writes the tape.

**Tags go in a sidecar, never into the raw record.** Tagging is interpretation with today's
logic; baking it into the tape means replay can never re-tag with improved logic, which
defeats the point of having a tape. Write tags to a separate derived layer keyed by
`(venue, stream, seq)`. The raw tape stays immutable and untagged.

Everything the tracker writes must be reproducible from the raw frames alone.

### 6.4 Frontend — labelling and exclusion

Minimal by design: the adjudication queue from §5.1 plus market-level exclusion.

**Exclusion is a label, not a delete.** Removing a market from being carried forward must
stop *detection and alerting*, never *recording*. If a human exclusion stops capture, then
every later re-analysis is silently conditioned on decisions made with partial information,
and the survivorship bias is unauditable because the evidence is gone.

```
exclusion(market_id, excluded_at, excluded_by, reason, scope)
   scope ∈ { ALERTING, ANALYSIS, BOTH }   -- never CAPTURE
```

Same principle as never gating capture on an economic threshold: exclusion is analysis, and
analysis is the reversible half.

### 6.5 Replay + mask operator

Consumes the tape, rebuilds books, generates Ω and masks per `structure_type`, runs the LP
and the placebo null. Has no network access and no venue awareness whatsoever — if it needs
to know which venue an event came from for anything other than a labelled field, the
canonical schema is wrong.

This is also where the detector lives in production: the live path is the same operator fed
by a streaming tape reader rather than a file reader, so live and replay share one
implementation by construction.

### 6.6 State store

The one piece not in the original sketch, and it will get scattered across the other four if
it isn't named. Postgres, holding: the manifest and its version history, known markets and
their metadata versions with `raw_text_hash`, subscription state, exclusions, adjudication
decisions, coverage statistics, and run metadata. No ticks.

---

## 7. Replay contract

Replay must be bit-identical given the same raw input:

1. Read raw NDJSON in `ts_recv` order across all venues.
2. Rebuild each book by applying deltas from the last snapshot with `seq` continuity.
3. Emit the same bar/event stream the live detector consumed.
4. Run the detector; diff against recorded `detector_output`.

A replay that doesn't reproduce recorded detections on unchanged code is a capture bug,
and finding it early is worth more than any analysis result. Make this a CI test over one
recorded day.

---

## 8. Operational requirements

- Reconnect with exponential backoff; force a full snapshot resync after every reconnect
- Detect `seq` discontinuity → resync + `stream_gap` record
- Respect per-venue rate limits; Kalshi session tokens expire every 30 minutes, so the
  refresh loop must be tested against expiry, not assumed
- One process per venue so a single venue outage doesn't stop the others
- Heartbeat and coverage alerting: silence on a subscribed market is indistinguishable from
  a dead socket unless you monitor for it
- Disk: short-dated crypto full-depth deltas across three venues is the heaviest stream by
  far. Measure a day's volume before sizing anything.

---

## 9. Rollout

Ordered by component, with the gates that must hold before moving on.

1. **State store + manifest schema.** Everything else writes to it; retrofitting is painful.
2. **Polymarket splice + tracker core**, P0 crypto ladders only. Raw passthrough working,
   gap detection working, tags in the sidecar.
3. **Replay CI test over one recorded day.** Rerun the operator on unchanged code, diff
   against recorded detector output. **Do not proceed until this passes.**
4. **Explorer**, with `market_created_at` vs `first_subscribed_at` measured. Report
   coverage-from-inception before trusting any short-dated analysis.
5. **Limitless splice.** Second venue on the same instrument class. Refactor the Splice
   interface now, once, with two real implementations in hand.
6. **Mask operator**: `price_buckets` generator, LP, placebo null, over recorded data.
   This is the P0 control experiment and the first real result.
7. **Frontend**: adjudication queue plus exclusion labelling.
8. **Kalshi splice** once access is resolved; P1 macro buckets.
9. P2–P4 as pure manifest additions. If any of them requires code, step 5 was done wrong.

---

## 10. Open questions

1. Does Limitless's WSS expose incremental deltas with sequence numbers, or snapshots only?
   If snapshots only, its replay fidelity is permanently lower and it should be labelled as
   such rather than pooled with the others.
2. Can a Kalshi account be created for market-data access without US residency? This is the
   single largest unknown in the plan.
3. Polymarket short-dated crypto markets: what is the actual creation rate, and does
   `discover_every_s: 60` catch them early enough in their life to be useful?
4. Which reference feed for the underlying, and can it be captured with the same timestamp
   discipline? Comparing a Pyth-resolved venue to a Binance-referenced one introduces a
   basis we need to measure, not assume away.
5. Do the three venues' crypto ladders share strike levels, or will cross-venue comparison
   require interpolating between non-matching strikes? If the latter, that interpolation is
   a model and must be labelled `ESTIMATED`.
---

## 11. Amendments — decided and measured since drafting

This section records what has changed, been decided, or been measured against live
venues since §1–§10 were written. The original text above is left intact; where
this section contradicts it, this section is current.

### 11.1 Sequencing is ours, not the venue's *(supersedes §4.2, §6.2, §7.2)*

§4.2 makes a venue `seq` mandatory and §7 rebuilds books on "`seq` continuity".
Neither is available: Polymarket publishes no sequence at all, and it is the volume
leader and first splice.

**Decision:** our monotonic capture sequence is authoritative for replay. The venue
cursor is demoted to typed evidence about the venue's own continuity — a different
question, answered in the analysis layer. §6.2's `sequence_of() -> None` is not a
rare labelled case; it is the primary venue. Three counters, three jobs — see
`splices/common/ENVELOPE.md`.

Consequence: our sequence guarantees replay determinism but cannot see a frame the
venue dropped, because our numbering is dense either way. Venue-side gap detection
is a separate mechanism with per-venue strength.

### 11.2 The component boundary is a file *(refines §6.2, §6.3)*

Splices append fsync'd NDJSON to per-venue spools; the Rust ingester tails them.
Not gRPC or an internal socket: those keep frames in the splice's memory until the
far side commits, so an ingester that is down or backpressured costs data. The
spool is the irreversible evidence and the ingester's store is derived from it.
This also satisfies §8's one-process-per-venue where outages actually happen.

### 11.3 Normalisation moves to replay *(extends §6.3)*

§6.3 puts tags in a sidecar because "tagging is interpretation with today's logic."
The same argument applies one level up, so the tape stores raw bytes plus the
envelope and nothing is normalised at capture. This is why there is no
fixed-point money stack (`decimal`, `money`, `scale`, `convert`) at this layer.

Empirically justified three times over — see §11.6.

### 11.4 Storage simplifies *(supersedes §4.5)*

Not four stores. NDJSON spools are the raw layer; SQLite is the fact log and the
state store; Parquet only when a query is actually slow. §9 step 1 no longer blocks
everything on standing up Postgres.

### 11.5 Open questions now answered

**Q1 — Limitless deltas?** No. `orderbookUpdate` is a full book every time, no
delta stream. But it carries a `version` field the documentation omits: monotonic
per market, **not dense**, ranges overlapping across markets. It orders and dates a
book — detecting a stale one — but a missing update leaves no hole, so a dropped
one is undetectable. Lowest fidelity of the three; labelled, not pooled.

**Q2 — Kalshi access.** Being resolved; keys pending. Note that `orderbook_delta`
is a *private* channel and every WSS connection is authenticated at handshake, so
depth is hard-gated on the account. Fallback if it never arrives: the REST
orderbook is public and unauthenticated (verified, HTTP 200 with no key) — that is
snapshot-by-polling and therefore lossy, a contingency rather than a plan.

**Q3 — creation rate / discovery cadence.** Limitless runs 5-minute, 15-minute,
hourly and daily crypto ladders created continuously. `discover_every_s: 60` loses
a fifth of a five-minute market's life, and it is the opening price-discovery
window. Both venues push new-market events the splices already record
(`NewMarketEvent`, `marketCreated`); until those are wired into the targeter,
coverage-from-inception is not a number we can report.

### 11.6 Measurements

| | |
|---|---|
| Polymarket, 20 assets, 45s | 3,823 frames — 3,707 `price_change`, 94 `book`, 37 `last_trade_price`, 4 `PONG` |
| Limitless, 11 markets, 40s | 451 frames — 448 `orderbookUpdate`, 3 `system` |
| **Volume** | **6.2M records/day, 6.8 GB/day** uncompressed at 20 Polymarket assets — §8's "measure a day's volume", now measured |

Three places the live wire contradicted the published documentation:

1. Polymarket's wire is flat snake_case (`event_type`, `price_changes`,
   `best_bid`); the docs describe wrapped camelCase.
2. Polymarket carries a book `hash` on every `price_change` entry (7,414/7,414),
   making checksum reconciliation viable.
3. Limitless carries `version` on every `orderbookUpdate`; its reference states no
   such field exists.

A splice that normalised against any of those documents would have produced
confident, wrong output. This is the operative argument for §6.2's "normalises, never
discards", strengthened here to "does not normalise at all".

### 11.7 Still open from the review, unchanged

- **`resolution_source` belongs in the P0 mask identity.** §2 claims strike-derived
  masks "cannot be wrong". True within a venue; false across, because a mask is only
  half a condition's identity and the settlement oracle is the other half. The
  manifest's `omega.params.reference_feed` is a single scalar and encodes an
  assumption that it is shared. This is the most likely way P0 manufactures a fake
  arbitrage in the one class nominated as immune.
- **Run the single-venue ladder monotonicity test early.** Within one venue's strike
  ladder a monotonicity violation is pure arbitrage — no cross-venue basis, no clock
  skew, no differing settlement source. Cleaner than anything cross-venue and
  available as soon as one splice records. §9 defers the first result to step 6 of 9.
- **"LP" and "placebo null" (§6.5, §9.6) are undefined** anywhere in this repo. The
  built test is a Σask threshold with VWAP depth-walking; the staleness control is
  `leg_skew` stratification, not a placebo. Define them or name what exists.
- **§5's `p outside [0.05, 0.95] → drop from discovery`** will drop the far strikes
  of every crypto ladder, which is where the monotonicity chain terminates.
- **§2's "the draw leg is historically the most mispriced"** is unsourced and is
  being used to set capture priority. Treat as an assumption to test.
