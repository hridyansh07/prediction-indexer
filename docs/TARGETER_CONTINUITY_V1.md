# Targeter continuity and terminal eviction

**Status:** specification. Nothing implemented.

Two changes to `targeter/v2`, both narrow:

1. **Terminal eviction** — stop subscribing to a market once trading on it has
   demonstrably ended, so concluded markets stop occupying ingester slots.
2. **Continuity hold** — stop a bundle we are already capturing from being
   displaced by a newcomer at the budget limit.

The hold is a *filter, never an admit*. It can preserve an existing budget claim
and can never create one. Every eligibility gate still runs first, unchanged.

---

## 1. What the venues publish

Measured against live and settled markets on 2026-08-15.

### 1.1 Terminal signals exist, and are definitive

| Signal | Kalshi | Polymarket | Limitless |
|---|---|---|---|
| **Trading stopped** | `status: finalized` | `acceptingOrders: false` | `expired: true` |
| Corroborating | `close_time < expiration_time` | `closed: true` | `status: RESOLVED` |
| **Never use** | — | `active` — stays `true` after close | `tradeType` — stays `clob` after resolution |
| Outcome | `result: 'yes'\|'no'` (`''` while open) | `outcomePrices` | `winningOutcomeIndex` (`null` while open) |
| Oracle finalisation | — | `closedTime` **≡ `umaEndDate`** | — |

Limitless measured on 2026-08-15 against markets selected on 2026-08-03:
`status` moves `FUNDED` → `RESOLVED`, `expired` moves `false` → `true`, and
`winningOutcomeIndex` moves `null` → `0`/`1`. **`tradeType` stays `"clob"` in
both states**, so the `tradeType != "clob"` branch asserted in `adapters.py` is
dead and must not be relied on. Production is unaffected today because
`accepting_orders` already ANDs in `not expired`.

Limitless `expirationTimestamp` is a padded deadline (~2 days past the match),
matching the pattern on the other two venues — not a match end.

`closedTime` and `umaEndDate` are identical to the second on every settled market
checked. It is the UMA resolution instant, not the trading stop, and is not used
here: we care when the book stops moving, not when the oracle agrees.

**Polymarket publishes no trading-stop timestamp at all.**
`acceptingOrdersTimestamp` is when orders *opened*. So Polymarket offers a
boolean and nothing else — the instant trading stopped is recoverable only from
our own tape. This is why §2.2 exists.

Kalshi's corroborator is free: while open, `close_time == expiration_time` (a
+48 h placeholder); once closed, `close_time` is rewritten to the real close
while `expiration_time` stays pinned.

### 1.2 Terminal markets are invisible to discovery

The discovery queries filter to live markets:

```
kalshi      /events?status=open&min_close_ts=…
polymarket  /markets?closed=false&end_date_min=…
```

Verified: a market that has gone terminal is **absent** from the query the
targeter issues.

```
direct lookup           status='finalized'  close_time=2026-08-15T05:49:39Z
discovery(status=open)  finalized event present = False
```

So terminal state cannot be read from discovery. Absence is the only thing
discovery reports, and absence conflates conclusion, delisting, pagination
failure, and an API blip. §2.1 therefore reads terminal state directly.

### 1.3 No venue publishes a usable end time

`occurrence_datetime` and `expected_expiration_time` are identical to each other
and equal **scheduled + 4.00 h**, at both p50 and p90, across all three series
measured. The true scheduled start appears only in the event ticker
(`KXDOTA2GAME-26AUG150900TYIRO` → Aug 15, 09:00 EDT) and in `rules_primary`
prose — never in a structured timestamp field.

**Caveat, stated because it matters:** these are snapshot observations. They
cannot distinguish "constant" from "updated while the market is live and
currently at its default". A venue that extends its own padding for an
overrunning match would look identical here. That is a further reason not to
build on it.

Observed durations from the true scheduled start, settled Kalshi esports markets:

| Series | n | p50 | p90 | p95 | > 8 h |
|---|---:|---:|---:|---:|---:|
| `KXCS2GAME` | 590 | 2.33 h | 4.00 h | 6.99 h | 3.7% |
| `KXDOTA2GAME` | 418 | 3.83 h | 7.16 h | 10.45 h | 6.5% |
| `KXLOLGAME` | 598 | 3.17 h | 7.16 h | 10.00 h | 7.4% |

These justify the clamp's magnitude and nothing more. Per-sport or per-format
tuning is **not** attempted in V1: the sample is one snapshot, the tail is
dominated by postponement rather than play, and `best_of` is published by
Polymarket alone — Kalshi carries no format text in `title`, `sub_title`, or
`rules_primary`. A clamp keyed on a field only one venue supplies would be a
false precision.

**V1 uses a single flat clamp of 8 hours for every market.** It is a backstop,
not a model of match length. Terminal state is what actually evicts.

---

## 2. Terminal eviction

### 2.1 Read terminal state directly, for held markets only

Because §1.2 makes discovery useless for this, each run looks up the markets it
currently holds:

```
kalshi      GET /markets?tickers=<csv>        one call, all held tickers
polymarket  GET /markets/{id}                 one call per held market
limitless   GET /markets/{slug}               one call per held market
```

Bounded by the number of held markets, which is bounded by the budget — ~39
markets on the 2026-08-15 run, so one Kalshi call plus ~20 Polymarket calls per
run. Kalshi batching is verified; Polymarket's `ids=` parameter is ignored by
the API and returns an unfiltered page, so it must be per-market.

A leg is `terminal` when:

```
kalshi      status == "finalized"  OR  close_time < expiration_time
polymarket  acceptingOrders == false
limitless   expired == true  OR  status == "RESOLVED"
```

A lookup that fails or returns a malformed record leaves the leg **unchanged**,
not terminal. Eviction on a failed read would drop live capture on an API blip.
The clamp is what bounds that case.

This includes **404**, which Limitless returns for some historical slugs — one of
four settled markets probed had been removed entirely. A 404 is indistinguishable
from a slug change or a transient fault, so it must not evict; the clamp handles
a market that stays unreadable.

### 2.2 The tape buffer

A leg going terminal does not immediately drop its subscription. It stays
subscribed for `terminal_buffer_seconds` (default **900**) so the tape records
the book going static. That transition is the evidence trading stopped — and for
Polymarket it is the *only* record of when, since the API never timestamps it.

### 2.3 A bundle is not terminal until every leg is

Kalshi closes early and precisely; Polymarket keeps trading past it. The window
between them is a known outcome on one venue against a live book on the other,
which is the cross-venue signal this project exists to find.

So: mark legs terminal individually, drop each leg's subscription after its
buffer, and release the bundle's budget when **all** legs are terminal or the
clamp fires.

### 2.4 The clamp

```
clamp_at = activation_at + terminal_clamp_seconds     # 28800, flat
```

`activation_at` is the anchor because it is the targeter's own reconciled value,
corroborated by two venues — on 2026-08-15 it was `02:00Z`, matching both
Polymarket's `eventStartTime` and the Kalshi ticker's `26AUG142200` EDT.

At `clamp_at` the bundle is terminal regardless of venue state. This is the
catch-all for postponement, cancellation, a flag that never flips, a malformed
record, and a lookup that keeps failing. It is deliberately generous: terminal
state should almost always evict first, and the clamp should almost never be the
reason.

---

## 3. Continuity hold

### 3.1 Two lists

Each run partitions eligible ranked candidates into:

- **held** — selected by the previous published generation, still eligible, no
  leg past `clamp_at`
- **additive** — everything else

Budget is claimed by `held` first, then by `additive` in rank order. Scoring and
`_ranking_key` are untouched; only the order in which budget is consumed changes.

**The purge only ever removes `additive` candidates.** A held bundle cannot be
displaced by rank.

### 3.2 It can never admit

`held` is intersected with the eligible ranked set before use. A bundle failing
any gate, or past its clamp, is not in `ranked` and so cannot be held. The hold
preserves a claim; it never creates one.

### 3.3 Full-budget behaviour

When `held` alone fills the budget, `additive` candidates are rejected as
`displaced_by_continuity_hold` — distinct from `target_budget_exceeded`, so a
saturated hold is visible rather than looking like ordinary budget pressure. The
generation is republished unchanged.

### 3.4 Degradation

If the prior generation cannot be read, hold the last published target set rather
than falling back to an empty hold. The filter exists to protect continuity, so
its degraded mode must also protect continuity; losing discovery for the length
of an outage is the cheaper failure.

### 3.5 Start-lateness bias

A market first seen well into its own window is worth less than one yet to start.
Applied as a **rank penalty**, not a gate — it should lose ties, not eligibility:

```
elapsed_fraction = (now - activation_at) / terminal_clamp_seconds
```

Motivating case: the 2026-08-15 05:41Z run selected
`KXDOTA2GAME-26AUG142200TYRES-TY`, which closed at **05:49:39Z — eight minutes
later**. Not a hypothetical.

---

## 4. Scope

| File | Change |
|---|---|
| `targeter/v2/domain.py` | `CanonicalMarket.terminal: bool`, `terminal_reason: str \| None` |
| `targeter/v2/adapters.py` | `probe_terminal(held_ids)` per venue, per §2.1 |
| `targeter/v2/registry.py` | `terminal_clamp_seconds`, `terminal_buffer_seconds`, `continuity_hold_enabled` |
| `targeter/v2/selection.py` | `held` param, two-pass allocation, clamp, lateness penalty, new rejection reason |
| `targeter/v2/run.py` | read prior generation, resolve to bundle ids, drive the terminal probe |
| `tests/test_targeter_v2.py` | §5 |

Nothing under `replay/` or `universe/` changes.

---

## 5. Tests

1. `held = ∅` reproduces today's selection byte-for-byte — the regression guard
2. a held bundle survives a higher-ranked newcomer at the budget limit
3. a held bundle past `clamp_at` is released even with every venue flag still open
4. a leg going terminal keeps its subscription for `terminal_buffer_seconds`
5. a bundle with one terminal leg and one live leg stays subscribed on the live leg
6. `active: true` on a closed Polymarket market does **not** read as live
7. Kalshi `close_time == expiration_time` reads as open; `<` reads as terminal
8. a failed, malformed, or 404 terminal probe leaves the leg live, not terminal
9. full budget held → `displaced_by_continuity_hold`, not `target_budget_exceeded`
10. Limitless `tradeType: "clob"` on a `RESOLVED` market does **not** read as live

---

## 6. Open

1. **Probe cadence.** The terminal probe runs per targeter run (10 min). A market
   that closes just after a run keeps its slot for up to one interval. Acceptable
   at current budget slack; revisit if the budget binds.
2. **Limitless probe addressing.** `/markets/{id}` returns 400 for the numeric
   `venue_market_id`; the API wants the slug, which the catalogue carries only in
   `source_ref` (`/markets/arsenal-1785924949664`). The probe must resolve slugs,
   so either `source_ref` becomes a first-class field or the adapter retains the
   slug explicitly.
3. **Clamp review.** 8 h covers ~93–96% of observed durations. Worth re-measuring
   once more sports are onboarded, since the current sample is esports only.
