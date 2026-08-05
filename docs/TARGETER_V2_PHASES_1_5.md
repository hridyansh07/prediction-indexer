# Targeter v2 — Delivery Phases 1–5

Status: implemented as a one-shot shadow pipeline. This document is the
normative contract for the code in `targeter/v2`. It deliberately stops before
S3 run archival, live target publication, scheduling, deployment changes, and a
review UI.

## 1. Objective and safety boundary

Targeter v2 discovers mature soccer and esports event families which have a
modeled combinatorial relationship across at least two venues. It prefers three
venues, activates a bundle at least one hour before the scheduled event, and
requires meaningful dollar-denominated anchor activity before using
market-class breadth and activity to rank work.

It is not an arbitrage executor. Every derived relationship is labeled
happy-path/conditional. Soccer score spaces are deliberately bounded and carry
`INCOMPLETE_COVERAGE`; they cannot support an unconditional locked-basket claim.

The phase-5 command:

- reads public venue catalogues;
- writes normalized catalogues, rule-template evidence, and a local selection
  report;
- never writes `targets_<venue>.json`;
- never changes a splice subscription;
- never uploads to S3.

The existing `targeter/run.py` is the legacy crypto-ladder targeter. It remains
untouched until the later publication/deployment phase intentionally replaces
it.

## 2. Strategy configuration

`configs/targeter_v2.json` is the single semantic configuration for phases
1–5. Unknown top-level fields, invalid regular expressions, duplicate class
identifiers, bad venue thresholds, and non-positive timing/budget fields are
fatal.

The default contract is:

- sports: soccer and esports;
- minimum venues: 2;
- preferred venues: 3;
- discovery horizon: 48 hours;
- capture start: event activation minus 3,600 seconds;
- scheduled-run lookahead: run interval plus a subscription guard, currently
  600 + 60 seconds;
- event-time matching tolerance: 900 seconds;
- minimum known market age: 1,800 seconds;
- minimum combined known moneyline lifetime volume: USD/USDC 25,000;
- post-start retention: 21,600 seconds;
- maximum selected bundles: 50;
- venue subscription budgets: explicit and independently enforced.

### Market-class registry

`market_classes` maps vendor product shapes to a canonical class, type, and
settlement scope. This mapping—not runtime prose similarity—is the day-one
semantic authority. Examples include:

- `soccer.moneyline_3way`;
- `soccer.spread`;
- `soccer.total_goals`;
- `soccer.both_teams_to_score`;
- `soccer.correct_score`;
- `esports.series_moneyline`;
- `esports.map_winner`;
- `esports.total_maps`;
- `esports.map_handicap`.

Adding or changing a reusable venue product is a strategy update plus a small
adapter-shape test. It must not require an event-specific parser or fixture.

`participant_aliases` is the conservative naming escape hatch. It maps a
preferred reusable team identity to reviewed aliases and is empty by default.
It applies to both event matching and market-side resolution. An alias conflict
is fatal; unknown nicknames remain unmatched rather than being guessed.

## 3. Phase 1 — canonical model and registry

`targeter/v2/domain.py` defines the vendor-independent records:

- `CanonicalEvent` — venue event ID, sport, league, two participants,
  activation, competition format, and source reference;
- `CanonicalMarket` — canonical class/type/scope, subscription IDs, outcome
  labels, normalized parameters, status, rules, age, vendor-native volume and
  liquidity, plus an optional explicitly normalized `volume_total_usd`;
- `CatalogSnapshot` — one adapter result with completeness diagnostics;
- `EventBundle` — a high-confidence cross-venue event match;
- `Relationship` — a pair of modeled affirmative claims and its scope and
  coverage.

Timestamps are aware UTC values. Identifiers are deterministic. Participant
normalization is Unicode-safe, case-insensitive, and removes common club-name
noise such as `FC`, `FK`, `CF`, `SC`, and `AFC`. It is conservative: an unknown
nickname is a false negative, not permission to guess that two teams are the
same. Latin diacritics are folded, while non-Latin letters, numbers, and
combining marks are retained so unrelated names cannot collapse to a shared
ASCII prefix.

## 4. Phase 2 — public sports adapters

`targeter/v2/adapters.py` is the only vendor-specific boundary. Downstream
modules consume canonical records only.

### Kalshi

1. Read the public Sports series catalogue.
2. Classify reusable soccer/esports series through the registry.
3. Page open events globally with nested markets and filter by the recognized
   series registry. This avoids one request per series and does not trust the
   API's category filter to remove unrelated events.
4. Prefer strike/occurrence time as event activation; expected expiration and
   close time are fallbacks in that order.
5. Accept current full-time `... Game` series naming and remove known product
   suffixes such as `: Spread` before parsing participants.
6. Reject first-half, second-half, extra-time, penalty, and corners series from
   regulation-fulltime classes through explicit registry patterns.
7. Subscribe by ticker.

### Polymarket

1. Use event keyset pagination for each configured sport tag.
2. Bound the request by the discovery horizon and retain nested markets.
3. Treat base, more-markets, and exact-score fragments as supported inputs.
4. Reject halftime, first-half, second-half, first-team-to-score, and corners
   fragments until their own canonical scopes exist.
5. Require a moneyline child to name a participant/draw or expose a matching
   multi-outcome set; the parent event's `vs` title alone is insufficient.
6. Subscribe by CLOB token ID.

### Limitless

1. Page the full active catalogue. Filtering only `automationType=sports`
   misses separately listed sports props.
2. Build canonical events from structured sports groups.
3. Attach standalone total-goals and both-teams-to-score props by canonical
   participant pair and the shared expiration identifier.
4. Read best-of length from `metadata.numberOfGames` when it is not present in
   the title.
5. Treat `FUNDED` CLOB markets as open and subscribe by market slug.

Pagination cursor repetition and malformed response shapes are fatal. A live
change to Limitless's `totalMarketsCount` triggers one bounded second pass and
the two observations are reconciled by stable vendor market ID. A stable
premature exhaustion before the reported total remains fatal. Probe page/series
caps set `complete: false`; they are for bounded validation, not a production
selection run.

## 5. Phase 3 — cross-venue event matching

`targeter/v2/matching.py` groups events by:

1. canonical sport;
2. exact unordered canonical participant pair;
3. activation times whose total cluster span is within the configured
   tolerance;
4. compatible published competition formats.

The time rule is cluster-span based, not transitive. Events at `t`, `t+10`, and
`t+20` minutes do not become one match under a 15-minute tolerance merely
because adjacent pairs fit.

Several fragments from one venue can join the same bundle. At least two
distinct venues are required. A known format mismatch rejects the bundle;
different non-empty league labels are retained as a warning. Unmatched or
ambiguous inputs are reported rather than guessed.

## 6. Phase 4 — outcome relationships and selection

`targeter/v2/relationships.py` adapts canonical markets into the existing
outcome-space/mask engine.

- Soccer uses a bounded regulation score grid. Its cap is derived from listed
  lines with a conservative floor and ceiling, and coverage is always
  `INCOMPLETE_COVERAGE`.
- Esports uses an exhaustive reachable map-sequence space only when a single
  odd best-of format is known.
- Positive handicaps retain handicap semantics. `Chelsea +1.5` includes a draw
  and a one-goal Chelsea loss; it is not misread as “Chelsea wins by more than
  1.5.”
- Binary conditions compile their affirmative claim. Multi-token outcomes are
  separate claims only when labels and subscription tokens align. A two-token
  handicap emits both complementary sides or fails closed; it never emits a
  partial condition.
- Correct-score parameters are translated from each venue event's participant
  order into the bundle's participant order before masks are compared.

The engine derives `IDENTITY`, `IMPLICATION`, `REVERSE_IMPLICATION`,
`MUTUAL_EXCLUSION`, and `OVERLAP`. Only markets participating in a cross-venue
structural relationship (`IDENTITY`, either implication direction, or
`MUTUAL_EXCLUSION`) consume target budget. Plain `OVERLAP` remains evidence in
the report but is too weak by itself to activate a bundle.

`targeter/v2/selection.py` applies these gates:

- event-level combined known USD/USDC lifetime volume across trusted moneyline
  anchors meets the configured 25,000 threshold;
- at least one cross-venue modeled relationship;
- at least two eligible venues;
- capture start reached within one run interval plus guard;
- not beyond post-start retention.

Market-level checks are separate from event admission. A market must be open,
accepting, mature when creation time is known, free of an explicit normal-scope
contradiction, and participate in a modeled cross-venue relationship before it
is subscribed. A sibling which fails any of those checks is excluded and
reported under `market_exclusions`; it does not veto an otherwise admissible
event. The event fails only when the surviving set can no longer satisfy an
event-level gate, such as two eligible venues or one useful relationship.

Missing creation time is not treated as proof that a market is new; activation
proximity still provides the primary maturity gate. Activity is a ranking input,
using log-scaled vendor fields. Those native fields do not have identical units
and never enter the hard dollar gate. Only `volume_total_usd` enters admission:
Polymarket `volumeNum` and Limitless USDC formatted volume map directly; Kalshi
contract `volume_fp` remains a contract count and contributes zero unless the
API explicitly supplies `dollar_volume`. Complementary binary outcomes do not
turn a contract count into cash turnover because each contract can trade at a
price anywhere between zero and one dollar.

Ranking uses an explicit tuple giving lexical priority to venue coverage, then
cross-venue relationship quality, class breadth, and activity. It does not use
a weighted scalar for allocation order, so no number of two-venue listings can
outrank a three-venue bundle. Allocation is bundle-atomic: if adding the bundle
exceeds any venue budget, none of it is selected. The report states
`target_budget_exceeded` or `maximum_bundles_reached` for allocation drops.

## 7. Phase 5 — templates, drift, and shadow run

`targeter/v2/rules.py` reduces rule text into content-addressed templates.
Normalization replaces participants, event dates/times, and canonical numeric
parameters while preserving the remaining language. A template ID hashes:

- normalizer version;
- venue;
- sport;
- canonical market class;
- normalized rule text.

There is no linear `v1`/`v2` ordering requirement. A newly observed template is
`UNREVIEWED`; adding its ID to `known_rule_templates` marks it `KNOWN`. Multiple
templates for one venue/class in a bundle generate non-blocking drift evidence.
Changed postponement, cancellation, correction, forfeit, or other exceptional
clauses do not prevent happy-path capture.

The only automated blockers are narrow textual contradictions to a configured
normal scope, currently:

- full-time/regulation class explicitly includes extra time or penalties;
- full-time class explicitly says it is first-half only;
- series-moneyline class explicitly says it is a single-map market.

An explicit exclusion such as “not including extra time or penalties” is not a
contradiction.

Run a live shadow discovery with:

```bash
python3 targeter/run_v2.py
```

Catalog refresh is the default. `--reuse-cache` is an explicit offline/debug
mode. For repeated live monitoring, `--no-response-cache` still performs fresh
requests and preserves the normalized run artifacts, but retains only the
durable per-host rate-limit state instead of raw HTTP response bodies. It is
mutually exclusive with `--reuse-cache`. Bounded probes can use:

```bash
python3 targeter/run_v2.py \
  --max-kalshi-pages 2 \
  --max-polymarket-pages 1 \
  --max-limitless-pages 2
```

A probe cap makes the input incomplete and the command exits nonzero after
preserving its report. Adapter failures are also recorded and cause nonzero
completion. A complete run with no qualifying bundles is a successful empty
result.

Each run writes a timestamped local directory containing:

- `catalog_<venue>_events.ndjson`;
- `catalog_<venue>_markets.ndjson`;
- `rule_templates.ndjson`;
- `rule_drift.ndjson`;
- `selection_report.json`.

`selection_report.json` records source completeness, match rejections, every
candidate and rejection reason, masks, relationships, rule evidence, score
components, budget use, and the proposed subscription IDs. Each candidate has
one event-level `event_status`, an `admission` block with threshold evidence,
per-venue known/unknown dollar-volume coverage, and nested per-market
exclusions. It always contains `mode: "shadow"` and `publication_performed:
false`.

## 8. Review workflow

For a new or changed vendor product:

1. Inspect the normalized market and template evidence from a shadow run.
2. If the product's normal settlement archetype is already represented, update
   the venue pattern for that canonical class.
3. If it is a genuinely different normal scope, add a new canonical class and
   resolver; do not broaden an existing regular expression until it accepts two
   meanings.
4. Add a small adapter-shape test and deterministic relationship test.
5. Add reviewed content-addressed template IDs to `known_rule_templates` when
   useful. This changes review status, not semantic equivalence.
6. Run another shadow pass. A UI may automate approve/deny later, but is not a
   day-one dependency.

## 9. Verification contract

The phase tests intentionally avoid large frozen vendor responses. They cover:

- strict strategy parsing and registry classification;
- small current adapter shapes, pagination, cursor failure, and fragment-scope
  rejection;
- participant reversal and multi-fragment event matching;
- format mismatches;
- stable template IDs, non-blocking exceptional drift, and explicit scope
  contradictions;
- handicap semantics and cross-venue relationship derivation;
- T−1-hour timing, new-market maturity, three-venue preference, atomic budgets,
  and shadow-only output.

Read-only live probes complement these tests. A vendor response change is fixed
at the adapter boundary and covered with the smallest record shape that proves
the regression.

The public discovery contracts used here are the venue-maintained references:

- [Kalshi series discovery](https://docs.kalshi.com/api-reference/market/get-series-list)
  and [paginated events with nested markets](https://docs.kalshi.com/api-reference/events/get-events);
- [Polymarket event keyset pagination](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination);
- [Limitless active-market pagination](https://docs.limitless.exchange/api-reference/markets/browse-active).

Run the focused gate with:

```bash
python3 -m unittest -v tests.test_targeter_v2 tests.test_masks
```

## 10. Delivery boundary after phase 5

The phase-5 shadow output itself still makes none of the following claims:

- immutable archival of a target run and its metadata;
- publication of selected subscriptions to splice target files;
- cron/systemd/Docker Compose scheduling and production enablement;
- reviewer UI;
- execution, pricing, or unconditional arbitrage claims.

Archival, publication, splice handoff, scheduling, and operational audit are now
implemented as the separately gated phases in `TARGETER_V2_PHASES_6_10.md`.
They consume this phase-5 selection report rather than duplicating discovery,
matching, template, or ranking logic. Reviewer UI, execution, pricing, and
unconditional arbitrage claims remain deferred.
