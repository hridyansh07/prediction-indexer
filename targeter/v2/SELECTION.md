# Targeter v2 selection

How one discovery pass turns public venue catalogues into a proposed
subscription set. Everything here runs inside `targeter/v2/run.py` and ends at
the selection report; archive, publication and retention are in
[`DELIVERY.md`](DELIVERY.md).

Pipeline: adapters (`adapters/`) -> canonical records (`models.py`) ->
cross-venue matching (`matching.py`) -> outcome spaces and relationships
(`relationships.py`) -> rule templates (`rules.py`) -> admission, ranking and
budgets (`selection.py`).

## Strategy configuration

`configs/targeter_v2.json` is the single semantic configuration, loaded by
`registry.load_strategy`. Unknown top-level fields, invalid regular
expressions, duplicate class or game-family identifiers, bad venue thresholds
and non-positive timing or budget values are fatal (`StrategyError`).

| Field | Shipped value |
|---|---|
| `sports` | `soccer`, `esports` |
| `selection.minimum_venues` / `preferred_venues` | 2 / 3 |
| `discovery_horizon_seconds` | 172800 (48 h) |
| `pre_event_seconds` | 3600: capture starts one hour before activation |
| `run_interval_seconds` + `subscription_guard_seconds` | 600 + 60: a bundle is admitted once its capture start is within 660 s |
| `event_time_tolerance_seconds` | 900 |
| `minimum_market_age_seconds` | 1800 |
| `minimum_combined_moneyline_volume_usd` | 25000 |
| `post_start_retention_seconds` | 21600: latest a fresh candidate may be admitted |
| `terminal_clamp_seconds` | 28800: how long a committed bundle may be retained |
| `continuity_degraded_after_seconds` | 14400 |
| `continuity_hold_enabled` | true |
| `maximum_bundles` | 50 |
| `target_budgets` | kalshi 1000, polymarket 400, limitless 400 subscription IDs |
| `polymarket_tags` | `soccer`, `esports` |
| `limitless_category_ids` | `[53]` |
| `participant_aliases` | empty |
| `known_rule_templates` | empty |

`version` is copied into the report as `strategy_version`; publication refuses
a report whose version differs from the strategy it is given.

`market_classes` maps vendor product shapes to a canonical class, market type
and settlement scope. The shipped classes are `soccer.moneyline_3way`,
`soccer.spread`, `soccer.total_goals`, `soccer.both_teams_to_score`,
`soccer.correct_score`, `esports.series_moneyline`, `esports.map_winner`,
`esports.total_maps` and `esports.map_handicap`. Soccer patterns reject
first-half, second-half, extra-time, penalty and corners products from the
regulation-fulltime classes. Adding or changing a reusable venue product is a
strategy edit plus a small adapter-shape test; it never needs an event-specific
parser or fixture.

`participant_aliases` maps a preferred team identity to reviewed aliases and
applies to both event matching and market-side resolution. An alias conflict is
fatal. An unknown nickname stays unmatched; nothing is guessed.

## Canonical model

`models.py` defines the vendor-independent records. Timestamps are aware UTC;
identifiers are deterministic.

- `CanonicalEvent`: venue event ID, sport, league, two participants,
  activation, format, source reference. Esports events also carry `game`,
  `topology`, `game_evidence` and `activation_evidence`.
- `CanonicalMarket`: canonical class, type and scope, subscription IDs, outcome
  labels, normalized parameters, status, rules, creation time, vendor-native
  volume and liquidity, optional `volume_total_usd`, and (for configured
  esports games) `classification_evidence`. `raw` holds the venue record and is
  not part of `as_record()`; see `target_records.py` in `DELIVERY.md`.
- `CatalogSnapshot`: one adapter result. `complete` is false when a pagination
  cap or failure truncated it, or when any classification diagnostic carries
  `completeness_effect`. Duplicate IDs and markets pointing at a missing event
  raise.
- `EventBundle`: a cross-venue event match. `Relationship`: a pair of modeled
  claims with scope and coverage.

Participant normalization is Unicode-safe and case-insensitive, strips club
noise such as `FC`, `FK`, `CF`, `SC` and `AFC`, folds Latin diacritics, and
keeps non-Latin letters and combining marks so unrelated names cannot collapse.

## Venue adapters

`adapters/` is the only place vendor fields are read. Downstream modules see
canonical records only. All three adapters take a shared durable JSON client
(`adapters/clients.py`); malformed response shapes and repeated pagination
cursors are fatal for that venue.

**Kalshi.** Reads the Sports series catalogue, classifies series through the
registry, then pages open events once with nested markets (`status=open`,
`min_close_ts` = now minus post-start retention) and keeps only events whose
`series_ticker` matched. Activation precedence is `strike_date`, then nested
`occurrence_datetime`, `expected_expiration_time`, `close_time`. Subscribes by
ticker. Kalshi publishes `volume_fp` as a contract count; the adapter sets
`volume_total_usd` to `volume_fp * last_price_dollars` when both are present
(otherwise unknown). That is an estimate of dollars traded, not an explicit
dollar field; `last_price_dollars` stands in for the unpublished average trade
price.

**Polymarket.** Event keyset pagination per configured tag, bounded by the
discovery horizon and post-start retention, nested markets retained. Base,
more-markets and exact-score fragments are inputs; halftime, first/second half,
first-team-to-score and corners fragments are rejected. A moneyline child must
name a participant or draw (or expose a matching multi-outcome set); the parent
`vs` title alone is not enough. Activation precedence for esports is
`eventStartTime`, `startTime`, `endDate`. `volume_total_usd` is `volumeNum`.
Subscribes by CLOB token ID.

**Limitless.** Pages `/markets/active` with `automationType=sports` plus one
`/markets/active/<category>` source per configured category ID (esports are
listed outside the sports automation type). A change in the reported total
during paging triggers one bounded second pass reconciled by stable vendor ID;
a stable early exhaustion is fatal. Standalone total-goals and
both-teams-to-score props attach by participant pair and shared expiration.
Esports structured fields: `metadata.esportTitle` / `videogameSlug` (game),
`homeTeam` / `awayTeam`, `eventId`, `startMatchTimestampInUTC`,
`numberOfGames`. Only CLOB markets that are not expired or hidden are
accepting; subscribes by market slug. `volume_total_usd` is `volumeFormatted`
(USDC).

Probe caps (`--max-kalshi-series`, `--max-kalshi-pages`,
`--max-polymarket-pages`, `--max-limitless-pages`) set `complete: false`; they
exist for bounded validation, not production runs.

## Esports game families

`game_families` in the strategy is the classification authority for esports.
Generic esports `market_classes` patterns do not apply once a record belongs to
a configured family. Each family has `id` (lower snake case), `sport: esports`,
`topology: best_of_series`, `polymarket_game_tags`, `venue_game_aliases` and
`venue_products` for all three venues.

Configured families: `league_of_legends`, `counter_strike_2`, `dota_2`,
`valorant`. Honor of Kings is not configured: no second venue publishes
reviewable match products, so its events could never clear the two-venue
minimum. Adding a game is a configuration entry (aliases, Polymarket game tag,
product mappings) plus a contract test; adapters contain no per-game branch.

Reviewed products per family: series winner (`esports.series_moneyline`, the
anchor), numbered map/game winner, total maps/games, and series map/game
handicap. Kalshi maps exact `series_ticker` values (`KX<GAME>GAME`, `...MAP`,
`...TOTALMAPS`; Valorant has no total-maps series). Polymarket maps anchored
`group_title` patterns (`^match winner$`, `^(?:game|map) N winner$`,
`^o/u X games|maps$`, `^(?:game|map) handicap...$`). Limitless maps exact
`metadata.marketType` enums (`match_winner`, `map_winner`, `total_maps`,
`map_handicap`). Tournament-future tickers, within-map rounds, kills, first
blood and similar props match nothing.

Loader rules: aliases must not collide across families; a Polymarket game tag
belongs to one family; Polymarket product patterns must be anchored with `^`
and `$`; a mapping has exactly one of `values` or `patterns`; each mapping
field must be on the venue allowlist (`series_ticker`, `group_title`,
`metadata_market_type`); every referenced canonical class must exist.

Game evidence, per venue:

- Kalshi: an exact configured series ticker establishes the game; the series
  title is supporting evidence. Conflicting configured games are
  `game_classification_conflict` and make the catalogue incomplete.
- Polymarket: the generic `esports` tag, the family's game tag and a
  colon-terminated title prefix (`LoL:`, `Counter-Strike:`, `Dota 2:`,
  `Valorant:`) must agree. A missing or conflicting pair fails closed
  (`unsupported_game` for an unconfigured game, `game_classification_conflict`
  for disagreement). An anchor-shaped product that cannot be classified is
  `unclassified_anchor_candidate` and makes the catalogue incomplete; an
  unclassified sibling is `unclassified_sibling_product` and does not.
- Limitless: exact `esportTitle` or `videogameSlug`, with a colon-prefix title
  fallback only on structurally sports records. Absence of any configured game
  is a normal zero-result observation.

Participant grammar: HTML-unescape, NFKC, collapse whitespace; remove a
configured game alias only as a colon-terminated prefix; require exactly one
separator (`vs`, `vs.`, `versus`, `v.`, `@`) else `participant_parse_failed`;
strip a terminal `(BO<N>)` / `(Best of <N>)` annotation from the right
participant and an exact product suffix (`: Map N`, `: Total Maps`, ...) only
after the split. Hyphens inside team names are data.

Best-of is parsed from `BO<N>` / `best of N` (one or two digits). Conflicting
values in one event are `intra_event_format_conflict`. The count of listed map
markets is never used to infer a format. Kalshi rule text may supply secondary
activation evidence through one anchored `originally scheduled for <Month> <d>,
<yyyy> at <h>:<mm> <AM|PM> <EDT|EST|ET>` grammar (`America/New_York`; an
explicit `EDT`/`EST` must match the date's offset); all clauses in an event
must agree, otherwise `conflicting_rule_times` and no rule evidence. Rule
evidence never overwrites structured time.

## Matching

`matching.match_events` operates on anchors first, then siblings.

1. **Anchors.** An esports event is an anchor when it owns a series moneyline
   (or `soccer.moneyline_3way`); other esports fragments are siblings. Events
   with no game (soccer) are all treated as anchors. Only anchors create
   bundles.
2. **Identity.** Anchors partition by exact `(sport, game, topology, unordered
   canonical participant pair)`, so identical teams in different games never
   share a bundle. League labels differing is a warning
   (`league_labels_differ`), not identity. A participant-alias collision
   rejects the event.
3. **Time clusters.** Evidence pairs from distinct venues within
   `event_time_tolerance_seconds` (900) produce proposals at their median;
   clusters are non-transitive: the span of a whole cluster must stay within
   tolerance, so events at `t`, `t+10`, `t+20` minutes do not merge. An anchor
   supporting two distinct clusters is `activation_time_ambiguous`; two anchors
   from one venue in a cluster is `same_venue_anchor_ambiguous` (that venue is
   dropped from the cluster).
4. **Bundle.** A cluster with anchors on at least `minimum_venues` distinct
   venues becomes a bundle. Activation is the median of the supporting
   evidence. Evidence precedence: structured primary, structured secondary,
   reviewed rule-template evidence, then lexical field. A venue whose primary
   time disagrees but whose secondary evidence agrees is attached and the
   disagreement is kept as `activation_primary_conflict`; a venue that cannot
   support the cluster is `activation_time_conflict` and never vetoes two
   agreeing venues.
5. **Format.** Known unequal formats are `competition_format_mismatch`. One
   unique known format plus unknown ones resolves to the known value. An
   all-unknown bundle can match but cannot build an outcome space.
6. **Siblings.** After anchors are fixed, a sibling fragment attaches when
   sport, game, topology and participants equal the bundle, one of its
   evidence instants is within tolerance, its known format equals the bundle's,
   and it supports exactly one bundle. Otherwise `sibling_no_anchor` or
   `sibling_ambiguous`. Several fragments per venue may attach; siblings never
   change activation, count toward the venue minimum, or contribute anchor
   volume.
7. Fewer than two venues is `fewer_than_minimum_venues`. Rejections are listed
   in the report, never guessed through.

## Relationships

`relationships.py` adapts canonical markets into `analysis/` outcome spaces and
masks.

- Soccer uses a bounded regulation score grid whose cap derives from the listed
  lines; its coverage is always `INCOMPLETE_COVERAGE`.
- Esports uses an exhaustive reachable map-sequence space only when the bundle
  has one known first-to-clinch format in `SUPPORTED_BEST_OF` = {1, 3, 5, 7, 9}
  (a sequence ends when a side reaches `N//2 + 1` wins; counts 2, 6, 20, 70,
  252). Any other observed format (BO2, even, greater than 9) is preserved in
  the report and the candidate is rejected `unsupported_series_format`; the odd
  enumerator is never called. An esports bundle without an unambiguous format
  yields `series_scope_missing_unambiguous_best_of_format`.
- Positive handicaps keep handicap semantics (`Chelsea +1.5` includes a draw
  and a one-goal loss). A two-token handicap emits both complementary sides or
  fails closed.
- Correct-score parameters are translated from each venue's participant order
  to the bundle's before masks are compared.

The engine derives `IDENTITY`, `IMPLICATION`, `REVERSE_IMPLICATION`,
`MUTUAL_EXCLUSION` and `OVERLAP`. Only non-`OVERLAP` cross-venue relationships
make a market eligible and consume budget; `OVERLAP` stays as report evidence.
Findings are happy-path/conditional discovery evidence, not unconditional
arbitrage.

## Rule templates

`rules.py` reduces rule text to content-addressed templates: participants,
event title, dates, times and numeric parameters are replaced by placeholders,
and the template ID hashes the normalizer version, venue, sport, canonical
class and normalized text. A new ID is `UNREVIEWED`; listing it in
`known_rule_templates` makes it `KNOWN` (review status only, not semantics).
Several templates for one venue and class inside a bundle emit non-blocking
drift evidence (`rule_drift`). Changed postponement, cancellation or forfeit
clauses do not block capture.

The only automated blockers are explicit contradictions of the configured
scope: a regulation/full-time class whose rules include extra time or
penalties (`rules_include_extra_time_but_class_is_regulation`), a full-time
class that says first half only (`rules_are_first_half_but_class_is_fulltime`),
and a series class that says single map (`rules_are_single_map_but_class_is_series`).
A negated mention ("not including extra time") is not a contradiction.

Review workflow for a new vendor product: inspect normalized market and
template evidence from a shadow run; if the normal settlement archetype already
exists, update that class's venue pattern; if it is a genuinely different
scope, add a new class and resolver rather than widening a regex; add a small
adapter-shape test and a relationship test; optionally list reviewed template
IDs; run another shadow pass.

## Admission, exclusion and ranking

Event admission (`selection._candidate`) rejects with these reasons:

| Reason | Condition |
|---|---|
| `fewer_than_minimum_eligible_venues` | eligible markets cover fewer than `minimum_venues` |
| `no_cross_venue_structural_relationship` | no non-`OVERLAP` cross-venue relationship |
| `unsupported_series_format` | see Relationships |
| `series_scope_missing_unambiguous_best_of_format` | see Relationships |
| `combined_moneyline_volume_usd_below_minimum` | known USD/USDC moneyline volume below 25000 |
| `before_capture_lookahead` | capture start later than now + 660 s |
| `past_post_start_retention` | activation older than now - 21600 s |

The volume gate sums `volume_total_usd` over trusted moneyline anchors
(`moneyline_3way`, `series_moneyline`) only; map, total and handicap volume
never counts. A market with unknown dollar volume contributes nothing and is
counted under `moneyline_volume_usd_coverage` (`known_markets` /
`unknown_markets` per venue). Native fields (`volume_24h`, `volume_total`,
`liquidity`) are log-scaled activity ranking inputs with different units and
never enter the gate. Kalshi's `volume_fp` is a contract count and is not
itself dollars; the dollar figure used for Kalshi is the adapter estimate
described above.

Market-level checks are independent of event admission. A market is excluded
(recorded under `market_exclusions`, never vetoing the event or other
siblings) when it is not open for orders, younger than
`minimum_market_age_seconds` when creation time is known, contradicts its
normal scope, has invalid parameters (`invalid_product_parameters`), lies
outside the series (`product_outside_series_format`: map index above best-of,
totals/handicaps not separating reachable lengths), fails esports product
validation, or participates in no cross-venue relationship
(`no_modeled_cross_venue_relationship`). The event fails only when the
surviving set can no longer meet an event-level gate. A missing creation time
is not treated as proof of a new market.

Ranking is a lexical tuple, not a weighted scalar: venue coverage (3 venues
outrank any number of 2-venue bundles), cross-venue relationship quality
(`IDENTITY` 40, implications 20, `MUTUAL_EXCLUSION` 10), market-class breadth,
activity, then activation time and bundle ID. `score` and `score_components`
are reported diagnostics. Allocation is bundle-atomic against per-venue
subscription-ID budgets and `maximum_bundles`; a bundle that does not fit is
not partially selected and is reported under `selection.allocation_rejections`
as `target_budget_exceeded` or `maximum_bundles_reached`. Continuity-protected
bundles are allocated first; see `DELIVERY.md`.

## Selection report

Written as `selection_report.json[.zst]` with `report_version: 3`,
`mode: "shadow"`, `strategy_version`, `generated_at`, `run_id`,
`input_complete`, `discovery_failures`, `catalogs` (per-venue summaries with
`complete` and `classification_diagnostics`), `match_rejections`, `candidates`,
`continuity`, `selection` (`bundle_ids`, `targets` per venue, `budget_used`,
`allocation_rejections`, `publication_performed: false`) and
`finding_semantics`. Each candidate carries `event_status`
(`ELIGIBLE`/`REJECTED`), `rejection_reasons`, `admission` (threshold, combined
and per-venue known volume, coverage), `market_exclusions`,
`eligible_market_ids`, `relationship_analysis`, `rule_assessment`, and for
esports `game`, `topology`, `format_observed`, `best_of`, `format_status`
(`supported`/`unknown`/`conflicting`/`unsupported`) and `outcome_space_status`
(`exhaustive_normal_path`, `not_built_unknown_format`,
`not_built_format_conflict`, `not_built_unsupported_format`). Arrays are
deterministically sorted. `input_complete` is true only when every supported
venue produced a complete catalogue with no adapter failure.

## Tests

Offline and contract-shaped; no live APIs and no frozen vendor responses.

```bash
.venv/bin/python -m unittest tests.test_targeter_v2 tests.test_targeter_v2_lol \
  tests.test_targeter_v2_esports_games tests.test_masks
```

A live acceptance pass is `targeter/run_v2.py --mode shadow --no-response-cache`
(see `targeter/README.md`); its run directory is evidence for review, not a
fixture.
