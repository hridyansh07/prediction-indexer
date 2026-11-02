# Targeter selection config (V1)

Status: **implemented on feat/targeter-selection-config**. Extends [`targeter/v2/SELECTION.md`](../../targeter/v2/SELECTION.md).
Schema: [`configs/targeter_v2.schema.json`](../../configs/targeter_v2.schema.json).

## 0. Goal

Targeter selects events by its strategy file alone. It does not know which
strategy or consumer reads its output. The same file format keeps feeding the
Indexer capture, and can also feed a consumer such as the executor that wants
"the single best CS2 event, compared by moneyline volume". That consumer keeps
its own strategy file. Building that file from research is the consumer's work,
not Targeter's.

V1 adds three optional parts to the strategy file:

1. **`scope`**: which sports, games and market classes may be selected.
2. **Market caps**: at most N markets per selected bundle, and at most M markets
   across the whole selection.
3. **`ranking`**: an ordered list of comparison keys, plus priority values
   supplied by the config itself.

**Compatibility rule.** A strategy file without any of these fields behaves
exactly as today. On the same inputs, its selection report is byte-identical
to the one from before this change.

Out of scope:
- the admission gates: 2 or 3 venues, the one-hour pre-activation window, and
  the USD 25,000 moneyline-volume gate;
- continuity-protected allocation, and publication's protected-floor check;
- the target-generation schema;
- the executor repo.

## 1. Strategy file additions

All fields are optional. Unknown keys anywhere in them are a fatal
`StrategyError`, the same as today.

```json
{
  "scope": {
    "sports": ["esports"],
    "games": ["counter_strike_2"],
    "market_classes": ["esports.series_moneyline", "esports.map_winner"]
  },
  "selection": {
    "maximum_bundles": 1,
    "maximum_markets_per_bundle": 6,
    "maximum_markets": 6
  },
  "ranking": {
    "keys": [
      {"key": "moneyline_volume_usd", "order": "desc"},
      {"key": "market_class_priority_max", "order": "desc"},
      {"key": "activation_time", "order": "asc"}
    ],
    "priorities": {
      "default": 0,
      "game": {"counter_strike_2": 10},
      "market_class": {"esports.series_moneyline": 5, "esports.map_winner": 3},
      "venue_set": {"kalshi+polymarket": 2, "kalshi+limitless+polymarket": 3}
    }
  }
}
```

### 1.1 `scope`

| Field | Meaning | Validation |
|---|---|---|
| `sports` | A bundle whose sport is not listed is rejected: `sport_outside_scope`. | Non-empty, unique, each listed in the top-level `sports`. |
| `games` | An **esports** bundle whose `game` is not listed is rejected: `game_outside_scope`. Bundles of other sports are unaffected. | Non-empty, unique, each a `game_families[].id`. |
| `market_classes` | A market whose `canonical_class` is not listed is excluded: `market_class_outside_scope`. | Non-empty, unique, each a `market_classes[].id`. |

- A rejected bundle stays in `candidates` as `REJECTED`, with the reason
  appended after the existing reasons.
- An excluded market is listed under `market_exclusions`, as today.
- **Order.** The class filter applies **after** the moneyline-volume gate is
  computed. The gate therefore still measures the event's moneyline liquidity
  even when moneylines are out of scope. It applies **before** relationship
  analysis, so only in-scope markets can form the cross-venue relationships
  that admit the event.

### 1.2 Market caps (in `selection`)

**`maximum_markets_per_bundle` (N).** This runs after the existing exclusions
and the scope filter, and before the event gates are evaluated.

1. Order the remaining markets by:
   - moneyline anchors first (`series_moneyline`, `moneyline_3way`);
   - then market-class priority, descending (§1.3, `default` when unlisted);
   - then activity, descending (the existing `_activity`);
   - then `target_id`, ascending.
2. Walk that order, while fewer than N are kept. A *partner* is a market with
   a cross-venue, non-`OVERLAP` relationship to this one.
   - If a partner is already kept, keep the market.
   - Otherwise, keep it together with its first not-yet-kept partner in the
     order, if both fit within N.
   - Otherwise, skip it.

   Every kept market therefore has a kept partner, and no slot goes to a
   market that would be left unpartnered.
3. Every market not kept is excluded as `market_cap_per_bundle`.
4. The event gates (`fewer_than_minimum_eligible_venues`,
   `no_cross_venue_structural_relationship`) are then evaluated on the kept
   set. A cap that breaks the event rejects it visibly through those reasons.

`eligible_market_ids` lists only the kept markets. Publication therefore
still sees selected targets equal to the eligible candidate markets.

**`maximum_markets` (M).** A bundle-atomic market budget, applied during
additive allocation only.
- Markets already allocated, including continuity-protected ones, count
  towards it.
- An additive candidate whose new markets would exceed M is skipped as
  `maximum_markets_reached`. Allocation continues, so a smaller later bundle
  can still fit, just as subscription budgets work today.
- Continuity-protected allocation and publication's protected-floor check do
  not change.

Both caps are positive integers.

### 1.3 `ranking`

`keys` is an ordered, non-empty list of `{key, order}`, where `order` is
`asc` or `desc`. A key may appear only once. `bundle_id` ascending is always
the last tie-break, added implicitly.

| Key | Value per eligible candidate |
|---|---|
| `venue_coverage` | `score_components.venue_coverage` |
| `cross_venue_relationships` | `score_components.cross_venue_relationships` |
| `market_class_breadth` | `score_components.market_class_breadth` |
| `activity` | `score_components.activity` |
| `moneyline_volume_usd` | `combined_moneyline_volume_usd` (the admission gate figure) |
| `activation_time` | the bundle's `activation_at` |
| `game_priority` | `priorities.game[bundle.game]`, else `default`; a bundle without a game gets `default` |
| `market_class_priority_max` | the largest class priority among eligible markets |
| `market_class_priority_sum` | the sum of class priorities over the distinct eligible classes |
| `venue_set_priority` | `priorities.venue_set[venues]`, where `venues` is the eligible venues sorted and joined by `+`; else `default` |

When `ranking` is absent, the order is today's:
1. `venue_coverage` desc
2. `cross_venue_relationships` desc
3. `market_class_breadth` desc
4. `activity` desc
5. `activation_time` asc
6. `bundle_id`

`priorities` is optional. When present:
- `default` is required;
- `game`, `market_class` and `venue_set` are optional;
- every value is an integer;
- every `game` key must be a game family id, and every `market_class` key a
  market class id;
- every `venue_set` key must be 2 or 3 distinct supported venues, sorted and
  joined by `+`.

Using a priority key without `priorities` is a fatal `StrategyError`.

Ranking stays a lexical tuple. No weighted sum decides the order.
`score` and `score_components` are still reported unchanged, and
continuity-protected bundles keep ordering by `score`, as today.

## 2. Report additions

These appear **only when the strategy file uses at least one V1 field**. A
file without them yields today's bytes. `report_version` stays 3, because
every addition is optional and existing readers read named fields only.

- `selection.configuration`: an echo of the resolved `scope`, both caps
  (`null` when unset), the effective `ranking.keys` including the implicit
  `bundle_id`, and `priorities`.
- On every candidate, `rank_values`: `{key: value}` for each configured key.
  Timestamps use `isoformat`. The values are absent for an ineligible
  candidate.

## 3. JSON schema

[`configs/targeter_v2.schema.json`](../../configs/targeter_v2.schema.json)
(draft 2020-12) documents the whole strategy file, including these fields.

- It is a guide for writing a config. `load_strategy` stays the authority,
  because cross-references (classes and games named in `scope` or
  `priorities`) and regex validity cannot be expressed in the schema.
- A test keeps them in step: each object's `properties` in the schema must
  equal the keys `load_strategy` accepts.
- [`configs/examples/targeter_v2_cs2_single_event.json`](../../configs/examples/targeter_v2_cs2_single_event.json)
  is a complete working example: CS2 only, one bundle, volume first.

## 4. Tests (offline, hand-authored shapes)

1. **Loader:** each new field is accepted; each invalid form is rejected,
   including unknown keys, unknown ids, duplicates, a bad venue set, a
   priority key without `priorities`, and non-integer or non-positive values.
2. **Compatibility:** the shipped config yields a selection result identical
   to the base code's on the existing fixtures. All existing Targeter tests
   pass unchanged.
3. **Scope:** a sport-filtered bundle, a game-filtered bundle and a
   class-filtered market, each visible with its reason. The class filter does
   not change the volume gate.
4. **Per-bundle cap:** anchors are kept first; priority order is respected;
   slots go to partnered pairs, not to unpartnered markets; a cap that leaves one venue
   rejects the event.
5. **Total cap:** a bundle skipped as `maximum_markets_reached`, and a smaller
   later bundle that is still selected.
6. **Ranking:** every key in both orders; the implicit `bundle_id` tie-break;
   five CS2 events that select a different single event under a volume-first
   config than under the default order.
7. **Report:** `configuration` and `rank_values` are present only with V1
   fields; publication validation accepts a capped selection.
8. **Schema:** the schema stays in step with the loader, and the example
   config loads.
