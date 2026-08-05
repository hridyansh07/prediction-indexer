# Targeter v2 — Structured League of Legends Discovery V1

Status: proposed normative implementation contract. This document supersedes
the generic esports classification and matching rules in
`TARGETER_V2_PHASES_1_5.md` for League of Legends only. It does not describe
the current implementation.

## 1. Objective

Implement League of Legends as an explicitly reviewed game family across
Kalshi, Polymarket, and Limitless. The targeter must discover one real match,
establish its cross-venue identity from series-winner anchors, attach supported
sibling products, derive relationships from an exhaustive best-of-series
outcome space, and apply the existing event-level admission policy.

The production implementation must not contain an event ticker, market ID,
team name, tournament name, or date belonging to one individual match. The
Shifters–Natus Vincere records described in section 14 are regression evidence,
not production configuration.

This specification fixes four proven failure classes:

1. a structurally valid Kalshi series such as `League of Legends Game` is
   dropped because its title does not contain `game winner`;
2. a presentation prefix such as `LoL:` becomes part of a Polymarket team name;
3. a Kalshi product suffix such as `: Map 1` or `: Total Maps` becomes part of a
   participant;
4. one venue's conflicting structured timestamp prevents two agreeing venues
   from forming the event.

## 2. Scope and non-goals

This version covers:

- the `league_of_legends` game family;
- `best_of_series` events with an odd known format;
- series winner, map winner, total maps, and map handicap products;
- all three current venues;
- structured classification, bounded vendor label parsing, time evidence,
  anchor matching, sibling attachment, reporting, and tests;
- the existing USD/USDC 25,000 series-moneyline admission gate;
- the existing one-hour pre-event capture policy.

This version does not cover:

- MLB or another traditional sport outcome space;
- Valorant, Counter-Strike, Dota, or another esports game family;
- fuzzy entity resolution, edit distance, embeddings, or runtime LLM calls;
- event-specific configuration or reviewer UI;
- crypto synthetic-book construction;
- trade execution or a claim that a structural relationship is executable;
- changes to the splice subscription or target publication protocols.

Other esports games must fail closed as unsupported game families until they
receive their own reviewed configuration and contract tests. They must not be
silently treated as League of Legends merely because they use maps or a best-of
format.

## 3. Normative invariants

1. **Structured facts establish the domain.** Vendor tags, stable product IDs,
   metadata enums, nested-market shape, and structured participant fields are
   consulted before presentation text.
2. **Regex is a bounded decoder, not semantic authority.** A label grammar may
   classify a product only after structured evidence has identified the event
   as the configured League of Legends family.
3. **Only an anchor can establish an event.** A series-winner product is an
   anchor. Map winners, totals, and handicaps are siblings and attach only after
   an anchor bundle exists.
4. **Two venues are sufficient and three are preferred.** A conflicting or
   malformed third venue cannot prevent two agreeing venues from forming a
   candidate.
5. **The time tolerance remains 900 seconds.** Do not widen it to accommodate a
   known four-hour discrepancy. Additional evidence may support the correct
   cluster; tolerance itself does not change.
6. **A sibling is never an event veto.** An unsupported or malformed sibling is
   excluded and reported. It does not reject a healthy anchor event.
7. **Best-of formats do not guess.** Known unequal formats are incompatible.
   An unknown format may join a bundle with one unique known odd format, but an
   all-unknown bundle cannot produce a series outcome space.
8. **Volume units remain strict.** Only explicit USD/USDC series-moneyline
   lifetime volume enters admission. Kalshi `volume_fp` remains a contract
   count and contributes no dollars.
9. **Every classification is auditable.** Normalized catalogue records state
   which raw field and configured mapping established the game and product.
10. **Unsupported records are visible.** A configured LoL record which looks
    like an anchor but cannot be classified makes that venue catalogue
    incomplete. It must not disappear as an ordinary zero-result run.

## 4. Canonical vocabulary

The following exact canonical values are used:

| Concept | Value |
|---|---|
| Sport | `esports` |
| Game | `league_of_legends` |
| Topology | `best_of_series` |
| Series scope | `series` |
| Anchor market type | `series_moneyline` |
| Sibling market types | `map_winner`, `total_maps`, `map_handicap` |
| Canonical classes | the existing `esports.series_moneyline`, `esports.map_winner`, `esports.total_maps`, `esports.map_handicap` |

The game family and the product class are separate dimensions. A future
Valorant implementation may reuse the same canonical product classes and
series topology, but it must identify itself as a different game family.

## 5. Strategy configuration contract

Increment `configs/targeter_v2.json` from strategy version `1` to `2` and add a
strict `game_families` array. League of Legends must be configured as follows;
the shown structure is normative, although ordering is not significant.

```json
{
  "game_families": [
    {
      "id": "league_of_legends",
      "sport": "esports",
      "topology": "best_of_series",
      "venue_game_aliases": {
        "kalshi": ["league of legends"],
        "polymarket": ["lol", "league of legends"],
        "limitless": ["lol", "league of legends"]
      },
      "venue_products": {
        "kalshi": [
          {
            "field": "series_ticker",
            "values": ["KXLOLGAME"],
            "canonical_class": "esports.series_moneyline"
          },
          {
            "field": "series_ticker",
            "values": ["KXLOLMAP"],
            "canonical_class": "esports.map_winner"
          },
          {
            "field": "series_ticker",
            "values": ["KXLOLTOTALMAPS"],
            "canonical_class": "esports.total_maps"
          }
        ],
        "polymarket": [
          {
            "field": "group_title",
            "patterns": ["^match winner$"],
            "canonical_class": "esports.series_moneyline"
          },
          {
            "field": "group_title",
            "patterns": ["^(?:game|map)\\s+(?P<index>[1-9][0-9]*)\\s+winner$"],
            "canonical_class": "esports.map_winner"
          },
          {
            "field": "group_title",
            "patterns": ["^o/u\\s+(?P<line>[0-9]+(?:\\.5)?)\\s+(?:games|maps)$"],
            "canonical_class": "esports.total_maps"
          },
          {
            "field": "group_title",
            "patterns": ["^(?:game|map)\\s+handicap(?::.*)?$"],
            "canonical_class": "esports.map_handicap"
          }
        ],
        "limitless": [
          {
            "field": "metadata_market_type",
            "values": ["match_winner"],
            "canonical_class": "esports.series_moneyline"
          },
          {
            "field": "metadata_market_type",
            "values": ["map_winner"],
            "canonical_class": "esports.map_winner"
          },
          {
            "field": "metadata_market_type",
            "values": ["total_maps"],
            "canonical_class": "esports.total_maps"
          },
          {
            "field": "metadata_market_type",
            "values": ["map_handicap"],
            "canonical_class": "esports.map_handicap"
          }
        ]
      }
    }
  ]
}
```

Patterns use Python `re` syntax, including `(?P<name>...)` named groups. The
loader must not silently translate another regex dialect.

### 5.1 Configuration validation

The loader must reject:

- unknown fields at every game-family level;
- a game-family ID which is not lower snake case;
- duplicate game-family IDs;
- a sport other than `esports` for this topology;
- an unsupported topology;
- a missing venue or empty alias/product list;
- aliases which collide across configured game families after canonical text
  normalization;
- a product mapping which references an unknown canonical class;
- a venue field not on that venue's explicit field allowlist;
- a mapping with neither or both of `values` and `patterns`;
- duplicate exact values for one venue field;
- a Polymarket product pattern which is not anchored with both `^` and `$`;
- an invalid regular expression;
- two exact mappings capable of classifying the same stable field value as
  different classes.

The only allowed product fields in this version are:

| Venue | Allowed product field |
|---|---|
| Kalshi | `series_ticker` |
| Polymarket | `group_title` |
| Limitless | `metadata_market_type` |

Kalshi stable identifiers are stripped and compared byte-for-byte. Limitless
enum values are stripped and case-folded. Polymarket label patterns receive
HTML-unescaped, NFKC-normalized, whitespace-collapsed text but no participant
or product words are removed before the pattern is evaluated.

Runtime overlap between regex patterns is a record-level
`ambiguous_product_classification`; the record is not guessed.

### 5.2 One semantic authority

The new game-family product mapping is the classification authority for
configured esports games. Remove the generic esports `venue_patterns` from the
old `market_classes` path, or make the registry provably bypass them whenever
`CanonicalEvent.game` is non-null. There must not be a structured result and a
second generic-regex result which can disagree.

Soccer remains on the existing registry until its own structured migration is
specified.

### 5.3 Venue game-alias matching

Normalize aliases and observed values with NFKC, case-folding, and collapsed
whitespace. Then apply these venue-specific rules only:

- **Kalshi:** an exact configured `series_ticker` product mapping establishes
  the enclosing game family by itself. Series-title evidence is additional and
  matches only when the title begins with a configured game alias followed by
  end-of-string or one of the exact reviewed remainders `game`, `map winner`,
  `total maps`, or `map handicap`.
- **Polymarket:** the configured alias must be the complete start-of-title
  prefix immediately followed by a colon. The esports tag is independently
  required.
- **Limitless:** a configured alias must equal the complete normalized value of
  `metadata.esportTitle` or `metadata.videogameSlug`. A title fallback uses the
  same colon-prefix rule as Polymarket and is permitted only on a structurally
  sports record.

Substring search elsewhere in a title is forbidden. In particular, a team or
tournament mentioning `LoL` does not establish the game family.

## 6. Canonical model extensions

Add these immutable types to `targeter/v2/domain.py`:

```python
@dataclass(frozen=True)
class ClassificationEvidence:
    mapping_id: str
    source_field: str
    observed_value: str


@dataclass(frozen=True)
class ActivationEvidence:
    instant: datetime
    source_kind: str       # "structured" or "rule_template"
    source_field: str
    primary: bool
    parser_id: str | None = None


@dataclass(frozen=True)
class TargeterDiagnostic:
    code: str
    venue: str
    source_ref: str
    severity: str           # "warning" or "error"
    completeness_effect: bool
    details: Mapping[str, object]
```

`mapping_id` is deterministic and human-readable. Product evidence uses:

```text
<game-family>:<venue>:<canonical-class>:<source-field>
```

Game evidence uses:

```text
<game-family>:<venue>:game:<source-field>
```

Examples:

```text
league_of_legends:kalshi:esports.series_moneyline:series_ticker
league_of_legends:polymarket:esports.map_winner:group_title
```

Extend `CanonicalEvent` with required fields for esports records:

```python
game: str | None
topology: str | None
game_evidence: tuple[ClassificationEvidence, ...]
activation_evidence: tuple[ActivationEvidence, ...]
```

Soccer may use `None`, `None`, and empty tuples until migrated. A League of
Legends event must use the exact game/topology values from section 4, have at
least one game-evidence item, and have at least one activation-evidence item.
`activation_at` remains the adapter's preferred local value and must equal one
of the event's evidence instants.

Extend `CanonicalMarket` with:

```python
classification_evidence: ClassificationEvidence | None
```

It is required for League of Legends markets. `canonical_class`,
`market_type`, and `scope` remain the downstream interface.

Extend `CatalogSnapshot` with a separate
`classification_diagnostics: tuple[TargeterDiagnostic, ...]`. Do not replace
the existing operational pagination `diagnostics` strings in this phase. The
catalogue summary serializes classification diagnostics as objects, sorted by
code, source reference, and canonical JSON of `details`.

A diagnostic with `completeness_effect=true` forces that snapshot's
`complete=false`. Code must not infer completeness impact from severity or
from a human-readable message.

Extend `EventBundle` with:

```python
game: str | None
topology: str | None
activation_support: tuple[dict[str, object], ...]
activation_conflicts: tuple[dict[str, object], ...]
```

The serialized event record must add:

```json
{
  "game": "league_of_legends",
  "topology": "best_of_series",
  "game_evidence": [
    {
      "mapping_id": "league_of_legends:polymarket:game:event_tag_and_prefix",
      "source_field": "tags,title_prefix",
      "observed_value": "esports|LoL"
    }
  ],
  "activation_evidence": [
    {
      "instant": "2026-08-03T15:00:00Z",
      "source_kind": "structured",
      "source_field": "eventStartTime",
      "primary": true,
      "parser_id": null
    }
  ]
}
```

The serialized market record must add:

```json
{
  "classification_evidence": {
    "mapping_id": "league_of_legends:polymarket:esports.series_moneyline:group_title",
    "source_field": "group_title",
    "observed_value": "Match Winner"
  }
}
```

All timestamps are aware UTC instants. Evidence arrays are deterministically
sorted by instant, source kind, source field, and parser ID before writing.

A serialized classification diagnostic has exactly this shape:

```json
{
  "code": "unclassified_anchor_candidate",
  "venue": "kalshi",
  "source_ref": "/series/KXLOLGAME",
  "severity": "error",
  "completeness_effect": true,
  "details": {
    "game": "league_of_legends",
    "series_ticker": "KXLOLGAME"
  }
}
```

Every `details` value must be JSON-serializable. Diagnostic construction must
reject non-finite numbers and non-string mapping keys.

## 7. Common decoding pipeline

Every venue adapter must apply the following order:

1. Verify the vendor response envelope and pagination contract.
2. Establish `sport=esports` from structured vendor category/tag metadata.
3. Establish `game=league_of_legends` from the configured venue evidence.
4. Extract the two participants from structured fields when available;
   otherwise use the bounded LoL fixture grammar in section 9.
5. Collect structured and approved rule-template activation evidence.
6. Classify each product through the configured venue-product mappings.
7. Parse only the parameters required by that already-known product class.
8. Validate subscription identifiers, outcomes, status, volume units, and
   product parameters.
9. Emit canonical events and markets plus classification diagnostics.

An arbitrary title match may not perform steps 2 or 3 by itself. Product text
may not be parsed until steps 2 and 3 have succeeded.

## 8. Venue contracts

### 8.1 Kalshi

#### Game and product classification

- Require series `category == "Sports"` and a canonicalized `Esports` tag.
- Collect game evidence independently from the configured stable
  `series_ticker` mapping and the configured League of Legends series-title
  alias. Either exact evidence source may establish LoL. If two available
  sources identify different configured games, mark the catalogue incomplete
  with `game_classification_conflict`.
- Product classification uses exact `series_ticker`, never a search for
  `winner` in `series.title`.
- `KXLOLGAME` therefore classifies as `esports.series_moneyline` even though
  the current series title is `League of Legends Game`.
- An unmapped Kalshi LoL series whose `product_metadata.scope` and nested
  markets look like a two-team game anchor emits
  `unclassified_anchor_candidate` and makes the Kalshi catalogue incomplete.
- Another unmapped LoL series is a reported `unclassified_sibling_product` and
  does not invalidate known anchors.

#### Participants

Parse `event.title`, not individual market questions. Apply the LoL prefix and
product-suffix grammar before splitting the fixture. Preserve the event's
published participant order for side-orientation logic.

#### Time evidence

Choose structured evidence in this order:

1. `event.strike_date` when present;
2. nested `market.occurrence_datetime`;
3. nested `market.expected_expiration_time` as a fallback;
4. nested `market.close_time` as a final fallback.

Use the first field containing one unique parseable instant and mark it
primary. If the highest available field contains multiple distinct instants,
retain those values as conflict details, emit
`intra_event_activation_conflict`, and try the next field. Exclude the event if
no field supplies one unique instant and no reviewed rule evidence exists.

For League of Legends only, add a strict reviewed parser for the Kalshi rule
clause:

```text
originally scheduled for <Month> <day>, <year> at <hour>:<minute> <AM|PM> <EDT|EST|ET>
```

Requirements:

- use a single anchored phrase matcher, not general date extraction;
- use `zoneinfo.ZoneInfo("America/New_York")` for `ET` and verify explicit
  `EDT`/`EST` offsets against that date;
- require every nested market in the event with a parseable clause to yield
  the same instant; this applies to anchor and sibling event fragments;
- emit one secondary `rule_template` evidence item with parser ID
  `kalshi_lol_originally_scheduled_v1` only when they agree;
- if parseable clauses disagree, emit `conflicting_rule_times` and emit no
  rule evidence;
- never overwrite or hide the structured time.

#### Best-of

Parse a published `BO1`, `BO3`, `BO5`, or `best of N` only after the game is
known. If Kalshi publishes no format, keep `format=None`; do not infer BO3 from
the number of currently listed map markets.

### 8.2 Polymarket

#### Discovery and game classification

- Continue keyset pagination with `tag_slug=esports` and the configured time
  bounds.
- Require an esports tag before consulting a game alias.
- Establish League of Legends from a configured, colon-terminated event-title
  prefix (`LoL:` or `League of Legends:`) or another structured game field if
  Polymarket adds one later.
- Strip the recognized prefix before participant parsing. `LoL:` must never
  become part of a participant key.
- An esports event without a configured game-family match is unsupported, not
  generic LoL.

#### Product classification

Classify `raw_market.groupItemTitle` using only the anchored patterns in the
configured LoL family. `event.title` is not a moneyline fallback once the
structured game-family path is active.

Required parameter validation:

- `series_moneyline`: outcomes identify both event participants and token
  count equals outcome count;
- `map_winner`: named `index` exists and is an integer greater than zero;
- `total_maps`: named `line` is positive, finite, and an integer or half-step;
- `map_handicap`: a finite non-zero line is present and all aligned outcome
  sides can be oriented.

An invalid market is excluded with `invalid_product_parameters`; it does not
remove the event or its other markets.

#### Activation and format

- Primary activation precedence remains `eventStartTime`, then `startTime`,
  then `endDate`. Use only the first available parseable field as structured
  activation evidence; lower-priority expiration fields must not become extra
  matching instants when a start field is already available.
- Parse an odd best-of value from the reviewed `BO<N>`/`best of N` grammar in
  event title or description.

### 8.3 Limitless

#### Discovery and bounded retention

- Call `/markets/active` with `automationType=sports`.
- Keep the existing stable-ID pagination reconciliation, but retain full raw
  payloads only for records which match a configured game family or are
  pending attachment to such a group. A set of stable IDs for deduplication is
  permitted; retaining every unrelated market payload is not.
- A count change remains a bounded two-pass reconciliation, not a fatal error
  and not an unbounded retry.

#### Game and event identity

- Require `automationType == "sports"` when the field is present.
- Establish League of Legends from normalized exact values in
  `metadata.esportTitle`, `metadata.videogameSlug`, or an equivalent configured
  metadata field. Title fallback is allowed only inside a structurally sports
  record and only through the configured game aliases.
- Prefer structured `metadata.homeTeam` and `metadata.awayTeam` over title
  parsing.
- Prefer `metadata.eventId` as the reusable vendor event identity shared by a
  group and standalone products. Use participant pair plus expiration only
  when `eventId` is absent.

#### Product, activation, and format

- Product classification uses exact `metadata.marketType` or
  `metadata.binaryMarketType` through the configured mapping.
- Use `metadata.startMatchTimestampInUTC` as primary structured activation
  evidence.
- Use positive odd `metadata.numberOfGames` as best-of; the reviewed title
  grammar is fallback evidence only.
- Retain only CLOB products with a usable slug as subscription targets. An AMM
  product may remain classification evidence but is not accepting orders for
  this capture pipeline.

## 9. Bounded League of Legends label grammar

All text is first HTML-unescaped, Unicode NFKC-normalized, and whitespace
collapsed. Matching is case-insensitive.

### 9.1 Game prefixes

Only remove a configured game alias when it appears at the start of the event
title and is followed by a colon:

```regex
^(?:lol|league\s+of\s+legends)\s*:\s*
```

Do not remove an alias appearing inside a team or tournament name.

### 9.2 Fixture separators

After prefix removal, require exactly one of:

```text
 vs 
 vs. 
 versus 
 v. 
 @ 
```

Zero or multiple separators is `participant_parse_failed`. Do not guess.

### 9.3 Product suffixes

The following suffixes may be removed only from the end of the complete event
title and only after the fixture separator has been found:

```regex
\s*:\s*(?:(?:map|game)\s+[1-9][0-9]*|total\s+(?:maps|games)|(?:map|game)\s+handicap|match\s+winner)\s*$
```

This turns `Shifters vs. Natus Vincere: Map 1` into the participants
`Shifters` and `Natus Vincere`. It must not alter `Giants Gaming`, `Maple`, or
another legitimate participant containing a product-like word.

### 9.4 Format and tournament suffixes

After participant splitting, remove a terminal format/tournament annotation
from the right participant only when it begins with a recognized format:

```regex
\s*\((?:bo|best\s+of)\s*[13579]\)(?:\s+-\s+.*)?$
```

A general `" - "` truncation is not permitted for LoL participant parsing
unless it follows the recognized format annotation.

### 9.5 Canonical participants

Apply the existing Unicode-safe participant canonicalization and reviewed
`participant_aliases` after parsing. No fuzzy matching is introduced. Alias
collisions remain fatal configuration errors.

## 10. Anchor matching and activation consensus

### 10.1 Anchor definition

A canonical event is an anchor when it owns at least one trusted
`series_moneyline` market. Map, total, and handicap-only event fragments are
siblings.

Only anchors participate in initial cross-venue bundle creation. This prevents
a mislabeled Map 1 product from inventing a real-world match.

### 10.2 Identity partition

Partition anchors by exact:

```text
(sport, game, topology, unordered reviewed participant keys)
```

League labels are warnings, not identity. Known best-of formats are checked
after time clustering. A participant-alias collision rejects that event as it
does today.

### 10.3 Time proposals

Within one identity partition:

1. For every pair of anchors from distinct venues, enumerate their activation
   evidence pairs.
2. An evidence pair supports a proposal when the absolute difference is at
   most `event_time_tolerance_seconds`.
3. The proposal instant is the median of the two evidence instants.
4. Sort proposals and form non-transitive clusters whose maximum minus minimum
   is at most the tolerance. Start a new cluster when adding a proposal would
   exceed that total span.
5. Collapse clusters with the same participating event references and choose
   their median proposal instant.

An anchor supports a proposal cluster when at least one of its evidence
instants is within tolerance of the cluster instant. If more than one evidence
item qualifies, choose by this precedence:

1. structured primary;
2. structured secondary;
3. reviewed rule-template evidence;
4. lexical `source_field`, then lexical `parser_id` as deterministic ties.

If one anchor supports more than one distinct eligible time cluster, exclude
it with `activation_time_ambiguous`. Do not use it to bridge the clusters.

If one venue supplies more than one anchor for the same cluster, exclude that
venue from the cluster with `same_venue_anchor_ambiguous`. The remaining
venues may still form a bundle when the minimum is met.

### 10.4 Bundle creation

A cluster becomes a bundle only when:

- it contains anchors from at least the configured minimum distinct venues;
- all non-null formats have one unique positive odd value;
- the event references are unambiguous.

The bundle activation is the median of the selected supporting evidence, not
the median of every event's local `activation_at`. Its bundle ID is computed
from:

```json
{
  "sport": "esports",
  "game": "league_of_legends",
  "topology": "best_of_series",
  "participants": ["<sorted-key-1>", "<sorted-key-2>"],
  "activation": "<unix-seconds>"
}
```

If an attached event's primary evidence lies outside tolerance but reviewed
secondary evidence supports the bundle, record an `activation_primary_conflict`
entry. Preserve both instants and their sources.

If two venues agree and a third cannot support the cluster, create the
two-venue bundle and report the third as `activation_time_conflict`. The third
venue cannot veto the bundle.

## 11. Sibling attachment

After anchor bundles are immutable, consider sibling event fragments.

A sibling attaches when all are true:

1. sport, game, topology, and reviewed participant keys equal the bundle;
2. at least one sibling activation evidence item is within tolerance of the
   bundle activation;
3. its non-null best-of format equals the bundle's known format;
4. it supports exactly one bundle.

If it supports no bundle, report `sibling_no_anchor`. If it supports more than
one, report `sibling_ambiguous` and attach it to neither. An activation-primary
conflict is recorded with the same rule as anchors.

Several fragments from one venue may attach to one bundle. They do not change
the bundle activation, do not establish the event's minimum venue count, and
do not contribute series-moneyline anchor volume unless they actually contain
a classified `series_moneyline` market.

An excluded sibling market does not remove its event or another sibling.

## 12. Outcome space and relationships

Use the existing exhaustive series sequence model, generalized in naming from
the current Dota comment to best-of esports.

For a bundle with unique `best_of=N`:

- enumerate every reachable H/A map-winner sequence;
- stop a sequence when either side reaches `floor(N/2) + 1` wins;
- mark coverage `EXHAUSTIVE` for the normal completed-series path;
- retain the existing happy-path caveat for cancellations, refunds, fair-price
  settlements, and post-start forfeits.

Parameter requirements before mask compilation:

- `series_moneyline`: a uniquely oriented participant claim;
- `map_winner`: `1 <= map_index <= best_of` and a uniquely oriented side;
- `total_maps`: a positive finite integer/half-step line;
- `map_handicap`: a finite non-zero line with both sides oriented for a
  multi-token condition.

The engine continues to derive `IDENTITY`, `IMPLICATION`,
`REVERSE_IMPLICATION`, `MUTUAL_EXCLUSION`, and `OVERLAP`. Only a non-overlap
cross-venue relationship can make a market eligible for capture.

A product outside the reachable best-of space is excluded as
`product_outside_series_format`; it does not veto the event.

## 13. Admission and reporting

### 13.1 Admission

Preserve the existing event gates:

- at least two eligible venues;
- at least one useful cross-venue relationship;
- combined known series-moneyline lifetime volume at least USD/USDC 25,000;
- capture start inside the run lookahead;
- event not beyond post-start retention.

Only `series_moneyline` is an anchor volume type. Map-winner, total-map, and
handicap volume does not satisfy the hard gate. Polymarket explicit dollar
volume and Limitless formatted USDC volume count. Kalshi contract counts are
reported as unknown dollar coverage.

### 13.2 Selection report additions

Keep `report_version: 1`; this is an additive pre-deployment extension. Bump
`strategy_version` to `2` and regenerate derived run fixtures. Phase 6–10
readers must continue accepting report version 1 and must compare against the
new strategy version as they do today.

Every LoL candidate record must add:

```json
{
  "game": "league_of_legends",
  "topology": "best_of_series",
  "best_of": 3,
  "activation_resolution": {
    "chosen_at": "2026-08-03T15:00:00Z",
    "support": [
      {
        "venue": "polymarket",
        "event_id": "...",
        "instant": "2026-08-03T15:00:00Z",
        "source_kind": "structured",
        "source_field": "eventStartTime"
      }
    ],
    "conflicts": [
      {
        "venue": "kalshi",
        "event_id": "...",
        "primary_instant": "2026-08-03T19:00:00Z",
        "supporting_instant": "2026-08-03T15:00:00Z",
        "code": "activation_primary_conflict"
      }
    ]
  }
}
```

`best_of` is an integer or `null`. Support and conflicts are deterministically
sorted by venue, event ID, instant, and source field.

Extend `MatchRejection` with optional `game` and `details`. Existing rejection
codes retain their meaning. Add these exact codes:

| Code | Level | Effect |
|---|---|---|
| `unsupported_game` | record | excluded; configured run remains complete |
| `game_classification_conflict` | catalogue | catalogue incomplete |
| `unclassified_anchor_candidate` | catalogue | catalogue incomplete |
| `unclassified_sibling_product` | market | sibling excluded |
| `ambiguous_product_classification` | market/anchor | anchor makes catalogue incomplete; sibling does not |
| `invalid_product_parameters` | market | market excluded |
| `participant_parse_failed` | event | event excluded |
| `intra_event_activation_conflict` | event | retained only if another unambiguous evidence source exists |
| `conflicting_rule_times` | event | rule evidence discarded |
| `activation_time_conflict` | match | event remains unmatched; agreeing bundle survives |
| `activation_time_ambiguous` | match | event attaches to no bundle |
| `same_venue_anchor_ambiguous` | match | ambiguous venue excluded from that cluster |
| `sibling_no_anchor` | match | sibling excluded |
| `sibling_ambiguous` | match | sibling excluded |
| `product_outside_series_format` | market | market excluded |

Do not encode these as free-form prose only. Human-readable context may
accompany the stable code.

### 13.3 Publication compatibility

The target files consumed by splices do not need a game field in this phase.
`bundle_id`, `canonical_class`, subscriptions, activation, capture start, and
source reference remain sufficient. Archive and publication verification must
accept the additive catalogue/candidate fields without weakening existing
identity, path, or selected-market equality checks.

## 14. Required Shifters–Natus Vincere regression

Add a minimal, hand-authored contract-shaped regression. It may use the real
team names because they make the observed failure recognizable, but it must
contain only fields read by the adapters. Do not check in complete live HTTP
responses.

The input represents:

- Kalshi series `KXLOLGAME`, title `League of Legends Game`, tags `Esports`,
  participants `Shifters` and `Natus Vincere`, structured activation `19:00Z`,
  and a reviewed rule timestamp `15:00Z`;
- Kalshi map and total series with titles ending `: Map 1`, `: Map 2`, and
  `: Total Maps`;
- Polymarket event `LoL: Natus Vincere vs Shifters (BO3) - LEC Regular
  Season`, activation `15:00Z`, and match/map/total/handicap children;
- Limitless structured group with game metadata, participants, event ID,
  `numberOfGames=3`, activation `15:00Z`, and match-winner children.

Use a fixed run time inside the capture lookahead; for example
`2026-08-03T13:50:00Z` for a `15:00Z` activation, a one-hour capture lead, and
the current 660-second scheduler guard.

The regression must prove:

1. every venue emits participant keys exactly `natus vincere` and `shifters`;
2. `LoL:` and all product suffixes are absent from participant keys;
3. Kalshi `KXLOLGAME` is a `series_moneyline` anchor;
4. Polymarket and Limitless establish the bundle at `15:00Z` without Kalshi;
5. Kalshi then supports the same bundle through reviewed rule-time evidence;
6. the bundle records Kalshi's `19:00Z` structured primary as a conflict;
7. the unique known format is BO3;
8. map and total fragments attach as siblings to the same bundle;
9. a Polymarket-only handicap with no cross-venue relationship is excluded
   without rejecting the event;
10. known series-moneyline USD/USDC volume exceeds the configured threshold;
11. Kalshi contract volume contributes zero dollars and appears as unknown
    dollar coverage;
12. the candidate is eligible and selected when budgets permit;
13. the selected market set equals the markets participating in useful
    cross-venue relationships;
14. no event ID, team name, or timestamp from this regression appears in a
    production module or strategy configuration.

## 15. Test matrix

### 15.1 Configuration tests

- valid League of Legends family loads under strategy version 2;
- unknown fields and unanchored Polymarket patterns fail;
- alias collisions fail;
- duplicate exact product identifiers fail;
- ambiguous runtime product matches fail closed with the stable diagnostic;
- generic esports registry patterns cannot override a game-family result.

### 15.2 Parser tests

Positive cases:

- `LoL: Natus Vincere vs Shifters (BO3) - LEC Regular Season`;
- `League of Legends: A vs. B (Best of 5)`;
- `Shifters vs. Natus Vincere: Map 1`;
- `Shifters vs. Natus Vincere: Game 2`;
- `Shifters vs. Natus Vincere: Total Maps`.

Negative cases:

- no separator and multiple separators;
- an unsupported prefix such as `Valorant:`;
- participant names containing `Gaming`, `Maple`, or `Game`;
- even and malformed best-of values;
- a product-like phrase which is not a terminal suffix.

### 15.3 Adapter contract tests

For each venue, prove:

- game-family evidence is required;
- every supported product maps to the expected canonical class;
- an unknown anchor-shaped product changes catalogue completeness;
- an unknown sibling is diagnostic only;
- required parameters and subscription identifiers are enforced;
- structured participant/time fields outrank title fallbacks;
- classification evidence names the actual source field and observed value.

Limitless tests must also prove `automationType=sports` is sent and unrelated
raw market payloads are not retained by the adapter.

### 15.4 Matching tests

- two structured agreeing anchors form a bundle;
- two agreeing venues survive a third primary-time conflict;
- reviewed secondary time evidence attaches the conflicting venue;
- a rule time alone cannot merge different participant pairs;
- same teams at two real times on one day remain two bundles;
- one event supporting two clusters is excluded as ambiguous;
- known BO3 and BO5 anchors reject each other;
- known BO3 plus unknown format resolves to BO3;
- siblings attach after anchors and cannot establish an event alone;
- a malformed sibling never rejects its anchor bundle.

### 15.5 Relationship and selection tests

- exhaustive BO1, BO3, and BO5 sequence counts are correct;
- series winner, each reachable map winner, totals, and handicaps compile to
  the expected masks;
- out-of-format map indices are excluded;
- cross-venue identities and implications are derived;
- overlap alone does not activate a market;
- only explicit dollar/USDC anchor volume enters the threshold;
- capture lookahead and event-level budget behavior remain unchanged;
- split ordering and deterministic IDs remain byte-stable across repeated
  runs with identical inputs.

### 15.6 Fixture policy

CI tests use minimal builders and hand-authored contract shapes. They must not
call live APIs and must not assert volatile catalogue totals or current market
availability. A live uncached shadow run is an operational acceptance gate,
not a golden fixture.

## 16. Implementation phases

### 16.1 Required code ownership

Keep raw vendor interpretation inside adapters and shared LoL semantics outside
them:

| Path | Required responsibility |
|---|---|
| `targeter/v2/registry.py` | strict game-family schema, exact mappings, compiled bounded patterns, collision validation |
| `targeter/v2/domain.py` | game/topology fields, classification/time evidence, structured diagnostics, serialization invariants |
| `targeter/v2/parsing/esports.py` | game alias normalization, best-of fixture grammar, reviewed Kalshi rule-time parser; no HTTP calls. Shared by every configured family — there is no LoL-specific parser module |
| `targeter/v2/adapters.py` | raw-field extraction, per-venue evidence construction, pagination, record-local failure policy |
| `targeter/v2/matching.py` | anchor partitioning, time proposals, conflict handling, bundle creation, sibling attachment |
| `targeter/v2/relationships.py` | best-of product-to-mask validation and existing series-space integration |
| `targeter/v2/selection.py` | unchanged admission semantics plus new candidate report fields |
| `targeter/v2/run.py` | deterministic catalogue/report serialization of additive evidence |
| `targeter/v2/run_archive.py` and `publication.py` | accept and verify strategy version 2 and additive report/catalogue fields without weakening existing gates |
| `tests/test_targeter_v2_lol.py` | focused unit, adapter, matching, relationship, selection, and end-to-end contract cases |

Do not create three independent LoL parsers inside the venue adapter classes.
Adapters select and name raw fields; `parsing/esports.py` applies the shared,
reviewed grammar and `parsing/products.py` the product-parameter rules.

### Phase A — red regressions

Add the Shifters–NAVI end-to-end regression and the parser/configuration matrix.
Prove the present implementation fails because:

- `KXLOLGAME` is not classified;
- `LoL:` survives participant parsing;
- map/total suffixes survive participant parsing;
- the agreeing venues do not form a bundle.

Do not change production code before these failures are demonstrated.

### Phase B — model, configuration, and venue classification

Implement strategy version 2, canonical evidence types, strict configuration
validation, and structured game/product classification for all three venues.
At the end of this phase, all normalized records are correct, but time consensus
and sibling attachment may still be red.

### Phase C — activation consensus and sibling attachment

Implement reviewed Kalshi rule-time evidence, anchor proposal clustering,
conflict reporting, and post-anchor sibling attachment. Preserve the existing
900-second tolerance.

### Phase D — relationship, selection, and delivery compatibility

Generalize the series outcome-space naming, validate product parameters against
best-of, extend reports, regenerate derived fixtures, and run phase 6–10 archive
and publication tests against strategy version 2.

### Phase E — live acceptance

Run one uncached shadow discovery. The implementation is accepted only when:

- input is complete for all enabled venues;
- at least one currently listed multi-venue LoL fixture appears as one
  candidate rather than several one-venue rejections; use Shifters–NAVI when
  it remains active, but do not make live acceptance depend on an expired
  event;
- Polymarket and Limitless form the event at minimum;
- Kalshi is attached through consistent reviewed evidence or is retained as an
  explicit activation conflict;
- volume and capture-window reasons are accurate;
- map/total siblings attach without participant pollution;
- unrelated esports games are reported unsupported rather than misclassified;
- no new participant, time, or format ambiguity produces a false bundle.

The live result is evidence for review and must not be copied into a CI fixture.

## 17. Verification commands

Minimum repository gate:

```bash
.venv/bin/python -m unittest \
  tests.test_targeter_v2_lol \
  tests.test_targeter_v2 \
  tests.test_masks \
  tests.test_targeter_v2_delivery

.venv/bin/python -m unittest discover -s tests
```

Operational gate:

```bash
.venv/bin/python targeter/run_v2.py \
  --mode shadow \
  --no-response-cache \
  --strategy configs/targeter_v2.json \
  --cache-root data/targeter-v2-monitor-state \
  --output-root data/targeter-v2-shadow
```

Do not use `--reuse-cache` for the live gate. Preserve the normalized run
directory and review its selection report, classification diagnostics, time
evidence, candidate admission, and exact selected market set.

## 18. Completion criteria

The work is complete only when:

1. League of Legends is a configured game family on all three venues;
2. no generic esports title regex can override its structured classification;
3. unsupported esports games fail closed;
4. anchor matching and sibling attachment are distinct phases;
5. two agreeing venues survive a third-venue time conflict;
6. every time rescue preserves the conflicting primary evidence;
7. the Shifters–NAVI regression passes end to end;
8. full Python and delivery suites pass;
9. one uncached live shadow run produces reviewable LoL candidates;
10. `TARGETER_V2_PHASES_1_5.md` and `targeter/README.md` are updated to describe
    the implemented structured game-family behavior rather than the former
    generic esports-regex path.
