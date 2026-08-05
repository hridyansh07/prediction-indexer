# Targeter v2 — Multi-Game Esports Expansion V1

Status: proposed normative implementation contract.

This document extends `TARGETER_V2_LEAGUE_OF_LEGENDS_V1.md` from one reviewed
game family to four more:

- Counter-Strike 2;
- Dota 2;
- Honor of Kings;
- Valorant.

Honor of Kings is specified but **not shipped**: no venue pair publishes
reviewable match products for it, so `configs/targeter_v2.json` carries no
`honor_of_kings` family. The rules that name it remain normative for a future
configuration change; see §3.1 and §6.2.

The League of Legends contract remains authoritative for shared anchor
matching, activation consensus, sibling attachment, admission, publication,
and archive behavior unless this document explicitly replaces a rule. This is
an implementation specification, not a description of the current code.

## 1. Objective

Make the existing structured esports path reusable across game families. One
shared decoder and one shared best-of outcome engine must handle all five
reviewed games; configuration supplies game aliases and venue product IDs.

The implementation must:

1. discover all four requested games from current public catalogues;
2. keep the games in distinct identity partitions;
3. match the same real fixture across at least two venues using participant and
   activation evidence;
4. establish the fixture from series-moneyline anchors before attaching map,
   game, total, or handicap siblings;
5. derive relationships only when the published series format has a supported,
   exhaustive outcome space;
6. preserve the existing USD/USDC 25,000 combined series-moneyline volume gate;
7. preserve the existing one-hour pre-activation capture start;
8. fail closed, with stable diagnostics, when a venue or series format is not
   reviewed.

Production configuration must not contain a current event ID, market ID, team
name, tournament name, or event date. Live examples in section 14 are
operational evidence only.

## 2. Scope and non-goals

### 2.1 In scope

- the canonical games `counter_strike_2`, `dota_2`, `honor_of_kings`, and
  `valorant`;
- the existing `league_of_legends` family without behavior regressions;
- normal odd BO1, BO3, BO5, BO7, and BO9 series which stop when one side
  clinches;
- series winner, numbered map/game winner, series map/game handicap, and total
  maps/games products;
- exact game/product classification from structured venue evidence plus
  bounded label decoding;
- cross-venue fixture matching and event-level reporting;
- minimal contract-shaped tests and uncached live shadow acceptance.

### 2.2 Explicitly out of scope

- Dota 2 or CS2 fixed BO2 series, group-table points, and series draws;
- tournament-winner futures;
- exact series score;
- kills, rounds, pistol rounds, first blood, Roshan, barracks, player props,
  map-duration, or other within-map state;
- a runtime LLM, embeddings, edit-distance matching, or unreviewed fuzzy team
  resolution;
- a reviewer UI;
- fixture-specific configuration;
- changing the two-venue minimum, volume threshold, capture timing, splice
  protocol, or target publication format.

An even series is common enough that it must be identified correctly, but V1
must not pretend that the current first-to-clinch outcome space models it.
Normalize the observed format and reject relationship construction with
`unsupported_series_format`. A later fixed-map topology can add draw outcomes
without weakening this contract.

## 3. Live catalogue baseline

The following observations were verified against the official public APIs on
2026-08-04. They explain the initial mappings; they are not permanent fixtures.
The probe sources were the
[Kalshi Sports series catalogue](https://api.elections.kalshi.com/trade-api/v2/series?category=Sports&limit=1000),
[Polymarket esports event catalogue](https://gamma-api.polymarket.com/events/keyset?limit=100&closed=false&tag_slug=esports),
and the
[Limitless active sports catalogue](https://api.limitless.exchange/markets/active?page=1&limit=25&automationType=sports).

### 3.1 Kalshi

Reviewed active match-product series were:

| Game | Series winner | Map winner | Total maps |
|---|---|---|---|
| Counter-Strike 2 | `KXCS2GAME` | `KXCS2MAP` | `KXCS2TOTALMAPS` |
| Dota 2 | `KXDOTA2GAME` | `KXDOTA2MAP` | `KXDOTA2TOTALMAPS` |
| Valorant | `KXVALORANTGAME` | `KXVALORANTMAP` | not reviewed/published |
| Honor of Kings | not reviewed/published | not reviewed/published | not reviewed/published |

The Kalshi series catalogue also contains historical, tournament-future, and
similarly named IDs. They are not aliases for the rows above. In particular,
`KXCS2`, `KXDOTA2`, `KXVALORANT`, and `KXEWCHONOROFKINGS` are tournament-level
products and must not be treated as match anchors.

### 3.2 Polymarket

Current event prefixes and structured tags were:

| Game | Required title prefix | Required game tag |
|---|---|---|
| Counter-Strike 2 | `Counter-Strike:` | `counter-strike-2` |
| Dota 2 | `Dota 2:` | `dota-2` |
| Honor of Kings | `Honor of Kings:` | `honor-of-kings` |
| Valorant | `Valorant:` | `valorant` |

Every event must also carry the generic `esports` tag. The game-specific tag
and the configured colon-terminated prefix must agree. A missing or conflicting
pair fails closed.

Reviewed product labels were:

- `Match Winner`;
- `Map N Winner` or `Game N Winner`;
- `Map Handicap: ...` or `Game Handicap: ...`;
- `O/U X Games` or `O/U X Maps`.

Labels such as `Map 1 Total Rounds`, `Map 2 Rounds Handicap`, `First Blood in
Game 1?`, and `Total Kills ...` are distinct products and remain excluded.

### 3.3 Limitless

The active sports catalogue contained none of these four games during the
baseline probe. The existing structured metadata contract remains supported:
exact `metadata.esportTitle`/`metadata.videogameSlug`, structured teams and
activation, and exact `metadata.marketType`/`metadata.binaryMarketType`.
Absence is a normal zero-result observation, not evidence for invented aliases.

### 3.4 Availability consequence

An enabled game need not be supported by every venue. A venue with no reviewed
product mapping for a game is explicitly disabled for that pair. It contributes
neither a false catalogue failure nor a candidate.

As of the baseline, Honor of Kings can be normalized from Polymarket but cannot
be selected until Kalshi or Limitless publishes a reviewed equivalent. Its
one-venue events must remain visible as `fewer_than_minimum_venues` match
rejections.

## 4. Normative invariants

1. **Game is part of event identity.** Events with the same teams and time but
   different games can never share a bundle.
2. **Structured game evidence is mandatory.** Presentation text alone cannot
   turn a generic sports record into one of these families.
3. **Configuration is data, not code branches.** Adding a game must not add a
   `if family.id == ...` branch to an adapter or participant parser.
4. **Exact vendor product mappings outrank words.** A title containing `game`,
   `map`, or `winner` does not establish a product class.
5. **Only series winner establishes an anchor.** Every other supported product
   attaches after the anchor bundle exists.
6. **Sibling failure is local.** One invalid rounds/kill/map product does not
   reject its event or healthy siblings.
7. **Participant order is presentation, not identity.** Matching uses the
   unordered canonical pair; market-side orientation retains each venue's
   original order.
8. **Format syntax and topology support are separate.** Parse a published BO2
   as `2`, then reject it as unsupported. Do not erase it into `unknown` and do
   not run the odd-series enumerator.
9. **Unknown format is never inferred from listed maps.** A venue may omit the
   decider market before it is needed.
10. **A second venue remains mandatory.** Single-venue Honor of Kings discovery
    proves classification, not eligibility.
11. **Live availability is not a CI invariant.** CI fixes contracts and
    semantics; an uncached shadow run reports the current catalogue.
12. **The LoL path is not special.** Its existing behavior must pass through
    the same shared decoder after this change.

## 5. Canonical vocabulary

| Game | Canonical `game` | Sport | Topology |
|---|---|---|---|
| League of Legends | `league_of_legends` | `esports` | `best_of_series` |
| Counter-Strike 2 | `counter_strike_2` | `esports` | `best_of_series` |
| Dota 2 | `dota_2` | `esports` | `best_of_series` |
| Honor of Kings | `honor_of_kings` | `esports` | `best_of_series` |
| Valorant | `valorant` | `esports` | `best_of_series` |

All five reuse these existing canonical classes:

- `esports.series_moneyline`, scope `series`;
- `esports.map_winner`, scope `map`;
- `esports.total_maps`, scope `series`;
- `esports.map_handicap`, scope `series`.

`map_winner` and `total_maps` are canonical internal names. A venue may call a
unit a `game`; its published term is retained in classification evidence and
the original title.

## 6. Strategy version 3

Bump `configs/targeter_v2.json` from strategy version 2 to 3. Retain the
existing League of Legends family and add the four families below.

### 6.1 Alias table

The following aliases are exact after HTML unescape, Unicode NFKC,
case-folding, and whitespace collapse:

| Game | Kalshi | Polymarket | Limitless |
|---|---|---|---|
| `counter_strike_2` | `counter-strike 2`, `cs2` | `counter-strike`, `counter-strike 2`, `cs2` | `counter-strike 2`, `counter-strike-2`, `cs2` |
| `dota_2` | `dota 2`, `dota2` | `dota 2`, `dota2` | `dota 2`, `dota-2`, `dota2` |
| `honor_of_kings` | `honor of kings`, `honour of kings`, `hok` | `honor of kings`, `honour of kings`, `hok` | `honor of kings`, `honor-of-kings`, `honour of kings`, `hok` |
| `valorant` | `valorant` | `valorant` | `valorant` |

These aliases are bounded vendor spellings, not participant aliases. Do not
add `counter`, `strike`, `dota`, `honor`, `kings`, or `val` as standalone game
aliases.

Add a required `polymarket_game_tags` string array to every game family. Its
initial exact values are:

| Game | `polymarket_game_tags` |
|---|---|
| `league_of_legends` | `["league-of-legends"]` |
| `counter_strike_2` | `["counter-strike-2"]` |
| `dota_2` | `["dota-2"]` |
| `honor_of_kings` | `["honor-of-kings"]` |
| `valorant` | `["valorant"]` |

These are classification evidence, not extra discovery requests. Discovery
continues to paginate the generic configured `esports` tag once. The adapter
then requires the event to carry exactly the game tag belonging to the family
identified by its title prefix. If it carries a configured tag for another
family, emit `game_classification_conflict`.

### 6.2 Kalshi product mappings

Configure these exact mappings:

| Game | `series_ticker` | Canonical class |
|---|---|---|
| `counter_strike_2` | `KXCS2GAME` | `esports.series_moneyline` |
| `counter_strike_2` | `KXCS2MAP` | `esports.map_winner` |
| `counter_strike_2` | `KXCS2TOTALMAPS` | `esports.total_maps` |
| `dota_2` | `KXDOTA2GAME` | `esports.series_moneyline` |
| `dota_2` | `KXDOTA2MAP` | `esports.map_winner` |
| `dota_2` | `KXDOTA2TOTALMAPS` | `esports.total_maps` |
| `valorant` | `KXVALORANTGAME` | `esports.series_moneyline` |
| `valorant` | `KXVALORANTMAP` | `esports.map_winner` |

`honor_of_kings.venue_products.kalshi` would be an empty array. No other Kalshi
IDs are permitted without a reviewed strategy-version change. Because a family
reachable on one venue can never clear the two-venue minimum, the shipped
`configs/targeter_v2.json` omits the `honor_of_kings` family entirely rather
than discovering events that cannot be selected; the rules below stay
normative for the day a second venue publishes reviewable products.

### 6.3 Polymarket product mappings

Each of the four new families uses these fully anchored patterns:

```json
[
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
]
```

The game-specific tag requirement in section 3.2 is additional to these
product patterns.

### 6.4 Limitless product mappings

Each new family uses the existing exact enum mapping:

```json
[
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
```

Do not create title-only Limitless product mappings because no live record was
available to review.

### 6.5 Empty venue support

Change the strict loader as follows:

- `venue_game_aliases` and `venue_products` still name exactly `kalshi`,
  `polymarket`, and `limitless`;
- an alias list remains non-empty;
- `polymarket_game_tags` is required, non-empty, duplicate-free after
  normalization, and each value matches
  `^[a-z0-9]+(?:-[a-z0-9]+)*$`;
- one Polymarket game tag cannot belong to two game families;
- a product list may be empty;
- an empty product list means that venue/game pair is disabled;
- a disabled pair must not classify through a title alias fallback;
- a disabled pair does not cause `unclassified_anchor_candidate` merely
  because the venue publishes another game or a tournament future;
- each game family must have at least one non-empty venue product list;
- all other strategy-version-2 collision, regex anchoring, field allowlist,
  unknown-field, and canonical-class checks remain unchanged.

This is preferable to inventing an Honor of Kings Kalshi ticker.

## 7. Shared esports decoder

### 7.1 Code ownership

Game-specific parsing ownership is replaced by the `targeter/v2/parsing/`
package, scoped by grammar rather than by game or venue:

- `parsing/text.py` — text normalization and sport/league label classification;
- `parsing/esports.py` — exact game alias matching, bounded participant
  parsing, best-of syntax parsing, and the reviewed Kalshi
  `originally scheduled for ...` timestamp parser;
- `parsing/traditional.py` — single-contest fixture grammar for non-series
  sports;
- `parsing/products.py` — product parameter decoding keyed by canonical
  `market_type`.

None of it contains HTTP calls, event IDs, team names, or per-game branches.
Adapters select raw fields and construct evidence. The registry supplies the
game aliases and product mappings.

Landed: the transitional `league_of_legends.py` re-export module is deleted and
no adapter carries an `if family.id == "league_of_legends"` branch. One path
serves all configured families; a new game is a strategy-configuration entry.

### 7.2 Participant grammar

For a configured family:

1. HTML-unescape, NFKC-normalize, collapse whitespace, and trim.
2. Remove a configured game alias only when it is the complete start-of-title
   prefix followed by a colon.
3. Require exactly one fixture separator from `vs`, `vs.`, `versus`, `v.`, or
   `@`, surrounded by whitespace.
4. Split into exactly two non-empty participants.
5. From the right participant only, remove a terminal format/tournament suffix
   beginning with `(BO<N>)` or `(Best of <N>)`.
6. Remove an exact terminal supported product suffix only after the fixture
   split: `: Map N`, `: Game N`, `: Total Maps`, `: Total Games`,
   `: Map Handicap`, `: Game Handicap`, or `: Match Winner`.
7. Apply existing canonical participant normalization and configured
   participant aliases.

The format suffix grammar accepts any positive integer with at most two digits
for syntax and reporting. Topology validation happens later. Thus `(BO2)` is
removed from the participant but remains canonical `format="2"` and is
rejected as an unsupported outcome space.

Do not use the generic `" - "` truncation unless it follows the recognized
format annotation. Hyphens inside team names remain data.

### 7.3 Best-of evidence

Parse a positive integer from only:

```regex
\b(?:bo|best\s+of)\s*([1-9][0-9]?)\b
```

Return three states distinctly:

- a unique positive value;
- unknown because no reviewed expression exists;
- conflicting because two reviewed sources publish different values.

Conflicting values exclude the event with `intra_event_format_conflict`.
Unknown remains `None`. Do not infer a format from the count of listed map
markets, a total-map line, or a league default.

### 7.4 Kalshi rule time

Generalize the existing LoL rule parser without changing its grammar or time
zone rules. Rename parser ID to:

```text
kalshi_esports_originally_scheduled_v1
```

Apply it only after an exact configured Kalshi product mapping establishes one
of the five games. All parseable clauses within the event fragment must agree,
as in the LoL contract. The rule time remains secondary evidence and never
hides a conflicting structured primary.

## 8. Venue adapter rules

### 8.1 Kalshi

- Discover the Sports series catalogue once and classify only the exact
  configured series tickers.
- Require the canonicalized `Esports` tag.
- Do not classify tournament futures from a game alias.
- Parse participants from `event.title`; series title supplies supporting game
  evidence only.
- Preserve the LoL activation evidence precedence and rule-time consensus.
- A mapped `*GAME` series is an anchor even when its title says `Game` rather
  than `Game Winner`.
- A mapped `*MAP` series is a sibling. The map index must come from event or
  market structured/title evidence and be a positive integer.
- A mapped `*TOTALMAPS` series is a sibling. The line must be positive and an
  integer or half-step.
- Kalshi contract counts remain non-USD diagnostics. Only an explicit dollar
  field may populate `volume_total_usd`.

If a configured match-series ticker disappears, the run may return zero for
it. A new, unmapped series must not be guessed from a similar title.

### 8.2 Polymarket

- Continue keyset discovery under `tag_slug=esports`.
- Require both the generic `esports` tag and the exact configured
  game-specific tag.
- Require the configured colon-terminated event-title prefix and require it to
  identify the same family as the game tag.
- Parse participants through the shared decoder.
- Use only the family product mappings in section 6.3.
- Keep start-time precedence `eventStartTime`, then `startTime`, then
  `endDate`.
- Parse the best-of value from title or description using section 7.3.

Product validation remains local:

- series moneyline outcomes must identify both participants and align with
  token count;
- map/game winner requires a positive index;
- total games/maps requires a positive finite integer/half-step line;
- series handicap requires a finite non-zero line and orientable outcomes.

Within-map rounds/kill/prop labels must not match these mappings even when they
contain `Map`, `Game`, `Handicap`, or `O/U`.

### 8.3 Limitless

- Keep bounded page-number reconciliation and stable-ID deduplication.
- Retain unrelated payloads only as long as required by pagination and
  grouping; do not create a global response cache.
- Require a structurally sports record.
- Prefer exact `metadata.esportTitle` or `metadata.videogameSlug` over the
  colon-prefix title fallback.
- Prefer structured home/away teams, event ID, activation, and number of
  games.
- Only exact configured product enum values classify.
- `numberOfGames` is format evidence even when it is even. Do not discard BO2
  into unknown.

An active catalogue with zero configured records is complete if pagination is
complete.

## 9. Matching and bundle identity

The anchor and sibling phases from the LoL contract remain unchanged. Partition
anchors by:

```text
(sport, game, topology, unordered canonical participant keys)
```

The exact game field makes these false matches impossible:

- a Team Liquid Dota event with a Team Liquid CS2 event;
- a same-name academy team playing in LoL and Valorant;
- an Honor of Kings tournament future with a match event.

Use the existing 900-second activation tolerance and evidence clustering.
Reversed participant order is valid. Side orientation is calculated per event,
then translated to bundle sides through canonical participant keys.

Format consensus rules:

- one unique supported odd value plus unknown values resolves to the known
  value;
- all unknown values may form an identity bundle but cannot derive a series
  outcome space or become eligible;
- unequal known values reject with `competition_format_mismatch`;
- one unique even value may be reported as a matched bundle but is ineligible
  with `unsupported_series_format`;
- odd values greater than 9 are also unsupported in V1;
- a sibling with a conflicting known format does not attach.

The bundle ID retains the existing identity inputs. Game and topology are
already included and therefore separate the new families deterministically.

## 10. Outcome-space contract

### 10.1 Supported first-to-clinch universe

For supported `N in {1, 3, 5, 7, 9}`:

- target wins are `floor(N / 2) + 1`;
- enumerate H/A map or game winners recursively;
- stop each sequence when either side reaches target wins;
- record maps played, both win counts, winner side, and margin;
- mark the normal completion path `EXHAUSTIVE`;
- retain the existing cancellation, fair-price, void, and forfeit caveat.

This same mathematical universe is valid for the reviewed normal formats of
all five games. Game-specific scoring inside a map is irrelevant to these
four supported product classes.

### 10.2 Unsupported format safety

Before calling `build_series_space`, verify that the resolved format is one of
the supported values. For an even, zero, negative, greater-than-nine, or
otherwise unsupported value:

- do not call the odd sequence enumerator;
- do not compile a series-moneyline, total, handicap, or map-winner mask;
- add relationship diagnostic `unsupported_series_format` with the observed
  value;
- make the candidate ineligible with the same stable reason;
- keep the normalized events and markets for audit.

This check must prevent both a crash and a false exhaustive claim.

### 10.3 Parameter bounds

Before mask compilation:

- `1 <= map_index <= best_of`;
- total-map/game lines must separate at least two reachable sequence lengths;
- a handicap line must produce a non-empty, non-universal mask and orient both
  participant claims;
- products referring to unreachable maps are excluded as
  `product_outside_series_format`;
- a bad product does not veto the event.

Continue to derive the existing relationship kinds. Only non-overlap
cross-venue relationships can make a market eligible.

## 11. Admission and reporting

Do not change admission policy:

- minimum two eligible venues, three preferred;
- at least one modeled non-overlap cross-venue relationship;
- at least USD/USDC 25,000 of known lifetime series-moneyline volume;
- capture start at `activation_at - 3600 seconds`;
- current lookahead, post-start retention, budget, maturity, and open-order
  checks.

Only series-moneyline volume counts toward the hard threshold. Do not promote
the high total volume of within-map CS2 or Dota props into anchor volume.

Keep `report_version: 1` and write `strategy_version: 3`. Every candidate and
match rejection already carries `game` and `topology`; additionally serialize:

```json
{
  "format_observed": [3],
  "best_of": 3,
  "format_status": "supported",
  "outcome_space_status": "exhaustive_normal_path"
}
```

Allowed `format_status` values are:

- `supported`;
- `unknown`;
- `conflicting`;
- `unsupported`.

Allowed `outcome_space_status` values are:

- `exhaustive_normal_path`;
- `not_built_unknown_format`;
- `not_built_format_conflict`;
- `not_built_unsupported_format`.

Arrays and rejection details must be deterministically sorted.

Add or retain these stable diagnostics:

| Code | Level | Effect |
|---|---|---|
| `unsupported_game` | record | excluded, catalogue remains complete |
| `game_classification_conflict` | event | excluded, catalogue incomplete for a configured pair |
| `participant_parse_failed` | event | event excluded |
| `intra_event_format_conflict` | event | event excluded |
| `competition_format_mismatch` | match | anchors do not bundle |
| `unsupported_series_format` | candidate/relationship | normalized but ineligible; no outcome space |
| `series_scope_missing_unambiguous_best_of_format` | relationship | normalized but ineligible |
| `unclassified_anchor_candidate` | configured enabled venue | catalogue incomplete |
| `unclassified_sibling_product` | market | sibling excluded |
| `invalid_product_parameters` | market | market excluded |
| `product_outside_series_format` | market | market excluded |
| `fewer_than_minimum_venues` | match | no candidate |

Do not emit `unclassified_anchor_candidate` for a disabled venue/game pair.

## 12. Required tests

Tests must be written red before production changes. They use minimal
hand-authored contract shapes and never call live APIs.

### 12.1 Registry tests

- all five families load under strategy version 3;
- all aliases normalize to exactly one family;
- cross-family alias collision fails configuration;
- the exact Kalshi mappings in section 6.2 classify correctly;
- tournament IDs do not classify as match products;
- an empty Honor of Kings Kalshi product list is valid and disables fallback;
- all product lists empty for one family is invalid;
- generic esports registry patterns cannot override a family mapping.

### 12.2 Shared parser matrix

Run the same parameterized participant cases for every family:

| Input | Expected participants | Expected format |
|---|---|---|
| `Counter-Strike: A vs B (BO3) - Group D` | `A`, `B` | `3` |
| `Dota 2: A vs. B (BO3) - Playoffs` | `A`, `B` | `3` |
| `Honor of Kings: A versus B (BO5) - Group A` | `A`, `B` | `5` |
| `Valorant: A @ B (BO3) - Regular Season` | `A`, `B` | `3` |
| `A vs. B: Map 2` | `A`, `B` | unknown |
| `A vs. B: Total Games` | `A`, `B` | unknown |
| `Dota 2: A vs B (BO2) - Group Stage` | `A`, `B` | `2`, later unsupported |

Negative cases:

- no separator or multiple separators;
- an alias occurring inside a participant or tournament name;
- an unconfigured prefix;
- empty or canonically equal participants;
- malformed `BO0`, `BO-3`, or more than two digits;
- a product phrase which is not a terminal suffix;
- a hyphenated team name being truncated;
- conflicting BO values in two approved source fields.

### 12.3 Adapter contract matrix

For each supported venue/game pair prove:

- structured game evidence is required;
- evidence identifies the actual raw field and observed value;
- the series winner is an anchor;
- numbered map/game winner, total, and handicap products classify only through
  their exact mappings;
- an unsupported sibling is excluded without deleting its anchor;
- participant order and activation evidence are preserved;
- no per-game adapter branch is required.

Add explicit false tests for:

- `KXCS2` and `KXDOTA2` tournament futures;
- Polymarket CS2 `Map 1 Total Rounds` and `Map 1 Rounds Handicap`;
- Polymarket Dota `First Blood`, `Total Kills`, `Roshan`, and `Barracks`;
- a Polymarket event whose generic `esports` tag and game-specific tag
  disagree with its title prefix;
- an Honor of Kings Kalshi title with no configured product ticker;
- a Limitless title match without sports structure or game metadata.

### 12.4 Matching matrix

- reversed participant order matches within one game;
- equivalent Unicode spelling such as `Barca`/`Barça` follows the existing
  canonicalization;
- identical participants and activation in different games never match;
- BO3 plus unknown resolves to BO3;
- BO3 versus BO5 rejects;
- BO2 anchors can be identified without crashing but remain ineligible;
- map-only fragments never establish an event;
- one malformed sibling cannot reject sibling markets or the anchor bundle;
- two agreeing venues survive a third-venue activation conflict.

### 12.5 Outcome-space matrix

For BO1, BO3, BO5, and BO7 prove:

- exact terminal sequence counts are BO1 = 2, BO3 = 6, BO5 = 20, and
  BO7 = 70; BO9 = 252 in a focused enumerator unit test;
- every sequence stops at the first clinch;
- series winner, each reachable map winner, total maps, and series handicap
  compile to expected masks;
- a map index above `best_of` is excluded;
- relationships are invariant under reversed venue participant order.

For BO2 prove:

- no call reaches the odd-series enumerator;
- no mask or relationship is emitted;
- the report carries `unsupported_series_format`;
- the run completes instead of throwing.

### 12.6 End-to-end contract cases

Add one small two-venue case for each new family. Each case contains only the
fields consumed by adapters and uses invented IDs and teams.

Each case proves:

1. both venues normalize to one game-specific bundle;
2. the anchor is established before siblings attach;
3. at least one useful cross-venue relationship exists;
4. known moneyline USD/USDC volume is on the correct side of the configured
   threshold;
5. the event rejection or selection decision is correct;
6. deterministic normalized output is byte-identical on a repeated run.

The Honor of Kings unit case may use a synthetic Limitless record conforming
to its documented structured contract. Its separate live gate must still show
the true current one-venue limitation.

## 13. Implementation phases

### Phase A — red shared-parser and registry tests

Add the configuration, parser, disabled-venue, and false-classification tests.
Prove the current implementation fails because only LoL has a configured
family, Honor of Kings is not a generic prefix, and the current best-of parser
erases even formats.

### Phase B — generalize the model and decoder

- create the shared esports module;
- remove LoL-only adapter branches;
- bump strategy to version 3;
- add four families and empty-venue support;
- preserve LoL normalized output for equivalent inputs.

At this gate, adapters must emit correctly classified canonical events and
markets for all configured families.

### Phase C — format safety and outcome spaces

- distinguish syntactic format parsing from supported topology values;
- guard `build_series_space` before invocation;
- validate map/total/handicap bounds;
- add stable format and outcome-space report fields;
- prove BO2 cannot crash or produce a false exhaustive relationship.

### Phase D — end-to-end selection

- add one minimal two-venue contract case per game;
- confirm game-partitioned matching, sibling attachment, admission, budgets,
  and deterministic output;
- regenerate only derived strategy-version/report fixtures.

### Phase E — live acceptance

Run one uncached shadow cycle and preserve its normalized run directory. Do not
turn the returned market IDs or totals into CI fixtures.

## 14. Live acceptance procedure

Run:

```bash
.venv/bin/python targeter/run_v2.py \
  --mode shadow \
  --no-response-cache \
  --strategy configs/targeter_v2.json \
  --cache-root data/targeter-v2-monitor-state \
  --output-root data/targeter-v2-shadow
```

Do not use `--reuse-cache`, archive, or publish for this gate.

Review the resulting `selection_report.json` by game. Record:

- normalized event and market counts per venue;
- complete/incomplete input and every venue failure;
- match rejection counts;
- candidates and selected bundles;
- participants, activation, format, and event status;
- known combined series-moneyline USD volume versus 25,000;
- eligible-market count and market-exclusion reasons;
- classification and activation evidence.

The following fixtures were simultaneously visible on Kalshi and Polymarket
during the 2026-08-04 design probe and are useful manual checks only:

- Dota 2: BetBoom Team vs OG;
- Dota 2: Team Resilience vs PlayTime;
- CS2: Nuclear TigeRES vs Just Players;
- CS2: STATE vs INFURITY Gaming;
- CS2: GenOne vs Nemiga;
- Valorant: FOKUS Sakura vs Karmine Corp GC;
- Valorant: G2 Gozen vs Barca/Barça eSports GC.

Their order and spelling differ across venues. The implementation should
normalize those differences conservatively. It must not be changed merely to
force a current example to match; unexplained mismatches stay visible for
review.

Live acceptance succeeds when:

1. discovery completes for every venue;
2. currently co-listed CS2, Dota 2, or Valorant fixtures form game-correct
   multi-venue candidates rather than independent one-venue rejections;
3. product props outside the allowlist are excluded without catalogue damage;
4. candidate volume and capture-window decisions are correct;
5. Honor of Kings events are normalized and visibly rejected for insufficient
   venues when only Polymarket lists them;
6. no game crosses into another game's identity partition;
7. repeated uncached runs do not depend on a process-local response cache.

No minimum live selected-bundle count is a CI gate because listings, volume,
and activation windows are external state. A zero selected count is acceptable
only when the report explains every discovered event.

## 15. Verification commands

Focused gate:

```bash
.venv/bin/python -m unittest \
  tests.test_targeter_v2_esports_games \
  tests.test_targeter_v2_lol \
  tests.test_targeter_v2 \
  tests.test_masks \
  tests.test_targeter_v2_delivery
```

Full repository gate:

```bash
.venv/bin/python -m unittest discover -s tests
```

The implementation must not require a network call in either test command.

## 16. Completion criteria

The expansion is complete only when:

1. strategy version 3 contains all five reviewed game families;
2. adapters contain no game-ID-specific parsing branches;
3. exact current Kalshi match tickers classify and tournament futures do not;
4. Polymarket requires agreeing generic tag, game tag, and title prefix;
5. a disabled venue/game pair cannot be guessed through text;
6. each new game has a minimal end-to-end two-venue contract test;
7. different games with identical teams/time never share a bundle;
8. BO2 is reported and safely rejected rather than erased, crashed, or
   mis-modeled;
9. LoL regressions and the full Python suite remain green;
10. one uncached live run produces a reviewable per-game report and preserves
    its normalized run directory;
11. `targeter/README.md` is updated after implementation to list the supported
    games, supported products, disabled venue pairs, and odd-series limitation.
