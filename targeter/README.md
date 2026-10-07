# Targeter

The targeter decides which event families are worth recording and hands that
set to the venue splices. Capture is irreversible (an order book that was never
recorded cannot be rebuilt), so target selection is a resource allocation
decision: every unnecessary subscription costs network, disk and review time,
and every missed event leaves a hole no later model can repair.

Targeter v2 is the only targeter. It is a scheduled one-shot transaction, not a
daemon: each invocation starts from fresh vendor state, takes a filesystem
lease, and writes one timestamped run. A host scheduler (cron or a systemd
timer) owns cadence.

Detailed contracts:

| File | Contents |
|---|---|
| [`v2/SELECTION.md`](v2/SELECTION.md) | strategy configuration, venue adapters, esports game families, matching, relationships, rule templates, admission and ranking, report shape |
| [`v2/DELIVERY.md`](v2/DELIVERY.md) | run artifacts, target records, run archive, atomic publication, splice handoff, continuity and terminal eviction, scheduling, run archiver and reaper |

## What it selects

The unit of admission is an event, not a sibling market. Selection is
sports-first (soccer and esports) because sports have conservative event keys
(participants and a scheduled time), enumerable normal outcomes and product
families that repeat across venues. An event is admitted when:

1. the same participants (and, for esports, the same game) and a compatible
   start time match on at least two venues (three rank ahead of two);
2. its series or three-way moneyline anchors carry at least USD/USDC 25,000 of
   known combined lifetime volume;
3. at least one modeled cross-venue structural relationship survives
   (`IDENTITY`, either implication, or `MUTUAL_EXCLUSION`; plain `OVERLAP` is
   evidence only);
4. it is inside the capture window, which opens one hour before the scheduled
   start; and
5. it has not passed the post-start retention window.

Venue coverage, relationship quality, market-class breadth and activity then
rank admitted events under explicit per-venue subscription budgets. Allocation
is bundle-atomic.

Conservative rules the code enforces:

- **A moneyline anchors the event.** Only a series winner (esports) or
  three-way moneyline (soccer) can establish an event. Sibling markets (maps,
  totals, handicaps, spreads, correct score) attach afterwards and widen the
  capture surface but never create an event.
- **A sibling is not a veto.** Each market is judged independently. A closed,
  too-new, contradictory or unmodeled sibling lands in `market_exclusions`; the
  event fails only if the survivors can no longer satisfy an event-level gate.
- **Two venues minimum.** A third venue that conflicts or is malformed cannot
  stop two agreeing venues forming a bundle.
- **Dollar volume only.** Native contract counts and activity fields never
  enter the hard gate; only `volume_total_usd` does. Kalshi publishes a
  contract count, so its dollar figure is an adapter estimate
  (`volume_fp * last_price_dollars`); unknown dollar volume contributes zero
  and is reported as unknown coverage. See `v2/SELECTION.md`.
- **Fail closed on unknown shapes.** Unknown games, products, series formats
  and ambiguous times are visible false negatives with stable diagnostic codes,
  not guesses. A configured anchor-shaped record that cannot be classified
  makes that venue's catalogue incomplete.
- **Semantics come from configuration, not prose.** `configs/targeter_v2.json`
  maps reusable vendor product shapes to canonical classes
  (`soccer.moneyline_3way`, `soccer.spread`, `soccer.total_goals`,
  `soccer.both_teams_to_score`, `soccer.correct_score`,
  `esports.series_moneyline`, `esports.map_winner`, `esports.total_maps`,
  `esports.map_handicap`). No event IDs, team names or dates appear in
  production configuration. Rule text is normalized into content-addressed
  templates for drift review; only narrow contradictions of the configured
  normal scope block a market.
- **Relationships are conditional evidence.** Soccer uses a bounded score space
  and always carries `INCOMPLETE_COVERAGE`; esports series spaces are
  exhaustive only for normal BO1, BO3, BO5, BO7 and BO9 first-to-clinch series.
  Other formats (such as BO2) are preserved in reports and rejected as
  `unsupported_series_format`. No finding is an unconditional arbitrage claim.

Configured esports games: League of Legends, Counter-Strike 2, Dota 2 and
Valorant, on all three venues where the venue lists reviewed products
(Valorant has no Kalshi total-maps series). Honor of Kings is not configured
because no second venue publishes reviewable match products.

## Pipeline

```mermaid
flowchart LR
    A["Fresh venue catalogues"] --> B["Canonical events and markets"]
    B --> C["Cross-venue event matching"]
    C --> D["Rules and relationship evidence"]
    D --> E["Event admission and ranking"]
    E --> F["Timestamped selection report"]
    F --> G["Immutable run archive"]
    G --> H["Atomic target generation"]
    H --> I["Venue splices"]
```

Publication is deliberately harder than discovery. An incomplete vendor run, an
unrelated empty selection, an archive verification failure or a partial local
generation cannot replace the last valid target pointer. The only automatic
empty generation is one proving retirement of every prior continuity bundle.

Once published, a bundle has continuity priority over newcomers: each run
directly probes every committed market for terminal state, retains the whole
bundle while any venue is open or unknown, and retires it only when every
market is terminal or the eight-hour clamp from activation elapses.

## Modes

| Mode | Effect |
|---|---|
| `shadow` | fetch, normalize, match, write a local run (default) |
| `archive` | shadow plus immutable object-store archival |
| `publish` | archive, verify, atomically replace the live target generation |
| `audit` | verify the current generation and archive without discovery |

Exit `0` completed, `1` input incomplete (evidence preserved, nothing
published), `2` configuration, durability, archive, publication or integrity
failure.

## Running

One fresh shadow pass:

```bash
.venv/bin/python targeter/run_v2.py --mode shadow --strategy configs/targeter_v2.json
```

Repeated observation without retaining raw HTTP bodies:

```bash
.venv/bin/python targeter/run_v2.py \
  --mode shadow --no-response-cache \
  --strategy configs/targeter_v2.json \
  --cache-root data/targeter-v2-monitor-state \
  --output-root data/targeter-v2-shadow
```

`--no-response-cache` still makes live requests and keeps durable per-host
rate-limit state and the normalized artifacts. `--reuse-cache` is the explicit
offline/debug option and is mutually exclusive with it; do not use it to claim
live discovery. `--artifact-format ndjson` writes plain `.ndjson` artifacts and
`selection_report.json` for direct inspection (default: one Zstandard frame per
file with the shared `encoder` profile). `--max-kalshi-series`,
`--max-kalshi-pages`, `--max-polymarket-pages` and `--max-limitless-pages`
bound a probe and make the input incomplete.

A run directory (`<output-root>/<run-id>/`) holds
`catalog_<venue>_{events,markets}`, `target_records_<venue>`, `rule_templates`
and `rule_drift` NDJSON artifacts plus `selection_report.json.zst` and its
`selection_report.meta.json` commit marker. Review a run by decoding the report
and reading:

- `input_complete` and `discovery_failures`: whether every catalogue completed;
- `candidates`: one record per matched bundle, with `event_status`,
  `rejection_reasons`, `admission` (volume, threshold, known/unknown coverage),
  `market_exclusions` and `relationship_analysis`;
- `match_rejections`: events that never formed a bundle, with reasons;
- `selection.targets` and `selection.allocation_rejections`: the proposed
  subscription set and what budget dropped.

Shadow mode never changes a splice subscription and never uploads. Report
incomplete venue discovery as incomplete; do not retry within an acceptance
cycle in a way that hides the failed input snapshot.

## Layout

| Path | Responsibility |
|---|---|
| `run_v2.py` | launcher for `v2/run.py` |
| `v2/adapters/` | the only vendor boundary: Kalshi, Polymarket, Limitless catalogues and terminal probes |
| `v2/parsing/` | bounded text, esports label, best-of, product-parameter and traditional-fixture grammars |
| `v2/registry.py` | strict strategy and game-family loading |
| `v2/models.py` | canonical events, markets, snapshots, bundles, relationships |
| `v2/matching.py` | conservative cross-venue event matching |
| `v2/relationships.py` | outcome spaces, masks, structural relationships (uses `analysis/`) |
| `v2/rules.py` | rule templates, drift, explicit contradictions |
| `v2/selection.py` | admission, market exclusion, ranking, budgets, continuity allocation |
| `v2/continuity.py` | committed-generation bundles and terminal probe model |
| `v2/run.py` | one-shot discovery, report materialization, CLI |
| `v2/target_records.py` | verbatim venue records for selected markets |
| `v2/run_archive.py`, `v2/manifest.py` | immutable run archive, manifest and receipts |
| `v2/publication.py`, `v2/publication_validation.py` | verified atomic multi-venue publication and audit |
| `v2/replay_stream.py` | receipt-driven archived run and target-record streamers for replay |
| `v2/run_archiver.py`, `v2/run_archiver_cli.py` | sweep that archives unreceipted runs |
| `v2/run_reaper.py`, `v2/run_reaper_cli.py` | local run reclamation (audit by default) |
| `v2/lease.py` | `<output-root>/.targeter-v2.lock` overlap guard |
| `targets.py` | splice-side committed-generation reader and target writer |
| `coverage.py` | first-sighting ledger written after a publication commits |

Dependencies run one way: `targeter/` uses `archive/` (object-store protocol,
store factory, durable filesystem primitives) and `analysis/`; nothing under
`archive/` imports `targeter`.

## Tests

```bash
.venv/bin/python -m unittest tests.test_targeter_v2 tests.test_targeter_v2_lol \
  tests.test_targeter_v2_esports_games tests.test_targeter_v2_delivery \
  tests.test_targeter_v2_retention tests.test_targeter_replay_stream \
  tests.test_targets tests.test_masks
```

Tests are offline and use small hand-authored vendor shapes; do not freeze live
responses or volatile totals as fixtures.

## Rollout

Run shadow monitoring until the selector repeatedly finds complete, liquid,
multi-venue bundles and its rejection evidence looks credible. Then enable
`publish` against an independent archive, audit the committed generation with
`--mode audit` (Compose `targeter-v2-integrity`), and point the splices at the
pointer. Deployment is described in `v2/DELIVERY.md` and `docs/DEPLOYMENT.md`.
