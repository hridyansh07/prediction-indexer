# Research packs

Builds the files behind the `/research` site from committed corpus results
([`replay/corpus`](../corpus/README.md)). It never runs a strategy and imports no
strategy runtime: it reads the outputs the corpus runner committed. Specification:
[RESEARCH_SITE_V1](../../docs/specs/RESEARCH_SITE_V1.md).

```bash
.venv/bin/python -m replay.research verify RESULTS/<key16> [RESULTS2/<key16> ...]
.venv/bin/python -m replay.research build  RESULTS/<key16> [...] --out .bench/research/out \
    [--config configs/research.json] [--game-state <local game-state archive root>]
```

- Pass key directories (`results_root/<corpus key[:16]>`). When a bundle appears
  in several, the **last** one on the command line wins; the others are listed as
  superseded in the corpus manifest and the viability report.
- Inputs are read in place and never written. Output goes only to `--out`.
- `build` writes one content-addressed version, `OUT/<version>/`, and then replaces
  `OUT/current.json`. An identical rebuild reuses the version; a differing one
  under the same version fails rather than rewrite it.

Every corpus result becomes a row of `events.parquet` with an explicit state
(`ok`, `uncommitted`, `receipt_invalid`, `context_invalid`), and every lens an
explicit state (`ok`, `failed`, `excluded`, `not_run`) with its reason. Only an
event whose `profile` lens is `ok` gets a pack under `events/<bundle_id>/`:
`manifest.json`, `overview.json`, `tiles/1s/*.json`, `tiles/raw/*.json`,
`availability.json`, `game.json` and one `overlays/<lens>.json` per ok episode lens.
The corpus level holds `manifest.json`, `events.parquet`, `episodes.parquet`,
`probes.parquet`, `volatility.parquet`, `summary.json`, `viability.md` and
`verify.json`.

Execution-realism evidence extends the offline tables and report. UI work is
deferred; the existing lens layers and charts are unchanged. Every episode has
nullable `pairing_class`, `settlement_review`, `stale_legs`, `edge_per_set` and
`t10_value`, plus `flags_state` and `quantity_exact`. A legacy input lacking flags
is explicitly `unavailable`, rather than a healthy empty stale-leg list.

`probes.parquet` has one row per recorded probe, joined by bundle, event, lens,
episode and scope, with game, markets, market types and eligibility flags. It
preserves every SDK probe field, including nested decision walks, quote ages and
arrival/excess/unwind legs. The derived fields `matched_sets`, `decision_sets`,
`value_exact`, `leg_loss_gross_exact` and `decision_edge_per_set` use exact
`step_contracts` and the configured lens monetary scale (36 for the reference
strategies). Native quantity atoms and native leg costs are never treated as
sets or common-unit money. Finite values are canonical decimal strings; a
nonterminating exact ratio uses reduced `numerator/denominator` notation, for
example `3.05 / 1067 = 61/21340`. Existing float columns remain display-only.

The delayed-fill report lists `t10` first and covers all settings and status
counts, followed by exact 1¢, 2¢ and 3¢ thresholds. A threshold uses the sizing's
own `decision.value / (decision.steps * step_contracts)`, compared as rational
numbers. It never substitutes an arrival value, another sizing's value or an
episode-opening value for a later decision. Older probes without decision value
remain in the unthresholded status table with `decision_edge_state: unavailable`.
The table records the mean fill ratio, median matched sets, sums of value,
gross unwind loss and net after loss, and the positive-event share; each is
grouped by game, pairing class and settlement review. Unresolved probes remain
in status counts and are excluded from ratio/money aggregates. Positive-event
share counts events with resolved net evidence in that selected group, and the
report shows that denominator. Duration views are labelled **non-causal research
context**; confirmation is the causal delayed decision.

`volatility.parquet` and each pack's `volatility.json` contain book and gap-pair
1 s and 3 s RMS and jump rates, derived from identity-verified profile bucket
rows. Exact `n` and `sum_sq4` are retained beside the derived floating-point RMS:
`sqrt(sum_sq4 / n) / 20000` in quote currency per contract. A recorded series
with no defined samples has explicit unavailable rows for both horizons. The
report joins loss to a gap only on the exact bundle, scope, both native book
orientations and probe settings, preserving the lens. A multi-leg basket or a
missing mask pair is unavailable. Missing probe/volatility files from legacy
results are explicitly unavailable in `summary.execution_realism.evidence` and
the report, never synthesized as zero. A declared, verified empty probe file is
available even when there were no fill episodes.

For a later UI consumer, `overlay.detail.probes` is
`{state: 'ok'|'unavailable', reason, items}`. Items preserve each SDK row and add
the exact derived fields above and `value_unit`; no browser economics is needed.
The overlay's top-level `probes` records file availability even with no episodes.

Probe ingestion checks the file identity, LF count, closed SDK row fields,
episode join and complete configured grid. It admits at most 1,000,000 probe
rows and 128 MiB source bytes per event, with a 1 MiB line limit. Retained probe
state has a conservative 512 MiB budget, enforced before each event row is
retained and again cumulatively across the build before an event is accepted.
The charge is `8192 + 16 * canonical_row_bytes`, reserving room for decoded
objects, Arrow copies and up to sixteen grouping links per row. Volatility is
streamed through the shared codec with at most 8,192 series, 4,096 pairs, eight
horizons and six jump thresholds. Exceeding a bound records a visible
`build_failed` pack and exits nonzero; no rows are truncated. Large inputs may
require separate builds or a separately reviewed resource-limit change.

Results committed without `context/` (before the corpus runner kept it) still get
packs, marked `context: missing`: the title is the bundle id, books are labelled by
instrument, and no cross-venue twins are known.

Contexts may pin outcomes document version 1 or version 2. The strict preparation
reader validates version-2 execution terms, including their change-point order,
when research loads committed game state or a local game-state archive. Reading
either version preserves the committed context bytes and identities.

Modules: `inputs` (discovery, attempts, lens states, context binding), `series`
(overview and tiles from `transitions.ndjson.zst`), `overlays` (the SDK episode
adapter), `game` (windowed game facts), `metadata` (titles, books, twins),
`tables` (Parquet, summary, viability report), `realism` (exact probe/volatility
adapters and aggregates), `build` (the version writer).

Tests: `.venv/bin/python -m unittest discover -s replay/tests -p 'test_research*.py'`.
