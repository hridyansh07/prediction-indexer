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
`summary.json`, `viability.md` and `verify.json`.

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
`tables` (Parquet, summary, viability report), `build` (the version writer).

Tests: `.venv/bin/python -m unittest discover -s replay/tests -p 'test_research*.py'`.
