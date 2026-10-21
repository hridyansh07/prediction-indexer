# Research build: compressed corpus outputs and multi-key runs

Status: **approved for implementation**. Extends
[RESEARCH_SITE_V1.md](RESEARCH_SITE_V1.md) and the research README. The research
build must read corpus run2: 1,212 committed bundles across four corpus keys, with
every economic and profile output stored as Zstandard.

## 1. Why

Run2 used the reference template's storage (#82):

- economic groups: `"storage": {"version": 1, "economic": "zstd"}`;
- profile: `"storage": {"version": 1, "profile": "zstd"}`.

With economic storage, every output file **and** every entity table is
committed as `<name>.zst` (`replay/economic_sdk/runtime.py`, `finish`). Examples
are `episodes.ndjson.zst`, `availability.ndjson.zst`, `probes.ndjson.zst`,
`entities.json.zst` and `reasons.json.zst`. The manifest lists the `.zst` names,
each with a logical and a stored identity. The profile uses manifest layout 3.

The research build today opens plain `episodes.ndjson`, `availability.ndjson`,
`probes.ndjson` and `entities.json` (`overlays.py`, `build.py`, `realism.py`).
On run2 it would crash or reject every probe-enabled bundle.

Run2 is also split across four keys, one per limit fix. All 1,212 bundles
verify, and a three-bundle rerun at 8717107 was byte-identical to the main key,
so the four keys form one corpus:

| Order | Key16 | Bundles | Code |
|---|---|---|---|
| 1 | `0be218a797a64c98` | 1118 | 1ee53fa |
| 2 | `b4f1e12438dc34be` | 88 | b3c5e44 |
| 3 | `de01c48d0cf4541e` | 4 | 95f0a99 |
| 4 | `8de7c45e618ff1dc` | 2 | 8717107 |

The key roots also hold 8 uncommitted failed-attempt directories for bundles
that later committed under a later key: 2 in key 1, 5 in key 2 and 1 in key 3.
The run's `run2-corpus-bundles.tsv` names the authoritative directory for every
bundle.

## 2. Base

Rebase `feat/research-site` onto `feat/corpus-runner` **plus**
`fix/snapshot-bounds` (PR #84, head 37ec91a). Readers need those bounds:

- `load_snapshot` accepts up to 64 MiB (b3c5e44);
- `MAX_ENTITY_TABLE` is 256 MiB (95f0a99).

The research build calls `replay.preparation.load_snapshot` (`game.py`) and reads
entity tables. If `fix/snapshot-bounds` has merged by then, rebase onto
`feat/corpus-runner` alone.

## 3. One reader for declared files

Add one helper, `replay/research/files.py`, that every research read goes
through. It takes a group output directory, its `manifest.json` and a logical
name such as `episodes.ndjson`, and resolves it exactly:

1. If the manifest's file inventory declares `<name>.zst`, read that file. Stream
   it through the shared `encoder` package's bounded decoder, which already
   backs `profile_stream_io.stream_lines`. Verify the logical identity
   (SHA-256, byte length, LF count) and the stored identity against the manifest
   entry. Reject truncation, concatenation, trailing data and checksum mismatch,
   as the encoder contract requires.
2. Otherwise, if the manifest declares plain `<name>`, read it plainly and verify
   its identity in the same way. This keeps older results, such as trial3 and
   run1, readable.
3. **Fail** when:
   - both forms are declared;
   - neither form is declared but a file of either form exists ("undeclared
     evidence");
   - a declared file is missing.
   These cases are never treated as absence.
4. Two access modes:
   - `lines(name)` streams NDJSON rows, bounded per line;
   - `document(name, limit)` returns one parsed JSON document. The decoded size
     is bounded by `limit`: `MAX_ENTITY_TABLE` for entity tables, and
     `MAX_METADATA` for reasons and summaries.

   Do not re-implement Zstd anywhere in `replay/research/`.

Replace every direct `open`, `read_json` or path-existence check on group output
files in `overlays.py`, `build.py` (availability), `realism.py` (probes and
volatility) and `series.py` (transitions) with this helper. The profile's
`transitions.ndjson.zst` and `volatility.ndjson.zst` reads already verify
identities, so move them onto the helper unchanged. Files outside group
outputs, such as `corpus_receipt.json`, `result.json`, `SUCCESS.json` and
`context/`, stay as they are.

## 4. Choosing the result for each bundle across keys

Replace "the last key directory wins" (`inputs.discover`) with:

- A bundle's **chosen** result is its single *committed* result, the one with a
  verified `corpus_receipt.json`, across all given key directories.
- Two or more committed results for the same bundle is an error, unless
  `--prefer-later` is passed. In that case the last committed one on the command
  line wins, and the others are listed as superseded.
- Uncommitted or invalid directories never win over a committed one. They are
  listed in the corpus manifest and `viability.md` as `superseded_uncommitted`,
  with their key prefix.
- A bundle with no committed result anywhere remains an explicit `uncommitted`
  event, as today.
- The existing check that each key directory holds exactly one corpus key stays.
- `events.parquet` gains `corpus_key` and `key_prefix` for every event, so every
  row names the run it came from. The corpus manifest lists all input keys, with
  bundle counts per key.

## 5. Filesystem hygiene

- Ignore AppleDouble `._*` files and `.DS_Store` wherever the build lists
  files: bundle discovery (already filtered by regex) and the
  rebuild-identity comparison in `build.py` (`rglob` over staging and target).
  Don't ignore any other unexpected file.
- `--out` should be on a POSIX filesystem. Document this in the README. The
  target is the spare machine's WSL ext4.
- The shared `storage.stored_lines` reader rejects any input path with a
  symlinked parent ("regular output required"). Resolve key-directory arguments
  with `Path.resolve()` once at the CLI, and document it, so a symlinked results
  root fails loudly rather than mysteriously. macOS `/var` → `/private/var` is
  the same issue in tests.

## 6. Tests (falsifying first)

1. **zstd economic group.** Write a fixture with the real
   `runtime.finish(storage=zstd)` path: episodes, availability, probes, entities
   and reasons as `.zst`. The research build produces the same episodes, probes
   and overlays as the same fixture written plain. It fails on the current branch.
2. **zstd profile, layout 3.** Transitions and volatility tiles build. Levels is
   absent and doesn't fail anything.
3. **Corruption.** Each of these is rejected with a precise message:
   - a stored-hash mismatch;
   - a logical-hash mismatch;
   - a truncated frame;
   - trailing bytes;
   - both forms declared;
   - an undeclared `.zst` present;
   - a declared file missing.
4. **Bounds.** An entity table over its limit is rejected. A 100 MB entity table
   under 256 MiB passes.
5. **Multi-key choice.** Bundle X is uncommitted in key 1 and committed in key 2,
   whatever the argument order: key 2 is chosen and key 1 is listed. Two committed
   results fail, or the later one wins with `--prefer-later`. A bundle never
   committed stays `uncommitted`.
6. **AppleDouble.** `._episodes.ndjson.zst` and `.DS_Store` in a key directory
   and in `--out` change nothing. An identical rebuild is still reused.
7. **Legacy.** The existing plain-file fixtures and test suites pass unchanged.

## 7. Acceptance

- The research and replay test gates pass. The independent checker
  (`scripts/research_independent_check.py`) reads through the same helper, or
  through its own decoder, never through the build's tables.
- After the push, on the spare machine where run2 lives (done by the corpus
  thread, not by the implementation runner), run `replay.research verify` and
  then `build` on a five-bundle sample: three from key 1, one from key 2, and the
  `cover_same` bundle from key 3. Then run the independent check. Report
  timings and output size.
- In that same spare-machine pass, cross-check the build's chosen `run_dir` per
  bundle against `run2-corpus-bundles.tsv`: all 1,212 must agree.
- No corpus, bench, Universe or venue run is part of this change. Results are
  read in place and never written.
