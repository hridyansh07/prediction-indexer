# Target record backfill and write-on-change

Status: unimplemented. The `target_records_<venue>` artifact, its projection
and the archive inventory are implemented and documented in
`targeter/v2/DELIVERY.md`. This file keeps the two pending pieces.

## 1. Backfill tool

A separate tool (proposed `scripts/backfill_target_records.py`) that walks
archived runs, reads the selected markets from each, fetches the venue's
current record and writes rows in the shape of `DELIVERY.md` with
`provenance: "asserted"` (rows the targeter itself writes are `captured`).

- It never edits an archived run. Archive writes are immutable and amending an
  artifact would change the run digest and break the receipt and manifest
  chain. Output goes to a parallel location keyed by `run_id`.
- Fetching current records is sound for historical windows because every field
  in the projection is creation-time immutable (token IDs, outcomes, condition
  ID, end date); a fetch today returns the original terms for any market that
  was not delisted.
- `asserted` means "fetched later, believed immutable", not "what the targeter
  used, proven". It stays labelled in the data.
- Record a per-market outcome (`fetched`, `not_found`, `error`) so coverage is
  a number.
- Open: whether asserted rows live in a parallel per-run tree or one store
  keyed by `run_id`. Gate 1 already honours `--allow-asserted-records` (off by
  default: only `captured` rows count).

## 2. Write-on-change compaction

Rows are currently written for every selected market on every run (roughly 54
KB per run, small next to the run archive). Compaction would emit a row only
when a market's `projection_sha256` differs from the last row written for that
`target_id`, plus a tiny tick
`{"version": 1, "run_id", "venue", "target_id", "observed_at",
"unchanged_since_run_id", "projection_sha256"}` for unchanged markets so absence
is never readable as "not observed".

It needs a baseline (the previous run's projection hashes), which conflicts
with a one-shot process that carries no cross-run state and with the run reaper
deleting old runs at the 18 hour floor. It saves roughly 7.8 MB per day against
a much larger archive, so it is deferred. Adding it later is subtractive: it
only skips emitting rows. A venue with no declared projection (Kalshi) is never
compacted. A counter of projection changes per run would verify stability.
