# Venue event time: implementation scope and verification

Source: [the specification](VENUE_EVENT_TIME.md), with the user's 2026-10-08
clarifications incorporated. Implementation is offline and retained-run verified. The retained
September 27–28 baseline corpus passes materialization and interpretation
comparison; the October 3 sending_ts_ms fixture passes timestamp acceptance and
restores all 20 planned Kalshi books in the real Risk engine. Linux Redis and
strategy smoke runs now pass within the scope recorded below.

## Settled scope

- Annotate Kalshi book/trade events and Polymarket WebSocket book/trade events.
- Keep REST AuditAnchor semantics and source_observed_ns unchanged.
- Keep required timestamp and REST cursor checks. No timestamp is inferred.
- Emit annotation inconsistency/extraction diagnostics to stderr for manual
  review. No diagnostic sidecar, new receipt profile, or fault class is needed.
- Preserve canonical clocks/order, child indexes, grouping, Risk state,
  validity, revisions and cut boundaries. No strategy timing behavior changes.

## Implementation

The domain writer selects SegmentRecord schema 4. VenueTime validates its closed
four-field representation, the event-time/resolution/kind all-or-none rule, and
at least an event or send clock. FullBook, BookDelta and TradeEvent add explicit
nullable venue_time; constructors preserve their existing signatures.

Frozen schema-3 wire types support both strict canonical audits and the
version-directed single-pass pinned reader. Re-encoding an old record keeps its
exact old bytes. Existing profile-1/schema-3 and profile-2/schema-3 derivatives
remain readable; profile 2 also admits schema 4. Each event's schema must match
its manifest. Manifest/receipt keys, address algorithms and commit ordering do
not change.

Kalshi v6 accepts sending_ts_ms on every supported venue message. Deltas parse
exact RFC3339 UTC/offset times, declare resolution from fractional digit count,
and compare the millisecond floor against ts_ms. Trades use ts_ms and check
whole-second ts consistency. Snapshot children share the send-only annotation.
An annotation inconsistency removes the event-time trio, independently retaining
valid send time and the original book/trade operation. Newly unparseable or
unrepresentable annotation values produce manual diagnostics rather than a
rejected delivery. Existing wire-field rejects retain their failure order.

Polymarket v4 labels price-change children book_update, trades trade_report, and
WebSocket books book_as_of. Millisecond conversion is checked and original
snapshot source_observed_ns remains present. Required-field rules stay strict.
Limitless v3 emits null annotations and preserves its current interpretation.

Rust transport publishes annotations only in market-event values, with integer
clocks as exact decimal strings. Transition operations use an explicit
projection retaining the previous key set. Python strict protocol helpers check
nullable clocks, enum values, closed key sets and the VenueTime invariants;
the documented trusted-publisher hot-path guard policy is unchanged.

The runner obtains producer identity from materialize_range --describe; its
checked-in config has no normalizer version pins to edit. The supervisor's
profile-2 metadata preflight now accepts schemas 3 and 4 and requires matching
receipt/manifest versions. New identities/schema produce new immutable
derivative addresses. Production capture, canonical
windows, archives, Targeter and Universe schemas are unchanged.

## Verification

- The initial sending_ts_ms regression was run against v5 and failed explicitly
  for unknown_field before production code changed. It passes after the fix.
- Offline contract tests cover exact and trimmed fractional times, ISO offsets,
  fallback, inconsistency, unsupported precision/overflow, message-child
  propagation, ignored/control messages, all Polymarket labels and required
  timestamps, and unchanged REST/Limitless semantics.
- Frozen conformance hashes still match the previous implementation after
  removing only venue_time and normalizing schema/parser versions. Original
  historical pins/golden fixtures remain intact where they test old readers.
- Domain tests exercise both decode paths, exact schema-3 bytes, required
  schema-4 fields, unknown/malformed annotations, and u64 bounds.
- Materializer tests cover old/new schema coexistence, independent audit, and
  rejection of a rebound event schema that disagrees with its manifest.
- Risk compares annotations present/absent across both venues, ties, trades,
  duplicates, underflow and recovery: same cuts, states, revisions,
  dispositions, canonical references and dependencies after normalizing pins.
- Rust transport regenerates its small offline golden through the real
  materializer, walker and Risk engine. Python independently validates the
  annotated golden, exact integers, transition schemas and final book digest.

Validation outcomes (2026-10-08):

- `CARGO_TARGET_DIR=engine/target cargo test --offline --manifest-path engine/Cargo.toml --workspace`:
  passed; 233 tests passed and 8 explicit Redis integration cases ignored.
- Final added compatibility/message-type tests were verified with
  `cargo test --offline --manifest-path engine/Cargo.toml -p replay-materialize -p replay-normalizers`:
  passed; 39 materializer tests, 88 normalizer tests, 7 example tests, and 3
  compile-fail documentation tests.
- `CARGO_TARGET_DIR=engine/target cargo clippy --offline --manifest-path engine/Cargo.toml --workspace --all-targets --all-features -- -D warnings`:
  passed.
- `cargo fmt --manifest-path engine/Cargo.toml --all --check` and
  `git diff --check`: passed.
- `.venv/bin/python -m unittest replay.tests.test_streams replay.tests.test_supervisor`:
  passed; the final run after the schema-4 bench regressions ran 39 tests,
  including 13 optional Redis-dependent skips.
- The inconsistent-time regression was also run with `--nocapture`: both delta
  and trade emitted diagnostic=inconsistent_event_time while preserving their
  original interpretation.
- The transport golden was regenerated deliberately with
  `REPLAY_UPDATE_GOLDEN=1`, then passed ordinary byte-equality verification.

The full Python test suites were not run. A local project .venv was created
using available system packages; extra bundle/cache checks initially lacked
google_crc32c. The subsequent smoke used the main checkout's project virtual
environment, which has that dependency, while importing this worktree's code.

## Retained baseline smoke (2026-10-08)

Inputs: the main checkout's existing .bench/canon, six certified canonical
windows from 1790548200000000000 through 1790559000000000000. Existing canonical
and derivative artifacts were read without mutation. Fresh outputs and the
reproducible scan/preflight scripts are retained at
`/private/tmp/venue-time-bench-smoke-ve0_l_xg`.

- The current materialize_range binary completed all six windows in 244.5 s,
  producing 3,577,114 events from 2,164,666 captured deliveries. Manual diagnostic
  stderr was empty. All manifest disposition counts match the existing schema-3
  derivatives, including the same 42 pre-existing Limitless parse faults.
- Shared identity-checking streaming decodes compare every window with its
  retained baseline. Event bytes match after removing venue_time, projecting
  schema 4 to 3, and replacing the 42 producer-derived fault/reject identities.
  Parse-reject parser versions and producer hashes are also projected; every
  other reject field is unchanged. Source-evidence logical hashes match exactly.
- 696,675 Kalshi and 2,873,228 Polymarket events carry event-time annotations.
  The corpus contains no sending_ts_ms fields, so it verifies historical
  interpretation and event-time propagation but does not exercise send-time
  extraction from retained data. No annotated event has negative receipt-minus-
  venue lag.
- All six fresh pins pass independent materialize_range --inspect-pin audits.
  A copy of the retained bench config with new pins and the returned normalizer
  identity passes Python metadata/descriptor/scale validation and the real Rust
  replay-publish --validate-only preflight. On macOS that Rust preflight was
  invoked directly instead of through the supervisor's Linux prctl containment
  hook; Redis and strategy processes were not started.
- Smoke discovery exposed the supervisor's schema-3-only metadata preflight.
  A minimal schema-4 regression failed before its fix, and now passes. Companion
  checks retain schema-3 support and reject unknown, noninteger and mismatched
  receipt/manifest schema versions.
- The bench, streams, supervisor, bundle-cache and jobs-contract suites pass:
  134 tests run, 13 optional Redis cases skipped. Bundle-cache process teardown
  tests emitted macOS sandbox killpg permission errors on their worker threads;
  the unittest suite itself passed. The two new focused schema regressions pass.
- A broader run including bundle-runner containment tests failed in macOS-only
  process setup: Linux prctl is unavailable and sandbox killpg is restricted.
  These runtime-containment failures are separate from metadata acceptance.

## October 3 retained send-time acceptance (2026-10-08)

Input: the user's completed `.bench/fixtures/lol-c9-lyon` fixture in the main
checkout. Its 12 certified windows cover 1791054000000000000 through
1791075600000000000, including the bundle tail into October 4. The committed
canonical store and existing schema-3 derivatives were read without mutation.
Fresh derivatives, verified streaming census, pin audits, old/new bench
preflight configs and the standalone Rust Risk probe are retained under
`/private/tmp/venue-time-oct3-smoke-frwl2vbt`.

- Current release materialize_range completed all windows in 41.3 s: 6,337,655
  captured deliveries, 10,826,163 accepted events and 369 paired fault events.
  Annotation diagnostic stderr was empty. All 12 fresh pins pass independent
  --inspect-pin audits, plus Python metadata/identity/scale validation and the
  real replay-publish --validate-only preflight.
- The old output has 1,694,851 Kalshi unknown_field rejects; the new output has
  zero Kalshi parse rejects. The remaining 369 parse rejects are unchanged
  Limitless notifications: 178 market_created_not_in_replay_domain and 191
  market_resolved_not_in_replay_domain.
- Every one of the 1,627,745 Kalshi deltas and 25,916 trades carries event and
  send time. All 1,760 normalized Kalshi Fulls (880 snapshot deliveries, two
  orientations each) carry send time only. All 9,052,174 Polymarket deltas,
  20,038 trades and 44,610 WebSocket Fulls carry their labelled event time.
  Eligible delta/trade event-time coverage is 100%, and no event or send clock
  is later than its captured visible_ns. Source-evidence logical identities
  are unchanged in every window.
- A standalone read-only probe links this worktree's real Risk engine and
  consumes the retained fixture's 38 book plans through verified EOF for both
  old and new pins. Old Kalshi: 0/20 planned books ever usable, no snapshots or
  applied operations. New Kalshi: 20/20 ever usable, 592 successful snapshot
  decisions and 980,765 applied operations. Remaining Kalshi invalidations are
  the recorded connection/subscription lifecycle; no underflow, scale or
  normalization invalidations appeared in the new planned-book walk.
- Both walks finish with the same 5,665,685 cuts. All 18 Polymarket books have
  identical per-book usability durations, snapshot/operation counts and
  invalidation counts before/after. This proves reconstruction recovery while
  preserving canonical cut structure and unaffected book behavior; it is not
  a live execution or completeness claim.

The fixture required no further production-code changes. No vendor API or
object-store request was made. These host preflights invoke the owning Rust
reader directly instead of the Linux prctl hook. Subsequent Linux execution is
recorded below; deployment remains unverified. No target pointer, archive object
or production service state was modified.

## Linux Redis and strategy smoke (2026-10-08)

The bench reused the already materialized schema-4 derivatives. Fresh contexts
were committed with their new pins through the preparation writer, preserving
the retained selection evidence and available outcome documents. The runner
was built from this worktree, then reused by image ID. All 238 installed Python
source files match the saved source hashes, and the installed materializer's
producer descriptor matches the host schema-4 descriptor. The source digest is
`7fbb2090e16fd821245c2986b963ffc2d405c4273c845130f4109be85210b2f8`.

Each run uses one supervisor attempt, its own internal Docker network and
disposable Redis 8.2 with positive maxmemory and noeviction. Derivatives,
contexts and the selected fee catalog are mounted read-only. The bench's strict
completed readers require actual supervisor SUCCESS, terminal acknowledgement,
and matching configuration, snapshot and content-receipt bindings.

| Retained input | Groups | Terminal entry count | Run seconds | Result |
|---|---|---:|---:|---|
| September baseline, full requested interval, 14 scopes/18 books | All seven | 2,008,580 | 149.7 | SUCCESS; every completed reader passed |
| October 3, first original occurrence, about ten minutes | All seven | 364,273 | 25.8 | SUCCESS; every completed reader passed |
| October 3, full requested interval, 30 scopes/38 books | Coverage, complement, multi-market, profile | 5,665,687 | 420.2 | SUCCESS; every completed reader passed |

The seven groups are bundle coverage, same-venue complement, cross-venue complete
sets, same-venue multi-market, both implication covers, and market profile.
Complement uses the existing fixture harness's 1/10/100 sizes without controls;
the structural strategies use their current example fill policies. The profile
uses minute buckets. Fees reuse the retained reviewed catalog models/source
evidence, scoped to each fixture, with the harness's direct-member and native
asset/parity research scenario. These are smoke configurations, not new default
production settings. Timings exclude image building and include completed
reading; the longer runs overlapped on shared local resources.

The full October 3 profile sees all 20 Kalshi and 18 Polymarket books become
usable. Every book's total usable duration exactly matches the separate Rust
Risk probe. The complement run has no positive episodes. The September
cross-venue output has 225 gross episodes and 42 net fill episodes; multi-market
has 26 gross episodes and three net fills. Besides completed-reader re-pricing,
independent rational arithmetic using the retained fee formulas verifies all
1,004 fee/cost leg payloads, 45 fill values, 90 kill-price properties and 45 edge
stops, with zero mismatches.

**Visible limitation.** The initial seven-group full October 3 attempt fails
during setup, before strategy consumption. Independent preflight reproduces
`entities.json` requirements of 13,562,233 bytes for cross-venue, 13,139,893 for
same-venue cover, and 13,983,433 for cross-venue cover, against the unchanged
8,388,608-byte cap. This failed attempt is preserved. The bounded occurrence
fits the cap and passes all seven readers. October 3's retained context has
outcomes unavailable, so its mask-dependent groups test visible rejection
behavior; they do not prove admitted-route economics. September retains outcome
masks and exercises admitted structural routes and actual fill checking.
No metadata cap, normalizer behavior or strategy implementation was changed to
make these runs pass.

Specs, preparation scripts, contexts, source hashes, all failed/successful output,
formula checks, receipt-binding checks and the consolidated `summary.json` are
retained at `/private/tmp/venue-time-oct3-smoke-frwl2vbt/strategy-bench`.
`cleanup.json` confirms removal of all task-owned containers, networks and
images after ownership verification, with no unrelated resources modified.
The eight ignored Rust Redis cases and 13 skipped Python Redis unit cases were
not rerun; these retained runs verify the real Linux publisher/consumer,
supervisor and strategy path. Deployment and live venue execution remain unverified.
