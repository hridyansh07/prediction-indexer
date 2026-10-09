# Event Research V1 backend implementation contract

Approved scope: the owner instructions in the Event Research implementation
thread override `docs/event-research-v1` at
`e36de765c8819d8239948edb34f570e1d98c0696`. This document records that boundary;
it is not evidence that a phase is implemented. The implementation base is
`329437cab5baed78f09067b301b6e99096b928e3`.

## Included

- SDK layout 3 descriptor, entity and reason NDJSON tables, with layout 1 bytes
  frozen and old layout 2 readable. New aggregate manifests carry `layout: 3`.
  A missing layout field retains the old policy-defined layout. Unknown layouts
  fail closed. Episode and denominator rows retain their existing indexes.
- Per-run `limits.state_bytes`, default 128 MiB, propagated to collectors and
  independent readers. This resource limit is not an experiment identity input.
- Existing Universe HTTP contracts are reused without schema, endpoint, auth or
  Replay job changes.
- One-shot game-state pulls and retry ledger. New scheduled storage is keyed
  directly by immutable Universe `event:d1:<sha256>` identity, not a mutable
  bundle or title. Existing raw/timeline format readers remain supported. No
  bundle-to-milestone association artifact and no preparation change.
- Offline independent run verification, exact-input-bound `verify.json`,
  deterministic per-event packs, episode and shared-book-side duplicate data.
  Scope exits and entries reset chart state; raw tiles carry state at their
  opening boundary. Unknown/unusable periods never acquire quotes by forward
  filling across a scope boundary.
- Complete verified Parquet downloads before bounded queries. No range reads.

## Excluded

Corpus orchestration, corpus publication, release manifests and all UI changes
are excluded. No new interpretations, classifications, inferred economics or
display filters are added. Metrics copy the upstream episode/fill contract.
Negated claims remain distinct: equivalence is based on effective payout keys,
not a base claim ID alone. Prepared-context bytes and identity are unchanged.

The spec calls for static file serving and no new API server. Backend commands
therefore produce and verify local immutable artifacts; installing or changing
a live server, bucket policy, CDN or service is an operator action. Conditional
current-pointer support, if used by a data adapter, must compare the exact prior
identity, verify new content first, and never change immutable objects.

## Evidence and failure boundaries

Inputs remain read-only. Writers refuse existing immutable output destinations.
Content is completed, synced and identity-verified before its commit marker.
Verification binds configuration, context/receipt, result/supervisor receipts,
manifest, every consumed stream and fee/game-state inputs by length and SHA-256.
Build rechecks those exact inputs, rather than trusting an earlier pass result.
Missing lenses are `not_run`; failed or tampered inputs are `failed`; a pack
requires an independently verified profile. A complete raw game-state receipt
does not imply settlement, and a missing/invalid timeline is visible.

No command deletes inputs, historical outputs or archive objects. Retry ledgers
are rebuildable local operational state, not evidence authority. Scheduled jobs
remain one-shot with external cadence. Rollout/backfill/cloud acceptance require
separate operator execution; repository tests use disposable local stores only.

## Implementation deviations from the broader plan

Part C's behavior-preserving Universe package/route-table relocation is omitted
as unrelated structural cleanup for this narrowed backend implementation. The
scheduled job uses existing bundle/history/outcome endpoints and has no import
of Universe internals. The new game-state root/data-layout documentation and
Compose service are included. This is an implementation scope interpretation,
not a claim that Part C was implemented.

Corpus aggregate histograms, viability prose, calibration and publication are
omitted alongside the owner-excluded corpus/release/UI work. Direct backend
event/episode Parquet records and bounded queries remain included. The local
build receipt is not a publication or release manifest. No current pointer is
ever written; its conditional-update invariant therefore remains unchanged.
Upstream-absent titles/labels are null/omitted; no classifications are invented.
