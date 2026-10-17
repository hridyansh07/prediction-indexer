# Execution-realism corpus reference

[`execution-realism.example.json`](execution-realism.example.json) is the checked
reference for the approved
[strategy rollout](../../docs/specs/STRATEGY_EXECUTION_REALISM_V1.md). It is a
template, not a runnable live configuration. Replace every `<...>` value with
reviewed local inputs before passing it to `python -m replay.corpus`.

The four fill groups use policies 4/2/4/4, keep the governing `edge` sizing,
record `t10`, and request all 60 reference probe scenarios. Flags use a 5-second
opening best-quote age and the `series_moneyline` settlement allowlist. The
standalone profile keeps its existing groups, including `transitions` and
`levels`, and adds the reference volatility settings. Coverage and the
same-venue complement are unchanged.

The source-owned configuration found during implementation was
`.bench/corpus/corpus-spec.json` in the primary checkout. It is outside version
control and has been preserved. This template retains its group names, comparison
keys, complement settings, fee class, and runtime limits, while replacing all
machine paths and the Universe endpoint with explicit placeholders. It contains
no credential, archive address, live event selection, or invented evidence pin.

To migrate an operator-owned configuration:

1. Save it unchanged and create a new configuration file. Carry forward its
   reviewed endpoint, canonical store, runner, materializer, image context, fees,
   work/output paths, and bundle selection. Use a fresh ledger/output namespace
   when comparing runs. Never use retained inputs as scratch space.
2. Copy the four fill policies and the profile policy from this template. Keep
   `@corpus:fees`, `@corpus:valuation`, `{context}`, and `{snapshot_sha256}` as
   literal tokens; the runner supplies their pinned per-bundle values.
3. Keep complement and coverage unchanged. Review profile ticks against the
   prepared plan scales. Verify the fee source bytes and catalog identity through
   the existing fee workflow; this template does not make new fee claims.
4. Validate the closed corpus spec and strategy policies offline. Commit the
   producer checkout before an authorized run. The existing `corpus_key` binds
   every complete group configuration, including sizings, probes, flags and
   volatility, as well as code, runner, materializer, fees and limits. Changing
   these settings therefore creates a new key; do not reuse old result receipts.
5. After the prerequisite PRs and rollout merge, separately authorize any Universe
   migration/deployment and retained-data acceptance. Universe execution terms
   require schema **v8**; the old specification's v7 single-rebuild wording was
   superseded by [offline migrations](../../docs/specs/UNIVERSE_MIGRATIONS_V1.md).
   Run the five-bundle reader/screen acceptance before a full corpus rerun.

This change does not run the corpus, access cloud archives, deploy Universe,
change services, or start a live pilot. See the
[corpus runner](../../replay/corpus/README.md) for the existing execution commands.
