# Event Universe Store

**Status:** specification. Nothing implemented. Nothing in `replay/` changes.

Build the universe of what was actually captured, from S3 alone, and a UI over it.
Replay stays untouched until the data model is known — the gates will likely be
rewritten to consume it, and doing that work now means doing it twice.

---

## 1. The problem: intent is not reality

Two populations exist and nothing reconciles them.

**What the targeter selected.** A run publishes targets for a venue and records
them in its selection report and `target_records_<venue>.ndjson`.

**What capture actually listened to.** A splice opens a connection, subscribes,
and may add or drop assets mid-connection or fail entirely.

A market can be selected and never subscribed — the splice was down, the targets
file was unreadable, the connection failed, or the subscribe was rejected. Nothing
reports that today, and no analysis can assume a selected market has data.

The universe answers one question per asset: **for which spans were we actually
listening, and did records arrive?**

---

## 2. Constraint: S3 only

The universe must not run on the capture host. That machine's job is to never drop
a frame, and a long-lived queryable service with an API is the wrong thing to put
beside it. Reading the spool directly is also self-defeating: the reaper deletes
it, so a spool-backed store silently loses history exactly when retention works.

Everything therefore reads archived, immutable objects. That also gives the reseed
property the store needs — rebuilding is deterministic rather than a race against
retention.

**Consequence:** the universe is complete for *closed* UTC days. An open day has no
published manifest, so today is approximate. Acceptable — realtime is not the goal.

---

## 3. What exists today, and what does not

### 3.1 Intent — already on S3

Archived by `targeter-v2-run-archiver` under
`targeter-v2/runs/date=<d>/run=<run_id>/`:

| Artifact | Carries |
|---|---|
| selection report | `run_id`, `generated_at`, published target digests |
| `target_records_<venue>.ndjson` | the venue's own record per selected market |
| `catalog_<venue>_events.ndjson` | `venue_event_id`, title |
| `catalog_<venue>_markets.ndjson` | market → `venue_event_id` |

Within-venue event grouping is a fact from the venue's API. No inference needed.

### 3.2 Span and volume — exists, but not on S3

`archive/archiver/manifest.py` already builds the daily manifest: one entry per
segment with lane, window, data key, seal key, logical identity and stored
identity, sorted by `(window_start_ns, lane rank, segment_index, segment_id)`.
Exactly the index this needs.

It is written to a local `--manifest-root` as `date=<d>/manifest.json` and **never
uploaded**. `put_immutable` covers data and seal objects only.

→ **Deliverable A.**

### 3.3 Reality — on S3, but only inside full segments

Control records are emitted by `splices/common/base.py` with `kind=control`:

| Record | Carries | Means |
|---|---|---|
| `connection_opened` | `target_digest`, `asset_ids`, `target_count`, `target_metadata_digest` | listening began under this exact generation |
| `subscription_sent` | `target_digest`, `target_count` | the subscribe was actually sent |
| `subscription_changed` | `from_digest`, `to_digest`, `added[]`, `removed[]` | membership changed mid-connection |
| `target_metadata_changed` | metadata digest transition | same targets, new metadata |
| `targets_unreadable` | error | intent existed, listening did not |
| `connection_closed` | — | listening ended |

`target_digest` is the join key: computed by `targeter/targets.py:74`, echoed
verbatim by the splice. "This epoch listened under that generation" is a recorded
fact, not an inference.

They are a tiny fraction of the tape but live inside segments, so reading them
today means streaming every segment in full.

→ **Deliverable B.**

---

## 4. Deliverables

### A. Publish the daily manifest — `archive/archiver/`

Upload each closed day's `manifest.json` to the object store beside the segments
it describes. Open days stay local-only; publish on close, after every included
receipt revalidates.

The manifest is a derived catalog, not a commit marker, and does not replace
per-object verification — identity is still checked when bytes are streamed.

### B. Control sidecar — `archive/archiver/`

At archive time, when the segment bytes are already in hand, write a second object
beside each segment containing only its `kind=control` lines:

```
<segment>.control.ndjson.zst
```

Verbatim envelopes, same order, no interpretation. Mechanical extraction, not
judgement, so it belongs in the archiver rather than the splice.

Its identity goes in the daily manifest entry beside the data and seal identities.

This is what makes the universe cheap: it reads a small artifact per segment
instead of pulling full tape.

### C. Store — `universe/store.py`

SQLite, split by what a reseed may destroy.

**Derived** — rebuilt from immutable S3 artifacts, drop and reseed freely:

```
lane_window    lane, start_ns, end_ns, segment_index, line_count, data_key, control_key
subscription   venue, asset_id, start_ns, end_ns, target_digest, epoch, lane
intent         venue, asset_id, run_id, target_digest, published_at_ns
market         venue, venue_market_id, venue_event_id, asset_id, record_sha256
event          venue, venue_event_id, title
```

**Human** — never touched by a reseed:

```
block          block_id, label, created_by, created_at
block_member   block_id, venue, venue_event_id, note
link_decision  block_id, venue_a, venue_b, decision, decided_by, decided_at, basis
```

Cross-venue linking is judgement and lives only in the human tables. Within-venue
grouping is fact and is derived.

Seeding is one idempotent command over immutable inputs. Hold that and the
substrate choice stays reversible.

### D. Ingest — `universe/ingest.py`

Three readers, all S3:

1. daily manifests → `lane_window`
2. control sidecars → `subscription`
3. targeter run artifacts → `intent`, `market`, `event`

Idempotent, keyed on the digests consumed, so re-running is free.

### E. Coverage — `universe/coverage.py`

Given assets and a period, report per asset:

- subscription intervals with the target generation in force
- lane span coverage, and any gap in `segment_index`
- record volume per window
- the three differences below, named rather than summarised

| State | Established by | Meaning |
|---|---|---|
| `intended` | targeter published a digest containing it | we meant to listen |
| `subscribed` | `connection_opened` / `subscription_changed` names it | we were listening |
| `delivered` | frames exist for it in that span | data arrived |

- `intended` not `subscribed` — selected, never listened to. The invisible gap.
- `subscribed` not `delivered` — listening, nothing arrived. Legitimate for a quiet
  market; indistinguishable from a broken subscription without volume context.
- `delivered` not `intended` — should be empty. If not, something is wrong.

Output is data, not a verdict. **This is availability, not correctness.** It says
when we were listening and whether bytes arrived. It makes no claim that nothing
was lost — that is replay's question and stays there.

### F. UI — `targeter-ui/`

Events as data: venue events with their markets, the periods we were listening,
volume. Enough to answer "is this event worth replaying" before replay exists.

Cross-venue links are proposed here and confirmed by a human, writing
`link_decision`. Rejection is a new decision row, never a delete, so results
explained under an earlier view stay explicable.

---

## 5. Explicitly not now

- No changes to `replay/`. Gates keep taking a directory.
- No interval-validity model, no capture/transport evidence classes.
- No materialized block files.
- No cross-venue basket formation.

The point of doing this first is that the harness can be built around a known data
model afterwards, even if the gates are rewritten to meet it.

---

## 6. Open questions

1. **Link derivation.** Are cross-venue links proposed from event titles and start
   times, from resolution text, or hand-seeded at first? Sets how much of F is
   machine work versus review.

2. **Per-asset delivery counts.** Nothing carries them. Manifests hold `line_count`
   per segment; the control sidecar holds no frames. Either the sidecar also carries
   a per-asset frame tally computed during extraction — cheap there, since the
   archiver is already reading every line — or `delivered` stays at lane
   granularity until replay exists. **The sidecar is the only place this is cheap;
   decide before B is built.**

3. **Backfill.** Segments already archived have no sidecar. Regenerate by streaming
   them once, or accept that the universe starts from the deployment date?

4. **Retention.** The store references objects the reaper may delete. Pin what it
   references, or record that a span is no longer retrievable?
