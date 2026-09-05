CREATE TABLE targeter_runs (
    run_id TEXT PRIMARY KEY,
    generated_at TEXT NOT NULL,
    generated_at_ns INTEGER NOT NULL,
    input_complete INTEGER NOT NULL CHECK(input_complete IN (0, 1)),
    report_version INTEGER NOT NULL CHECK(report_version = 3),
    strategy_version INTEGER NOT NULL CHECK(strategy_version > 0),
    manifest_key TEXT NOT NULL UNIQUE,
    manifest_sha256 TEXT NOT NULL,
    manifest_byte_length INTEGER NOT NULL CHECK(manifest_byte_length > 0),
    report_key TEXT NOT NULL UNIQUE,
    report_sha256 TEXT NOT NULL,
    report_byte_length INTEGER NOT NULL CHECK(report_byte_length > 0),
    report_decoded_sha256 TEXT NOT NULL,
    report_decoded_byte_length INTEGER NOT NULL
        CHECK(report_decoded_byte_length > 0),
    projection_version INTEGER NOT NULL CHECK(projection_version = 1),
    projection_sha256 TEXT NOT NULL,
    projection_row_count INTEGER NOT NULL CHECK(projection_row_count >= 0),
    indexed_at_ns INTEGER NOT NULL,
    CHECK(input_complete = 1 OR projection_row_count = 0)
) STRICT;
CREATE INDEX targeter_runs_generated
    ON targeter_runs(generated_at_ns, run_id);

CREATE TABLE bundle_contexts (
    context_sha256 TEXT PRIMARY KEY,
    bundle_id TEXT NOT NULL,
    sport TEXT NOT NULL,
    game TEXT,
    topology TEXT,
    activation_at TEXT NOT NULL,
    activation_at_ns INTEGER NOT NULL,
    capture_start_at TEXT NOT NULL,
    capture_start_at_ns INTEGER NOT NULL,
    CHECK(capture_start_at_ns < activation_at_ns)
) STRICT;
CREATE INDEX bundle_contexts_event_time
    ON bundle_contexts(activation_at_ns, bundle_id);

CREATE TABLE context_participants (
    context_sha256 TEXT NOT NULL
        REFERENCES bundle_contexts(context_sha256),
    position INTEGER NOT NULL CHECK(position IN (0, 1)),
    name TEXT NOT NULL,
    participant_key TEXT NOT NULL,
    PRIMARY KEY(context_sha256, position)
) STRICT;

CREATE TABLE context_events (
    context_sha256 TEXT NOT NULL
        REFERENCES bundle_contexts(context_sha256),
    event_ref TEXT NOT NULL,
    venue TEXT NOT NULL,
    PRIMARY KEY(context_sha256, event_ref)
) STRICT;

CREATE TABLE context_markets (
    context_sha256 TEXT NOT NULL
        REFERENCES bundle_contexts(context_sha256),
    target_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
    PRIMARY KEY(context_sha256, target_id)
) STRICT;

CREATE TABLE context_targets (
    context_sha256 TEXT NOT NULL,
    target_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    canonical_class TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    PRIMARY KEY(context_sha256, target_id),
    FOREIGN KEY(context_sha256, target_id)
        REFERENCES context_markets(context_sha256, target_id)
) STRICT;

CREATE TABLE context_target_assets (
    context_sha256 TEXT NOT NULL,
    target_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    PRIMARY KEY(context_sha256, target_id, asset_id),
    FOREIGN KEY(context_sha256, target_id)
        REFERENCES context_targets(context_sha256, target_id)
) STRICT;

CREATE TABLE context_relationships (
    context_sha256 TEXT NOT NULL
        REFERENCES bundle_contexts(context_sha256),
    relationship_index INTEGER NOT NULL CHECK(relationship_index >= 0),
    left_market TEXT NOT NULL,
    right_market TEXT NOT NULL,
    relationship TEXT NOT NULL,
    scope TEXT NOT NULL,
    left_venue TEXT NOT NULL,
    right_venue TEXT NOT NULL,
    coverage TEXT NOT NULL,
    PRIMARY KEY(context_sha256, relationship_index)
) STRICT;

CREATE TABLE selection_occurrences (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    bundle_id TEXT NOT NULL,
    context_sha256 TEXT NOT NULL
        REFERENCES bundle_contexts(context_sha256),
    occurrence_kind TEXT NOT NULL
        CHECK(occurrence_kind IN ('complete', 'retained')),
    origin_run_id TEXT NOT NULL,
    continuity_selected INTEGER NOT NULL CHECK(continuity_selected IN (0, 1)),
    continuity_disposition TEXT CHECK(
        continuity_disposition IS NULL OR
        continuity_disposition IN ('held_current_candidate', 'retained')
    ),
    PRIMARY KEY(run_id, bundle_id),
    FOREIGN KEY(origin_run_id, bundle_id)
        REFERENCES selection_occurrences(run_id, bundle_id)
        DEFERRABLE INITIALLY DEFERRED,
    CHECK(
        (occurrence_kind = 'complete' AND origin_run_id = run_id) OR
        (occurrence_kind = 'retained' AND origin_run_id <> run_id)
    ),
    CHECK(
        (continuity_selected = 0 AND continuity_disposition IS NULL) OR
        (continuity_selected = 1 AND continuity_disposition IS NOT NULL)
    ),
    CHECK(
        (occurrence_kind = 'retained' AND continuity_disposition = 'retained') OR
        (occurrence_kind = 'complete' AND
         (continuity_disposition IS NULL OR
          continuity_disposition = 'held_current_candidate'))
    )
) STRICT;
CREATE INDEX selection_occurrences_context
    ON selection_occurrences(context_sha256, run_id, bundle_id);
CREATE INDEX selection_occurrences_origin
    ON selection_occurrences(origin_run_id, bundle_id, run_id);

CREATE TABLE bundle_retirements (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    bundle_id TEXT NOT NULL,
    origin_run_id TEXT NOT NULL,
    context_sha256 TEXT NOT NULL
        REFERENCES bundle_contexts(context_sha256),
    disposition TEXT NOT NULL CHECK(
        disposition IN ('all_markets_terminal', 'terminal_clamp_elapsed')
    ),
    PRIMARY KEY(run_id, bundle_id),
    FOREIGN KEY(origin_run_id, bundle_id)
        REFERENCES selection_occurrences(run_id, bundle_id)
) STRICT;
CREATE INDEX bundle_retirements_bundle
    ON bundle_retirements(bundle_id, run_id);
CREATE INDEX bundle_retirements_origin
    ON bundle_retirements(origin_run_id, bundle_id, run_id);

CREATE TABLE checkpoints (
    name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE event_identity_lineage (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generated_start TEXT NOT NULL,
    generated_end TEXT NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('running', 'complete'))
) STRICT;

CREATE TABLE universe_run_projections (
    run_id TEXT PRIMARY KEY REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    projection_version INTEGER NOT NULL CHECK(projection_version = 3),
    projection_sha256 TEXT NOT NULL,
    projection_row_count INTEGER NOT NULL CHECK(projection_row_count >= 0)
) STRICT;

CREATE TABLE umbrella_events (
    event_id TEXT PRIMARY KEY,
    identity_version INTEGER NOT NULL CHECK(identity_version = 1),
    identity_activation_date TEXT NOT NULL
        CHECK(length(identity_activation_date) = 10),
    identity_ordinal INTEGER NOT NULL CHECK(identity_ordinal >= 0),
    sport TEXT NOT NULL,
    game TEXT,
    topology TEXT,
    activation_at TEXT NOT NULL,
    activation_at_ns INTEGER NOT NULL,
    participants_json TEXT NOT NULL CHECK(json_valid(participants_json)),
    participant_keys_json TEXT NOT NULL CHECK(json_valid(participant_keys_json)),
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id)
) STRICT;
CREATE UNIQUE INDEX umbrella_events_identity
    ON umbrella_events(
        identity_version,
        sport,
        COALESCE(game, ''),
        COALESCE(topology, ''),
        participant_keys_json,
        identity_activation_date,
        identity_ordinal
    );
CREATE INDEX umbrella_events_activation
    ON umbrella_events(activation_at_ns, event_id);

CREATE TABLE event_observations (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    bundle_id TEXT NOT NULL,
    observed_activation_at TEXT NOT NULL,
    observed_activation_at_ns INTEGER NOT NULL,
    PRIMARY KEY(run_id, event_id, bundle_id)
) STRICT;
CREATE INDEX event_observations_event_time
    ON event_observations(event_id, observed_activation_at_ns, run_id, bundle_id);

CREATE TABLE universe_sync_failures (
    manifest_key TEXT PRIMARY KEY,
    first_failed_at_ns INTEGER NOT NULL,
    last_failed_at_ns INTEGER NOT NULL,
    next_retry_at_ns INTEGER NOT NULL,
    attempts INTEGER NOT NULL CHECK(attempts > 0),
    error TEXT NOT NULL
) STRICT;
CREATE INDEX universe_sync_failures_retry
    ON universe_sync_failures(next_retry_at_ns, manifest_key);

-- Venue contracts are assumed to make (venue, venue_event_id) globally unique
-- and non-reusable. The umbrella event association is therefore not part of
-- this source identity.
CREATE TABLE venue_events (
    venue TEXT NOT NULL,
    venue_event_id TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    title TEXT NOT NULL,
    league TEXT,
    status TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    format TEXT,
    fragment_type TEXT,
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(venue, venue_event_id)
) STRICT;
CREATE INDEX venue_events_event ON venue_events(event_id, venue, venue_event_id);

CREATE TABLE canonical_markets (
    market_id TEXT NOT NULL,
    market_template_version INTEGER NOT NULL CHECK(market_template_version > 0),
    outcome_space_version INTEGER NOT NULL CHECK(outcome_space_version > 0),
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    canonical_class TEXT NOT NULL,
    market_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    parameters_json TEXT NOT NULL CHECK(json_valid(parameters_json)),
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(market_id, market_template_version, outcome_space_version)
) STRICT;
CREATE INDEX canonical_markets_event
    ON canonical_markets(event_id, canonical_class, market_id);

-- Venue market identity follows the same global non-reuse assumption as
-- venue_events. Event and canonical-market links are mutable derived views.
CREATE TABLE venue_markets (
    venue TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    venue_event_id TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    market_id TEXT NOT NULL,
    market_template_version INTEGER NOT NULL,
    outcome_space_version INTEGER NOT NULL,
    canonical_class TEXT NOT NULL,
    market_type TEXT NOT NULL,
    scope TEXT NOT NULL,
    title TEXT NOT NULL,
    parameters_json TEXT NOT NULL CHECK(json_valid(parameters_json)),
    subscription_ids_json TEXT NOT NULL CHECK(json_valid(subscription_ids_json)),
    outcome_labels_json TEXT NOT NULL CHECK(json_valid(outcome_labels_json)),
    status TEXT NOT NULL,
    accepting_orders INTEGER NOT NULL CHECK(accepting_orders IN (0, 1)),
    rules_hash TEXT,
    rule_template_id TEXT,
    source_ref TEXT NOT NULL,
    created_at TEXT,
    volume_24h REAL,
    volume_total REAL,
    volume_total_usd REAL,
    liquidity REAL,
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(venue, venue_market_id),
    FOREIGN KEY(venue, venue_event_id)
        REFERENCES venue_events(venue, venue_event_id),
    FOREIGN KEY(market_id, market_template_version, outcome_space_version)
        REFERENCES canonical_markets(
            market_id, market_template_version, outcome_space_version
        )
) STRICT;
CREATE INDEX venue_markets_event ON venue_markets(event_id, venue, venue_market_id);
CREATE INDEX venue_markets_canonical
    ON venue_markets(market_id, market_template_version, outcome_space_version);

CREATE TABLE candidate_decisions (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    bundle_id TEXT NOT NULL,
    eligible INTEGER NOT NULL CHECK(eligible IN (0, 1)),
    selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
    score REAL NOT NULL,
    score_components_json TEXT NOT NULL CHECK(json_valid(score_components_json)),
    rejection_reasons_json TEXT NOT NULL CHECK(json_valid(rejection_reasons_json)),
    allocation_rejection TEXT,
    admission_json TEXT NOT NULL CHECK(json_valid(admission_json)),
    market_exclusions_json TEXT NOT NULL CHECK(json_valid(market_exclusions_json)),
    eligible_market_ids_json TEXT NOT NULL CHECK(json_valid(eligible_market_ids_json)),
    PRIMARY KEY(run_id, bundle_id)
) STRICT;
CREATE INDEX candidate_decisions_event ON candidate_decisions(event_id, run_id);

CREATE TABLE selected_market_occurrences (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    bundle_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    market_id TEXT NOT NULL,
    market_template_version INTEGER NOT NULL CHECK(market_template_version > 0),
    outcome_space_version INTEGER NOT NULL CHECK(outcome_space_version > 0),
    canonical_class TEXT NOT NULL,
    continuity_score REAL NOT NULL,
    selection_reason TEXT NOT NULL CHECK(
        selection_reason IN ('selected', 'held_current_candidate', 'retained')
    ),
    origin_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(run_id, venue, venue_market_id),
    FOREIGN KEY(venue, venue_market_id)
        REFERENCES venue_markets(venue, venue_market_id),
    FOREIGN KEY(market_id, market_template_version, outcome_space_version)
        REFERENCES canonical_markets(
            market_id, market_template_version, outcome_space_version
        )
) STRICT;
CREATE INDEX selected_market_occurrences_event
    ON selected_market_occurrences(event_id, run_id, venue, venue_market_id);
CREATE INDEX selected_market_occurrences_market
    ON selected_market_occurrences(venue, venue_market_id, run_id);
CREATE INDEX selected_market_occurrences_canonical
    ON selected_market_occurrences(
        market_id, market_template_version, outcome_space_version, run_id
    );

-- A claim is the outcome subset a market resolves YES on, inside one outcome
-- space shape. `analysis.masks.relationship` reads the two subsets and no other
-- field, so the subset is the whole semantic content and two markets naming it
-- are the same claim however their venues word it.
--
-- Outcome keys are participant-independent -- series spaces key outcomes
-- seq:{sequence} and score spaces key them score:{h}-{a} -- so a claim id is
-- global: one row serves every event and every run that expresses it.
--
-- first_seen_run_id/last_seen_run_id are targeter observation bounds, not
-- lifecycle. Absence after last_seen_run_id is ambiguous by construction: the
-- market may have settled, been delisted, or stopped being a candidate, and
-- Universe cannot tell which without the venue.
CREATE TABLE claim_classes (
    claim_id TEXT PRIMARY KEY,
    space_shape_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    -- Whether the space this claim is drawn from enumerates every reachable
    -- outcome. A finding over an INCOMPLETE_COVERAGE space is conditional
    -- discovery evidence, never an unconditional claim.
    coverage TEXT NOT NULL,
    outcome_key_count INTEGER NOT NULL CHECK(outcome_key_count > 0),
    claim_identity_version INTEGER NOT NULL CHECK(claim_identity_version = 1),
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id)
) STRICT;
CREATE INDEX claim_classes_shape ON claim_classes(space_shape_id, claim_id);

-- How two claims of one space shape relate. A function of the two subsets, so
-- it names no event, run, venue, or bundle and is reusable wherever that shape
-- occurs.
--
-- IDENTITY is absent by construction: equal subsets are one claim_id, so
-- equivalence is two market_claims rows sharing a claim, never an edge.
-- REVERSE_IMPLICATION is normalized to (antecedent, consequent) ordering, and
-- OVERLAP -- the catch-all branch of relationship(), which the targeter's own
-- scorer discards -- is not stored at all.
CREATE TABLE claim_relations (
    space_shape_id TEXT NOT NULL,
    left_claim_id TEXT NOT NULL REFERENCES claim_classes(claim_id),
    right_claim_id TEXT NOT NULL REFERENCES claim_classes(claim_id),
    relation_type TEXT NOT NULL
        CHECK(relation_type IN ('IMPLICATION', 'MUTUAL_EXCLUSION')),
    algebra_version INTEGER NOT NULL CHECK(algebra_version = 1),
    PRIMARY KEY(space_shape_id, left_claim_id, right_claim_id),
    CHECK(left_claim_id <> right_claim_id)
) STRICT;

-- Which claim one venue market's tradable token expresses, and over which runs
-- the targeter observed it doing so. One row per market per claim era: a second
-- row appears only if the market's semantics genuinely change, never per run.
CREATE TABLE market_claims (
    venue TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    claim_id TEXT NOT NULL REFERENCES claim_classes(claim_id),
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    first_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    last_seen_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(venue, venue_market_id, claim_key, claim_id),
    FOREIGN KEY(venue, venue_market_id)
        REFERENCES venue_markets(venue, venue_market_id)
) STRICT;
CREATE INDEX market_claims_claim
    ON market_claims(claim_id, event_id, venue, venue_market_id);
CREATE INDEX market_claims_event ON market_claims(event_id, claim_id);
