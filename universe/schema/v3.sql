CREATE TABLE universe_run_projections (
    run_id TEXT PRIMARY KEY REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    projection_version INTEGER NOT NULL CHECK(projection_version = 1),
    projection_sha256 TEXT NOT NULL,
    projection_row_count INTEGER NOT NULL CHECK(projection_row_count >= 0)
) STRICT;

CREATE TABLE umbrella_events (
    event_id TEXT PRIMARY KEY,
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
CREATE INDEX umbrella_events_activation
    ON umbrella_events(activation_at_ns, event_id);

CREATE TABLE event_observations (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    bundle_id TEXT NOT NULL,
    PRIMARY KEY(run_id, event_id, bundle_id)
) STRICT;

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
    canonical_class TEXT NOT NULL,
    continuity_score REAL NOT NULL,
    selection_reason TEXT NOT NULL CHECK(
        selection_reason IN ('selected', 'held_current_candidate', 'retained')
    ),
    origin_run_id TEXT NOT NULL REFERENCES targeter_runs(run_id),
    PRIMARY KEY(run_id, venue, venue_market_id),
    FOREIGN KEY(venue, venue_market_id)
        REFERENCES venue_markets(venue, venue_market_id)
) STRICT;
CREATE INDEX selected_market_occurrences_event
    ON selected_market_occurrences(event_id, run_id, venue, venue_market_id);
CREATE INDEX selected_market_occurrences_market
    ON selected_market_occurrences(venue, venue_market_id, run_id);

CREATE TABLE relations (
    relation_id INTEGER PRIMARY KEY,
    relation_type TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES umbrella_events(event_id),
    scope TEXT NOT NULL,
    coverage TEXT NOT NULL,
    generation_version INTEGER NOT NULL CHECK(generation_version > 0),
    canonical_hash TEXT NOT NULL,
    UNIQUE(relation_type, canonical_hash, generation_version)
) STRICT;
CREATE INDEX relations_event ON relations(event_id, relation_type, relation_id);

CREATE TABLE relation_members (
    relation_id INTEGER NOT NULL REFERENCES relations(relation_id) ON DELETE CASCADE,
    venue TEXT NOT NULL,
    venue_market_id TEXT NOT NULL,
    claim_key TEXT NOT NULL,
    role TEXT NOT NULL,
    PRIMARY KEY(relation_id, venue, venue_market_id, claim_key, role),
    FOREIGN KEY(venue, venue_market_id)
        REFERENCES venue_markets(venue, venue_market_id)
) STRICT;
CREATE INDEX relation_members_market
    ON relation_members(venue, venue_market_id, relation_id);

CREATE TABLE relation_observations (
    run_id TEXT NOT NULL REFERENCES targeter_runs(run_id) ON DELETE CASCADE,
    relation_id INTEGER NOT NULL REFERENCES relations(relation_id),
    bundle_id TEXT NOT NULL,
    PRIMARY KEY(run_id, relation_id, bundle_id)
) STRICT;
