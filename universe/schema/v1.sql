CREATE TABLE ingest_sources (
    source_key TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    identity_sha256 TEXT NOT NULL,
    ingested_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE checkpoints (
    name TEXT PRIMARY KEY,
    cursor TEXT NOT NULL,
    updated_at_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE targeter_sources (
    run_id TEXT PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE REFERENCES ingest_sources(source_key) DEFERRABLE INITIALLY DEFERRED,
    source_kind TEXT NOT NULL CHECK(source_kind IN ('manifest_index', 'derived_index')),
    generated_at TEXT NOT NULL,
    generated_at_ns INTEGER NOT NULL,
    input_complete INTEGER NOT NULL CHECK(input_complete IN (0, 1)),
    strategy_version INTEGER,
    manifest_key TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    index_key TEXT NOT NULL,
    index_sha256 TEXT NOT NULL,
    index_byte_length INTEGER NOT NULL CHECK(index_byte_length >= 0),
    CHECK(strategy_version IS NULL OR strategy_version > 0)
) STRICT;
CREATE INDEX targeter_sources_generated
    ON targeter_sources(generated_at_ns DESC, run_id);

CREATE TABLE selected_bundles (
    run_id TEXT NOT NULL REFERENCES targeter_sources(run_id) ON DELETE CASCADE,
    bundle_id TEXT NOT NULL,
    sport TEXT NOT NULL,
    game TEXT,
    topology TEXT,
    activation_at TEXT NOT NULL,
    activation_at_ns INTEGER NOT NULL,
    capture_start_at TEXT NOT NULL,
    capture_start_at_ns INTEGER NOT NULL,
    planned_capture_end_at TEXT NOT NULL,
    planned_capture_end_at_ns INTEGER NOT NULL,
    post_start_retention_seconds INTEGER NOT NULL
        CHECK(post_start_retention_seconds > 0),
    PRIMARY KEY(run_id, bundle_id),
    CHECK(capture_start_at_ns < activation_at_ns),
    CHECK(activation_at_ns < planned_capture_end_at_ns)
) STRICT;
CREATE INDEX selected_bundles_window
    ON selected_bundles(capture_start_at_ns, planned_capture_end_at_ns, bundle_id);
CREATE INDEX selected_bundles_identity
    ON selected_bundles(bundle_id, run_id);

CREATE TABLE bundle_participants (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK(position IN (0, 1)),
    name TEXT NOT NULL,
    participant_key TEXT NOT NULL,
    PRIMARY KEY(run_id, bundle_id, position),
    FOREIGN KEY(run_id, bundle_id)
        REFERENCES selected_bundles(run_id, bundle_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bundle_events (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    event_ref TEXT NOT NULL,
    venue TEXT NOT NULL,
    PRIMARY KEY(run_id, bundle_id, event_ref),
    FOREIGN KEY(run_id, bundle_id)
        REFERENCES selected_bundles(run_id, bundle_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bundle_markets (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
    PRIMARY KEY(run_id, bundle_id, target_id),
    FOREIGN KEY(run_id, bundle_id)
        REFERENCES selected_bundles(run_id, bundle_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE selected_targets (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    canonical_class TEXT NOT NULL,
    PRIMARY KEY(run_id, target_id),
    FOREIGN KEY(run_id, bundle_id, target_id)
        REFERENCES bundle_markets(run_id, bundle_id, target_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE selected_target_assets (
    run_id TEXT NOT NULL,
    target_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    PRIMARY KEY(run_id, target_id, asset_id),
    FOREIGN KEY(run_id, target_id)
        REFERENCES selected_targets(run_id, target_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE bundle_relationships (
    run_id TEXT NOT NULL,
    bundle_id TEXT NOT NULL,
    relationship_index INTEGER NOT NULL,
    left_market TEXT NOT NULL,
    right_market TEXT NOT NULL,
    relationship TEXT NOT NULL,
    scope TEXT NOT NULL,
    left_venue TEXT NOT NULL,
    right_venue TEXT NOT NULL,
    coverage TEXT NOT NULL,
    PRIMARY KEY(run_id, bundle_id, relationship_index),
    FOREIGN KEY(run_id, bundle_id)
        REFERENCES selected_bundles(run_id, bundle_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE subscription_sets (
    run_id TEXT NOT NULL REFERENCES targeter_sources(run_id) ON DELETE CASCADE,
    venue TEXT NOT NULL,
    target_digest TEXT NOT NULL,
    asset_count INTEGER NOT NULL,
    PRIMARY KEY(run_id, venue)
) STRICT;
CREATE INDEX subscription_sets_digest ON subscription_sets(venue, target_digest);

CREATE TABLE subscription_assets (
    run_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    PRIMARY KEY(run_id, venue, asset_id),
    FOREIGN KEY(run_id, venue)
        REFERENCES subscription_sets(run_id, venue) ON DELETE CASCADE
) STRICT;

CREATE TABLE segment_receipts (
    receipt_key TEXT PRIMARY KEY,
    lane_id TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    segment_index INTEGER NOT NULL,
    window_start_ns INTEGER NOT NULL,
    window_end_ns INTEGER NOT NULL,
    data_key TEXT NOT NULL UNIQUE,
    control_key TEXT NOT NULL UNIQUE,
    control_sha256 TEXT NOT NULL,
    control_byte_length INTEGER NOT NULL,
    control_line_count INTEGER NOT NULL,
    published_at_ns INTEGER NOT NULL,
    UNIQUE(lane_id, segment_id)
) STRICT;
CREATE INDEX segment_interval
    ON segment_receipts(window_start_ns, window_end_ns, lane_id);

CREATE TABLE control_records (
    lane_id TEXT NOT NULL,
    delivery_index INTEGER NOT NULL,
    record_id TEXT NOT NULL,
    envelope_sha256 TEXT NOT NULL,
    visible_ns INTEGER NOT NULL,
    monotonic_ns INTEGER,
    venue TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    local_counter INTEGER NOT NULL,
    event TEXT NOT NULL,
    target_digest TEXT,
    target_metadata_digest TEXT,
    target_count INTEGER CHECK(target_count IS NULL OR target_count >= 0),
    receipt_key TEXT NOT NULL REFERENCES segment_receipts(receipt_key),
    PRIMARY KEY(lane_id, delivery_index),
    UNIQUE(record_id)
) STRICT;
CREATE INDEX controls_epoch
    ON control_records(lane_id, connection_epoch, delivery_index);

CREATE TABLE connection_epochs (
    lane_id TEXT NOT NULL,
    connection_epoch TEXT NOT NULL,
    venue TEXT NOT NULL,
    predecessor_epoch TEXT,
    first_delivery_index INTEGER NOT NULL,
    last_delivery_index INTEGER NOT NULL,
    observed_start_ns INTEGER NOT NULL,
    observed_end_ns INTEGER,
    socket_status TEXT NOT NULL,
    socket_opened_delivery_index INTEGER,
    send_status TEXT NOT NULL,
    send_completed_delivery_index INTEGER,
    venue_acceptance_status TEXT NOT NULL,
    venue_acceptance_delivery_index INTEGER,
    close_status TEXT NOT NULL,
    closed_delivery_index INTEGER,
    target_digest TEXT,
    target_digest_status TEXT NOT NULL,
    target_metadata_digest TEXT,
    PRIMARY KEY(lane_id, connection_epoch)
) STRICT;
CREATE INDEX epochs_digest ON connection_epochs(venue, target_digest);
CREATE INDEX epochs_interval
    ON connection_epochs(observed_start_ns, observed_end_ns, lane_id);
