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

CREATE TABLE active_snapshot (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    run_id TEXT NOT NULL UNIQUE,
    generated_at TEXT NOT NULL,
    generated_at_ns INTEGER NOT NULL,
    indexed_at_ns INTEGER NOT NULL,
    strategy_version INTEGER,
    manifest_key TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    report_key TEXT NOT NULL,
    report_sha256 TEXT NOT NULL,
    report_byte_length INTEGER NOT NULL CHECK(report_byte_length >= 0),
    index_key TEXT NOT NULL,
    index_sha256 TEXT NOT NULL,
    index_byte_length INTEGER NOT NULL CHECK(index_byte_length >= 0),
    CHECK(strategy_version IS NULL OR strategy_version > 0)
) STRICT;

CREATE TABLE active_bundles (
    bundle_id TEXT PRIMARY KEY,
    origin_run_id TEXT NOT NULL,
    origin_generated_at TEXT NOT NULL,
    origin_generated_at_ns INTEGER NOT NULL,
    origin_manifest_key TEXT NOT NULL,
    origin_manifest_sha256 TEXT NOT NULL,
    origin_report_key TEXT NOT NULL,
    origin_report_sha256 TEXT NOT NULL,
    origin_report_byte_length INTEGER NOT NULL CHECK(origin_report_byte_length >= 0),
    origin_index_key TEXT NOT NULL,
    origin_index_sha256 TEXT NOT NULL,
    origin_index_byte_length INTEGER NOT NULL CHECK(origin_index_byte_length >= 0),
    continuity_selected INTEGER NOT NULL CHECK(continuity_selected IN (0, 1)),
    continuity_disposition TEXT CHECK(
        continuity_disposition IS NULL OR
        continuity_disposition IN ('held_current_candidate', 'retained')
    ),
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
    CHECK(
        (continuity_selected = 0 AND continuity_disposition IS NULL) OR
        (continuity_selected = 1 AND continuity_disposition IS NOT NULL)
    ),
    CHECK(capture_start_at_ns < activation_at_ns),
    CHECK(activation_at_ns < planned_capture_end_at_ns)
) STRICT;
CREATE INDEX active_bundles_window
    ON active_bundles(capture_start_at_ns, planned_capture_end_at_ns, bundle_id);
CREATE INDEX active_bundles_origin
    ON active_bundles(origin_run_id, bundle_id);

CREATE TABLE bundle_participants (
    bundle_id TEXT NOT NULL REFERENCES active_bundles(bundle_id) ON DELETE CASCADE,
    position INTEGER NOT NULL CHECK(position IN (0, 1)),
    name TEXT NOT NULL,
    participant_key TEXT NOT NULL,
    PRIMARY KEY(bundle_id, position)
) STRICT;

CREATE TABLE bundle_events (
    bundle_id TEXT NOT NULL REFERENCES active_bundles(bundle_id) ON DELETE CASCADE,
    event_ref TEXT NOT NULL,
    venue TEXT NOT NULL,
    PRIMARY KEY(bundle_id, event_ref)
) STRICT;

CREATE TABLE bundle_markets (
    bundle_id TEXT NOT NULL REFERENCES active_bundles(bundle_id) ON DELETE CASCADE,
    target_id TEXT NOT NULL,
    venue TEXT NOT NULL,
    selected INTEGER NOT NULL CHECK(selected IN (0, 1)),
    PRIMARY KEY(bundle_id, target_id)
) STRICT;

CREATE TABLE selected_targets (
    bundle_id TEXT NOT NULL,
    target_id TEXT PRIMARY KEY,
    venue TEXT NOT NULL,
    canonical_class TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    FOREIGN KEY(bundle_id, target_id)
        REFERENCES bundle_markets(bundle_id, target_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE selected_target_assets (
    target_id TEXT NOT NULL REFERENCES selected_targets(target_id) ON DELETE CASCADE,
    asset_id TEXT NOT NULL,
    PRIMARY KEY(target_id, asset_id)
) STRICT;

CREATE TABLE bundle_relationships (
    bundle_id TEXT NOT NULL REFERENCES active_bundles(bundle_id) ON DELETE CASCADE,
    relationship_index INTEGER NOT NULL,
    left_market TEXT NOT NULL,
    right_market TEXT NOT NULL,
    relationship TEXT NOT NULL,
    scope TEXT NOT NULL,
    left_venue TEXT NOT NULL,
    right_venue TEXT NOT NULL,
    coverage TEXT NOT NULL,
    PRIMARY KEY(bundle_id, relationship_index)
) STRICT;

CREATE TABLE subscription_sets (
    venue TEXT PRIMARY KEY,
    target_digest TEXT NOT NULL,
    asset_count INTEGER NOT NULL CHECK(asset_count >= 0)
) STRICT;
CREATE INDEX subscription_sets_digest ON subscription_sets(venue, target_digest);

CREATE TABLE subscription_assets (
    venue TEXT NOT NULL REFERENCES subscription_sets(venue) ON DELETE CASCADE,
    asset_id TEXT NOT NULL,
    PRIMARY KEY(venue, asset_id)
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
