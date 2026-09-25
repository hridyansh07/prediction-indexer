CREATE TABLE IF NOT EXISTS nonces (
    nonce TEXT PRIMARY KEY CHECK(typeof(nonce) = 'text' AND length(nonce) = 32 AND nonce = lower(nonce) AND nonce NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL CHECK(typeof(created_at) = 'integer' AND created_at >= 0),
    expires_at INTEGER NOT NULL CHECK(typeof(expires_at) = 'integer' AND expires_at > created_at),
    used_at INTEGER CHECK(used_at IS NULL OR (typeof(used_at) = 'integer' AND used_at >= created_at AND used_at < expires_at))
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS nonces_expiry ON nonces(expires_at);

CREATE TABLE IF NOT EXISTS sessions (
    token_hash TEXT PRIMARY KEY CHECK(typeof(token_hash) = 'text' AND length(token_hash) = 64 AND token_hash = lower(token_hash) AND token_hash NOT GLOB '*[^0-9a-f]*'),
    address TEXT NOT NULL CHECK(typeof(address) = 'text' AND length(address) = 42 AND address = lower(address) AND substr(address, 1, 2) = '0x' AND substr(address, 3) NOT GLOB '*[^0-9a-f]*'),
    role TEXT NOT NULL CHECK(typeof(role) = 'text' AND role IN ('member', 'admin')),
    created_at INTEGER NOT NULL CHECK(typeof(created_at) = 'integer' AND created_at >= 0),
    expires_at INTEGER NOT NULL CHECK(typeof(expires_at) = 'integer' AND expires_at > created_at),
    revoked_at INTEGER CHECK(revoked_at IS NULL OR (typeof(revoked_at) = 'integer' AND revoked_at >= created_at))
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions(expires_at);
CREATE INDEX IF NOT EXISTS sessions_live_address ON sessions(address, expires_at) WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS allowlist (
    address TEXT PRIMARY KEY CHECK(typeof(address) = 'text' AND length(address) = 42 AND address = lower(address) AND substr(address, 1, 2) = '0x' AND substr(address, 3) NOT GLOB '*[^0-9a-f]*'),
    note TEXT NOT NULL CHECK(typeof(note) = 'text' AND length(note) <= 256 AND note NOT GLOB '*[^ -~]*'),
    created_at INTEGER NOT NULL CHECK(typeof(created_at) = 'integer' AND created_at >= 0),
    created_by TEXT NOT NULL CHECK(typeof(created_by) = 'text' AND length(created_by) = 42 AND created_by = lower(created_by) AND substr(created_by, 1, 2) = '0x' AND substr(created_by, 3) NOT GLOB '*[^0-9a-f]*')
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS allowlist_events (
    event_id INTEGER PRIMARY KEY,
    action TEXT NOT NULL CHECK(typeof(action) = 'text' AND action IN ('add', 'remove')),
    address TEXT NOT NULL CHECK(typeof(address) = 'text' AND length(address) = 42 AND address = lower(address) AND substr(address, 1, 2) = '0x' AND substr(address, 3) NOT GLOB '*[^0-9a-f]*'),
    note TEXT CHECK(note IS NULL OR (typeof(note) = 'text' AND length(note) <= 256 AND note NOT GLOB '*[^ -~]*')),
    actor_address TEXT NOT NULL CHECK(typeof(actor_address) = 'text' AND length(actor_address) = 42 AND actor_address = lower(actor_address) AND substr(actor_address, 1, 2) = '0x' AND substr(actor_address, 3) NOT GLOB '*[^0-9a-f]*'),
    created_at INTEGER NOT NULL CHECK(typeof(created_at) = 'integer' AND created_at >= 0)
);

CREATE INDEX IF NOT EXISTS allowlist_events_address ON allowlist_events(address, event_id);

CREATE TRIGGER IF NOT EXISTS allowlist_events_no_update
BEFORE UPDATE ON allowlist_events
BEGIN
    SELECT RAISE(ABORT, 'allowlist events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS allowlist_events_no_delete
BEFORE DELETE ON allowlist_events
BEGIN
    SELECT RAISE(ABORT, 'allowlist events are append-only');
END;
