PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    installed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('file', 'http')),
    location TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    mode TEXT NOT NULL CHECK(mode IN ('snapshot', 'delta')),
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'failed', 'completed')),
    snapshot_id TEXT,
    boundary INTEGER,
    next_cursor TEXT,
    page_count INTEGER NOT NULL DEFAULT 0,
    accepted INTEGER NOT NULL DEFAULT 0,
    rejected INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    updated INTEGER NOT NULL DEFAULT 0,
    unchanged INTEGER NOT NULL DEFAULT 0,
    stale INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_snapshot
ON runs(source_id) WHERE mode = 'snapshot' AND status IN ('pending', 'running');
CREATE TABLE IF NOT EXISTS pages (
    run_id TEXT NOT NULL REFERENCES runs(id),
    cursor TEXT NOT NULL,
    next_cursor TEXT,
    raw_sha256 TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    accepted INTEGER NOT NULL,
    rejected INTEGER NOT NULL,
    committed_at TEXT NOT NULL,
    PRIMARY KEY(run_id, cursor)
);
CREATE TABLE IF NOT EXISTS products (
    source_id TEXT NOT NULL REFERENCES sources(id),
    sku TEXT NOT NULL,
    revision INTEGER NOT NULL,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    currency TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    stock INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    attributes TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    last_run_id TEXT NOT NULL REFERENCES runs(id),
    updated_at TEXT NOT NULL,
    PRIMARY KEY(source_id, sku)
);
CREATE INDEX IF NOT EXISTS product_category ON products(category, source_id);
CREATE INDEX IF NOT EXISTS product_revision ON products(source_id, revision);
CREATE TABLE IF NOT EXISTS product_versions (
    source_id TEXT NOT NULL REFERENCES sources(id),
    sku TEXT NOT NULL,
    revision INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    payload TEXT NOT NULL,
    first_run_id TEXT NOT NULL REFERENCES runs(id),
    observed_at TEXT NOT NULL,
    PRIMARY KEY(source_id, sku, revision)
);
CREATE TABLE IF NOT EXISTS rejects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES runs(id),
    cursor TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    reason TEXT NOT NULL,
    raw_record TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(run_id, cursor, ordinal)
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT,
    run_id TEXT,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS publications (
    id TEXT PRIMARY KEY,
    source_id TEXT REFERENCES sources(id),
    status TEXT NOT NULL CHECK(status IN ('building', 'ready', 'failed')),
    format TEXT NOT NULL CHECK(format IN ('jsonl', 'csv')),
    sellable_only INTEGER NOT NULL,
    output_path TEXT,
    sha256 TEXT,
    row_count INTEGER,
    revision_vector TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE TABLE IF NOT EXISTS source_profiles (
    source_id TEXT NOT NULL REFERENCES sources(id),
    version INTEGER NOT NULL,
    document TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(source_id, version)
);
CREATE TABLE IF NOT EXISTS run_profiles (
    run_id TEXT PRIMARY KEY REFERENCES runs(id),
    profile_version INTEGER NOT NULL,
    document TEXT NOT NULL,
    fingerprint TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inbox_batches (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    run_id TEXT REFERENCES runs(id),
    status TEXT NOT NULL CHECK(status IN ('reserved', 'committed')),
    event_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_reserved_inbox
ON inbox_batches(source_id) WHERE status = 'reserved';
CREATE TABLE IF NOT EXISTS inbox_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL REFERENCES sources(id),
    event_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'reserved', 'committed', 'rejected')),
    batch_id TEXT REFERENCES inbox_batches(id),
    received_at TEXT NOT NULL,
    processed_at TEXT,
    UNIQUE(source_id, event_id)
);
CREATE INDEX IF NOT EXISTS inbox_pending ON inbox_events(source_id, status, sequence);
CREATE TABLE IF NOT EXISTS file_feeds (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    path TEXT NOT NULL,
    file_identity TEXT NOT NULL,
    byte_offset INTEGER NOT NULL,
    line_count INTEGER NOT NULL,
    checked_at TEXT NOT NULL
);
