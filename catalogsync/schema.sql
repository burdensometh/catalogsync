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
-- replays reference a run or a reprocess job as their origin; exactly one is set
CREATE TABLE IF NOT EXISTS products (
    source_id TEXT NOT NULL REFERENCES sources(id),
    sku TEXT NOT NULL,
    revision INTEGER NOT NULL,
    rule_version INTEGER NOT NULL DEFAULT 0,
    title TEXT NOT NULL,
    category TEXT NOT NULL,
    currency TEXT NOT NULL,
    price_cents INTEGER NOT NULL,
    stock INTEGER NOT NULL,
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    attributes TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    last_run_id TEXT REFERENCES runs(id),
    last_job_id TEXT,
    updated_at TEXT NOT NULL,
    CHECK ((last_run_id IS NULL) != (last_job_id IS NULL)),
    PRIMARY KEY(source_id, sku)
);
CREATE INDEX IF NOT EXISTS product_category ON products(category, source_id);
CREATE INDEX IF NOT EXISTS product_revision ON products(source_id, revision);
-- the same source revision may be observed once per cleaning-rule version;
-- source_fingerprint identifies the raw source record independently of the
-- cleaning rules, so a rule change cannot hide a genuine source-side conflict.
CREATE TABLE IF NOT EXISTS product_versions (
    source_id TEXT NOT NULL REFERENCES sources(id),
    sku TEXT NOT NULL,
    revision INTEGER NOT NULL,
    rule_version INTEGER NOT NULL DEFAULT 0,
    fingerprint TEXT NOT NULL,
    source_fingerprint TEXT,
    parser TEXT,
    payload TEXT NOT NULL,
    raw_record TEXT,
    first_run_id TEXT REFERENCES runs(id),
    first_job_id TEXT,
    observed_at TEXT NOT NULL,
    CHECK ((first_run_id IS NULL) != (first_job_id IS NULL)),
    PRIMARY KEY(source_id, sku, revision, rule_version)
);
CREATE INDEX IF NOT EXISTS product_version_source
ON product_versions(source_id, rule_version);
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
-- reprocessing replays already-archived raw pages under one frozen rule version
CREATE TABLE IF NOT EXISTS reprocess_jobs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    base_run_id TEXT REFERENCES runs(id),
    target_profile_version INTEGER NOT NULL,
    profile_document TEXT NOT NULL,
    profile_fingerprint TEXT NOT NULL,
    snapshot_id TEXT,
    boundary INTEGER,
    status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'failed', 'completed', 'cancelled')),
    page_total INTEGER NOT NULL DEFAULT 0,
    next_ordinal INTEGER NOT NULL DEFAULT 0,
    page_count INTEGER NOT NULL DEFAULT 0,
    accepted INTEGER NOT NULL DEFAULT 0,
    rejected INTEGER NOT NULL DEFAULT 0,
    inserted INTEGER NOT NULL DEFAULT 0,
    updated INTEGER NOT NULL DEFAULT 0,
    unchanged INTEGER NOT NULL DEFAULT 0,
    stale INTEGER NOT NULL DEFAULT 0,
    conflicts INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    switched_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_reprocess
ON reprocess_jobs(source_id) WHERE status IN ('pending', 'running', 'failed');
CREATE TABLE IF NOT EXISTS reprocess_pages (
    job_id TEXT NOT NULL REFERENCES reprocess_jobs(id),
    ordinal INTEGER NOT NULL,
    source_run_id TEXT NOT NULL REFERENCES runs(id),
    cursor TEXT NOT NULL,
    next_cursor TEXT,
    boundary INTEGER NOT NULL,
    raw_sha256 TEXT NOT NULL,
    archive_path TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    accepted INTEGER,
    rejected INTEGER,
    committed_at TEXT,
    PRIMARY KEY(job_id, ordinal)
);
CREATE TABLE IF NOT EXISTS reprocess_records (
    job_id TEXT NOT NULL REFERENCES reprocess_jobs(id),
    page_ordinal INTEGER NOT NULL,
    record_ordinal INTEGER NOT NULL,
    sku TEXT NOT NULL,
    revision INTEGER NOT NULL,
    rule_version INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    source_fingerprint TEXT NOT NULL,
    parser TEXT NOT NULL,
    payload TEXT NOT NULL,
    raw_record TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(job_id, page_ordinal, record_ordinal)
);
CREATE INDEX IF NOT EXISTS reprocess_record_version
ON reprocess_records(job_id, sku, revision, rule_version);
CREATE TABLE IF NOT EXISTS reprocess_rejects (
    job_id TEXT NOT NULL REFERENCES reprocess_jobs(id),
    page_ordinal INTEGER NOT NULL,
    record_ordinal INTEGER NOT NULL,
    cursor TEXT NOT NULL,
    reason TEXT NOT NULL,
    raw_record TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(job_id, page_ordinal, record_ordinal)
);
