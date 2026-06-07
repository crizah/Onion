CREATE TABLE IF NOT EXISTS tasks (
    id           UUID PRIMARY KEY,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,
    queue        TEXT NOT NULL,
    args         JSONB,
    output       JSONB,
    config       JSONB,
    error        JSONB,
    created_at   TIMESTAMPTZ,
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    retried_at TIMESTAMPTZ
    duration_ms  BIGINT DEFAULT 0
);
