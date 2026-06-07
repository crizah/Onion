CREATE TABLE IF NOT EXISTS tasks (
    id            UUID PRIMARY KEY,
    name          TEXT NOT NULL,
    status        TEXT NOT NULL,
    queue         TEXT NOT NULL,
    args          JSONB,
    output        JSONB,
    config        JSONB,
    error         JSONB,
    retry_attempt INT DEFAULT 0,
    created_at    TIMESTAMPTZ,
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    retried_at    TIMESTAMPTZ,
    duration_ms   BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tasks_status     ON tasks (status);
CREATE INDEX IF NOT EXISTS idx_tasks_queue      ON tasks (queue);
CREATE INDEX IF NOT EXISTS idx_tasks_name       ON tasks (name);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks (created_at DESC);
