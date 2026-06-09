CREATE TABLE IF NOT EXISTS onion_tasks (
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

CREATE INDEX IF NOT EXISTS idx_onion_tasks_status     ON onion_tasks (status);
CREATE INDEX IF NOT EXISTS idx_onion_tasks_queue      ON onion_tasks (queue);
CREATE INDEX IF NOT EXISTS idx_onion_tasks_name       ON onion_tasks (name);
CREATE INDEX IF NOT EXISTS idx_onion_tasks_created_at ON onion_tasks (created_at DESC);
