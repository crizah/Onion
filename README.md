- Task Queueing system that uses redis for the queues and postgresql for the backend. 
- Uses goroutines to carry out the tasks, pretty much an abstraction over goroutine tasks with more tranperency + you can define worker queues. 
- Like celery
- Has a "real time" dashboard to moniter tasks, workers and queue depths
- Configurable worker pools with priority queues, critical tasks always run first
- Gives you more control over goroutines as they go to a broker queue and can be ingested by any machine, goroutines run on the main server machine, these tasks can be ingested by any worker machine. 
- Supports beat scheduling tasks (repeated cron tasks)
- Supports configuring retries executed automatically with exponential backoff
- main.go has an example on how to use it
- These values are stored in the task table in the configured db 
```
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
```
