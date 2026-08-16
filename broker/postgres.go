package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/crizah/Onion/task"

	"github.com/lib/pq" // registers the "postgres" driver, also gives us pq.Array below
)

// pgPollInterval is how often Dequeue re-checks for work while waiting.
// Postgres has no blocking-pop primitive like Redis's BRPOP, so we poll
// instead of blocking server-side.
const pgPollInterval = 250 * time.Millisecond

type PostgresBroker struct {
	db *sql.DB
}

func NewPostgresBroker(connStr string) (*PostgresBroker, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres broker: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres broker: ping: %w", err)
	}

	broker := &PostgresBroker{db: db}
	if err := broker.migrate(); err != nil {
		return nil, err
	}
	return broker, nil
}

func (p *PostgresBroker) migrate() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS onion_queue (
            id         BIGSERIAL PRIMARY KEY,
            queue      TEXT NOT NULL,
            payload    JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`,
		`CREATE INDEX IF NOT EXISTS idx_onion_queue_queue_created_at ON onion_queue (queue, created_at)`,
	}
	for _, s := range statements {
		if _, err := p.db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresBroker) Enqueue(ctx context.Context, queueName string, t *task.Task) error {
	payload, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	_, err = p.db.ExecContext(ctx, `INSERT INTO onion_queue (queue, payload) VALUES ($1, $2)`, queueName, payload)
	return err
}

// Dequeue polls for a task on any of the given queues, checked in the order
// given (callers pass them pre-sorted by priority), until one arrives or
// timeout elapses. Returns (nil, "", nil) on timeout.
func (p *PostgresBroker) Dequeue(ctx context.Context, queues []Queue, timeout time.Duration) (*task.Task, string, error) {
	names := make([]string, len(queues))
	for i, q := range queues {
		names[i] = q.Name
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(pgPollInterval)
	defer ticker.Stop()

	for {
		t, qn, err := p.tryDequeue(ctx, names)
		if err != nil || t != nil {
			return t, qn, err
		}
		if !time.Now().Before(deadline) {
			return nil, "", nil
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-ticker.C:
		}
	}
}

// tryDequeue attempts a single atomic pop: lock and remove the oldest task
// across the given queues, in queue-priority order, skipping rows already
// locked by another worker. Returns (nil, "", nil) if nothing is available.
func (p *PostgresBroker) tryDequeue(ctx context.Context, names []string) (*task.Task, string, error) {
	row := p.db.QueryRowContext(ctx, `
        DELETE FROM onion_queue
        WHERE id = (
            SELECT id FROM onion_queue
            WHERE queue = ANY($1)
            ORDER BY array_position($1::text[], queue), created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING queue, payload
    `, pq.Array(names))

	var queueName string
	var payload []byte
	err := row.Scan(&queueName, &payload)
	if err == sql.ErrNoRows {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", fmt.Errorf("dequeue: %w", err)
	}

	var t task.Task
	if err := json.Unmarshal(payload, &t); err != nil {
		return nil, "", fmt.Errorf("unmarshal task: %w", err)
	}
	return &t, queueName, nil
}

func (p *PostgresBroker) Ping(ctx context.Context) error {
	return p.db.PingContext(ctx)
}

func (p *PostgresBroker) Len(ctx context.Context, queueName string) (int64, error) {
	var n int64
	err := p.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM onion_queue WHERE queue = $1`, queueName).Scan(&n)
	return n, err
}
