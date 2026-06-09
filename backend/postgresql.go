package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/crizah/Onion/errors"
	"github.com/crizah/Onion/task"
)

type statsCache struct {
	value     Stats
	expiresAt time.Time
}

type PostgresBackend struct {
	db    *sql.DB
	mu    sync.Mutex
	cache *statsCache
}

func New(connStr string) (*PostgresBackend, error) {
	// create and apply migrations here
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	backend := &PostgresBackend{db: db}
	err = backend.migrate()
	if err != nil {
		// dont continue if we cant migrate
		return nil, err
	}
	return backend, nil
}

func (p *PostgresBackend) migrate() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS onion_tasks (
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
        )`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_status     ON onion_tasks (status)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_queue      ON onion_tasks (queue)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_name       ON onion_tasks (name)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_created_at ON onion_tasks (created_at DESC)`,
	}
	for _, s := range statements {
		if _, err := p.db.Exec(s); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostgresBackend) Save(ctx context.Context, r *TaskRecord) error {
	args, err := json.Marshal(r.Args)
	if err != nil {
		return fmt.Errorf("marshal args: %w", err)
	}
	output, err := json.Marshal(r.Output)
	if err != nil {
		return fmt.Errorf("marshal output: %w", err)
	}
	config, err := json.Marshal(r.Config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	errJsonB, err := json.Marshal(r.Error)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	_, err = p.db.ExecContext(ctx, `
        INSERT INTO tasks (id, name, args, output, retry_attempt, error, status, queue, config, created_at, started_at, completed_at, retried_at, duration_ms)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (id) DO UPDATE SET
            status       = EXCLUDED.status,
            output       = CASE WHEN EXCLUDED.output != 'null'::jsonb THEN EXCLUDED.output ELSE tasks.output END,
			retry_attempt = CASE WHEN EXCLUDED.retry_attempt != 0 THEN EXCLUDED.retry_attempt ELSE tasks.retry_attempt END,
			error        = CASE WHEN EXCLUDED.error != 'null'::jsonb THEN EXCLUDED.error ELSE tasks.error END,
            started_at   = CASE WHEN EXCLUDED.started_at != '0001-01-01' THEN EXCLUDED.started_at ELSE tasks.started_at END,
            completed_at = CASE WHEN EXCLUDED.completed_at != '0001-01-01' THEN EXCLUDED.completed_at ELSE tasks.completed_at END,
			retried_at   = CASE WHEN EXCLUDED.retried_at != '0001-01-01' THEN EXCLUDED.retried_at ELSE tasks.retried_at END,
            duration_ms  = CASE WHEN EXCLUDED.duration_ms != 0 THEN EXCLUDED.duration_ms ELSE tasks.duration_ms END
    `,
		r.Id, r.Name, args, output, r.RetryAttempt, errJsonB,
		string(r.Status), r.Queue, config,
		r.CreatedAt, r.StartedAt, r.CompletedAt, r.RetriedAt, r.DurationMs,
	)
	return err
}

func (p *PostgresBackend) List(ctx context.Context, f TaskFilter) (ListResult, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	where := " WHERE 1=1"
	var args []any
	i := 1
	if f.Status != "" {
		where += fmt.Sprintf(" AND status = $%d", i)
		args = append(args, f.Status)
		i++
	}
	if f.Queue != "" {
		where += fmt.Sprintf(" AND queue = $%d", i)
		args = append(args, f.Queue)
		i++
	}
	if f.Search != "" {
		where += fmt.Sprintf(" AND (name ILIKE $%d OR id::text ILIKE $%d)", i, i)
		args = append(args, "%"+f.Search+"%")
		i++
	}

	var total int
	if err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks"+where, args...).Scan(&total); err != nil {
		return ListResult{}, err
	}

	q := `SELECT id, name, args, output, retry_attempt, error, status, queue, config,
	             created_at, started_at, completed_at, retried_at, duration_ms
	      FROM tasks` + where + fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", i, i+1)
	args = append(args, f.Limit, offset)

	rows, err := p.db.QueryContext(ctx, q, args...)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()

	var records []*TaskRecord
	for rows.Next() {
		var t task.Task
		var r TaskRecord
		r.Task = &t
		var rawArgs, output, errb, config []byte
		var status string
		var startedAt, completedAt, retriedAt sql.NullTime
		if err := rows.Scan(
			&t.Id, &t.Name, &rawArgs, &output, &t.RetryAttempt, &errb,
			&status, &r.Queue, &config,
			&t.CreatedAt, &startedAt, &completedAt, &retriedAt, &t.DurationMs,
		); err != nil {
			return ListResult{}, err
		}
		t.Status = task.State(status)
		t.StartedAt = startedAt.Time
		t.CompletedAt = completedAt.Time
		t.RetriedAt = retriedAt.Time
		json.Unmarshal(rawArgs, &t.Args)
		json.Unmarshal(output, &r.Output)
		json.Unmarshal(errb, &r.Error)
		json.Unmarshal(config, &r.Config)
		records = append(records, &r)
	}
	if records == nil {
		records = []*TaskRecord{}
	}
	return ListResult{Records: records, Total: total, Page: f.Page, Limit: f.Limit}, rows.Err()
}

func (p *PostgresBackend) Stats(ctx context.Context) (Stats, error) {
	p.mu.Lock()
	if p.cache != nil && time.Now().Before(p.cache.expiresAt) {
		s := p.cache.value
		p.mu.Unlock()
		return s, nil
	}
	p.mu.Unlock()

	row := p.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'running'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed')
		FROM tasks
	`)
	var s Stats
	if err := row.Scan(&s.Total, &s.Pending, &s.Running, &s.Completed, &s.Failed); err != nil {
		return Stats{}, err
	}

	p.mu.Lock()
	p.cache = &statsCache{value: s, expiresAt: time.Now().Add(5 * time.Second)}
	p.mu.Unlock()
	return s, nil
}

func (p *PostgresBackend) Get(ctx context.Context, id string) (*TaskRecord, error) {
	// not used
	row := p.db.QueryRowContext(ctx, `
        SELECT id, name, args, output, retry_attempt, error, status, queue, config, created_at, started_at, completed_at, retried_at, duration_ms
        FROM tasks WHERE id = $1
    `, id)

	var t task.Task
	var r TaskRecord
	r.Task = &t
	var args, output, errr, config []byte
	var status string
	var startedAt, completedAt, retriedAt sql.NullTime

	err := row.Scan(
		&t.Id, &t.Name, &args, &output, &t.RetryAttempt, &errr,
		&status, &r.Queue, &config,
		&t.CreatedAt, &startedAt, &completedAt, &retriedAt, &t.DurationMs,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf(errors.ErrTaskNotFound.Error(), id)
	}
	if err != nil {
		return nil, err
	}

	t.Status = task.State(status)
	t.StartedAt = startedAt.Time
	t.CompletedAt = completedAt.Time
	t.RetriedAt = retriedAt.Time
	json.Unmarshal(args, &t.Args)
	json.Unmarshal(output, &r.Output)
	json.Unmarshal(config, &r.Config)
	return &r, nil
}
