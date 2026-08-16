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

type sqliteBackend struct {
	db    *sql.DB
	mu    sync.Mutex
	cache *statsCache
}

func NewSqlite(dbPath string) (*sqliteBackend, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("sqlite: ping: %w", err)
	}

	// Enable Write-Ahead Logging for better concurrent read/write performance
	if _, err := db.Exec(`PRAGMA journal_mode = WAL;`); err != nil {
		return nil, fmt.Errorf("sqlite: enable wal: %w", err)
	}

	backend := &sqliteBackend{db: db}
	err = backend.migrate()
	if err != nil {
		return nil, err
	}

	return backend, nil
}

func (b *sqliteBackend) migrate() error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS onion_tasks (
            id            TEXT PRIMARY KEY,
            name          TEXT NOT NULL,
            status        TEXT NOT NULL,
            queue         TEXT NOT NULL,
            args          TEXT,
            output        TEXT,
            config        TEXT,
            error         TEXT,
            retry_attempt INTEGER DEFAULT 0,
            created_at    DATETIME,
            started_at    DATETIME,
            completed_at  DATETIME,
            retried_at    DATETIME,
            duration_ms   INTEGER DEFAULT 0
        )`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_status     ON onion_tasks (status)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_queue      ON onion_tasks (queue)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_name       ON onion_tasks (name)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_created_at ON onion_tasks (created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_status_created_at ON onion_tasks (status, created_at DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_onion_tasks_queue_created_at  ON onion_tasks (queue, created_at DESC)`,
	}
	for _, stmt := range statements {
		if _, err := b.db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil

}

func (d *sqliteBackend) Save(ctx context.Context, r *TaskRecord) error {
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
		return fmt.Errorf("marshal error: %w", err)
	}

	_, err = d.db.ExecContext(ctx, `
        INSERT INTO onion_tasks (id, name, args, output, retry_attempt, error, status, queue, config, created_at, started_at, completed_at, retried_at, duration_ms)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT (id) DO UPDATE SET
            status       = EXCLUDED.status,
            output       = CASE WHEN EXCLUDED.output != 'null' THEN EXCLUDED.output ELSE onion_tasks.output END,
            retry_attempt = CASE WHEN EXCLUDED.retry_attempt != 0 THEN EXCLUDED.retry_attempt ELSE onion_tasks.retry_attempt END,
            error        = CASE WHEN EXCLUDED.error != 'null' THEN EXCLUDED.error ELSE onion_tasks.error END,
            started_at   = CASE WHEN EXCLUDED.started_at != '0001-01-01' AND EXCLUDED.started_at != '0001-01-01 00:00:00+00:00' THEN EXCLUDED.started_at ELSE onion_tasks.started_at END,
            completed_at = CASE WHEN EXCLUDED.completed_at != '0001-01-01' AND EXCLUDED.completed_at != '0001-01-01 00:00:00+00:00' THEN EXCLUDED.completed_at ELSE onion_tasks.completed_at END,
            retried_at   = CASE WHEN EXCLUDED.retried_at != '0001-01-01' AND EXCLUDED.retried_at != '0001-01-01 00:00:00+00:00' THEN EXCLUDED.retried_at ELSE onion_tasks.retried_at END,
            duration_ms  = CASE WHEN EXCLUDED.duration_ms != 0 THEN EXCLUDED.duration_ms ELSE onion_tasks.duration_ms END
    `,
		r.Id, r.Name, args, output, r.RetryAttempt, errJsonB,
		string(r.Status), r.Queue, config,
		r.CreatedAt, r.StartedAt, r.CompletedAt, r.RetriedAt, r.DurationMs,
	)
	return err
}

func (d *sqliteBackend) Get(ctx context.Context, id string) (*TaskRecord, error) {
	// not used
	row := d.db.QueryRowContext(ctx, `
        SELECT id, name, args, output, retry_attempt, error, status, queue, config, created_at, started_at, completed_at, retried_at, duration_ms
        FROM onion_tasks WHERE id = ?
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
		return nil, fmt.Errorf("%w: %s", errors.ErrTaskNotFound, id)
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
	json.Unmarshal(errr, &r.Error)
	json.Unmarshal(config, &r.Config)
	return &r, nil
}

func (b *sqliteBackend) List(ctx context.Context, f TaskFilter) (*ListResult, error) {
	if f.Limit <= 0 {
		f.Limit = 50
	} else if f.Limit > maxTaskLimit {
		f.Limit = maxTaskLimit
	}
	if f.Page <= 0 {
		f.Page = 1
	}
	offset := (f.Page - 1) * f.Limit

	where := " WHERE 1=1"
	var args []any
	if f.Status != "" {
		where += " AND status = ?"
		args = append(args, f.Status)
	}
	if f.Queue != "" {
		where += " AND queue = ?"
		args = append(args, f.Queue)
	}
	if f.Search != "" {
		// SQLite uses LIKE for case-insensitive matches (for ASCII)
		// and doesn't require explicit ::text casting
		where += " AND (name LIKE ? OR id LIKE ?)"
		args = append(args, "%"+f.Search+"%", "%"+f.Search+"%")
	}

	var total int
	if err := b.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM onion_tasks"+where, args...).Scan(&total); err != nil {
		return nil, err
	}

	q := `SELECT id, name, args, output, retry_attempt, error, status, queue, config,
                 created_at, started_at, completed_at, retried_at, duration_ms
          FROM onion_tasks` + where + " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, f.Limit, offset)

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
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
			return nil, err
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
	return &ListResult{Records: records, Total: total, Page: f.Page, Limit: f.Limit}, rows.Err()
}

func (b *sqliteBackend) Stats(ctx context.Context) (*Stats, error) {
	b.mu.Lock()
	if b.cache != nil && time.Now().Before(b.cache.expiresAt) {
		stat := b.cache.value
		b.mu.Unlock()
		return &stat, nil
	}
	b.mu.Unlock()

	row := b.db.QueryRowContext(ctx, `
        SELECT
            COUNT(*),
            COUNT(*) FILTER (WHERE status = 'pending'),
            COUNT(*) FILTER (WHERE status = 'running'),
            COUNT(*) FILTER (WHERE status = 'completed'),
            COUNT(*) FILTER (WHERE status = 'failed')
        FROM onion_tasks
    `)
	var stat Stats
	if err := row.Scan(&stat.Total, &stat.Pending, &stat.Running, &stat.Completed, &stat.Failed); err != nil {
		return nil, err
	}

	b.mu.Lock()
	b.cache = &statsCache{value: stat, expiresAt: time.Now().Add(5 * time.Second)}
	b.mu.Unlock()
	return &stat, nil
}
