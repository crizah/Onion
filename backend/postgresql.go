package backend

import (
	"Onion/errors"
	"Onion/task"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

type PostgresBackend struct {
	db *sql.DB
}

func New(connStr string) (*PostgresBackend, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &PostgresBackend{db: db}, nil
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

	err := row.Scan(
		&t.Id, &t.Name, &args, &output, &t.RetryAttempt, &errr,
		&status, &r.Queue, &config,
		&t.CreatedAt, &t.StartedAt, &t.CompletedAt, &t.RetriedAt, &t.DurationMs,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf(errors.ErrTaskNotFound.Error(), id)
	}
	if err != nil {
		return nil, err
	}

	t.Status = task.State(status)
	json.Unmarshal(args, &t.Args)
	json.Unmarshal(output, &r.Output)
	json.Unmarshal(config, &r.Config)
	return &r, nil
}
