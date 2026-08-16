package backend

//sql backend prolly

import (
	"context"

	"github.com/crizah/Onion/task"
)

// TaskRecord is the DB projection — includes queue and config resolved at execution time,
// kept out of task.Task to avoid a second source of truth.
type TaskRecord struct {
	*task.Task
	Queue  string          `json:"queue"`
	Config task.TaskConfig `json:"config"`
	Output any             `json:"output"`
	Error  any             `json:"error"`
}

type TaskFilter struct {
	Search string
	Status string
	Queue  string
	Page   int // 1-indexed
	Limit  int // default 50
}

type ListResult struct {
	Records []*TaskRecord `json:"records"`
	Total   int           `json:"total"`
	Page    int           `json:"page"`
	Limit   int           `json:"limit"`
}

type Stats struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type Backend interface {
	Save(ctx context.Context, r *TaskRecord) error
	Get(ctx context.Context, id string) (*TaskRecord, error)
	List(ctx context.Context, f TaskFilter) (*ListResult, error)
	Stats(ctx context.Context) (*Stats, error)
	migrate() error
}

type DatabaseType string

const (
	DBTypeSQLite   DatabaseType = "SQLITE"
	DBTypePostgres DatabaseType = "POSTGRESQL"
)
