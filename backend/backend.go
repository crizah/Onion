package backend

//sql backend prolly

import (
	"Onion/task"
	"context"
)

// TaskRecord is the DB projection — includes queue and config resolved at execution time,
// kept out of task.Task to avoid a second source of truth.
type TaskRecord struct {
	*task.Task
	Queue  string
	Config task.TaskConfig
	Output any // output of the function
	Error  any
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
	List(ctx context.Context, f TaskFilter) (ListResult, error)
	Stats(ctx context.Context) (Stats, error)
}
