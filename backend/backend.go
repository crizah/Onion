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

type Backend interface {
	Save(ctx context.Context, r *TaskRecord) error
	Get(ctx context.Context, id string) (*TaskRecord, error)
}
