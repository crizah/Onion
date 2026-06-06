package task

import (
	"context"

	"github.com/google/uuid"
)

type State string

const (
	RUNNING   State = "running"
	FAILED    State = "failed"
	PENDING   State = "pending"
	COMPLETED State = "completed"
)

type Task struct {
	Id     uuid.UUID      `json:"id"`
	Status State          `json:"status"`
	Name   string         `json:"name"`
	Args   map[string]any `json:"args"` // function args
	// TaskConfig TaskConfig     `json:"taskConfig"` // already sits in registry
}

type TaskFunction func(ctx context.Context, args map[string]any) (any, error)

type TaskConfig struct {
	MaxRetries int
	TimeLimit  int
	// othr shit
}

func New(name string, args map[string]any) *Task {

	return &Task{
		Id:     uuid.New(),
		Name:   name,
		Status: PENDING,
		Args:   args,
	}
}
