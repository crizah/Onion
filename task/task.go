package task

import (
	"context"
	"time"

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
	Output      any       `json:"output"` // output of the function
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurationMs  int64     `json:"duration_ms"`
}

type TaskFunction func(ctx context.Context, args map[string]any) (any, error)

type TaskConfig struct {
	MaxRetries int
	TimeLimit  int
	// othr shit
}

func New(name string, args map[string]any) *Task {

	return &Task{
		Id:        uuid.New(),
		Name:      name,
		Status:    PENDING,
		Args:      args,
		CreatedAt: time.Now(),
	}
}
