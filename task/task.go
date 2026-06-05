package task

import (
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
	Id         uuid.UUID      `json:"id"`
	Name       string         `json:"string"`
	Args       map[string]any `json:"args"`
	State      State          `json:"state"`
	MaxRetries int            `json:"max_retries"`
}

type Config struct {
	MaxRetries int // defaults to 3
}

func New(name string, config ...Config) *Task {
	maxRetries := 3
	if len(config) > 0 {
		maxRetries = config[0].MaxRetries
	}

	return &Task{
		Id:         uuid.New(),
		Name:       name,
		State:      PENDING,
		MaxRetries: maxRetries,
	}
}
