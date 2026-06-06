package broker

import (
	"context"

	"Onion/task"
)

type Broker interface {
	Enqueue(ctx context.Context, queueName string, task *task.Task) error
	Dequeue(ctx context.Context, queue Queue) (*task.Task, error)
	TryDequeue(ctx context.Context, queue Queue) (*task.Task, error)
}

type Queue struct {
	Name     string
	Priority int
}
