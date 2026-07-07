package broker

import (
	"context"

	"github.com/crizah/Onion/task"
)

type Broker interface {
	Enqueue(ctx context.Context, queueName string, task *task.Task) error
	Dequeue(ctx context.Context, queue Queue) (*task.Task, error)
	TryDequeue(ctx context.Context, queue Queue) (*task.Task, error)
	Ping(ctx context.Context) error
	Len(ctx context.Context, queueName string) (int64, error)
}

type Queue struct {
	Name     string
	Priority int
}
