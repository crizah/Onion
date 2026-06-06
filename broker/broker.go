package broker

import (
	"context"

	"Onion/queue"
	"Onion/task"
)

type Broker interface {
	Enqueue(ctx context.Context, queueName string, task *task.Task) error
	Dequeue(ctx context.Context, queue queue.Queue) (*task.Task, error)
	TryDequeue(ctx context.Context, queue queue.Queue) (*task.Task, error)
}
