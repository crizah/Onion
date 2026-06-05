package broker

import (
	"context"

	"Onion/task"
)

type Broker interface {
	Enqueue(ctx context.Context, queue string, task *task.Task) error
}
