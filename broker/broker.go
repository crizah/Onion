package broker

import (
	"context"
	"time"

	"github.com/crizah/Onion/task"
)

type Broker interface {
	Enqueue(ctx context.Context, queueName string, task *task.Task) error
	// Dequeue blocks on all given queues (in priority order) until a task
	// arrives or timeout elapses. Returns (nil, "", nil) on timeout.
	Dequeue(ctx context.Context, queues []Queue, timeout time.Duration) (*task.Task, string, error)
	Ping(ctx context.Context) error
	Len(ctx context.Context, queueName string) (int64, error)
}

type Queue struct {
	Name     string
	Priority int
}

type BrokerType string

const (
	BrokerRedis    BrokerType = "REDIS"
	BrokerPostgres BrokerType = "POSTGRESQL"
)
