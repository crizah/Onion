package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/crizah/Onion/task"

	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client *redis.Client
}

func New(addr string) *RedisBroker {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisBroker{client: client}
}

func (r *RedisBroker) Enqueue(ctx context.Context, qn string, t *task.Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	return r.client.LPush(ctx, qn, data).Err() // change this to have priority, at some point
}

// Dequeue blocks (server-side, in Redis) until a task is available on any
// of the given queues or timeout elapses. Queues are checked in the order
// given, so callers should pass them pre-sorted by priority. Uses RPOP's
// blocking form (BRPOP) to pair with Enqueue's LPUSH, giving FIFO order —
// oldest task in a given queue is popped first.
func (r *RedisBroker) Dequeue(ctx context.Context, queues []Queue, timeout time.Duration) (*task.Task, string, error) {
	names := make([]string, len(queues))
	for i, q := range queues {
		names[i] = q.Name
	}
	result, err := r.client.BRPop(ctx, timeout, names...).Result()
	if err == redis.Nil {
		return nil, "", nil // timeout, nothing arrived
	}
	if err != nil {
		return nil, "", fmt.Errorf("dequeue: %w", err)
	}
	var t task.Task
	if err := json.Unmarshal([]byte(result[1]), &t); err != nil {
		return nil, "", fmt.Errorf("unmarshal task: %w", err)
	}
	return &t, result[0], nil
}

func (r *RedisBroker) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RedisBroker) Len(ctx context.Context, queueName string) (int64, error) {
	return r.client.LLen(ctx, queueName).Result()
}
