package broker

import (
	"context"
	"encoding/json"
	"fmt"

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

func (r *RedisBroker) Dequeue(ctx context.Context, q Queue) (*task.Task, error) {
	result, err := r.client.BRPop(ctx, 0, q.Name).Result()
	if err != nil {
		return nil, fmt.Errorf("dequeue: %w", err)
	}
	var t task.Task
	if err := json.Unmarshal([]byte(result[1]), &t); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &t, nil
}

func (r *RedisBroker) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func (r *RedisBroker) Len(ctx context.Context, queueName string) (int64, error) {
	return r.client.LLen(ctx, queueName).Result()
}

func (r *RedisBroker) TryDequeue(ctx context.Context, q Queue) (*task.Task, error) {

	result, err := r.client.LPop(ctx, q.Name).Result()
	if err == redis.Nil {
		return nil, nil // queue empty, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("dequeue %q: %w", q.Name, err)
	}
	var t task.Task
	if err := json.Unmarshal([]byte(result), &t); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &t, nil
}
