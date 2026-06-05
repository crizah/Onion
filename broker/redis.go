package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"Onion/task"

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

func (r *RedisBroker) Enqueue(ctx context.Context, queue string, t *task.Task) error {
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	return r.client.LPush(ctx, queue, data).Err()
}
