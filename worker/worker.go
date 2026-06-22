package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/crizah/Onion/task"

	"github.com/crizah/Onion/backend"
	"github.com/crizah/Onion/broker"
	oerrors "github.com/crizah/Onion/errors"
)

type Worker struct {
	ID       int
	Queues   []broker.Queue // wroker can be subscribes to multiple queues
	Broker   broker.Broker
	Registry *Registry
	Backend  backend.Backend
	Pool     *Pool
}

func (w *Worker) Run(ctx context.Context) {
	// sort queue based on priority
	sort.Slice(w.Queues, func(i, j int) bool {
		return w.Queues[i].Priority > w.Queues[j].Priority
	})

	for {
		select {
		case <-ctx.Done():
			return
		default:
			t, q, err := w.dequeue(ctx) // dequeue in priority order
			if err != nil {
				return
			}
			if t == nil {
				continue // nothing in any queue
			}

			entry, err := w.Registry.Lookup(t.Name)
			if err != nil {
				fmt.Printf("[worker] lookup failed for %q: %v\n", t.Name, err)
				continue
			}

			record := &backend.TaskRecord{Task: t, Queue: q.Name, Config: entry.TaskConfig}

			w.Pool.SetBusy(w.ID, t.Name)
			t.Status = task.RUNNING
			t.StartedAt = time.Now()
			w.Backend.Save(ctx, record)

			output, err := entry.TaskFunction(ctx, t.Args)
			now := time.Now()
			// if err == ErrRetry, we retry here
			if err != nil {
				if errors.Is(err, oerrors.ErrRetry) && t.RetryAttempt < entry.TaskConfig.MaxRetries {
					t.RetryAttempt++
					backoff := time.Duration(math.Pow(2, float64(t.RetryAttempt))) * time.Second
					t.RetriedAt = now.Add(backoff)

					t.Status = task.PENDING
					w.Backend.Save(ctx, record) // save RETRY state + retry_at
					time.AfterFunc(backoff, func() {
						w.Broker.Enqueue(context.Background(), q.Name, t)
					})
					fmt.Printf("[task] retried for %q", t.Name)
				} else {
					t.DurationMs = now.Sub(t.StartedAt).Milliseconds()
					t.CompletedAt = now
					t.Status = task.FAILED
					record.Error = err.Error()
					w.Backend.Save(ctx, record)
				}
			} else {
				t.CompletedAt = now
				t.DurationMs = now.Sub(t.StartedAt).Milliseconds()
				t.Status = task.COMPLETED
				t.Output = output
				record.Output = output 

			}
			w.Backend.Save(ctx, record)
			w.Pool.SetIdle(w.ID)
		}
	}
}

func (w *Worker) dequeue(ctx context.Context) (*task.Task, broker.Queue, error) {
	for _, q := range w.Queues {
		t, err := w.Broker.TryDequeue(ctx, q) // LPOP, non blocking
		if err != nil {
			return nil, broker.Queue{}, err
		}
		if t != nil {
			return t, q, nil // got one, stop checking
		}
	}
	// nothing in any queue, sleep before next poll
	select {
	case <-ctx.Done():
		return nil, broker.Queue{}, ctx.Err()
	case <-time.After(2 * time.Second):
		return nil, broker.Queue{}, nil // try again
	}
}
