package worker

import (
	"Onion/broker"
	"Onion/task"
	"context"
	"fmt"
	"sort"
	"time"
)

type Worker struct {
	Queues   []broker.Queue // wroker can be subscribes to multiple queues
	Broker   broker.Broker
	Registry *Registry
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
			t, err := w.dequeue(ctx) // dequeue in priority order
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
			t.Status = task.RUNNING
			if _, err := entry.TaskFunction(ctx, t.Args); err != nil {
				t.Status = task.FAILED
			} else {
				t.Status = task.COMPLETED
			}
		}
	}
}

func (w *Worker) dequeue(ctx context.Context) (*task.Task, error) {
	for _, q := range w.Queues {
		t, err := w.Broker.TryDequeue(ctx, q) // LPOP, non blocking
		if err != nil {
			return nil, err
		}
		if t != nil {
			return t, nil // got one, stop checking
		}
	}
	// nothing in any queue, sleep before next poll
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(2 * time.Second):
		return nil, nil // try again
	}
}
