package worker

import (
	"Onion/broker"
	"Onion/queue"
	"Onion/task"
	"context"
	"sort"
	"time"
)

type Worker struct {
	Queues   []queue.Queue // wroker can be subscribes to multiple queues
	Broker   broker.Broker
	Registry *Registry
}

func (w *Worker) Run(ctx context.Context) {
	// sort queue based on priority, should prolly use a better data structure here
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
				// raise ErrTaskNotRegistered here
				continue
			}

			t.State = task.RUNNING
			if err := entry.Function(ctx, t.Args); err != nil { // need to make function a type
				t.State = task.FAILED
			} else {
				t.State = task.COMPLETED
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
		return nil, nil
	}
}
