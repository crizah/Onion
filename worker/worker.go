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
			t, qName, err := w.dequeue(ctx) // blocks on all queues, priority order
			if err != nil {
				if ctx.Err() != nil {
					return // shutting down, not a broker failure
				}
				// transient broker error (e.g. network blip) — log, back off,
				// and keep polling instead of silently killing this worker
				fmt.Printf("[worker %d] dequeue error: %v — retrying in 2s\n", w.ID, err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				continue
			}
			if t == nil {
				continue // timed out waiting, nothing arrived — loop and block again
			}

			entry, lookupErr := w.Registry.Lookup(t.Name)
			if lookupErr != nil {
				fmt.Printf("[worker] lookup failed for %q: %v\n", t.Name, lookupErr)
				t.Status = task.FAILED
				t.CompletedAt = time.Now()
				w.Backend.Save(ctx, &backend.TaskRecord{Task: t, Queue: qName, Error: lookupErr.Error()})
				continue
			}

			record := &backend.TaskRecord{Task: t, Queue: qName, Config: entry.TaskConfig}

			w.Pool.SetBusy(w.ID, t.Name)
			t.Status = task.RUNNING
			t.StartedAt = time.Now()
			w.Backend.Save(ctx, record)

			output, err := func() (out any, funcErr error) {
				defer func() {
					if r := recover(); r != nil {
						fmt.Printf("[worker %d] recovered panic in task %q: %v\n", w.ID, t.Name, r)
						funcErr = fmt.Errorf("panic: %v", r)
					}
				}()
				return entry.TaskFunction(ctx, t.Args)
			}()
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
						w.Broker.Enqueue(context.Background(), qName, t)
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

func (w *Worker) dequeue(ctx context.Context) (*task.Task, string, error) {
	// Single blocking call across all queues (already priority-sorted in
	// Run()) — Redis waits server-side and returns as soon as any queue has
	// a task, checking queues in the order given. No busy-poll loop needed.
	return w.Broker.Dequeue(ctx, w.Queues, 2*time.Second)
}
