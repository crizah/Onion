package beat

import (
	"Onion/backend"
	"Onion/broker"
	"Onion/task"
	"context"
	"fmt"

	"github.com/robfig/cron/v3"
)

type ScheduleEntry struct {
	ScheduleName string
	TaskName     string
	Expr         string
	Args         map[string]any
}

type Beat struct {
	Schedules    []ScheduleEntry
	Broker       broker.Broker
	Backend      backend.Backend
	Queues       []broker.Queue
	TaskRoutes   map[string]string
	DefaultQueue string
	cron         *cron.Cron
}

func New(schedules []ScheduleEntry, br broker.Broker, queue []broker.Queue, taskRoutes map[string]string, dq string, ba backend.Backend) *Beat {
	// only called when making an actual beat
	return &Beat{
		Schedules:    schedules,
		Broker:       br,
		Queues:       queue,
		DefaultQueue: dq,
		TaskRoutes:   taskRoutes,
		cron:         cron.New(),
		Backend:      ba,
	}
}

func (b *Beat) Start(ctx context.Context) error {
	for _, s := range b.Schedules {
		s := s
		_, err := b.cron.AddFunc(s.Expr, func() { // schedule the cron for it to add to queue
			t := task.New(s.TaskName, s.Args)

			q := b.resolveQueue(s.TaskName)
			record := &backend.TaskRecord{Task: t, Queue: q.Name} // no config — beat doesn't have registry access
			b.Backend.Save(ctx, record)                           // write

			if err := b.Broker.Enqueue(ctx, q.Name, t); err != nil {
				fmt.Printf("beat: failed to enqueue %q: %v\n", s.TaskName, err)
			}
		})
		if err != nil {
			return fmt.Errorf("beat: invalid cron expr for %q: %w", s.ScheduleName, err)
		}
	}
	b.cron.Start()
	<-ctx.Done()
	b.cron.Stop()
	return nil
}

func (b *Beat) resolveQueue(taskName string) broker.Queue {
	name := b.DefaultQueue

	if qn, ok := b.TaskRoutes[taskName]; ok {
		name = qn
	}

	for _, q := range b.Queues {
		if q.Name == name {
			return q
		}
	}

	// fallback — return default queue bare if not found in list
	return broker.Queue{Name: b.DefaultQueue, Priority: 5}
}
