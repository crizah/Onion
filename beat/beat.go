package beat

import (
	"Onion/broker"
	"Onion/cron"
	"Onion/queue"
	"Onion/task"
	"context"
	"fmt"
)

type ScheduleEntry struct {
	TaskName string
	Cron     string // "0 9 * * *"
}

type Beat struct {
	Schedules []ScheduleEntry
	Broker    broker.Broker
	Queue     []queue.Queue
	cron      *cron.Cron
}

func New(schedules []ScheduleEntry, br broker.Broker, queue []queue.Queue) *Beat {
	return &Beat{
		Schedules: schedules,
		Broker:    br,
		Queue:     queue,
		cron:      cron.New(),
	}
}

func (b *Beat) Start(ctx context.Context) error {
	for _, s := range b.Schedules {
		s := s // capture loop var
		_, err := b.cron.AddFunc(s.Cron, func() {
			t := task.New(s.TaskName)
			if err := b.Broker.Enqueue(ctx, b.Queue, t); err != nil {
				fmt.Printf("beat: failed to enqueue %q: %v\n", s.TaskName, err)
			}
		})
		if err != nil {
			return fmt.Errorf("beat: bad schedule for %q: %w", s.TaskName, err)
		}
	}
	b.cron.Start()
	<-ctx.Done()
	b.cron.Stop()
	return nil
}
