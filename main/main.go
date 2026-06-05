package main

import (
	"Onion/beat"
	"Onion/broker"
	"Onion/queue"
	"Onion/task"
	"Onion/worker"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type App struct {
	Broker    *broker.RedisBroker
	Backend   interface{} // dk wat to do with this for rn
	Registry  *worker.Registry
	Schedules []beat.ScheduleEntry
	Config    Config
}

type Config struct {
	BrokerAddr  string
	BackendURL  string // dk wat to do with this for rn
	Concurrency int
	Queues      []queue.Queue
}

func New(config ...Config) (*App, error) {
	// initialise new app with broker and bakend

	if len(config) == 0 {
		// log here
	}
	cfg := config[0] // unpack

	br := broker.New(cfg.BrokerAddr)
	ba := backend.New(cfg.BackendURL)

	// initialise an empty registry and only every append to it
	r := worker.New()

	return &App{
		Broker:   br,
		Backend:  ba,
		Registry: r,
	}, nil

}

func (a *App) Register(name string, fn interface{}, config ...task.Config) error {
	// only register the task, dont create one just yet

	cfg := task.Config{MaxRetries: 3} // defaults here
	if len(config) > 0 {
		cfg = config[0]
	}
	a.Registry.Set(name, worker.RegistryEntry{
		Function: fn,
		Config:   cfg,
	})

	return nil
}

func (a *App) Schedule(name string, cron string) {
	// adds tasks for the beat to read from
	a.Schedules = append(a.Schedules, beat.ScheduleEntry{
		TaskName: name,
		Cron:     cron,
	})
}

func (a *App) Start() error {
	// when to mention queue priority guys
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	concurrency := a.Config.Concurrency // TODO: default to 5 if not preset
	var wg sync.WaitGroup

	// 1. start worker pool
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := &worker.Worker{
				Queues:   a.Config.Queues,
				Broker:   *a.Broker,
				Registry: a.Registry,
			}
			w.Run(ctx)
		}()
	}

	// 2. start beat if schedules exist
	if len(a.Schedules) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := beat.New(a.Schedules, a.Broker, a.Config.Queues) // handle proprity quesues here
			if err := b.Start(ctx); err != nil {
				fmt.Printf("beat error: %v\n", err)
			}
		}()
	}

	// 3. block until signal
	wg.Wait()
	return nil
}

func main() {
	// example

	// app := Onion.New(config) // config has their broker and other configs
	// register a task
	// app.Register("name", function, args) // registers a task
	// app.Schedule("name", "cron") // schedule repeated tasks

	// app.Start() // runs
	// app.Enqueue(ctx, "send_email", args, goqueue.WithQueue("emails"))

}
