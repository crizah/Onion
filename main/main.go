package main

import (
	"Onion/beat"
	"Onion/broker"
	"Onion/errors"
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
	Broker    broker.Broker
	Backend   interface{} // dk wat to do with this for rn
	Registry  *worker.Registry
	running   bool
	Schedules []beat.ScheduleEntry
	Config    Config
}

type Config struct {
	BrokerAddr   string
	BackendURL   string // dk wat to do with this for rn
	Concurrency  int
	Queues       []queue.Queue
	TaskRoutes   map[string]string // user can update as {"taskName", "queuename"}
	DefaultQueue string            // default queue for user to define
}

func defaultConfig() Config {
	return Config{
		Concurrency:  5,
		DefaultQueue: "default",
		TaskRoutes:   make(map[string]string),
		Queues:       []queue.Queue{{Name: "defult", Priority: 5}},
	}
}

func New(cfg Config) (*App, error) {
	// add defaults in here at some point

	if cfg.BrokerAddr == "" { // need broker address
		return nil, errors.ErrBrokerRequired
	}
	br := broker.New(cfg.BrokerAddr)
	// ba := backend.New(cfg.BackendURL)

	// initialise an empty registry and only every append to it
	r := worker.New()

	// initiliase all maps and arrays in configs so user only needs to append
	// and doesnt cause errors

	return &App{
		Broker: br,
		// Backend:  ba,
		Registry: r,
		Config:   cfg,
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
	a.running = true
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
				Broker:   a.Broker,
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
func (a *App) UpdateConfig(cfg Config) error {
	if a.running {
		return errors.ErrAppRunning
	}
	if cfg.BrokerAddr != "" { // recompute
		a.Config.BrokerAddr = cfg.BrokerAddr
		a.Broker = broker.New(cfg.BrokerAddr) // recompute
	}
	// if cfg.BackendURL != "" { //recompute
	// 	a.Config.BackendURL = cfg.BackendURL
	// 	a.Backend = backend.New(cfg.BackendURL) // recompute
	// }

	if cfg.Concurrency > 0 {
		a.Config.Concurrency = cfg.Concurrency
	} else {
		// log and fallback to prev version
	}

	if cfg.DefaultQueue != "" {
		a.Config.DefaultQueue = cfg.DefaultQueue
	}

	for name, priority := range cfg.Queues {
		a.Config.Queues[name] = priority
	}
	for task, queue := range cfg.TaskRoutes {
		a.Config.TaskRoutes[task] = queue
	}
	return nil // ideally, run a a.validate here but eh
}

func main() {
	// example

	// app := Onion.New(config) // config has their brokers, queues, task routing etc

	// register a task
	// app.Register("name", function, args) // registers a task

	// schedule a task (beat)
	// app.Schedule("name", "cron") // schedule repeated tasks

	// update config
	// app.UpdateConfig(config) // only updates the mentioned fields, doesnt replace entire obj

	// app.Start() // runs

	// when want to call a task
	// app.Enqueue(ctx, "send_email", args, goqueue.WithQueue("emails"))

}
