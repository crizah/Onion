package app

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/crizah/Onion/backend"
	"github.com/crizah/Onion/beat"
	"github.com/crizah/Onion/broker"
	"github.com/crizah/Onion/dashboard"
	"github.com/crizah/Onion/errors"
	"github.com/crizah/Onion/task"
	"github.com/crizah/Onion/worker"
)

type App struct {
	Broker    broker.Broker
	Backend   backend.Backend
	Registry  *worker.Registry
	Pool      *worker.Pool
	running   bool
	Schedules []beat.ScheduleEntry
	Config    Config
}

type Config struct {
	BrokerAddr    BrokerAddr
	BackendURL    BackendURL
	Concurrency   int
	Queues        []broker.Queue
	TaskRoutes    map[string]string // user can update as {"taskName", "queuename"}
	DefaultQueue  string            // default queue for user to define
	DashboardAddr string            // e.g. ":8080", empty disables dashboard
	Location      *time.Location    // timezone cron schedules are interpreted in; nil defaults to time.Local
}
type BrokerAddr struct {
	Broker broker.BrokerType
	Addr   string
}
type BackendURL struct {
	DB               backend.DatabaseType
	ConnectionString string
}

func backendHelper(back BackendURL) (backend.Backend, error) {
	var ba backend.Backend
	var err error

	switch back.DB {
	case backend.DBTypeSQLite:
		ba, err = backend.NewSqlite(back.ConnectionString)
		if err != nil {
			return nil, err
		}

	case backend.DBTypePostgres:
		ba, err = backend.NewPostgres(back.ConnectionString)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unsupported database type: %q", back.DB)

	}

	return ba, nil

}

func brokerHelper(br BrokerAddr) (broker.Broker, error) {
	var b broker.Broker
	var err error
	switch br.Broker {
	case broker.BrokerRedis:
		b = broker.NewRedisBroker(br.Addr)
	case broker.BrokerPostgres:
		b, err = broker.NewPostgresBroker(br.Addr)
	default:
		return nil, fmt.Errorf("unsupported broker type: %q", br.Broker)
	}
	return b, err
}

func New(cfg Config) (*App, error) {
	// add defaults in here at some point

	if cfg.DefaultQueue != "" { // if user gave us a deafult queue
		if cfg.Queues == nil { // and didnt register ANY quues
			cfg.Queues = []broker.Queue{{Name: cfg.DefaultQueue, Priority: 5}} // we regsiter one ourselves
		} else { // if they did register queue, try to find default
			found := false
			for _, q := range cfg.Queues {
				if q.Name == cfg.DefaultQueue {
					found = true
					break
				}
			}
			if !found { // if not found, make one ourselves
				cfg.Queues = append(cfg.Queues, broker.Queue{Name: cfg.DefaultQueue, Priority: 5})
			}
		}
	}

	if cfg.BrokerAddr.Addr == "" { // need broker address
		return nil, errors.ErrBrokerRequired
	}
	// find out which broker
	br, err := brokerHelper(cfg.BrokerAddr)
	if err != nil {
		return nil, err
	}
	// only need this ping for redis i think
	if err := br.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("broker: ping: %w", err)
	}

	// find out which db
	ba, err := backendHelper(cfg.BackendURL)
	if err != nil {
		return nil, err
	}

	// initialise an empty registry and only every append to it
	r := worker.New()

	// initiliase all maps and arrays in configs so user only needs to append
	// and doesnt cause errors

	return &App{
		Broker:   br,
		Backend:  ba,
		Registry: r,
		Pool:     worker.NewPool(),
		Config:   cfg,
	}, nil
}

func defaultConfig() Config {
	return Config{
		Concurrency:  5,
		DefaultQueue: "default",
		TaskRoutes:   make(map[string]string),
		Queues:       []broker.Queue{{Name: "defult", Priority: 5}},
	}
}

func (a *App) Register(name string, fn task.TaskFunction, config ...task.TaskConfig) error {
	// only register the task, dont create one just yet

	cfg := task.TaskConfig{MaxRetries: 3, TimeLimit: 3600} // defaults here
	if len(config) > 0 {
		cfg = config[0]
	}
	a.Registry.Set(name, worker.RegistryEntry{
		TaskFunction: fn,
		TaskConfig:   cfg,
	})

	return nil
}

func (a *App) Schedule(scheduleName string, taskName string, cronExpr string, args map[string]any) error {
	// can only schedule if registered first
	if _, err := a.Registry.Lookup(taskName); err != nil {
		return err
	}

	a.Schedules = append(a.Schedules, beat.ScheduleEntry{
		ScheduleName: scheduleName,
		TaskName:     taskName,
		Expr:         cronExpr,
		Args:         args,
	})
	return nil
}

func (a *App) Start() error {
	a.running = true
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	concurrency := a.Config.Concurrency // TODO: default to 5 if not preset
	var wg sync.WaitGroup

	// 1. start worker pool
	for i := 0; i < concurrency; i++ {
		a.Pool.Register(i)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// each worker gets its own copy — Worker.Run sorts w.Queues in
			// place, and sharing a.Config.Queues across goroutines races
			queues := append([]broker.Queue(nil), a.Config.Queues...)
			w := &worker.Worker{
				ID:       id,
				Queues:   queues,
				Broker:   a.Broker,
				Registry: a.Registry,
				Backend:  a.Backend,
				Pool:     a.Pool,
			}
			w.Run(ctx)
		}(i)
	}

	// 2. start dashboard
	if a.Config.DashboardAddr != "" {
		d := dashboard.New(a.Backend, a.Pool, a.Broker, a.Config.Queues)
		go func() {
			if err := d.Start(a.Config.DashboardAddr); err != nil {
				fmt.Printf("dashboard error: %v\n", err)
			}
		}()
	}

	// 3. start beat if schedules exist
	if len(a.Schedules) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := beat.New(a.Schedules, a.Broker, a.Config.Queues, a.Config.TaskRoutes, a.Config.DefaultQueue, a.Backend, a.Config.Location)
			if err := b.Start(ctx); err != nil {
				// raise error here
				fmt.Printf("beat error: %v\n", err)
			}
		}()
	}

	// 4. block until signal
	wg.Wait()
	return nil
}

func (a *App) UpdateConfig(cfg Config) error {
	if a.running {
		return errors.ErrAppRunning
	}

	// this wont work because sqs doesnt have a addr field
	if cfg.BrokerAddr.Addr != "" { // recompute
		a.Config.BrokerAddr = cfg.BrokerAddr
		br, err := brokerHelper(cfg.BrokerAddr)
		if err != nil {
			return err
		}
		a.Broker = br // recompute
	}
	if cfg.BackendURL.ConnectionString != "" { //recompute
		a.Config.BackendURL = cfg.BackendURL
		b, err := backendHelper(cfg.BackendURL)
		if err != nil {
			return err
		}

		a.Backend = b
	}

	if cfg.Concurrency > 0 {
		a.Config.Concurrency = cfg.Concurrency
	} else {
		// log and fallback to prev version
	}

	if cfg.DefaultQueue != "" {
		a.Config.DefaultQueue = cfg.DefaultQueue
	}

	if cfg.Location != nil {
		a.Config.Location = cfg.Location
	}

	// FIX 1: Safely append new queues instead of assigning by index
	if len(cfg.Queues) > 0 {
		a.Config.Queues = append(a.Config.Queues, cfg.Queues...)
	}

	// FIX 2: Initialize map before assignment to prevent nil map panic
	if a.Config.TaskRoutes == nil {
		a.Config.TaskRoutes = make(map[string]string)
	}

	for task, queue := range cfg.TaskRoutes {
		a.Config.TaskRoutes[task] = queue
	}

	return nil // ideally, run a a.validate here but eh
}

func (a *App) resolveQueue(taskName string) (broker.Queue, error) {
	name := a.Config.DefaultQueue

	if qn, ok := a.Config.TaskRoutes[taskName]; ok {
		// TODO: log using default queue when !ok
		name = qn
	}

	for _, q := range a.Config.Queues {
		if q.Name == name {
			return q, nil
		}
	}
	// no queue matched the task AND no default configured
	return broker.Queue{}, errors.ErrQueueNotFound
}

func (a *App) Enqueue(ctx context.Context, taskName string, args map[string]any) error {
	// create task, resolve queue from app.config.taskroutes, falback to app.config.defaultqueue
	t := task.New(taskName, args)
	q, err := a.resolveQueue(taskName)
	if err != nil {
		return err
	}

	var cfg task.TaskConfig
	if entry, err := a.Registry.Lookup(taskName); err == nil {
		cfg = entry.TaskConfig // task may not be registered on the producer side, that's ok
	}
	a.Backend.Save(ctx, &backend.TaskRecord{Task: t, Queue: q.Name, Config: cfg}) // write

	return a.Broker.Enqueue(ctx, q.Name, t)
}
