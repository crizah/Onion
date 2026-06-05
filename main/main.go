package main

import (
	"Onion/broker"
	"Onion/registry"
	"Onion/task"
)

type App struct {
	Broker   *broker.RedisBroker
	Backend  interface{} // dk wat to do with this for rn
	Registry *registry.Registry
}

type Config struct {
	BrokerAddr string
	BackendURL string // dk wat to do with this for rn

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
	r := registry.New()

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
	a.Registry.Set(name, registry.RegistryEntry{
		Function: fn,
		Config:   cfg,
	})

	return nil
}

func main() {
	// example

	// app := Onion.New(config) // config has their broker and other configs
	// register a task
	// app.Register("name", function, args) // registers a task

}
