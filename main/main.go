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
	// registers tasks into the registery that the worker reads from
	// we dont need to retrurn the registry, prolly shouldnt

	// create a task and enter it into registry
	t := task.New(name, config...)

	// add task to a.Registry
	a.Registry.Set(name, registry.RegistryEntry{
		Task:     t,
		Function: fn,
	})

	return nil

}

func main() {
	// example

	// app := Onion.New(config) // config has their broker and other configs
	// define a task
	// app.Register("name", function, args) // registers a task

}
