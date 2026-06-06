package main

import (
	"Onion/broker"
	"Onion/errors"
	"Onion/worker"
)

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

func main() {
	// example

	// app := Onion.New(config) // config has their brokers, queues, task routing etc

	// register a task
	// app.Register("name", function, taskConfig) // registers a task
	// have an option to update the taskConfig  as well

	// schedule a task (beat)
	// can only schedule if registerred first
	// app.Schedule("scheduleName", "taskName", "*****", args map[string]any) // schedule repeated tasks

	// update config
	// app.UpdateConfig(config) // only updates the mentioned fields, doesnt replace entire obj

	// app.Start() // runs

	// when want to call a task
	// app.Enqueue(ctx, "taskName", args map[string]any)

	// how to define a Taskfunction: // TODO: make this way less restrictive
	// func sendEmail(ctx context.Context, args map[string]any) error {
	// 	to := args["to"].(string)
	// 	subject := args["subject"].(string)
	// 	// do work
	// 	return nil
	// }

}
