package main

import (
	"Onion/app"
	"context"
	"fmt"
	"time"
)

// simulate a long running task
func sendEmail(ctx context.Context, args map[string]any) error {
	to := args["to"].(string)
	fmt.Printf("[sendEmail] starting — to: %s\n", to)

	time.Sleep(3 * time.Second) // simulate work
	fmt.Printf("[sendEmail] done — email sent to %s\n", to)

	return nil
}

func generateReport(ctx context.Context, args map[string]any) error {

	reportID := args["report_id"].(string)
	fmt.Printf("[generateReport] starting — id: %s\n", reportID)
	time.Sleep(5 * time.Second) // simulate heavy work
	fmt.Printf("[generateReport] done — report %s generated\n", reportID)
	return nil
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

	App, err := app.New(app.Config{
		BrokerAddr:   "localhost:6379",
		Concurrency:  3,
		DefaultQueue: "default",
	})
	if err != nil {
		panic(err)
	}

	// register tasks
	App.Register("send_email", sendEmail)
	App.Register("generate_report", generateReport)

	// enqueue some tasks — this returns immediately
	ctx := context.Background()
	fmt.Println("[main] enqueueing tasks...")

	App.Enqueue(ctx, "send_email", map[string]any{"to": "alice@example.com"})
	App.Enqueue(ctx, "send_email", map[string]any{"to": "bob@example.com"})
	App.Enqueue(ctx, "send_email", map[string]any{"to": "carol@example.com"})
	App.Enqueue(ctx, "generate_report", map[string]any{"report_id": "q3-2024"})

	fmt.Println("[main] all tasks enqueued, this prints immediately")
	fmt.Println("[main] starting workers, Ctrl+C to stop...")

	// start blocks — workers pick up and execute tasks
	App.Start()

}
