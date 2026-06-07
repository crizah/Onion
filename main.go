// // package main

// // import (
// // 	"Onion/app"
// // 	"context"
// // 	"fmt"
// // 	"time"
// // )

// // // simulate a long running task
// // func sendEmail(ctx context.Context, args map[string]any) error {
// // 	to := args["to"].(string)
// // 	fmt.Printf("[sendEmail] starting — to: %s\n", to)

// // 	time.Sleep(3 * time.Second) // simulate work
// // 	fmt.Printf("[sendEmail] done — email sent to %s\n", to)

// // 	return nil
// // }

// // func generateReport(ctx context.Context, args map[string]any) error {

// // 	reportID := args["report_id"].(string)
// // 	fmt.Printf("[generateReport] starting — id: %s\n", reportID)
// // 	time.Sleep(5 * time.Second) // simulate heavy work
// // 	fmt.Printf("[generateReport] done — report %s generated\n", reportID)
// // 	return nil
// // }

// // func main() {
// // 	// example

// // 	// app := Onion.New(config) // config has their brokers, queues, task routing etc

// // 	// register a task
// // 	// app.Register("name", function, taskConfig) // registers a task
// // 	// have an option to update the taskConfig  as well

// // 	// schedule a task (beat)
// // 	// can only schedule if registerred first
// // 	// app.Schedule("scheduleName", "taskName", "*****", args map[string]any) // schedule repeated tasks

// // 	// update config
// // 	// app.UpdateConfig(config) // only updates the mentioned fields, doesnt replace entire obj

// // 	// app.Start() // runs

// // 	// when want to call a task
// // 	// app.Enqueue(ctx, "taskName", args map[string]any)

// // 	// how to define a Taskfunction: // TODO: make this way less restrictive
// // 	// func sendEmail(ctx context.Context, args map[string]any) error {
// // 	// 	to := args["to"].(string)
// // 	// 	subject := args["subject"].(string)
// // 	// 	// do work
// // 	// 	return nil
// // 	// }

// // 	App, err := app.New(app.Config{
// // 		BrokerAddr:   "localhost:6379",
// // 		Concurrency:  3,
// // 		DefaultQueue: "default",
// // 	})
// // 	if err != nil {
// // 		panic(err)
// // 	}

// // 	// register tasks
// // 	App.Register("send_email", sendEmail)
// // 	App.Register("generate_report", generateReport)

// // 	// enqueue some tasks — this returns immediately
// // 	ctx := context.Background()
// // 	fmt.Println("[main] enqueueing tasks...")

// // 	App.Enqueue(ctx, "send_email", map[string]any{"to": "alice@example.com"})
// // 	App.Enqueue(ctx, "send_email", map[string]any{"to": "bob@example.com"})
// // 	App.Enqueue(ctx, "send_email", map[string]any{"to": "carol@example.com"})
// // 	App.Enqueue(ctx, "generate_report", map[string]any{"report_id": "q3-2024"})

// // 	fmt.Println("[main] all tasks enqueued, this prints immediately")
// // 	fmt.Println("[main] starting workers, Ctrl+C to stop...")

// // 	// start blocks — workers pick up and execute tasks
// // 	App.Start()

// // }

package main

import (
	"Onion/app"
	"Onion/broker"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq" // this registers the "postgres" driver
)

// ── task functions ────────────────────────────────────────────────────────────

func sendEmail(ctx context.Context, args map[string]any) (any, error) {
	to := args["to"].(string)
	fmt.Printf("[sendEmail] starting — to: %s\n", to)
	time.Sleep(3 * time.Second)
	fmt.Printf("[sendEmail] done — sent to %s\n", to)
	return map[string]any{"status": "sent", "to": to}, nil
}

func generateReport(ctx context.Context, args map[string]any) (any, error) {
	reportID := args["report_id"].(string)
	fmt.Printf("[generateReport] starting — id: %s\n", reportID)
	time.Sleep(5 * time.Second)
	fmt.Printf("[generateReport] done — report %s generated\n", reportID)
	return map[string]any{"report_id": reportID, "pages": 42}, nil
}

func cleanupOrphans(ctx context.Context, args map[string]any) (any, error) {
	fmt.Printf("[cleanupOrphans] starting — scanning for orphaned tasks\n")
	time.Sleep(1 * time.Second)
	fmt.Printf("[cleanupOrphans] done — cleaned up\n")
	return nil, nil
}

func bulkExport(ctx context.Context, args map[string]any) (any, error) {
	format := args["format"].(string)
	fmt.Printf("[bulkExport] starting — format: %s\n", format)
	time.Sleep(8 * time.Second) // heavy task
	fmt.Printf("[bulkExport] done — exported as %s\n", format)
	return nil, nil
}

// ── main ─────────────────────────────────────────────────────────────────────

func main() {
	godotenv.Load()
	// ── 1. init app ──────────────────────────────────────────────────────────
	backend_url := os.Getenv("BACKED_URl")
	broker_addr := os.Getenv("BROKER_URL")
	App, err := app.New(app.Config{
		BrokerAddr:   broker_addr,
		BackendURL:   backend_url,
		Concurrency:  3,
		DefaultQueue: "default",
		Queues: []broker.Queue{
			{Name: "critical", Priority: 10},
			{Name: "bulk", Priority: 1},
			// "default" auto added since DefaultQueue is set
		},
		TaskRoutes: map[string]string{
			"send_email":  "critical", // emails go to critical queue
			"bulk_export": "bulk",     // heavy exports go to bulk queue
			// generate_report and cleanupOrphans fall back to default
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("[main] app initialised")
	fmt.Printf("[main] queues: %+v\n", App.Config.Queues)
	fmt.Printf("[main] routes: %+v\n", App.Config.TaskRoutes)

	// ── 2. register tasks ────────────────────────────────────────────────────
	App.Register("send_email", sendEmail)
	App.Register("generate_report", generateReport)
	App.Register("cleanup_orphans", cleanupOrphans)
	App.Register("bulk_export", bulkExport)

	fmt.Println("[main] tasks registered")

	// ── 3. update config after init ──────────────────────────────────────────
	App.UpdateConfig(app.Config{
		Concurrency: 5, // bump concurrency
	})
	fmt.Printf("[main] concurrency updated to: %d\n", App.Config.Concurrency)

	// ── 4. schedule beat tasks ───────────────────────────────────────────────
	// cleanup runs every 10 seconds so we can actually see it fire
	err = App.Schedule("orphan_cleanup", "cleanup_orphans", "@every 10s", nil)
	if err != nil {
		panic(fmt.Sprintf("schedule error: %v", err))
	}
	fmt.Println("[main] beat schedule registered — cleanup every 10s")

	// ── 5. enqueue on-demand tasks ───────────────────────────────────────────
	ctx := context.Background()

	fmt.Println("[main] enqueueing tasks...")

	// goes to critical queue (via task route)
	App.Enqueue(ctx, "send_email", map[string]any{"to": "alice@example.com"})
	App.Enqueue(ctx, "send_email", map[string]any{"to": "bob@example.com"})
	App.Enqueue(ctx, "send_email", map[string]any{"to": "carol@example.com"})

	// goes to default queue (no route defined)
	App.Enqueue(ctx, "generate_report", map[string]any{"report_id": "q3-2024"})
	App.Enqueue(ctx, "generate_report", map[string]any{"report_id": "q4-2024"})

	// goes to bulk queue (via task route)
	App.Enqueue(ctx, "bulk_export", map[string]any{"format": "csv"})
	App.Enqueue(ctx, "bulk_export", map[string]any{"format": "pdf"})

	fmt.Println("[main] all tasks enqueued — this prints immediately")
	fmt.Println("[main] starting workers — Ctrl+C to stop")
	fmt.Println("[main] watch: critical queue drains first, bulk last, beat fires every 10s")

	// ── 6. start — blocks until Ctrl+C ──────────────────────────────────────
	App.Start()
}

// package main

// func main(){

// }
