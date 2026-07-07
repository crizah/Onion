package dashboard

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/crizah/Onion/backend"
	"github.com/crizah/Onion/broker"
	"github.com/crizah/Onion/worker"
)

//go:embed index.html
var indexHTML []byte

type Dashboard struct {
	backend backend.Backend
	pool    *worker.Pool
	broker  broker.Broker
	queues  []broker.Queue
	mux     *http.ServeMux
}

func New(b backend.Backend, p *worker.Pool, br broker.Broker, queues []broker.Queue) *Dashboard {
	d := &Dashboard{backend: b, pool: p, broker: br, queues: queues, mux: http.NewServeMux()}
	d.mux.HandleFunc("/", d.serveUI)
	d.mux.HandleFunc("/api/stats", d.handleStats)
	d.mux.HandleFunc("/api/tasks", d.handleTasks)
	d.mux.HandleFunc("/api/workers", d.handleWorkers)
	d.mux.HandleFunc("/api/queues", d.handleQueues)
	return d
}

func (d *Dashboard) Start(addr string) error {
	fmt.Printf("dashboard starting on %s\n", addr)
	err := http.ListenAndServe(addr, d.mux)
	fmt.Printf("dashboard exited: %v\n", err)
	return err
}

func (d *Dashboard) serveUI(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/html")
    w.Write(indexHTML)
}

func (d *Dashboard) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := d.backend.Stats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, stats)
}

func (d *Dashboard) handleTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	result, err := d.backend.List(r.Context(), backend.TaskFilter{
		Search: q.Get("search"),
		Status: q.Get("status"),
		Queue:  q.Get("queue"),
		Page:   page,
		Limit:  limit,
	})
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	writeJSON(w, result)
}

func (d *Dashboard) handleWorkers(w http.ResponseWriter, r *http.Request) {
	workers := d.pool.Snapshot()
	writeJSON(w, map[string]any{
		"count":   len(workers),
		"workers": workers,
	})
}

type queueInfo struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
	Depth    int64  `json:"depth"`
}

func (d *Dashboard) handleQueues(w http.ResponseWriter, r *http.Request) {
	out := make([]queueInfo, 0, len(d.queues))
	for _, q := range d.queues {
		depth, err := d.broker.Len(r.Context(), q.Name)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		out = append(out, queueInfo{Name: q.Name, Priority: q.Priority, Depth: depth})
	}
	writeJSON(w, out)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
