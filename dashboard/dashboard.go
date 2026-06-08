package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/crizah/Onion/backend"
	"github.com/crizah/Onion/worker"
)

type Dashboard struct {
	backend backend.Backend
	pool    *worker.Pool
	mux     *http.ServeMux
}

func New(b backend.Backend, p *worker.Pool) *Dashboard {
	d := &Dashboard{backend: b, pool: p, mux: http.NewServeMux()}
	d.mux.HandleFunc("/", d.serveUI)
	d.mux.HandleFunc("/api/stats", d.handleStats)
	d.mux.HandleFunc("/api/tasks", d.handleTasks)
	d.mux.HandleFunc("/api/workers", d.handleWorkers)
	return d
}

func (d *Dashboard) Start(addr string) error {
	fmt.Printf("dashboard starting on %s\n", addr)
	err := http.ListenAndServe(addr, d.mux)
	fmt.Printf("dashboard exited: %v\n", err)
	return err
}

func (d *Dashboard) serveUI(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "dashboard/index.html")
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

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
