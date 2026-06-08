package worker

import (
	"sync"
	"time"
)

type WorkerStatus string

const (
	Idle WorkerStatus = "idle"
	Busy WorkerStatus = "busy"
)

type WorkerState struct {
	ID          int          `json:"id"`
	Status      WorkerStatus `json:"status"`
	CurrentTask string       `json:"current_task"` // task name, empty when idle
	Processed   int          `json:"processed"`
	StartedAt   time.Time    `json:"started_at"`
}

type Pool struct {
	mu      sync.RWMutex
	workers map[int]*WorkerState
}

func NewPool() *Pool {
	return &Pool{workers: make(map[int]*WorkerState)}
}

func (p *Pool) Register(id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.workers[id] = &WorkerState{ID: id, Status: Idle, StartedAt: time.Now()}
}

func (p *Pool) SetBusy(id int, taskName string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.workers[id]; ok {
		w.Status = Busy
		w.CurrentTask = taskName
	}
}

func (p *Pool) SetIdle(id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.workers[id]; ok {
		w.Status = Idle
		w.CurrentTask = ""
		w.Processed++
	}
}

func (p *Pool) Snapshot() []WorkerState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]WorkerState, 0, len(p.workers))
	for _, w := range p.workers {
		out = append(out, *w)
	}
	return out
}
