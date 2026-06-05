package registry

import (
	"Onion/task"
	"sync"
)

type RegistryEntry struct {
	Function interface{} // dk abut the type for this right now
	Config   task.Config // options/config
}

type Registry struct {
	mu      sync.RWMutex
	entries map[string]RegistryEntry
}

func New() *Registry {
	return &Registry{
		entries: make(map[string]RegistryEntry),
	}
}

func (r *Registry) Set(name string, entry RegistryEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[name] = entry
}
