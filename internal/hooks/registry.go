package hooks

import (
	"sync"

	v1 "deeproute.ai/snapscheduler/api/v1"
)

// 简单注册器，按 name 注册 Hook
type Registry struct {
	mu    sync.RWMutex
	hooks map[v1.ServiceType]Hook
}

func NewRegistry() *Registry {
	return &Registry{hooks: map[v1.ServiceType]Hook{}}
}

func (r *Registry) Register(h Hook) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks[h.Name()] = h
}

func (r *Registry) Unregister(name v1.ServiceType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.hooks, name)
}

func (r *Registry) GetHook(hName v1.ServiceType) Hook {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.hooks[hName]; ok {
		return h
	}
	return nil
}
