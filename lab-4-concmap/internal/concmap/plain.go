package concmap

import "sync"

// Plain[K,V] — одно RWMutex вокруг встроенного map (baseline для бенчмарков/concurrency-тестов).
// Чтение Get/Range подключают RLock ко всей таблице: любая запись блокирует всех читателей.
type Plain[K comparable, V any] struct {
	mu sync.RWMutex
	m  map[K]V
}

func NewPlain[K comparable, V any]() *Plain[K, V] {
	return &Plain[K, V]{m: make(map[K]V)}
}

func (p *Plain[K, V]) Put(key K, val V) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m[key] = val
}

func (p *Plain[K, V]) Get(key K) (V, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.m[key]
	return v, ok
}

func (p *Plain[K, V]) Merge(key K, val V, merger func(existing, incoming V) V) V {
	if merger == nil {
		panic("plain: Merge nil merger")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.m[key]; ok {
		nv := merger(old, val)
		p.m[key] = nv
		return nv
	}
	p.m[key] = val
	return val
}

func (p *Plain[K, V]) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	clear(p.m)
}

func (p *Plain[K, V]) Size() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return uint64(len(p.m))
}

func (p *Plain[K, V]) Range(fn func(K, V) bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for k, v := range p.m {
		if !fn(k, v) {
			return
		}
	}
}
