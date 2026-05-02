// Package concmap — striping hash-table с закрытой адресацией (цепочки в бакетах)
// и отдельной RWMutex на бакет. Чтение Get/Range блокируется только записью в тот же бакет,
// но не блокируется параллельными чтениями и записями в другие бакеты (принцип сегментов CHM JDK).
//
// Наблюдаемый порядок (happens-before) между завершёнными операциями:
// отпускание мьютекса записи перед захватом мьютекса читателя/писателя того же или другого бакета,
// см. память-порядок sync пакета (аналог документации ConcurrentHashMap).
package concmap

import (
	"encoding/binary"
	"hash/maphash"
	"sync"
	"sync/atomic"
)

// Option параметризация конструктора.
type Option[K comparable, V any] func(*Map[K, V])

// WithHasher задаёт пользовательскую функцию хеширования ключа в uint64 (младшие биты мапятся в бакет).
func WithHasher[K comparable, V any](fn func(K) uint64) Option[K, V] {
	return func(m *Map[K, V]) {
		if fn == nil {
			panic("concmap: WithHasher(nil)")
		}
		m.hash = fn
	}
}

type bucket[K comparable, V any] struct {
	mu   sync.RWMutex
	head *node[K, V]
}

type node[K comparable, V any] struct {
	key  K
	val  V
	next *node[K, V]
}

// Map[K,V] потокобезопасная хеш-таблица.
type Map[K comparable, V any] struct {
	buckets []bucket[K, V]
	mask    uint64
	hash    func(K) uint64
	size    atomic.Uint64
}

// New создаёт таблицу с 2^bucketBits бакетами (bucketBits в [4..26] типичный диапазон).
func New[K comparable, V any](bucketBits uint8, opts ...Option[K, V]) *Map[K, V] {
	if bucketBits < 1 || bucketBits > 26 {
		panic("concmap: bucketBits должно быть в [1..26]")
	}
	n := uint64(1) << bucketBits
	m := &Map[K, V]{
		buckets: make([]bucket[K, V], n),
		mask:    n - 1,
	}
	for _, o := range opts {
		o(m)
	}
	if m.hash == nil {
		m.hash = makeDefaultHashFunc[K]()
	}
	return m
}

// makeDefaultHashFunc выбирает специализированное хеширование без reflect.ValueOf(K) для hot-path string/int.
func makeDefaultHashFunc[K comparable]() func(K) uint64 {
	var probe K
	switch any(probe).(type) {
	case string:
		seed := maphash.MakeSeed()
		return func(k K) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			_, _ = h.WriteString(any(k).(string))
			return h.Sum64()
		}
	case int:
		seed := maphash.MakeSeed()
		return func(k K) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(any(k).(int)))
			_, _ = h.Write(buf[:])
			return h.Sum64()
		}
	case int64:
		seed := maphash.MakeSeed()
		return func(k K) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(any(k).(int64)))
			_, _ = h.Write(buf[:])
			return h.Sum64()
		}
	default:
		seed := maphash.MakeSeed()
		return newDefaultHasher[K](seed)
	}
}

func (m *Map[K, V]) bucketOf(key K) *bucket[K, V] {
	i := int(m.hash(key) & m.mask)
	return &m.buckets[i]
}

// Put вставляет или перезаписывает ключ. Если ключ новый — size увеличивается.
func (m *Map[K, V]) Put(key K, val V) {
	b := m.bucketOf(key)
	b.mu.Lock()
	for cur := b.head; cur != nil; cur = cur.next {
		if cur.key == key {
			cur.val = val
			b.mu.Unlock()
			return
		}
	}
	b.head = &node[K, V]{key: key, val: val, next: b.head}
	m.size.Add(1)
	b.mu.Unlock()
}

// Get без блокировки чужих бакетов; разделяет RLock только с братскими операциями в том же бакете.
func (m *Map[K, V]) Get(key K) (V, bool) {
	b := m.bucketOf(key)
	b.mu.RLock()
	for cur := b.head; cur != nil; cur = cur.next {
		if cur.key == key {
			v := cur.val
			b.mu.RUnlock()
			return v, true
		}
	}
	var zero V
	b.mu.RUnlock()
	return zero, false
}

// Merge как в JDK: если ключа не было — сохраняет value без вызова merger и возвращает его;
// иначе newVal := merger(existing, val), сохраняет newVal и возвращает его.
func (m *Map[K, V]) Merge(key K, val V, merger func(existing, incoming V) V) V {
	if merger == nil {
		panic("concmap: Merge(..., merger: nil)")
	}
	b := m.bucketOf(key)
	b.mu.Lock()
	for cur := b.head; cur != nil; cur = cur.next {
		if cur.key == key {
			nv := merger(cur.val, val)
			cur.val = nv
			b.mu.Unlock()
			return nv
		}
	}
	b.head = &node[K, V]{key: key, val: val, next: b.head}
	m.size.Add(1)
	b.mu.Unlock()
	return val
}

// Clear удаляет все пары под глобальным порядком блокировки бакетов (слева направо) против взаимоблокировок.
func (m *Map[K, V]) Clear() {
	for i := range m.buckets {
		m.buckets[i].mu.Lock()
	}
	for i := range m.buckets {
		m.buckets[i].head = nil
	}
	m.size.Store(0)
	for i := range m.buckets {
		m.buckets[i].mu.Unlock()
	}
}

// Size число ключей (~ JDK size() точный при отсутствии гонок на Clear).
func (m *Map[K, V]) Size() uint64 {
	return m.size.Load()
}

// Range обходит ключи; итерация сопоставима с «слабо согласованным» видом JDK:
// не бросает при конкуррентной модификации, но может не видеть одновременно вставленные элементы других потоков.
// Для каждого бакета чтение идёт под RLock, поэтому структура цепочки в бакете стабильна.
func (m *Map[K, V]) Range(fn func(key K, val V) bool) {
	for i := range m.buckets {
		b := &m.buckets[i]
		b.mu.RLock()
		for cur := b.head; cur != nil; cur = cur.next {
			if !fn(cur.key, cur.val) {
				b.mu.RUnlock()
				return
			}
		}
		b.mu.RUnlock()
	}
}
