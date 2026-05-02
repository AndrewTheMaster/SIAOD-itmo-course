package concmap

import (
	"fmt"
	"sync"
	"testing"
)

func TestPutGetMergeSize(t *testing.T) {
	m := New[string, int](6)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k_%d", i)
		m.Put(key, i)
		v, ok := m.Get(key)
		if !ok || v != i {
			t.Fatalf("Get после Put: got %v %v wanted %v", v, ok, i)
		}
	}
	if m.Size() != 20 {
		t.Fatalf("Size want 20 got %d", m.Size())
	}

	v := m.Merge("k_0", 7, func(a, b int) int { return a + b })
	if v != 7 {
		t.Fatalf("merge existing want 7 got %v", v)
	}
	v2 := m.Merge("new", 3, func(a, b int) int { return a + b })
	if v2 != 3 {
		t.Fatalf("merge absent want 3 got %v", v2)
	}
	if m.Size() != 21 {
		t.Fatalf("Size after merge inserts want 21 got %d", m.Size())
	}

	cnt := 0
	m.Range(func(k string, v int) bool {
		cnt++
		return true
	})
	if cnt != 21 {
		t.Fatalf("Range count got %d", cnt)
	}

	m.Clear()
	if m.Size() != 0 {
		t.Fatalf("Clear size wanted 0 got %d", m.Size())
	}
	_, ok := m.Get("k_10")
	if ok {
		t.Fatalf("expected miss after clear")
	}
}

func TestPutOverwriteNoSizeIncrease(t *testing.T) {
	m := New[string, int](4)
	m.Put("a", 1)
	m.Put("a", 2)
	if m.Size() != 1 {
		t.Fatalf("size want 1 got %d", m.Size())
	}
}

func TestHappensBeforePutGet(t *testing.T) {
	m := New[string, int](4)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.Put("x", 42)
	}()
	wg.Wait()
	v, ok := m.Get("x")
	if !ok || v != 42 {
		t.Fatalf("happens-before read got %v %v", v, ok)
	}
}
