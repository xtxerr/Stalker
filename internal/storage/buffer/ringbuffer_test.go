package buffer

import (
	"sync"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestRingBuffer_Basic(t *testing.T) {
	rb := New(10)

	if rb.Cap() != 10 {
		t.Errorf("expected capacity=10, got %d", rb.Cap())
	}

	if !rb.IsEmpty() {
		t.Error("new buffer should be empty")
	}

	if rb.IsFull() {
		t.Error("new buffer should not be full")
	}
}

func TestRingBuffer_PushPop(t *testing.T) {
	rb := New(5)

	now := time.Now().UnixMilli()

	// Push samples
	for i := 0; i < 5; i++ {
		ok := rb.Push(types.Sample{
			Namespace:   "prod",
			Target:      "router",
			Poller:      "cpu",
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i),
			Valid:       true,
		})
		if !ok {
			t.Errorf("push %d should succeed", i)
		}
	}

	if rb.Len() != 5 {
		t.Errorf("expected len=5, got %d", rb.Len())
	}

	if !rb.IsFull() {
		t.Error("buffer should be full")
	}

	// Push to full buffer should fail
	ok := rb.Push(types.Sample{Value: 999})
	if ok {
		t.Error("push to full buffer should fail")
	}

	// Pop samples - should be in FIFO order
	for i := 0; i < 5; i++ {
		sample, ok := rb.Pop()
		if !ok {
			t.Errorf("pop %d should succeed", i)
		}
		if sample.Value != float64(i) {
			t.Errorf("expected value=%d, got %f", i, sample.Value)
		}
	}

	if !rb.IsEmpty() {
		t.Error("buffer should be empty after popping all")
	}

	// Pop from empty buffer
	_, ok = rb.Pop()
	if ok {
		t.Error("pop from empty buffer should fail")
	}
}

func TestRingBuffer_PushOverwrite(t *testing.T) {
	rb := New(3)

	now := time.Now().UnixMilli()

	// Fill buffer
	for i := 0; i < 3; i++ {
		rb.PushOverwrite(types.Sample{
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i),
			Valid:       true,
		})
	}

	// Overwrite oldest
	rb.PushOverwrite(types.Sample{
		TimestampMs: now + 3000,
		Value:       3.0,
		Valid:       true,
	})

	// Should still have 3 elements
	if rb.Len() != 3 {
		t.Errorf("expected len=3, got %d", rb.Len())
	}

	// Oldest should now be value 1 (0 was overwritten)
	sample, _ := rb.Pop()
	if sample.Value != 1.0 {
		t.Errorf("expected oldest value=1, got %f", sample.Value)
	}
}

func TestRingBuffer_PopN(t *testing.T) {
	rb := New(10)

	now := time.Now().UnixMilli()

	// Push 5 samples
	for i := 0; i < 5; i++ {
		rb.Push(types.Sample{
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i),
			Valid:       true,
		})
	}

	// Pop 3
	samples := rb.PopN(3)
	if len(samples) != 3 {
		t.Fatalf("expected 3 samples, got %d", len(samples))
	}

	for i, s := range samples {
		if s.Value != float64(i) {
			t.Errorf("sample %d: expected value=%d, got %f", i, i, s.Value)
		}
	}

	// Should have 2 left
	if rb.Len() != 2 {
		t.Errorf("expected len=2, got %d", rb.Len())
	}

	// Pop more than available
	samples = rb.PopN(10)
	if len(samples) != 2 {
		t.Errorf("expected 2 samples, got %d", len(samples))
	}
}

func TestRingBuffer_Peek(t *testing.T) {
	rb := New(5)

	// Peek empty buffer
	_, ok := rb.Peek()
	if ok {
		t.Error("peek on empty buffer should fail")
	}

	now := time.Now().UnixMilli()

	rb.Push(types.Sample{TimestampMs: now, Value: 1.0, Valid: true})
	rb.Push(types.Sample{TimestampMs: now + 1000, Value: 2.0, Valid: true})
	rb.Push(types.Sample{TimestampMs: now + 2000, Value: 3.0, Valid: true})

	// Peek oldest
	oldest, ok := rb.Peek()
	if !ok {
		t.Error("peek should succeed")
	}
	if oldest.Value != 1.0 {
		t.Errorf("expected oldest value=1, got %f", oldest.Value)
	}

	// Peek newest
	newest, ok := rb.PeekNewest()
	if !ok {
		t.Error("peek newest should succeed")
	}
	if newest.Value != 3.0 {
		t.Errorf("expected newest value=3, got %f", newest.Value)
	}

	// Peek should not remove
	if rb.Len() != 3 {
		t.Errorf("peek should not remove, expected len=3, got %d", rb.Len())
	}
}

func TestRingBuffer_Query(t *testing.T) {
	rb := New(100)

	now := time.Now().UnixMilli()

	// Add samples for different pollers
	for i := 0; i < 10; i++ {
		rb.Push(types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i),
			Valid:       true,
		})
		rb.Push(types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "memory",
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i * 10),
			Valid:       true,
		})
	}

	// Query by poller
	results := rb.QueryPoller("prod", "router-01", "cpu", 0)
	if len(results) != 10 {
		t.Errorf("expected 10 cpu samples, got %d", len(results))
	}

	// Query with limit
	results = rb.QueryPoller("prod", "router-01", "cpu", 5)
	if len(results) != 5 {
		t.Errorf("expected 5 cpu samples with limit, got %d", len(results))
	}

	// Query by time range
	results = rb.QueryRange(now+3000, now+6000)
	if len(results) != 8 { // 4 cpu + 4 memory samples in range
		t.Errorf("expected 8 samples in time range, got %d", len(results))
	}

	// Query with filter
	results = rb.Query(SampleFilter{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "memory",
		Since:     now + 5000,
	}, 0)
	if len(results) != 5 {
		t.Errorf("expected 5 memory samples since 5000, got %d", len(results))
	}
}

func TestRingBuffer_Evict(t *testing.T) {
	rb := New(10)

	now := time.Now().UnixMilli()

	// Add samples
	for i := 0; i < 10; i++ {
		rb.Push(types.Sample{
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i),
			Valid:       true,
		})
	}

	// Evict samples older than now+5000
	evicted := rb.EvictOlderThan(now + 5000)
	if evicted != 5 {
		t.Errorf("expected 5 evicted, got %d", evicted)
	}

	if rb.Len() != 5 {
		t.Errorf("expected 5 remaining, got %d", rb.Len())
	}

	// Oldest should now be value 5
	oldest, _ := rb.Peek()
	if oldest.Value != 5.0 {
		t.Errorf("expected oldest value=5, got %f", oldest.Value)
	}
}

func TestRingBuffer_EvictToCapacity(t *testing.T) {
	rb := New(10)

	now := time.Now().UnixMilli()

	// Fill buffer
	for i := 0; i < 10; i++ {
		rb.Push(types.Sample{
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i),
			Valid:       true,
		})
	}

	// Evict to 50% capacity
	evicted := rb.EvictToCapacity(0.5)
	if evicted != 5 {
		t.Errorf("expected 5 evicted, got %d", evicted)
	}

	if rb.Len() != 5 {
		t.Errorf("expected 5 remaining, got %d", rb.Len())
	}
}

func TestRingBuffer_TimeRange(t *testing.T) {
	rb := New(10)

	// Empty buffer
	oldest, newest := rb.TimeRange()
	if oldest != 0 || newest != 0 {
		t.Error("empty buffer should return 0,0")
	}

	now := time.Now().UnixMilli()

	rb.Push(types.Sample{TimestampMs: now, Value: 1, Valid: true})
	rb.Push(types.Sample{TimestampMs: now + 5000, Value: 2, Valid: true})
	rb.Push(types.Sample{TimestampMs: now + 10000, Value: 3, Valid: true})

	oldest, newest = rb.TimeRange()
	if oldest != now {
		t.Errorf("expected oldest=%d, got %d", now, oldest)
	}
	if newest != now+10000 {
		t.Errorf("expected newest=%d, got %d", now+10000, newest)
	}

	duration := rb.Duration()
	if duration != 10*time.Second {
		t.Errorf("expected duration=10s, got %v", duration)
	}
}

func TestRingBuffer_Stats(t *testing.T) {
	rb := New(10)

	now := time.Now().UnixMilli()

	// Push some samples
	for i := 0; i < 5; i++ {
		rb.Push(types.Sample{TimestampMs: now + int64(i), Value: float64(i), Valid: true})
	}

	// Pop some
	rb.Pop()
	rb.Pop()

	stats := rb.Stats()

	if stats.Capacity != 10 {
		t.Errorf("expected capacity=10, got %d", stats.Capacity)
	}
	if stats.Count != 3 {
		t.Errorf("expected count=3, got %d", stats.Count)
	}
	if stats.PushCount != 5 {
		t.Errorf("expected push_count=5, got %d", stats.PushCount)
	}
	if stats.PopCount != 2 {
		t.Errorf("expected pop_count=2, got %d", stats.PopCount)
	}
}

func TestRingBuffer_Clear(t *testing.T) {
	rb := New(10)

	now := time.Now().UnixMilli()

	for i := 0; i < 5; i++ {
		rb.Push(types.Sample{TimestampMs: now + int64(i), Value: float64(i), Valid: true})
	}

	rb.Clear()

	if !rb.IsEmpty() {
		t.Error("buffer should be empty after clear")
	}

	if rb.Len() != 0 {
		t.Errorf("expected len=0, got %d", rb.Len())
	}
}

func TestRingBuffer_Concurrent(t *testing.T) {
	rb := New(1000)

	var wg sync.WaitGroup
	numWriters := 10
	numReaders := 5
	samplesPerWriter := 100

	// Writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			now := time.Now().UnixMilli()
			for i := 0; i < samplesPerWriter; i++ {
				rb.PushOverwrite(types.Sample{
					Namespace:   "prod",
					Target:      "router",
					Poller:      "cpu",
					TimestampMs: now + int64(i),
					Value:       float64(writerID*1000 + i),
					Valid:       true,
				})
			}
		}(w)
	}

	// Readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				rb.Query(SampleFilter{Namespace: "prod"}, 10)
				rb.Len()
				rb.UsageRatio()
			}
		}()
	}

	wg.Wait()

	// Buffer should have some samples
	if rb.Len() == 0 {
		t.Error("buffer should not be empty after concurrent operations")
	}
}

func TestSampleFilter_Matches(t *testing.T) {
	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: 1000,
		Value:       50,
		Valid:       true,
	}

	tests := []struct {
		name     string
		filter   SampleFilter
		expected bool
	}{
		{"empty filter matches all", SampleFilter{}, true},
		{"matching namespace", SampleFilter{Namespace: "prod"}, true},
		{"non-matching namespace", SampleFilter{Namespace: "dev"}, false},
		{"matching target", SampleFilter{Target: "router-01"}, true},
		{"non-matching target", SampleFilter{Target: "router-02"}, false},
		{"matching poller", SampleFilter{Poller: "cpu"}, true},
		{"non-matching poller", SampleFilter{Poller: "memory"}, false},
		{"within time range", SampleFilter{Since: 500, Until: 1500}, true},
		{"before time range", SampleFilter{Since: 1500}, false},
		{"after time range", SampleFilter{Until: 500}, false},
		{"complex match", SampleFilter{Namespace: "prod", Target: "router-01", Poller: "cpu"}, true},
		{"complex non-match", SampleFilter{Namespace: "prod", Target: "router-01", Poller: "memory"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if result := tt.filter.Matches(&sample); result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func BenchmarkRingBuffer_Push(b *testing.B) {
	rb := New(100000)
	now := time.Now().UnixMilli()

	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       50,
		Valid:       true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sample.TimestampMs = now + int64(i)
		rb.PushOverwrite(sample)
	}
}

func BenchmarkRingBuffer_Query(b *testing.B) {
	rb := New(10000)
	now := time.Now().UnixMilli()

	// Fill buffer
	for i := 0; i < 10000; i++ {
		rb.Push(types.Sample{
			Namespace:   "prod",
			Target:      "router",
			Poller:      "cpu",
			TimestampMs: now + int64(i),
			Value:       float64(i),
			Valid:       true,
		})
	}

	filter := SampleFilter{Namespace: "prod", Target: "router", Poller: "cpu"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.Query(filter, 100)
	}
}
