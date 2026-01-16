package aggregate

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestStreamingAggregate_Basic(t *testing.T) {
	now := time.Now().UnixMilli()
	bucketStart := now
	bucketEnd := now + 5*60*1000 // 5 minutes

	agg := New("prod", "router-01", "cpu", bucketStart, bucketEnd, false)

	if !agg.IsEmpty() {
		t.Error("new aggregate should be empty")
	}

	// Add some values
	agg.Add(10.0, now)
	agg.Add(20.0, now+1000)
	agg.Add(30.0, now+2000)

	if agg.IsEmpty() {
		t.Error("aggregate should not be empty")
	}

	if agg.Count() != 3 {
		t.Errorf("expected count=3, got %d", agg.Count())
	}

	result := agg.Result()

	if result.Count != 3 {
		t.Errorf("expected count=3, got %d", result.Count)
	}

	if result.Sum != 60.0 {
		t.Errorf("expected sum=60, got %f", result.Sum)
	}

	if result.Min != 10.0 {
		t.Errorf("expected min=10, got %f", result.Min)
	}

	if result.Max != 30.0 {
		t.Errorf("expected max=30, got %f", result.Max)
	}

	expectedAvg := 20.0
	if math.Abs(result.Avg-expectedAvg) > 0.001 {
		t.Errorf("expected avg=%f, got %f", expectedAvg, result.Avg)
	}

	if result.HasPercentiles() {
		t.Error("should not have percentiles")
	}
}

func TestStreamingAggregate_WithPercentiles(t *testing.T) {
	now := time.Now().UnixMilli()
	bucketStart := now
	bucketEnd := now + 5*60*1000

	agg := New("prod", "router-01", "latency", bucketStart, bucketEnd, true)

	// Add 100 values: 1, 2, 3, ..., 100
	for i := 1; i <= 100; i++ {
		agg.Add(float64(i), now+int64(i)*100)
	}

	result := agg.Result()

	if !result.HasPercentiles() {
		t.Fatal("should have percentiles")
	}

	// P50 should be around 50
	if math.Abs(*result.P50-50.0) > 2.0 {
		t.Errorf("expected P50 near 50, got %f", *result.P50)
	}

	// P95 should be around 95
	if math.Abs(*result.P95-95.0) > 2.0 {
		t.Errorf("expected P95 near 95, got %f", *result.P95)
	}

	// P99 should be around 99
	if math.Abs(*result.P99-99.0) > 2.0 {
		t.Errorf("expected P99 near 99, got %f", *result.P99)
	}
}

func TestStreamingAggregate_Reset(t *testing.T) {
	now := time.Now().UnixMilli()
	bucketStart := now
	bucketEnd := now + 5*60*1000

	agg := New("prod", "router-01", "cpu", bucketStart, bucketEnd, true)

	agg.Add(10.0, now)
	agg.Add(20.0, now+1000)

	if agg.Count() != 2 {
		t.Errorf("expected count=2, got %d", agg.Count())
	}

	// Reset to new bucket
	newBucketStart := bucketEnd
	newBucketEnd := bucketEnd + 5*60*1000
	agg.Reset(newBucketStart, newBucketEnd)

	if !agg.IsEmpty() {
		t.Error("aggregate should be empty after reset")
	}

	if agg.BucketStart() != newBucketStart {
		t.Errorf("expected bucket start=%d, got %d", newBucketStart, agg.BucketStart())
	}
}

func TestStreamingAggregate_Merge(t *testing.T) {
	now := time.Now().UnixMilli()
	bucketStart := now
	bucketEnd := now + 5*60*1000

	agg1 := New("prod", "router-01", "cpu", bucketStart, bucketEnd, false)
	agg1.Add(10.0, now)
	agg1.Add(20.0, now+1000)

	agg2 := New("prod", "router-01", "cpu", bucketStart, bucketEnd, false)
	agg2.Add(30.0, now+2000)
	agg2.Add(40.0, now+3000)

	agg1.Merge(agg2)

	result := agg1.Result()

	if result.Count != 4 {
		t.Errorf("expected count=4, got %d", result.Count)
	}

	if result.Sum != 100.0 {
		t.Errorf("expected sum=100, got %f", result.Sum)
	}

	if result.Min != 10.0 {
		t.Errorf("expected min=10, got %f", result.Min)
	}

	if result.Max != 40.0 {
		t.Errorf("expected max=40, got %f", result.Max)
	}
}

func TestStreamingAggregate_AddSample(t *testing.T) {
	now := time.Now().UnixMilli()
	bucketStart := now
	bucketEnd := now + 5*60*1000

	agg := New("prod", "router-01", "cpu", bucketStart, bucketEnd, false)

	// Valid sample
	agg.AddSample(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       50.0,
		Valid:       true,
	})

	// Invalid sample (should be ignored)
	agg.AddSample(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now + 1000,
		Value:       100.0,
		Valid:       false,
		Error:       "timeout",
	})

	if agg.Count() != 1 {
		t.Errorf("expected count=1 (invalid sample ignored), got %d", agg.Count())
	}
}

func TestStreamingAggregate_Concurrent(t *testing.T) {
	now := time.Now().UnixMilli()
	bucketStart := now
	bucketEnd := now + 5*60*1000

	agg := New("prod", "router-01", "cpu", bucketStart, bucketEnd, true)

	var wg sync.WaitGroup
	numGoroutines := 10
	valuesPerGoroutine := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := 0; j < valuesPerGoroutine; j++ {
				agg.Add(float64(base*valuesPerGoroutine+j), now+int64(j))
			}
		}(i)
	}

	wg.Wait()

	expectedCount := int64(numGoroutines * valuesPerGoroutine)
	if agg.Count() != expectedCount {
		t.Errorf("expected count=%d, got %d", expectedCount, agg.Count())
	}
}

func TestManager_Basic(t *testing.T) {
	manager := NewManager(5*time.Minute, false)

	if manager.ActiveCount() != 0 {
		t.Errorf("expected 0 active aggregates, got %d", manager.ActiveCount())
	}

	now := time.Now().UnixMilli()

	// Process samples for the same poller
	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       50.0,
		Valid:       true,
	})

	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now + 1000,
		Value:       60.0,
		Valid:       true,
	})

	if manager.ActiveCount() != 1 {
		t.Errorf("expected 1 active aggregate, got %d", manager.ActiveCount())
	}

	stats := manager.Stats()
	if stats.SamplesProcessed != 2 {
		t.Errorf("expected 2 samples processed, got %d", stats.SamplesProcessed)
	}
}

func TestManager_BucketTransition(t *testing.T) {
	manager := NewManager(5*time.Minute, false)

	// First bucket: 00:00 - 00:05
	bucket1Start := int64(0)
	bucket1End := int64(5 * 60 * 1000)

	// Second bucket: 00:05 - 00:10
	bucket2Start := bucket1End
	bucket2End := bucket1End + 5*60*1000

	// Add sample to first bucket
	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: bucket1Start + 1000,
		Value:       50.0,
		Valid:       true,
	})

	// No completed yet
	if manager.CompletedCount() != 0 {
		t.Errorf("expected 0 completed, got %d", manager.CompletedCount())
	}

	// Add sample to second bucket - this should complete the first
	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: bucket2Start + 1000,
		Value:       60.0,
		Valid:       true,
	})

	// First bucket should be completed
	if manager.CompletedCount() != 1 {
		t.Errorf("expected 1 completed, got %d", manager.CompletedCount())
	}

	// Flush completed
	completed := manager.FlushCompleted()
	if len(completed) != 1 {
		t.Fatalf("expected 1 completed result, got %d", len(completed))
	}

	if completed[0].Count != 1 {
		t.Errorf("expected count=1, got %d", completed[0].Count)
	}

	if completed[0].Avg != 50.0 {
		t.Errorf("expected avg=50, got %f", completed[0].Avg)
	}

	// After flush, completed should be empty
	if manager.CompletedCount() != 0 {
		t.Errorf("expected 0 completed after flush, got %d", manager.CompletedCount())
	}
	_ = bucket2End // use variable
}

func TestManager_FlushAll(t *testing.T) {
	manager := NewManager(5*time.Minute, false)

	now := time.Now().UnixMilli()

	// Process samples for multiple pollers
	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       50.0,
		Valid:       true,
	})

	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "memory",
		TimestampMs: now,
		Value:       70.0,
		Valid:       true,
	})

	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-02",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       30.0,
		Valid:       true,
	})

	if manager.ActiveCount() != 3 {
		t.Errorf("expected 3 active aggregates, got %d", manager.ActiveCount())
	}

	// Flush all
	results := manager.FlushAll()

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	if manager.ActiveCount() != 0 {
		t.Errorf("expected 0 active after flush all, got %d", manager.ActiveCount())
	}
}

func TestManager_ProcessBatch(t *testing.T) {
	manager := NewManager(5*time.Minute, false)

	now := time.Now().UnixMilli()

	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 10, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + 1000, Value: 20, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "memory", TimestampMs: now, Value: 30, Valid: true},
		{Namespace: "prod", Target: "r2", Poller: "cpu", TimestampMs: now, Value: 40, Valid: true},
	}

	manager.ProcessBatch(samples)

	stats := manager.Stats()
	if stats.SamplesProcessed != 4 {
		t.Errorf("expected 4 samples processed, got %d", stats.SamplesProcessed)
	}

	if manager.ActiveCount() != 3 {
		t.Errorf("expected 3 active aggregates, got %d", manager.ActiveCount())
	}
}

func TestManager_WithPercentiles(t *testing.T) {
	manager := NewManager(5*time.Minute, true)

	now := time.Now().UnixMilli()

	// Add 100 samples
	for i := 1; i <= 100; i++ {
		manager.Process(types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "latency",
			TimestampMs: now + int64(i)*100,
			Value:       float64(i),
			Valid:       true,
		})
	}

	results := manager.FlushAll()

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]
	if !result.HasPercentiles() {
		t.Fatal("expected percentiles")
	}

	// P50 should be around 50
	if math.Abs(*result.P50-50.0) > 2.0 {
		t.Errorf("expected P50 near 50, got %f", *result.P50)
	}
}

func TestManager_InvalidSamples(t *testing.T) {
	manager := NewManager(5*time.Minute, false)

	now := time.Now().UnixMilli()

	// Invalid sample
	manager.Process(types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       50.0,
		Valid:       false,
		Error:       "timeout",
	})

	// Invalid samples should not create aggregates
	if manager.ActiveCount() != 0 {
		t.Errorf("expected 0 active (invalid ignored), got %d", manager.ActiveCount())
	}
}

func BenchmarkStreamingAggregate_Add(b *testing.B) {
	now := time.Now().UnixMilli()
	agg := New("prod", "router-01", "cpu", now, now+5*60*1000, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg.Add(float64(i), now+int64(i))
	}
}

func BenchmarkStreamingAggregate_AddWithPercentile(b *testing.B) {
	now := time.Now().UnixMilli()
	agg := New("prod", "router-01", "cpu", now, now+5*60*1000, true)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg.Add(float64(i), now+int64(i))
	}
}

func BenchmarkManager_Process(b *testing.B) {
	manager := NewManager(5*time.Minute, false)
	now := time.Now().UnixMilli()

	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       50.0,
		Valid:       true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sample.TimestampMs = now + int64(i%1000) // Stay in same bucket
		manager.Process(sample)
	}
}
