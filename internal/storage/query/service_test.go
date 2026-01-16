package query

import (
	"context"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/buffer"
	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestService_New(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	if svc == nil {
		t.Fatal("service is nil")
	}
}

func TestService_ExecuteSQL(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	ctx := context.Background()

	// Simple query
	results, err := svc.ExecuteSQL(ctx, "SELECT 1 AS value")
	if err != nil {
		t.Fatalf("ExecuteSQL: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	stats := svc.Stats()
	if stats.QueriesExecuted != 1 {
		t.Errorf("expected 1 query executed, got %d", stats.QueriesExecuted)
	}
}

func TestService_QueryBuffer(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	// Create buffer with samples
	buf := buffer.New(1000)

	now := time.Now()
	nowMs := now.UnixMilli()

	for i := 0; i < 10; i++ {
		buf.Push(types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: nowMs + int64(i)*1000,
			Value:       float64(i * 10), // 0, 10, 20, ..., 90
			Valid:       true,
			PollMs:      25,
		})
	}

	svc, err := New(cfg, buf)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	// Query buffer through service
	q := PollerQuery{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "cpu",
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	}

	ctx := context.Background()
	results, err := svc.QueryPoller(ctx, q)
	if err != nil {
		t.Fatalf("QueryPoller: %v", err)
	}

	// Should have 1 aggregate from buffer data
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	r := results[0]
	if r.Count != 10 {
		t.Errorf("expected count=10, got %d", r.Count)
	}

	// Sum of 0+10+20+...+90 = 450
	if r.Sum != 450 {
		t.Errorf("expected sum=450, got %f", r.Sum)
	}

	if r.Min != 0 {
		t.Errorf("expected min=0, got %f", r.Min)
	}

	if r.Max != 90 {
		t.Errorf("expected max=90, got %f", r.Max)
	}
}

func TestService_AggregateSamples(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	now := time.Now()
	nowMs := now.UnixMilli()

	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: nowMs, Value: 10, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: nowMs + 1000, Value: 20, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: nowMs + 2000, Value: 30, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: nowMs + 3000, Value: 40, Valid: false}, // Invalid
	}

	results := svc.aggregateSamples(samples, now.Add(-time.Hour), now.Add(time.Hour))

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	r := results[0]
	if r.Count != 3 { // Only valid samples
		t.Errorf("expected count=3, got %d", r.Count)
	}

	if r.Sum != 60 {
		t.Errorf("expected sum=60, got %f", r.Sum)
	}
}

func TestService_MergeResults(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	parquet := []types.AggregateResult{
		{Namespace: "prod", Target: "r1", Poller: "cpu", Count: 10},
	}

	buffer := []types.AggregateResult{
		{Namespace: "prod", Target: "r1", Poller: "cpu", Count: 5},
	}

	merged := svc.mergeResults(parquet, buffer)

	if len(merged) != 2 {
		t.Errorf("expected 2 results, got %d", len(merged))
	}
}

func TestService_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer svc.Close()

	stats := svc.Stats()

	if stats.QueriesExecuted != 0 {
		t.Errorf("expected 0 queries executed, got %d", stats.QueriesExecuted)
	}

	// Execute some queries
	ctx := context.Background()
	svc.ExecuteSQL(ctx, "SELECT 1")
	svc.ExecuteSQL(ctx, "SELECT 2")

	stats = svc.Stats()
	if stats.QueriesExecuted != 2 {
		t.Errorf("expected 2 queries executed, got %d", stats.QueriesExecuted)
	}
}

func TestPollerQuery(t *testing.T) {
	q := PollerQuery{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "cpu",
		StartTime: time.Now().Add(-time.Hour),
		EndTime:   time.Now(),
		Limit:     100,
	}

	if q.Namespace != "prod" {
		t.Error("unexpected namespace")
	}

	if q.Limit != 100 {
		t.Error("unexpected limit")
	}
}

func TestTimeRangeQuery(t *testing.T) {
	q := TimeRangeQuery{
		Namespace: "prod",
		Target:    "router-01",
		StartTime: time.Now().Add(-24 * time.Hour),
		EndTime:   time.Now(),
		Limit:     1000,
	}

	if q.Namespace != "prod" {
		t.Error("unexpected namespace")
	}

	duration := q.EndTime.Sub(q.StartTime)
	if duration != 24*time.Hour {
		t.Errorf("expected 24h duration, got %v", duration)
	}
}

func BenchmarkService_AggregateSamples(b *testing.B) {
	tmpDir := b.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, _ := New(cfg, nil)
	defer svc.Close()

	now := time.Now()
	nowMs := now.UnixMilli()

	// Create 1000 samples
	samples := make([]types.Sample, 1000)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: nowMs + int64(i)*100,
			Value:       float64(i),
			Valid:       true,
			PollMs:      25,
		}
	}

	startTime := now.Add(-time.Hour)
	endTime := now.Add(time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		svc.aggregateSamples(samples, startTime, endTime)
	}
}
