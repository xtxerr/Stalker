package ingestion

import (
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestService_New(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Scale.PollerCount = 10000
	cfg.Scale.PollIntervalSec = 25

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if svc == nil {
		t.Fatal("service is nil")
	}

	if svc.IsRunning() {
		t.Error("service should not be running before Start()")
	}
}

func TestService_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Ingestion.Flush.Interval = 100 * time.Millisecond

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Start
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !svc.IsRunning() {
		t.Error("service should be running after Start()")
	}

	// Double start should fail
	if err := svc.Start(); err == nil {
		t.Error("expected error on double start")
	}

	// Stop
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if svc.IsRunning() {
		t.Error("service should not be running after Stop()")
	}
}

func TestService_Ingest(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Ingestion.Flush.Interval = 100 * time.Millisecond

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	// Ingest samples
	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 50, Valid: true, PollMs: 25},
		{Namespace: "prod", Target: "r1", Poller: "mem", TimestampMs: now, Value: 70, Valid: true, PollMs: 25},
		{Namespace: "prod", Target: "r2", Poller: "cpu", TimestampMs: now, Value: 30, Valid: true, PollMs: 25},
	}

	if err := svc.Ingest(samples); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	stats := svc.Stats()

	if stats.SamplesReceived != 3 {
		t.Errorf("expected 3 samples received, got %d", stats.SamplesReceived)
	}

	if stats.SamplesIngested != 3 {
		t.Errorf("expected 3 samples ingested, got %d", stats.SamplesIngested)
	}

	if stats.BatchesProcessed != 1 {
		t.Errorf("expected 1 batch processed, got %d", stats.BatchesProcessed)
	}
}

func TestService_IngestSingle(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: now,
		Value:       42.5,
		Valid:       true,
		PollMs:      25,
	}

	if err := svc.IngestSingle(sample); err != nil {
		t.Fatalf("IngestSingle: %v", err)
	}

	stats := svc.Stats()
	if stats.SamplesIngested != 1 {
		t.Errorf("expected 1 sample ingested, got %d", stats.SamplesIngested)
	}
}

func TestService_Buffer(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.RawBuffer.Enabled = true
	cfg.Features.RawBuffer.Duration = time.Minute

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	// Ingest samples
	for i := 0; i < 100; i++ {
		sample := types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now + int64(i)*100,
			Value:       float64(i),
			Valid:       true,
			PollMs:      25,
		}
		svc.IngestSingle(sample)
	}

	// Query buffer
	buf := svc.Buffer()
	if buf.Len() != 100 {
		t.Errorf("expected 100 samples in buffer, got %d", buf.Len())
	}

	// Query specific poller
	results := buf.QueryPoller("prod", "router-01", "cpu", 0)
	if len(results) != 100 {
		t.Errorf("expected 100 results, got %d", len(results))
	}
}

func TestService_Aggregation(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.Percentile.Enabled = false

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	// Ingest samples for same poller
	for i := 0; i < 10; i++ {
		sample := types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now + int64(i)*1000,
			Value:       float64(i * 10), // 0, 10, 20, ..., 90
			Valid:       true,
			PollMs:      25,
		}
		svc.IngestSingle(sample)
	}

	// Check aggregate manager
	agg := svc.AggregateManager()
	if agg.ActiveCount() != 1 {
		t.Errorf("expected 1 active aggregate, got %d", agg.ActiveCount())
	}
}

func TestService_ForceFlush(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Ingestion.Flush.Interval = time.Hour // Long interval

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Force flush shouldn't panic
	svc.ForceFlush()

	// Multiple force flushes shouldn't block
	for i := 0; i < 10; i++ {
		svc.ForceFlush()
	}
}

func TestService_IngestWhenNotRunning(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Don't start service

	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", Value: 50, Valid: true},
	}

	err = svc.Ingest(samples)
	if err == nil {
		t.Error("expected error when ingesting to non-running service")
	}
}

func TestService_EmptyIngest(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Empty ingest should succeed
	if err := svc.Ingest(nil); err != nil {
		t.Errorf("empty ingest should succeed: %v", err)
	}

	if err := svc.Ingest([]types.Sample{}); err != nil {
		t.Errorf("empty slice ingest should succeed: %v", err)
	}
}

func TestService_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	stats := svc.Stats()

	if !stats.Running {
		t.Error("expected Running=true")
	}

	if stats.SamplesReceived != 0 {
		t.Errorf("expected 0 samples received initially, got %d", stats.SamplesReceived)
	}

	now := time.Now().UnixMilli()

	// Ingest some samples
	for i := 0; i < 50; i++ {
		svc.IngestSingle(types.Sample{
			Namespace:   "prod",
			Target:      "r1",
			Poller:      "cpu",
			TimestampMs: now + int64(i),
			Value:       float64(i),
			Valid:       true,
			PollMs:      25,
		})
	}

	stats = svc.Stats()

	if stats.SamplesReceived != 50 {
		t.Errorf("expected 50 samples received, got %d", stats.SamplesReceived)
	}

	if stats.BatchesProcessed != 50 {
		t.Errorf("expected 50 batches processed, got %d", stats.BatchesProcessed)
	}

	if stats.WALSegments < 1 {
		t.Errorf("expected at least 1 WAL segment, got %d", stats.WALSegments)
	}
}

func TestCalculateBufferCapacity(t *testing.T) {
	tests := []struct {
		name            string
		pollerCount     int
		pollInterval    int
		bufferDuration  time.Duration
		bufferEnabled   bool
		expectedMin     int
		expectedMax     int
	}{
		{
			name:           "disabled buffer",
			bufferEnabled:  false,
			expectedMin:    10000,
			expectedMax:    10000,
		},
		{
			name:           "small scale",
			pollerCount:    1000,
			pollInterval:   25,
			bufferDuration: time.Minute,
			bufferEnabled:  true,
			expectedMin:    10000, // Minimum
			expectedMax:    10000,
		},
		{
			name:           "medium scale",
			pollerCount:    100000,
			pollInterval:   25,
			bufferDuration: 5 * time.Minute,
			bufferEnabled:  true,
			expectedMin:    100000,
			expectedMax:    2000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.DefaultConfig()
			cfg.Scale.PollerCount = tt.pollerCount
			cfg.Scale.PollIntervalSec = tt.pollInterval
			cfg.Features.RawBuffer.Enabled = tt.bufferEnabled
			cfg.Features.RawBuffer.Duration = tt.bufferDuration

			capacity := calculateBufferCapacity(cfg)

			if capacity < tt.expectedMin {
				t.Errorf("capacity %d < expected min %d", capacity, tt.expectedMin)
			}
			if capacity > tt.expectedMax {
				t.Errorf("capacity %d > expected max %d", capacity, tt.expectedMax)
			}
		})
	}
}

func BenchmarkService_Ingest(b *testing.B) {
	tmpDir := b.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Ingestion.Flush.Interval = time.Hour // Disable periodic flush

	svc, err := New(cfg)
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	// Prepare batch
	samples := make([]types.Sample, 100)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now,
			Value:       float64(i),
			Valid:       true,
			PollMs:      25,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Update timestamps
		for j := range samples {
			samples[j].TimestampMs = now + int64(i*100+j)
		}
		svc.Ingest(samples)
	}
}
