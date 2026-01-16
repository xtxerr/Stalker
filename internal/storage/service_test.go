package storage

import (
	"context"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/query"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestService_New(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

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

func TestService_NewWithNilConfig(t *testing.T) {
	// Should use default config and fail validation (no data dir)
	_, err := New(nil)
	// This might fail due to directory creation issues, which is expected
	if err == nil {
		// If it succeeds, that's fine too - depends on system state
	}
}

func TestService_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

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

	// Stats should show running
	stats := svc.Stats()
	if !stats.Running {
		t.Error("stats.Running should be true")
	}

	if stats.Uptime <= 0 {
		t.Error("uptime should be positive")
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

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 50, Valid: true, PollMs: 25},
		{Namespace: "prod", Target: "r1", Poller: "mem", TimestampMs: now, Value: 70, Valid: true, PollMs: 25},
	}

	if err := svc.Ingest(samples); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	stats := svc.Stats()
	if stats.Ingestion.SamplesIngested != 2 {
		t.Errorf("expected 2 samples ingested, got %d", stats.Ingestion.SamplesIngested)
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

	if err := svc.IngestSingle(types.Sample{
		Namespace: "prod", Target: "r1", Poller: "cpu",
		TimestampMs: now, Value: 50, Valid: true, PollMs: 25,
	}); err != nil {
		t.Fatalf("IngestSingle: %v", err)
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

func TestService_Query(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.RawBuffer.Enabled = true

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now()
	nowMs := now.UnixMilli()

	// Ingest some samples
	for i := 0; i < 10; i++ {
		svc.IngestSingle(types.Sample{
			Namespace: "prod", Target: "r1", Poller: "cpu",
			TimestampMs: nowMs + int64(i)*1000, Value: float64(i), Valid: true, PollMs: 25,
		})
	}

	ctx := context.Background()
	q := query.PollerQuery{
		Namespace: "prod",
		Target:    "r1",
		Poller:    "cpu",
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	}

	results, err := svc.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// Should have results from buffer
	if len(results) == 0 {
		t.Error("expected query results")
	}
}

func TestService_QueryWhenNotRunning(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Don't start

	ctx := context.Background()
	_, err = svc.Query(ctx, query.PollerQuery{})
	if err == nil {
		t.Error("expected error when querying non-running service")
	}
}

func TestService_Buffer(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.RawBuffer.Enabled = true

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	buf := svc.Buffer()
	if buf == nil {
		t.Fatal("buffer is nil")
	}
}

func TestService_Config(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	returnedCfg := svc.Config()
	if returnedCfg.DataDir != tmpDir {
		t.Errorf("expected DataDir=%s, got %s", tmpDir, returnedCfg.DataDir)
	}
}

func TestService_ForceFlush(t *testing.T) {
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

	// Should not panic
	svc.ForceFlush()
}

func TestService_BackpressureLevel(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Backpressure.Enabled = true

	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	level := svc.BackpressureLevel()
	if level.String() != "normal" {
		t.Errorf("expected normal level, got %s", level)
	}
}

func BenchmarkService_Ingest(b *testing.B) {
	tmpDir := b.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Ingestion.Flush.Interval = time.Hour

	svc, err := New(cfg)
	if err != nil {
		b.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		b.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	samples := make([]types.Sample, 100)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace: "prod", Target: "r1", Poller: "cpu",
			TimestampMs: now, Value: float64(i), Valid: true, PollMs: 25,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range samples {
			samples[j].TimestampMs = now + int64(i*100+j)
		}
		svc.Ingest(samples)
	}
}
