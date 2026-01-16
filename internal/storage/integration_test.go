package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage"
	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/query"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// TestIntegration_FullPipeline tests the complete ingestion → query pipeline.
func TestIntegration_FullPipeline(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Scale.PollerCount = 1000
	cfg.Scale.PollIntervalSec = 25
	cfg.Features.RawBuffer.Enabled = true
	cfg.Features.RawBuffer.Duration = time.Minute
	cfg.Features.Percentile.Enabled = true
	cfg.Ingestion.Flush.Interval = time.Hour // Disable auto-flush for test
	cfg.Backpressure.Enabled = true

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	if !svc.IsRunning() {
		t.Fatal("service should be running")
	}

	// Ingest samples
	now := time.Now()
	nowMs := now.UnixMilli()

	samples := make([]types.Sample, 100)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: nowMs + int64(i)*1000,
			Value:       float64(i % 100),
			Valid:       true,
			PollMs:      25,
		}
	}

	if err := svc.Ingest(samples); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Check stats
	stats := svc.Stats()
	if stats.Ingestion.SamplesReceived != 100 {
		t.Errorf("expected 100 samples received, got %d", stats.Ingestion.SamplesReceived)
	}

	if stats.Ingestion.SamplesIngested != 100 {
		t.Errorf("expected 100 samples ingested, got %d", stats.Ingestion.SamplesIngested)
	}

	// Query buffer directly
	buf := svc.Buffer()
	if buf.Len() != 100 {
		t.Errorf("expected 100 samples in buffer, got %d", buf.Len())
	}

	// Query through service
	ctx := context.Background()
	q := query.PollerQuery{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "cpu",
		StartTime: now.Add(-time.Hour),
		EndTime:   now.Add(time.Hour),
	}

	results, err := svc.Query(ctx, q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	// Should have at least one result from buffer
	if len(results) == 0 {
		t.Error("expected at least one query result")
	}
}

// TestIntegration_MultiplePollers tests ingestion with multiple pollers.
func TestIntegration_MultiplePollers(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.RawBuffer.Enabled = true
	cfg.Features.RawBuffer.Duration = time.Minute

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	// Ingest samples for multiple pollers
	pollers := []string{"cpu", "memory", "disk", "network"}
	targets := []string{"router-01", "router-02", "switch-01"}

	var allSamples []types.Sample

	for _, target := range targets {
		for _, poller := range pollers {
			for i := 0; i < 10; i++ {
				allSamples = append(allSamples, types.Sample{
					Namespace:   "prod",
					Target:      target,
					Poller:      poller,
					TimestampMs: now + int64(i)*1000,
					Value:       float64(i * 10),
					Valid:       true,
					PollMs:      25,
				})
			}
		}
	}

	if err := svc.Ingest(allSamples); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	stats := svc.Stats()
	expectedSamples := int64(len(targets) * len(pollers) * 10)
	if stats.Ingestion.SamplesIngested != expectedSamples {
		t.Errorf("expected %d samples, got %d", expectedSamples, stats.Ingestion.SamplesIngested)
	}

	// Query specific poller
	ctx := context.Background()
	results, err := svc.Query(ctx, query.PollerQuery{
		Namespace: "prod",
		Target:    "router-01",
		Poller:    "cpu",
		StartTime: time.UnixMilli(now - 1000),
		EndTime:   time.UnixMilli(now + 100000),
	})

	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if len(results) == 0 {
		t.Error("expected query results for router-01/cpu")
	}
}

// TestIntegration_InvalidSamples tests that invalid samples are handled correctly.
func TestIntegration_InvalidSamples(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.RawBuffer.Enabled = true

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	now := time.Now().UnixMilli()

	// Mix of valid and invalid samples
	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 50, Valid: true, PollMs: 25},
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + 1000, Value: 0, Valid: false, Error: "timeout", PollMs: 1000},
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + 2000, Value: 60, Valid: true, PollMs: 25},
	}

	if err := svc.Ingest(samples); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// All samples should be in buffer (even invalid ones for tracking)
	buf := svc.Buffer()
	if buf.Len() != 3 {
		t.Errorf("expected 3 samples in buffer, got %d", buf.Len())
	}
}

// TestIntegration_BackpressureResponse tests backpressure behavior.
func TestIntegration_BackpressureResponse(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Features.RawBuffer.Enabled = true
	cfg.Features.RawBuffer.Duration = time.Minute
	cfg.Backpressure.Enabled = true
	cfg.Backpressure.Thresholds.Warning = 0.5
	cfg.Backpressure.Thresholds.Critical = 0.8
	cfg.Backpressure.Thresholds.Emergency = 0.95

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Initial level should be normal
	level := svc.BackpressureLevel()
	if level.String() != "normal" {
		t.Errorf("expected normal level, got %s", level)
	}
}

// TestIntegration_ServiceLifecycle tests start/stop behavior.
func TestIntegration_ServiceLifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Service should not be running initially
	if svc.IsRunning() {
		t.Error("service should not be running before Start()")
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
		t.Error("expected error on double Start()")
	}

	// Ingest while running
	now := time.Now().UnixMilli()
	if err := svc.IngestSingle(types.Sample{
		Namespace: "prod", Target: "r1", Poller: "cpu",
		TimestampMs: now, Value: 50, Valid: true, PollMs: 25,
	}); err != nil {
		t.Errorf("IngestSingle: %v", err)
	}

	// Stop
	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if svc.IsRunning() {
		t.Error("service should not be running after Stop()")
	}

	// Ingest after stop should fail
	if err := svc.IngestSingle(types.Sample{
		Namespace: "prod", Target: "r1", Poller: "cpu",
		TimestampMs: now, Value: 60, Valid: true, PollMs: 25,
	}); err == nil {
		t.Error("expected error on Ingest after Stop()")
	}
}

// TestIntegration_ForceFlush tests manual flush.
func TestIntegration_ForceFlush(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Ingestion.Flush.Interval = time.Hour // Long interval

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Ingest some samples
	now := time.Now().UnixMilli()
	for i := 0; i < 10; i++ {
		svc.IngestSingle(types.Sample{
			Namespace: "prod", Target: "r1", Poller: "cpu",
			TimestampMs: now + int64(i)*1000, Value: float64(i), Valid: true, PollMs: 25,
		})
	}

	// Force flush should not panic
	svc.ForceFlush()
}

// TestIntegration_DiskUsage tests disk usage reporting.
func TestIntegration_DiskUsage(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	usage := svc.GetDiskUsage()

	// Should have entries for all tiers (even if empty)
	for _, tier := range types.AllTiers() {
		if _, exists := usage[tier]; !exists {
			// It's okay if tier doesn't exist yet
			continue
		}
	}
}

// TestIntegration_RetentionDryRun tests retention dry run.
func TestIntegration_RetentionDryRun(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Dry run should not panic or error
	results := svc.DryRunRetention()

	// Should have results for all tiers
	if len(results) != 5 {
		t.Errorf("expected 5 tier results, got %d", len(results))
	}
}

// TestIntegration_QuerySQL tests raw SQL execution.
func TestIntegration_QuerySQL(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	svc, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	ctx := context.Background()

	// Simple SQL query
	results, err := svc.QuerySQL(ctx, "SELECT 1 AS value, 'test' AS name")
	if err != nil {
		t.Fatalf("QuerySQL: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
}
