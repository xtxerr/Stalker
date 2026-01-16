package compaction

import (
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestEngine_New(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if engine == nil {
		t.Fatal("engine is nil")
	}

	if engine.IsRunning() {
		t.Error("engine should not be running before Start()")
	}
}

func TestEngine_StartStop(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Compaction.Workers = 2

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Start
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !engine.IsRunning() {
		t.Error("engine should be running after Start()")
	}

	// Double start should fail
	if err := engine.Start(); err == nil {
		t.Error("expected error on double start")
	}

	// Stop
	if err := engine.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if engine.IsRunning() {
		t.Error("engine should not be running after Stop()")
	}
}

func TestEngine_MergeAggregates(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	now := time.Now().UnixMilli()

	aggs := []types.AggregateResult{
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			BucketStart: now,
			BucketEnd:   now + 5*60*1000,
			Count:       10,
			Sum:         100,
			Min:         5,
			Max:         15,
			Avg:         10,
			FirstTs:     now,
			LastTs:      now + 4*60*1000,
		},
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			BucketStart: now + 5*60*1000,
			BucketEnd:   now + 10*60*1000,
			Count:       10,
			Sum:         200,
			Min:         10,
			Max:         30,
			Avg:         20,
			FirstTs:     now + 5*60*1000,
			LastTs:      now + 9*60*1000,
		},
	}

	bucketStart := now
	merged := engine.mergeAggregates(aggs, bucketStart, types.TierHourly)

	if merged.Count != 20 {
		t.Errorf("expected count=20, got %d", merged.Count)
	}

	if merged.Sum != 300 {
		t.Errorf("expected sum=300, got %f", merged.Sum)
	}

	if merged.Min != 5 {
		t.Errorf("expected min=5, got %f", merged.Min)
	}

	if merged.Max != 30 {
		t.Errorf("expected max=30, got %f", merged.Max)
	}

	expectedAvg := 15.0
	if merged.Avg != expectedAvg {
		t.Errorf("expected avg=%f, got %f", expectedAvg, merged.Avg)
	}

	if merged.FirstTs != now {
		t.Errorf("expected firstTs=%d, got %d", now, merged.FirstTs)
	}
}

func TestEngine_ReAggregate(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Create aggregates for different pollers
	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli()

	aggs := []types.AggregateResult{
		// Poller 1, first bucket
		{
			Namespace: "prod", Target: "r1", Poller: "cpu",
			BucketStart: baseTime, BucketEnd: baseTime + 5*60*1000,
			Count: 10, Sum: 100, Min: 5, Max: 15, Avg: 10,
		},
		// Poller 1, second bucket (same hour)
		{
			Namespace: "prod", Target: "r1", Poller: "cpu",
			BucketStart: baseTime + 5*60*1000, BucketEnd: baseTime + 10*60*1000,
			Count: 10, Sum: 200, Min: 10, Max: 30, Avg: 20,
		},
		// Poller 2
		{
			Namespace: "prod", Target: "r1", Poller: "memory",
			BucketStart: baseTime, BucketEnd: baseTime + 5*60*1000,
			Count: 5, Sum: 500, Min: 80, Max: 120, Avg: 100,
		},
	}

	// Re-aggregate to hourly
	result := engine.reAggregate(aggs, types.TierHourly)

	if len(result) != 2 {
		t.Fatalf("expected 2 aggregates (cpu + memory), got %d", len(result))
	}

	// Find CPU aggregate
	var cpuAgg *types.AggregateResult
	for i := range result {
		if result[i].Poller == "cpu" {
			cpuAgg = &result[i]
			break
		}
	}

	if cpuAgg == nil {
		t.Fatal("cpu aggregate not found")
	}

	if cpuAgg.Count != 20 {
		t.Errorf("cpu: expected count=20, got %d", cpuAgg.Count)
	}

	if cpuAgg.Sum != 300 {
		t.Errorf("cpu: expected sum=300, got %f", cpuAgg.Sum)
	}
}

func TestEngine_SubmitJob(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir
	cfg.Compaction.Workers = 1

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Cannot submit when not running
	job := Job{
		SourceTier: types.TierRaw,
		TargetTier: types.Tier5Min,
	}

	if engine.SubmitJob(job) {
		t.Error("should not be able to submit job when not running")
	}

	// Start engine
	if err := engine.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer engine.Stop()

	// Now should be able to submit
	if !engine.SubmitJob(job) {
		t.Error("should be able to submit job when running")
	}

	stats := engine.Stats()
	if stats.JobsScheduled != 1 {
		t.Errorf("expected 1 job scheduled, got %d", stats.JobsScheduled)
	}
}

func TestEngine_OutputPath(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	testTime := time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		tier     types.Tier
		expected string
	}{
		{types.Tier5Min, "5min/2026-01-15_10-30.parquet"},
		{types.TierHourly, "hourly/2026-01-15.parquet"},
		{types.TierDaily, "daily/2026-01.parquet"},
		{types.TierWeekly, "weekly/2026.parquet"},
	}

	for _, tt := range tests {
		path := engine.outputPath(tt.tier, testTime)
		expected := tmpDir + "/" + tt.expected

		if path != expected {
			t.Errorf("tier %s: expected %s, got %s", tt.tier, expected, path)
		}
	}
}

func TestEngine_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.DefaultConfig()
	cfg.DataDir = tmpDir

	engine, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stats := engine.Stats()

	if stats.Running {
		t.Error("expected Running=false")
	}

	if stats.JobsScheduled != 0 {
		t.Errorf("expected 0 jobs scheduled, got %d", stats.JobsScheduled)
	}

	// Start and check
	engine.Start()
	defer engine.Stop()

	stats = engine.Stats()
	if !stats.Running {
		t.Error("expected Running=true")
	}
}

func TestJob(t *testing.T) {
	job := Job{
		SourceTier:  types.TierRaw,
		TargetTier:  types.Tier5Min,
		StartTime:   time.Now(),
		EndTime:     time.Now().Add(time.Hour),
		SourceFiles: []string{"/path/to/file1.parquet", "/path/to/file2.parquet"},
		OutputFile:  "/path/to/output.parquet",
	}

	if job.SourceTier != types.TierRaw {
		t.Error("unexpected source tier")
	}

	if len(job.SourceFiles) != 2 {
		t.Error("unexpected number of source files")
	}
}
