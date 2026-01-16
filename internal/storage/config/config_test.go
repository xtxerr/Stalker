package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.DataDir == "" {
		t.Error("expected default data_dir")
	}

	if cfg.Scale.PollerCount <= 0 {
		t.Error("expected positive poller_count")
	}

	if cfg.Scale.PollIntervalSec <= 0 {
		t.Error("expected positive poll_interval_sec")
	}

	if !cfg.Features.RawBuffer.Enabled {
		t.Error("expected raw_buffer enabled by default")
	}

	if !cfg.Features.Percentile.Enabled {
		t.Error("expected percentile enabled by default")
	}

	if cfg.Retention.Raw <= 0 {
		t.Error("expected positive raw retention")
	}
}

func TestConfigValidate(t *testing.T) {
	// Valid config
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid: %v", err)
	}

	// Invalid: empty data_dir
	cfg = DefaultConfig()
	cfg.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty data_dir")
	}

	// Invalid: negative poller count
	cfg = DefaultConfig()
	cfg.Scale.PollerCount = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative poller_count")
	}

	// Invalid: bad compression algorithm
	cfg = DefaultConfig()
	cfg.Features.Compression.Algorithm = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid compression algorithm")
	}
}

func TestRetentionValidation(t *testing.T) {
	cfg := DefaultConfig()

	// Valid: increasing retention
	if err := cfg.Retention.Validate(); err != nil {
		t.Errorf("valid retention should pass: %v", err)
	}

	// Invalid: 5min < raw
	cfg.Retention.FiveMin = 24 * time.Hour
	cfg.Retention.Raw = 48 * time.Hour
	if err := cfg.Retention.Validate(); err == nil {
		t.Error("expected error when 5min < raw")
	}
}

func TestBackpressureValidation(t *testing.T) {
	cfg := DefaultConfig()

	// Valid thresholds
	if err := cfg.Backpressure.Validate(); err != nil {
		t.Errorf("valid backpressure should pass: %v", err)
	}

	// Invalid: warning >= critical
	cfg.Backpressure.Thresholds.Warning = 0.90
	cfg.Backpressure.Thresholds.Critical = 0.80
	if err := cfg.Backpressure.Validate(); err == nil {
		t.Error("expected error when warning >= critical")
	}
}

func TestLoadConfig(t *testing.T) {
	// Create temp config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test.yaml")

	configContent := `
data_dir: /tmp/test-storage
scale:
  poller_count: 100000
  poll_interval_sec: 30
features:
  raw_buffer:
    enabled: true
    duration: 10m
  percentile:
    enabled: false
    accuracy: 0.01
  compression:
    algorithm: snappy
    level: 0
retention:
  raw: 24h
  five_min: 168h
  hourly: 720h
  daily: 8760h
  weekly: 43800h
ingestion:
  wal:
    sync_mode: async
    sync_interval: 1s
    max_segment_size: 104857600
  flush:
    interval: 5m
    max_buffer_size: 52428800
backpressure:
  enabled: true
  thresholds:
    warning: 0.5
    critical: 0.8
    emergency: 0.95
  recovery:
    hysteresis: 0.1
    cooldown: 30s
query:
  memory_limit: 1GB
  timeout: 15s
  max_rows: 500000
`

	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.DataDir != "/tmp/test-storage" {
		t.Errorf("expected data_dir=/tmp/test-storage, got %s", cfg.DataDir)
	}

	if cfg.Scale.PollerCount != 100000 {
		t.Errorf("expected poller_count=100000, got %d", cfg.Scale.PollerCount)
	}

	if cfg.Scale.PollIntervalSec != 30 {
		t.Errorf("expected poll_interval_sec=30, got %d", cfg.Scale.PollIntervalSec)
	}

	if cfg.Features.RawBuffer.Duration != 10*time.Minute {
		t.Errorf("expected duration=10m, got %v", cfg.Features.RawBuffer.Duration)
	}

	if cfg.Features.Percentile.Enabled {
		t.Error("expected percentile disabled")
	}

	if cfg.Features.Compression.Algorithm != "snappy" {
		t.Errorf("expected compression=snappy, got %s", cfg.Features.Compression.Algorithm)
	}
}

func TestLoadConfigInvalidFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "invalid.yaml")

	if err := os.WriteFile(configPath, []byte("invalid: yaml: content: ["), 0644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := Load(configPath)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestCalculateRequirements(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Scale.PollerCount = 1000000
	cfg.Scale.PollIntervalSec = 25

	req := cfg.CalculateRequirements()

	// Expected: 1M / 25 = 40,000 samples/sec
	expectedSPS := int64(40000)
	if req.SamplesPerSecond != expectedSPS {
		t.Errorf("expected %d samples/sec, got %d", expectedSPS, req.SamplesPerSecond)
	}

	// Should have positive values
	if req.RawBufferBytes <= 0 {
		t.Error("expected positive raw buffer bytes")
	}

	if req.AggregateBufferBytes <= 0 {
		t.Error("expected positive aggregate buffer bytes")
	}

	if req.TotalStorageBytes <= 0 {
		t.Error("expected positive total storage bytes")
	}

	if req.RecommendedCPUCores <= 0 {
		t.Error("expected positive CPU cores")
	}
}

func TestFormatRequirements(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Scale.PollerCount = 1000000

	req := cfg.CalculateRequirements()
	output := req.FormatRequirements()

	// Should contain key sections
	if len(output) < 100 {
		t.Error("expected substantial output")
	}
}

func TestParseMemoryLimit(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"1GB", 1 * 1024 * 1024 * 1024},
		{"2GB", 2 * 1024 * 1024 * 1024},
		{"512MB", 512 * 1024 * 1024},
		{"1024KB", 1024 * 1024},
		{"", 2 * 1024 * 1024 * 1024}, // Default
	}

	for _, tt := range tests {
		result := parseMemoryLimit(tt.input)
		if result != tt.expected {
			t.Errorf("parseMemoryLimit(%s): expected %d, got %d", tt.input, tt.expected, result)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{500, "500 B"},
		{1024, "1.00 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.input)
		if result != tt.expected {
			t.Errorf("formatBytes(%d): expected %s, got %s", tt.input, tt.expected, result)
		}
	}
}

func TestWALDir(t *testing.T) {
	cfg := DefaultConfig()

	// Default: DataDir/wal
	expected := filepath.Join(cfg.DataDir, "wal")
	if cfg.WALDir() != expected {
		t.Errorf("expected %s, got %s", expected, cfg.WALDir())
	}

	// Custom WAL dir
	cfg.Ingestion.WAL.Dir = "/custom/wal"
	if cfg.WALDir() != "/custom/wal" {
		t.Errorf("expected /custom/wal, got %s", cfg.WALDir())
	}
}

func TestTierDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = "/data/stalker"

	tests := []struct {
		tier     string
		expected string
	}{
		{"raw", "/data/stalker/raw"},
		{"5min", "/data/stalker/5min"},
		{"hourly", "/data/stalker/hourly"},
	}

	for _, tt := range tests {
		result := cfg.TierDir(tt.tier)
		if result != tt.expected {
			t.Errorf("TierDir(%s): expected %s, got %s", tt.tier, tt.expected, result)
		}
	}
}

func TestEnsureDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultConfig()
	cfg.DataDir = filepath.Join(tmpDir, "storage")

	if err := cfg.EnsureDirectories(); err != nil {
		t.Fatalf("EnsureDirectories: %v", err)
	}

	// Check directories exist
	dirs := []string{
		cfg.DataDir,
		cfg.WALDir(),
		filepath.Join(cfg.DataDir, "raw"),
		filepath.Join(cfg.DataDir, "5min"),
		filepath.Join(cfg.DataDir, "hourly"),
		filepath.Join(cfg.DataDir, "daily"),
		filepath.Join(cfg.DataDir, "weekly"),
	}

	for _, dir := range dirs {
		info, err := os.Stat(dir)
		if err != nil {
			t.Errorf("directory %s not created: %v", dir, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("%s is not a directory", dir)
		}
	}
}
