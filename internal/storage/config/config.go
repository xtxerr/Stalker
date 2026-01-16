package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the complete storage configuration.
type Config struct {
	// DataDir is the root directory for all storage files.
	DataDir string `yaml:"data_dir"`

	// Scale defines the expected load parameters.
	Scale ScaleConfig `yaml:"scale"`

	// Features configures optional features.
	Features FeaturesConfig `yaml:"features"`

	// Retention defines how long to keep data in each tier.
	Retention RetentionConfig `yaml:"retention"`

	// Ingestion configures the ingestion pipeline.
	Ingestion IngestionConfig `yaml:"ingestion"`

	// Compaction configures the compaction engine.
	Compaction CompactionConfig `yaml:"compaction"`

	// Backpressure configures load shedding.
	Backpressure BackpressureConfig `yaml:"backpressure"`

	// Query configures the query service.
	Query QueryConfig `yaml:"query"`
}

// ScaleConfig defines the expected load parameters.
type ScaleConfig struct {
	// PollerCount is the expected number of pollers (metric series).
	PollerCount int `yaml:"poller_count"`

	// PollIntervalSec is the polling interval in seconds.
	PollIntervalSec int `yaml:"poll_interval_sec"`
}

// FeaturesConfig configures optional features.
type FeaturesConfig struct {
	// RawBuffer configures the in-memory raw sample buffer.
	RawBuffer RawBufferConfig `yaml:"raw_buffer"`

	// Percentile configures DDSketch percentile calculation.
	Percentile PercentileConfig `yaml:"percentile"`

	// Compression configures Parquet compression.
	Compression CompressionConfig `yaml:"compression"`
}

// RawBufferConfig configures the in-memory raw sample buffer.
type RawBufferConfig struct {
	// Enabled enables the raw buffer.
	Enabled bool `yaml:"enabled"`

	// Duration is the maximum age of samples in the buffer.
	// Format: "5m", "10m", "30m"
	Duration time.Duration `yaml:"duration"`

	// MemoryLimit is an alternative to Duration - maximum memory usage.
	// Format: "4GB", "8GB"
	MemoryLimit string `yaml:"memory_limit"`
}

// PercentileConfig configures DDSketch percentile calculation.
type PercentileConfig struct {
	// Enabled enables percentile calculation.
	Enabled bool `yaml:"enabled"`

	// Accuracy is the relative accuracy (0.01 = 1% error).
	Accuracy float64 `yaml:"accuracy"`
}

// CompressionConfig configures Parquet compression.
type CompressionConfig struct {
	// Algorithm is the compression algorithm: snappy, zstd, lz4, none.
	Algorithm string `yaml:"algorithm"`

	// Level is the compression level (for zstd: 1-22).
	Level int `yaml:"level"`
}

// RetentionConfig defines how long to keep data in each tier.
type RetentionConfig struct {
	// Raw is the retention for raw samples.
	Raw time.Duration `yaml:"raw"`

	// FiveMin is the retention for 5-minute aggregates.
	FiveMin time.Duration `yaml:"five_min"`

	// Hourly is the retention for hourly aggregates.
	Hourly time.Duration `yaml:"hourly"`

	// Daily is the retention for daily aggregates.
	Daily time.Duration `yaml:"daily"`

	// Weekly is the retention for weekly aggregates.
	Weekly time.Duration `yaml:"weekly"`
}

// IngestionConfig configures the ingestion pipeline.
type IngestionConfig struct {
	// WAL configures the Write-Ahead Log.
	WAL WALConfig `yaml:"wal"`

	// Flush configures flush behavior.
	Flush FlushConfig `yaml:"flush"`
}

// WALConfig configures the Write-Ahead Log.
type WALConfig struct {
	// Dir is the WAL directory. Defaults to {DataDir}/wal.
	Dir string `yaml:"dir"`

	// SyncMode is the sync mode: async, sync, fsync.
	SyncMode string `yaml:"sync_mode"`

	// SyncInterval is the sync interval for async mode.
	SyncInterval time.Duration `yaml:"sync_interval"`

	// MaxSegmentSize is the maximum segment size before rotation.
	MaxSegmentSize int64 `yaml:"max_segment_size"`
}

// FlushConfig configures flush behavior.
type FlushConfig struct {
	// Interval is the flush interval.
	Interval time.Duration `yaml:"interval"`

	// MaxBufferSize triggers a flush when reached.
	MaxBufferSize int64 `yaml:"max_buffer_size"`
}

// CompactionConfig configures the compaction engine.
type CompactionConfig struct {
	// Workers is the number of parallel compaction workers.
	Workers int `yaml:"workers"`

	// Schedule configures compaction job schedules.
	Schedule CompactionSchedule `yaml:"schedule"`
}

// CompactionSchedule configures compaction job schedules.
type CompactionSchedule struct {
	// RawTo5Min is the schedule for Raw to 5min compaction.
	RawTo5Min string `yaml:"raw_to_5min"`

	// FiveMinToHourly is the schedule for 5min to hourly compaction.
	FiveMinToHourly string `yaml:"five_min_to_hourly"`

	// HourlyToDaily is the schedule for hourly to daily compaction.
	HourlyToDaily string `yaml:"hourly_to_daily"`

	// DailyToWeekly is the schedule for daily to weekly compaction.
	DailyToWeekly string `yaml:"daily_to_weekly"`
}

// BackpressureConfig configures load shedding.
type BackpressureConfig struct {
	// Enabled enables backpressure handling.
	Enabled bool `yaml:"enabled"`

	// Thresholds defines buffer usage thresholds for level changes.
	Thresholds BackpressureThresholds `yaml:"thresholds"`

	// Recovery configures recovery behavior.
	Recovery BackpressureRecovery `yaml:"recovery"`
}

// BackpressureThresholds defines buffer usage thresholds.
type BackpressureThresholds struct {
	// Warning threshold (0.0-1.0).
	Warning float64 `yaml:"warning"`

	// Critical threshold (0.0-1.0).
	Critical float64 `yaml:"critical"`

	// Emergency threshold (0.0-1.0).
	Emergency float64 `yaml:"emergency"`
}

// BackpressureRecovery configures recovery behavior.
type BackpressureRecovery struct {
	// Hysteresis to prevent flapping (0.0-1.0).
	Hysteresis float64 `yaml:"hysteresis"`

	// Cooldown is the minimum time between level changes.
	Cooldown time.Duration `yaml:"cooldown"`
}

// QueryConfig configures the query service.
type QueryConfig struct {
	// MemoryLimit is the DuckDB memory limit.
	MemoryLimit string `yaml:"memory_limit"`

	// Timeout is the query timeout.
	Timeout time.Duration `yaml:"timeout"`

	// MaxRows is the maximum number of rows returned.
	MaxRows int `yaml:"max_rows"`
}

// Load loads configuration from a YAML file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	config := DefaultConfig()
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}

// DefaultConfig returns a configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		DataDir: "/var/lib/stalker/storage",
		Scale: ScaleConfig{
			PollerCount:     1000000,
			PollIntervalSec: 25,
		},
		Features: FeaturesConfig{
			RawBuffer: RawBufferConfig{
				Enabled:  true,
				Duration: 5 * time.Minute,
			},
			Percentile: PercentileConfig{
				Enabled:  true,
				Accuracy: 0.01,
			},
			Compression: CompressionConfig{
				Algorithm: "zstd",
				Level:     3,
			},
		},
		Retention: RetentionConfig{
			Raw:     48 * time.Hour,
			FiveMin: 30 * 24 * time.Hour,
			Hourly:  90 * 24 * time.Hour,
			Daily:   2 * 365 * 24 * time.Hour,
			Weekly:  10 * 365 * 24 * time.Hour,
		},
		Ingestion: IngestionConfig{
			WAL: WALConfig{
				SyncMode:       "async",
				SyncInterval:   time.Second,
				MaxSegmentSize: 100 * 1024 * 1024, // 100MB
			},
			Flush: FlushConfig{
				Interval:      10 * time.Minute,
				MaxBufferSize: 100 * 1024 * 1024, // 100MB
			},
		},
		Compaction: CompactionConfig{
			Workers: 4,
			Schedule: CompactionSchedule{
				RawTo5Min:       "0 * * * *",     // Every hour
				FiveMinToHourly: "0 2 * * *",     // Daily at 2am
				HourlyToDaily:   "0 3 * * 0",     // Weekly on Sunday at 3am
				DailyToWeekly:   "0 4 1 * *",     // Monthly on 1st at 4am
			},
		},
		Backpressure: BackpressureConfig{
			Enabled: true,
			Thresholds: BackpressureThresholds{
				Warning:   0.50,
				Critical:  0.80,
				Emergency: 0.95,
			},
			Recovery: BackpressureRecovery{
				Hysteresis: 0.10,
				Cooldown:   30 * time.Second,
			},
		},
		Query: QueryConfig{
			MemoryLimit: "2GB",
			Timeout:     30 * time.Second,
			MaxRows:     1000000,
		},
	}
}
