package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	var errs []error

	// DataDir
	if c.DataDir == "" {
		errs = append(errs, errors.New("data_dir is required"))
	}

	// Scale
	if err := c.Scale.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("scale: %w", err))
	}

	// Features
	if err := c.Features.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("features: %w", err))
	}

	// Retention
	if err := c.Retention.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("retention: %w", err))
	}

	// Ingestion
	if err := c.Ingestion.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("ingestion: %w", err))
	}

	// Backpressure
	if err := c.Backpressure.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("backpressure: %w", err))
	}

	// Query
	if err := c.Query.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("query: %w", err))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Validate checks the scale configuration.
func (c *ScaleConfig) Validate() error {
	var errs []error

	if c.PollerCount <= 0 {
		errs = append(errs, errors.New("poller_count must be positive"))
	}

	if c.PollIntervalSec <= 0 {
		errs = append(errs, errors.New("poll_interval_sec must be positive"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Validate checks the features configuration.
func (c *FeaturesConfig) Validate() error {
	var errs []error

	// RawBuffer
	if c.RawBuffer.Enabled {
		if c.RawBuffer.Duration <= 0 && c.RawBuffer.MemoryLimit == "" {
			errs = append(errs, errors.New("raw_buffer: either duration or memory_limit required when enabled"))
		}
	}

	// Percentile
	if c.Percentile.Enabled {
		if c.Percentile.Accuracy <= 0 || c.Percentile.Accuracy > 1 {
			errs = append(errs, errors.New("percentile.accuracy must be between 0 and 1"))
		}
	}

	// Compression
	validAlgorithms := map[string]bool{
		"snappy": true,
		"zstd":   true,
		"lz4":    true,
		"none":   true,
		"":       true, // Empty defaults to zstd
	}
	if !validAlgorithms[c.Compression.Algorithm] {
		errs = append(errs, fmt.Errorf("compression.algorithm must be one of: snappy, zstd, lz4, none"))
	}

	if c.Compression.Algorithm == "zstd" && (c.Compression.Level < 0 || c.Compression.Level > 22) {
		errs = append(errs, errors.New("compression.level for zstd must be between 0 and 22"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Validate checks the retention configuration.
func (c *RetentionConfig) Validate() error {
	var errs []error

	if c.Raw <= 0 {
		errs = append(errs, errors.New("raw retention must be positive"))
	}

	if c.FiveMin <= 0 {
		errs = append(errs, errors.New("five_min retention must be positive"))
	}

	if c.Hourly <= 0 {
		errs = append(errs, errors.New("hourly retention must be positive"))
	}

	if c.Daily <= 0 {
		errs = append(errs, errors.New("daily retention must be positive"))
	}

	if c.Weekly <= 0 {
		errs = append(errs, errors.New("weekly retention must be positive"))
	}

	// Check that higher tiers have longer retention
	if c.FiveMin < c.Raw {
		errs = append(errs, errors.New("five_min retention should be >= raw retention"))
	}
	if c.Hourly < c.FiveMin {
		errs = append(errs, errors.New("hourly retention should be >= five_min retention"))
	}
	if c.Daily < c.Hourly {
		errs = append(errs, errors.New("daily retention should be >= hourly retention"))
	}
	if c.Weekly < c.Daily {
		errs = append(errs, errors.New("weekly retention should be >= daily retention"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Validate checks the ingestion configuration.
func (c *IngestionConfig) Validate() error {
	var errs []error

	// WAL
	validSyncModes := map[string]bool{
		"async": true,
		"sync":  true,
		"fsync": true,
		"":      true, // Empty defaults to async
	}
	if !validSyncModes[c.WAL.SyncMode] {
		errs = append(errs, errors.New("wal.sync_mode must be one of: async, sync, fsync"))
	}

	if c.WAL.SyncMode == "async" && c.WAL.SyncInterval <= 0 {
		errs = append(errs, errors.New("wal.sync_interval must be positive for async mode"))
	}

	if c.WAL.MaxSegmentSize < 0 {
		errs = append(errs, errors.New("wal.max_segment_size must be non-negative"))
	}

	// Flush
	if c.Flush.Interval <= 0 {
		errs = append(errs, errors.New("flush.interval must be positive"))
	}

	if c.Flush.MaxBufferSize < 0 {
		errs = append(errs, errors.New("flush.max_buffer_size must be non-negative"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Validate checks the backpressure configuration.
func (c *BackpressureConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	var errs []error

	// Thresholds must be in order
	if c.Thresholds.Warning <= 0 || c.Thresholds.Warning >= 1 {
		errs = append(errs, errors.New("thresholds.warning must be between 0 and 1"))
	}
	if c.Thresholds.Critical <= 0 || c.Thresholds.Critical >= 1 {
		errs = append(errs, errors.New("thresholds.critical must be between 0 and 1"))
	}
	if c.Thresholds.Emergency <= 0 || c.Thresholds.Emergency >= 1 {
		errs = append(errs, errors.New("thresholds.emergency must be between 0 and 1"))
	}

	if c.Thresholds.Warning >= c.Thresholds.Critical {
		errs = append(errs, errors.New("thresholds.warning must be < thresholds.critical"))
	}
	if c.Thresholds.Critical >= c.Thresholds.Emergency {
		errs = append(errs, errors.New("thresholds.critical must be < thresholds.emergency"))
	}

	// Recovery
	if c.Recovery.Hysteresis < 0 || c.Recovery.Hysteresis >= 0.5 {
		errs = append(errs, errors.New("recovery.hysteresis must be between 0 and 0.5"))
	}
	if c.Recovery.Cooldown <= 0 {
		errs = append(errs, errors.New("recovery.cooldown must be positive"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Validate checks the query configuration.
func (c *QueryConfig) Validate() error {
	var errs []error

	if c.Timeout <= 0 {
		errs = append(errs, errors.New("timeout must be positive"))
	}

	if c.MaxRows <= 0 {
		errs = append(errs, errors.New("max_rows must be positive"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// EnsureDirectories creates all required directories.
func (c *Config) EnsureDirectories() error {
	dirs := []string{
		c.DataDir,
		c.WALDir(),
		filepath.Join(c.DataDir, "raw"),
		filepath.Join(c.DataDir, "5min"),
		filepath.Join(c.DataDir, "hourly"),
		filepath.Join(c.DataDir, "daily"),
		filepath.Join(c.DataDir, "weekly"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	return nil
}

// WALDir returns the WAL directory path.
func (c *Config) WALDir() string {
	if c.Ingestion.WAL.Dir != "" {
		return c.Ingestion.WAL.Dir
	}
	return filepath.Join(c.DataDir, "wal")
}

// TierDir returns the directory path for a tier.
func (c *Config) TierDir(tier string) string {
	return filepath.Join(c.DataDir, tier)
}
