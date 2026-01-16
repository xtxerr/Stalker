package config

import (
	"fmt"
	"time"
)

// Requirements represents calculated resource requirements.
type Requirements struct {
	// Memory requirements
	RawBufferBytes      int64
	AggregateBufferBytes int64
	QueryCacheBytes     int64
	TotalRAMBytes       int64

	// Storage requirements per tier
	RawStorageBytes     int64
	FiveMinStorageBytes int64
	HourlyStorageBytes  int64
	DailyStorageBytes   int64
	WeeklyStorageBytes  int64
	TotalStorageBytes   int64

	// Throughput
	SamplesPerSecond    int64
	BytesPerSecond      int64
	AggregatesPerDay    int64

	// CPU estimate
	RecommendedCPUCores int
}

// Constants for calculations
const (
	// Bytes per sample (in-memory)
	bytesPerSample = 25

	// Bytes per aggregate (in-memory, without DDSketch)
	bytesPerAggregate = 80

	// Bytes per aggregate (in-memory, with DDSketch)
	bytesPerAggregateWithSketch = 150

	// Compression ratio for Parquet
	compressionRatio = 5

	// Bytes per row in Parquet (compressed)
	bytesPerParquetRowCompressed = 25

	// Bytes per raw row in Parquet (compressed)
	bytesPerRawRowCompressed = 15
)

// CalculateRequirements computes resource requirements based on configuration.
func (c *Config) CalculateRequirements() Requirements {
	r := Requirements{}

	// Calculate samples per second
	r.SamplesPerSecond = int64(c.Scale.PollerCount) / int64(c.Scale.PollIntervalSec)

	// Raw bytes per second (uncompressed)
	r.BytesPerSecond = r.SamplesPerSecond * bytesPerSample

	// -------------------------------------------------------------------------
	// Memory Requirements
	// -------------------------------------------------------------------------

	// Raw buffer memory
	if c.Features.RawBuffer.Enabled && c.Features.RawBuffer.Duration > 0 {
		durationSec := int64(c.Features.RawBuffer.Duration / time.Second)
		samplesInBuffer := r.SamplesPerSecond * durationSec
		r.RawBufferBytes = samplesInBuffer * bytesPerSample
	}

	// Aggregate buffer memory (one aggregate per poller for current bucket)
	bytesPerAgg := int64(bytesPerAggregate)
	if c.Features.Percentile.Enabled {
		bytesPerAgg = bytesPerAggregateWithSketch
	}
	r.AggregateBufferBytes = int64(c.Scale.PollerCount) * bytesPerAgg

	// Query cache (from config or default)
	r.QueryCacheBytes = parseMemoryLimit(c.Query.MemoryLimit)

	// Total RAM
	r.TotalRAMBytes = r.RawBufferBytes + r.AggregateBufferBytes + r.QueryCacheBytes
	// Add 2GB for OS and Go runtime
	r.TotalRAMBytes += 2 * 1024 * 1024 * 1024

	// -------------------------------------------------------------------------
	// Storage Requirements
	// -------------------------------------------------------------------------

	// Samples per day
	samplesPerDay := r.SamplesPerSecond * 86400

	// Raw tier: samples * retention
	rawRetentionDays := float64(c.Retention.Raw) / float64(24*time.Hour)
	r.RawStorageBytes = int64(float64(samplesPerDay) * bytesPerRawRowCompressed * rawRetentionDays)

	// 5min tier: 288 buckets per day * pollers * retention
	fiveMinBucketsPerDay := int64(288)
	fiveMinRetentionDays := float64(c.Retention.FiveMin) / float64(24*time.Hour)
	r.FiveMinStorageBytes = int64(float64(fiveMinBucketsPerDay*int64(c.Scale.PollerCount)) * bytesPerParquetRowCompressed * fiveMinRetentionDays)

	// Hourly tier: 24 buckets per day * pollers * retention
	hourlyBucketsPerDay := int64(24)
	hourlyRetentionDays := float64(c.Retention.Hourly) / float64(24*time.Hour)
	r.HourlyStorageBytes = int64(float64(hourlyBucketsPerDay*int64(c.Scale.PollerCount)) * bytesPerParquetRowCompressed * hourlyRetentionDays)

	// Daily tier: 1 bucket per day * pollers * retention
	dailyRetentionDays := float64(c.Retention.Daily) / float64(24*time.Hour)
	r.DailyStorageBytes = int64(float64(int64(c.Scale.PollerCount)) * bytesPerParquetRowCompressed * dailyRetentionDays)

	// Weekly tier: 1/7 bucket per day * pollers * retention
	weeklyRetentionDays := float64(c.Retention.Weekly) / float64(24*time.Hour)
	r.WeeklyStorageBytes = int64(float64(int64(c.Scale.PollerCount)) * bytesPerParquetRowCompressed * weeklyRetentionDays / 7)

	// Total storage
	r.TotalStorageBytes = r.RawStorageBytes + r.FiveMinStorageBytes + r.HourlyStorageBytes + r.DailyStorageBytes + r.WeeklyStorageBytes

	// -------------------------------------------------------------------------
	// CPU Requirements
	// -------------------------------------------------------------------------

	// Rough estimate: 1 core per 100k samples/sec for ingestion
	// Plus cores for compaction
	ingestCores := int(r.SamplesPerSecond/100000) + 1
	compactCores := c.Compaction.Workers
	r.RecommendedCPUCores = ingestCores + compactCores

	// Aggregates per day
	r.AggregatesPerDay = fiveMinBucketsPerDay * int64(c.Scale.PollerCount)

	return r
}

// FormatRequirements returns a human-readable summary of requirements.
func (r *Requirements) FormatRequirements() string {
	return fmt.Sprintf(`Resource Requirements
=====================

Throughput:
  Samples/sec:       %s
  Bytes/sec:         %s
  Aggregates/day:    %s

Memory:
  Raw Buffer:        %s
  Aggregate Buffer:  %s
  Query Cache:       %s
  Total RAM:         %s (recommended)

Storage:
  Raw Tier:          %s
  5min Tier:         %s
  Hourly Tier:       %s
  Daily Tier:        %s
  Weekly Tier:       %s
  Total Storage:     %s (recommended)

CPU:
  Recommended Cores: %d
`,
		formatNumber(r.SamplesPerSecond),
		formatBytes(r.BytesPerSecond),
		formatNumber(r.AggregatesPerDay),
		formatBytes(r.RawBufferBytes),
		formatBytes(r.AggregateBufferBytes),
		formatBytes(r.QueryCacheBytes),
		formatBytes(r.TotalRAMBytes),
		formatBytes(r.RawStorageBytes),
		formatBytes(r.FiveMinStorageBytes),
		formatBytes(r.HourlyStorageBytes),
		formatBytes(r.DailyStorageBytes),
		formatBytes(r.WeeklyStorageBytes),
		formatBytes(r.TotalStorageBytes),
		r.RecommendedCPUCores,
	)
}

// parseMemoryLimit parses a memory limit string like "2GB" into bytes.
func parseMemoryLimit(s string) int64 {
	if s == "" {
		return 2 * 1024 * 1024 * 1024 // Default 2GB
	}

	var value int64
	var unit string
	_, err := fmt.Sscanf(s, "%d%s", &value, &unit)
	if err != nil {
		// Try without space
		for i, c := range s {
			if c < '0' || c > '9' {
				fmt.Sscanf(s[:i], "%d", &value)
				unit = s[i:]
				break
			}
		}
	}

	switch unit {
	case "B", "b", "":
		return value
	case "KB", "kb", "K", "k":
		return value * 1024
	case "MB", "mb", "M", "m":
		return value * 1024 * 1024
	case "GB", "gb", "G", "g":
		return value * 1024 * 1024 * 1024
	case "TB", "tb", "T", "t":
		return value * 1024 * 1024 * 1024 * 1024
	default:
		return value
	}
}

// formatBytes formats bytes as a human-readable string.
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
		TB = 1024 * GB
	)

	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatNumber formats a number with thousand separators.
func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	if n < 1000000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	return fmt.Sprintf("%.1fB", float64(n)/1000000000)
}
