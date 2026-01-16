package aggregate

import (
	"sync"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

// Manager manages streaming aggregates for multiple pollers.
// It handles bucket transitions and flushing completed aggregates.
type Manager struct {
	mu sync.RWMutex

	// Configuration
	bucketSize        time.Duration
	percentileEnabled bool
	percentileAccuracy float64

	// Active aggregates: key -> aggregate
	// Key format: "namespace/target/poller"
	aggregates map[string]*StreamingAggregate

	// Completed aggregates waiting to be flushed
	completed []types.AggregateResult

	// Statistics
	stats ManagerStats
}

// ManagerStats holds statistics for the manager.
type ManagerStats struct {
	ActiveAggregates   int64
	CompletedPending   int64
	SamplesProcessed   int64
	BucketsCompleted   int64
	FlushesPerformed   int64
}

// NewManager creates a new aggregate manager.
func NewManager(bucketSize time.Duration, percentileEnabled bool) *Manager {
	return &Manager{
		bucketSize:         bucketSize,
		percentileEnabled:  percentileEnabled,
		percentileAccuracy: 0.01, // 1% default
		aggregates:         make(map[string]*StreamingAggregate),
		completed:          make([]types.AggregateResult, 0, 1000),
	}
}

// NewManagerWithAccuracy creates a manager with custom percentile accuracy.
func NewManagerWithAccuracy(bucketSize time.Duration, accuracy float64) *Manager {
	return &Manager{
		bucketSize:         bucketSize,
		percentileEnabled:  true,
		percentileAccuracy: accuracy,
		aggregates:         make(map[string]*StreamingAggregate),
		completed:          make([]types.AggregateResult, 0, 1000),
	}
}

// Process adds a sample to the appropriate aggregate.
// If the sample belongs to a new bucket, the old bucket is completed.
func (m *Manager) Process(sample types.Sample) {
	if !sample.Valid {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := sample.Key()
	bucketStart, bucketEnd := m.calculateBucket(sample.TimestampMs)

	agg, exists := m.aggregates[key]

	if !exists {
		// Create new aggregate
		agg = m.createAggregate(sample.Namespace, sample.Target, sample.Poller, bucketStart, bucketEnd)
		m.aggregates[key] = agg
	} else if bucketStart > agg.BucketStart() {
		// New bucket - complete the old one and create new
		if !agg.IsEmpty() {
			m.completed = append(m.completed, agg.Result())
			m.stats.BucketsCompleted++
		}
		agg.Reset(bucketStart, bucketEnd)
	}

	agg.AddSample(sample)
	m.stats.SamplesProcessed++
}

// ProcessBatch processes multiple samples efficiently.
func (m *Manager) ProcessBatch(samples []types.Sample) {
	for i := range samples {
		m.Process(samples[i])
	}
}

// FlushCompleted returns and clears all completed aggregates.
func (m *Manager) FlushCompleted() []types.AggregateResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.completed) == 0 {
		return nil
	}

	result := m.completed
	m.completed = make([]types.AggregateResult, 0, 1000)
	m.stats.FlushesPerformed++

	return result
}

// FlushAll completes all active aggregates and returns them.
// This is typically called during shutdown.
func (m *Manager) FlushAll() []types.AggregateResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Add all non-empty active aggregates to completed
	for _, agg := range m.aggregates {
		if !agg.IsEmpty() {
			m.completed = append(m.completed, agg.Result())
			m.stats.BucketsCompleted++
		}
	}

	// Clear active aggregates
	m.aggregates = make(map[string]*StreamingAggregate)

	// Return all completed
	result := m.completed
	m.completed = make([]types.AggregateResult, 0, 1000)
	m.stats.FlushesPerformed++

	return result
}

// FlushOlderThan completes aggregates with bucket start older than the given timestamp.
func (m *Manager) FlushOlderThan(cutoffMs int64) []types.AggregateResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	var flushed []types.AggregateResult

	for key, agg := range m.aggregates {
		if agg.BucketStart() < cutoffMs && !agg.IsEmpty() {
			flushed = append(flushed, agg.Result())
			m.stats.BucketsCompleted++
			delete(m.aggregates, key)
		}
	}

	return flushed
}

// ForceFlushBucket forces completion of aggregates for the given bucket.
func (m *Manager) ForceFlushBucket(bucketStartMs int64) []types.AggregateResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	var flushed []types.AggregateResult

	for _, agg := range m.aggregates {
		if agg.BucketStart() == bucketStartMs && !agg.IsEmpty() {
			flushed = append(flushed, agg.Result())
			m.stats.BucketsCompleted++
		}
	}

	return flushed
}

// Stats returns current statistics.
func (m *Manager) Stats() ManagerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := m.stats
	stats.ActiveAggregates = int64(len(m.aggregates))
	stats.CompletedPending = int64(len(m.completed))
	return stats
}

// ActiveCount returns the number of active aggregates.
func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.aggregates)
}

// CompletedCount returns the number of completed aggregates pending flush.
func (m *Manager) CompletedCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.completed)
}

// calculateBucket calculates the bucket start and end for a timestamp.
func (m *Manager) calculateBucket(timestampMs int64) (start, end int64) {
	bucketMs := m.bucketSize.Milliseconds()
	start = (timestampMs / bucketMs) * bucketMs
	end = start + bucketMs
	return
}

// createAggregate creates a new aggregate with the manager's settings.
func (m *Manager) createAggregate(namespace, target, poller string, bucketStart, bucketEnd int64) *StreamingAggregate {
	if m.percentileEnabled {
		return NewWithAccuracy(namespace, target, poller, bucketStart, bucketEnd, m.percentileAccuracy)
	}
	return New(namespace, target, poller, bucketStart, bucketEnd, false)
}

// CurrentBucketStart returns the current bucket start timestamp.
func (m *Manager) CurrentBucketStart() int64 {
	now := time.Now().UnixMilli()
	start, _ := m.calculateBucket(now)
	return start
}

// BucketSize returns the configured bucket size.
func (m *Manager) BucketSize() time.Duration {
	return m.bucketSize
}
