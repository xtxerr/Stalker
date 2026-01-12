// Package manager provides business logic and entity management for stalker.
//
// This file defines the PollerStats type for tracking runtime statistics
// and the StatsManager for managing multiple poller statistics.
//
package manager

import (
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Poller Statistics
// =============================================================================

// PollerStats tracks runtime statistics for a poller.
//
// This type is the canonical definition used throughout the codebase.
// PollerStats is safe for concurrent use. Counters use atomic operations
// for lock-free updates, while timing statistics are protected by a mutex.
type PollerStats struct {
	Namespace string
	Target    string
	Poller    string

	// Counters - use atomic operations for lock-free updates
	PollsTotal   atomic.Int64
	PollsSuccess atomic.Int64
	PollsFailed  atomic.Int64
	PollsTimeout atomic.Int64

	// Timing statistics - protected by mu
	mu          sync.RWMutex
	pollMsSum   int64
	pollMsMin   int // -1 means not set
	pollMsMax   int
	pollMsCount int

	// Dirty flag for persistence
	dirty atomic.Bool
}

// NewPollerStats creates a new PollerStats for a poller.
func NewPollerStats(namespace, target, poller string) *PollerStats {
	return &PollerStats{
		Namespace: namespace,
		Target:    target,
		Poller:    poller,
		pollMsMin: -1, // not set
	}
}

// Key returns the unique key for this poller.
func (s *PollerStats) Key() string {
	return s.Namespace + "/" + s.Target + "/" + s.Poller
}

// RecordPoll records a poll result.
func (s *PollerStats) RecordPoll(success, timeout bool, pollMs int) {
	s.PollsTotal.Add(1)

	if success {
		s.PollsSuccess.Add(1)
	} else {
		s.PollsFailed.Add(1)
		if timeout {
			s.PollsTimeout.Add(1)
		}
	}

	// Update timing stats under lock
	s.mu.Lock()
	s.pollMsSum += int64(pollMs)
	s.pollMsCount++
	if s.pollMsMin < 0 || pollMs < s.pollMsMin {
		s.pollMsMin = pollMs
	}
	if pollMs > s.pollMsMax {
		s.pollMsMax = pollMs
	}
	s.mu.Unlock()

	s.dirty.Store(true)
}

// GetStats returns current statistics.
//
// Returns: total, success, failed, timeout counts and avg, min, max poll times in ms.
func (s *PollerStats) GetStats() (total, success, failed, timeout int64, avgMs, minMs, maxMs int) {
	total = s.PollsTotal.Load()
	success = s.PollsSuccess.Load()
	failed = s.PollsFailed.Load()
	timeout = s.PollsTimeout.Load()

	s.mu.RLock()
	if s.pollMsCount > 0 {
		avgMs = int(s.pollMsSum / int64(s.pollMsCount))
	}
	minMs = s.pollMsMin
	if minMs < 0 {
		minMs = 0
	}
	maxMs = s.pollMsMax
	s.mu.RUnlock()

	return
}

// GetAvgPollMs returns the average poll time in milliseconds.
func (s *PollerStats) GetAvgPollMs() float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.pollMsCount == 0 {
		return 0
	}
	return float64(s.pollMsSum) / float64(s.pollMsCount)
}

// GetMinPollMs returns the minimum poll time in milliseconds.
// Returns -1 if no polls have been recorded.
func (s *PollerStats) GetMinPollMs() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pollMsMin
}

// GetMaxPollMs returns the maximum poll time in milliseconds.
func (s *PollerStats) GetMaxPollMs() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pollMsMax
}

// GetPollCount returns the number of polls recorded for timing.
func (s *PollerStats) GetPollCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.pollMsCount
}

// IsDirty returns true if stats have changed since last flush.
func (s *PollerStats) IsDirty() bool {
	return s.dirty.Load()
}

// ClearDirty clears the dirty flag.
func (s *PollerStats) ClearDirty() {
	s.dirty.Store(false)
}

// Reset resets all statistics.
func (s *PollerStats) Reset() {
	s.PollsTotal.Store(0)
	s.PollsSuccess.Store(0)
	s.PollsFailed.Store(0)
	s.PollsTimeout.Store(0)

	s.mu.Lock()
	s.pollMsSum = 0
	s.pollMsMin = -1
	s.pollMsMax = 0
	s.pollMsCount = 0
	s.mu.Unlock()

	s.dirty.Store(true)
}

// =============================================================================
// Stats Manager
// =============================================================================

// StatsManager manages poller statistics in memory.
//
// StatsManager is safe for concurrent use.
type StatsManager struct {
	mu    sync.RWMutex
	stats map[string]*PollerStats // key: namespace/target/poller
}

// NewStatsManager creates a new stats manager.
func NewStatsManager() *StatsManager {
	return &StatsManager{
		stats: make(map[string]*PollerStats),
	}
}

func statsKey(namespace, target, poller string) string {
	return namespace + "/" + target + "/" + poller
}

// Get returns statistics for a poller, creating if needed.
func (m *StatsManager) Get(namespace, target, poller string) *PollerStats {
	key := statsKey(namespace, target, poller)

	// Fast path: read lock
	m.mu.RLock()
	stats, ok := m.stats[key]
	m.mu.RUnlock()

	if ok {
		return stats
	}

	// Slow path: write lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if stats, ok := m.stats[key]; ok {
		return stats
	}

	stats = NewPollerStats(namespace, target, poller)
	m.stats[key] = stats
	return stats
}

// GetIfExists returns statistics for a poller if they exist.
func (m *StatsManager) GetIfExists(namespace, target, poller string) *PollerStats {
	key := statsKey(namespace, target, poller)

	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.stats[key]
}

// Remove removes statistics for a poller.
func (m *StatsManager) Remove(namespace, target, poller string) {
	key := statsKey(namespace, target, poller)

	m.mu.Lock()
	delete(m.stats, key)
	m.mu.Unlock()
}

// RemoveForTarget removes statistics for all pollers in a target.
func (m *StatsManager) RemoveForTarget(namespace, target string) {
	prefix := namespace + "/" + target + "/"

	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.stats {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			delete(m.stats, key)
		}
	}
}

// GetDirty returns all dirty statistics.
//
// The returned slice contains references to the actual stats objects.
// Callers should not modify the stats without proper synchronization.
func (m *StatsManager) GetDirty() []*PollerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Pre-allocate with estimated capacity (10% dirty)
	estimatedSize := len(m.stats) / 10
	if estimatedSize < 10 {
		estimatedSize = 10
	}

	dirty := make([]*PollerStats, 0, estimatedSize)
	for _, stats := range m.stats {
		if stats.IsDirty() {
			dirty = append(dirty, stats)
		}
	}
	return dirty
}

// GetAll returns all statistics.
func (m *StatsManager) GetAll() []*PollerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*PollerStats, 0, len(m.stats))
	for _, stats := range m.stats {
		result = append(result, stats)
	}
	return result
}

// Count returns the number of tracked pollers.
func (m *StatsManager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.stats)
}

// Aggregate returns aggregated counters for all pollers.
func (m *StatsManager) Aggregate() (total, success, failed, timeout int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, stats := range m.stats {
		total += stats.PollsTotal.Load()
		success += stats.PollsSuccess.Load()
		failed += stats.PollsFailed.Load()
		timeout += stats.PollsTimeout.Load()
	}
	return
}

// AggregateByNamespace returns aggregated stats for a namespace.
func (m *StatsManager) AggregateByNamespace(namespace string) (total, success, failed int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, stats := range m.stats {
		if stats.Namespace == namespace {
			total += stats.PollsTotal.Load()
			success += stats.PollsSuccess.Load()
			failed += stats.PollsFailed.Load()
		}
	}
	return
}

// AggregateByTarget returns aggregated stats for a target.
func (m *StatsManager) AggregateByTarget(namespace, target string) (total, success, failed int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, stats := range m.stats {
		if stats.Namespace == namespace && stats.Target == target {
			total += stats.PollsTotal.Load()
			success += stats.PollsSuccess.Load()
			failed += stats.PollsFailed.Load()
		}
	}
	return
}

// =============================================================================
// Aggregated Statistics
// =============================================================================

// AggregatedStats holds aggregated statistics across multiple pollers.
type AggregatedStats struct {
	PollerCount  int
	PollsTotal   int64
	PollsSuccess int64
	PollsFailed  int64
	PollsTimeout int64
	AvgPollMs    float64
	Since        time.Time
}

// GetAggregatedStats returns aggregated statistics for all pollers.
func (m *StatsManager) GetAggregatedStats() AggregatedStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var agg AggregatedStats
	agg.PollerCount = len(m.stats)
	agg.Since = time.Now()

	var totalPollMs int64
	var pollCount int

	for _, stats := range m.stats {
		agg.PollsTotal += stats.PollsTotal.Load()
		agg.PollsSuccess += stats.PollsSuccess.Load()
		agg.PollsFailed += stats.PollsFailed.Load()
		agg.PollsTimeout += stats.PollsTimeout.Load()

		stats.mu.RLock()
		totalPollMs += stats.pollMsSum
		pollCount += stats.pollMsCount
		stats.mu.RUnlock()
	}

	if pollCount > 0 {
		agg.AvgPollMs = float64(totalPollMs) / float64(pollCount)
	}

	return agg
}
