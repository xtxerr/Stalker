// Package manager provides business logic and entity management for stalker.
//
// This file implements the SyncManager which handles batched persistence
// of state, stats, and samples to the database.
//
package manager

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/config"
	"github.com/xtxerr/stalker/internal/logging"
	"github.com/xtxerr/stalker/internal/store"
)

var syncLog = logging.Component("sync")

// =============================================================================
// Sync Manager
// =============================================================================

// SyncManager handles batched persistence of state, stats, and samples.
//
// It provides buffered writes to improve database performance and includes
// retry logic for transient database errors.
//
// SyncManager is safe for concurrent use.
type SyncManager struct {
	store        *store.Store
	stateManager *StateManager
	statsManager *StatsManager

	// Sample buffer - protected by sampleMu
	sampleMu     sync.Mutex
	sampleBuffer []*store.Sample

	// Overflow buffer for samples that couldn't be written
	overflowMu     sync.Mutex
	overflowBuffer []*store.Sample

	// Configuration
	stateFlushInterval time.Duration
	statsFlushInterval time.Duration
	sampleBatchSize    int
	sampleFlushTimeout time.Duration
	maxRetries         int
	maxOverflowSize    int

	// Metrics
	samplesFlushed atomic.Int64
	samplesDropped atomic.Int64
	flushErrors    atomic.Int64
	overflowCount  atomic.Int64
	retryCount     atomic.Int64

	// Control
	shutdown chan struct{}
	wg       sync.WaitGroup
}

// SyncConfig holds sync manager configuration.
type SyncConfig struct {
	StateFlushInterval time.Duration
	StatsFlushInterval time.Duration
	SampleBatchSize    int
	SampleFlushTimeout time.Duration
	MaxRetries         int
	MaxOverflowSize    int
}

// DefaultSyncConfig returns default sync configuration.
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		StateFlushInterval: time.Duration(config.DefaultStateFlushIntervalSec) * time.Second,
		StatsFlushInterval: time.Duration(config.DefaultStatsFlushIntervalSec) * time.Second,
		SampleBatchSize:    config.DefaultSampleBatchSize,
		SampleFlushTimeout: time.Duration(config.DefaultSampleFlushTimeoutSec) * time.Second,
		MaxRetries:         config.DefaultMaxRetries,
		MaxOverflowSize:    config.DefaultMaxOverflowSize,
	}
}

// NewSyncManager creates a new sync manager.
func NewSyncManager(s *store.Store, stateMgr *StateManager, statsMgr *StatsManager, cfg *SyncConfig) *SyncManager {
	if cfg == nil {
		cfg = DefaultSyncConfig()
	}

	return &SyncManager{
		store:              s,
		stateManager:       stateMgr,
		statsManager:       statsMgr,
		sampleBuffer:       make([]*store.Sample, 0, cfg.SampleBatchSize),
		overflowBuffer:     make([]*store.Sample, 0),
		stateFlushInterval: cfg.StateFlushInterval,
		statsFlushInterval: cfg.StatsFlushInterval,
		sampleBatchSize:    cfg.SampleBatchSize,
		sampleFlushTimeout: cfg.SampleFlushTimeout,
		maxRetries:         cfg.MaxRetries,
		maxOverflowSize:    cfg.MaxOverflowSize,
		shutdown:           make(chan struct{}),
	}
}

// Start starts the sync manager background goroutines.
func (m *SyncManager) Start() {
	m.wg.Add(3)
	go m.stateFlushLoop()
	go m.statsFlushLoop()
	go m.sampleFlushLoop()
	syncLog.Info("sync manager started")
}

// Stop stops the sync manager and flushes all pending data.
func (m *SyncManager) Stop() {
	close(m.shutdown)
	m.wg.Wait()

	// Final flush with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m.flushStates(ctx)
	m.flushStats(ctx)
	m.flushSamplesWithRetry(ctx)
	m.flushOverflow(ctx)

	syncLog.Info("sync manager stopped",
		"samples_flushed", m.samplesFlushed.Load(),
		"samples_dropped", m.samplesDropped.Load(),
		"flush_errors", m.flushErrors.Load(),
		"retries", m.retryCount.Load())
}

// =============================================================================
// Sample Operations
// =============================================================================

// AddSample adds a sample to the buffer.
// If the buffer is full, it triggers a flush.
func (m *SyncManager) AddSample(sample *store.Sample) {
	m.AddSampleWithContext(context.Background(), sample)
}

// AddSampleWithContext adds a sample with context support for cancellation.
//
func (m *SyncManager) AddSampleWithContext(ctx context.Context, sample *store.Sample) error {
	// Check context before acquiring lock
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.sampleMu.Lock()
	defer m.sampleMu.Unlock()

	// Check context again after acquiring lock
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.sampleBuffer = append(m.sampleBuffer, sample)

	if len(m.sampleBuffer) >= m.sampleBatchSize {
		m.flushSamplesLocked(ctx)
	}

	return nil
}

// flushSamplesLocked flushes samples while the lock is already held.
//
func (m *SyncManager) flushSamplesLocked(ctx context.Context) {
	if len(m.sampleBuffer) == 0 {
		return
	}

	// Take ownership of buffer
	samples := m.sampleBuffer
	m.sampleBuffer = make([]*store.Sample, 0, m.sampleBatchSize)

	// Release lock while doing I/O
	m.sampleMu.Unlock()
	defer m.sampleMu.Lock()

	// Flush with retry
	err := m.insertSamplesWithRetry(ctx, samples)
	if err != nil {
		syncLog.Error("failed to flush samples", "error", err, "count", len(samples))
		m.moveToOverflow(samples)
		m.flushErrors.Add(1)
		return
	}

	m.samplesFlushed.Add(int64(len(samples)))
}

// =============================================================================
// Flush Loops
// =============================================================================

func (m *SyncManager) stateFlushLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.stateFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			m.flushStates(ctx)
			cancel()
		case <-m.shutdown:
			return
		}
	}
}

func (m *SyncManager) statsFlushLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.statsFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			m.flushStats(ctx)
			cancel()
		case <-m.shutdown:
			return
		}
	}
}

func (m *SyncManager) sampleFlushLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.sampleFlushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			m.flushSamplesWithRetry(ctx)
			m.flushOverflow(ctx)
			cancel()
		case <-m.shutdown:
			return
		}
	}
}

// =============================================================================
// =============================================================================

// flushStates flushes dirty poller states to the database.
//
func (m *SyncManager) flushStates(ctx context.Context) {
	if m.stateManager == nil {
		return
	}

	states := m.stateManager.GetDirtyStates()
	if len(states) == 0 {
		return
	}

	storeStates := make([]*store.PollerState, 0, len(states))
	for _, s := range states {
		storeStates = append(storeStates, s.ToStoreState())
	}

	err := m.store.BatchUpdatePollerStatesContext(ctx, storeStates)
	if err != nil {
		syncLog.Error("failed to flush states", "error", err, "count", len(states))
		m.flushErrors.Add(1)
		return
	}

	// Mark states as clean
	for _, s := range states {
		s.MarkClean()
	}

	syncLog.Debug("flushed poller states", "count", len(storeStates))
}

// flushStats flushes poller stats to the database.
//
func (m *SyncManager) flushStats(ctx context.Context) {
	if m.statsManager == nil {
		return
	}

	stats := m.statsManager.GetAllStats()
	if len(stats) == 0 {
		return
	}

	storeStats := make([]*store.PollerStatsRecord, 0, len(stats))
	for _, s := range stats {
		storeStats = append(storeStats, s.ToStoreStats())
	}

	err := m.store.BatchUpdatePollerStatsContext(ctx, storeStats)
	if err != nil {
		syncLog.Error("failed to flush stats", "error", err, "count", len(stats))
		m.flushErrors.Add(1)
		return
	}

	syncLog.Debug("flushed poller stats", "count", len(storeStats))
}

func (m *SyncManager) flushSamplesWithRetry(ctx context.Context) {
	m.sampleMu.Lock()
	if len(m.sampleBuffer) == 0 {
		m.sampleMu.Unlock()
		return
	}

	samples := m.sampleBuffer
	m.sampleBuffer = make([]*store.Sample, 0, m.sampleBatchSize)
	m.sampleMu.Unlock()

	err := m.insertSamplesWithRetry(ctx, samples)
	if err != nil {
		syncLog.Error("failed to flush samples after retries",
			"error", err,
			"count", len(samples))
		m.moveToOverflow(samples)
		m.flushErrors.Add(1)
		return
	}

	m.samplesFlushed.Add(int64(len(samples)))
}

// =============================================================================
// =============================================================================

// RetryConfig holds retry configuration for database operations.
type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

// DefaultRetryConfig returns sensible defaults for database retries.
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
	}
}

// insertSamplesWithRetry inserts samples with automatic retry on transient errors.
//
func (m *SyncManager) insertSamplesWithRetry(ctx context.Context, samples []*store.Sample) error {
	cfg := &RetryConfig{
		MaxRetries:     m.maxRetries,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		Multiplier:     2.0,
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		// Check context before attempt
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := m.store.InsertSamplesBatchContext(ctx, samples)
		if err == nil {
			return nil // Success
		}

		// Check if error is retryable
		if !isRetryableError(err) {
			return fmt.Errorf("non-retryable error: %w", err)
		}

		lastErr = err
		m.retryCount.Add(1)

		if attempt < cfg.MaxRetries {
			// Add jitter: ±25% of backoff
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			if rand.Intn(2) == 0 {
				jitter = -jitter
			}
			sleepTime := backoff + jitter

			syncLog.Warn("retrying sample insert",
				"attempt", attempt+1,
				"max_retries", cfg.MaxRetries,
				"backoff", sleepTime,
				"error", err)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleepTime):
			}

			// Increase backoff for next attempt
			backoff = time.Duration(float64(backoff) * cfg.Multiplier)
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// isRetryableError returns true if the error is transient and worth retrying.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	// DuckDB/SQLite transient errors
	retryablePatterns := []string{
		"database is locked",
		"busy",
		"timeout",
		"connection reset",
		"broken pipe",
		"temporary failure",
		"sqlite_busy",
		"sqlite_locked",
		"i/o error",
		"disk full",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// =============================================================================
// Overflow Buffer
// =============================================================================

func (m *SyncManager) moveToOverflow(samples []*store.Sample) {
	m.overflowMu.Lock()
	defer m.overflowMu.Unlock()

	// Check if overflow would exceed max size
	spaceAvailable := m.maxOverflowSize - len(m.overflowBuffer)
	if spaceAvailable <= 0 {
		// Drop oldest samples
		drop := len(samples)
		if drop > len(m.overflowBuffer) {
			drop = len(m.overflowBuffer)
		}
		m.overflowBuffer = m.overflowBuffer[drop:]
		m.samplesDropped.Add(int64(drop))
		syncLog.Warn("overflow buffer full, dropping samples", "dropped", drop)
	}

	// Add as many as we can
	toAdd := samples
	if len(toAdd) > spaceAvailable {
		toAdd = toAdd[:spaceAvailable]
		m.samplesDropped.Add(int64(len(samples) - spaceAvailable))
	}

	m.overflowBuffer = append(m.overflowBuffer, toAdd...)
	m.overflowCount.Store(int64(len(m.overflowBuffer)))
}

func (m *SyncManager) flushOverflow(ctx context.Context) {
	m.overflowMu.Lock()
	if len(m.overflowBuffer) == 0 {
		m.overflowMu.Unlock()
		return
	}

	samples := m.overflowBuffer
	m.overflowBuffer = make([]*store.Sample, 0)
	m.overflowCount.Store(0)
	m.overflowMu.Unlock()

	// Try to insert overflow samples
	err := m.insertSamplesWithRetry(ctx, samples)
	if err != nil {
		syncLog.Error("failed to flush overflow samples",
			"error", err,
			"count", len(samples))
		// Put back in overflow (but they might get dropped if still full)
		m.moveToOverflow(samples)
		return
	}

	m.samplesFlushed.Add(int64(len(samples)))
	syncLog.Info("flushed overflow samples", "count", len(samples))
}

// =============================================================================
// Metrics
// =============================================================================

// SyncMetrics contains sync manager statistics.
type SyncMetrics struct {
	SamplesFlushed int64
	SamplesDropped int64
	FlushErrors    int64
	OverflowCount  int64
	RetryCount     int64
}

// GetMetrics returns current sync manager metrics.
func (m *SyncManager) GetMetrics() SyncMetrics {
	return SyncMetrics{
		SamplesFlushed: m.samplesFlushed.Load(),
		SamplesDropped: m.samplesDropped.Load(),
		FlushErrors:    m.flushErrors.Load(),
		OverflowCount:  m.overflowCount.Load(),
		RetryCount:     m.retryCount.Load(),
	}
}
