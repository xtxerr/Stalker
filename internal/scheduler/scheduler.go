// Package scheduler provides heap-based poll scheduling.
//
// The scheduler uses a min-heap to efficiently track when each poller
// is due to be polled. Workers execute polls concurrently and results
// are sent to a channel for processing.
//
// Key features:
//   - O(log n) add/remove/update operations
//   - Jitter on initial poll to prevent thundering herd
//   - Backpressure handling when workers are busy
//   - Graceful shutdown with drain timeout
package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/config"
	"github.com/xtxerr/stalker/internal/logging"
)

var log = logging.Component("scheduler")

// =============================================================================
// Types
// =============================================================================

// PollerKey uniquely identifies a poller.
type PollerKey struct {
	Namespace string
	Target    string
	Poller    string
}

// String returns the string representation of the key.
func (k PollerKey) String() string {
	return k.Namespace + "/" + k.Target + "/" + k.Poller
}

// PollJob represents a poll job to be executed.
type PollJob struct {
	Key PollerKey
}

// PollResult represents the result of a poll.
type PollResult struct {
	Key         PollerKey
	TimestampMs int64
	Success     bool
	Timeout     bool
	Error       string
	PollMs      int

	// Value (one of these is set)
	Counter *uint64
	Text    *string
	Gauge   *float64
}

// PollItem represents an item in the scheduler heap.
type PollItem struct {
	Key        PollerKey
	NextPollMs int64 // Unix ms when next poll is due
	IntervalMs int64 // Poll interval in ms
	Polling    bool  // Currently being polled
	deleted    bool  // Marked for deletion
	index      int   // Heap index for O(log n) updates
}

// =============================================================================
// Heap Implementation
// =============================================================================

// PollHeap implements heap.Interface for PollItems.
type PollHeap []*PollItem

func (h PollHeap) Len() int { return len(h) }

func (h PollHeap) Less(i, j int) bool {
	return h[i].NextPollMs < h[j].NextPollMs
}

func (h PollHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *PollHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*PollItem)
	item.index = n
	*h = append(*h, item)
}

func (h *PollHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.index = -1 // For safety
	*h = old[0 : n-1]
	return item
}

// Peek returns the top item without removing it.
func (h PollHeap) Peek() *PollItem {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

// =============================================================================
// Scheduler Configuration
// =============================================================================

// BackpressureDelayMs is the delay applied when the job queue is full.
// Extracted as constant to avoid magic numbers in code.
const BackpressureDelayMs = 1000

// Config holds scheduler configuration.
type Config struct {
	// Workers is the number of concurrent poll workers.
	Workers int

	// QueueSize is the job queue capacity.
	QueueSize int

	// ResultsSize is the results channel capacity.
	ResultsSize int

	// TickInterval is how often the scheduler checks for due polls.
	TickInterval time.Duration

	// DrainTimeout is how long to wait for in-flight polls during shutdown.
	// Similar to Kubernetes terminationGracePeriodSeconds.
	DrainTimeout time.Duration
}

// DefaultConfig returns default scheduler configuration.
func DefaultConfig() *Config {
	return &Config{
		Workers:      config.DefaultPollerWorkers,
		QueueSize:    config.DefaultPollerQueueSize,
		ResultsSize:  config.DefaultPollerQueueSize,
		TickInterval: config.DefaultSchedulerTickInterval,
		DrainTimeout: time.Duration(config.DefaultDrainTimeoutSec) * time.Second,
	}
}

// =============================================================================
// Scheduler
// =============================================================================

// Scheduler manages poll scheduling using a min-heap.
//
// Scheduler is safe for concurrent use.
type Scheduler struct {
	mu      sync.Mutex
	heap    PollHeap
	heapIdx map[string]*PollItem // key string -> item

	jobs    chan PollJob
	results chan PollResult

	pollFunc func(context.Context, PollerKey) PollResult

	shutdown chan struct{}
	wg       sync.WaitGroup

	// Worker tracking for graceful drain
	activeWorkers atomic.Int32

	// Wakeup signal for immediate processing
	wakeup chan struct{}

	// Configuration
	workers      int
	tickInterval time.Duration
	drainTimeout time.Duration

	// Metrics
	backpressure atomic.Int64
	pollsQueued  atomic.Int64
	pollsActive  atomic.Int64
}

// New creates a new Scheduler.
func New(cfg *Config) *Scheduler {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	return &Scheduler{
		heap:         make(PollHeap, 0),
		heapIdx:      make(map[string]*PollItem),
		jobs:         make(chan PollJob, cfg.QueueSize),
		results:      make(chan PollResult, cfg.ResultsSize),
		shutdown:     make(chan struct{}),
		wakeup:       make(chan struct{}, 1),
		workers:      cfg.Workers,
		tickInterval: cfg.TickInterval,
		drainTimeout: cfg.DrainTimeout,
	}
}

// SetPollFunc sets the function to execute polls.
func (s *Scheduler) SetPollFunc(fn func(context.Context, PollerKey) PollResult) {
	s.pollFunc = fn
}

// Results returns the results channel for reading poll results.
func (s *Scheduler) Results() <-chan PollResult {
	return s.results
}

// =============================================================================
// Lifecycle
// =============================================================================

// Start starts the scheduler.
func (s *Scheduler) Start() {
	// Start workers
	for i := 0; i < s.workers; i++ {
		s.wg.Add(1)
		go s.worker(context.Background())
	}

	// Start scheduler loop
	s.wg.Add(1)
	go s.scheduleLoop()

	log.Info("scheduler started", "workers", s.workers)
}

// Stop stops the scheduler gracefully, waiting for in-flight polls.
// Uses the configured drain timeout.
func (s *Scheduler) Stop() {
	s.StopWithContext(context.Background())
}

// StopWithContext stops the scheduler with a custom context.
// The drain timeout from config is still respected as a maximum.
func (s *Scheduler) StopWithContext(ctx context.Context) {
	log.Info("scheduler stopping")

	// Signal shutdown (stops accepting new jobs and schedule loop)
	close(s.shutdown)

	// Create drain context with timeout
	drainCtx, cancel := context.WithTimeout(ctx, s.drainTimeout)
	defer cancel()

	// Wait for workers with drain timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("scheduler stopped gracefully")
	case <-drainCtx.Done():
		activeCount := s.activeWorkers.Load()
		if activeCount > 0 {
			log.Warn("scheduler drain timeout",
				"active_workers", activeCount)
		} else {
			log.Info("scheduler stopped after drain timeout")
		}
	}

	// Close channels after workers have stopped or timeout
	close(s.jobs)
	close(s.results)
}

// =============================================================================
// Poller Management
// =============================================================================

// Add adds a new poller to the scheduler.
// The first poll is scheduled with random jitter to distribute load.
func (s *Scheduler) Add(key PollerKey, intervalMs uint32) {
	keyStr := key.String()
	interval := int64(intervalMs)
	jitter := rand.Int63n(interval) // Random 0 to interval

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already exists
	if _, ok := s.heapIdx[keyStr]; ok {
		return
	}

	item := &PollItem{
		Key:        key,
		NextPollMs: time.Now().UnixMilli() + jitter,
		IntervalMs: interval,
		Polling:    false,
	}

	heap.Push(&s.heap, item)
	s.heapIdx[keyStr] = item
	s.signalWakeup()

	log.Debug("poller added", "key", keyStr, "interval_ms", intervalMs)
}

// Remove removes a poller from the scheduler.
//
// New behavior:
//   - If not polling: remove from both heap and heapIdx immediately
//   - If polling: mark as deleted, keep in heapIdx so MarkComplete can clean up
func (s *Scheduler) Remove(key PollerKey) {
	keyStr := key.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.heapIdx[keyStr]
	if !ok {
		return
	}

	// Mark as deleted (in case currently polling)
	item.deleted = true

	if !item.Polling {
		// Not currently polling - safe to remove immediately
		if item.index >= 0 {
			heap.Remove(&s.heap, item.index)
		}
		delete(s.heapIdx, keyStr)
	}
	// If Polling=true, leave in heapIdx so MarkComplete can find and clean it up

	log.Debug("poller removed", "key", keyStr, "was_polling", item.Polling)
}

// UpdateInterval updates the polling interval for a poller.
func (s *Scheduler) UpdateInterval(key PollerKey, intervalMs uint32) {
	keyStr := key.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.heapIdx[keyStr]
	if !ok {
		return
	}

	item.IntervalMs = int64(intervalMs)

	log.Debug("poller interval updated", "key", keyStr, "interval_ms", intervalMs)
}

// Contains returns true if the poller is in the scheduler.
func (s *Scheduler) Contains(key PollerKey) bool {
	keyStr := key.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.heapIdx[keyStr]
	// Also check deleted flag - a deleted item shouldn't count as "contained"
	return ok && !item.deleted
}

// =============================================================================
// Schedule Loop
// =============================================================================

func (s *Scheduler) scheduleLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.processDueItems()
		case <-s.wakeup:
			s.processDueItems()
		case <-s.shutdown:
			return
		}
	}
}

func (s *Scheduler) processDueItems() {
	now := time.Now().UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.heap.Len() > 0 {
		next := s.heap.Peek()

		// Stop if next item isn't due yet
		if next.NextPollMs > now {
			break
		}

		// Pop the item first
		item := heap.Pop(&s.heap).(*PollItem)

		// Skip and clean up deleted items
		if item.deleted {
			keyStr := item.Key.String()
			delete(s.heapIdx, keyStr)
			continue
		}

		// Mark as polling
		item.Polling = true

		// Try to queue job
		select {
		case s.jobs <- PollJob{Key: item.Key}:
			s.pollsQueued.Add(1)
		default:
			// Queue full - reschedule with backpressure delay
			item.NextPollMs = now + BackpressureDelayMs
			item.Polling = false
			heap.Push(&s.heap, item)
			s.backpressure.Add(1)
		}
	}
}

// MarkComplete marks a poll as complete and reschedules it.
func (s *Scheduler) MarkComplete(key PollerKey) {
	keyStr := key.String()
	now := time.Now().UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.heapIdx[keyStr]
	if !ok {
		return // Already removed
	}

	if item.deleted {
		delete(s.heapIdx, keyStr)
		// Item is already out of heap (was popped in processDueItems)
		return
	}

	// Calculate next poll time
	item.NextPollMs = now + item.IntervalMs
	item.Polling = false

	// Add back to heap
	if item.index < 0 {
		heap.Push(&s.heap, item)
	} else {
		heap.Fix(&s.heap, item.index)
	}

	s.signalWakeup()
}

// =============================================================================
// Worker
// =============================================================================

func (s *Scheduler) worker(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case job, ok := <-s.jobs:
			if !ok {
				return // Channel closed
			}

			result := s.executeWithRecovery(ctx, job.Key)

			// Mark complete to reschedule
			s.MarkComplete(job.Key)

			// Send result
			select {
			case s.results <- result:
			case <-s.shutdown:
				return
			}

		case <-s.shutdown:
			return
		}
	}
}

// executeWithRecovery executes a poll with proper counter management and panic recovery.
//
func (s *Scheduler) executeWithRecovery(ctx context.Context, key PollerKey) (result PollResult) {
	s.activeWorkers.Add(1)
	s.pollsActive.Add(1)

	defer func() {
		s.pollsActive.Add(-1)
		s.activeWorkers.Add(-1)

		// Recover from panic and convert to error result
		if r := recover(); r != nil {
			log.Error("panic in poll execution",
				"key", key.String(),
				"panic", r)

			result = PollResult{
				Key:         key,
				TimestampMs: time.Now().UnixMilli(),
				Success:     false,
				Error:       fmt.Sprintf("panic: %v", r),
			}
		}
	}()

	// Create job context with timeout
	jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	result = s.executePoll(jobCtx, key)
	return result
}

func (s *Scheduler) executePoll(ctx context.Context, key PollerKey) PollResult {
	if s.pollFunc == nil {
		return PollResult{
			Key:         key,
			TimestampMs: time.Now().UnixMilli(),
			Success:     false,
			Error:       "no poll function configured",
		}
	}

	return s.pollFunc(ctx, key)
}

// =============================================================================
// Utility Methods
// =============================================================================

func (s *Scheduler) signalWakeup() {
	select {
	case s.wakeup <- struct{}{}:
	default:
		// Already signaled
	}
}

// Stats returns scheduler statistics.
func (s *Scheduler) Stats() (heapSize, queueUsed, active int, backpressure int64) {
	s.mu.Lock()
	heapSize = s.heap.Len()
	s.mu.Unlock()

	queueUsed = len(s.jobs)
	active = int(s.pollsActive.Load())
	backpressure = s.backpressure.Load()

	return
}

// GetScheduledPollers returns all scheduled poller keys.
func (s *Scheduler) GetScheduledPollers() []PollerKey {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]PollerKey, 0, len(s.heapIdx))
	for _, item := range s.heapIdx {
		if !item.deleted {
			keys = append(keys, item.Key)
		}
	}
	return keys
}

// GetNextPollTime returns the next poll time for a poller.
func (s *Scheduler) GetNextPollTime(key PollerKey) (time.Time, bool) {
	keyStr := key.String()

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.heapIdx[keyStr]
	if !ok || item.deleted {
		return time.Time{}, false
	}

	return time.UnixMilli(item.NextPollMs), true
}

// ActiveWorkerCount returns the number of currently active workers.
func (s *Scheduler) ActiveWorkerCount() int {
	return int(s.activeWorkers.Load())
}

// Count returns the number of scheduled pollers.
func (s *Scheduler) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := 0
	for _, item := range s.heapIdx {
		if !item.deleted {
			count++
		}
	}
	return count
}
