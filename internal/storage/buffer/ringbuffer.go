package buffer

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

// RingBuffer is a thread-safe circular buffer for samples.
// It uses a simple mutex-based approach for correctness.
// For higher throughput, consider a lock-free implementation.
type RingBuffer struct {
	mu       sync.RWMutex
	data     []types.Sample
	head     int64 // Next write position
	tail     int64 // Oldest data position
	count    int64 // Current number of elements
	capacity int64

	// Statistics
	pushCount atomic.Int64
	popCount  atomic.Int64
	dropCount atomic.Int64
}

// New creates a new RingBuffer with the given capacity.
func New(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 1024
	}
	return &RingBuffer{
		data:     make([]types.Sample, capacity),
		capacity: int64(capacity),
	}
}

// Push adds a sample to the buffer.
// Returns false if the buffer is full and the sample was dropped.
func (rb *RingBuffer) Push(sample types.Sample) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count >= rb.capacity {
		rb.dropCount.Add(1)
		return false
	}

	idx := rb.head % rb.capacity
	rb.data[idx] = sample
	rb.head++
	rb.count++
	rb.pushCount.Add(1)

	return true
}

// PushOverwrite adds a sample to the buffer, overwriting oldest if full.
func (rb *RingBuffer) PushOverwrite(sample types.Sample) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count >= rb.capacity {
		// Overwrite oldest
		rb.tail++
		rb.count--
		rb.dropCount.Add(1)
	}

	idx := rb.head % rb.capacity
	rb.data[idx] = sample
	rb.head++
	rb.count++
	rb.pushCount.Add(1)
}

// Pop removes and returns the oldest sample.
// Returns false if the buffer is empty.
func (rb *RingBuffer) Pop() (types.Sample, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return types.Sample{}, false
	}

	idx := rb.tail % rb.capacity
	sample := rb.data[idx]
	rb.data[idx] = types.Sample{} // Clear for GC
	rb.tail++
	rb.count--
	rb.popCount.Add(1)

	return sample, true
}

// PopN removes and returns up to n oldest samples.
func (rb *RingBuffer) PopN(n int) []types.Sample {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 || n <= 0 {
		return nil
	}

	count := int64(n)
	if count > rb.count {
		count = rb.count
	}

	result := make([]types.Sample, count)
	for i := int64(0); i < count; i++ {
		idx := (rb.tail + i) % rb.capacity
		result[i] = rb.data[idx]
		rb.data[idx] = types.Sample{}
	}

	rb.tail += count
	rb.count -= count
	rb.popCount.Add(count)

	return result
}

// Peek returns the oldest sample without removing it.
// Returns false if the buffer is empty.
func (rb *RingBuffer) Peek() (types.Sample, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return types.Sample{}, false
	}

	idx := rb.tail % rb.capacity
	return rb.data[idx], true
}

// PeekNewest returns the newest sample without removing it.
// Returns false if the buffer is empty.
func (rb *RingBuffer) PeekNewest() (types.Sample, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return types.Sample{}, false
	}

	idx := (rb.head - 1) % rb.capacity
	if idx < 0 {
		idx += rb.capacity
	}
	return rb.data[idx], true
}

// Len returns the current number of samples in the buffer.
func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return int(rb.count)
}

// Cap returns the capacity of the buffer.
func (rb *RingBuffer) Cap() int {
	return int(rb.capacity)
}

// IsEmpty returns true if the buffer is empty.
func (rb *RingBuffer) IsEmpty() bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count == 0
}

// IsFull returns true if the buffer is full.
func (rb *RingBuffer) IsFull() bool {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count >= rb.capacity
}

// UsageRatio returns the current usage as a ratio (0.0 - 1.0).
func (rb *RingBuffer) UsageRatio() float64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return float64(rb.count) / float64(rb.capacity)
}

// Clear removes all samples from the buffer.
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Clear all data for GC
	for i := range rb.data {
		rb.data[i] = types.Sample{}
	}

	rb.head = 0
	rb.tail = 0
	rb.count = 0
}

// Stats returns buffer statistics.
func (rb *RingBuffer) Stats() BufferStats {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	return BufferStats{
		Capacity:   int(rb.capacity),
		Count:      int(rb.count),
		UsageRatio: float64(rb.count) / float64(rb.capacity),
		PushCount:  rb.pushCount.Load(),
		PopCount:   rb.popCount.Load(),
		DropCount:  rb.dropCount.Load(),
	}
}

// BufferStats holds buffer statistics.
type BufferStats struct {
	Capacity   int
	Count      int
	UsageRatio float64
	PushCount  int64
	PopCount   int64
	DropCount  int64
}

// SampleFilter defines criteria for filtering samples.
type SampleFilter struct {
	Namespace string
	Target    string
	Poller    string
	Since     int64 // Unix milliseconds, 0 = no filter
	Until     int64 // Unix milliseconds, 0 = no filter
}

// Matches returns true if the sample matches the filter.
func (f *SampleFilter) Matches(s *types.Sample) bool {
	if f.Namespace != "" && s.Namespace != f.Namespace {
		return false
	}
	if f.Target != "" && s.Target != f.Target {
		return false
	}
	if f.Poller != "" && s.Poller != f.Poller {
		return false
	}
	if f.Since > 0 && s.TimestampMs < f.Since {
		return false
	}
	if f.Until > 0 && s.TimestampMs > f.Until {
		return false
	}
	return true
}

// Query returns samples matching the filter.
// Results are ordered from oldest to newest.
func (rb *RingBuffer) Query(filter SampleFilter, limit int) []types.Sample {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return nil
	}

	var results []types.Sample
	maxResults := limit
	if maxResults <= 0 {
		maxResults = int(rb.count)
	}

	// Iterate from oldest to newest
	for i := int64(0); i < rb.count && len(results) < maxResults; i++ {
		idx := (rb.tail + i) % rb.capacity
		sample := &rb.data[idx]
		if filter.Matches(sample) {
			results = append(results, *sample)
		}
	}

	return results
}

// QueryRange returns samples within the given time range.
// Results are ordered from oldest to newest.
func (rb *RingBuffer) QueryRange(startMs, endMs int64) []types.Sample {
	return rb.Query(SampleFilter{Since: startMs, Until: endMs}, 0)
}

// QueryPoller returns samples for a specific poller.
func (rb *RingBuffer) QueryPoller(namespace, target, poller string, limit int) []types.Sample {
	return rb.Query(SampleFilter{
		Namespace: namespace,
		Target:    target,
		Poller:    poller,
	}, limit)
}

// EvictOlderThan removes samples older than the given timestamp.
// Returns the number of samples evicted.
func (rb *RingBuffer) EvictOlderThan(cutoffMs int64) int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	evicted := 0
	for rb.count > 0 {
		idx := rb.tail % rb.capacity
		if rb.data[idx].TimestampMs >= cutoffMs {
			break
		}
		rb.data[idx] = types.Sample{}
		rb.tail++
		rb.count--
		evicted++
	}

	return evicted
}

// EvictToCapacity evicts oldest samples until usage is below the target ratio.
// Returns the number of samples evicted.
func (rb *RingBuffer) EvictToCapacity(targetRatio float64) int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	targetCount := int64(float64(rb.capacity) * targetRatio)
	evicted := 0

	for rb.count > targetCount {
		idx := rb.tail % rb.capacity
		rb.data[idx] = types.Sample{}
		rb.tail++
		rb.count--
		evicted++
	}

	return evicted
}

// TimeRange returns the time range of samples in the buffer.
// Returns (0, 0) if the buffer is empty.
func (rb *RingBuffer) TimeRange() (oldest, newest int64) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return 0, 0
	}

	oldestIdx := rb.tail % rb.capacity
	newestIdx := (rb.head - 1) % rb.capacity
	if newestIdx < 0 {
		newestIdx += rb.capacity
	}

	return rb.data[oldestIdx].TimestampMs, rb.data[newestIdx].TimestampMs
}

// Duration returns the time duration covered by samples in the buffer.
func (rb *RingBuffer) Duration() time.Duration {
	oldest, newest := rb.TimeRange()
	if oldest == 0 || newest == 0 {
		return 0
	}
	return time.Duration(newest-oldest) * time.Millisecond
}
