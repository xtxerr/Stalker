package aggregate

import (
	"math"
	"sync"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// StreamingAggregate maintains running statistics for a single time bucket.
// It supports optional percentile calculation using DDSketch.
type StreamingAggregate struct {
	mu sync.Mutex

	// Identity
	namespace string
	target    string
	poller    string

	// Time bucket
	bucketStart int64 // Unix milliseconds
	bucketEnd   int64 // Unix milliseconds

	// Running statistics
	count   int64
	sum     float64
	min     float64
	max     float64
	firstTs int64
	lastTs  int64

	// DDSketch for percentiles (nil if disabled)
	sketch *ddsketch.DDSketch

	// Whether percentiles are enabled
	percentileEnabled bool
}

// New creates a new StreamingAggregate for the given bucket.
func New(namespace, target, poller string, bucketStart, bucketEnd int64, enablePercentile bool) *StreamingAggregate {
	agg := &StreamingAggregate{
		namespace:         namespace,
		target:            target,
		poller:            poller,
		bucketStart:       bucketStart,
		bucketEnd:         bucketEnd,
		min:               math.MaxFloat64,
		max:               -math.MaxFloat64,
		percentileEnabled: enablePercentile,
	}

	if enablePercentile {
		// Create DDSketch with default relative accuracy of 1%
		sketch, err := ddsketch.NewDefaultDDSketch(0.01)
		if err == nil {
			agg.sketch = sketch
		}
	}

	return agg
}

// NewWithAccuracy creates a new StreamingAggregate with custom percentile accuracy.
func NewWithAccuracy(namespace, target, poller string, bucketStart, bucketEnd int64, accuracy float64) *StreamingAggregate {
	agg := &StreamingAggregate{
		namespace:         namespace,
		target:            target,
		poller:            poller,
		bucketStart:       bucketStart,
		bucketEnd:         bucketEnd,
		min:               math.MaxFloat64,
		max:               -math.MaxFloat64,
		percentileEnabled: true,
	}

	sketch, err := ddsketch.NewDefaultDDSketch(accuracy)
	if err == nil {
		agg.sketch = sketch
	}

	return agg
}

// Add adds a value to the aggregate.
func (a *StreamingAggregate) Add(value float64, timestampMs int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.count++
	a.sum += value

	if value < a.min {
		a.min = value
	}
	if value > a.max {
		a.max = value
	}

	if a.firstTs == 0 || timestampMs < a.firstTs {
		a.firstTs = timestampMs
	}
	if timestampMs > a.lastTs {
		a.lastTs = timestampMs
	}

	if a.sketch != nil {
		a.sketch.Add(value)
	}
}

// AddSample adds a sample to the aggregate.
func (a *StreamingAggregate) AddSample(s types.Sample) {
	if !s.Valid {
		return
	}
	a.Add(s.Value, s.TimestampMs)
}

// Count returns the number of samples added.
func (a *StreamingAggregate) Count() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count
}

// IsEmpty returns true if no samples have been added.
func (a *StreamingAggregate) IsEmpty() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.count == 0
}

// Result returns the aggregation result.
func (a *StreamingAggregate) Result() types.AggregateResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	result := types.AggregateResult{
		Namespace:   a.namespace,
		Target:      a.target,
		Poller:      a.poller,
		BucketStart: a.bucketStart,
		BucketEnd:   a.bucketEnd,
		Count:       a.count,
		Sum:         a.sum,
		FirstTs:     a.firstTs,
		LastTs:      a.lastTs,
	}

	if a.count > 0 {
		result.Avg = a.sum / float64(a.count)
		result.Min = a.min
		result.Max = a.max
	}

	// Calculate percentiles if enabled and we have data
	if a.sketch != nil && a.count > 0 {
		p50, _ := a.sketch.GetValueAtQuantile(0.50)
		p90, _ := a.sketch.GetValueAtQuantile(0.90)
		p95, _ := a.sketch.GetValueAtQuantile(0.95)
		p99, _ := a.sketch.GetValueAtQuantile(0.99)
		result.SetPercentiles(p50, p90, p95, p99)
	}

	return result
}

// Reset resets the aggregate for a new bucket.
func (a *StreamingAggregate) Reset(bucketStart, bucketEnd int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.bucketStart = bucketStart
	a.bucketEnd = bucketEnd
	a.count = 0
	a.sum = 0
	a.min = math.MaxFloat64
	a.max = -math.MaxFloat64
	a.firstTs = 0
	a.lastTs = 0

	if a.sketch != nil {
		// Create a new sketch (DDSketch doesn't have a Clear method)
		newSketch, err := ddsketch.NewDefaultDDSketch(0.01)
		if err == nil {
			a.sketch = newSketch
		}
	}
}

// Merge combines another aggregate into this one.
// Both aggregates must be for the same time bucket.
func (a *StreamingAggregate) Merge(other *StreamingAggregate) {
	if other == nil || other.count == 0 {
		return
	}

	a.mu.Lock()
	other.mu.Lock()
	defer a.mu.Unlock()
	defer other.mu.Unlock()

	a.count += other.count
	a.sum += other.sum

	if other.min < a.min {
		a.min = other.min
	}
	if other.max > a.max {
		a.max = other.max
	}

	if a.firstTs == 0 || (other.firstTs != 0 && other.firstTs < a.firstTs) {
		a.firstTs = other.firstTs
	}
	if other.lastTs > a.lastTs {
		a.lastTs = other.lastTs
	}

	// Merge sketches
	if a.sketch != nil && other.sketch != nil {
		a.sketch.MergeWith(other.sketch)
	}
}

// BucketStart returns the bucket start timestamp.
func (a *StreamingAggregate) BucketStart() int64 {
	return a.bucketStart
}

// BucketEnd returns the bucket end timestamp.
func (a *StreamingAggregate) BucketEnd() int64 {
	return a.bucketEnd
}

// Key returns the unique key for this aggregate's series.
func (a *StreamingAggregate) Key() string {
	return a.namespace + "/" + a.target + "/" + a.poller
}

// BucketDuration returns the bucket duration.
func (a *StreamingAggregate) BucketDuration() time.Duration {
	return time.Duration(a.bucketEnd-a.bucketStart) * time.Millisecond
}
