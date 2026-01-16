package types

import "time"

// AggregateResult represents aggregated statistics for a time bucket.
// This is the output of the streaming aggregation process.
type AggregateResult struct {
	// Identity
	Namespace string // Logical tenant separation
	Target    string // Network device
	Poller    string // Metric name

	// Time bucket
	BucketStart int64 // Unix timestamp in milliseconds (bucket start)
	BucketEnd   int64 // Unix timestamp in milliseconds (bucket end)

	// Basic statistics (always present)
	Count int64   // Number of samples in this bucket
	Sum   float64 // Sum of all values
	Min   float64 // Minimum value
	Max   float64 // Maximum value
	Avg   float64 // Average value (Sum / Count)

	// Percentiles (optional, nil if not enabled)
	P50 *float64 // 50th percentile (median)
	P90 *float64 // 90th percentile
	P95 *float64 // 95th percentile
	P99 *float64 // 99th percentile

	// Timestamps of actual samples
	FirstTs int64 // Timestamp of first sample in bucket
	LastTs  int64 // Timestamp of last sample in bucket
}

// Key returns a unique identifier for this aggregate's series.
func (a *AggregateResult) Key() string {
	return a.Namespace + "/" + a.Target + "/" + a.Poller
}

// BucketStartTime returns the bucket start as a time.Time.
func (a *AggregateResult) BucketStartTime() time.Time {
	return time.UnixMilli(a.BucketStart)
}

// BucketEndTime returns the bucket end as a time.Time.
func (a *AggregateResult) BucketEndTime() time.Time {
	return time.UnixMilli(a.BucketEnd)
}

// Duration returns the bucket duration.
func (a *AggregateResult) Duration() time.Duration {
	return time.Duration(a.BucketEnd-a.BucketStart) * time.Millisecond
}

// IsEmpty returns true if no samples were aggregated.
func (a *AggregateResult) IsEmpty() bool {
	return a.Count == 0
}

// HasPercentiles returns true if percentile data is available.
func (a *AggregateResult) HasPercentiles() bool {
	return a.P50 != nil
}

// SetPercentiles sets all percentile values.
func (a *AggregateResult) SetPercentiles(p50, p90, p95, p99 float64) {
	a.P50 = &p50
	a.P90 = &p90
	a.P95 = &p95
	a.P99 = &p99
}

// AggregateBatch represents a collection of aggregate results.
type AggregateBatch struct {
	Results []AggregateResult
}

// NewAggregateBatch creates a new batch with the given capacity.
func NewAggregateBatch(capacity int) *AggregateBatch {
	return &AggregateBatch{
		Results: make([]AggregateResult, 0, capacity),
	}
}

// Add appends an aggregate result to the batch.
func (b *AggregateBatch) Add(r AggregateResult) {
	b.Results = append(b.Results, r)
}

// Len returns the number of results in the batch.
func (b *AggregateBatch) Len() int {
	return len(b.Results)
}

// Clear resets the batch for reuse.
func (b *AggregateBatch) Clear() {
	b.Results = b.Results[:0]
}
