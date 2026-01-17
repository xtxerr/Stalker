package types

import "time"

// ValueType indicates the type of metric value.
type ValueType int

const (
	// ValueTypeGauge is a point-in-time measurement (e.g., temperature, CPU usage).
	ValueTypeGauge ValueType = iota
	// ValueTypeCounter is a monotonically increasing counter (e.g., bytes received).
	// The Value field contains the calculated rate (delta / time).
	ValueTypeCounter
	// ValueTypeText is a string value (e.g., firmware version, status).
	ValueTypeText
)

// String returns a human-readable representation of the ValueType.
func (v ValueType) String() string {
	switch v {
	case ValueTypeGauge:
		return "gauge"
	case ValueTypeCounter:
		return "counter"
	case ValueTypeText:
		return "text"
	default:
		return "unknown"
	}
}

// Sample represents a single measurement from a poller.
// This is the primary data unit flowing through the storage system.
type Sample struct {
	// Identity
	Namespace string // Logical tenant separation (e.g., "prod", "dev")
	Target    string // Network device (e.g., "core-router-01")
	Poller    string // Metric name (e.g., "ifInOctets-Gi0-0")

	// Timestamp
	TimestampMs int64 // Unix timestamp in milliseconds

	// Value type indicator
	ValueType ValueType

	// Value - for counter metrics, this should be the calculated rate
	Value float64

	// TextValue holds the value for ValueTypeText samples
	TextValue string

	// Validity
	Valid bool   // True if the sample is valid
	Error string // Error message if invalid

	// Metadata
	PollMs int32 // Time taken for the poll in milliseconds
}

// TimestampTime returns the timestamp as a time.Time.
func (s *Sample) TimestampTime() time.Time {
	return time.UnixMilli(s.TimestampMs)
}

// Key returns a unique identifier for this sample's series.
func (s *Sample) Key() string {
	return s.Namespace + "/" + s.Target + "/" + s.Poller
}

// SampleBatch represents a collection of samples for batch processing.
type SampleBatch struct {
	Samples []Sample
}

// NewSampleBatch creates a new batch with the given capacity.
func NewSampleBatch(capacity int) *SampleBatch {
	return &SampleBatch{
		Samples: make([]Sample, 0, capacity),
	}
}

// Add appends a sample to the batch.
func (b *SampleBatch) Add(s Sample) {
	b.Samples = append(b.Samples, s)
}

// Len returns the number of samples in the batch.
func (b *SampleBatch) Len() int {
	return len(b.Samples)
}

// Clear resets the batch for reuse.
func (b *SampleBatch) Clear() {
	b.Samples = b.Samples[:0]
}
