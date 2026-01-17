package proto

import (
	"github.com/xtxerr/stalker/internal/storage/backpressure"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// ============================================================================
// Proto Types (compatible with generated protobuf types)
// These will be replaced by actual generated types after running 'make proto'
// ============================================================================

// ProtoSample matches the proto Sample message.
type ProtoSample struct {
	Namespace    string
	Target       string
	Poller       string
	TimestampMs  int64
	ValueCounter uint64
	ValueText    string
	ValueGauge   float64
	Valid        bool
	Error        string
	PollMs       int32
}

// ProtoSampleBatch matches the proto SampleBatch message.
type ProtoSampleBatch struct {
	Samples      []ProtoSample
	ReceivedAtMs int64
}

// ProtoAggregateResult matches the proto AggregateResult message.
type ProtoAggregateResult struct {
	Namespace     string
	Target        string
	Poller        string
	BucketStartMs int64
	BucketEndMs   int64
	Count         int64
	Sum           float64
	Min           float64
	Max           float64
	Avg           float64
	P50           *float64
	P90           *float64
	P95           *float64
	P99           *float64
	FirstTsMs     int64
	LastTsMs      int64
}

// ProtoStorageTier matches the proto StorageTier enum.
type ProtoStorageTier int32

const (
	StorageTierRaw    ProtoStorageTier = 0
	StorageTier5Min   ProtoStorageTier = 1
	StorageTierHourly ProtoStorageTier = 2
	StorageTierDaily  ProtoStorageTier = 3
	StorageTierWeekly ProtoStorageTier = 4
)

// ProtoBackpressureLevel matches the proto BackpressureLevel enum.
type ProtoBackpressureLevel int32

const (
	BackpressureNormal    ProtoBackpressureLevel = 0
	BackpressureWarning   ProtoBackpressureLevel = 1
	BackpressureCritical  ProtoBackpressureLevel = 2
	BackpressureEmergency ProtoBackpressureLevel = 3
)

// ProtoStorageStats matches the proto StorageStats message.
type ProtoStorageStats struct {
	Running            bool
	UptimeMs           int64
	SamplesReceived    int64
	SamplesIngested    int64
	SamplesDropped     int64
	BatchesProcessed   int64
	AggregatesWritten  int64
	FlushesCompleted   int64
	BufferUsageRatio   float64
	BufferCount        int32
	BackpressureLevel  ProtoBackpressureLevel
}

// ============================================================================
// Sample Conversion
// ============================================================================

// SampleToProto converts a storage Sample to a proto Sample.
func SampleToProto(s types.Sample) ProtoSample {
	p := ProtoSample{
		Namespace:   s.Namespace,
		Target:      s.Target,
		Poller:      s.Poller,
		TimestampMs: s.TimestampMs,
		Valid:       s.Valid,
		Error:       s.Error,
		PollMs:      s.PollMs,
	}

	// Set the appropriate value field based on type
	switch s.ValueType {
	case types.ValueTypeCounter:
		p.ValueCounter = uint64(s.Value)
	case types.ValueTypeText:
		p.ValueText = s.TextValue
	default: // ValueTypeGauge
		p.ValueGauge = s.Value
	}

	return p
}

// SampleFromProto converts a proto Sample to a storage Sample.
func SampleFromProto(p ProtoSample) types.Sample {
	s := types.Sample{
		Namespace:   p.Namespace,
		Target:      p.Target,
		Poller:      p.Poller,
		TimestampMs: p.TimestampMs,
		Valid:       p.Valid,
		Error:       p.Error,
		PollMs:      p.PollMs,
	}

	// Determine value type based on which proto field is set.
	// Priority: Counter > Text > Gauge (default)
	if p.ValueCounter != 0 {
		s.ValueType = types.ValueTypeCounter
		s.Value = float64(p.ValueCounter)
	} else if p.ValueText != "" {
		s.ValueType = types.ValueTypeText
		s.TextValue = p.ValueText
	} else {
		s.ValueType = types.ValueTypeGauge
		s.Value = p.ValueGauge
	}

	return s
}

// SamplesToProto converts a slice of storage Samples to proto Samples.
func SamplesToProto(samples []types.Sample) []ProtoSample {
	result := make([]ProtoSample, len(samples))
	for i, s := range samples {
		result[i] = SampleToProto(s)
	}
	return result
}

// SamplesFromProto converts a slice of proto Samples to storage Samples.
func SamplesFromProto(protos []ProtoSample) []types.Sample {
	result := make([]types.Sample, len(protos))
	for i, p := range protos {
		result[i] = SampleFromProto(p)
	}
	return result
}

// SampleBatchToProto converts a slice of samples to a proto SampleBatch.
func SampleBatchToProto(samples []types.Sample, receivedAtMs int64) ProtoSampleBatch {
	return ProtoSampleBatch{
		Samples:      SamplesToProto(samples),
		ReceivedAtMs: receivedAtMs,
	}
}

// SampleBatchFromProto converts a proto SampleBatch to a slice of samples.
func SampleBatchFromProto(batch ProtoSampleBatch) []types.Sample {
	return SamplesFromProto(batch.Samples)
}

// ============================================================================
// AggregateResult Conversion
// ============================================================================

// AggregateToProto converts a storage AggregateResult to a proto AggregateResult.
func AggregateToProto(a types.AggregateResult) ProtoAggregateResult {
	p := ProtoAggregateResult{
		Namespace:     a.Namespace,
		Target:        a.Target,
		Poller:        a.Poller,
		BucketStartMs: a.BucketStart,
		BucketEndMs:   a.BucketEnd,
		Count:         a.Count,
		Sum:           a.Sum,
		Min:           a.Min,
		Max:           a.Max,
		Avg:           a.Avg,
		FirstTsMs:     a.FirstTs,
		LastTsMs:      a.LastTs,
	}

	// Copy percentiles if present
	if a.P50 != nil {
		v := *a.P50
		p.P50 = &v
	}
	if a.P90 != nil {
		v := *a.P90
		p.P90 = &v
	}
	if a.P95 != nil {
		v := *a.P95
		p.P95 = &v
	}
	if a.P99 != nil {
		v := *a.P99
		p.P99 = &v
	}

	return p
}

// AggregateFromProto converts a proto AggregateResult to a storage AggregateResult.
func AggregateFromProto(p ProtoAggregateResult) types.AggregateResult {
	a := types.AggregateResult{
		Namespace:   p.Namespace,
		Target:      p.Target,
		Poller:      p.Poller,
		BucketStart: p.BucketStartMs,
		BucketEnd:   p.BucketEndMs,
		Count:       p.Count,
		Sum:         p.Sum,
		Min:         p.Min,
		Max:         p.Max,
		Avg:         p.Avg,
		FirstTs:     p.FirstTsMs,
		LastTs:      p.LastTsMs,
	}

	// Copy percentiles if present
	if p.P50 != nil {
		v := *p.P50
		a.P50 = &v
	}
	if p.P90 != nil {
		v := *p.P90
		a.P90 = &v
	}
	if p.P95 != nil {
		v := *p.P95
		a.P95 = &v
	}
	if p.P99 != nil {
		v := *p.P99
		a.P99 = &v
	}

	return a
}

// AggregatesToProto converts a slice of storage AggregateResults to proto.
func AggregatesToProto(aggregates []types.AggregateResult) []ProtoAggregateResult {
	result := make([]ProtoAggregateResult, len(aggregates))
	for i, a := range aggregates {
		result[i] = AggregateToProto(a)
	}
	return result
}

// AggregatesFromProto converts a slice of proto AggregateResults to storage.
func AggregatesFromProto(protos []ProtoAggregateResult) []types.AggregateResult {
	result := make([]types.AggregateResult, len(protos))
	for i, p := range protos {
		result[i] = AggregateFromProto(p)
	}
	return result
}

// ============================================================================
// Tier Conversion
// ============================================================================

// TierToProto converts a storage Tier to a proto StorageTier.
func TierToProto(t types.Tier) ProtoStorageTier {
	switch t {
	case types.TierRaw:
		return StorageTierRaw
	case types.Tier5Min:
		return StorageTier5Min
	case types.TierHourly:
		return StorageTierHourly
	case types.TierDaily:
		return StorageTierDaily
	case types.TierWeekly:
		return StorageTierWeekly
	default:
		return StorageTierRaw
	}
}

// TierFromProto converts a proto StorageTier to a storage Tier.
func TierFromProto(p ProtoStorageTier) types.Tier {
	switch p {
	case StorageTierRaw:
		return types.TierRaw
	case StorageTier5Min:
		return types.Tier5Min
	case StorageTierHourly:
		return types.TierHourly
	case StorageTierDaily:
		return types.TierDaily
	case StorageTierWeekly:
		return types.TierWeekly
	default:
		return types.TierRaw
	}
}

// ============================================================================
// Backpressure Level Conversion
// ============================================================================

// BackpressureLevelToProto converts a backpressure Level to proto.
func BackpressureLevelToProto(l backpressure.Level) ProtoBackpressureLevel {
	switch l {
	case backpressure.LevelNormal:
		return BackpressureNormal
	case backpressure.LevelWarning:
		return BackpressureWarning
	case backpressure.LevelCritical:
		return BackpressureCritical
	case backpressure.LevelEmergency:
		return BackpressureEmergency
	default:
		return BackpressureNormal
	}
}

// BackpressureLevelFromProto converts a proto BackpressureLevel to backpressure.Level.
func BackpressureLevelFromProto(p ProtoBackpressureLevel) backpressure.Level {
	switch p {
	case BackpressureNormal:
		return backpressure.LevelNormal
	case BackpressureWarning:
		return backpressure.LevelWarning
	case BackpressureCritical:
		return backpressure.LevelCritical
	case BackpressureEmergency:
		return backpressure.LevelEmergency
	default:
		return backpressure.LevelNormal
	}
}
