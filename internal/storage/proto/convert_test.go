package proto

import (
	"testing"

	"github.com/xtxerr/stalker/internal/storage/backpressure"
	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestSampleConversion(t *testing.T) {
	original := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: 1234567890123,
		Value:       42.5,
		Valid:       true,
		Error:       "",
		PollMs:      25,
	}

	// Convert to proto
	proto := SampleToProto(original)

	if proto.Namespace != original.Namespace {
		t.Errorf("namespace: expected %s, got %s", original.Namespace, proto.Namespace)
	}
	if proto.Target != original.Target {
		t.Errorf("target: expected %s, got %s", original.Target, proto.Target)
	}
	if proto.Poller != original.Poller {
		t.Errorf("poller: expected %s, got %s", original.Poller, proto.Poller)
	}
	if proto.TimestampMs != original.TimestampMs {
		t.Errorf("timestamp: expected %d, got %d", original.TimestampMs, proto.TimestampMs)
	}
	if proto.ValueGauge != original.Value {
		t.Errorf("value: expected %f, got %f", original.Value, proto.ValueGauge)
	}
	if proto.Valid != original.Valid {
		t.Errorf("valid: expected %v, got %v", original.Valid, proto.Valid)
	}
	if proto.PollMs != original.PollMs {
		t.Errorf("poll_ms: expected %d, got %d", original.PollMs, proto.PollMs)
	}

	// Convert back
	converted := SampleFromProto(proto)

	if converted.Namespace != original.Namespace {
		t.Errorf("roundtrip namespace: expected %s, got %s", original.Namespace, converted.Namespace)
	}
	if converted.Value != original.Value {
		t.Errorf("roundtrip value: expected %f, got %f", original.Value, converted.Value)
	}
}

func TestSampleWithError(t *testing.T) {
	original := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: 1234567890123,
		Value:       0,
		Valid:       false,
		Error:       "timeout",
		PollMs:      1000,
	}

	proto := SampleToProto(original)
	converted := SampleFromProto(proto)

	if converted.Valid {
		t.Error("expected invalid sample")
	}
	if converted.Error != "timeout" {
		t.Errorf("expected error='timeout', got %s", converted.Error)
	}
}

func TestSamplesSliceConversion(t *testing.T) {
	originals := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", Value: 10, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "mem", Value: 20, Valid: true},
		{Namespace: "dev", Target: "r2", Poller: "cpu", Value: 30, Valid: true},
	}

	protos := SamplesToProto(originals)
	if len(protos) != 3 {
		t.Fatalf("expected 3 protos, got %d", len(protos))
	}

	converted := SamplesFromProto(protos)
	if len(converted) != 3 {
		t.Fatalf("expected 3 converted, got %d", len(converted))
	}

	for i, orig := range originals {
		if converted[i].Namespace != orig.Namespace {
			t.Errorf("sample %d: namespace mismatch", i)
		}
		if converted[i].Value != orig.Value {
			t.Errorf("sample %d: value mismatch", i)
		}
	}
}

func TestSampleBatchConversion(t *testing.T) {
	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", Value: 10, Valid: true},
		{Namespace: "prod", Target: "r1", Poller: "mem", Value: 20, Valid: true},
	}

	receivedAt := int64(1234567890123)
	batch := SampleBatchToProto(samples, receivedAt)

	if batch.ReceivedAtMs != receivedAt {
		t.Errorf("received_at: expected %d, got %d", receivedAt, batch.ReceivedAtMs)
	}
	if len(batch.Samples) != 2 {
		t.Errorf("expected 2 samples, got %d", len(batch.Samples))
	}

	converted := SampleBatchFromProto(batch)
	if len(converted) != 2 {
		t.Errorf("expected 2 converted samples, got %d", len(converted))
	}
}

func TestAggregateConversion(t *testing.T) {
	p50 := 50.0
	p90 := 90.0
	p95 := 95.0
	p99 := 99.0

	original := types.AggregateResult{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "latency",
		BucketStart: 1234567800000,
		BucketEnd:   1234568100000,
		Count:       100,
		Sum:         5000,
		Min:         10,
		Max:         100,
		Avg:         50,
		P50:         &p50,
		P90:         &p90,
		P95:         &p95,
		P99:         &p99,
		FirstTs:     1234567800001,
		LastTs:      1234568099999,
	}

	proto := AggregateToProto(original)

	if proto.Namespace != original.Namespace {
		t.Errorf("namespace mismatch")
	}
	if proto.Count != original.Count {
		t.Errorf("count: expected %d, got %d", original.Count, proto.Count)
	}
	if proto.Sum != original.Sum {
		t.Errorf("sum: expected %f, got %f", original.Sum, proto.Sum)
	}
	if proto.P50 == nil || *proto.P50 != p50 {
		t.Errorf("p50 mismatch")
	}
	if proto.P99 == nil || *proto.P99 != p99 {
		t.Errorf("p99 mismatch")
	}

	converted := AggregateFromProto(proto)

	if converted.Count != original.Count {
		t.Errorf("roundtrip count: expected %d, got %d", original.Count, converted.Count)
	}
	if converted.P50 == nil || *converted.P50 != p50 {
		t.Errorf("roundtrip p50 mismatch")
	}
}

func TestAggregateWithoutPercentiles(t *testing.T) {
	original := types.AggregateResult{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		BucketStart: 1234567800000,
		BucketEnd:   1234568100000,
		Count:       50,
		Sum:         2500,
		Min:         10,
		Max:         90,
		Avg:         50,
		// No percentiles
	}

	proto := AggregateToProto(original)

	if proto.P50 != nil {
		t.Error("expected nil P50")
	}
	if proto.P90 != nil {
		t.Error("expected nil P90")
	}

	converted := AggregateFromProto(proto)

	if converted.P50 != nil {
		t.Error("roundtrip: expected nil P50")
	}
}

func TestAggregatesSliceConversion(t *testing.T) {
	originals := []types.AggregateResult{
		{Namespace: "prod", Target: "r1", Poller: "cpu", Count: 10},
		{Namespace: "prod", Target: "r1", Poller: "mem", Count: 20},
	}

	protos := AggregatesToProto(originals)
	if len(protos) != 2 {
		t.Fatalf("expected 2 protos, got %d", len(protos))
	}

	converted := AggregatesFromProto(protos)
	if len(converted) != 2 {
		t.Fatalf("expected 2 converted, got %d", len(converted))
	}
}

func TestTierConversion(t *testing.T) {
	tests := []struct {
		tier  types.Tier
		proto ProtoStorageTier
	}{
		{types.TierRaw, StorageTierRaw},
		{types.Tier5Min, StorageTier5Min},
		{types.TierHourly, StorageTierHourly},
		{types.TierDaily, StorageTierDaily},
		{types.TierWeekly, StorageTierWeekly},
	}

	for _, tt := range tests {
		proto := TierToProto(tt.tier)
		if proto != tt.proto {
			t.Errorf("tier %s: expected proto %d, got %d", tt.tier, tt.proto, proto)
		}

		converted := TierFromProto(proto)
		if converted != tt.tier {
			t.Errorf("proto %d: expected tier %s, got %s", tt.proto, tt.tier, converted)
		}
	}
}

func TestBackpressureLevelConversion(t *testing.T) {
	tests := []struct {
		level backpressure.Level
		proto ProtoBackpressureLevel
	}{
		{backpressure.LevelNormal, BackpressureNormal},
		{backpressure.LevelWarning, BackpressureWarning},
		{backpressure.LevelCritical, BackpressureCritical},
		{backpressure.LevelEmergency, BackpressureEmergency},
	}

	for _, tt := range tests {
		proto := BackpressureLevelToProto(tt.level)
		if proto != tt.proto {
			t.Errorf("level %s: expected proto %d, got %d", tt.level, tt.proto, proto)
		}

		converted := BackpressureLevelFromProto(proto)
		if converted != tt.level {
			t.Errorf("proto %d: expected level %s, got %s", tt.proto, tt.level, converted)
		}
	}
}

func BenchmarkSampleToProto(b *testing.B) {
	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: 1234567890123,
		Value:       42.5,
		Valid:       true,
		PollMs:      25,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SampleToProto(sample)
	}
}

func BenchmarkSampleFromProto(b *testing.B) {
	proto := ProtoSample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: 1234567890123,
		ValueGauge:  42.5,
		Valid:       true,
		PollMs:      25,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		SampleFromProto(proto)
	}
}

func BenchmarkSampleBatchConversion(b *testing.B) {
	samples := make([]types.Sample, 100)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: int64(1234567890123 + i),
			Value:       float64(i),
			Valid:       true,
			PollMs:      25,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := SampleBatchToProto(samples, 1234567890123)
		SampleBatchFromProto(batch)
	}
}
