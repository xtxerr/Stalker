package parquet

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestSampleWriterBasic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "samples.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewSampleWriter: %v", err)
	}

	samples := []types.Sample{
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: time.Now().UnixMilli(),
			Value:       50.5,
			Valid:       true,
			PollMs:      100,
		},
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "memory",
			TimestampMs: time.Now().UnixMilli(),
			Value:       75.0,
			Valid:       true,
			PollMs:      50,
		},
	}

	if err := w.Write(samples); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify file exists
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file should exist: %v", err)
	}
	if stat.Size() == 0 {
		t.Error("file should not be empty")
	}
}

func TestSampleWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "samples.parquet")

	now := time.Now().UnixMilli()
	samples := []types.Sample{
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now,
			Value:       50.5,
			Valid:       true,
			Error:       "",
			PollMs:      100,
		},
		{
			Namespace:   "dev",
			Target:      "switch-02",
			Poller:      "ifInOctets",
			TimestampMs: now + 1000,
			Value:       1234567.89,
			Valid:       false,
			Error:       "timeout",
			PollMs:      5000,
		},
	}

	// Write
	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewSampleWriter: %v", err)
	}
	if err := w.Write(samples); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read
	r, err := NewSampleReader(path)
	if err != nil {
		t.Fatalf("NewSampleReader: %v", err)
	}
	defer r.Close()

	readSamples, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(readSamples) != 2 {
		t.Fatalf("expected 2 samples, got %d", len(readSamples))
	}

	// Verify first sample
	s := readSamples[0]
	if s.Namespace != "prod" {
		t.Errorf("expected namespace=prod, got %s", s.Namespace)
	}
	if s.Value != 50.5 {
		t.Errorf("expected value=50.5, got %f", s.Value)
	}
	if !s.Valid {
		t.Error("expected valid=true")
	}

	// Verify second sample
	s = readSamples[1]
	if s.Error != "timeout" {
		t.Errorf("expected error=timeout, got %s", s.Error)
	}
	if s.Valid {
		t.Error("expected valid=false")
	}
}

func TestAggregateWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "aggregates.parquet")

	now := time.Now().UnixMilli()
	aggregates := []types.AggregateResult{
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			BucketStart: now,
			BucketEnd:   now + 300000,
			Count:       100,
			Sum:         5000,
			Min:         10,
			Max:         90,
			Avg:         50,
			FirstTs:     now + 1000,
			LastTs:      now + 299000,
		},
	}
	aggregates[0].SetPercentiles(48, 85, 92, 98)

	// Write
	w, err := NewAggregateWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewAggregateWriter: %v", err)
	}
	if err := w.Write(aggregates); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read
	r, err := NewAggregateReader(path)
	if err != nil {
		t.Fatalf("NewAggregateReader: %v", err)
	}
	defer r.Close()

	readAggregates, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(readAggregates) != 1 {
		t.Fatalf("expected 1 aggregate, got %d", len(readAggregates))
	}

	a := readAggregates[0]
	if a.Namespace != "prod" {
		t.Errorf("expected namespace=prod, got %s", a.Namespace)
	}
	if a.Count != 100 {
		t.Errorf("expected count=100, got %d", a.Count)
	}
	if a.Avg != 50 {
		t.Errorf("expected avg=50, got %f", a.Avg)
	}
	if !a.HasPercentiles() {
		t.Error("expected percentiles")
	}
	if *a.P50 != 48 {
		t.Errorf("expected p50=48, got %f", *a.P50)
	}
}

func TestLargeWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewSampleWriter: %v", err)
	}

	now := time.Now().UnixMilli()

	// Write 10000 samples
	samples := make([]types.Sample, 10000)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router",
			Poller:      "cpu",
			TimestampMs: now + int64(i),
			Value:       float64(i % 100),
			Valid:       true,
			PollMs:      100,
		}
	}

	if err := w.Write(samples); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back
	r, err := NewSampleReader(path)
	if err != nil {
		t.Fatalf("NewSampleReader: %v", err)
	}
	defer r.Close()

	if r.NumRows() != 10000 {
		t.Errorf("expected 10000 rows, got %d", r.NumRows())
	}

	readSamples, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(readSamples) != 10000 {
		t.Errorf("expected 10000 samples, got %d", len(readSamples))
	}
}

func TestCompressionTypes(t *testing.T) {
	compressions := []struct {
		name string
		ct   CompressionType
	}{
		{"none", CompressionNone},
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
		{"lz4", CompressionLZ4},
	}

	for _, tc := range compressions {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.parquet")

			opts := DefaultOptions()
			opts.Compression = tc.ct

			w, err := NewSampleWriter(path, opts)
			if err != nil {
				t.Fatalf("NewSampleWriter: %v", err)
			}

			samples := []types.Sample{
				{Namespace: "prod", Target: "r1", Poller: "p1", TimestampMs: 1000, Value: 50, Valid: true},
			}

			if err := w.Write(samples); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			// Verify can read back
			r, err := NewSampleReader(path)
			if err != nil {
				t.Fatalf("NewSampleReader: %v", err)
			}
			defer r.Close()

			readSamples, err := r.ReadAll()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}

			if len(readSamples) != 1 {
				t.Errorf("expected 1 sample, got %d", len(readSamples))
			}
		})
	}
}

func TestParseCompressionType(t *testing.T) {
	tests := []struct {
		input    string
		expected CompressionType
	}{
		{"snappy", CompressionSnappy},
		{"zstd", CompressionZstd},
		{"lz4", CompressionLZ4},
		{"gzip", CompressionGzip},
		{"none", CompressionNone},
		{"", CompressionNone},
		{"invalid", CompressionZstd}, // Default
	}

	for _, tt := range tests {
		result := ParseCompressionType(tt.input)
		if result != tt.expected {
			t.Errorf("ParseCompressionType(%s): expected %d, got %d", tt.input, tt.expected, result)
		}
	}
}

func TestRowConversions(t *testing.T) {
	// Sample conversion
	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router",
		Poller:      "cpu",
		TimestampMs: 1000,
		Value:       50.5,
		Valid:       true,
		Error:       "test",
		PollMs:      100,
	}

	row := SampleToRow(&sample)
	back := RowToSample(&row)

	if back.Namespace != sample.Namespace ||
		back.Target != sample.Target ||
		back.Value != sample.Value {
		t.Error("sample conversion roundtrip failed")
	}

	// Aggregate conversion
	agg := types.AggregateResult{
		Namespace:   "prod",
		Target:      "router",
		Poller:      "cpu",
		BucketStart: 1000,
		BucketEnd:   2000,
		Count:       100,
		Sum:         5000,
		Min:         10,
		Max:         90,
		Avg:         50,
		FirstTs:     1100,
		LastTs:      1900,
	}
	agg.SetPercentiles(45, 80, 90, 95)

	aggRow := AggregateToRow(&agg)
	aggBack := RowToAggregate(&aggRow)

	if aggBack.Namespace != agg.Namespace ||
		aggBack.Count != agg.Count ||
		*aggBack.P50 != *agg.P50 {
		t.Error("aggregate conversion roundtrip failed")
	}
}

func TestEmptyWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewSampleWriter: %v", err)
	}

	// Empty write should be no-op
	if err := w.Write(nil); err != nil {
		t.Errorf("nil write should succeed: %v", err)
	}
	if err := w.Write([]types.Sample{}); err != nil {
		t.Errorf("empty write should succeed: %v", err)
	}

	if w.RowCount() != 0 {
		t.Errorf("expected 0 rows, got %d", w.RowCount())
	}

	w.Close()
}

func TestWriteToClosedWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewSampleWriter: %v", err)
	}

	w.Close()

	err = w.Write([]types.Sample{{Valid: true}})
	if err != ErrWriterClosed {
		t.Errorf("expected ErrWriterClosed, got %v", err)
	}
}

func TestGetFileInfo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		t.Fatalf("NewSampleWriter: %v", err)
	}

	samples := make([]types.Sample, 100)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router",
			Poller:      "cpu",
			TimestampMs: int64(i),
			Value:       float64(i),
			Valid:       true,
		}
	}

	w.Write(samples)
	w.Close()

	info, err := GetFileInfo(path)
	if err != nil {
		t.Fatalf("GetFileInfo: %v", err)
	}

	if info.NumRows != 100 {
		t.Errorf("expected 100 rows, got %d", info.NumRows)
	}
	if info.Size <= 0 {
		t.Error("expected positive size")
	}
}

func BenchmarkSampleWrite(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		b.Fatalf("NewSampleWriter: %v", err)
	}
	defer w.Close()

	sample := types.Sample{
		Namespace:   "prod",
		Target:      "router-01",
		Poller:      "cpu",
		TimestampMs: time.Now().UnixMilli(),
		Value:       50.5,
		Valid:       true,
		PollMs:      100,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Write([]types.Sample{sample})
	}
}

func BenchmarkSampleWriteBatch1000(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "bench.parquet")

	w, err := NewSampleWriter(path, DefaultOptions())
	if err != nil {
		b.Fatalf("NewSampleWriter: %v", err)
	}
	defer w.Close()

	now := time.Now().UnixMilli()
	batch := make([]types.Sample, 1000)
	for i := range batch {
		batch[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now + int64(i),
			Value:       float64(i),
			Valid:       true,
			PollMs:      100,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Write(batch)
	}
}
