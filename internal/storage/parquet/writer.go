package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// Options configures the Parquet writer.
type Options struct {
	// Compression algorithm
	Compression CompressionType

	// CompressionLevel for algorithms that support it (zstd: 1-22)
	CompressionLevel int

	// RowGroupSize is the target number of rows per row group
	RowGroupSize int

	// PageSize is the target page size in bytes
	PageSize int
}

// CompressionType represents a Parquet compression algorithm.
type CompressionType int

const (
	CompressionNone CompressionType = iota
	CompressionSnappy
	CompressionZstd
	CompressionLZ4
	CompressionGzip
)

// DefaultOptions returns default Parquet options.
func DefaultOptions() Options {
	return Options{
		Compression:      CompressionZstd,
		CompressionLevel: 3,
		RowGroupSize:     100000,
		PageSize:         1024 * 1024, // 1MB
	}
}

// ParseCompressionType parses a compression type string.
func ParseCompressionType(s string) CompressionType {
	switch s {
	case "snappy":
		return CompressionSnappy
	case "zstd":
		return CompressionZstd
	case "lz4":
		return CompressionLZ4
	case "gzip":
		return CompressionGzip
	case "none", "":
		return CompressionNone
	default:
		return CompressionZstd
	}
}

// getCompression returns the parquet-go compression codec.
func getCompression(ct CompressionType) compress.Codec {
	switch ct {
	case CompressionSnappy:
		return &parquet.Snappy
	case CompressionZstd:
		return &parquet.Zstd
	case CompressionLZ4:
		return &parquet.Lz4Raw
	case CompressionGzip:
		return &parquet.Gzip
	default:
		return &parquet.Uncompressed
	}
}

// SampleRow represents a sample in Parquet format.
type SampleRow struct {
	Namespace   string  `parquet:"namespace,zstd"`
	Target      string  `parquet:"target,zstd"`
	Poller      string  `parquet:"poller,zstd"`
	TimestampMs int64   `parquet:"timestamp_ms"`
	Value       float64 `parquet:"value"`
	Valid       bool    `parquet:"valid"`
	Error       string  `parquet:"error,optional,zstd"`
	PollMs      int32   `parquet:"poll_ms"`
}

// AggregateRow represents an aggregate in Parquet format.
type AggregateRow struct {
	Namespace   string  `parquet:"namespace,zstd"`
	Target      string  `parquet:"target,zstd"`
	Poller      string  `parquet:"poller,zstd"`
	BucketStart int64   `parquet:"bucket_start"`
	BucketEnd   int64   `parquet:"bucket_end"`
	Count       int64   `parquet:"count"`
	Sum         float64 `parquet:"sum"`
	Min         float64 `parquet:"min"`
	Max         float64 `parquet:"max"`
	Avg         float64 `parquet:"avg"`
	P50         float64 `parquet:"p50,optional"`
	P90         float64 `parquet:"p90,optional"`
	P95         float64 `parquet:"p95,optional"`
	P99         float64 `parquet:"p99,optional"`
	FirstTs     int64   `parquet:"first_ts"`
	LastTs      int64   `parquet:"last_ts"`
}

// SampleToRow converts a Sample to a SampleRow.
func SampleToRow(s *types.Sample) SampleRow {
	return SampleRow{
		Namespace:   s.Namespace,
		Target:      s.Target,
		Poller:      s.Poller,
		TimestampMs: s.TimestampMs,
		Value:       s.Value,
		Valid:       s.Valid,
		Error:       s.Error,
		PollMs:      s.PollMs,
	}
}

// RowToSample converts a SampleRow to a Sample.
func RowToSample(r *SampleRow) types.Sample {
	return types.Sample{
		Namespace:   r.Namespace,
		Target:      r.Target,
		Poller:      r.Poller,
		TimestampMs: r.TimestampMs,
		Value:       r.Value,
		Valid:       r.Valid,
		Error:       r.Error,
		PollMs:      r.PollMs,
	}
}

// AggregateToRow converts an AggregateResult to an AggregateRow.
func AggregateToRow(a *types.AggregateResult) AggregateRow {
	row := AggregateRow{
		Namespace:   a.Namespace,
		Target:      a.Target,
		Poller:      a.Poller,
		BucketStart: a.BucketStart,
		BucketEnd:   a.BucketEnd,
		Count:       a.Count,
		Sum:         a.Sum,
		Min:         a.Min,
		Max:         a.Max,
		Avg:         a.Avg,
		FirstTs:     a.FirstTs,
		LastTs:      a.LastTs,
	}

	if a.P50 != nil {
		row.P50 = *a.P50
	}
	if a.P90 != nil {
		row.P90 = *a.P90
	}
	if a.P95 != nil {
		row.P95 = *a.P95
	}
	if a.P99 != nil {
		row.P99 = *a.P99
	}

	return row
}

// RowToAggregate converts an AggregateRow to an AggregateResult.
func RowToAggregate(r *AggregateRow) types.AggregateResult {
	result := types.AggregateResult{
		Namespace:   r.Namespace,
		Target:      r.Target,
		Poller:      r.Poller,
		BucketStart: r.BucketStart,
		BucketEnd:   r.BucketEnd,
		Count:       r.Count,
		Sum:         r.Sum,
		Min:         r.Min,
		Max:         r.Max,
		Avg:         r.Avg,
		FirstTs:     r.FirstTs,
		LastTs:      r.LastTs,
	}

	// Set percentiles if present
	if r.P50 != 0 || r.P90 != 0 || r.P95 != 0 || r.P99 != 0 {
		result.SetPercentiles(r.P50, r.P90, r.P95, r.P99)
	}

	return result
}

// SampleWriter writes samples to a Parquet file.
type SampleWriter struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	writer   *parquet.GenericWriter[SampleRow]
	rowCount int64
	closed   bool
}

// NewSampleWriter creates a new sample Parquet writer.
func NewSampleWriter(path string, opts Options) (*SampleWriter, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	writerOpts := []parquet.WriterOption{
		parquet.Compression(getCompression(opts.Compression)),
	}

	writer := parquet.NewGenericWriter[SampleRow](f, writerOpts...)

	return &SampleWriter{
		path:   path,
		file:   f,
		writer: writer,
	}, nil
}

// Write writes samples to the Parquet file.
func (w *SampleWriter) Write(samples []types.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWriterClosed
	}

	rows := make([]SampleRow, len(samples))
	for i := range samples {
		rows[i] = SampleToRow(&samples[i])
	}

	n, err := w.writer.Write(rows)
	if err != nil {
		return fmt.Errorf("write rows: %w", err)
	}

	w.rowCount += int64(n)
	return nil
}

// Close closes the writer.
func (w *SampleWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close writer: %w", err)
	}

	return w.file.Close()
}

// RowCount returns the number of rows written.
func (w *SampleWriter) RowCount() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rowCount
}

// Path returns the file path.
func (w *SampleWriter) Path() string {
	return w.path
}

// AggregateWriter writes aggregates to a Parquet file.
type AggregateWriter struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	writer   *parquet.GenericWriter[AggregateRow]
	rowCount int64
	closed   bool
}

// NewAggregateWriter creates a new aggregate Parquet writer.
func NewAggregateWriter(path string, opts Options) (*AggregateWriter, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create file: %w", err)
	}

	writerOpts := []parquet.WriterOption{
		parquet.Compression(getCompression(opts.Compression)),
	}

	writer := parquet.NewGenericWriter[AggregateRow](f, writerOpts...)

	return &AggregateWriter{
		path:   path,
		file:   f,
		writer: writer,
	}, nil
}

// Write writes aggregates to the Parquet file.
func (w *AggregateWriter) Write(aggregates []types.AggregateResult) error {
	if len(aggregates) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrWriterClosed
	}

	rows := make([]AggregateRow, len(aggregates))
	for i := range aggregates {
		rows[i] = AggregateToRow(&aggregates[i])
	}

	n, err := w.writer.Write(rows)
	if err != nil {
		return fmt.Errorf("write rows: %w", err)
	}

	w.rowCount += int64(n)
	return nil
}

// Close closes the writer.
func (w *AggregateWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if err := w.writer.Close(); err != nil {
		w.file.Close()
		return fmt.Errorf("close writer: %w", err)
	}

	return w.file.Close()
}

// RowCount returns the number of rows written.
func (w *AggregateWriter) RowCount() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rowCount
}

// Path returns the file path.
func (w *AggregateWriter) Path() string {
	return w.path
}

// ErrWriterClosed is returned when writing to a closed writer.
var ErrWriterClosed = fmt.Errorf("parquet writer is closed")
