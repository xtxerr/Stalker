package parquet

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// SampleReader reads samples from a Parquet file.
type SampleReader struct {
	file   *os.File
	reader *parquet.GenericReader[SampleRow]
	path   string
}

// NewSampleReader creates a new sample Parquet reader.
func NewSampleReader(path string) (*SampleReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat file: %w", err)
	}

	reader := parquet.NewGenericReader[SampleRow](f, parquet.ReadBufferSize(1024*1024))

	return &SampleReader{
		file:   f,
		reader: reader,
		path:   path,
	}, nil
	_ = stat // suppress unused warning
}

// Read reads up to n samples from the file.
func (r *SampleReader) Read(n int) ([]types.Sample, error) {
	rows := make([]SampleRow, n)
	count, err := r.reader.Read(rows)
	if err != nil {
		return nil, err
	}

	samples := make([]types.Sample, count)
	for i := 0; i < count; i++ {
		samples[i] = RowToSample(&rows[i])
	}

	return samples, nil
}

// ReadAll reads all samples from the file.
func (r *SampleReader) ReadAll() ([]types.Sample, error) {
	numRows := r.reader.NumRows()
	rows := make([]SampleRow, numRows)

	n, err := r.reader.Read(rows)
	if err != nil {
		return nil, err
	}

	samples := make([]types.Sample, n)
	for i := 0; i < n; i++ {
		samples[i] = RowToSample(&rows[i])
	}

	return samples, nil
}

// NumRows returns the total number of rows in the file.
func (r *SampleReader) NumRows() int64 {
	return r.reader.NumRows()
}

// Close closes the reader.
func (r *SampleReader) Close() error {
	if err := r.reader.Close(); err != nil {
		r.file.Close()
		return err
	}
	return r.file.Close()
}

// Path returns the file path.
func (r *SampleReader) Path() string {
	return r.path
}

// AggregateReader reads aggregates from a Parquet file.
type AggregateReader struct {
	file   *os.File
	reader *parquet.GenericReader[AggregateRow]
	path   string
}

// NewAggregateReader creates a new aggregate Parquet reader.
func NewAggregateReader(path string) (*AggregateReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	reader := parquet.NewGenericReader[AggregateRow](f, parquet.ReadBufferSize(1024*1024))

	return &AggregateReader{
		file:   f,
		reader: reader,
		path:   path,
	}, nil
}

// Read reads up to n aggregates from the file.
func (r *AggregateReader) Read(n int) ([]types.AggregateResult, error) {
	rows := make([]AggregateRow, n)
	count, err := r.reader.Read(rows)
	if err != nil {
		return nil, err
	}

	results := make([]types.AggregateResult, count)
	for i := 0; i < count; i++ {
		results[i] = RowToAggregate(&rows[i])
	}

	return results, nil
}

// ReadAll reads all aggregates from the file.
func (r *AggregateReader) ReadAll() ([]types.AggregateResult, error) {
	numRows := r.reader.NumRows()
	rows := make([]AggregateRow, numRows)

	n, err := r.reader.Read(rows)
	if err != nil {
		return nil, err
	}

	results := make([]types.AggregateResult, n)
	for i := 0; i < n; i++ {
		results[i] = RowToAggregate(&rows[i])
	}

	return results, nil
}

// NumRows returns the total number of rows in the file.
func (r *AggregateReader) NumRows() int64 {
	return r.reader.NumRows()
}

// Close closes the reader.
func (r *AggregateReader) Close() error {
	if err := r.reader.Close(); err != nil {
		r.file.Close()
		return err
	}
	return r.file.Close()
}

// Path returns the file path.
func (r *AggregateReader) Path() string {
	return r.path
}

// FileInfo holds information about a Parquet file.
type FileInfo struct {
	Path     string
	Size     int64
	NumRows  int64
	NumCols  int
	Metadata map[string]string
}

// GetFileInfo returns information about a Parquet file.
func GetFileInfo(path string) (*FileInfo, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// Try to read as sample file first
	reader := parquet.NewGenericReader[SampleRow](f)
	defer reader.Close()

	info := &FileInfo{
		Path:    path,
		Size:    stat.Size(),
		NumRows: reader.NumRows(),
	}

	return info, nil
}
