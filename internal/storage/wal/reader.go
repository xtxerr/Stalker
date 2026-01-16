package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/xtxerr/stalker/internal/storage/types"
)

// Reader reads samples from WAL segment files.
type Reader struct {
	path string
	file *os.File

	// Statistics
	stats ReaderStats
}

// ReaderStats holds WAL reader statistics.
type ReaderStats struct {
	RecordsRead    int64
	SamplesRead    int64
	BytesRead      int64
	CorruptRecords int64
}

// NewReader creates a new WAL reader for a segment file.
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open segment: %w", err)
	}

	// Verify header
	var header [headerSize]byte
	if _, err := io.ReadFull(f, header[:]); err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}

	magic := binary.LittleEndian.Uint64(header[0:8])
	if magic != walMagic {
		f.Close()
		return nil, fmt.Errorf("invalid magic: expected %x, got %x", walMagic, magic)
	}

	version := binary.LittleEndian.Uint32(header[8:12])
	if version != walVersion {
		f.Close()
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	return &Reader{
		path: path,
		file: f,
	}, nil
}

// ReadAll reads all samples from the segment.
func (r *Reader) ReadAll() ([]types.Sample, error) {
	var allSamples []types.Sample

	for {
		samples, err := r.ReadRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log corrupt record but continue
			r.stats.CorruptRecords++
			continue
		}

		allSamples = append(allSamples, samples...)
	}

	return allSamples, nil
}

// ReadRecord reads the next record from the segment.
// Returns io.EOF when there are no more records.
func (r *Reader) ReadRecord() ([]types.Sample, error) {
	// Read record header
	var header [recordHeaderSize]byte
	if _, err := io.ReadFull(r.file, header[:]); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read record header: %w", err)
	}

	length := binary.LittleEndian.Uint32(header[0:4])
	expectedCRC := binary.LittleEndian.Uint32(header[4:8])

	// Sanity check length
	if length > 100*1024*1024 { // 100MB max
		return nil, fmt.Errorf("record too large: %d bytes", length)
	}

	// Read payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(r.file, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// Verify CRC
	actualCRC := crc32.ChecksumIEEE(payload)
	if actualCRC != expectedCRC {
		return nil, fmt.Errorf("CRC mismatch: expected %x, got %x", expectedCRC, actualCRC)
	}

	// Decode samples
	samples, err := decodeSamples(payload)
	if err != nil {
		return nil, fmt.Errorf("decode samples: %w", err)
	}

	r.stats.RecordsRead++
	r.stats.SamplesRead += int64(len(samples))
	r.stats.BytesRead += int64(recordHeaderSize + len(payload))

	return samples, nil
}

// Close closes the reader.
func (r *Reader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// Stats returns reader statistics.
func (r *Reader) Stats() ReaderStats {
	return r.stats
}

// Path returns the segment path.
func (r *Reader) Path() string {
	return r.path
}

// ReadSegment is a convenience function to read all samples from a segment file.
func ReadSegment(path string) ([]types.Sample, error) {
	r, err := NewReader(path)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return r.ReadAll()
}

// ReadAllSegments reads all samples from multiple segment files.
// Segments are read in order.
func ReadAllSegments(paths []string) ([]types.Sample, error) {
	var allSamples []types.Sample

	for _, path := range paths {
		samples, err := ReadSegment(path)
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", path, err)
		}
		allSamples = append(allSamples, samples...)
	}

	return allSamples, nil
}

// Iterator iterates over samples in a segment.
type Iterator struct {
	reader   *Reader
	buffer   []types.Sample
	position int
	done     bool
	err      error
}

// NewIterator creates an iterator for a segment file.
func NewIterator(path string) (*Iterator, error) {
	r, err := NewReader(path)
	if err != nil {
		return nil, err
	}

	return &Iterator{
		reader: r,
	}, nil
}

// Next returns the next sample.
// Returns false when there are no more samples.
func (it *Iterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}

	// If buffer is exhausted, read next record
	for it.position >= len(it.buffer) {
		samples, err := it.reader.ReadRecord()
		if err == io.EOF {
			it.done = true
			return false
		}
		if err != nil {
			it.err = err
			return false
		}

		it.buffer = samples
		it.position = 0
	}

	return true
}

// Sample returns the current sample.
func (it *Iterator) Sample() types.Sample {
	if it.position < len(it.buffer) {
		s := it.buffer[it.position]
		it.position++
		return s
	}
	return types.Sample{}
}

// Err returns any error encountered during iteration.
func (it *Iterator) Err() error {
	return it.err
}

// Close closes the iterator.
func (it *Iterator) Close() error {
	return it.reader.Close()
}
