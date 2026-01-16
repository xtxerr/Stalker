package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

// Writer implements a Write-Ahead Log for crash-safe sample persistence.
// Each segment file contains a sequence of records with CRC checksums.
//
// File format:
//   - Header: 8 bytes magic + 4 bytes version
//   - Records: [4 bytes length][4 bytes crc32][payload]
type Writer struct {
	mu sync.Mutex

	dir            string
	currentSegment *os.File
	currentPath    string
	currentSize    int64
	segmentSeq     int64

	writer *bufio.Writer

	opts Options

	// Statistics
	stats WriterStats
}

// Options configures the WAL writer.
type Options struct {
	// MaxSegmentSize is the maximum size of a segment file before rotation.
	// Default: 100MB
	MaxSegmentSize int64

	// SyncMode controls how writes are synced to disk.
	// "async" - buffered, sync on interval
	// "sync" - sync after each write batch
	// "fsync" - fsync after each write batch
	SyncMode string

	// SyncInterval is the interval for async sync mode.
	// Default: 1s
	SyncInterval time.Duration

	// BufferSize is the size of the write buffer.
	// Default: 64KB
	BufferSize int
}

// DefaultOptions returns default WAL options.
func DefaultOptions() Options {
	return Options{
		MaxSegmentSize: 100 * 1024 * 1024, // 100MB
		SyncMode:       "async",
		SyncInterval:   time.Second,
		BufferSize:     64 * 1024, // 64KB
	}
}

// WriterStats holds WAL writer statistics.
type WriterStats struct {
	SegmentsCreated int64
	RecordsWritten  int64
	BytesWritten    int64
	SyncsPerformed  int64
	Errors          int64
}

const (
	walMagic   = 0x53544B57414C0001 // "STKWAL" + version 1
	walVersion = 1
	headerSize = 12 // 8 bytes magic + 4 bytes version
	recordHeaderSize = 8 // 4 bytes length + 4 bytes crc
)

// NewWriter creates a new WAL writer.
func NewWriter(dir string, opts Options) (*Writer, error) {
	if opts.MaxSegmentSize <= 0 {
		opts.MaxSegmentSize = DefaultOptions().MaxSegmentSize
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = DefaultOptions().BufferSize
	}
	if opts.SyncMode == "" {
		opts.SyncMode = "async"
	}

	// Ensure directory exists
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	w := &Writer{
		dir:  dir,
		opts: opts,
	}

	// Find the highest existing segment number
	segments, err := w.listSegments()
	if err != nil {
		return nil, fmt.Errorf("list segments: %w", err)
	}

	if len(segments) > 0 {
		w.segmentSeq = segments[len(segments)-1].seq + 1
	}

	// Create first segment
	if err := w.rotate(); err != nil {
		return nil, fmt.Errorf("create initial segment: %w", err)
	}

	return w, nil
}

// Write writes samples to the WAL.
func (w *Writer) Write(samples []types.Sample) error {
	if len(samples) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Encode samples
	payload, err := encodeSamples(samples)
	if err != nil {
		w.stats.Errors++
		return fmt.Errorf("encode samples: %w", err)
	}

	// Check if we need to rotate
	recordSize := int64(recordHeaderSize + len(payload))
	if w.currentSize+recordSize > w.opts.MaxSegmentSize {
		if err := w.rotateUnlocked(); err != nil {
			w.stats.Errors++
			return fmt.Errorf("rotate segment: %w", err)
		}
	}

	// Write record
	if err := w.writeRecord(payload); err != nil {
		w.stats.Errors++
		return fmt.Errorf("write record: %w", err)
	}

	w.stats.RecordsWritten++
	w.stats.BytesWritten += recordSize

	// Sync if needed
	if w.opts.SyncMode == "sync" || w.opts.SyncMode == "fsync" {
		if err := w.syncUnlocked(); err != nil {
			w.stats.Errors++
			return fmt.Errorf("sync: %w", err)
		}
	}

	return nil
}

// writeRecord writes a single record to the current segment.
func (w *Writer) writeRecord(payload []byte) error {
	// Calculate CRC
	crc := crc32.ChecksumIEEE(payload)

	// Write length
	var header [recordHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc)

	if _, err := w.writer.Write(header[:]); err != nil {
		return err
	}

	// Write payload
	if _, err := w.writer.Write(payload); err != nil {
		return err
	}

	w.currentSize += int64(recordHeaderSize + len(payload))
	return nil
}

// Sync flushes buffered data to disk.
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncUnlocked()
}

func (w *Writer) syncUnlocked() error {
	if w.writer == nil {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	if w.opts.SyncMode == "fsync" {
		if err := w.currentSegment.Sync(); err != nil {
			return err
		}
	}

	w.stats.SyncsPerformed++
	return nil
}

// Rotate closes the current segment and creates a new one.
func (w *Writer) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.rotateUnlocked()
}

func (w *Writer) rotate() error {
	return w.rotateUnlocked()
}

func (w *Writer) rotateUnlocked() error {
	// Close current segment
	if w.currentSegment != nil {
		if w.writer != nil {
			w.writer.Flush()
		}
		w.currentSegment.Close()
	}

	// Create new segment
	segmentName := fmt.Sprintf("%016d.wal", w.segmentSeq)
	segmentPath := filepath.Join(w.dir, segmentName)

	f, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create segment %s: %w", segmentPath, err)
	}

	// Write header
	var header [headerSize]byte
	binary.LittleEndian.PutUint64(header[0:8], walMagic)
	binary.LittleEndian.PutUint32(header[8:12], walVersion)

	if _, err := f.Write(header[:]); err != nil {
		f.Close()
		os.Remove(segmentPath)
		return fmt.Errorf("write header: %w", err)
	}

	w.currentSegment = f
	w.currentPath = segmentPath
	w.currentSize = headerSize
	w.writer = bufio.NewWriterSize(f, w.opts.BufferSize)
	w.segmentSeq++
	w.stats.SegmentsCreated++

	return nil
}

// Close closes the WAL writer.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		w.writer.Flush()
	}

	if w.currentSegment != nil {
		return w.currentSegment.Close()
	}

	return nil
}

// Stats returns writer statistics.
func (w *Writer) Stats() WriterStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.stats
}

// CurrentSegment returns the current segment path.
func (w *Writer) CurrentSegment() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.currentPath
}

// segmentInfo holds information about a segment file.
type segmentInfo struct {
	path string
	seq  int64
	size int64
}

// listSegments returns all segment files in order.
func (w *Writer) listSegments() ([]segmentInfo, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var segments []segmentInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if len(name) != 20 || name[16:] != ".wal" {
			continue
		}

		var seq int64
		if _, err := fmt.Sscanf(name, "%016d.wal", &seq); err != nil {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		segments = append(segments, segmentInfo{
			path: filepath.Join(w.dir, name),
			seq:  seq,
			size: info.Size(),
		})
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].seq < segments[j].seq
	})

	return segments, nil
}

// ListSegments returns all segment file paths in order.
func (w *Writer) ListSegments() ([]string, error) {
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	paths := make([]string, len(segments))
	for i, s := range segments {
		paths[i] = s.path
	}
	return paths, nil
}

// DeleteSegment deletes a segment file.
func (w *Writer) DeleteSegment(path string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Don't delete current segment
	if path == w.currentPath {
		return fmt.Errorf("cannot delete current segment")
	}

	return os.Remove(path)
}

// DeleteSegmentsBefore deletes all segments older than the given sequence.
func (w *Writer) DeleteSegmentsBefore(seq int64) (int, error) {
	segments, err := w.listSegments()
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, s := range segments {
		if s.seq >= seq {
			break
		}
		if err := w.DeleteSegment(s.path); err != nil {
			continue
		}
		deleted++
	}

	return deleted, nil
}
