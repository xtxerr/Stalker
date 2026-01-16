package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xtxerr/stalker/internal/storage/types"
)

func TestEncodeDecode(t *testing.T) {
	samples := []types.Sample{
		{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: 1234567890123,
			Value:       42.5,
			Valid:       true,
			Error:       "",
			PollMs:      25,
		},
		{
			Namespace:   "dev",
			Target:      "switch-02",
			Poller:      "memory",
			TimestampMs: 1234567890456,
			Value:       75.3,
			Valid:       false,
			Error:       "timeout",
			PollMs:      100,
		},
	}

	// Encode
	data, err := encodeSamples(samples)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Decode
	decoded, err := decodeSamples(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(decoded) != len(samples) {
		t.Fatalf("expected %d samples, got %d", len(samples), len(decoded))
	}

	for i, s := range samples {
		d := decoded[i]
		if d.Namespace != s.Namespace {
			t.Errorf("sample %d: namespace mismatch", i)
		}
		if d.Target != s.Target {
			t.Errorf("sample %d: target mismatch", i)
		}
		if d.Poller != s.Poller {
			t.Errorf("sample %d: poller mismatch", i)
		}
		if d.TimestampMs != s.TimestampMs {
			t.Errorf("sample %d: timestamp mismatch", i)
		}
		if d.Value != s.Value {
			t.Errorf("sample %d: value mismatch", i)
		}
		if d.Valid != s.Valid {
			t.Errorf("sample %d: valid mismatch", i)
		}
		if d.Error != s.Error {
			t.Errorf("sample %d: error mismatch", i)
		}
		if d.PollMs != s.PollMs {
			t.Errorf("sample %d: poll_ms mismatch", i)
		}
	}
}

func TestWriter_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	now := time.Now().UnixMilli()

	// Write samples
	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 50, Valid: true, PollMs: 25},
		{Namespace: "prod", Target: "r1", Poller: "mem", TimestampMs: now, Value: 70, Valid: true, PollMs: 25},
	}

	if err := w.Write(samples); err != nil {
		t.Fatalf("Write: %v", err)
	}

	stats := w.Stats()
	if stats.RecordsWritten != 1 {
		t.Errorf("expected 1 record written, got %d", stats.RecordsWritten)
	}

	// Sync and close
	if err := w.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

func TestWriter_Rotation(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.MaxSegmentSize = 1024 // Small segment for testing

	w, err := NewWriter(tmpDir, opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	now := time.Now().UnixMilli()

	// Write many samples to trigger rotation
	for i := 0; i < 100; i++ {
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	segments, err := w.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}

	if len(segments) < 2 {
		t.Errorf("expected at least 2 segments due to rotation, got %d", len(segments))
	}

	stats := w.Stats()
	if stats.SegmentsCreated < 2 {
		t.Errorf("expected at least 2 segments created, got %d", stats.SegmentsCreated)
	}
}

func TestReader_Basic(t *testing.T) {
	tmpDir := t.TempDir()

	// Write samples
	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()
	written := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 50, Valid: true, PollMs: 25},
		{Namespace: "prod", Target: "r1", Poller: "mem", TimestampMs: now + 1, Value: 70, Valid: true, PollMs: 25},
		{Namespace: "dev", Target: "r2", Poller: "cpu", TimestampMs: now + 2, Value: 30, Valid: false, Error: "timeout", PollMs: 100},
	}

	if err := w.Write(written); err != nil {
		t.Fatalf("Write: %v", err)
	}

	segmentPath := w.CurrentSegment()
	w.Close()

	// Read samples
	r, err := NewReader(segmentPath)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	defer r.Close()

	read, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(read) != len(written) {
		t.Fatalf("expected %d samples, got %d", len(written), len(read))
	}

	for i, s := range written {
		if read[i].Namespace != s.Namespace ||
			read[i].Target != s.Target ||
			read[i].Poller != s.Poller ||
			read[i].Value != s.Value {
			t.Errorf("sample %d mismatch", i)
		}
	}
}

func TestReader_MultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()

	// Write multiple records
	for i := 0; i < 10; i++ {
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	segmentPath := w.CurrentSegment()
	w.Close()

	// Read all
	samples, err := ReadSegment(segmentPath)
	if err != nil {
		t.Fatalf("ReadSegment: %v", err)
	}

	if len(samples) != 10 {
		t.Errorf("expected 10 samples, got %d", len(samples))
	}
}

func TestReadSegment(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()
	samples := []types.Sample{
		{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now, Value: 50, Valid: true, PollMs: 25},
	}

	if err := w.Write(samples); err != nil {
		t.Fatalf("Write: %v", err)
	}

	segmentPath := w.CurrentSegment()
	w.Close()

	// Use convenience function
	read, err := ReadSegment(segmentPath)
	if err != nil {
		t.Fatalf("ReadSegment: %v", err)
	}

	if len(read) != 1 {
		t.Errorf("expected 1 sample, got %d", len(read))
	}
}

func TestReadAllSegments(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.MaxSegmentSize = 512 // Small for quick rotation

	w, err := NewWriter(tmpDir, opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()

	// Write enough to create multiple segments
	for i := 0; i < 50; i++ {
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	w.Close()

	// List segments
	segments, err := w.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}

	// Read all
	allSamples, err := ReadAllSegments(segments)
	if err != nil {
		t.Fatalf("ReadAllSegments: %v", err)
	}

	if len(allSamples) != 50 {
		t.Errorf("expected 50 samples, got %d", len(allSamples))
	}
}

func TestIterator(t *testing.T) {
	tmpDir := t.TempDir()

	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()

	// Write multiple records
	for i := 0; i < 5; i++ {
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	segmentPath := w.CurrentSegment()
	w.Close()

	// Use iterator
	it, err := NewIterator(segmentPath)
	if err != nil {
		t.Fatalf("NewIterator: %v", err)
	}
	defer it.Close()

	count := 0
	for it.Next() {
		s := it.Sample()
		if s.Value != float64(count) {
			t.Errorf("expected value=%d, got %f", count, s.Value)
		}
		count++
	}

	if err := it.Err(); err != nil {
		t.Errorf("iterator error: %v", err)
	}

	if count != 5 {
		t.Errorf("expected 5 samples, got %d", count)
	}
}

func TestWriter_DeleteSegments(t *testing.T) {
	tmpDir := t.TempDir()

	opts := DefaultOptions()
	opts.MaxSegmentSize = 256

	w, err := NewWriter(tmpDir, opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()

	// Write to create multiple segments
	for i := 0; i < 50; i++ {
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	segments, err := w.ListSegments()
	if err != nil {
		t.Fatalf("ListSegments: %v", err)
	}

	initialCount := len(segments)
	if initialCount < 3 {
		t.Skipf("not enough segments created (%d), skipping delete test", initialCount)
	}

	// Delete old segments
	deleted, err := w.DeleteSegmentsBefore(2)
	if err != nil {
		t.Fatalf("DeleteSegmentsBefore: %v", err)
	}

	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}

	remainingSegments, _ := w.ListSegments()
	if len(remainingSegments) != initialCount-1 {
		t.Errorf("expected %d remaining, got %d", initialCount-1, len(remainingSegments))
	}

	w.Close()
}

func TestWriter_Recovery(t *testing.T) {
	tmpDir := t.TempDir()

	now := time.Now().UnixMilli()

	// Write some data
	{
		w, err := NewWriter(tmpDir, DefaultOptions())
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}

		for i := 0; i < 10; i++ {
			samples := []types.Sample{
				{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
			}
			if err := w.Write(samples); err != nil {
				t.Fatalf("Write %d: %v", i, err)
			}
		}

		w.Sync()
		w.Close()
	}

	// Re-open (recovery scenario)
	{
		w, err := NewWriter(tmpDir, DefaultOptions())
		if err != nil {
			t.Fatalf("NewWriter after recovery: %v", err)
		}
		defer w.Close()

		// Should create new segment
		segments, _ := w.ListSegments()
		if len(segments) != 2 {
			t.Errorf("expected 2 segments after recovery, got %d", len(segments))
		}

		// Write more
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + 100, Value: 100, Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			t.Fatalf("Write after recovery: %v", err)
		}
	}

	// Verify all data
	entries, _ := os.ReadDir(tmpDir)
	var allPaths []string
	for _, e := range entries {
		if !e.IsDir() {
			allPaths = append(allPaths, filepath.Join(tmpDir, e.Name()))
		}
	}

	allSamples, err := ReadAllSegments(allPaths)
	if err != nil {
		t.Fatalf("ReadAllSegments: %v", err)
	}

	if len(allSamples) != 11 {
		t.Errorf("expected 11 samples total, got %d", len(allSamples))
	}
}

func TestReader_InvalidFile(t *testing.T) {
	tmpDir := t.TempDir()
	invalidPath := filepath.Join(tmpDir, "invalid.wal")

	// Create invalid file
	if err := os.WriteFile(invalidPath, []byte("invalid content"), 0644); err != nil {
		t.Fatalf("write invalid file: %v", err)
	}

	_, err := NewReader(invalidPath)
	if err == nil {
		t.Error("expected error for invalid file")
	}
}

func BenchmarkWriter_Write(b *testing.B) {
	tmpDir := b.TempDir()

	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}
	defer w.Close()

	now := time.Now().UnixMilli()
	samples := make([]types.Sample, 100)
	for i := range samples {
		samples[i] = types.Sample{
			Namespace:   "prod",
			Target:      "router-01",
			Poller:      "cpu",
			TimestampMs: now + int64(i),
			Value:       float64(i),
			Valid:       true,
			PollMs:      25,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Write(samples); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}
}

func BenchmarkReader_ReadAll(b *testing.B) {
	tmpDir := b.TempDir()

	// Write test data
	w, err := NewWriter(tmpDir, DefaultOptions())
	if err != nil {
		b.Fatalf("NewWriter: %v", err)
	}

	now := time.Now().UnixMilli()
	for i := 0; i < 1000; i++ {
		samples := []types.Sample{
			{Namespace: "prod", Target: "r1", Poller: "cpu", TimestampMs: now + int64(i), Value: float64(i), Valid: true, PollMs: 25},
		}
		if err := w.Write(samples); err != nil {
			b.Fatalf("Write: %v", err)
		}
	}

	segmentPath := w.CurrentSegment()
	w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := NewReader(segmentPath)
		r.ReadAll()
		r.Close()
	}
}
