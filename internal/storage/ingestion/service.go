package ingestion

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/internal/storage/aggregate"
	"github.com/xtxerr/stalker/internal/storage/buffer"
	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/parquet"
	"github.com/xtxerr/stalker/internal/storage/types"
	"github.com/xtxerr/stalker/internal/storage/wal"
)

// Service orchestrates the sample ingestion pipeline.
// It manages the flow: Samples → Buffer → WAL → Aggregation → Parquet
type Service struct {
	mu sync.RWMutex

	config *config.Config

	// Components
	buffer    *buffer.RingBuffer
	wal       *wal.Writer
	aggregate *aggregate.Manager

	// State
	running  atomic.Bool
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// Statistics
	stats Stats

	// Channels
	flushCh chan struct{}
}

// Stats holds ingestion statistics.
type Stats struct {
	SamplesReceived   atomic.Int64
	SamplesIngested   atomic.Int64
	SamplesDropped    atomic.Int64
	BatchesProcessed  atomic.Int64
	FlushesCompleted  atomic.Int64
	AggregatesWritten atomic.Int64
	Errors            atomic.Int64
}

// New creates a new ingestion service.
func New(cfg *config.Config) (*Service, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// Calculate buffer capacity based on config
	bufferCapacity := calculateBufferCapacity(cfg)

	// Create ring buffer
	ringBuffer := buffer.New(bufferCapacity)

	// Create WAL writer
	walOpts := wal.Options{
		MaxSegmentSize: cfg.Ingestion.WAL.MaxSegmentSize,
		SyncMode:       cfg.Ingestion.WAL.SyncMode,
		SyncInterval:   cfg.Ingestion.WAL.SyncInterval,
	}

	walWriter, err := wal.NewWriter(cfg.WALDir(), walOpts)
	if err != nil {
		return nil, fmt.Errorf("create WAL writer: %w", err)
	}

	// Create aggregate manager (5-minute buckets)
	aggManager := aggregate.NewManager(5*time.Minute, cfg.Features.Percentile.Enabled)

	ctx, cancel := context.WithCancel(context.Background())

	return &Service{
		config:    cfg,
		buffer:    ringBuffer,
		wal:       walWriter,
		aggregate: aggManager,
		ctx:       ctx,
		cancel:    cancel,
		flushCh:   make(chan struct{}, 1),
	}, nil
}

// calculateBufferCapacity calculates buffer capacity based on config.
func calculateBufferCapacity(cfg *config.Config) int {
	if !cfg.Features.RawBuffer.Enabled {
		return 10000 // Minimal buffer
	}

	// Calculate based on duration
	samplesPerSec := cfg.Scale.PollerCount / cfg.Scale.PollIntervalSec
	bufferDurationSec := int(cfg.Features.RawBuffer.Duration.Seconds())

	capacity := samplesPerSec * bufferDurationSec
	if capacity < 10000 {
		capacity = 10000
	}
	if capacity > 100000000 { // 100M max
		capacity = 100000000
	}

	return capacity
}

// Start starts the ingestion service.
func (s *Service) Start() error {
	if s.running.Load() {
		return fmt.Errorf("service already running")
	}

	s.running.Store(true)

	// Start flush worker
	s.wg.Add(1)
	go s.flushWorker()

	// Start eviction worker
	s.wg.Add(1)
	go s.evictionWorker()

	return nil
}

// Stop stops the ingestion service gracefully.
func (s *Service) Stop() error {
	if !s.running.Load() {
		return nil
	}

	s.running.Store(false)
	s.cancel()

	// Wait for workers
	s.wg.Wait()

	// Final flush
	s.flushAll()

	// Close WAL
	if err := s.wal.Close(); err != nil {
		return fmt.Errorf("close WAL: %w", err)
	}

	return nil
}

// Ingest ingests a batch of samples.
func (s *Service) Ingest(samples []types.Sample) error {
	if !s.running.Load() {
		return fmt.Errorf("service not running")
	}

	if len(samples) == 0 {
		return nil
	}

	s.stats.SamplesReceived.Add(int64(len(samples)))

	// Write to WAL first (crash safety)
	if err := s.wal.Write(samples); err != nil {
		s.stats.Errors.Add(1)
		return fmt.Errorf("WAL write: %w", err)
	}

	// Add to buffer and aggregation
	ingested := 0
	dropped := 0

	for i := range samples {
		sample := &samples[i]

		// Add to ring buffer
		if s.buffer.Push(*sample) {
			ingested++
		} else {
			dropped++
		}

		// Add to aggregation
		s.aggregate.Process(*sample)
	}

	s.stats.SamplesIngested.Add(int64(ingested))
	s.stats.SamplesDropped.Add(int64(dropped))
	s.stats.BatchesProcessed.Add(1)

	return nil
}

// IngestSingle ingests a single sample.
func (s *Service) IngestSingle(sample types.Sample) error {
	return s.Ingest([]types.Sample{sample})
}

// flushWorker periodically flushes completed aggregates.
func (s *Service) flushWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.Ingestion.Flush.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.flushCompleted()
		case <-s.flushCh:
			s.flushCompleted()
		}
	}
}

// evictionWorker periodically evicts old samples from the buffer.
func (s *Service) evictionWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.evictOldSamples()
		}
	}
}

// flushCompleted flushes completed aggregates to Parquet.
func (s *Service) flushCompleted() {
	// Get completed aggregates
	aggregates := s.aggregate.FlushCompleted()
	if len(aggregates) == 0 {
		return
	}

	// Write to Parquet
	if err := s.writeAggregates(aggregates); err != nil {
		s.stats.Errors.Add(1)
		// Log error but don't fail
		return
	}

	s.stats.AggregatesWritten.Add(int64(len(aggregates)))
	s.stats.FlushesCompleted.Add(1)
}

// flushAll flushes all data (used during shutdown).
func (s *Service) flushAll() {
	// Flush all aggregates
	aggregates := s.aggregate.FlushAll()
	if len(aggregates) > 0 {
		s.writeAggregates(aggregates)
		s.stats.AggregatesWritten.Add(int64(len(aggregates)))
	}

	// Sync WAL
	s.wal.Sync()
}

// writeAggregates writes aggregates to a Parquet file.
func (s *Service) writeAggregates(aggregates []types.AggregateResult) error {
	if len(aggregates) == 0 {
		return nil
	}

	// Generate filename based on bucket time
	bucketTime := time.UnixMilli(aggregates[0].BucketStart)
	filename := fmt.Sprintf("%s.parquet", bucketTime.Format("2006-01-02_15-04"))
	path := filepath.Join(s.config.TierDir("5min"), filename)

	// Create writer
	opts := parquet.DefaultOptions()
	opts.Compression = parquet.CompressionType(s.config.Features.Compression.Level)

	writer, err := parquet.NewWriter(path, opts)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}
	defer writer.Close()

	// Write aggregates
	if err := writer.WriteAggregates(aggregates); err != nil {
		return fmt.Errorf("write aggregates: %w", err)
	}

	return nil
}

// evictOldSamples evicts samples older than the configured duration.
func (s *Service) evictOldSamples() {
	if !s.config.Features.RawBuffer.Enabled {
		return
	}

	cutoff := time.Now().Add(-s.config.Features.RawBuffer.Duration).UnixMilli()
	s.buffer.EvictOlderThan(cutoff)
}

// ForceFlush triggers an immediate flush.
func (s *Service) ForceFlush() {
	select {
	case s.flushCh <- struct{}{}:
	default:
		// Flush already pending
	}
}

// Stats returns current statistics.
func (s *Service) Stats() ServiceStats {
	bufferStats := s.buffer.Stats()
	walStats := s.wal.Stats()
	aggStats := s.aggregate.Stats()

	return ServiceStats{
		Running:           s.running.Load(),
		SamplesReceived:   s.stats.SamplesReceived.Load(),
		SamplesIngested:   s.stats.SamplesIngested.Load(),
		SamplesDropped:    s.stats.SamplesDropped.Load(),
		BatchesProcessed:  s.stats.BatchesProcessed.Load(),
		FlushesCompleted:  s.stats.FlushesCompleted.Load(),
		AggregatesWritten: s.stats.AggregatesWritten.Load(),
		Errors:            s.stats.Errors.Load(),
		BufferUsage:       bufferStats.UsageRatio,
		BufferCount:       bufferStats.Count,
		WALSegments:       walStats.SegmentsCreated,
		WALBytesWritten:   walStats.BytesWritten,
		ActiveAggregates:  aggStats.ActiveAggregates,
	}
}

// ServiceStats holds combined service statistics.
type ServiceStats struct {
	Running           bool
	SamplesReceived   int64
	SamplesIngested   int64
	SamplesDropped    int64
	BatchesProcessed  int64
	FlushesCompleted  int64
	AggregatesWritten int64
	Errors            int64
	BufferUsage       float64
	BufferCount       int
	WALSegments       int64
	WALBytesWritten   int64
	ActiveAggregates  int64
}

// Buffer returns the ring buffer for queries.
func (s *Service) Buffer() *buffer.RingBuffer {
	return s.buffer
}

// AggregateManager returns the aggregate manager.
func (s *Service) AggregateManager() *aggregate.Manager {
	return s.aggregate
}

// IsRunning returns whether the service is running.
func (s *Service) IsRunning() bool {
	return s.running.Load()
}
