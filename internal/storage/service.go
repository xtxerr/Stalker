package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/internal/storage/backpressure"
	"github.com/xtxerr/stalker/internal/storage/buffer"
	"github.com/xtxerr/stalker/internal/storage/compaction"
	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/ingestion"
	"github.com/xtxerr/stalker/internal/storage/query"
	"github.com/xtxerr/stalker/internal/storage/retention"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// Service is the main storage service that orchestrates all components.
type Service struct {
	mu sync.RWMutex

	config *config.Config

	// Components
	ingestion   *ingestion.Service
	compaction  *compaction.Engine
	query       *query.Service
	backpressure *backpressure.Controller
	retention   *retention.Manager

	// State
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Statistics
	startTime time.Time
}

// New creates a new storage service.
func New(cfg *config.Config) (*Service, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Ensure directories exist
	if err := cfg.EnsureDirectories(); err != nil {
		return nil, fmt.Errorf("ensure directories: %w", err)
	}

	// Create ingestion service
	ing, err := ingestion.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("create ingestion: %w", err)
	}

	// Create compaction engine
	comp, err := compaction.New(cfg)
	if err != nil {
		ing.Stop()
		return nil, fmt.Errorf("create compaction: %w", err)
	}

	// Create query service (with ingestion buffer)
	qry, err := query.New(cfg, ing.Buffer())
	if err != nil {
		ing.Stop()
		comp.Stop()
		return nil, fmt.Errorf("create query: %w", err)
	}

	// Create backpressure controller
	bp := backpressure.New(cfg, ing.Buffer())

	// Create retention manager
	ret := retention.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())

	return &Service{
		config:       cfg,
		ingestion:    ing,
		compaction:   comp,
		query:        qry,
		backpressure: bp,
		retention:    ret,
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Start starts all components.
func (s *Service) Start() error {
	if s.running.Load() {
		return fmt.Errorf("service already running")
	}

	s.running.Store(true)
	s.startTime = time.Now()

	// Start ingestion
	if err := s.ingestion.Start(); err != nil {
		s.running.Store(false)
		return fmt.Errorf("start ingestion: %w", err)
	}

	// Start compaction
	if err := s.compaction.Start(); err != nil {
		s.ingestion.Stop()
		s.running.Store(false)
		return fmt.Errorf("start compaction: %w", err)
	}

	// Start background workers
	s.wg.Add(1)
	go s.backpressureWorker()

	s.wg.Add(1)
	go s.retentionWorker()

	// Setup backpressure callbacks
	s.backpressure.SetOnLevelChange(s.onBackpressureChange)

	return nil
}

// Stop stops all components gracefully.
func (s *Service) Stop() error {
	if !s.running.Load() {
		return nil
	}

	s.running.Store(false)
	s.cancel()

	// Wait for background workers
	s.wg.Wait()

	// Stop components in reverse order
	var errs []error

	if err := s.compaction.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stop compaction: %w", err))
	}

	if err := s.ingestion.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("stop ingestion: %w", err))
	}

	if err := s.query.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close query: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("stop errors: %v", errs)
	}

	return nil
}

// Ingest ingests samples into the storage system.
func (s *Service) Ingest(samples []types.Sample) error {
	if !s.running.Load() {
		return fmt.Errorf("service not running")
	}

	// Check backpressure
	if s.backpressure.ShouldDrop() {
		s.backpressure.RecordDrop()
		return fmt.Errorf("samples dropped due to backpressure")
	}

	// Apply throttling if needed
	if s.backpressure.ShouldThrottle() {
		delay := s.backpressure.ThrottleDelay()
		if delay > 0 {
			time.Sleep(delay)
		}
	}

	return s.ingestion.Ingest(samples)
}

// IngestSingle ingests a single sample.
func (s *Service) IngestSingle(sample types.Sample) error {
	return s.Ingest([]types.Sample{sample})
}

// Query executes a poller query.
func (s *Service) Query(ctx context.Context, q query.PollerQuery) ([]types.AggregateResult, error) {
	if !s.running.Load() {
		return nil, fmt.Errorf("service not running")
	}

	return s.query.QueryPoller(ctx, q)
}

// QueryTimeRange executes a time range query.
func (s *Service) QueryTimeRange(ctx context.Context, q query.TimeRangeQuery) ([]types.AggregateResult, error) {
	if !s.running.Load() {
		return nil, fmt.Errorf("service not running")
	}

	return s.query.QueryTimeRange(ctx, q)
}

// QuerySQL executes a raw SQL query.
func (s *Service) QuerySQL(ctx context.Context, sql string) ([]map[string]interface{}, error) {
	if !s.running.Load() {
		return nil, fmt.Errorf("service not running")
	}

	return s.query.ExecuteSQL(ctx, sql)
}

// backpressureWorker periodically checks backpressure.
func (s *Service) backpressureWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.backpressure.Check()
		}
	}
}

// retentionWorker periodically runs retention cleanup.
func (s *Service) retentionWorker() {
	defer s.wg.Done()

	// Run daily at 5 AM
	for {
		now := time.Now()
		next := time.Date(now.Year(), now.Month(), now.Day()+1, 5, 0, 0, 0, now.Location())
		if now.Hour() >= 5 {
			next = next.Add(24 * time.Hour)
		}

		select {
		case <-s.ctx.Done():
			return
		case <-time.After(next.Sub(now)):
			s.retention.RunCleanup()
		}
	}
}

// onBackpressureChange handles backpressure level changes.
func (s *Service) onBackpressureChange(old, new backpressure.Level) {
	// Pause compaction during high load
	if new >= backpressure.LevelWarning {
		// Compaction will check ShouldPauseCompaction
	}
}

// Stats returns combined statistics.
func (s *Service) Stats() ServiceStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var uptime time.Duration
	if !s.startTime.IsZero() {
		uptime = time.Since(s.startTime)
	}

	return ServiceStats{
		Running:     s.running.Load(),
		Uptime:      uptime,
		Ingestion:   s.ingestion.Stats(),
		Compaction:  s.compaction.Stats(),
		Query:       s.query.Stats(),
		Backpressure: s.backpressure.Stats(),
		Retention:   s.retention.Stats(),
	}
}

// ServiceStats holds combined statistics.
type ServiceStats struct {
	Running      bool
	Uptime       time.Duration
	Ingestion    ingestion.ServiceStats
	Compaction   compaction.EngineStats
	Query        query.ServiceStats
	Backpressure backpressure.ControllerStats
	Retention    retention.ManagerStats
}

// Buffer returns the raw sample buffer for direct queries.
func (s *Service) Buffer() *buffer.RingBuffer {
	return s.ingestion.Buffer()
}

// Config returns the current configuration.
func (s *Service) Config() *config.Config {
	return s.config
}

// IsRunning returns whether the service is running.
func (s *Service) IsRunning() bool {
	return s.running.Load()
}

// ForceFlush forces an immediate flush of all buffers.
func (s *Service) ForceFlush() {
	s.ingestion.ForceFlush()
}

// RunRetention manually triggers retention cleanup.
func (s *Service) RunRetention() []retention.CleanupResult {
	return s.retention.RunCleanup()
}

// DryRunRetention simulates retention cleanup.
func (s *Service) DryRunRetention() []retention.CleanupResult {
	return s.retention.DryRun()
}

// GetDiskUsage returns disk usage per tier.
func (s *Service) GetDiskUsage() map[types.Tier]retention.DiskUsage {
	return s.retention.GetDiskUsage()
}

// BackpressureLevel returns the current backpressure level.
func (s *Service) BackpressureLevel() backpressure.Level {
	return s.backpressure.CurrentLevel()
}
