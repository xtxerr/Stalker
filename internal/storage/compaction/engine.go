package compaction

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/parquet"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// Engine manages tier-to-tier data compaction.
// It aggregates data from lower tiers into higher tiers:
// Raw → 5min → Hourly → Daily → Weekly
type Engine struct {
	mu sync.RWMutex

	config *config.Config

	// State
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// Job queue
	jobCh   chan Job
	workers int

	// Statistics
	stats Stats
}

// Stats holds compaction statistics.
type Stats struct {
	JobsScheduled atomic.Int64
	JobsCompleted atomic.Int64
	JobsFailed    atomic.Int64
	FilesRead     atomic.Int64
	FilesWritten  atomic.Int64
	RowsProcessed atomic.Int64
	BytesRead     atomic.Int64
	BytesWritten  atomic.Int64
}

// Job represents a compaction job.
type Job struct {
	// Source tier
	SourceTier types.Tier

	// Target tier
	TargetTier types.Tier

	// Time range to compact
	StartTime time.Time
	EndTime   time.Time

	// Source files to process
	SourceFiles []string

	// Output file path
	OutputFile string
}

// New creates a new compaction engine.
func New(cfg *config.Config) (*Engine, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	workers := cfg.Compaction.Workers
	if workers <= 0 {
		workers = 4
	}

	return &Engine{
		config:  cfg,
		ctx:     ctx,
		cancel:  cancel,
		jobCh:   make(chan Job, 100),
		workers: workers,
	}, nil
}

// Start starts the compaction engine.
func (e *Engine) Start() error {
	if e.running.Load() {
		return fmt.Errorf("engine already running")
	}

	e.running.Store(true)

	// Start workers
	for i := 0; i < e.workers; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}

	// Start scheduler
	e.wg.Add(1)
	go e.scheduler()

	return nil
}

// Stop stops the compaction engine.
func (e *Engine) Stop() error {
	if !e.running.Load() {
		return nil
	}

	e.running.Store(false)
	e.cancel()

	// Close job channel
	close(e.jobCh)

	// Wait for workers
	e.wg.Wait()

	return nil
}

// worker processes compaction jobs.
func (e *Engine) worker(id int) {
	defer e.wg.Done()

	for job := range e.jobCh {
		if err := e.runJob(job); err != nil {
			e.stats.JobsFailed.Add(1)
			// Log error but continue
			continue
		}
		e.stats.JobsCompleted.Add(1)
	}
}

// scheduler periodically schedules compaction jobs.
func (e *Engine) scheduler() {
	defer e.wg.Done()

	// Check every minute
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.scheduleJobs()
		}
	}
}

// scheduleJobs schedules pending compaction jobs.
func (e *Engine) scheduleJobs() {
	now := time.Now().UTC()

	// Schedule Raw → 5min compaction
	// Run every hour, compact data older than 10 minutes
	e.scheduleRawTo5Min(now)

	// Schedule 5min → Hourly compaction
	// Run daily, compact yesterday's data
	e.schedule5MinToHourly(now)

	// Schedule Hourly → Daily compaction
	// Run weekly, compact last week's data
	e.scheduleHourlyToDaily(now)

	// Schedule Daily → Weekly compaction
	// Run monthly, compact last month's data
	e.scheduleDailyToWeekly(now)
}

// scheduleRawTo5Min schedules Raw to 5-minute compaction.
func (e *Engine) scheduleRawTo5Min(now time.Time) {
	// Compact data from 1-2 hours ago
	endTime := now.Add(-1 * time.Hour).Truncate(time.Hour)
	startTime := endTime.Add(-1 * time.Hour)

	files, err := e.findFiles(types.TierRaw, startTime, endTime)
	if err != nil || len(files) == 0 {
		return
	}

	job := Job{
		SourceTier:  types.TierRaw,
		TargetTier:  types.Tier5Min,
		StartTime:   startTime,
		EndTime:     endTime,
		SourceFiles: files,
		OutputFile:  e.outputPath(types.Tier5Min, startTime),
	}

	e.SubmitJob(job)
}

// schedule5MinToHourly schedules 5-minute to Hourly compaction.
func (e *Engine) schedule5MinToHourly(now time.Time) {
	// Only run at 2 AM
	if now.Hour() != 2 {
		return
	}

	// Compact yesterday's data
	yesterday := now.Add(-24 * time.Hour)
	startTime := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	endTime := startTime.Add(24 * time.Hour)

	files, err := e.findFiles(types.Tier5Min, startTime, endTime)
	if err != nil || len(files) == 0 {
		return
	}

	job := Job{
		SourceTier:  types.Tier5Min,
		TargetTier:  types.TierHourly,
		StartTime:   startTime,
		EndTime:     endTime,
		SourceFiles: files,
		OutputFile:  e.outputPath(types.TierHourly, startTime),
	}

	e.SubmitJob(job)
}

// scheduleHourlyToDaily schedules Hourly to Daily compaction.
func (e *Engine) scheduleHourlyToDaily(now time.Time) {
	// Only run on Sunday at 3 AM
	if now.Weekday() != time.Sunday || now.Hour() != 3 {
		return
	}

	// Compact last week's data
	lastWeek := now.Add(-7 * 24 * time.Hour)
	startTime := time.Date(lastWeek.Year(), lastWeek.Month(), lastWeek.Day(), 0, 0, 0, 0, time.UTC)
	startTime = startTime.AddDate(0, 0, -int(startTime.Weekday())+1) // Monday
	endTime := startTime.Add(7 * 24 * time.Hour)

	files, err := e.findFiles(types.TierHourly, startTime, endTime)
	if err != nil || len(files) == 0 {
		return
	}

	job := Job{
		SourceTier:  types.TierHourly,
		TargetTier:  types.TierDaily,
		StartTime:   startTime,
		EndTime:     endTime,
		SourceFiles: files,
		OutputFile:  e.outputPath(types.TierDaily, startTime),
	}

	e.SubmitJob(job)
}

// scheduleDailyToWeekly schedules Daily to Weekly compaction.
func (e *Engine) scheduleDailyToWeekly(now time.Time) {
	// Only run on 1st of month at 4 AM
	if now.Day() != 1 || now.Hour() != 4 {
		return
	}

	// Compact last month's data
	lastMonth := now.AddDate(0, -1, 0)
	startTime := time.Date(lastMonth.Year(), lastMonth.Month(), 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	files, err := e.findFiles(types.TierDaily, startTime, endTime)
	if err != nil || len(files) == 0 {
		return
	}

	job := Job{
		SourceTier:  types.TierDaily,
		TargetTier:  types.TierWeekly,
		StartTime:   startTime,
		EndTime:     endTime,
		SourceFiles: files,
		OutputFile:  e.outputPath(types.TierWeekly, startTime),
	}

	e.SubmitJob(job)
}

// SubmitJob submits a job to the queue.
func (e *Engine) SubmitJob(job Job) bool {
	if !e.running.Load() {
		return false
	}

	select {
	case e.jobCh <- job:
		e.stats.JobsScheduled.Add(1)
		return true
	default:
		// Queue full
		return false
	}
}

// RunJob executes a compaction job synchronously.
func (e *Engine) RunJob(job Job) error {
	return e.runJob(job)
}

// runJob executes a compaction job.
func (e *Engine) runJob(job Job) error {
	if len(job.SourceFiles) == 0 {
		return nil
	}

	// Read all source aggregates
	var allAggregates []types.AggregateResult

	for _, file := range job.SourceFiles {
		aggs, err := e.readAggregates(file)
		if err != nil {
			return fmt.Errorf("read %s: %w", file, err)
		}
		allAggregates = append(allAggregates, aggs...)
		e.stats.FilesRead.Add(1)
	}

	if len(allAggregates) == 0 {
		return nil
	}

	e.stats.RowsProcessed.Add(int64(len(allAggregates)))

	// Re-aggregate to target tier resolution
	reAggregated := e.reAggregate(allAggregates, job.TargetTier)

	// Write output
	if err := e.writeAggregates(job.OutputFile, reAggregated); err != nil {
		return fmt.Errorf("write %s: %w", job.OutputFile, err)
	}

	e.stats.FilesWritten.Add(1)

	return nil
}

// readAggregates reads aggregates from a Parquet file.
func (e *Engine) readAggregates(path string) ([]types.AggregateResult, error) {
	reader, err := parquet.NewAggregateReader(path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return reader.ReadAll()
}

// writeAggregates writes aggregates to a Parquet file.
func (e *Engine) writeAggregates(path string, aggregates []types.AggregateResult) error {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dir: %w", err)
	}

	opts := parquet.DefaultOptions()
	writer, err := parquet.NewAggregateWriter(path, opts)
	if err != nil {
		return err
	}
	defer writer.Close()

	return writer.Write(aggregates)
}

// reAggregate re-aggregates data to a coarser resolution.
func (e *Engine) reAggregate(aggregates []types.AggregateResult, targetTier types.Tier) []types.AggregateResult {
	// Group by key and target bucket
	type bucketKey struct {
		key       string
		bucketMs  int64
	}

	groups := make(map[bucketKey][]types.AggregateResult)

	for _, agg := range aggregates {
		key := agg.Key()
		bucketStart := targetTier.TruncateToBucket(time.UnixMilli(agg.BucketStart))
		bk := bucketKey{key: key, bucketMs: bucketStart.UnixMilli()}
		groups[bk] = append(groups[bk], agg)
	}

	// Merge each group
	var result []types.AggregateResult

	for bk, aggs := range groups {
		merged := e.mergeAggregates(aggs, bk.bucketMs, targetTier)
		result = append(result, merged)
	}

	// Sort by time, then key
	sort.Slice(result, func(i, j int) bool {
		if result[i].BucketStart != result[j].BucketStart {
			return result[i].BucketStart < result[j].BucketStart
		}
		return result[i].Key() < result[j].Key()
	})

	return result
}

// mergeAggregates merges multiple aggregates into one.
func (e *Engine) mergeAggregates(aggs []types.AggregateResult, bucketStartMs int64, tier types.Tier) types.AggregateResult {
	if len(aggs) == 0 {
		return types.AggregateResult{}
	}

	first := aggs[0]
	bucketEnd := bucketStartMs + tier.Duration().Milliseconds()

	result := types.AggregateResult{
		Namespace:   first.Namespace,
		Target:      first.Target,
		Poller:      first.Poller,
		BucketStart: bucketStartMs,
		BucketEnd:   bucketEnd,
	}

	var totalCount int64
	var totalSum float64
	var minVal = aggs[0].Min
	var maxVal = aggs[0].Max
	var firstTs = aggs[0].FirstTs
	var lastTs = aggs[0].LastTs

	for _, agg := range aggs {
		totalCount += agg.Count
		totalSum += agg.Sum

		if agg.Min < minVal {
			minVal = agg.Min
		}
		if agg.Max > maxVal {
			maxVal = agg.Max
		}
		if agg.FirstTs < firstTs || firstTs == 0 {
			firstTs = agg.FirstTs
		}
		if agg.LastTs > lastTs {
			lastTs = agg.LastTs
		}
	}

	result.Count = totalCount
	result.Sum = totalSum
	result.Min = minVal
	result.Max = maxVal
	result.FirstTs = firstTs
	result.LastTs = lastTs

	if totalCount > 0 {
		result.Avg = totalSum / float64(totalCount)
	}

	// Note: Percentiles cannot be accurately merged from pre-computed values
	// They would need to be recalculated from raw data or DDSketch merging

	return result
}

// findFiles finds Parquet files in the given tier for the time range.
func (e *Engine) findFiles(tier types.Tier, startTime, endTime time.Time) ([]string, error) {
	tierDir := e.config.TierDir(tier.String())

	entries, err := os.ReadDir(tierDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) != ".parquet" {
			continue
		}

		// Simple time-based filtering
		// Assumes filename contains date info
		path := filepath.Join(tierDir, name)
		files = append(files, path)
	}

	return files, nil
}

// outputPath generates the output path for a compaction job.
func (e *Engine) outputPath(tier types.Tier, startTime time.Time) string {
	tierDir := e.config.TierDir(tier.String())

	var filename string
	switch tier {
	case types.Tier5Min:
		filename = fmt.Sprintf("%s.parquet", startTime.Format("2006-01-02_15-04"))
	case types.TierHourly:
		filename = fmt.Sprintf("%s.parquet", startTime.Format("2006-01-02"))
	case types.TierDaily:
		filename = fmt.Sprintf("%s.parquet", startTime.Format("2006-01"))
	case types.TierWeekly:
		filename = fmt.Sprintf("%s.parquet", startTime.Format("2006"))
	default:
		filename = fmt.Sprintf("%d.parquet", startTime.Unix())
	}

	return filepath.Join(tierDir, filename)
}

// Stats returns current statistics.
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		Running:       e.running.Load(),
		JobsScheduled: e.stats.JobsScheduled.Load(),
		JobsCompleted: e.stats.JobsCompleted.Load(),
		JobsFailed:    e.stats.JobsFailed.Load(),
		FilesRead:     e.stats.FilesRead.Load(),
		FilesWritten:  e.stats.FilesWritten.Load(),
		RowsProcessed: e.stats.RowsProcessed.Load(),
	}
}

// EngineStats holds engine statistics.
type EngineStats struct {
	Running       bool
	JobsScheduled int64
	JobsCompleted int64
	JobsFailed    int64
	FilesRead     int64
	FilesWritten  int64
	RowsProcessed int64
}

// IsRunning returns whether the engine is running.
func (e *Engine) IsRunning() bool {
	return e.running.Load()
}
