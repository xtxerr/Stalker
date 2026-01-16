package query

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/xtxerr/stalker/internal/storage/buffer"
	"github.com/xtxerr/stalker/internal/storage/config"
	"github.com/xtxerr/stalker/internal/storage/types"
)

// Service provides query capabilities over stored data.
// It uses DuckDB to query Parquet files and combines results with hot data from the ring buffer.
type Service struct {
	mu sync.RWMutex

	config *config.Config
	db     *sql.DB
	buffer *buffer.RingBuffer

	// Statistics
	stats Stats
}

// Stats holds query statistics.
type Stats struct {
	QueriesExecuted int64
	RowsReturned    int64
	Errors          int64
	CacheHits       int64
	CacheMisses     int64
}

// PollerQuery defines parameters for querying a specific poller.
type PollerQuery struct {
	Namespace string
	Target    string
	Poller    string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
}

// TimeRangeQuery defines parameters for querying a time range.
type TimeRangeQuery struct {
	Namespace string
	Target    string
	StartTime time.Time
	EndTime   time.Time
	Limit     int
}

// New creates a new query service.
func New(cfg *config.Config, buf *buffer.RingBuffer) (*Service, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// Open in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	// Configure DuckDB
	if cfg.Query.MemoryLimit != "" {
		_, err = db.Exec(fmt.Sprintf("SET memory_limit='%s'", cfg.Query.MemoryLimit))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("set memory limit: %w", err)
		}
	}

	return &Service{
		config: cfg,
		db:     db,
		buffer: buf,
	}, nil
}

// Close closes the query service.
func (s *Service) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// QueryPoller queries data for a specific poller.
func (s *Service) QueryPoller(ctx context.Context, q PollerQuery) ([]types.AggregateResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Select appropriate tier based on time range
	tier := types.SelectTierForRange(q.StartTime, q.EndTime)

	// Query Parquet files
	parquetResults, err := s.queryParquet(ctx, tier, q)
	if err != nil {
		s.stats.Errors++
		return nil, fmt.Errorf("query parquet: %w", err)
	}

	// Query hot data from buffer
	bufferResults := s.queryBuffer(q)

	// Merge results
	results := s.mergeResults(parquetResults, bufferResults)

	// Apply limit
	if q.Limit > 0 && len(results) > q.Limit {
		results = results[:q.Limit]
	}

	s.stats.QueriesExecuted++
	s.stats.RowsReturned += int64(len(results))

	return results, nil
}

// QueryTimeRange queries all data in a time range.
func (s *Service) QueryTimeRange(ctx context.Context, q TimeRangeQuery) ([]types.AggregateResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tier := types.SelectTierForRange(q.StartTime, q.EndTime)

	results, err := s.queryParquetRange(ctx, tier, q)
	if err != nil {
		s.stats.Errors++
		return nil, err
	}

	if q.Limit > 0 && len(results) > q.Limit {
		results = results[:q.Limit]
	}

	s.stats.QueriesExecuted++
	s.stats.RowsReturned += int64(len(results))

	return results, nil
}

// queryParquet queries Parquet files using DuckDB.
func (s *Service) queryParquet(ctx context.Context, tier types.Tier, q PollerQuery) ([]types.AggregateResult, error) {
	tierDir := s.config.TierDir(tier.String())
	pattern := filepath.Join(tierDir, "*.parquet")

	query := `
		SELECT 
			namespace, target, poller,
			bucket_start, bucket_end,
			count, sum, min, max, avg,
			p50, p90, p95, p99,
			first_ts, last_ts
		FROM read_parquet($1)
		WHERE namespace = $2 
		  AND target = $3 
		  AND poller = $4
		  AND bucket_start >= $5
		  AND bucket_end <= $6
		ORDER BY bucket_start
	`

	rows, err := s.db.QueryContext(ctx, query,
		pattern,
		q.Namespace,
		q.Target,
		q.Poller,
		q.StartTime.UnixMilli(),
		q.EndTime.UnixMilli(),
	)
	if err != nil {
		// If no files exist, return empty result
		return nil, nil
	}
	defer rows.Close()

	return s.scanAggregates(rows)
}

// queryParquetRange queries Parquet files for a time range.
func (s *Service) queryParquetRange(ctx context.Context, tier types.Tier, q TimeRangeQuery) ([]types.AggregateResult, error) {
	tierDir := s.config.TierDir(tier.String())
	pattern := filepath.Join(tierDir, "*.parquet")

	query := `
		SELECT 
			namespace, target, poller,
			bucket_start, bucket_end,
			count, sum, min, max, avg,
			p50, p90, p95, p99,
			first_ts, last_ts
		FROM read_parquet($1)
		WHERE namespace = $2 
		  AND target = $3
		  AND bucket_start >= $4
		  AND bucket_end <= $5
		ORDER BY bucket_start, poller
	`

	rows, err := s.db.QueryContext(ctx, query,
		pattern,
		q.Namespace,
		q.Target,
		q.StartTime.UnixMilli(),
		q.EndTime.UnixMilli(),
	)
	if err != nil {
		return nil, nil
	}
	defer rows.Close()

	return s.scanAggregates(rows)
}

// scanAggregates scans rows into AggregateResult slice.
func (s *Service) scanAggregates(rows *sql.Rows) ([]types.AggregateResult, error) {
	var results []types.AggregateResult

	for rows.Next() {
		var r types.AggregateResult
		var p50, p90, p95, p99 sql.NullFloat64

		err := rows.Scan(
			&r.Namespace, &r.Target, &r.Poller,
			&r.BucketStart, &r.BucketEnd,
			&r.Count, &r.Sum, &r.Min, &r.Max, &r.Avg,
			&p50, &p90, &p95, &p99,
			&r.FirstTs, &r.LastTs,
		)
		if err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		if p50.Valid {
			r.SetPercentiles(p50.Float64, p90.Float64, p95.Float64, p99.Float64)
		}

		results = append(results, r)
	}

	return results, rows.Err()
}

// queryBuffer queries the ring buffer for recent data.
func (s *Service) queryBuffer(q PollerQuery) []types.AggregateResult {
	if s.buffer == nil {
		return nil
	}

	// Query raw samples from buffer
	samples := s.buffer.Query(buffer.SampleFilter{
		Namespace: q.Namespace,
		Target:    q.Target,
		Poller:    q.Poller,
		Since:     q.StartTime.UnixMilli(),
		Until:     q.EndTime.UnixMilli(),
	}, 0)

	if len(samples) == 0 {
		return nil
	}

	// Aggregate samples into a single result
	return s.aggregateSamples(samples, q.StartTime, q.EndTime)
}

// aggregateSamples aggregates raw samples into AggregateResults.
func (s *Service) aggregateSamples(samples []types.Sample, startTime, endTime time.Time) []types.AggregateResult {
	if len(samples) == 0 {
		return nil
	}

	// Group by key
	type group struct {
		samples []types.Sample
	}
	groups := make(map[string]*group)

	for _, s := range samples {
		key := s.Key()
		if groups[key] == nil {
			groups[key] = &group{}
		}
		groups[key].samples = append(groups[key].samples, s)
	}

	// Calculate aggregates
	var results []types.AggregateResult

	for _, g := range groups {
		if len(g.samples) == 0 {
			continue
		}

		first := g.samples[0]
		r := types.AggregateResult{
			Namespace:   first.Namespace,
			Target:      first.Target,
			Poller:      first.Poller,
			BucketStart: startTime.UnixMilli(),
			BucketEnd:   endTime.UnixMilli(),
		}

		var sum float64
		var min, max float64 = g.samples[0].Value, g.samples[0].Value
		var firstTs, lastTs int64 = g.samples[0].TimestampMs, g.samples[0].TimestampMs

		for _, s := range g.samples {
			if !s.Valid {
				continue
			}

			r.Count++
			sum += s.Value

			if s.Value < min {
				min = s.Value
			}
			if s.Value > max {
				max = s.Value
			}
			if s.TimestampMs < firstTs {
				firstTs = s.TimestampMs
			}
			if s.TimestampMs > lastTs {
				lastTs = s.TimestampMs
			}
		}

		if r.Count > 0 {
			r.Sum = sum
			r.Min = min
			r.Max = max
			r.Avg = sum / float64(r.Count)
			r.FirstTs = firstTs
			r.LastTs = lastTs
			results = append(results, r)
		}
	}

	return results
}

// mergeResults merges Parquet and buffer results.
func (s *Service) mergeResults(parquet, buffer []types.AggregateResult) []types.AggregateResult {
	// Simple concatenation - buffer data is more recent
	results := make([]types.AggregateResult, 0, len(parquet)+len(buffer))
	results = append(results, parquet...)
	results = append(results, buffer...)
	return results
}

// Stats returns query statistics.
func (s *Service) Stats() ServiceStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return ServiceStats{
		QueriesExecuted: s.stats.QueriesExecuted,
		RowsReturned:    s.stats.RowsReturned,
		Errors:          s.stats.Errors,
	}
}

// ServiceStats holds service statistics.
type ServiceStats struct {
	QueriesExecuted int64
	RowsReturned    int64
	Errors          int64
}

// ExecuteSQL executes a raw SQL query using DuckDB.
// This is useful for ad-hoc queries and debugging.
func (s *Service) ExecuteSQL(ctx context.Context, query string) ([]map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		s.stats.Errors++
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}

	s.stats.QueriesExecuted++
	s.stats.RowsReturned += int64(len(results))

	return results, rows.Err()
}
