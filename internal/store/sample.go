// LOCATION: internal/store/sample.go
// REPLACEMENT - Optimierte Version mit Multi-Row INSERT
//
// OPTIMIERUNGEN:
// 1. Multi-Row INSERT statt einzelner Prepared Statements
// 2. Chunk-basiertes Einfügen für sehr große Batches
// 3. Pre-allokierte Slices mit korrekter Kapazität

package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Sample represents a single poll result.
type Sample struct {
	Namespace    string
	Target       string
	Poller       string
	TimestampMs  int64
	ValueCounter *uint64
	ValueText    *string
	ValueGauge   *float64
	Valid        bool
	Error        string
	PollMs       int
}

// InsertSample inserts a single sample.
func (s *Store) InsertSample(sample *Sample) error {
	_, err := s.db.Exec(`
		INSERT INTO samples (namespace, target, poller, timestamp_ms, value_counter, 
		                     value_text, value_gauge, valid, error, poll_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, sample.Namespace, sample.Target, sample.Poller, sample.TimestampMs,
		sample.ValueCounter, sample.ValueText, sample.ValueGauge,
		sample.Valid, sample.Error, sample.PollMs)
	return err
}

// maxInsertParams ist die maximale Anzahl von Parametern pro INSERT.
// SQLite/DuckDB haben Limits, wir bleiben konservativ.
// 10 Spalten * 100 Rows = 1000 Parameter pro Statement
const maxSamplesPerInsert = 100

// InsertSamplesBatchContext inserts multiple samples with context support.
func (s *Store) InsertSamplesBatchContext(ctx context.Context, samples []*Sample) error {
	// For now, delegate to non-context version
	// TODO: Add proper context cancellation support
	return s.InsertSamplesBatch(samples)
}

// InsertSamplesBatch inserts multiple samples efficiently using multi-row INSERT.
// OPTIMIZED: Uses multi-row VALUES instead of individual prepared statements.
func (s *Store) InsertSamplesBatch(samples []*Sample) error {
	if len(samples) == 0 {
		return nil
	}

	// Für kleine Batches: Direkt einfügen
	if len(samples) <= maxSamplesPerInsert {
		return s.insertSamplesMultiRow(samples)
	}

	// Für große Batches: In Chunks aufteilen
	return s.Transaction(func(tx *sql.Tx) error {
		for i := 0; i < len(samples); i += maxSamplesPerInsert {
			end := i + maxSamplesPerInsert
			if end > len(samples) {
				end = len(samples)
			}
			chunk := samples[i:end]

			if err := s.insertSamplesMultiRowTx(tx, chunk); err != nil {
				return err
			}
		}
		return nil
	})
}

// insertSamplesMultiRow führt Multi-Row INSERT ohne Transaktion aus.
func (s *Store) insertSamplesMultiRow(samples []*Sample) error {
	query, args := buildMultiRowInsert(samples)
	_, err := s.db.Exec(query, args...)
	return err
}

// insertSamplesMultiRowTx führt Multi-Row INSERT innerhalb einer Transaktion aus.
func (s *Store) insertSamplesMultiRowTx(tx *sql.Tx, samples []*Sample) error {
	query, args := buildMultiRowInsert(samples)
	_, err := tx.Exec(query, args...)
	return err
}

// buildMultiRowInsert baut das Multi-Row INSERT Statement.
// OPTIMIZED: Pre-allokierte Slices, strings.Builder für Query
func buildMultiRowInsert(samples []*Sample) (string, []interface{}) {
	const columnsPerRow = 10

	// Pre-allokiere args mit exakter Größe
	args := make([]interface{}, 0, len(samples)*columnsPerRow)

	// Baue Query mit strings.Builder
	var query strings.Builder
	query.Grow(200 + len(samples)*50) // Geschätzte Größe

	query.WriteString(`INSERT INTO samples (namespace, target, poller, timestamp_ms, 
		value_counter, value_text, value_gauge, valid, error, poll_ms) VALUES `)

	for i, sample := range samples {
		if i > 0 {
			query.WriteByte(',')
		}
		query.WriteString("(?,?,?,?,?,?,?,?,?,?)")

		args = append(args,
			sample.Namespace,
			sample.Target,
			sample.Poller,
			sample.TimestampMs,
			sample.ValueCounter,
			sample.ValueText,
			sample.ValueGauge,
			sample.Valid,
			sample.Error,
			sample.PollMs,
		)
	}

	return query.String(), args
}

// InsertSamplesBatchLegacy ist die ursprüngliche Implementierung (für Fallback).
// Verwendet Prepared Statement mit einzelnen Exec-Aufrufen.
func (s *Store) InsertSamplesBatchLegacy(samples []*Sample) error {
	if len(samples) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			INSERT INTO samples (namespace, target, poller, timestamp_ms, value_counter, 
			                     value_text, value_gauge, valid, error, poll_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, sample := range samples {
			_, err = stmt.Exec(
				sample.Namespace, sample.Target, sample.Poller, sample.TimestampMs,
				sample.ValueCounter, sample.ValueText, sample.ValueGauge,
				sample.Valid, sample.Error, sample.PollMs)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// ============================================================================
// Query-Methoden (unverändert, aber mit Kapazitäts-Hints)
// ============================================================================

// GetSamples retrieves samples for a poller.
func (s *Store) GetSamples(namespace, target, poller string, limit int, sinceMs, untilMs int64) ([]*Sample, error) {
	query := `
		SELECT namespace, target, poller, timestamp_ms, value_counter, 
		       value_text, value_gauge, valid, error, poll_ms
		FROM samples 
		WHERE namespace = ? AND target = ? AND poller = ?
	`
	args := []interface{}{namespace, target, poller}

	if sinceMs > 0 {
		query += ` AND timestamp_ms >= ?`
		args = append(args, sinceMs)
	}
	if untilMs > 0 {
		query += ` AND timestamp_ms <= ?`
		args = append(args, untilMs)
	}

	query += ` ORDER BY timestamp_ms DESC`

	if limit > 0 {
		query += fmt.Sprintf(` LIMIT %d`, limit)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query samples: %w", err)
	}
	defer rows.Close()

	// OPTIMIZED: Pre-allokiere mit erwarteter Größe
	capacity := limit
	if capacity <= 0 {
		capacity = 100 // Default-Schätzung
	}
	samples := make([]*Sample, 0, capacity)

	for rows.Next() {
		var sample Sample
		var valueCounter sql.NullInt64
		var valueText sql.NullString
		var valueGauge sql.NullFloat64
		var errStr sql.NullString

		if err := rows.Scan(
			&sample.Namespace, &sample.Target, &sample.Poller, &sample.TimestampMs,
			&valueCounter, &valueText, &valueGauge,
			&sample.Valid, &errStr, &sample.PollMs,
		); err != nil {
			return nil, fmt.Errorf("scan sample: %w", err)
		}

		if valueCounter.Valid {
			v := uint64(valueCounter.Int64)
			sample.ValueCounter = &v
		}
		if valueText.Valid {
			sample.ValueText = &valueText.String
		}
		if valueGauge.Valid {
			sample.ValueGauge = &valueGauge.Float64
		}
		if errStr.Valid {
			sample.Error = errStr.String
		}

		samples = append(samples, &sample)
	}

	return samples, rows.Err()
}

// CountSamples returns the number of samples for a poller.
func (s *Store) CountSamples(namespace, target, poller string) (int, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM samples 
		WHERE namespace = ? AND target = ? AND poller = ?
	`, namespace, target, poller).Scan(&count)
	return count, err
}

// DeleteOldSamples deletes samples older than the given timestamp.
func (s *Store) DeleteOldSamples(namespace, target, poller string, beforeMs int64) (int64, error) {
	result, err := s.db.Exec(`
		DELETE FROM samples 
		WHERE namespace = ? AND target = ? AND poller = ? AND timestamp_ms < ?
	`, namespace, target, poller, beforeMs)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// DeleteSamplesForPoller deletes all samples for a poller.
func (s *Store) DeleteSamplesForPoller(namespace, target, poller string) error {
	_, err := s.db.Exec(`
		DELETE FROM samples WHERE namespace = ? AND target = ? AND poller = ?
	`, namespace, target, poller)
	return err
}

// TrimSamples keeps only the most recent N samples for a poller.
func (s *Store) TrimSamples(namespace, target, poller string, keepCount int) (int64, error) {
	var cutoffMs sql.NullInt64
	err := s.db.QueryRow(`
		SELECT timestamp_ms FROM samples 
		WHERE namespace = ? AND target = ? AND poller = ?
		ORDER BY timestamp_ms DESC
		LIMIT 1 OFFSET ?
	`, namespace, target, poller, keepCount-1).Scan(&cutoffMs)

	if err == sql.ErrNoRows || !cutoffMs.Valid {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	result, err := s.db.Exec(`
		DELETE FROM samples 
		WHERE namespace = ? AND target = ? AND poller = ? AND timestamp_ms < ?
	`, namespace, target, poller, cutoffMs.Int64)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// GetLatestSample returns the most recent sample for a poller.
func (s *Store) GetLatestSample(namespace, target, poller string) (*Sample, error) {
	samples, err := s.GetSamples(namespace, target, poller, 1, 0, 0)
	if err != nil {
		return nil, err
	}
	if len(samples) == 0 {
		return nil, nil
	}
	return samples[0], nil
}

// GetSampleRange returns the time range of samples for a poller.
func (s *Store) GetSampleRange(namespace, target, poller string) (oldest, newest int64, err error) {
	var oldestNull, newestNull sql.NullInt64
	err = s.db.QueryRow(`
		SELECT MIN(timestamp_ms), MAX(timestamp_ms) FROM samples 
		WHERE namespace = ? AND target = ? AND poller = ?
	`, namespace, target, poller).Scan(&oldestNull, &newestNull)
	if err == sql.ErrNoRows {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if oldestNull.Valid {
		oldest = oldestNull.Int64
	}
	if newestNull.Valid {
		newest = newestNull.Int64
	}
	return oldest, newest, nil
}

// CleanupOldSamples removes samples older than retention period for all pollers.
func (s *Store) CleanupOldSamples(retentionMs int64) (int64, error) {
	cutoff := time.Now().UnixMilli() - retentionMs
	result, err := s.db.Exec(`DELETE FROM samples WHERE timestamp_ms < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
