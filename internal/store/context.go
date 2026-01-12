// Package store - Context-aware Database Operations
//
// FIX #10: Context-Propagation für kritische Store-Operationen.
//
// PROBLEM:
// SyncManager erstellt Contexts mit Timeouts, aber Store-Methoden nutzen
// s.db.Exec() statt s.db.ExecContext(). Der Context wird ignoriert.
//
// LÖSUNG:
// - Neue *Context-Methoden für alle Hot-Path-Operationen
// - Legacy-Methoden werden zu Wrappern mit s.defaultContext()
// - TransactionContext prüft Context vor Commit
//
// BETROFFENE METHODEN:
// - InsertSamplesBatch → InsertSamplesBatchContext
// - BatchUpdatePollerStates → BatchUpdatePollerStatesContext
// - BatchUpdatePollerStats → BatchUpdatePollerStatsContext
// - BulkCreatePollers → BulkCreatePollersContext
// - BulkUpdatePollers → BulkUpdatePollersContext
// - BulkDeletePollers → BulkDeletePollersContext
// - BulkCreateTargets → BulkCreateTargetsContext
// - BulkUpdateTargets → BulkUpdateTargetsContext
// - BulkDeleteTargets → BulkDeleteTargetsContext
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// =============================================================================
// Context Helpers
// =============================================================================

// defaultContext erstellt einen Context mit Store.config.QueryTimeout.
func (s *Store) defaultContext() (context.Context, context.CancelFunc) {
	timeout := s.config.QueryTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(context.Background(), timeout)
}

// ctxCheckInterval: Context wird alle N Iterationen geprüft (Performance-Balance)
const ctxCheckInterval = 50

// =============================================================================
// Enhanced Transaction Support
// =============================================================================

// TransactionContext führt fn innerhalb einer Transaktion aus.
//
// WICHTIG: Prüft Context VOR dem Commit - verhindert Commit nach Timeout.
func (s *Store) TransactionContext(ctx context.Context, fn func(*sql.Tx) error) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original: %w)", rbErr, err)
		}
		return err
	}

	// KRITISCH: Context vor Commit prüfen
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("context cancelled before commit: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Transaction ist die Legacy-Version (nutzt config.QueryTimeout).
func (s *Store) Transaction(fn func(*sql.Tx) error) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.TransactionContext(ctx, fn)
}

// =============================================================================
// Sample Operations - Context-Aware
// =============================================================================

// InsertSampleContext fügt ein Sample mit Context ein.
func (s *Store) InsertSampleContext(ctx context.Context, sample *Sample) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO samples (namespace, target, poller, timestamp_ms, value_counter, 
		                     value_text, value_gauge, valid, error, poll_ms)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, sample.Namespace, sample.Target, sample.Poller, sample.TimestampMs,
		sample.ValueCounter, sample.ValueText, sample.ValueGauge,
		sample.Valid, sample.Error, sample.PollMs)
	return err
}

// InsertSample fügt ein Sample ein (Legacy).
func (s *Store) InsertSample(sample *Sample) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.InsertSampleContext(ctx, sample)
}

// InsertSamplesBatchContext fügt Samples mit Context ein.
//
// KRITISCH: Wird vom SyncManager alle ~5s mit 1000+ Samples aufgerufen.
func (s *Store) InsertSamplesBatchContext(ctx context.Context, samples []*Sample) error {
	if len(samples) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Kleine Batches: Direkt ohne Transaktion
	if len(samples) <= maxSamplesPerInsert {
		query, args := buildMultiRowInsert(samples)
		_, err := s.db.ExecContext(ctx, query, args...)
		return err
	}

	// Große Batches: Chunked mit Transaktion
	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		for i := 0; i < len(samples); i += maxSamplesPerInsert {
			// Context periodisch prüfen
			if i > 0 && (i/maxSamplesPerInsert)%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			end := i + maxSamplesPerInsert
			if end > len(samples) {
				end = len(samples)
			}

			query, args := buildMultiRowInsert(samples[i:end])
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("chunk %d: %w", i/maxSamplesPerInsert, err)
			}
		}
		return nil
	})
}

// InsertSamplesBatch fügt Samples ein (Legacy).
func (s *Store) InsertSamplesBatch(samples []*Sample) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.InsertSamplesBatchContext(ctx, samples)
}

// =============================================================================
// Poller State Operations - Context-Aware
// =============================================================================

// UpdatePollerStateContext aktualisiert einen State mit Context.
func (s *Store) UpdatePollerStateContext(ctx context.Context, state *PollerState) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE poller_state 
		SET oper_state = ?, health_state = ?, last_error = ?, consecutive_failures = ?,
		    last_poll_at = ?, last_success_at = ?, last_failure_at = ?
		WHERE namespace = ? AND target = ? AND poller = ?
	`, state.OperState, state.HealthState, state.LastError, state.ConsecutiveFailures,
		state.LastPollAt, state.LastSuccessAt, state.LastFailureAt,
		state.Namespace, state.Target, state.Poller)
	return err
}

// UpdatePollerState aktualisiert einen State (Legacy).
func (s *Store) UpdatePollerState(state *PollerState) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.UpdatePollerStateContext(ctx, state)
}

// BatchUpdatePollerStatesContext aktualisiert States mit Context.
//
// KRITISCH: Wird vom SyncManager alle ~5s aufgerufen.
func (s *Store) BatchUpdatePollerStatesContext(ctx context.Context, states []*PollerState) error {
	if len(states) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			UPDATE poller_state 
			SET oper_state = ?, health_state = ?, last_error = ?, consecutive_failures = ?,
			    last_poll_at = ?, last_success_at = ?, last_failure_at = ?
			WHERE namespace = ? AND target = ? AND poller = ?
		`)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer stmt.Close()

		for i, state := range states {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			_, err = stmt.ExecContext(ctx,
				state.OperState, state.HealthState, state.LastError, state.ConsecutiveFailures,
				state.LastPollAt, state.LastSuccessAt, state.LastFailureAt,
				state.Namespace, state.Target, state.Poller)
			if err != nil {
				return fmt.Errorf("state %s/%s/%s: %w",
					state.Namespace, state.Target, state.Poller, err)
			}
		}
		return nil
	})
}

// BatchUpdatePollerStates aktualisiert States (Legacy).
func (s *Store) BatchUpdatePollerStates(states []*PollerState) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BatchUpdatePollerStatesContext(ctx, states)
}

// =============================================================================
// Poller Stats Operations - Context-Aware
// =============================================================================

// UpdatePollerStatsContext aktualisiert Stats mit Context.
func (s *Store) UpdatePollerStatsContext(ctx context.Context, stats *PollerStatsRecord) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE poller_stats 
		SET polls_total = ?, polls_success = ?, polls_failed = ?, polls_timeout = ?,
		    poll_ms_sum = ?, poll_ms_min = ?, poll_ms_max = ?, poll_ms_count = ?
		WHERE namespace = ? AND target = ? AND poller = ?
	`, stats.PollsTotal, stats.PollsSuccess, stats.PollsFailed, stats.PollsTimeout,
		stats.PollMsSum, stats.PollMsMin, stats.PollMsMax, stats.PollMsCount,
		stats.Namespace, stats.Target, stats.Poller)
	return err
}

// UpdatePollerStats aktualisiert Stats (Legacy).
func (s *Store) UpdatePollerStats(stats *PollerStatsRecord) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.UpdatePollerStatsContext(ctx, stats)
}

// BatchUpdatePollerStatsContext aktualisiert Stats mit Context.
//
// KRITISCH: Wird vom SyncManager alle ~10s aufgerufen.
func (s *Store) BatchUpdatePollerStatsContext(ctx context.Context, statsList []*PollerStatsRecord) error {
	if len(statsList) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			UPDATE poller_stats 
			SET polls_total = ?, polls_success = ?, polls_failed = ?, polls_timeout = ?,
			    poll_ms_sum = ?, poll_ms_min = ?, poll_ms_max = ?, poll_ms_count = ?
			WHERE namespace = ? AND target = ? AND poller = ?
		`)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer stmt.Close()

		for i, stats := range statsList {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			_, err = stmt.ExecContext(ctx,
				stats.PollsTotal, stats.PollsSuccess, stats.PollsFailed, stats.PollsTimeout,
				stats.PollMsSum, stats.PollMsMin, stats.PollMsMax, stats.PollMsCount,
				stats.Namespace, stats.Target, stats.Poller)
			if err != nil {
				return fmt.Errorf("stats %s/%s/%s: %w",
					stats.Namespace, stats.Target, stats.Poller, err)
			}
		}
		return nil
	})
}

// BatchUpdatePollerStats aktualisiert Stats (Legacy).
func (s *Store) BatchUpdatePollerStats(statsList []*PollerStatsRecord) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BatchUpdatePollerStatsContext(ctx, statsList)
}

// =============================================================================
// Bulk Poller Operations - Context-Aware
// =============================================================================

// BulkCreatePollersContext erstellt Pollers mit Context.
func (s *Store) BulkCreatePollersContext(ctx context.Context, pollers []*Poller) error {
	if len(pollers) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		pollerStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO pollers (namespace, target, name, description, protocol, protocol_config,
			                     polling_config, admin_state, source, content_hash, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		`)
		if err != nil {
			return fmt.Errorf("prepare poller: %w", err)
		}
		defer pollerStmt.Close()

		stateStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO poller_state (namespace, target, poller, oper_state, health_state)
			VALUES (?, ?, ?, 'stopped', 'unknown')
		`)
		if err != nil {
			return fmt.Errorf("prepare state: %w", err)
		}
		defer stateStmt.Close()

		statsStmt, err := tx.PrepareContext(ctx, `
			INSERT INTO poller_stats (namespace, target, poller)
			VALUES (?, ?, ?)
		`)
		if err != nil {
			return fmt.Errorf("prepare stats: %w", err)
		}
		defer statsStmt.Close()

		now := time.Now()
		for i, p := range pollers {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			if p.Source == "" {
				p.Source = "yaml"
			}
			if p.AdminState == "" {
				p.AdminState = "disabled"
			}

			var pollingJSON []byte
			// PollingConfig JSON marshaling hier

			if _, err := pollerStmt.ExecContext(ctx,
				p.Namespace, p.Target, p.Name, p.Description, p.Protocol,
				string(p.ProtocolConfig), string(pollingJSON), p.AdminState,
				p.Source, p.ContentHash, now, now); err != nil {
				return fmt.Errorf("poller %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			if _, err := stateStmt.ExecContext(ctx, p.Namespace, p.Target, p.Name); err != nil {
				return fmt.Errorf("state %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			if _, err := statsStmt.ExecContext(ctx, p.Namespace, p.Target, p.Name); err != nil {
				return fmt.Errorf("stats %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			p.CreatedAt = now
			p.UpdatedAt = now
			p.Version = 1
		}
		return nil
	})
}

// BulkCreatePollers erstellt Pollers (Legacy).
func (s *Store) BulkCreatePollers(pollers []*Poller) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BulkCreatePollersContext(ctx, pollers)
}

// BulkUpdatePollersContext aktualisiert Pollers mit Context.
func (s *Store) BulkUpdatePollersContext(ctx context.Context, pollers []*Poller) error {
	if len(pollers) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			UPDATE pollers 
			SET description = ?, protocol = ?, protocol_config = ?, polling_config = ?,
			    admin_state = ?, source = ?, content_hash = ?, synced_at = ?,
			    updated_at = ?, version = version + 1
			WHERE namespace = ? AND target = ? AND name = ?
		`)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for i, p := range pollers {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			var pollingJSON []byte

			if _, err := stmt.ExecContext(ctx,
				p.Description, p.Protocol, string(p.ProtocolConfig), string(pollingJSON),
				p.AdminState, p.Source, p.ContentHash, &now, now,
				p.Namespace, p.Target, p.Name); err != nil {
				return fmt.Errorf("poller %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			p.SyncedAt = &now
			p.UpdatedAt = now
		}
		return nil
	})
}

// BulkUpdatePollers aktualisiert Pollers (Legacy).
func (s *Store) BulkUpdatePollers(pollers []*Poller) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BulkUpdatePollersContext(ctx, pollers)
}

// BulkDeletePollersContext löscht Pollers mit Context.
func (s *Store) BulkDeletePollersContext(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		pollerStmt, err := tx.PrepareContext(ctx, `DELETE FROM pollers WHERE namespace = ? AND target = ? AND name = ?`)
		if err != nil {
			return err
		}
		defer pollerStmt.Close()

		stateStmt, err := tx.PrepareContext(ctx, `DELETE FROM poller_state WHERE namespace = ? AND target = ? AND poller = ?`)
		if err != nil {
			return err
		}
		defer stateStmt.Close()

		statsStmt, err := tx.PrepareContext(ctx, `DELETE FROM poller_stats WHERE namespace = ? AND target = ? AND poller = ?`)
		if err != nil {
			return err
		}
		defer statsStmt.Close()

		for i, key := range keys {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			parts := strings.SplitN(key, "/", 3)
			if len(parts) != 3 {
				continue
			}
			ns, target, name := parts[0], parts[1], parts[2]

			if _, err := pollerStmt.ExecContext(ctx, ns, target, name); err != nil {
				return fmt.Errorf("poller %s: %w", key, err)
			}
			if _, err := stateStmt.ExecContext(ctx, ns, target, name); err != nil {
				return fmt.Errorf("state %s: %w", key, err)
			}
			if _, err := statsStmt.ExecContext(ctx, ns, target, name); err != nil {
				return fmt.Errorf("stats %s: %w", key, err)
			}
		}
		return nil
	})
}

// BulkDeletePollers löscht Pollers (Legacy).
func (s *Store) BulkDeletePollers(keys []string) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BulkDeletePollersContext(ctx, keys)
}

// =============================================================================
// Bulk Target Operations - Context-Aware
// =============================================================================

// BulkCreateTargetsContext erstellt Targets mit Context.
func (s *Store) BulkCreateTargetsContext(ctx context.Context, targets []*Target) error {
	if len(targets) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO targets (namespace, name, description, labels, config,
			                     source, content_hash, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		`)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for i, t := range targets {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			if t.Source == "" {
				t.Source = "yaml"
			}

			labelsJSON := []byte("{}")
			var configJSON []byte

			if _, err := stmt.ExecContext(ctx,
				t.Namespace, t.Name, t.Description, labelsJSON, configJSON,
				t.Source, t.ContentHash, now, now); err != nil {
				return fmt.Errorf("target %s/%s: %w", t.Namespace, t.Name, err)
			}

			t.CreatedAt = now
			t.UpdatedAt = now
			t.Version = 1
		}
		return nil
	})
}

// BulkCreateTargets erstellt Targets (Legacy).
func (s *Store) BulkCreateTargets(targets []*Target) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BulkCreateTargetsContext(ctx, targets)
}

// BulkUpdateTargetsContext aktualisiert Targets mit Context.
func (s *Store) BulkUpdateTargetsContext(ctx context.Context, targets []*Target) error {
	if len(targets) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			UPDATE targets 
			SET description = ?, labels = ?, config = ?, source = ?, 
			    content_hash = ?, synced_at = ?, updated_at = ?, version = version + 1
			WHERE namespace = ? AND name = ?
		`)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer stmt.Close()

		now := time.Now()
		for i, t := range targets {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			labelsJSON := []byte("{}")
			var configJSON []byte

			if _, err := stmt.ExecContext(ctx,
				t.Description, labelsJSON, configJSON, t.Source,
				t.ContentHash, &now, now, t.Namespace, t.Name); err != nil {
				return fmt.Errorf("target %s/%s: %w", t.Namespace, t.Name, err)
			}

			t.SyncedAt = &now
			t.UpdatedAt = now
		}
		return nil
	})
}

// BulkUpdateTargets aktualisiert Targets (Legacy).
func (s *Store) BulkUpdateTargets(targets []*Target) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BulkUpdateTargetsContext(ctx, targets)
}

// BulkDeleteTargetsContext löscht Targets mit Context.
func (s *Store) BulkDeleteTargetsContext(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	return s.TransactionContext(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `DELETE FROM targets WHERE namespace = ? AND name = ?`)
		if err != nil {
			return fmt.Errorf("prepare: %w", err)
		}
		defer stmt.Close()

		for i, key := range keys {
			if i > 0 && i%ctxCheckInterval == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}

			idx := strings.IndexByte(key, '/')
			if idx < 0 {
				continue
			}
			ns, name := key[:idx], key[idx+1:]
			if ns == "" || name == "" {
				continue
			}

			if _, err := stmt.ExecContext(ctx, ns, name); err != nil {
				return fmt.Errorf("target %s: %w", key, err)
			}
		}
		return nil
	})
}

// BulkDeleteTargets löscht Targets (Legacy).
func (s *Store) BulkDeleteTargets(keys []string) error {
	ctx, cancel := s.defaultContext()
	defer cancel()
	return s.BulkDeleteTargetsContext(ctx, keys)
}
