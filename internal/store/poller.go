// Package store provides database operations for the stalker application.
//
// This file handles poller CRUD operations, state management, and statistics.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// =============================================================================
// Poller Types
// =============================================================================

// Poller represents a poller in the store.
type Poller struct {
	Namespace      string
	Target         string
	Name           string
	Description    string
	Protocol       string
	ProtocolConfig json.RawMessage
	PollingConfig  *PollingConfig
	AdminState     string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int

	// Sync support
	Source      string // "yaml", "api", "import"
	ContentHash uint64
	SyncedAt    *time.Time
}

// PollingConfig holds polling configuration.
type PollingConfig struct {
	IntervalMs *uint32 `json:"interval_ms,omitempty"`
	TimeoutMs  *uint32 `json:"timeout_ms,omitempty"`
	Retries    *uint32 `json:"retries,omitempty"`
	BufferSize *uint32 `json:"buffer_size,omitempty"`
}

// PollerState represents runtime state of a poller.
type PollerState struct {
	Namespace           string
	Target              string
	Poller              string
	OperState           string
	HealthState         string
	LastError           string
	ConsecutiveFailures int
	LastPollAt          *time.Time
	LastSuccessAt       *time.Time
	LastFailureAt       *time.Time
}

// PollerStatsRecord holds statistics for a poller.
type PollerStatsRecord struct {
	Namespace    string
	Target       string
	Poller       string
	PollsTotal   int64
	PollsSuccess int64
	PollsFailed  int64
	PollsTimeout int64
	PollMsSum    int64
	PollMsMin    *int
	PollMsMax    *int
	PollMsCount  int
}

// =============================================================================
// Update Result Types (Fix #20)
// =============================================================================

// UpdatePollerResult contains detailed information about an update attempt.
//
// FIX #20: This type allows callers to distinguish between "not found" and
// "version mismatch" when an update fails. The previous implementation only
// returned ErrConcurrentModification for both cases.
type UpdatePollerResult struct {
	// Updated is true if the update was successful.
	Updated bool

	// Exists is true if the entity exists (regardless of version match).
	Exists bool

	// CurrentVersion is the current version in the database.
	// Only valid when Exists is true.
	CurrentVersion int
}

// =============================================================================
// Poller CRUD Operations
// =============================================================================

// CreatePoller creates a new poller.
func (s *Store) CreatePoller(p *Poller) error {
	pollingJSON, err := json.Marshal(p.PollingConfig)
	if err != nil {
		return fmt.Errorf("marshal polling config: %w", err)
	}

	now := time.Now()

	err = s.Transaction(func(tx *sql.Tx) error {
		// Insert poller
		_, err := tx.Exec(`
			INSERT INTO pollers (namespace, target, name, description, protocol, protocol_config, 
			                     polling_config, admin_state, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		`, p.Namespace, p.Target, p.Name, p.Description, p.Protocol,
			string(p.ProtocolConfig), string(pollingJSON), p.AdminState, now, now)
		if err != nil {
			return fmt.Errorf("insert poller: %w", err)
		}

		// Initialize poller state
		_, err = tx.Exec(`
			INSERT INTO poller_state (namespace, target, poller, oper_state, health_state)
			VALUES (?, ?, ?, 'stopped', 'unknown')
		`, p.Namespace, p.Target, p.Name)
		if err != nil {
			return fmt.Errorf("insert poller state: %w", err)
		}

		// Initialize poller stats
		_, err = tx.Exec(`
			INSERT INTO poller_stats (namespace, target, poller)
			VALUES (?, ?, ?)
		`, p.Namespace, p.Target, p.Name)
		if err != nil {
			return fmt.Errorf("insert poller stats: %w", err)
		}

		return nil
	})

	if err != nil {
		return err
	}

	p.Version = 1
	p.CreatedAt = now
	p.UpdatedAt = now
	return nil
}

// GetPoller retrieves a poller by namespace, target, and name.
func (s *Store) GetPoller(namespace, target, name string) (*Poller, error) {
	var p Poller
	var protocolConfig, pollingConfig sql.NullString

	err := s.db.QueryRow(`
		SELECT namespace, target, name, description, protocol, protocol_config, 
		       polling_config, admin_state, created_at, updated_at, version
		FROM pollers WHERE namespace = ? AND target = ? AND name = ?
	`, namespace, target, name).Scan(
		&p.Namespace, &p.Target, &p.Name, &p.Description, &p.Protocol,
		&protocolConfig, &pollingConfig, &p.AdminState, &p.CreatedAt, &p.UpdatedAt, &p.Version)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query poller: %w", err)
	}

	if protocolConfig.Valid {
		p.ProtocolConfig = json.RawMessage(protocolConfig.String)
	}

	if pollingConfig.Valid && pollingConfig.String != "" {
		p.PollingConfig = &PollingConfig{}
		if err := json.Unmarshal([]byte(pollingConfig.String), p.PollingConfig); err != nil {
			return nil, fmt.Errorf("unmarshal polling config: %w", err)
		}
	}

	return &p, nil
}

// ListPollers returns all pollers for a target.
func (s *Store) ListPollers(namespace, target string) ([]*Poller, error) {
	rows, err := s.db.Query(`
		SELECT namespace, target, name, description, protocol, protocol_config, 
		       polling_config, admin_state, created_at, updated_at, version
		FROM pollers WHERE namespace = ? AND target = ? ORDER BY name
	`, namespace, target)
	if err != nil {
		return nil, fmt.Errorf("query pollers: %w", err)
	}
	defer rows.Close()

	return scanPollers(rows)
}

// ListPollersInNamespace returns all pollers in a namespace.
func (s *Store) ListPollersInNamespace(namespace string) ([]*Poller, error) {
	rows, err := s.db.Query(`
		SELECT namespace, target, name, description, protocol, protocol_config, 
		       polling_config, admin_state, created_at, updated_at, version
		FROM pollers WHERE namespace = ? ORDER BY target, name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query pollers: %w", err)
	}
	defer rows.Close()

	return scanPollers(rows)
}

// ListAllPollers returns all pollers.
func (s *Store) ListAllPollers() ([]*Poller, error) {
	rows, err := s.db.Query(`
		SELECT namespace, target, name, description, protocol, protocol_config, 
		       polling_config, admin_state, created_at, updated_at, version
		FROM pollers ORDER BY namespace, target, name
	`)
	if err != nil {
		return nil, fmt.Errorf("query pollers: %w", err)
	}
	defer rows.Close()

	return scanPollers(rows)
}

func scanPollers(rows *sql.Rows) ([]*Poller, error) {
	var pollers []*Poller
	for rows.Next() {
		var p Poller
		var protocolConfig, pollingConfig sql.NullString

		if err := rows.Scan(
			&p.Namespace, &p.Target, &p.Name, &p.Description, &p.Protocol,
			&protocolConfig, &pollingConfig, &p.AdminState, &p.CreatedAt, &p.UpdatedAt, &p.Version,
		); err != nil {
			return nil, fmt.Errorf("scan poller: %w", err)
		}

		if protocolConfig.Valid {
			p.ProtocolConfig = json.RawMessage(protocolConfig.String)
		}

		if pollingConfig.Valid && pollingConfig.String != "" {
			p.PollingConfig = &PollingConfig{}
			if err := json.Unmarshal([]byte(pollingConfig.String), p.PollingConfig); err != nil {
				return nil, fmt.Errorf("unmarshal polling config: %w", err)
			}
		}

		pollers = append(pollers, &p)
	}

	return pollers, rows.Err()
}

// UpdatePoller updates a poller with optimistic locking.
//
// FIX #20: This method now returns ErrPollerNotFound when the entity doesn't exist,
// and ErrConcurrentModification when the version doesn't match. The previous
// implementation couldn't distinguish between these two cases.
func (s *Store) UpdatePoller(p *Poller) error {
	result, err := s.UpdatePollerWithResult(p)
	if err != nil {
		return err
	}

	if !result.Updated {
		if !result.Exists {
			return ErrPollerNotFound
		}
		return ErrConcurrentModification
	}

	return nil
}

// UpdatePollerWithResult updates a poller and returns detailed result information.
//
// FIX #20 (Variant 2): This method returns the current version when a version
// mismatch occurs, allowing callers to implement optimistic concurrency control
// patterns like "retry with current version".
func (s *Store) UpdatePollerWithResult(p *Poller) (*UpdatePollerResult, error) {
	pollingJSON, err := json.Marshal(p.PollingConfig)
	if err != nil {
		return nil, fmt.Errorf("marshal polling config: %w", err)
	}

	result, err := s.db.Exec(`
		UPDATE pollers 
		SET description = ?, protocol_config = ?, polling_config = ?, 
		    admin_state = ?, updated_at = ?, version = version + 1
		WHERE namespace = ? AND target = ? AND name = ? AND version = ?
	`, p.Description, string(p.ProtocolConfig), string(pollingJSON),
		p.AdminState, time.Now(), p.Namespace, p.Target, p.Name, p.Version)

	if err != nil {
		return nil, fmt.Errorf("update poller: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("rows affected: %w", err)
	}

	if rows == 0 {
		// FIX #20: Check if entity exists to distinguish not found vs version mismatch
		var currentVersion int
		err := s.db.QueryRow(`
			SELECT version FROM pollers 
			WHERE namespace = ? AND target = ? AND name = ?
		`, p.Namespace, p.Target, p.Name).Scan(&currentVersion)

		if err == sql.ErrNoRows {
			// Entity doesn't exist
			return &UpdatePollerResult{
				Updated: false,
				Exists:  false,
			}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("check exists: %w", err)
		}

		// Entity exists but version mismatch
		return &UpdatePollerResult{
			Updated:        false,
			Exists:         true,
			CurrentVersion: currentVersion,
		}, nil
	}

	// Success
	p.Version++
	p.UpdatedAt = time.Now()

	return &UpdatePollerResult{
		Updated:        true,
		Exists:         true,
		CurrentVersion: p.Version,
	}, nil
}

// UpdatePollerAdminState updates only the admin state.
func (s *Store) UpdatePollerAdminState(namespace, target, name, adminState string) error {
	_, err := s.db.Exec(`
		UPDATE pollers SET admin_state = ?, updated_at = ?
		WHERE namespace = ? AND target = ? AND name = ?
	`, adminState, time.Now(), namespace, target, name)
	return err
}

// DeletePoller deletes a poller.
func (s *Store) DeletePoller(namespace, target, name string) (linksDeleted int, err error) {
	err = s.Transaction(func(tx *sql.Tx) error {
		// Count links
		err := tx.QueryRow(`
			SELECT COUNT(*) FROM tree_nodes 
			WHERE namespace = ? AND link_ref = ?
		`, namespace, fmt.Sprintf("poller:%s/%s", target, name)).Scan(&linksDeleted)
		if err != nil {
			return err
		}

		// Delete samples
		_, err = tx.Exec(`
			DELETE FROM samples WHERE namespace = ? AND target = ? AND poller = ?
		`, namespace, target, name)
		if err != nil {
			return err
		}

		// Delete state
		_, err = tx.Exec(`
			DELETE FROM poller_state WHERE namespace = ? AND target = ? AND poller = ?
		`, namespace, target, name)
		if err != nil {
			return err
		}

		// Delete stats
		_, err = tx.Exec(`
			DELETE FROM poller_stats WHERE namespace = ? AND target = ? AND poller = ?
		`, namespace, target, name)
		if err != nil {
			return err
		}

		// Delete tree links
		_, err = tx.Exec(`
			DELETE FROM tree_nodes WHERE namespace = ? AND link_ref = ?
		`, namespace, fmt.Sprintf("poller:%s/%s", target, name))
		if err != nil {
			return err
		}

		// Delete poller
		_, err = tx.Exec(`
			DELETE FROM pollers WHERE namespace = ? AND target = ? AND name = ?
		`, namespace, target, name)
		return err
	})

	return linksDeleted, err
}

// PollerExists checks if a poller exists.
func (s *Store) PollerExists(namespace, target, name string) (bool, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM pollers WHERE namespace = ? AND target = ? AND name = ?
	`, namespace, target, name).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// =============================================================================
// Poller State Operations
// =============================================================================

// GetPollerState retrieves the runtime state of a poller.
func (s *Store) GetPollerState(namespace, target, poller string) (*PollerState, error) {
	var state PollerState
	var lastError sql.NullString
	var lastPollAt, lastSuccessAt, lastFailureAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT namespace, target, poller, oper_state, health_state, last_error,
		       consecutive_failures, last_poll_at, last_success_at, last_failure_at
		FROM poller_state WHERE namespace = ? AND target = ? AND poller = ?
	`, namespace, target, poller).Scan(
		&state.Namespace, &state.Target, &state.Poller, &state.OperState, &state.HealthState,
		&lastError, &state.ConsecutiveFailures, &lastPollAt, &lastSuccessAt, &lastFailureAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query poller state: %w", err)
	}

	if lastError.Valid {
		state.LastError = lastError.String
	}
	if lastPollAt.Valid {
		state.LastPollAt = &lastPollAt.Time
	}
	if lastSuccessAt.Valid {
		state.LastSuccessAt = &lastSuccessAt.Time
	}
	if lastFailureAt.Valid {
		state.LastFailureAt = &lastFailureAt.Time
	}

	return &state, nil
}

// UpdatePollerState updates the runtime state of a poller.
func (s *Store) UpdatePollerState(state *PollerState) error {
	_, err := s.db.Exec(`
		UPDATE poller_state 
		SET oper_state = ?, health_state = ?, last_error = ?, consecutive_failures = ?,
		    last_poll_at = ?, last_success_at = ?, last_failure_at = ?
		WHERE namespace = ? AND target = ? AND poller = ?
	`, state.OperState, state.HealthState, state.LastError, state.ConsecutiveFailures,
		state.LastPollAt, state.LastSuccessAt, state.LastFailureAt,
		state.Namespace, state.Target, state.Poller)
	return err
}

// BatchUpdatePollerStates updates multiple poller states in one transaction.
func (s *Store) BatchUpdatePollerStates(states []*PollerState) error {
	if len(states) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE poller_state 
			SET oper_state = ?, health_state = ?, last_error = ?, consecutive_failures = ?,
			    last_poll_at = ?, last_success_at = ?, last_failure_at = ?
			WHERE namespace = ? AND target = ? AND poller = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, state := range states {
			_, err = stmt.Exec(
				state.OperState, state.HealthState, state.LastError, state.ConsecutiveFailures,
				state.LastPollAt, state.LastSuccessAt, state.LastFailureAt,
				state.Namespace, state.Target, state.Poller)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// =============================================================================
// Poller Stats Operations
// =============================================================================

// GetPollerStats retrieves statistics for a poller.
func (s *Store) GetPollerStats(namespace, target, poller string) (*PollerStatsRecord, error) {
	var stats PollerStatsRecord
	var pollMsMin, pollMsMax sql.NullInt32

	err := s.db.QueryRow(`
		SELECT namespace, target, poller, polls_total, polls_success, polls_failed, polls_timeout,
		       poll_ms_sum, poll_ms_min, poll_ms_max, poll_ms_count
		FROM poller_stats WHERE namespace = ? AND target = ? AND poller = ?
	`, namespace, target, poller).Scan(
		&stats.Namespace, &stats.Target, &stats.Poller,
		&stats.PollsTotal, &stats.PollsSuccess, &stats.PollsFailed, &stats.PollsTimeout,
		&stats.PollMsSum, &pollMsMin, &pollMsMax, &stats.PollMsCount)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query poller stats: %w", err)
	}

	if pollMsMin.Valid {
		v := int(pollMsMin.Int32)
		stats.PollMsMin = &v
	}
	if pollMsMax.Valid {
		v := int(pollMsMax.Int32)
		stats.PollMsMax = &v
	}

	return &stats, nil
}

// UpdatePollerStats updates statistics for a poller.
func (s *Store) UpdatePollerStats(stats *PollerStatsRecord) error {
	_, err := s.db.Exec(`
		UPDATE poller_stats 
		SET polls_total = ?, polls_success = ?, polls_failed = ?, polls_timeout = ?,
		    poll_ms_sum = ?, poll_ms_min = ?, poll_ms_max = ?, poll_ms_count = ?
		WHERE namespace = ? AND target = ? AND poller = ?
	`, stats.PollsTotal, stats.PollsSuccess, stats.PollsFailed, stats.PollsTimeout,
		stats.PollMsSum, stats.PollMsMin, stats.PollMsMax, stats.PollMsCount,
		stats.Namespace, stats.Target, stats.Poller)
	return err
}

// BatchUpdatePollerStats updates multiple poller stats in one transaction.
func (s *Store) BatchUpdatePollerStats(statsList []*PollerStatsRecord) error {
	if len(statsList) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE poller_stats 
			SET polls_total = ?, polls_success = ?, polls_failed = ?, polls_timeout = ?,
			    poll_ms_sum = ?, poll_ms_min = ?, poll_ms_max = ?, poll_ms_count = ?
			WHERE namespace = ? AND target = ? AND poller = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, stats := range statsList {
			_, err = stmt.Exec(
				stats.PollsTotal, stats.PollsSuccess, stats.PollsFailed, stats.PollsTimeout,
				stats.PollMsSum, stats.PollMsMin, stats.PollMsMax, stats.PollMsCount,
				stats.Namespace, stats.Target, stats.Poller)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// =============================================================================
// Bulk Operations (for Sync)
// =============================================================================

// BulkCreatePollers creates multiple pollers in a single transaction.
func (s *Store) BulkCreatePollers(pollers []*Poller) error {
	if len(pollers) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		pollerStmt, err := tx.Prepare(`
			INSERT INTO pollers (namespace, target, name, description, protocol, protocol_config,
			                     polling_config, admin_state, source, content_hash, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		`)
		if err != nil {
			return err
		}
		defer pollerStmt.Close()

		stateStmt, err := tx.Prepare(`
			INSERT INTO poller_state (namespace, target, poller, oper_state, health_state)
			VALUES (?, ?, ?, 'stopped', 'unknown')
		`)
		if err != nil {
			return err
		}
		defer stateStmt.Close()

		statsStmt, err := tx.Prepare(`
			INSERT INTO poller_stats (namespace, target, poller)
			VALUES (?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer statsStmt.Close()

		now := time.Now()
		for _, p := range pollers {
			if p.Source == "" {
				p.Source = "yaml"
			}
			if p.AdminState == "" {
				p.AdminState = "disabled"
			}

			var pollingJSON []byte
			if p.PollingConfig != nil {
				pollingJSON, _ = json.Marshal(p.PollingConfig)
			}

			_, err := pollerStmt.Exec(
				p.Namespace, p.Target, p.Name, p.Description, p.Protocol,
				string(p.ProtocolConfig), string(pollingJSON), p.AdminState,
				p.Source, p.ContentHash, now, now,
			)
			if err != nil {
				return fmt.Errorf("create poller %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			_, err = stateStmt.Exec(p.Namespace, p.Target, p.Name)
			if err != nil {
				return fmt.Errorf("create poller state %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			_, err = statsStmt.Exec(p.Namespace, p.Target, p.Name)
			if err != nil {
				return fmt.Errorf("create poller stats %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			p.CreatedAt = now
			p.UpdatedAt = now
			p.Version = 1
		}
		return nil
	})
}

// BulkUpdatePollers updates multiple pollers in a single transaction.
func (s *Store) BulkUpdatePollers(pollers []*Poller) error {
	if len(pollers) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE pollers 
			SET description = ?, protocol = ?, protocol_config = ?, polling_config = ?,
			    admin_state = ?, source = ?, content_hash = ?, synced_at = ?,
			    updated_at = ?, version = version + 1
			WHERE namespace = ? AND target = ? AND name = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, p := range pollers {
			var pollingJSON []byte
			if p.PollingConfig != nil {
				pollingJSON, _ = json.Marshal(p.PollingConfig)
			}

			_, err := stmt.Exec(
				p.Description, p.Protocol, string(p.ProtocolConfig), string(pollingJSON),
				p.AdminState, p.Source, p.ContentHash, &now, now,
				p.Namespace, p.Target, p.Name,
			)
			if err != nil {
				return fmt.Errorf("update poller %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
			}

			p.SyncedAt = &now
			p.UpdatedAt = now
		}
		return nil
	})
}

// BulkDeletePollers deletes multiple pollers by key (namespace/target/name).
func (s *Store) BulkDeletePollers(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		pollerStmt, err := tx.Prepare(`DELETE FROM pollers WHERE namespace = ? AND target = ? AND name = ?`)
		if err != nil {
			return err
		}
		defer pollerStmt.Close()

		stateStmt, err := tx.Prepare(`DELETE FROM poller_state WHERE namespace = ? AND target = ? AND poller = ?`)
		if err != nil {
			return err
		}
		defer stateStmt.Close()

		statsStmt, err := tx.Prepare(`DELETE FROM poller_stats WHERE namespace = ? AND target = ? AND poller = ?`)
		if err != nil {
			return err
		}
		defer statsStmt.Close()

		for _, key := range keys {
			ns, target, name := parsePollerKey(key)
			if ns == "" || target == "" || name == "" {
				continue
			}

			// Delete in order: stats, state, poller
			if _, err := statsStmt.Exec(ns, target, name); err != nil {
				return fmt.Errorf("delete poller stats %s: %w", key, err)
			}
			if _, err := stateStmt.Exec(ns, target, name); err != nil {
				return fmt.Errorf("delete poller state %s: %w", key, err)
			}
			if _, err := pollerStmt.Exec(ns, target, name); err != nil {
				return fmt.Errorf("delete poller %s: %w", key, err)
			}
		}
		return nil
	})
}

// parsePollerKey splits "namespace/target/name" into parts.
func parsePollerKey(key string) (namespace, target, name string) {
	parts := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(key) && len(parts) < 2; i++ {
		if key[i] == '/' {
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	if start < len(key) {
		parts = append(parts, key[start:])
	}

	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	return "", "", ""
}

// ListAllPollers returns all pollers across all namespaces.
func (s *Store) ListAllPollers() ([]*Poller, error) {
	rows, err := s.db.Query(`
		SELECT namespace, target, name, description, protocol, protocol_config,
		       polling_config, admin_state, source, content_hash, synced_at,
		       created_at, updated_at, version
		FROM pollers ORDER BY namespace, target, name
	`)
	if err != nil {
		return nil, fmt.Errorf("query pollers: %w", err)
	}
	defer rows.Close()

	return scanPollersWithSync(rows)
}

// ListPollersByNamespace returns all pollers in a namespace.
func (s *Store) ListPollersByNamespace(namespace string) ([]*Poller, error) {
	rows, err := s.db.Query(`
		SELECT namespace, target, name, description, protocol, protocol_config,
		       polling_config, admin_state, source, content_hash, synced_at,
		       created_at, updated_at, version
		FROM pollers WHERE namespace = ? ORDER BY target, name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query pollers: %w", err)
	}
	defer rows.Close()

	return scanPollersWithSync(rows)
}

func scanPollersWithSync(rows *sql.Rows) ([]*Poller, error) {
	var pollers []*Poller
	for rows.Next() {
		p := &Poller{}
		var protocolConfig, pollingConfig sql.NullString
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&p.Namespace, &p.Target, &p.Name, &p.Description, &p.Protocol,
			&protocolConfig, &pollingConfig, &p.AdminState,
			&source, &contentHash, &syncedAt,
			&p.CreatedAt, &p.UpdatedAt, &p.Version,
		); err != nil {
			return nil, fmt.Errorf("scan poller: %w", err)
		}

		if protocolConfig.Valid {
			p.ProtocolConfig = json.RawMessage(protocolConfig.String)
		}
		if pollingConfig.Valid && pollingConfig.String != "" {
			p.PollingConfig = &PollingConfig{}
			json.Unmarshal([]byte(pollingConfig.String), p.PollingConfig)
		}
		if source.Valid {
			p.Source = source.String
		}
		if contentHash.Valid {
			p.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			p.SyncedAt = &syncedAt.Time
		}

		pollers = append(pollers, p)
	}

	return pollers, rows.Err()
}

// =============================================================================
// Errors
// =============================================================================

// ErrPollerNotFound is returned when a poller doesn't exist.
var ErrPollerNotFound = fmt.Errorf("poller not found")

// ErrConcurrentModification is returned when a version mismatch is detected.
var ErrConcurrentModification = fmt.Errorf("concurrent modification detected (version mismatch)")
