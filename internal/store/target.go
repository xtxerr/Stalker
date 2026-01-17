// Package store - Target operations
//
// LOCATION: internal/store/target.go
//
// Provides CRUD operations for targets with sync support.

package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// =============================================================================
// Target Types
// =============================================================================

// Target represents a target (network device) in the store.
type Target struct {
	Namespace   string
	Name        string
	Description string
	Labels      map[string]string
	Config      *TargetConfig
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Version     int

	// Sync support
	Source      string // "yaml", "api", "import"
	ContentHash uint64
	SyncedAt    *time.Time
}

// TargetConfig holds target-level configuration.
type TargetConfig struct {
	Host     string          `json:"host,omitempty"`
	Port     uint16          `json:"port,omitempty"`
	Defaults *PollerDefaults `json:"defaults,omitempty"`
}

// TargetStats holds statistics for a target.
type TargetStats struct {
	PollerCount  int
	EnabledCount int
}

// PollerDefaults holds default settings inherited by pollers.
type PollerDefaults struct {
	// SNMPv2c
	Community string `json:"community,omitempty"`

	// SNMPv3
	SecurityName  string `json:"security_name,omitempty"`
	SecurityLevel string `json:"security_level,omitempty"`
	AuthProtocol  string `json:"auth_protocol,omitempty"`
	AuthPassword  string `json:"auth_password,omitempty"`
	PrivProtocol  string `json:"priv_protocol,omitempty"`
	PrivPassword  string `json:"priv_password,omitempty"`

	// Timing
	IntervalMs *uint32 `json:"interval_ms,omitempty"`
	TimeoutMs  *uint32 `json:"timeout_ms,omitempty"`
	Retries    *uint32 `json:"retries,omitempty"`
	BufferSize *uint32 `json:"buffer_size,omitempty"`
}

// =============================================================================
// CRUD Operations
// =============================================================================

// CreateTarget creates a new target.
func (s *Store) CreateTarget(t *Target) error {
	if t.Source == "" {
		t.Source = "api"
	}

	labelsJSON, err := json.Marshal(t.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var configJSON []byte
	if t.Config != nil {
		configJSON, err = json.Marshal(t.Config)
		if err != nil {
			return fmt.Errorf("marshal config: %w", err)
		}
	}

	now := time.Now()
	_, err = s.db.Exec(`
		INSERT INTO targets (namespace, name, description, labels, config, 
		                     source, content_hash, created_at, updated_at, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
	`, t.Namespace, t.Name, t.Description, labelsJSON, configJSON,
		t.Source, t.ContentHash, now, now)

	if err != nil {
		return fmt.Errorf("insert target: %w", err)
	}

	t.CreatedAt = now
	t.UpdatedAt = now
	t.Version = 1
	return nil
}

// GetTarget retrieves a target by namespace and name.
func (s *Store) GetTarget(namespace, name string) (*Target, error) {
	t := &Target{}
	var labelsJSON, configJSON sql.NullString
	var source sql.NullString
	var contentHash sql.NullInt64
	var syncedAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT namespace, name, description, labels, config, 
		       source, content_hash, synced_at, created_at, updated_at, version
		FROM targets WHERE namespace = ? AND name = ?
	`, namespace, name).Scan(
		&t.Namespace, &t.Name, &t.Description, &labelsJSON, &configJSON,
		&source, &contentHash, &syncedAt, &t.CreatedAt, &t.UpdatedAt, &t.Version,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query target: %w", err)
	}

	if labelsJSON.Valid && labelsJSON.String != "" {
		if err := json.Unmarshal([]byte(labelsJSON.String), &t.Labels); err != nil {
			return nil, fmt.Errorf("unmarshal labels: %w", err)
		}
	}

	if configJSON.Valid && configJSON.String != "" {
		t.Config = &TargetConfig{}
		if err := json.Unmarshal([]byte(configJSON.String), t.Config); err != nil {
			return nil, fmt.Errorf("unmarshal config: %w", err)
		}
	}

	if source.Valid {
		t.Source = source.String
	}
	if contentHash.Valid {
		t.ContentHash = uint64(contentHash.Int64)
	}
	if syncedAt.Valid {
		t.SyncedAt = &syncedAt.Time
	}

	return t, nil
}

// ListTargets returns all targets in a namespace.
func (s *Store) ListTargets(namespace string) ([]*Target, error) {
	rows, err := s.db.Query(`
		SELECT namespace, name, description, labels, config, 
		       source, content_hash, synced_at, created_at, updated_at, version
		FROM targets WHERE namespace = ? ORDER BY name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query targets: %w", err)
	}
	defer rows.Close()

	return scanTargets(rows)
}

// ListAllTargets returns all targets across all namespaces.
func (s *Store) ListAllTargets() ([]*Target, error) {
	rows, err := s.db.Query(`
		SELECT namespace, name, description, labels, config, 
		       source, content_hash, synced_at, created_at, updated_at, version
		FROM targets ORDER BY namespace, name
	`)
	if err != nil {
		return nil, fmt.Errorf("query targets: %w", err)
	}
	defer rows.Close()

	return scanTargets(rows)
}

func scanTargets(rows *sql.Rows) ([]*Target, error) {
	var targets []*Target
	for rows.Next() {
		t := &Target{}
		var labelsJSON, configJSON sql.NullString
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&t.Namespace, &t.Name, &t.Description, &labelsJSON, &configJSON,
			&source, &contentHash, &syncedAt, &t.CreatedAt, &t.UpdatedAt, &t.Version,
		); err != nil {
			return nil, fmt.Errorf("scan target: %w", err)
		}

		if labelsJSON.Valid && labelsJSON.String != "" {
			json.Unmarshal([]byte(labelsJSON.String), &t.Labels)
		}
		if configJSON.Valid && configJSON.String != "" {
			t.Config = &TargetConfig{}
			json.Unmarshal([]byte(configJSON.String), t.Config)
		}
		if source.Valid {
			t.Source = source.String
		}
		if contentHash.Valid {
			t.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			t.SyncedAt = &syncedAt.Time
		}

		targets = append(targets, t)
	}

	return targets, rows.Err()
}

// UpdateTarget updates an existing target.
func (s *Store) UpdateTarget(t *Target) error {
	labelsJSON, err := json.Marshal(t.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var configJSON []byte
	if t.Config != nil {
		configJSON, err = json.Marshal(t.Config)
		if err != nil {
			return fmt.Errorf("marshal config: %w", err)
		}
	}

	now := time.Now()
	result, err := s.db.Exec(`
		UPDATE targets 
		SET description = ?, labels = ?, config = ?, source = ?, 
		    content_hash = ?, synced_at = ?, updated_at = ?, version = version + 1
		WHERE namespace = ? AND name = ? AND version = ?
	`, t.Description, labelsJSON, configJSON, t.Source,
		t.ContentHash, t.SyncedAt, now, t.Namespace, t.Name, t.Version)

	if err != nil {
		return fmt.Errorf("update target: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("target not found or version mismatch")
	}

	t.UpdatedAt = now
	t.Version++
	return nil
}

// DeleteTarget deletes a target and optionally its pollers.
// Returns the number of pollers and tree links deleted.
func (s *Store) DeleteTarget(namespace, name string, force bool) (pollersDeleted, linksDeleted int, err error) {
	if force {
		// Count pollers first
		err = s.db.QueryRow(`SELECT COUNT(*) FROM pollers WHERE namespace = ? AND target = ?`, namespace, name).Scan(&pollersDeleted)
		if err != nil {
			return 0, 0, fmt.Errorf("count pollers: %w", err)
		}

		// Count tree links
		err = s.db.QueryRow(`SELECT COUNT(*) FROM tree_nodes WHERE namespace = ? AND target_ref = ?`, namespace, name).Scan(&linksDeleted)
		if err != nil {
			// Tree nodes table might not exist, ignore error
			linksDeleted = 0
		}

		// Delete pollers
		_, err = s.db.Exec(`DELETE FROM pollers WHERE namespace = ? AND target = ?`, namespace, name)
		if err != nil {
			return 0, 0, fmt.Errorf("delete pollers: %w", err)
		}

		// Delete tree links (ignore errors if table doesn't exist)
		s.db.Exec(`DELETE FROM tree_nodes WHERE namespace = ? AND target_ref = ?`, namespace, name)
	} else {
		// Check if target has pollers
		var count int
		err = s.db.QueryRow(`SELECT COUNT(*) FROM pollers WHERE namespace = ? AND target = ?`, namespace, name).Scan(&count)
		if err != nil {
			return 0, 0, fmt.Errorf("check pollers: %w", err)
		}
		if count > 0 {
			return 0, 0, fmt.Errorf("target has %d pollers, use force=true to delete", count)
		}
	}

	// Delete target
	_, err = s.db.Exec(`DELETE FROM targets WHERE namespace = ? AND name = ?`, namespace, name)
	if err != nil {
		return 0, 0, fmt.Errorf("delete target: %w", err)
	}

	return pollersDeleted, linksDeleted, nil
}

// TargetExists checks if a target exists.
func (s *Store) TargetExists(namespace, name string) (bool, error) {
	var count int
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM targets WHERE namespace = ? AND name = ?
	`, namespace, name).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ListTargetsFiltered returns targets matching filters with pagination.
func (s *Store) ListTargetsFiltered(namespace string, labels map[string]string, limit int, cursor string) ([]*Target, string, error) {
	// For now, simple implementation without label filtering
	// TODO: Implement proper label filtering with JSON queries
	query := `
		SELECT name, description, config, source, content_hash, synced_at,
		       created_at, updated_at, version
		FROM targets WHERE namespace = ?`
	args := []interface{}{namespace}

	if cursor != "" {
		query += ` AND name > ?`
		args = append(args, cursor)
	}

	query += ` ORDER BY name LIMIT ?`
	args = append(args, limit+1) // Fetch one extra to determine if there's more

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("query targets: %w", err)
	}
	defer rows.Close()

	var targets []*Target
	for rows.Next() {
		t := &Target{Namespace: namespace}
		var configJSON sql.NullString
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&t.Name, &t.Description, &configJSON, &source, &contentHash, &syncedAt,
			&t.CreatedAt, &t.UpdatedAt, &t.Version,
		); err != nil {
			return nil, "", fmt.Errorf("scan target: %w", err)
		}

		if configJSON.Valid && configJSON.String != "" {
			t.Config = &TargetConfig{}
			json.Unmarshal([]byte(configJSON.String), t.Config)
		}
		if source.Valid {
			t.Source = source.String
		}
		if contentHash.Valid {
			t.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			t.SyncedAt = &syncedAt.Time
		}

		targets = append(targets, t)
	}

	// Check for next cursor
	var nextCursor string
	if len(targets) > limit {
		nextCursor = targets[limit-1].Name
		targets = targets[:limit]
	}

	return targets, nextCursor, rows.Err()
}

// GetTargetStats returns statistics for a target.
func (s *Store) GetTargetStats(namespace, name string) (*TargetStats, error) {
	stats := &TargetStats{}

	// Count pollers
	err := s.db.QueryRow(`
		SELECT COUNT(*) FROM pollers WHERE namespace = ? AND target = ?
	`, namespace, name).Scan(&stats.PollerCount)
	if err != nil {
		return nil, fmt.Errorf("count pollers: %w", err)
	}

	// Count enabled pollers
	err = s.db.QueryRow(`
		SELECT COUNT(*) FROM pollers WHERE namespace = ? AND target = ? AND admin_state = 'enabled'
	`, namespace, name).Scan(&stats.EnabledCount)
	if err != nil {
		return nil, fmt.Errorf("count enabled pollers: %w", err)
	}

	return stats, nil
}

// =============================================================================
// Bulk Operations (for Sync)
// =============================================================================

// BulkCreateTargets creates multiple targets.
func (s *Store) BulkCreateTargets(targets []*Target) error {
	if len(targets) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			INSERT INTO targets (namespace, name, description, labels, config,
			                     source, content_hash, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, t := range targets {
			if t.Source == "" {
				t.Source = "yaml"
			}

			labelsJSON, _ := json.Marshal(t.Labels)
			var configJSON []byte
			if t.Config != nil {
				configJSON, _ = json.Marshal(t.Config)
			}

			_, err := stmt.Exec(t.Namespace, t.Name, t.Description, labelsJSON, configJSON,
				t.Source, t.ContentHash, now, now)
			if err != nil {
				return fmt.Errorf("create target %s/%s: %w", t.Namespace, t.Name, err)
			}

			t.CreatedAt = now
			t.UpdatedAt = now
			t.Version = 1
		}
		return nil
	})
}

// BulkUpdateTargets updates multiple targets.
func (s *Store) BulkUpdateTargets(targets []*Target) error {
	if len(targets) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE targets 
			SET description = ?, labels = ?, config = ?, source = ?, 
			    content_hash = ?, synced_at = ?, updated_at = ?, version = version + 1
			WHERE namespace = ? AND name = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, t := range targets {
			labelsJSON, _ := json.Marshal(t.Labels)
			var configJSON []byte
			if t.Config != nil {
				configJSON, _ = json.Marshal(t.Config)
			}

			_, err := stmt.Exec(t.Description, labelsJSON, configJSON, t.Source,
				t.ContentHash, &now, now, t.Namespace, t.Name)
			if err != nil {
				return fmt.Errorf("update target %s/%s: %w", t.Namespace, t.Name, err)
			}

			t.SyncedAt = &now
			t.UpdatedAt = now
		}
		return nil
	})
}

// BulkDeleteTargets deletes multiple targets by key (namespace/name).
func (s *Store) BulkDeleteTargets(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`DELETE FROM targets WHERE namespace = ? AND name = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, key := range keys {
			ns, name := parseTargetKey(key)
			if ns == "" || name == "" {
				continue
			}
			if _, err := stmt.Exec(ns, name); err != nil {
				return fmt.Errorf("delete target %s: %w", key, err)
			}
		}
		return nil
	})
}

// parseTargetKey splits "namespace/name" into parts.
func parseTargetKey(key string) (namespace, name string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return "", ""
}

// =============================================================================
// Label-Based Queries (for Tree Views)
// =============================================================================

// ListTargetsByLabel returns targets matching a label key/value.
func (s *Store) ListTargetsByLabel(namespace, labelKey, labelValue string) ([]*Target, error) {
	// DuckDB JSON query syntax
	query := `
		SELECT namespace, name, description, labels, config, 
		       source, content_hash, synced_at, created_at, updated_at, version
		FROM targets 
		WHERE namespace = ? 
		  AND json_extract_string(labels, '$.' || ?) = ?
		ORDER BY name
	`

	rows, err := s.db.Query(query, namespace, labelKey, labelValue)
	if err != nil {
		return nil, fmt.Errorf("query targets by label: %w", err)
	}
	defer rows.Close()

	return scanTargets(rows)
}

// ListTargetsWithLabel returns targets that have a specific label (any value).
func (s *Store) ListTargetsWithLabel(namespace, labelKey string) ([]*Target, error) {
	query := `
		SELECT namespace, name, description, labels, config, 
		       source, content_hash, synced_at, created_at, updated_at, version
		FROM targets 
		WHERE namespace = ? 
		  AND json_extract_string(labels, '$.' || ?) IS NOT NULL
		ORDER BY name
	`

	rows, err := s.db.Query(query, namespace, labelKey)
	if err != nil {
		return nil, fmt.Errorf("query targets with label: %w", err)
	}
	defer rows.Close()

	return scanTargets(rows)
}

// ListDistinctLabelValues returns distinct values for a label key.
func (s *Store) ListDistinctLabelValues(namespace, labelKey string) ([]string, error) {
	query := `
		SELECT DISTINCT json_extract_string(labels, '$.' || ?) as val
		FROM targets 
		WHERE namespace = ? 
		  AND val IS NOT NULL
		ORDER BY val
	`

	rows, err := s.db.Query(query, labelKey, namespace)
	if err != nil {
		return nil, fmt.Errorf("query label values: %w", err)
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		values = append(values, v)
	}

	return values, rows.Err()
}
