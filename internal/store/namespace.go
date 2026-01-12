// Package store - Namespace operations
//
// LOCATION: internal/store/namespace.go
//
// Provides CRUD operations for namespaces with sync support.

package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// =============================================================================
// Namespace Types
// =============================================================================

// Namespace represents a multi-tenant namespace.
type Namespace struct {
	Name        string
	Description string
	Config      *NamespaceConfig
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Version     int

	// Sync support
	Source      string // "yaml", "api", "import"
	ContentHash uint64
	SyncedAt    *time.Time
}

// NamespaceConfig holds namespace-level configuration.
type NamespaceConfig struct {
	Defaults                  *PollerDefaults `json:"defaults,omitempty"`
	SessionCleanupIntervalSec *int            `json:"session_cleanup_interval_sec,omitempty"`
}

// =============================================================================
// CRUD Operations
// =============================================================================

// CreateNamespace creates a new namespace.
func (s *Store) CreateNamespace(ns *Namespace) error {
	if ns.Source == "" {
		ns.Source = "api"
	}

	var configJSON []byte
	if ns.Config != nil {
		var err error
		configJSON, err = json.Marshal(ns.Config)
		if err != nil {
			return fmt.Errorf("marshal config: %w", err)
		}
	}

	now := time.Now()
	_, err := s.db.Exec(`
		INSERT INTO namespaces (name, description, config, source, content_hash, created_at, updated_at, version)
		VALUES (?, ?, ?, ?, ?, ?, ?, 1)
	`, ns.Name, ns.Description, configJSON, ns.Source, ns.ContentHash, now, now)

	if err != nil {
		return fmt.Errorf("insert namespace: %w", err)
	}

	ns.CreatedAt = now
	ns.UpdatedAt = now
	ns.Version = 1
	return nil
}

// GetNamespace retrieves a namespace by name.
func (s *Store) GetNamespace(name string) (*Namespace, error) {
	ns := &Namespace{}
	var configJSON sql.NullString
	var source sql.NullString
	var contentHash sql.NullInt64
	var syncedAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT name, description, config, source, content_hash, synced_at,
		       created_at, updated_at, version
		FROM namespaces WHERE name = ?
	`, name).Scan(
		&ns.Name, &ns.Description, &configJSON, &source, &contentHash, &syncedAt,
		&ns.CreatedAt, &ns.UpdatedAt, &ns.Version,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query namespace: %w", err)
	}

	if configJSON.Valid && configJSON.String != "" {
		ns.Config = &NamespaceConfig{}
		if err := json.Unmarshal([]byte(configJSON.String), ns.Config); err != nil {
			return nil, fmt.Errorf("unmarshal config: %w", err)
		}
	}

	if source.Valid {
		ns.Source = source.String
	}
	if contentHash.Valid {
		ns.ContentHash = uint64(contentHash.Int64)
	}
	if syncedAt.Valid {
		ns.SyncedAt = &syncedAt.Time
	}

	return ns, nil
}

// ListNamespaces returns all namespaces.
func (s *Store) ListNamespaces() ([]*Namespace, error) {
	rows, err := s.db.Query(`
		SELECT name, description, config, source, content_hash, synced_at,
		       created_at, updated_at, version
		FROM namespaces ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("query namespaces: %w", err)
	}
	defer rows.Close()

	var namespaces []*Namespace
	for rows.Next() {
		ns := &Namespace{}
		var configJSON sql.NullString
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&ns.Name, &ns.Description, &configJSON, &source, &contentHash, &syncedAt,
			&ns.CreatedAt, &ns.UpdatedAt, &ns.Version,
		); err != nil {
			return nil, fmt.Errorf("scan namespace: %w", err)
		}

		if configJSON.Valid && configJSON.String != "" {
			ns.Config = &NamespaceConfig{}
			if err := json.Unmarshal([]byte(configJSON.String), ns.Config); err != nil {
				return nil, fmt.Errorf("unmarshal config: %w", err)
			}
		}

		if source.Valid {
			ns.Source = source.String
		}
		if contentHash.Valid {
			ns.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			ns.SyncedAt = &syncedAt.Time
		}

		namespaces = append(namespaces, ns)
	}

	return namespaces, rows.Err()
}

// UpdateNamespace updates an existing namespace.
func (s *Store) UpdateNamespace(ns *Namespace) error {
	var configJSON []byte
	if ns.Config != nil {
		var err error
		configJSON, err = json.Marshal(ns.Config)
		if err != nil {
			return fmt.Errorf("marshal config: %w", err)
		}
	}

	now := time.Now()
	result, err := s.db.Exec(`
		UPDATE namespaces 
		SET description = ?, config = ?, source = ?, content_hash = ?, 
		    synced_at = ?, updated_at = ?, version = version + 1
		WHERE name = ? AND version = ?
	`, ns.Description, configJSON, ns.Source, ns.ContentHash,
		ns.SyncedAt, now, ns.Name, ns.Version)

	if err != nil {
		return fmt.Errorf("update namespace: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("namespace not found or version mismatch")
	}

	ns.UpdatedAt = now
	ns.Version++
	return nil
}

// DeleteNamespace deletes a namespace.
func (s *Store) DeleteNamespace(name string) error {
	_, err := s.db.Exec(`DELETE FROM namespaces WHERE name = ?`, name)
	return err
}

// NamespaceExists checks if a namespace exists.
func (s *Store) NamespaceExists(name string) (bool, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM namespaces WHERE name = ?`, name).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// =============================================================================
// Bulk Operations (for Sync)
// =============================================================================

// BulkCreateNamespaces creates multiple namespaces.
func (s *Store) BulkCreateNamespaces(namespaces []*Namespace) error {
	if len(namespaces) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			INSERT INTO namespaces (name, description, config, source, content_hash, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, 1)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, ns := range namespaces {
			if ns.Source == "" {
				ns.Source = "yaml"
			}

			var configJSON []byte
			if ns.Config != nil {
				configJSON, _ = json.Marshal(ns.Config)
			}

			_, err := stmt.Exec(ns.Name, ns.Description, configJSON, ns.Source, ns.ContentHash, now, now)
			if err != nil {
				return fmt.Errorf("create namespace %s: %w", ns.Name, err)
			}

			ns.CreatedAt = now
			ns.UpdatedAt = now
			ns.Version = 1
		}
		return nil
	})
}

// BulkUpdateNamespaces updates multiple namespaces.
func (s *Store) BulkUpdateNamespaces(namespaces []*Namespace) error {
	if len(namespaces) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE namespaces 
			SET description = ?, config = ?, source = ?, content_hash = ?, 
			    synced_at = ?, updated_at = ?, version = version + 1
			WHERE name = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, ns := range namespaces {
			var configJSON []byte
			if ns.Config != nil {
				configJSON, _ = json.Marshal(ns.Config)
			}

			_, err := stmt.Exec(ns.Description, configJSON, ns.Source, ns.ContentHash, &now, now, ns.Name)
			if err != nil {
				return fmt.Errorf("update namespace %s: %w", ns.Name, err)
			}

			ns.SyncedAt = &now
			ns.UpdatedAt = now
		}
		return nil
	})
}

// BulkDeleteNamespaces deletes multiple namespaces by name.
func (s *Store) BulkDeleteNamespaces(names []string) error {
	if len(names) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`DELETE FROM namespaces WHERE name = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, name := range names {
			if _, err := stmt.Exec(name); err != nil {
				return fmt.Errorf("delete namespace %s: %w", name, err)
			}
		}
		return nil
	})
}

// ListNamespacesBySource returns namespaces filtered by source.
func (s *Store) ListNamespacesBySource(source string) ([]*Namespace, error) {
	rows, err := s.db.Query(`
		SELECT name, description, config, source, content_hash, synced_at,
		       created_at, updated_at, version
		FROM namespaces WHERE source = ? ORDER BY name
	`, source)
	if err != nil {
		return nil, fmt.Errorf("query namespaces: %w", err)
	}
	defer rows.Close()

	var namespaces []*Namespace
	for rows.Next() {
		ns := &Namespace{}
		var configJSON sql.NullString
		var sourceVal sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&ns.Name, &ns.Description, &configJSON, &sourceVal, &contentHash, &syncedAt,
			&ns.CreatedAt, &ns.UpdatedAt, &ns.Version,
		); err != nil {
			return nil, fmt.Errorf("scan namespace: %w", err)
		}

		if configJSON.Valid && configJSON.String != "" {
			ns.Config = &NamespaceConfig{}
			json.Unmarshal([]byte(configJSON.String), ns.Config)
		}
		if sourceVal.Valid {
			ns.Source = sourceVal.String
		}
		if contentHash.Valid {
			ns.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			ns.SyncedAt = &syncedAt.Time
		}

		namespaces = append(namespaces, ns)
	}

	return namespaces, rows.Err()
}
