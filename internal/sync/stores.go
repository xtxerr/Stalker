// Package sync - Store Adapters for Sync Engine
//
// FIX #22: Changed all SQL placeholders from PostgreSQL-style ($1, $2, ...)
// to DuckDB/SQLite-style (?, ?, ...). DuckDB does not support $N placeholders.
//
// This file provides SyncStore implementations for:
//   - Namespaces
//   - Targets
//   - Pollers
//
// Each adapter wraps the underlying *sql.DB and implements bulk operations
// needed by the sync engine.

package sync

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Namespace Store Adapter
// =============================================================================

// NamespaceSyncStore implements SyncStore for namespaces.
type NamespaceSyncStore struct {
	db *sql.DB
}

// NewNamespaceSyncStore creates a new namespace sync store.
func NewNamespaceSyncStore(db *sql.DB) *NamespaceSyncStore {
	return &NamespaceSyncStore{db: db}
}

// ListAll implements SyncStore.
func (s *NamespaceSyncStore) ListAll(ctx context.Context, namespace string) ([]*SyncableNamespace, error) {
	// Note: namespace parameter is ignored for namespaces (they are top-level)
	query := `
		SELECT name, description, config, source, created_at, updated_at, version
		FROM namespaces
	`

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query namespaces: %w", err)
	}
	defer rows.Close()

	var result []*SyncableNamespace
	for rows.Next() {
		ns := &store.Namespace{}
		var config, source sql.NullString

		err := rows.Scan(
			&ns.Name,
			&ns.Description,
			&config,
			&source,
			&ns.CreatedAt,
			&ns.UpdatedAt,
			&ns.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("scan namespace: %w", err)
		}

		if source.Valid {
			ns.Source = source.String
		}
		// TODO: Parse config JSON if needed

		result = append(result, WrapNamespace(ns))
	}

	return result, rows.Err()
}

// BulkCreate implements SyncStore.
//
// FIX #22: Changed from PostgreSQL $N placeholders to DuckDB ? placeholders.
func (s *NamespaceSyncStore) BulkCreate(ctx context.Context, entities []*SyncableNamespace) error {
	if len(entities) == 0 {
		return nil
	}

	// Build bulk insert with ? placeholders (DuckDB compatible)
	var values []string
	var args []interface{}

	for _, e := range entities {
		ns := e.UnwrapNamespace()
		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, ns.Name, ns.Description, nil, ns.Source, time.Now())
	}

	query := fmt.Sprintf(`
		INSERT INTO namespaces (name, description, config, source, created_at)
		VALUES %s
	`, strings.Join(values, ", "))

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// BulkUpdate implements SyncStore.
func (s *NamespaceSyncStore) BulkUpdate(ctx context.Context, entities []*SyncableNamespace) error {
	for _, e := range entities {
		ns := e.UnwrapNamespace()
		_, err := s.db.ExecContext(ctx, `
			UPDATE namespaces 
			SET description = ?, config = ?, source = ?, updated_at = ?, version = version + 1
			WHERE name = ?
		`, ns.Description, nil, ns.Source, time.Now(), ns.Name)
		if err != nil {
			return fmt.Errorf("update namespace %s: %w", ns.Name, err)
		}
	}
	return nil
}

// BulkDelete implements SyncStore.
//
// FIX #22: Changed from PostgreSQL $N placeholders to DuckDB ? placeholders.
// Also simplified to use individual deletes for better error handling.
func (s *NamespaceSyncStore) BulkDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	// Build IN clause with ? placeholders
	placeholders := make([]string, len(keys))
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		placeholders[i] = "?"
		args[i] = key
	}

	query := fmt.Sprintf(`
		DELETE FROM namespaces WHERE name IN (%s)
	`, strings.Join(placeholders, ", "))

	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// =============================================================================
// Target Store Adapter
// =============================================================================

// TargetSyncStore implements SyncStore for targets.
type TargetSyncStore struct {
	db *sql.DB
}

// NewTargetSyncStore creates a new target sync store.
func NewTargetSyncStore(db *sql.DB) *TargetSyncStore {
	return &TargetSyncStore{db: db}
}

// ListAll implements SyncStore.
//
// FIX #22: Changed from PostgreSQL $1 placeholder to DuckDB ? placeholder.
func (s *TargetSyncStore) ListAll(ctx context.Context, namespace string) ([]*SyncableTarget, error) {
	query := `
		SELECT namespace, name, description, labels, config, source, created_at, updated_at, version
		FROM targets
	`
	args := []interface{}{}

	if namespace != "" {
		query += " WHERE namespace = ?"
		args = append(args, namespace)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query targets: %w", err)
	}
	defer rows.Close()

	var result []*SyncableTarget
	for rows.Next() {
		t := &store.Target{}
		var labels, config, source sql.NullString

		err := rows.Scan(
			&t.Namespace,
			&t.Name,
			&t.Description,
			&labels,
			&config,
			&source,
			&t.CreatedAt,
			&t.UpdatedAt,
			&t.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("scan target: %w", err)
		}

		if source.Valid {
			t.Source = source.String
		}
		// TODO: Parse labels and config JSON

		result = append(result, WrapTarget(t))
	}

	return result, rows.Err()
}

// BulkCreate implements SyncStore.
func (s *TargetSyncStore) BulkCreate(ctx context.Context, entities []*SyncableTarget) error {
	for _, e := range entities {
		t := e.UnwrapTarget()
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO targets (namespace, name, description, labels, config, source, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
		`, t.Namespace, t.Name, t.Description, nil, nil, t.Source, time.Now(), time.Now())
		if err != nil {
			return fmt.Errorf("create target %s/%s: %w", t.Namespace, t.Name, err)
		}
	}
	return nil
}

// BulkUpdate implements SyncStore.
func (s *TargetSyncStore) BulkUpdate(ctx context.Context, entities []*SyncableTarget) error {
	for _, e := range entities {
		t := e.UnwrapTarget()
		_, err := s.db.ExecContext(ctx, `
			UPDATE targets 
			SET description = ?, labels = ?, config = ?, source = ?, updated_at = ?, version = version + 1
			WHERE namespace = ? AND name = ?
		`, t.Description, nil, nil, t.Source, time.Now(), t.Namespace, t.Name)
		if err != nil {
			return fmt.Errorf("update target %s/%s: %w", t.Namespace, t.Name, err)
		}
	}
	return nil
}

// BulkDelete implements SyncStore.
func (s *TargetSyncStore) BulkDelete(ctx context.Context, keys []string) error {
	for _, key := range keys {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) != 2 {
			continue
		}
		_, err := s.db.ExecContext(ctx, `
			DELETE FROM targets WHERE namespace = ? AND name = ?
		`, parts[0], parts[1])
		if err != nil {
			return fmt.Errorf("delete target %s: %w", key, err)
		}
	}
	return nil
}

// =============================================================================
// Poller Store Adapter
// =============================================================================

// PollerSyncStore implements SyncStore for pollers.
type PollerSyncStore struct {
	db *sql.DB
}

// NewPollerSyncStore creates a new poller sync store.
func NewPollerSyncStore(db *sql.DB) *PollerSyncStore {
	return &PollerSyncStore{db: db}
}

// ListAll implements SyncStore.
//
// FIX #22: Changed from PostgreSQL $1 placeholder to DuckDB ? placeholder.
func (s *PollerSyncStore) ListAll(ctx context.Context, namespace string) ([]*SyncablePoller, error) {
	query := `
		SELECT namespace, target, name, description, protocol, protocol_config, 
		       polling_config, admin_state, source, created_at, updated_at, version
		FROM pollers
	`
	args := []interface{}{}

	if namespace != "" {
		query += " WHERE namespace = ?"
		args = append(args, namespace)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query pollers: %w", err)
	}
	defer rows.Close()

	var result []*SyncablePoller
	for rows.Next() {
		p := &store.Poller{}
		var pollingConfig, source sql.NullString

		err := rows.Scan(
			&p.Namespace,
			&p.Target,
			&p.Name,
			&p.Description,
			&p.Protocol,
			&p.ProtocolConfig,
			&pollingConfig,
			&p.AdminState,
			&source,
			&p.CreatedAt,
			&p.UpdatedAt,
			&p.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("scan poller: %w", err)
		}

		if source.Valid {
			p.Source = source.String
		}
		// TODO: Parse polling config JSON

		result = append(result, WrapPoller(p))
	}

	return result, rows.Err()
}

// BulkCreate implements SyncStore.
func (s *PollerSyncStore) BulkCreate(ctx context.Context, entities []*SyncablePoller) error {
	for _, e := range entities {
		p := e.UnwrapPoller()
		_, err := s.db.ExecContext(ctx, `
			INSERT INTO pollers (namespace, target, name, description, protocol, protocol_config,
			                     polling_config, admin_state, source, created_at, updated_at, version)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
		`, p.Namespace, p.Target, p.Name, p.Description, p.Protocol, p.ProtocolConfig,
			nil, p.AdminState, p.Source, time.Now(), time.Now())
		if err != nil {
			return fmt.Errorf("create poller %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
		}
	}
	return nil
}

// BulkUpdate implements SyncStore.
func (s *PollerSyncStore) BulkUpdate(ctx context.Context, entities []*SyncablePoller) error {
	for _, e := range entities {
		p := e.UnwrapPoller()
		_, err := s.db.ExecContext(ctx, `
			UPDATE pollers 
			SET description = ?, protocol = ?, protocol_config = ?, polling_config = ?,
			    admin_state = ?, source = ?, updated_at = ?, version = version + 1
			WHERE namespace = ? AND target = ? AND name = ?
		`, p.Description, p.Protocol, p.ProtocolConfig, nil, p.AdminState, p.Source,
			time.Now(), p.Namespace, p.Target, p.Name)
		if err != nil {
			return fmt.Errorf("update poller %s/%s/%s: %w", p.Namespace, p.Target, p.Name, err)
		}
	}
	return nil
}

// BulkDelete implements SyncStore.
func (s *PollerSyncStore) BulkDelete(ctx context.Context, keys []string) error {
	for _, key := range keys {
		parts := strings.SplitN(key, "/", 3)
		if len(parts) != 3 {
			continue
		}
		_, err := s.db.ExecContext(ctx, `
			DELETE FROM pollers WHERE namespace = ? AND target = ? AND name = ?
		`, parts[0], parts[1], parts[2])
		if err != nil {
			return fmt.Errorf("delete poller %s: %w", key, err)
		}
	}
	return nil
}
