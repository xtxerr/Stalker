// Package sync - Store Extensions Documentation
//
// This file documents the required changes to store entity types
// to support the sync engine.
//
// REQUIRED CHANGES TO internal/store TYPES:
//
// 1. Namespace (internal/store/namespace.go):
//
//    type Namespace struct {
//        Name        string
//        Description string
//        Config      *NamespaceConfig
//        CreatedAt   time.Time
//        UpdatedAt   time.Time
//        Version     int
//        Source      string  // NEW: "yaml", "api", "import"
//    }
//
// 2. Target (internal/store/target.go):
//
//    type Target struct {
//        Namespace   string
//        Name        string
//        Description string
//        Labels      map[string]string
//        Config      *TargetConfig
//        CreatedAt   time.Time
//        UpdatedAt   time.Time
//        Version     int
//        Source      string  // NEW: "yaml", "api", "import"
//    }
//
// 3. Poller (internal/store/poller.go):
//
//    type Poller struct {
//        Namespace      string
//        Target         string
//        Name           string
//        Description    string
//        Protocol       string
//        ProtocolConfig json.RawMessage
//        PollingConfig  *PollingConfig
//        AdminState     string
//        CreatedAt      time.Time
//        UpdatedAt      time.Time
//        Version        int
//        Source         string  // NEW: "yaml", "api", "import"
//    }
//
// 4. TreeNode (internal/store/tree.go):
//
//    type TreeNode struct {
//        Namespace   string
//        Path        string
//        NodeType    string
//        LinkRef     string
//        Description string
//        CreatedAt   time.Time
//        Source      string  // NEW: "yaml", "api", "import"
//    }
//
// REQUIRED QUERY CHANGES:
//
// All SELECT queries must include the source column.
// All INSERT queries must include the source column.
// All UPDATE queries must preserve the source column (unless explicitly changing it).

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
		// TODO: Parse config JSON

		result = append(result, WrapNamespace(ns))
	}

	return result, rows.Err()
}

// BulkCreate implements SyncStore.
func (s *NamespaceSyncStore) BulkCreate(ctx context.Context, entities []*SyncableNamespace) error {
	if len(entities) == 0 {
		return nil
	}

	// Build bulk insert
	var values []string
	var args []interface{}

	for i, e := range entities {
		ns := e.UnwrapNamespace()
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)",
			i*5+1, i*5+2, i*5+3, i*5+4, i*5+5))
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
func (s *NamespaceSyncStore) BulkDelete(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	placeholders := make([]string, len(keys))
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
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
func (s *TargetSyncStore) ListAll(ctx context.Context, namespace string) ([]*SyncableTarget, error) {
	query := `
		SELECT namespace, name, description, labels, config, source, created_at, updated_at, version
		FROM targets
	`
	args := []interface{}{}

	if namespace != "" {
		query += " WHERE namespace = $1"
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
func (s *PollerSyncStore) ListAll(ctx context.Context, namespace string) ([]*SyncablePoller, error) {
	query := `
		SELECT namespace, target, name, description, protocol, protocol_config, 
		       polling_config, admin_state, source, created_at, updated_at, version
		FROM pollers
	`
	args := []interface{}{}

	if namespace != "" {
		query += " WHERE namespace = $1"
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
