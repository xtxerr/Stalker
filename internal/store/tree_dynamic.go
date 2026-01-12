// Package store - Tree Views and Smart Folders
//
// LOCATION: internal/store/tree_dynamic.go
//
// Provides operations for label-based views and query-based smart folders.

package store

import (
	"database/sql"
	"fmt"
	"time"
)

// =============================================================================
// Tree View Types
// =============================================================================

// TreeView represents a label-based view definition.
// Views automatically create tree paths based on entity labels.
type TreeView struct {
	Namespace    string
	Name         string
	Description  string
	PathTemplate string // e.g., "/by-site/{label:site}/{name}"
	EntityType   string // "target" or "poller"
	FilterExpr   string // e.g., "label:site"
	Priority     int
	Enabled      bool
	CreatedAt    time.Time
	UpdatedAt    time.Time

	// Sync support
	Source      string
	ContentHash uint64
	SyncedAt    *time.Time
}

// =============================================================================
// Tree View CRUD
// =============================================================================

// CreateTreeView creates a new tree view.
func (s *Store) CreateTreeView(v *TreeView) error {
	if v.Source == "" {
		v.Source = "api"
	}
	if v.Priority == 0 {
		v.Priority = 50
	}

	now := time.Now()
	_, err := s.db.Exec(`
		INSERT INTO tree_views (namespace, name, description, path_template, entity_type,
		                        filter_expr, priority, enabled, source, content_hash,
		                        created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, v.Namespace, v.Name, v.Description, v.PathTemplate, v.EntityType,
		v.FilterExpr, v.Priority, v.Enabled, v.Source, v.ContentHash, now, now)

	if err != nil {
		return fmt.Errorf("insert tree view: %w", err)
	}

	v.CreatedAt = now
	v.UpdatedAt = now
	return nil
}

// GetTreeView retrieves a tree view by namespace and name.
func (s *Store) GetTreeView(namespace, name string) (*TreeView, error) {
	v := &TreeView{}
	var source sql.NullString
	var contentHash sql.NullInt64
	var syncedAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT namespace, name, description, path_template, entity_type,
		       filter_expr, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_views WHERE namespace = ? AND name = ?
	`, namespace, name).Scan(
		&v.Namespace, &v.Name, &v.Description, &v.PathTemplate, &v.EntityType,
		&v.FilterExpr, &v.Priority, &v.Enabled, &source, &contentHash, &syncedAt,
		&v.CreatedAt, &v.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query tree view: %w", err)
	}

	if source.Valid {
		v.Source = source.String
	}
	if contentHash.Valid {
		v.ContentHash = uint64(contentHash.Int64)
	}
	if syncedAt.Valid {
		v.SyncedAt = &syncedAt.Time
	}

	return v, nil
}

// ListTreeViews returns all views in a namespace.
func (s *Store) ListTreeViews(namespace string) ([]*TreeView, error) {
	rows, err := s.db.Query(`
		SELECT namespace, name, description, path_template, entity_type,
		       filter_expr, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_views WHERE namespace = ? ORDER BY priority DESC, name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query tree views: %w", err)
	}
	defer rows.Close()

	return scanTreeViews(rows)
}

// ListAllTreeViews returns all views across all namespaces.
func (s *Store) ListAllTreeViews() ([]*TreeView, error) {
	rows, err := s.db.Query(`
		SELECT namespace, name, description, path_template, entity_type,
		       filter_expr, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_views ORDER BY namespace, priority DESC, name
	`)
	if err != nil {
		return nil, fmt.Errorf("query tree views: %w", err)
	}
	defer rows.Close()

	return scanTreeViews(rows)
}

// ListEnabledTreeViews returns all enabled views in a namespace.
func (s *Store) ListEnabledTreeViews(namespace string) ([]*TreeView, error) {
	rows, err := s.db.Query(`
		SELECT namespace, name, description, path_template, entity_type,
		       filter_expr, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_views WHERE namespace = ? AND enabled = true
		ORDER BY priority DESC, name
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query tree views: %w", err)
	}
	defer rows.Close()

	return scanTreeViews(rows)
}

func scanTreeViews(rows *sql.Rows) ([]*TreeView, error) {
	var views []*TreeView
	for rows.Next() {
		v := &TreeView{}
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&v.Namespace, &v.Name, &v.Description, &v.PathTemplate, &v.EntityType,
			&v.FilterExpr, &v.Priority, &v.Enabled, &source, &contentHash, &syncedAt,
			&v.CreatedAt, &v.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tree view: %w", err)
		}

		if source.Valid {
			v.Source = source.String
		}
		if contentHash.Valid {
			v.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			v.SyncedAt = &syncedAt.Time
		}

		views = append(views, v)
	}

	return views, rows.Err()
}

// UpdateTreeView updates an existing tree view.
func (s *Store) UpdateTreeView(v *TreeView) error {
	now := time.Now()
	_, err := s.db.Exec(`
		UPDATE tree_views 
		SET description = ?, path_template = ?, entity_type = ?, filter_expr = ?,
		    priority = ?, enabled = ?, source = ?, content_hash = ?, synced_at = ?,
		    updated_at = ?
		WHERE namespace = ? AND name = ?
	`, v.Description, v.PathTemplate, v.EntityType, v.FilterExpr,
		v.Priority, v.Enabled, v.Source, v.ContentHash, v.SyncedAt,
		now, v.Namespace, v.Name)

	if err != nil {
		return fmt.Errorf("update tree view: %w", err)
	}

	v.UpdatedAt = now
	return nil
}

// DeleteTreeView deletes a tree view.
func (s *Store) DeleteTreeView(namespace, name string) error {
	_, err := s.db.Exec(`DELETE FROM tree_views WHERE namespace = ? AND name = ?`,
		namespace, name)
	return err
}

// =============================================================================
// Tree View Bulk Operations
// =============================================================================

// BulkCreateTreeViews creates multiple tree views.
func (s *Store) BulkCreateTreeViews(views []*TreeView) error {
	if len(views) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			INSERT INTO tree_views (namespace, name, description, path_template, entity_type,
			                        filter_expr, priority, enabled, source, content_hash,
			                        created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, v := range views {
			if v.Source == "" {
				v.Source = "yaml"
			}
			if v.Priority == 0 {
				v.Priority = 50
			}

			_, err := stmt.Exec(v.Namespace, v.Name, v.Description, v.PathTemplate, v.EntityType,
				v.FilterExpr, v.Priority, v.Enabled, v.Source, v.ContentHash, now, now)
			if err != nil {
				return fmt.Errorf("create tree view %s/%s: %w", v.Namespace, v.Name, err)
			}

			v.CreatedAt = now
			v.UpdatedAt = now
		}
		return nil
	})
}

// BulkUpdateTreeViews updates multiple tree views.
func (s *Store) BulkUpdateTreeViews(views []*TreeView) error {
	if len(views) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE tree_views 
			SET description = ?, path_template = ?, entity_type = ?, filter_expr = ?,
			    priority = ?, enabled = ?, source = ?, content_hash = ?, synced_at = ?,
			    updated_at = ?
			WHERE namespace = ? AND name = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, v := range views {
			_, err := stmt.Exec(v.Description, v.PathTemplate, v.EntityType, v.FilterExpr,
				v.Priority, v.Enabled, v.Source, v.ContentHash, &now, now, v.Namespace, v.Name)
			if err != nil {
				return fmt.Errorf("update tree view %s/%s: %w", v.Namespace, v.Name, err)
			}

			v.SyncedAt = &now
			v.UpdatedAt = now
		}
		return nil
	})
}

// BulkDeleteTreeViews deletes multiple tree views by key.
func (s *Store) BulkDeleteTreeViews(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`DELETE FROM tree_views WHERE namespace = ? AND name = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, key := range keys {
			ns, name := parseViewKey(key)
			if ns == "" {
				continue
			}
			if _, err := stmt.Exec(ns, name); err != nil {
				return fmt.Errorf("delete tree view %s: %w", key, err)
			}
		}
		return nil
	})
}

// parseViewKey splits "namespace/view/name" into parts.
func parseViewKey(key string) (namespace, name string) {
	// Key format: "namespace/view/name"
	parts := splitKey(key, 3)
	if len(parts) == 3 && parts[1] == "view" {
		return parts[0], parts[2]
	}
	return "", ""
}

// =============================================================================
// Smart Folder Types
// =============================================================================

// TreeSmartFolder represents a query-based smart folder.
// Contents are computed dynamically based on queries.
type TreeSmartFolder struct {
	Namespace   string
	Path        string
	Description string
	TargetQuery string // Query for matching targets
	PollerQuery string // Query for matching pollers
	CacheTTL    int    // Cache TTL in seconds
	Priority    int
	Enabled     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time

	// Sync support
	Source      string
	ContentHash uint64
	SyncedAt    *time.Time
}

// =============================================================================
// Smart Folder CRUD
// =============================================================================

// CreateSmartFolder creates a new smart folder.
func (s *Store) CreateSmartFolder(f *TreeSmartFolder) error {
	if f.Source == "" {
		f.Source = "api"
	}
	if f.Priority == 0 {
		f.Priority = 25
	}
	if f.CacheTTL == 0 {
		f.CacheTTL = 60
	}

	now := time.Now()
	_, err := s.db.Exec(`
		INSERT INTO tree_smart_folders (namespace, path, description, target_query,
		                                poller_query, cache_ttl_sec, priority, enabled,
		                                source, content_hash, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, f.Namespace, f.Path, f.Description, f.TargetQuery,
		f.PollerQuery, f.CacheTTL, f.Priority, f.Enabled,
		f.Source, f.ContentHash, now, now)

	if err != nil {
		return fmt.Errorf("insert smart folder: %w", err)
	}

	f.CreatedAt = now
	f.UpdatedAt = now
	return nil
}

// GetSmartFolder retrieves a smart folder by namespace and path.
func (s *Store) GetSmartFolder(namespace, path string) (*TreeSmartFolder, error) {
	f := &TreeSmartFolder{}
	var source sql.NullString
	var contentHash sql.NullInt64
	var syncedAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT namespace, path, description, target_query, poller_query,
		       cache_ttl_sec, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_smart_folders WHERE namespace = ? AND path = ?
	`, namespace, path).Scan(
		&f.Namespace, &f.Path, &f.Description, &f.TargetQuery, &f.PollerQuery,
		&f.CacheTTL, &f.Priority, &f.Enabled, &source, &contentHash, &syncedAt,
		&f.CreatedAt, &f.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query smart folder: %w", err)
	}

	if source.Valid {
		f.Source = source.String
	}
	if contentHash.Valid {
		f.ContentHash = uint64(contentHash.Int64)
	}
	if syncedAt.Valid {
		f.SyncedAt = &syncedAt.Time
	}

	return f, nil
}

// ListSmartFolders returns all smart folders in a namespace.
func (s *Store) ListSmartFolders(namespace string) ([]*TreeSmartFolder, error) {
	rows, err := s.db.Query(`
		SELECT namespace, path, description, target_query, poller_query,
		       cache_ttl_sec, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_smart_folders WHERE namespace = ? ORDER BY path
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query smart folders: %w", err)
	}
	defer rows.Close()

	return scanSmartFolders(rows)
}

// ListAllSmartFolders returns all smart folders across all namespaces.
func (s *Store) ListAllSmartFolders() ([]*TreeSmartFolder, error) {
	rows, err := s.db.Query(`
		SELECT namespace, path, description, target_query, poller_query,
		       cache_ttl_sec, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_smart_folders ORDER BY namespace, path
	`)
	if err != nil {
		return nil, fmt.Errorf("query smart folders: %w", err)
	}
	defer rows.Close()

	return scanSmartFolders(rows)
}

// ListEnabledSmartFolders returns all enabled smart folders in a namespace.
func (s *Store) ListEnabledSmartFolders(namespace string) ([]*TreeSmartFolder, error) {
	rows, err := s.db.Query(`
		SELECT namespace, path, description, target_query, poller_query,
		       cache_ttl_sec, priority, enabled, source, content_hash, synced_at,
		       created_at, updated_at
		FROM tree_smart_folders WHERE namespace = ? AND enabled = true ORDER BY path
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query smart folders: %w", err)
	}
	defer rows.Close()

	return scanSmartFolders(rows)
}

func scanSmartFolders(rows *sql.Rows) ([]*TreeSmartFolder, error) {
	var folders []*TreeSmartFolder
	for rows.Next() {
		f := &TreeSmartFolder{}
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&f.Namespace, &f.Path, &f.Description, &f.TargetQuery, &f.PollerQuery,
			&f.CacheTTL, &f.Priority, &f.Enabled, &source, &contentHash, &syncedAt,
			&f.CreatedAt, &f.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan smart folder: %w", err)
		}

		if source.Valid {
			f.Source = source.String
		}
		if contentHash.Valid {
			f.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			f.SyncedAt = &syncedAt.Time
		}

		folders = append(folders, f)
	}

	return folders, rows.Err()
}

// UpdateSmartFolder updates an existing smart folder.
func (s *Store) UpdateSmartFolder(f *TreeSmartFolder) error {
	now := time.Now()
	_, err := s.db.Exec(`
		UPDATE tree_smart_folders 
		SET description = ?, target_query = ?, poller_query = ?, cache_ttl_sec = ?,
		    priority = ?, enabled = ?, source = ?, content_hash = ?, synced_at = ?,
		    updated_at = ?
		WHERE namespace = ? AND path = ?
	`, f.Description, f.TargetQuery, f.PollerQuery, f.CacheTTL,
		f.Priority, f.Enabled, f.Source, f.ContentHash, f.SyncedAt,
		now, f.Namespace, f.Path)

	if err != nil {
		return fmt.Errorf("update smart folder: %w", err)
	}

	f.UpdatedAt = now
	return nil
}

// DeleteSmartFolder deletes a smart folder.
func (s *Store) DeleteSmartFolder(namespace, path string) error {
	_, err := s.db.Exec(`DELETE FROM tree_smart_folders WHERE namespace = ? AND path = ?`,
		namespace, path)
	return err
}

// =============================================================================
// Smart Folder Bulk Operations
// =============================================================================

// BulkCreateSmartFolders creates multiple smart folders.
func (s *Store) BulkCreateSmartFolders(folders []*TreeSmartFolder) error {
	if len(folders) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			INSERT INTO tree_smart_folders (namespace, path, description, target_query,
			                                poller_query, cache_ttl_sec, priority, enabled,
			                                source, content_hash, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, f := range folders {
			if f.Source == "" {
				f.Source = "yaml"
			}
			if f.Priority == 0 {
				f.Priority = 25
			}
			if f.CacheTTL == 0 {
				f.CacheTTL = 60
			}

			_, err := stmt.Exec(f.Namespace, f.Path, f.Description, f.TargetQuery,
				f.PollerQuery, f.CacheTTL, f.Priority, f.Enabled,
				f.Source, f.ContentHash, now, now)
			if err != nil {
				return fmt.Errorf("create smart folder %s:%s: %w", f.Namespace, f.Path, err)
			}

			f.CreatedAt = now
			f.UpdatedAt = now
		}
		return nil
	})
}

// BulkUpdateSmartFolders updates multiple smart folders.
func (s *Store) BulkUpdateSmartFolders(folders []*TreeSmartFolder) error {
	if len(folders) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE tree_smart_folders 
			SET description = ?, target_query = ?, poller_query = ?, cache_ttl_sec = ?,
			    priority = ?, enabled = ?, source = ?, content_hash = ?, synced_at = ?,
			    updated_at = ?
			WHERE namespace = ? AND path = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, f := range folders {
			_, err := stmt.Exec(f.Description, f.TargetQuery, f.PollerQuery, f.CacheTTL,
				f.Priority, f.Enabled, f.Source, f.ContentHash, &now, now,
				f.Namespace, f.Path)
			if err != nil {
				return fmt.Errorf("update smart folder %s:%s: %w", f.Namespace, f.Path, err)
			}

			f.SyncedAt = &now
			f.UpdatedAt = now
		}
		return nil
	})
}

// BulkDeleteSmartFolders deletes multiple smart folders by key.
func (s *Store) BulkDeleteSmartFolders(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`DELETE FROM tree_smart_folders WHERE namespace = ? AND path = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, key := range keys {
			ns, path := parseSmartFolderKey(key)
			if ns == "" {
				continue
			}
			if _, err := stmt.Exec(ns, path); err != nil {
				return fmt.Errorf("delete smart folder %s: %w", key, err)
			}
		}
		return nil
	})
}

// parseSmartFolderKey splits "namespace/smart:/path" into parts.
func parseSmartFolderKey(key string) (namespace, path string) {
	// Key format: "namespace/smart:/path"
	idx := 0
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			idx = i
			break
		}
	}
	if idx == 0 {
		return "", ""
	}

	namespace = key[:idx]
	rest := key[idx+1:]

	if len(rest) > 6 && rest[:6] == "smart:" {
		path = rest[6:]
		return namespace, path
	}

	return "", ""
}

// =============================================================================
// Helper Functions
// =============================================================================

func splitKey(key string, maxParts int) []string {
	var parts []string
	start := 0
	for i := 0; i < len(key) && len(parts) < maxParts-1; i++ {
		if key[i] == '/' {
			parts = append(parts, key[start:i])
			start = i + 1
		}
	}
	if start < len(key) {
		parts = append(parts, key[start:])
	}
	return parts
}
