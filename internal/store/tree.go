// Package store provides database operations for the stalker application.
//
// This file handles tree (virtual filesystem) operations including directories
// and symlinks to targets/pollers.
package store

import (
	"database/sql"
	"fmt"
	"path"
	"strings"
	"time"
)

// TreeNode represents a node in the tree (directory or symlink).
type TreeNode struct {
	Namespace   string
	Path        string
	NodeType    string // "directory", "link_target", "link_poller"
	LinkRef     string // For links: "target:name" or "poller:target/name"
	Description string
	CreatedAt   time.Time

	// Sync support
	Source      string // "yaml", "api", "import"
	ContentHash uint64
	SyncedAt    *time.Time
}

// =============================================================================
// Directory Operations
// =============================================================================

// CreateTreeDirectory creates a directory node and all parent directories.
func (s *Store) CreateTreeDirectory(namespace, nodePath, description string) error {
	nodePath = normalizePath(nodePath)

	return s.Transaction(func(tx *sql.Tx) error {
		// Create parent directories if needed
		parts := strings.Split(strings.Trim(nodePath, "/"), "/")
		currentPath := ""

		for i, part := range parts {
			if part == "" {
				continue
			}
			currentPath = currentPath + "/" + part
			desc := ""
			if i == len(parts)-1 {
				desc = description
			}

			// Try to insert (ignore if exists)
			_, err := tx.Exec(`
				INSERT OR IGNORE INTO tree_nodes (namespace, path, node_type, description, created_at)
				VALUES (?, ?, 'directory', ?, ?)
			`, namespace, currentPath, desc, time.Now())
			if err != nil {
				return fmt.Errorf("create directory %s: %w", currentPath, err)
			}
		}
		return nil
	})
}

// =============================================================================
// Link Operations
// =============================================================================

// validateLinkName validates that a link name is safe and doesn't contain
// path traversal characters.
//
// FIX #8: This function prevents path traversal attacks by rejecting link names
// that contain path separators or special directory references. Without this
// validation, an attacker could create links like "../../../admin" to escape
// the intended directory structure.
func validateLinkName(linkName string) error {
	// Check for empty name
	if linkName == "" {
		return fmt.Errorf("link name cannot be empty")
	}

	// Check for path separators
	if strings.Contains(linkName, "/") {
		return fmt.Errorf("link name cannot contain '/': %s", linkName)
	}
	if strings.Contains(linkName, "\\") {
		return fmt.Errorf("link name cannot contain '\\': %s", linkName)
	}

	// Check for special directory references
	if linkName == "." || linkName == ".." {
		return fmt.Errorf("link name cannot be '.' or '..': %s", linkName)
	}

	// Check for hidden files (starting with .)
	if strings.HasPrefix(linkName, ".") {
		return fmt.Errorf("link name cannot start with '.': %s", linkName)
	}

	// Check for control characters
	for _, c := range linkName {
		if c < 32 || c == 127 {
			return fmt.Errorf("link name cannot contain control characters: %s", linkName)
		}
	}

	return nil
}

// CreateTreeLink creates a symlink to a target or poller.
//
// FIX #8: This function now validates linkName to prevent path traversal.
func (s *Store) CreateTreeLink(namespace, treePath, linkName, linkType, linkRef string) error {
	// FIX #8: Validate link name to prevent path traversal
	if err := validateLinkName(linkName); err != nil {
		return fmt.Errorf("invalid link name: %w", err)
	}

	treePath = normalizePath(treePath)
	fullPath := path.Join(treePath, linkName)

	// FIX #8: Additional validation - ensure fullPath stays under treePath
	// This catches edge cases that might slip through validateLinkName
	if !strings.HasPrefix(normalizePath(fullPath), treePath) && treePath != "/" {
		return fmt.Errorf("path traversal detected: resulting path would escape base directory")
	}

	// Validate link type
	var nodeType string
	switch linkType {
	case "target":
		nodeType = "link_target"
	case "poller":
		nodeType = "link_poller"
	default:
		return fmt.Errorf("invalid link type: %s", linkType)
	}

	// Validate that target/poller exists
	exists, err := s.validateLinkRef(namespace, linkType, linkRef)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%s not found: %s", linkType, linkRef)
	}

	// Format linkRef
	formattedRef := fmt.Sprintf("%s:%s", linkType, linkRef)

	_, err = s.db.Exec(`
		INSERT INTO tree_nodes (namespace, path, node_type, link_ref, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, namespace, fullPath, nodeType, formattedRef, time.Now())

	return err
}

func (s *Store) validateLinkRef(namespace, linkType, linkRef string) (bool, error) {
	switch linkType {
	case "target":
		return s.TargetExists(namespace, linkRef)
	case "poller":
		parts := strings.SplitN(linkRef, "/", 2)
		if len(parts) != 2 {
			return false, fmt.Errorf("invalid poller reference: %s (expected target/poller)", linkRef)
		}
		return s.PollerExists(namespace, parts[0], parts[1])
	default:
		return false, fmt.Errorf("invalid link type: %s", linkType)
	}
}

// =============================================================================
// Query Operations
// =============================================================================

// GetTreeNode retrieves a single tree node.
func (s *Store) GetTreeNode(namespace, nodePath string) (*TreeNode, error) {
	nodePath = normalizePath(nodePath)

	var node TreeNode
	var linkRef, description sql.NullString

	err := s.db.QueryRow(`
		SELECT namespace, path, node_type, link_ref, description, created_at
		FROM tree_nodes WHERE namespace = ? AND path = ?
	`, namespace, nodePath).Scan(&node.Namespace, &node.Path, &node.NodeType,
		&linkRef, &description, &node.CreatedAt)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query tree node: %w", err)
	}

	if linkRef.Valid {
		node.LinkRef = linkRef.String
	}
	if description.Valid {
		node.Description = description.String
	}

	return &node, nil
}

// =============================================================================
// Delete Operations
// =============================================================================

// DeleteTreeNode deletes a tree node.
func (s *Store) DeleteTreeNode(namespace, nodePath string, recursive, force bool) (int, error) {
	nodePath = normalizePath(nodePath)

	// Check if has children
	children, err := s.ListTreeChildren(namespace, nodePath)
	if err != nil {
		return 0, err
	}

	if len(children) > 0 && !recursive && !force {
		return 0, fmt.Errorf("directory not empty: %d children", len(children))
	}

	var deleted int
	err = s.Transaction(func(tx *sql.Tx) error {
		if recursive {
			// Delete all descendants
			result, err := tx.Exec(`
				DELETE FROM tree_nodes 
				WHERE namespace = ? AND (path = ? OR path LIKE ?)
			`, namespace, nodePath, nodePath+"/%")
			if err != nil {
				return err
			}
			rows, _ := result.RowsAffected()
			deleted = int(rows)
		} else {
			// Delete only this node
			result, err := tx.Exec(`
				DELETE FROM tree_nodes WHERE namespace = ? AND path = ?
			`, namespace, nodePath)
			if err != nil {
				return err
			}
			rows, _ := result.RowsAffected()
			deleted = int(rows)
		}
		return nil
	})

	return deleted, err
}

// DeleteLinksToTarget deletes all links pointing to a target.
func (s *Store) DeleteLinksToTarget(namespace, targetName string) (int, error) {
	result, err := s.db.Exec(`
		DELETE FROM tree_nodes 
		WHERE namespace = ? AND link_ref = ?
	`, namespace, "target:"+targetName)
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

// DeleteLinksToPoller deletes all links pointing to a poller.
func (s *Store) DeleteLinksToPoller(namespace, targetName, pollerName string) (int, error) {
	result, err := s.db.Exec(`
		DELETE FROM tree_nodes 
		WHERE namespace = ? AND link_ref = ?
	`, namespace, fmt.Sprintf("poller:%s/%s", targetName, pollerName))
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

// DeleteLinksToTargetAndPollers deletes all links to a target and all its pollers.
func (s *Store) DeleteLinksToTargetAndPollers(namespace, targetName string) (int, error) {
	result, err := s.db.Exec(`
		DELETE FROM tree_nodes 
		WHERE namespace = ? AND (link_ref = ? OR link_ref LIKE ?)
	`, namespace, "target:"+targetName, "poller:"+targetName+"/%")
	if err != nil {
		return 0, err
	}
	rows, _ := result.RowsAffected()
	return int(rows), nil
}

// =============================================================================
// Link Query Operations
// =============================================================================

// GetLinksToTarget returns all tree paths that link to a target.
func (s *Store) GetLinksToTarget(namespace, targetName string) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT path FROM tree_nodes 
		WHERE namespace = ? AND link_ref = ?
	`, namespace, "target:"+targetName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// GetLinksToPoller returns all tree paths that link to a poller.
func (s *Store) GetLinksToPoller(namespace, targetName, pollerName string) ([]string, error) {
	rows, err := s.db.Query(`
		SELECT path FROM tree_nodes 
		WHERE namespace = ? AND link_ref = ?
	`, namespace, fmt.Sprintf("poller:%s/%s", targetName, pollerName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		paths = append(paths, p)
	}
	return paths, rows.Err()
}

// =============================================================================
// Tree Children Operations
// =============================================================================

// ListTreeChildren returns immediate children of a path.
func (s *Store) ListTreeChildren(namespace, parentPath string) ([]*TreeNode, error) {
	parentPath = normalizePath(parentPath)

	// Query for direct children only
	// Children have parentPath as prefix and no additional slashes in the suffix
	var pattern string
	if parentPath == "/" {
		pattern = "/%"
	} else {
		pattern = parentPath + "/%"
	}

	rows, err := s.db.Query(`
		SELECT namespace, path, node_type, link_ref, description, source, content_hash, synced_at, created_at
		FROM tree_nodes 
		WHERE namespace = ? 
		  AND path LIKE ?
		  AND path NOT LIKE ?
		ORDER BY path
	`, namespace, pattern, pattern+"/%")
	if err != nil {
		return nil, fmt.Errorf("query tree children: %w", err)
	}
	defer rows.Close()

	return scanTreeNodes(rows)
}

// ListAllTreeNodes returns all tree nodes in a namespace.
func (s *Store) ListAllTreeNodes(namespace string) ([]*TreeNode, error) {
	rows, err := s.db.Query(`
		SELECT namespace, path, node_type, link_ref, description, source, content_hash, synced_at, created_at
		FROM tree_nodes WHERE namespace = ? ORDER BY path
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("query tree nodes: %w", err)
	}
	defer rows.Close()

	return scanTreeNodes(rows)
}

func scanTreeNodes(rows *sql.Rows) ([]*TreeNode, error) {
	var nodes []*TreeNode
	for rows.Next() {
		n := &TreeNode{}
		var linkRef, description sql.NullString
		var source sql.NullString
		var contentHash sql.NullInt64
		var syncedAt sql.NullTime

		if err := rows.Scan(
			&n.Namespace, &n.Path, &n.NodeType, &linkRef, &description,
			&source, &contentHash, &syncedAt, &n.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tree node: %w", err)
		}

		if linkRef.Valid {
			n.LinkRef = linkRef.String
		}
		if description.Valid {
			n.Description = description.String
		}
		if source.Valid {
			n.Source = source.String
		}
		if contentHash.Valid {
			n.ContentHash = uint64(contentHash.Int64)
		}
		if syncedAt.Valid {
			n.SyncedAt = &syncedAt.Time
		}

		nodes = append(nodes, n)
	}

	return nodes, rows.Err()
}

// =============================================================================
// Bulk Operations (for Sync)
// =============================================================================

// BulkCreateTreeNodes creates multiple tree nodes in a single transaction.
func (s *Store) BulkCreateTreeNodes(nodes []*TreeNode) error {
	if len(nodes) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			INSERT INTO tree_nodes (namespace, path, node_type, link_ref, description, 
			                        source, content_hash, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, n := range nodes {
			if n.Source == "" {
				n.Source = "yaml"
			}

			_, err := stmt.Exec(
				n.Namespace, n.Path, n.NodeType, n.LinkRef, n.Description,
				n.Source, n.ContentHash, now,
			)
			if err != nil {
				return fmt.Errorf("create tree node %s:%s: %w", n.Namespace, n.Path, err)
			}

			n.CreatedAt = now
		}
		return nil
	})
}

// BulkUpdateTreeNodes updates multiple tree nodes in a single transaction.
func (s *Store) BulkUpdateTreeNodes(nodes []*TreeNode) error {
	if len(nodes) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`
			UPDATE tree_nodes 
			SET node_type = ?, link_ref = ?, description = ?, 
			    source = ?, content_hash = ?, synced_at = ?
			WHERE namespace = ? AND path = ?
		`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		now := time.Now()
		for _, n := range nodes {
			_, err := stmt.Exec(
				n.NodeType, n.LinkRef, n.Description,
				n.Source, n.ContentHash, &now,
				n.Namespace, n.Path,
			)
			if err != nil {
				return fmt.Errorf("update tree node %s:%s: %w", n.Namespace, n.Path, err)
			}

			n.SyncedAt = &now
		}
		return nil
	})
}

// BulkDeleteTreeNodes deletes multiple tree nodes by key (namespace:path).
func (s *Store) BulkDeleteTreeNodes(keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	return s.Transaction(func(tx *sql.Tx) error {
		stmt, err := tx.Prepare(`DELETE FROM tree_nodes WHERE namespace = ? AND path = ?`)
		if err != nil {
			return err
		}
		defer stmt.Close()

		for _, key := range keys {
			ns, nodePath := parseTreeNodeKey(key)
			if ns == "" || nodePath == "" {
				continue
			}

			if _, err := stmt.Exec(ns, nodePath); err != nil {
				return fmt.Errorf("delete tree node %s: %w", key, err)
			}
		}
		return nil
	})
}

// parseTreeNodeKey splits "namespace:/path" into parts.
func parseTreeNodeKey(key string) (namespace, nodePath string) {
	idx := strings.Index(key, ":")
	if idx == -1 {
		return "", ""
	}
	return key[:idx], key[idx+1:]
}

// =============================================================================
// Path Utilities
// =============================================================================

// normalizePath normalizes a path to have a leading slash and no trailing slash.
func normalizePath(p string) string {
	p = path.Clean(p)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if p != "/" && strings.HasSuffix(p, "/") {
		p = strings.TrimSuffix(p, "/")
	}
	return p
}
