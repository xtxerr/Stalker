// Package manager provides business logic for entity management.
package manager

import (
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/xtxerr/stalker/internal/constants"
	"github.com/xtxerr/stalker/internal/store"
)

// TreeManager handles tree operations with efficient cache invalidation.
type TreeManager struct {
	store *store.Store
	mu    sync.RWMutex

	// Primary cache: "namespace:path" -> node
	nodes map[string]*store.TreeNode

	// Secondary index: namespace -> set of cache keys
	// Enables O(1) invalidation of all nodes in a namespace
	byNamespace map[string]map[string]struct{}
}

// NewTreeManager creates a new tree manager.
func NewTreeManager(s *store.Store) *TreeManager {
	return &TreeManager{
		store:       s,
		nodes:       make(map[string]*store.TreeNode),
		byNamespace: make(map[string]map[string]struct{}),
	}
}

// treeKey returns the cache key for a tree node.
func treeKey(namespace, nodePath string) string {
	return namespace + ":" + normalizePath(nodePath)
}

// normalizePath normalizes a path.
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

// Load loads tree nodes from store for a specific namespace (on-demand loading).
func (m *TreeManager) Load(namespace string) error {
	// We load on-demand, but could pre-load for specific namespaces
	return nil
}

// =============================================================================
// Cache Management with Secondary Index
// =============================================================================

// addToCache adds a node to the cache and secondary index.
// Caller must hold the write lock.
func (m *TreeManager) addToCache(namespace, nodePath string, node *store.TreeNode) {
	key := treeKey(namespace, nodePath)
	m.nodes[key] = node

	// Update secondary index
	if m.byNamespace[namespace] == nil {
		m.byNamespace[namespace] = make(map[string]struct{})
	}
	m.byNamespace[namespace][key] = struct{}{}
}

// removeFromCache removes a node from the cache and secondary index.
// Caller must hold the write lock.
func (m *TreeManager) removeFromCache(key string) {
	if node, ok := m.nodes[key]; ok {
		// Remove from secondary index
		if keys, ok := m.byNamespace[node.Namespace]; ok {
			delete(keys, key)
			if len(keys) == 0 {
				delete(m.byNamespace, node.Namespace)
			}
		}
	}
	delete(m.nodes, key)
}

// invalidatePath removes a path from the cache.
func (m *TreeManager) invalidatePath(namespace, nodePath string) {
	key := treeKey(namespace, nodePath)

	m.mu.Lock()
	m.removeFromCache(key)
	m.mu.Unlock()
}

// invalidateAllForNamespace removes all cached nodes for a namespace.
// Uses secondary index for O(1) lookup instead of O(n) iteration.
func (m *TreeManager) invalidateAllForNamespace(namespace string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Use secondary index for efficient invalidation
	keys, ok := m.byNamespace[namespace]
	if !ok {
		return
	}

	// Remove all keys for this namespace
	for key := range keys {
		delete(m.nodes, key)
	}

	// Clear the namespace index
	delete(m.byNamespace, namespace)
}

// invalidatePathAndChildren removes a path and all its children from cache.
func (m *TreeManager) invalidatePathAndChildren(namespace, nodePath string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	keyPrefix := treeKey(namespace, nodePath)

	// Get all keys for this namespace
	keys, ok := m.byNamespace[namespace]
	if !ok {
		return
	}

	// Remove matching keys
	for key := range keys {
		if key == keyPrefix || strings.HasPrefix(key, keyPrefix+"/") {
			delete(m.nodes, key)
			delete(keys, key)
		}
	}

	if len(keys) == 0 {
		delete(m.byNamespace, namespace)
	}
}

// =============================================================================
// Directory Operations
// =============================================================================

// CreateDirectory creates a directory and all parent directories.
func (m *TreeManager) CreateDirectory(namespace, nodePath, description string) error {
	nodePath = normalizePath(nodePath)

	if err := m.store.CreateTreeDirectory(namespace, nodePath, description); err != nil {
		return err
	}

	// Invalidate cache for this path and parents
	m.invalidatePath(namespace, nodePath)
	return nil
}

// =============================================================================
// Link Operations
// =============================================================================

// CreateLink creates a symlink to a target or poller.
func (m *TreeManager) CreateLink(namespace, treePath, linkName, linkType, linkRef string) error {
	treePath = normalizePath(treePath)

	// Ensure parent directory exists
	if treePath != "/" {
		if err := m.store.CreateTreeDirectory(namespace, treePath, ""); err != nil {
			return err
		}
	}

	if err := m.store.CreateTreeLink(namespace, treePath, linkName, linkType, linkRef); err != nil {
		return err
	}

	// Invalidate cache
	fullPath := path.Join(treePath, linkName)
	m.invalidatePath(namespace, fullPath)
	return nil
}

// =============================================================================
// Query Operations
// =============================================================================

// Get returns a tree node.
func (m *TreeManager) Get(namespace, nodePath string) (*store.TreeNode, error) {
	nodePath = normalizePath(nodePath)
	key := treeKey(namespace, nodePath)

	m.mu.RLock()
	node, ok := m.nodes[key]
	m.mu.RUnlock()

	if ok {
		return node, nil
	}

	// Load from store
	node, err := m.store.GetTreeNode(namespace, nodePath)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}

	// Update cache with proper index management
	m.mu.Lock()
	m.addToCache(namespace, nodePath, node)
	m.mu.Unlock()

	return node, nil
}

// ListChildren returns direct children of a path.
func (m *TreeManager) ListChildren(namespace, parentPath string) ([]*store.TreeNode, error) {
	parentPath = normalizePath(parentPath)
	return m.store.ListTreeChildren(namespace, parentPath)
}

// =============================================================================
// Delete Operations
// =============================================================================

// Delete deletes a tree node.
func (m *TreeManager) Delete(namespace, nodePath string, recursive, force bool) (int, error) {
	nodePath = normalizePath(nodePath)

	deleted, err := m.store.DeleteTreeNode(namespace, nodePath, recursive, force)
	if err != nil {
		return 0, err
	}

	// Invalidate cache
	if recursive {
		m.invalidatePathAndChildren(namespace, nodePath)
	} else {
		m.invalidatePath(namespace, nodePath)
	}

	return deleted, nil
}

// =============================================================================
// Path Resolution
// =============================================================================

// ResolvePath resolves a tree path, following symlinks.
// Returns the final entity type and reference.
func (m *TreeManager) ResolvePath(namespace, nodePath string) (nodeType, linkRef string, err error) {
	nodePath = normalizePath(nodePath)

	node, err := m.Get(namespace, nodePath)
	if err != nil {
		return "", "", err
	}
	if node == nil {
		return "", "", fmt.Errorf("path not found: %s", nodePath)
	}

	return node.NodeType, node.LinkRef, nil
}

// =============================================================================
// Link Query Operations
// =============================================================================

// GetLinksToTarget returns all tree paths that link to a target.
func (m *TreeManager) GetLinksToTarget(namespace, targetName string) ([]string, error) {
	return m.store.GetLinksToTarget(namespace, targetName)
}

// GetLinksToPoller returns all tree paths that link to a poller.
func (m *TreeManager) GetLinksToPoller(namespace, targetName, pollerName string) ([]string, error) {
	return m.store.GetLinksToPoller(namespace, targetName, pollerName)
}

// =============================================================================
// Link Deletion Operations
// =============================================================================

// DeleteLinksToTarget deletes all links pointing to a target.
func (m *TreeManager) DeleteLinksToTarget(namespace, targetName string) (int, error) {
	deleted, err := m.store.DeleteLinksToTarget(namespace, targetName)
	if err != nil {
		return 0, err
	}

	// Invalidate cache
	m.invalidateAllForNamespace(namespace)
	return deleted, nil
}

// DeleteLinksToPoller deletes all links pointing to a poller.
func (m *TreeManager) DeleteLinksToPoller(namespace, targetName, pollerName string) (int, error) {
	deleted, err := m.store.DeleteLinksToPoller(namespace, targetName, pollerName)
	if err != nil {
		return 0, err
	}

	// Invalidate cache
	m.invalidateAllForNamespace(namespace)
	return deleted, nil
}

// DeleteLinksToTargetAndPollers deletes all links to a target and its pollers.
func (m *TreeManager) DeleteLinksToTargetAndPollers(namespace, targetName string) (int, error) {
	deleted, err := m.store.DeleteLinksToTargetAndPollers(namespace, targetName)
	if err != nil {
		return 0, err
	}

	// Invalidate cache
	m.invalidateAllForNamespace(namespace)
	return deleted, nil
}

// =============================================================================
// Link Reference Parsing
// =============================================================================

// ParseLinkRef parses a link reference.
// Returns linkType ("target" or "poller") and the reference parts.
func ParseLinkRef(ref string) (linkType string, target string, poller string, err error) {
	if strings.HasPrefix(ref, constants.LinkPrefixTarget) {
		return "target", ref[len(constants.LinkPrefixTarget):], "", nil
	}
	if strings.HasPrefix(ref, constants.LinkPrefixPoller) {
		parts := strings.SplitN(ref[len(constants.LinkPrefixPoller):], "/", 2)
		if len(parts) != 2 {
			return "", "", "", fmt.Errorf("invalid poller reference: %s", ref)
		}
		return "poller", parts[0], parts[1], nil
	}
	return "", "", "", fmt.Errorf("unknown link type: %s", ref)
}

// =============================================================================
// Browse with Link Resolution
// =============================================================================

// TreeBrowseResult represents a result when browsing the tree.
type TreeBrowseResult struct {
	Path        string
	NodeType    string
	Description string
	LinkRef     string

	// If this is a link, these are populated
	LinkedTarget *store.Target
	LinkedPoller *store.Poller
}

// Browse returns contents of a tree path with resolved link information.
func (m *TreeManager) Browse(namespace, parentPath string, resolveLinks bool, targetMgr *TargetManager, pollerMgr *PollerManager) ([]*TreeBrowseResult, error) {
	children, err := m.ListChildren(namespace, parentPath)
	if err != nil {
		return nil, err
	}

	results := make([]*TreeBrowseResult, 0, len(children))
	for _, child := range children {
		result := &TreeBrowseResult{
			Path:        child.Path,
			NodeType:    child.NodeType,
			Description: child.Description,
			LinkRef:     child.LinkRef,
		}

		// Resolve links if requested
		if resolveLinks && child.LinkRef != "" {
			linkType, targetName, pollerName, err := ParseLinkRef(child.LinkRef)
			if err == nil {
				switch linkType {
				case "target":
					if target, err := targetMgr.Get(namespace, targetName); err == nil && target != nil {
						result.LinkedTarget = target
					}
				case "poller":
					if poller, err := pollerMgr.Get(namespace, targetName, pollerName); err == nil && poller != nil {
						result.LinkedPoller = poller
					}
				}
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// =============================================================================
// Statistics
// =============================================================================

// CacheStats returns cache statistics.
func (m *TreeManager) CacheStats() (nodeCount, namespaceCount int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes), len(m.byNamespace)
}
