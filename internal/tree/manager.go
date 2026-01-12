// Package tree - Tree Manager
//
// LOCATION: internal/tree/manager.go
//
// The tree manager provides a unified view of the virtual filesystem by
// combining static links, label-based views, and smart folders.

package tree

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/xtxerr/stalker/internal/store"
)

// =============================================================================
// Browse Result Types
// =============================================================================

// BrowseEntry represents a single entry in a directory listing.
type BrowseEntry struct {
	Name        string // Entry name (last path component)
	Path        string // Full path
	Type        string // "directory", "link_target", "link_poller", "smart_folder"
	LinkRef     string // For links: "target:name" or "poller:target/name"
	Description string
	Priority    int    // Higher priority entries override lower ones
	Source      string // "static", "view", "smart"
	ViewName    string // Name of view that generated this (for view entries)
}

// BrowseResult contains the results of a browse operation.
type BrowseResult struct {
	Path       string
	Entries    []BrowseEntry
	IsLeaf     bool   // True if this is a link (no children)
	ResolvedTo string // If IsLeaf, what it resolves to
}

// =============================================================================
// Tree Manager
// =============================================================================

// Manager provides unified access to the tree.
type Manager struct {
	store *store.Store

	// Resolvers
	viewResolver   *ViewResolver
	smartResolver  *SmartFolderResolver

	// Cache for directory listings
	dirCache   map[string]*dirCacheEntry
	dirCacheMu sync.RWMutex
}

type dirCacheEntry struct {
	entries []BrowseEntry
	// Could add expiration here
}

// NewManager creates a new tree manager.
func NewManager(s *store.Store) *Manager {
	return &Manager{
		store:          s,
		viewResolver:   NewViewResolver(s),
		smartResolver:  NewSmartFolderResolver(s),
		dirCache:       make(map[string]*dirCacheEntry),
	}
}

// =============================================================================
// Browse Operations
// =============================================================================

// Browse returns the contents of a directory path.
//
// It combines entries from three sources:
// 1. Static links (highest priority)
// 2. View-generated paths (medium priority)
// 3. Smart folder contents (lowest priority)
//
// When paths conflict, higher priority wins.
func (m *Manager) Browse(ctx context.Context, namespace, path string) (*BrowseResult, error) {
	path = normalizePath(path)

	result := &BrowseResult{
		Path: path,
	}

	// Collect entries from all sources
	var allEntries []BrowseEntry

	// 1. Static entries (priority 100)
	staticEntries, err := m.getStaticEntries(namespace, path)
	if err != nil {
		return nil, err
	}
	allEntries = append(allEntries, staticEntries...)

	// 2. View-generated entries (priority from view, typically 50)
	viewEntries, err := m.getViewEntries(namespace, path)
	if err != nil {
		return nil, err
	}
	allEntries = append(allEntries, viewEntries...)

	// 3. Smart folder entries (priority from folder, typically 25)
	smartEntries, err := m.getSmartFolderEntries(namespace, path)
	if err != nil {
		return nil, err
	}
	allEntries = append(allEntries, smartEntries...)

	// Deduplicate by name (higher priority wins)
	result.Entries = m.deduplicateEntries(allEntries)

	return result, nil
}

// Resolve follows a path to its final target/poller.
func (m *Manager) Resolve(ctx context.Context, namespace, path string) (*ResolveResult, error) {
	path = normalizePath(path)

	// Check static first
	node, err := m.store.GetTreeNode(namespace, path)
	if err != nil {
		return nil, err
	}

	if node != nil && node.LinkRef != "" {
		return parseResolveResult(node.LinkRef), nil
	}

	// Check views
	// This is more complex - we'd need to reverse-lookup which view could
	// generate this path. For now, just check static.

	return nil, nil
}

// ResolveResult contains the resolved target/poller.
type ResolveResult struct {
	Type    string // "target" or "poller"
	Target  string
	Poller  string // Only for poller links
	LinkRef string
}

func parseResolveResult(linkRef string) *ResolveResult {
	if strings.HasPrefix(linkRef, "target:") {
		return &ResolveResult{
			Type:    "target",
			Target:  linkRef[7:],
			LinkRef: linkRef,
		}
	}
	if strings.HasPrefix(linkRef, "poller:") {
		ref := linkRef[7:]
		parts := strings.SplitN(ref, "/", 2)
		if len(parts) == 2 {
			return &ResolveResult{
				Type:    "poller",
				Target:  parts[0],
				Poller:  parts[1],
				LinkRef: linkRef,
			}
		}
	}
	return nil
}

// =============================================================================
// Static Entries
// =============================================================================

func (m *Manager) getStaticEntries(namespace, path string) ([]BrowseEntry, error) {
	children, err := m.store.ListTreeChildren(namespace, path)
	if err != nil {
		return nil, err
	}

	entries := make([]BrowseEntry, 0, len(children))
	for _, child := range children {
		name := lastPathComponent(child.Path)
		entries = append(entries, BrowseEntry{
			Name:        name,
			Path:        child.Path,
			Type:        child.NodeType,
			LinkRef:     child.LinkRef,
			Description: child.Description,
			Priority:    100, // Static always highest
			Source:      "static",
		})
	}

	return entries, nil
}

// =============================================================================
// View Entries
// =============================================================================

func (m *Manager) getViewEntries(namespace, path string) ([]BrowseEntry, error) {
	// Load views if not cached
	if err := m.viewResolver.LoadViews(namespace); err != nil {
		return nil, err
	}

	// Get all targets and resolve their view paths
	targets, err := m.store.ListTargets(namespace)
	if err != nil {
		return nil, err
	}

	var entries []BrowseEntry

	for _, target := range targets {
		resolvedPaths := m.viewResolver.ResolveTarget(target)
		for _, rp := range resolvedPaths {
			// Check if this resolved path is a child of the requested path
			if isChildOf(path, rp.Path) {
				// Get the immediate child name
				childName := getImmediateChild(path, rp.Path)
				if childName == "" {
					continue
				}

				// Determine if this is the final entry or an intermediate directory
				if rp.Path == path+"/"+childName {
					// Direct child - it's the link
					entries = append(entries, BrowseEntry{
						Name:     childName,
						Path:     rp.Path,
						Type:     "link_target",
						LinkRef:  rp.LinkRef,
						Priority: rp.Priority,
						Source:   "view",
						ViewName: rp.ViewName,
					})
				} else {
					// Intermediate directory
					entries = append(entries, BrowseEntry{
						Name:     childName,
						Path:     path + "/" + childName,
						Type:     "directory",
						Priority: rp.Priority,
						Source:   "view",
						ViewName: rp.ViewName,
					})
				}
			}
		}
	}

	// Also resolve pollers
	// (simplified - in production you'd batch this)

	return entries, nil
}

// =============================================================================
// Smart Folder Entries
// =============================================================================

func (m *Manager) getSmartFolderEntries(namespace, path string) ([]BrowseEntry, error) {
	// Check if this path IS a smart folder
	folder, err := m.store.GetSmartFolder(namespace, path)
	if err != nil {
		return nil, err
	}

	if folder == nil || !folder.Enabled {
		return nil, nil
	}

	// Resolve the smart folder
	contents, err := m.smartResolver.Resolve(folder)
	if err != nil {
		return nil, err
	}

	entries := make([]BrowseEntry, 0, len(contents))
	for _, c := range contents {
		entryType := "link_target"
		if c.Type == "poller" {
			entryType = "link_poller"
		}

		entries = append(entries, BrowseEntry{
			Name:     c.Name,
			Path:     path + "/" + c.Name,
			Type:     entryType,
			LinkRef:  c.LinkRef,
			Priority: folder.Priority,
			Source:   "smart",
		})
	}

	return entries, nil
}

// =============================================================================
// Entry Deduplication
// =============================================================================

func (m *Manager) deduplicateEntries(entries []BrowseEntry) []BrowseEntry {
	if len(entries) == 0 {
		return entries
	}

	// Sort by priority (descending) then name
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Priority != entries[j].Priority {
			return entries[i].Priority > entries[j].Priority
		}
		return entries[i].Name < entries[j].Name
	})

	// Deduplicate by name (keep first = highest priority)
	seen := make(map[string]bool)
	result := make([]BrowseEntry, 0, len(entries))

	for _, e := range entries {
		if !seen[e.Name] {
			seen[e.Name] = true
			result = append(result, e)
		}
	}

	// Re-sort by name for consistent output
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// =============================================================================
// Cache Management
// =============================================================================

// InvalidateCache clears the cache for a path.
func (m *Manager) InvalidateCache(namespace, path string) {
	key := namespace + ":" + path

	m.dirCacheMu.Lock()
	delete(m.dirCache, key)
	m.dirCacheMu.Unlock()

	// Also invalidate smart folder cache
	m.smartResolver.InvalidateCache(namespace, path)
}

// InvalidateAllCaches clears all caches.
func (m *Manager) InvalidateAllCaches() {
	m.dirCacheMu.Lock()
	m.dirCache = make(map[string]*dirCacheEntry)
	m.dirCacheMu.Unlock()

	m.smartResolver.InvalidateAll()
}

// =============================================================================
// Helper Functions
// =============================================================================

func normalizePath(p string) string {
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	// Remove trailing slash
	if len(p) > 1 && strings.HasSuffix(p, "/") {
		p = p[:len(p)-1]
	}
	return p
}

func lastPathComponent(p string) string {
	idx := strings.LastIndex(p, "/")
	if idx == -1 {
		return p
	}
	return p[idx+1:]
}

// isChildOf returns true if childPath is under parentPath.
func isChildOf(parentPath, childPath string) bool {
	if parentPath == "/" {
		return strings.HasPrefix(childPath, "/") && childPath != "/"
	}
	return strings.HasPrefix(childPath, parentPath+"/")
}

// getImmediateChild returns the immediate child name from parentPath to childPath.
// e.g., parentPath="/a", childPath="/a/b/c" returns "b"
func getImmediateChild(parentPath, childPath string) string {
	if !isChildOf(parentPath, childPath) {
		return ""
	}

	var rest string
	if parentPath == "/" {
		rest = childPath[1:]
	} else {
		rest = childPath[len(parentPath)+1:]
	}

	idx := strings.Index(rest, "/")
	if idx == -1 {
		return rest
	}
	return rest[:idx]
}
